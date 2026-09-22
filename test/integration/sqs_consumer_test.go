//go:build integration

// M7.5: consumidor SQS contra PostgreSQL + LocalStack reais. Uma única
// transação SQL processa inbox → caso de uso → inbox complete / outbox e a
// mensagem é apagada após o commit. Rejeições de negócio commitadas também são
// apagadas; mensagens inválidas (envelope, tipo, OPENING, hash divergente) vão
// para a DLQ com atributo do motivo; replay HTTP×SQS não movimenta saldo.
package integration

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/BrunoMoreno/backend-challenge-go/internal/app/processwager"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wager"
	"github.com/BrunoMoreno/backend-challenge-go/internal/infra/postgres"
	"github.com/BrunoMoreno/backend-challenge-go/internal/infra/sqsconsumer"
)

func inputQueueURL(t string) string { return sqsEndpoint + "/000000000000/wager-transactions.fifo" }
func dlqURL(t string) string        { return sqsEndpoint + "/000000000000/wager-transactions-dlq.fifo" }

// requestedJSON monta o corpo do envelope WagerTransactionRequested.
func requestedJSON(t *testing.T, messageID string, in processwager.Input) []byte {
	t.Helper()
	obj := map[string]any{
		"providerId":            in.ProviderID,
		"externalTransactionId": in.ExternalTransactionID,
		"idempotencyKey":        in.IdempotencyKey,
		"playerId":              in.PlayerID,
		"walletId":              in.WalletID,
		"roundId":               in.RoundID,
		"gameId":                in.GameID,
		"kind":                  in.Kind,
		"money": map[string]string{
			"amount":   in.Amount.String(),
			"currency": in.Amount.Currency().String(),
		},
	}
	if in.ReferenceExternalTransactionID != "" {
		obj["referenceExternalTransactionId"] = in.ReferenceExternalTransactionID
	}
	raw, err := json.Marshal(obj)
	if err != nil {
		t.Fatalf("marshal data: %v", err)
	}
	env := map[string]any{
		"messageId":  messageID,
		"type":       "WagerTransactionRequested",
		"occurredAt": time.Now().UTC().Format(time.RFC3339),
		"data":       json.RawMessage(raw),
	}
	body, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return body
}

// sendRequested publica a mensagem na fila de entrada com MessageGroupId e
// MessageDeduplicationId do contrato.
func sendRequested(t *testing.T, client *awssqs.Client, queueURL, messageID, walletID string, body []byte) {
	t.Helper()
	_, err := client.SendMessage(context.Background(), &awssqs.SendMessageInput{
		QueueUrl:               aws.String(queueURL),
		MessageBody:            aws.String(string(body)),
		MessageGroupId:         aws.String(walletID),
		MessageDeduplicationId: aws.String(messageID),
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
}

// receiveNext lê a próxima mensagem (sem apagar) e devolve o conteúdo e os
// atributos; "" quando a fila está vazia.
func receiveNext(t *testing.T, client *awssqs.Client, queueURL string) (string, map[string]string) {
	t.Helper()
	out, err := client.ReceiveMessage(context.Background(), &awssqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     1,
	})
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if len(out.Messages) == 0 {
		return "", nil
	}
	return aws.ToString(out.Messages[0].Body), out.Messages[0].Attributes
}

// waitQueueEmpty espera a fila ficar vazia enquanto o consumidor NÃO estiver
// ativo (senão o poll roubaria a mensagem).
func waitQueueEmpty(t *testing.T, client *awssqs.Client, queueURL string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if body, _ := receiveNext(t, client, queueURL); body == "" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("fila %s não esvaziou em %v", queueURL, timeout)
}

// startConsumer instancia o consumidor real (LocalStack + Postgres).
func startConsumer(t *testing.T, f *postgres.UnitOfWorkFactory) *sqsconsumer.Consumer {
	t.Helper()
	client := newSQSClient(t)
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	svc := processwager.NewService(postgres.NewDatabase(f))
	return sqsconsumer.New(client, svc, f, logger, sqsconsumer.Config{
		QueueURL:          inputQueueURL(t.Name()),
		DLQURL:            dlqURL(t.Name()),
		VisibilityTimeout: 5 * time.Second,
		WaitTime:          1 * time.Second,
		Concurrency:       2,
		MaxReceiveCount:   5,
		BackoffBase:       time.Second,
		BackoffMax:        time.Minute,
	})
}

// runConsumer executa o consumidor em background até o cancelamento. O stop é
// registrado como t.Cleanup IMEDIATAMENTE após o start: se o teste falhar antes
// de chamá-lo explicitamente, o goroutine é encerrado antes de o t.Cleanup do
// pool fechar as conexões — sem isso uma t.Fatalf fecharia o pool com o
// consumidor ainda lendo (docs/solve/TEST-INTEGRATION.md). O stop é idempotente
// (sync.Once), então a chamada explícita + cleanup não bloqueia o segundo.
func runConsumer(t *testing.T, c *sqsconsumer.Consumer) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Run(ctx)
	}()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("consumidor não encerrou após cancelamento")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

// waitInboxCompleted espera a inbox do consumidor concluir a mensagem.
func waitInboxCompleted(t *testing.T, messageID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n := countRows(t, `SELECT count(*) FROM inbox WHERE consumer_name = $1 AND message_id = $2 AND completed_at IS NOT NULL`,
			sqsconsumer.ConsumerName, messageID); n == 1 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("inbox %s não concluiu em %v", messageID, timeout)
}

// waitDLQ espera uma mensagem na DLQ, devolve o body e a apaga (o consumidor
// não lê a DLQ, então o poll não interfere).
func waitDLQ(t *testing.T, client *awssqs.Client, dlq string, timeout time.Duration) (string, map[string]string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out, err := client.ReceiveMessage(context.Background(), &awssqs.ReceiveMessageInput{
			QueueUrl:            aws.String(dlq),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     1,
			MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{
				sqstypes.MessageSystemAttributeNameApproximateReceiveCount,
			},
			MessageAttributeNames: []string{"failureReason", "failureDetail"},
		})
		if err != nil {
			t.Fatalf("receive dlq: %v", err)
		}
		if len(out.Messages) == 0 {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		m := out.Messages[0]
		if _, err := client.DeleteMessage(context.Background(), &awssqs.DeleteMessageInput{
			QueueUrl:      aws.String(dlq),
			ReceiptHandle: m.ReceiptHandle,
		}); err != nil {
			t.Fatalf("delete dlq: %v", err)
		}
		return aws.ToString(m.Body), messageAttrs(m)
	}
	t.Fatalf("DLQ vazia após %v", timeout)
	return "", nil
}

// messageAttrs agrupa atributos do sistema e de mensagem num mapa de strings.
func messageAttrs(m sqstypes.Message) map[string]string {
	out := make(map[string]string)
	for k, v := range m.Attributes {
		out[k] = v
	}
	for k, v := range m.MessageAttributes {
		out[k] = aws.ToString(v.StringValue)
	}
	return out
}

func TestSQSConsumerProcessesAndDeletes(t *testing.T) {
	f := newTestUoW(t)
	seedWallet(t, f, "wal-sqs", "player-sqs", "100.00", 10000)
	client := newSQSClient(t)
	queueURL := inputQueueURL(t.Name())

	in := processwager.Input{
		ProviderID:            unique("p-sqs"),
		ExternalTransactionID: unique("ext-sqs"),
		PlayerID:              unique("player-sqs"),
		WalletID:              unique("wal-sqs"),
		RoundID:               "r1",
		GameID:                "g1",
		Kind:                  wager.KindBet,
		Amount:                mm(t, "30.00", "BRL"),
		IdempotencyKey:        unique("key-sqs"),
	}
	messageID := unique("msg-sqs")
	body := requestedJSON(t, messageID, in)
	sendRequested(t, client, queueURL, messageID, unique("wal-sqs"), body)

	c := startConsumer(t, f)
	stop := runConsumer(t, c)
	waitInboxCompleted(t, messageID, 10*time.Second)
	stop()

	if b, v := walletBalance(t, "wal-sqs"); b != 7000 || v != 3 {
		t.Fatalf("wallet = %d v%d, want 7000/3", b, v)
	}
	if n := countWagersForWallet(t, "wal-sqs"); n != 2 {
		t.Fatalf("wagers = %d, want 2 (open + bet)", n)
	}
	waitQueueEmpty(t, client, queueURL, 3*time.Second)
}

func TestSQSConsumerBusinessRejectionCommitsAndDeletes(t *testing.T) {
	f := newTestUoW(t)
	seedWallet(t, f, "wal-sqsrej", "player-sqsrej", "10.00", 1000)
	client := newSQSClient(t)
	queueURL := inputQueueURL(t.Name())

	in := processwager.Input{
		ProviderID:            unique("p-sqr"),
		ExternalTransactionID: unique("ext-sqr"),
		PlayerID:              unique("player-sqsrej"),
		WalletID:              unique("wal-sqsrej"),
		RoundID:               "r1",
		GameID:                "g1",
		Kind:                  wager.KindBet,
		Amount:                mm(t, "50.00", "BRL"),
		IdempotencyKey:        unique("key-sqr"),
	}
	body := requestedJSON(t, unique("msg-sqr"), in)
	sendRequested(t, client, queueURL, unique("msg-sqr"), unique("wal-sqsrej"), body)

	c := startConsumer(t, f)
	stop := runConsumer(t, c)
	waitInboxCompleted(t, unique("msg-sqr"), 10*time.Second)
	stop()

	if n := countRows(t, `SELECT count(*) FROM wager_transactions
		WHERE idempotency_key = $1 AND state = 'REJECTED' AND failure_code = 'INSUFFICIENT_FUNDS'`,
		unique("key-sqr")); n != 1 {
		t.Fatalf("rejeição commitada = %d, want 1", n)
	}
	if b, _ := walletBalance(t, "wal-sqsrej"); b != 1000 {
		t.Fatalf("wallet = %d, want 1000", b)
	}
	waitQueueEmpty(t, client, queueURL, 3*time.Second)
}

// TestSQSConsumerReplayAfterHTTP: a MESMA operação (mesmo idempotencyKey e
// conteúdo) já processada por HTTP chega pelo SQS → replay idempotente, sem
// movimentar saldo, e a mensagem é apagada.
func TestSQSConsumerReplayAfterHTTP(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)
	seedWallet(t, f, "wal-sqsh", "player-sqsh", "100.00", 10000)
	client := newSQSClient(t)
	queueURL := inputQueueURL(t.Name())
	svc := processwager.NewService(postgres.NewDatabase(f))

	in := processwager.Input{
		ProviderID:            unique("p-sqh"),
		ExternalTransactionID: unique("ext-sqh"),
		PlayerID:              unique("player-sqsh"),
		WalletID:              unique("wal-sqsh"),
		RoundID:               "r1",
		GameID:                "g1",
		Kind:                  wager.KindBet,
		Amount:                mm(t, "30.00", "BRL"),
		IdempotencyKey:        unique("key-sqh"),
	}
	first, err := svc.Process(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if b, v := walletBalance(t, "wal-sqsh"); b != 7000 || v != 3 {
		t.Fatalf("pré-replay: wallet = %d v%d, want 7000/3", b, v)
	}

	body := requestedJSON(t, unique("msg-sqh"), in)
	sendRequested(t, client, queueURL, unique("msg-sqh"), unique("wal-sqsh"), body)

	c := startConsumer(t, f)
	stop := runConsumer(t, c)
	waitInboxCompleted(t, unique("msg-sqh"), 10*time.Second)
	stop()

	if b, v := walletBalance(t, "wal-sqsh"); b != 7000 || v != 3 {
		t.Fatalf("replay SQS movimentou: wallet = %d v%d, want 7000/3", b, v)
	}
	if n := countWagersForWallet(t, "wal-sqsh"); n != 2 {
		t.Fatalf("replay criou transação: %d, want 2", n)
	}
	if n := countEventsForTx(t, first.TransactionID); n != 2 {
		t.Fatalf("replay duplicou eventos: %d, want 2", n)
	}
	waitQueueEmpty(t, client, queueURL, 3*time.Second)
}

func TestSQSConsumerInvalidEnvelopeGoesToDLQ(t *testing.T) {
	client := newSQSClient(t)
	queueURL := inputQueueURL(t.Name())
	dlq := dlqURL(t.Name())

	sendRequested(t, client, queueURL, unique("msg-bad"),
		unique("wal-bad"), []byte(`not json at all`))

	c := startConsumer(t, newTestUoW(t))
	stop := runConsumer(t, c)
	_, attrs := waitDLQ(t, client, dlq, 10*time.Second)
	stop()

	if attrs["failureReason"] != "INVALID_ENVELOPE" {
		t.Fatalf("failureReason = %q, want INVALID_ENVELOPE", attrs["failureReason"])
	}
}

func TestSQSConsumerUnknownTypeGoesToDLQ(t *testing.T) {
	client := newSQSClient(t)
	queueURL := inputQueueURL(t.Name())
	dlq := dlqURL(t.Name())

	body := []byte(`{"messageId":"x","type":"SomethingElse","occurredAt":"2026-01-01T00:00:00Z","data":{}}`)
	sendRequested(t, client, queueURL, unique("msg-typ"), unique("wal-typ"), body)

	c := startConsumer(t, newTestUoW(t))
	stop := runConsumer(t, c)
	_, attrs := waitDLQ(t, client, dlq, 10*time.Second)
	stop()

	if attrs["failureReason"] != "UNKNOWN_TYPE" {
		t.Fatalf("failureReason = %q, want UNKNOWN_TYPE", attrs["failureReason"])
	}
}

func TestSQSConsumerOpeningGoesToDLQ(t *testing.T) {
	f := newTestUoW(t)
	seedWallet(t, f, "wal-sqsopen", "player-sqsopen", "100.00", 10000)
	client := newSQSClient(t)
	queueURL := inputQueueURL(t.Name())
	dlq := dlqURL(t.Name())

	in := processwager.Input{
		ProviderID:            unique("p-sqo"),
		ExternalTransactionID: unique("ext-sqo"),
		PlayerID:              unique("player-sqsopen"),
		WalletID:              unique("wal-sqsopen"),
		RoundID:               "r1",
		GameID:                "g1",
		Kind:                  wager.KindOpening,
		Amount:                mm(t, "100.00", "BRL"),
		IdempotencyKey:        unique("key-sqo"),
	}
	body := requestedJSON(t, unique("msg-sqo"), in)
	sendRequested(t, client, queueURL, unique("msg-sqo"), unique("wal-sqsopen"), body)

	c := startConsumer(t, f)
	stop := runConsumer(t, c)
	_, attrs := waitDLQ(t, client, dlq, 10*time.Second)
	stop()

	if attrs["failureReason"] != "OPENING_NOT_ALLOWED" {
		t.Fatalf("failureReason = %q, want OPENING_NOT_ALLOWED", attrs["failureReason"])
	}
}

// TestSQSConsumerHashMismatchGoesToDLQ: mesma mensagemId com conteúdo diferente
// (mesmo idempotencyKey — aqui escolhemos valores que divergem no hash) é
// rejeitada como permanente (MESSAGING §3).
func TestSQSConsumerHashMismatchGoesToDLQ(t *testing.T) {
	f := newTestUoW(t)
	seedWallet(t, f, "wal-sqsh1", "player-sqsh1", "100.00", 10000)
	client := newSQSClient(t)
	queueURL := inputQueueURL(t.Name())
	dlq := dlqURL(t.Name())

	// Primeira entrega: BET 30.00.
	in := processwager.Input{
		ProviderID:            unique("p-sq1h"),
		ExternalTransactionID: unique("ext-h1"),
		PlayerID:              unique("player-sqsh1"),
		WalletID:              unique("wal-sqsh1"),
		RoundID:               "r1",
		GameID:                "g1",
		Kind:                  wager.KindBet,
		Amount:                mm(t, "30.00", "BRL"),
		IdempotencyKey:        unique("key-h1"),
	}
	messageID := unique("msg-h1")
	body := requestedJSON(t, messageID, in)
	sendRequested(t, client, queueURL, messageID, unique("wal-sqsh1"), body)

	c := startConsumer(t, f)
	stop := runConsumer(t, c)
	waitInboxCompleted(t, messageID, 10*time.Second)
	stop()
	if b, v := walletBalance(t, "wal-sqsh1"); b != 7000 || v != 3 {
		t.Fatalf("primeira entrega: wallet = %d v%d, want 7000/3", b, v)
	}

	// Reentrega com a MESMA messageId no envelope mas conteúdo diferente
	// (valor 99.00) e MessageDeduplicationId distinto para passar pela dedup
	// do FIFO — o consumidor deve detectar o hash divergente e ir à DLQ.
	in2 := in
	in2.Amount = mm(t, "99.00", "BRL")
	body2 := requestedJSON(t, messageID, in2)
	sendRequested(t, client, queueURL, messageID+"-redo", unique("wal-sqsh1"), body2)

	c2 := startConsumer(t, f)
	stop2 := runConsumer(t, c2)
	_, attrs := waitDLQ(t, client, dlq, 10*time.Second)
	stop2()

	if attrs["failureReason"] != "MESSAGE_HASH_MISMATCH" {
		t.Fatalf("failureReason = %q, want MESSAGE_HASH_MISMATCH", attrs["failureReason"])
	}
	if b, _ := walletBalance(t, "wal-sqsh1"); b != 7000 {
		t.Fatalf("hash divergente não pode movimentar: wallet = %d, want 7000", b)
	}
	waitQueueEmpty(t, client, queueURL, 3*time.Second)
}

// TestSQSConsumerPermanentBusinessErrorToDLQ: carteira inexistente é falha
// permanente de negócio → DLQ com motivo WALLET_NOT_FOUND.
func TestSQSConsumerPermanentBusinessErrorToDLQ(t *testing.T) {
	client := newSQSClient(t)
	queueURL := inputQueueURL(t.Name())
	dlq := dlqURL(t.Name())

	in := processwager.Input{
		ProviderID:            unique("p-sqnw"),
		ExternalTransactionID: unique("ext-nw"),
		PlayerID:              unique("player-nw"),
		WalletID:              unique("wal-nw"),
		RoundID:               "r1",
		GameID:                "g1",
		Kind:                  wager.KindBet,
		Amount:                mm(t, "30.00", "BRL"),
		IdempotencyKey:        unique("key-nw"),
	}
	body := requestedJSON(t, unique("msg-nw"), in)
	sendRequested(t, client, queueURL, unique("msg-nw"), unique("wal-nw"), body)

	c := startConsumer(t, newTestUoW(t))
	stop := runConsumer(t, c)
	_, attrs := waitDLQ(t, client, dlq, 10*time.Second)
	stop()

	if attrs["failureReason"] != "WALLET_NOT_FOUND" {
		t.Fatalf("failureReason = %q, want WALLET_NOT_FOUND", attrs["failureReason"])
	}
	waitQueueEmpty(t, client, queueURL, 3*time.Second)
}

// TestSQSConsumerReusesAvailableHelpers assegura que a infra de helpers está
// coerente (personam do MESSAGING §2).
func TestSQSConsumerEnvelopeContract(t *testing.T) {
	in := processwager.Input{
		ProviderID: "provider-a", ExternalTransactionID: "transaction-123",
		IdempotencyKey: "provider-a:transaction-123",
		PlayerID:       "player", WalletID: "wallet",
		RoundID: "round-987", GameID: "fortune-chimp",
		Kind: wager.KindBet, Amount: mustValue(money.Parse("25.00", "BRL")),
	}
	body := string(requestedJSON(t, "msg-123", in))
	for _, want := range []string{`"messageId":"msg-123"`, `"type":"WagerTransactionRequested"`,
		`"kind":"BET"`, `"amount":"25.00"`, `"currency":"BRL"`, `"idempotencyKey":"provider-a:transaction-123"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("envelope deve conter %s, veio %s", want, body)
		}
	}
}
