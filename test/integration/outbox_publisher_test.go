//go:build integration

// M5.3: publisher da outbox contra PostgreSQL + LocalStack reais.
//  1. Dois publishers disputam os mesmos eventos — uma única publicação real
//     (dedup FIFO por MessageDeduplicationId=eventId) e confirmação na outbox.
//  2. Crash entre commit do claim e publicação: o lease expira e outra
//     instância assume o evento, republicando com o MESMO eventId.
package integration

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/events"
	"github.com/BrunoMoreno/backend-challenge-go/internal/infra/outboxpublisher"
	"github.com/BrunoMoreno/backend-challenge-go/internal/infra/postgres"
	"github.com/BrunoMoreno/backend-challenge-go/internal/infra/sqs"
)

const sqsEndpoint = "http://localhost:4566"

func eventsQueueURL(t string) string { return sqsEndpoint + "/000000000000/wager-events.fifo" }

func newEventSender(t *testing.T) *sqs.Sender {
	t.Helper()
	ctx := context.Background()
	s, err := sqs.NewSender(ctx, sqsEndpoint, "us-east-1", eventsQueueURL(t.Name()))
	if err != nil {
		t.Fatalf("sender: %v", err)
	}
	return s
}

// newSQSClient conecta direto ao LocalStack para receber/apagar da fila.
func newSQSClient(t *testing.T) *awssqs.Client {
	t.Helper()
	client, err := testSQSClientErr()
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	return client
}

// testSQSClient monta o cliente sem exigir *testing.T (usado no TestMain).
func testSQSClient() *awssqs.Client {
	client, _ := testSQSClientErr()
	return client
}

func testSQSClientErr() (*awssqs.Client, error) {
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithBaseEndpoint(sqsEndpoint),
		awsconfig.WithCredentialsProvider(aws.NewCredentialsCache(
			credentials.NewStaticCredentialsProvider("test", "test", ""))))
	if err != nil {
		return nil, err
	}
	return awssqs.NewFromConfig(cfg), nil
}

func startPublisher(t *testing.T, f *postgres.UnitOfWorkFactory, sender outboxpublisher.Sender) *outboxpublisher.Publisher {
	t.Helper()
	return outboxpublisher.New(f, sender, testLogger(t), outboxpublisher.Config{
		BatchSize: 1, PollInterval: 50 * time.Millisecond, Lease: 2 * time.Second,
		SendTimeout: 5 * time.Second, BackoffBase: time.Second, BackoffMax: time.Minute,
	})
}

func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// receiveEvent lê a próxima mensagem de wager-events.fifo, devolve o eventId
// (ou "" quando vazia) e a remove para o teste não vazar.
func receiveEvent(t *testing.T, client *awssqs.Client, queueURL string) string {
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
		return ""
	}
	m := out.Messages[0]
	var env events.Envelope
	if err := json.Unmarshal([]byte(aws.ToString(m.Body)), &env); err != nil {
		t.Fatalf("body: %v", err)
	}
	if _, err := client.DeleteMessage(context.Background(), &awssqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: m.ReceiptHandle,
	}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	return env.EventID
}

func insertOutboxEvent(t *testing.T, f *postgres.UnitOfWorkFactory) string {
	t.Helper()
	ctx := context.Background()
	eventID := unique("outbox-evt") + "-" + uniqueEventSuffix()
	env := mustValue(events.NewWalletBalanceChanged(eventID,
		unique("corr-outbox"), "",
		events.WalletBalanceChangedData{
			WalletID: unique("wal-outbox"), TransactionID: unique("tx-outbox"),
			Direction: "CREDIT", Money: mm(t, "1.00", "BRL"),
			BalanceBefore: mm(t, "0.00", "BRL"), BalanceAfter: mm(t, "1.00", "BRL"),
			WalletVersion: 2,
		}))
	uow := beginOK(t, f)
	if err := uow.OutboxRepository.Insert(ctx, env); err != nil {
		t.Fatalf("insert outbox: %v", err)
	}
	if err := uow.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return eventID
}

// uniqueEventSuffix gera um sufixo único por chamada (eventos da outbox usam
// event_id como PK; testes no mesmo runID precisam divergir).
func uniqueEventSuffix() string {
	return strconv.FormatInt(time.Now().UnixNano(), 36)
}

func outboxState(t *testing.T, f *postgres.UnitOfWorkFactory, eventID string) (published bool, attempts int) {
	t.Helper()
	uow := beginOK(t, f)
	defer uow.Rollback(context.Background())
	err := uow.Tx().QueryRow(context.Background(),
		`SELECT published_at IS NOT NULL, attempts FROM outbox_events WHERE event_id = $1`, eventID).
		Scan(&published, &attempts)
	if err != nil {
		t.Fatalf("outbox state: %v", err)
	}
	return published, attempts
}

func waitEvent(t *testing.T, client *awssqs.Client, queueURL, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got := receiveEvent(t, client, queueURL); got == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("evento %s não publicado em %v", want, timeout)
}

// noEvent espera um intervalo e falha se o evento alvo chegar na fila (o
// lease ativo impede a publicação; outras mensagens — publicadas por outras
// instâncias/shared infra — são consumidas e descartadas).
func noEvent(t *testing.T, client *awssqs.Client, queueURL, eventID string, wait time.Duration) {
	t.Helper()
	time.Sleep(wait)
	if got := receiveEvent(t, client, queueURL); got == eventID {
		t.Fatalf("evento %s publicado durante o lease", eventID)
	}
}

// drainEvents consome mensagens da fila de eventos por um instante, falhando
// se o eventId alvo reaparecer (o FIFO deduplica pelo MessageDeduplicationId,
// então uma segunda cópia do mesmo evento indicaria bug real).
func drainEvents(t *testing.T, client *awssqs.Client, queueURL, eventID string, wait time.Duration) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		got := receiveEvent(t, client, queueURL)
		if got == eventID {
			t.Fatalf("evento %s duplicado na fila", eventID)
		}
		time.Sleep(30 * time.Millisecond)
	}
}

func TestOutboxPublisherSingleDelivers(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)
	queueURL := eventsQueueURL(t.Name())
	eventID := insertOutboxEvent(t, f)
	client := newSQSClient(t)

	pub := startPublisher(t, f, newEventSender(t))
	done := make(chan error, 1)
	go func() { done <- pub.PublishBatch(ctx) }()
	if err := <-done; err != nil {
		t.Fatalf("publish batch: %v", err)
	}

	waitEvent(t, client, queueURL, eventID, 5*time.Second)
	published, _ := outboxState(t, f, eventID)
	if !published {
		t.Fatal("evento deveria estar publicado na outbox")
	}
}

func TestOutboxTwoPublishersDispute(t *testing.T) {
	ctx := context.Background()
	queueURL := eventsQueueURL(t.Name())
	f1, f2 := newTestUoW(t), newTestUoW(t)
	eventID := insertOutboxEvent(t, f1)
	client := newSQSClient(t)

	p1 := startPublisher(t, f1, newEventSender(t))
	p2 := startPublisher(t, f2, newEventSender(t))
	runErr := make(chan error, 2)
	go func() { runErr <- p1.PublishBatch(ctx) }()
	go func() { runErr <- p2.PublishBatch(ctx) }()
	if err := <-runErr; err != nil {
		t.Fatalf("publisher 1: %v", err)
	}
	if err := <-runErr; err != nil {
		t.Fatalf("publisher 2: %v", err)
	}

	// Entre os dois publishers há UMA publicação real na fila: o FIFO
	// deduplica por MessageDeduplicationId=eventId (e o repassador também).
	waitEvent(t, client, queueURL, eventID, 5*time.Second)
	drainEvents(t, client, queueURL, eventID, 300*time.Millisecond)
	published, attempts := outboxState(t, f1, eventID)
	if !published || attempts != 0 {
		t.Fatalf("outbox = published=%v attempts=%d, want true/0", published, attempts)
	}
}

// TestOutboxCrashAfterClaim: o "publisher A" reclama e commit o lease, mas
// "morre" antes de publicar. Enquanto o lease vale, ninguém publica; depois da
// expiração, outra instância assume e publica o MESMO eventId.
func TestOutboxCrashAfterClaimPublisherRecovers(t *testing.T) {
	ctx := context.Background()
	f := newTestUoW(t)
	queueURL := eventsQueueURL(t.Name())
	eventID := insertOutboxEvent(t, f)
	client := newSQSClient(t)

	// A: claim com lease curto (2s) + commit — equivale ao crash entre o commit
	// do claim e a publicação.
	uow := beginOK(t, f)
	claimed, err := uow.OutboxRepository.ClaimPending(ctx, 1, 2*time.Second)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v (%d)", err, len(claimed))
	}
	if err := uow.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// Enquanto o lease está ativo, nenhuma instância publica.
	noEvent(t, client, queueURL, eventID, 1500*time.Millisecond)
	if published, _ := outboxState(t, f, eventID); published {
		t.Fatal("evento não deveria estar publicado durante o lease")
	}

	// Publisher B assume após a expiração do lease.
	pub := startPublisher(t, f, newEventSender(t))
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if err := pub.PublishBatch(ctx); err != nil {
			t.Fatalf("publish: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	waitEvent(t, client, queueURL, eventID, 5*time.Second)
	published, _ := outboxState(t, f, eventID)
	if !published {
		t.Fatal("evento deveria estar publicado após a recuperação")
	}
}

// TestOutboxShutdownStopsLoop verifica o encerramento observável do loop.
func TestOutboxShutdownStopsLoop(t *testing.T) {
	f := newTestUoW(t)
	pub := startPublisher(t, f, &senderFailing{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { pub.Run(ctx); close(done) }()
	time.Sleep(150 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run não terminou após o cancelamento")
	}
}

type senderFailing struct{}

func (s *senderFailing) Send(context.Context, events.Envelope) error {
	return context.DeadlineExceeded
}
