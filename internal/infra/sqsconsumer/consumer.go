// Package sqsconsumer implementa o consumidor da fila de entrada de operações
// (wager-transactions.fifo): long polling, concorrência limitada com heartbeat,
// processamento na MESMA transação SQL da inbox/outbox (MESSAGING §3) e retry
// com backoff por ApproximateReceiveCount. Falhas permanentes vão para a DLQ
// com atributo do motivo; transientas são devolvidas para reentrega.
package sqsconsumer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/BrunoMoreno/backend-challenge-go/internal/app/processwager"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wager"
	"github.com/BrunoMoreno/backend-challenge-go/internal/infra/postgres"
	"github.com/BrunoMoreno/backend-challenge-go/internal/platform/logging"
	"github.com/BrunoMoreno/backend-challenge-go/internal/platform/metrics"
)

// typeWagerTransactionRequested é o único tipo de mensagem aceito na fila de
// entrada; a fila não distingue origem (HTTP vs SQS) no caso de uso.
const typeWagerTransactionRequested = "WagerTransactionRequested"

// ConsumerName identifica este consumidor na inbox (H1.9).
const ConsumerName = "process-wager"

// messageAPI é a fatia do cliente SQS usada pelo consumidor.
type messageAPI interface {
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput,
		optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput,
		optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, in *sqs.ChangeMessageVisibilityInput,
		optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
	SendMessage(ctx context.Context, in *sqs.SendMessageInput,
		optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

// Config parametriza o consumidor.
type Config struct {
	// QueueURL é a fila FIFO de entrada (wager-transactions.fifo).
	QueueURL string
	// DLQURL é a fila FIFO de mensagens mortas (falhas permanentes).
	DLQURL string
	// VisibilityTimeout é o prazo de visibilidade de cada mensagem; o
	// heartbeat estende enquanto processa.
	VisibilityTimeout time.Duration
	// WaitTime é o long polling da ReceiveMessage.
	WaitTime time.Duration
	// Concurrency é o número de workers que recebem mensagens em paralelo.
	Concurrency int
	// MaxReceiveCount é o teto de entregas (vermelha): ao alcançar, o
	// consumidor deixa o redrive automático da fila mover para a DLQ.
	MaxReceiveCount int
	// BackoffBase é o atraso inicial do backoff de reentrega.
	BackoffBase time.Duration
	// BackoffMax é o teto do backoff de reentrega.
	BackoffMax time.Duration
	// ProcessTimeout é o prazo para processar uma mensagem (cancel + release).
	ProcessTimeout time.Duration
	// Metrics instrumenta os desfechos das mensagens; nil desativa.
	Metrics *metrics.Metrics
}

// Consumer recebe e processa as mensagens da fila de entrada.
type Consumer struct {
	api     messageAPI
	service *processwager.Service
	factory *postgres.UnitOfWorkFactory
	logger  *slog.Logger
	cfg     Config
	jitter  func(max time.Duration) time.Duration
}

// New cria o consumidor sobre um cliente SQS pronto.
func New(api messageAPI, service *processwager.Service, factory *postgres.UnitOfWorkFactory,
	logger *slog.Logger, cfg Config) *Consumer {
	if cfg.VisibilityTimeout <= 0 {
		cfg.VisibilityTimeout = 30 * time.Second
	}
	if cfg.WaitTime <= 0 {
		cfg.WaitTime = 20 * time.Second
	}
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 1
	}
	if cfg.MaxReceiveCount < 1 {
		cfg.MaxReceiveCount = 5
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = time.Second
	}
	if cfg.BackoffMax <= 0 {
		cfg.BackoffMax = 15 * time.Minute
	}
	if cfg.ProcessTimeout <= 0 {
		cfg.ProcessTimeout = 5 * time.Minute
	}
	return &Consumer{
		api:     api,
		service: service,
		factory: factory,
		logger:  logger,
		cfg:     cfg,
		jitter:  func(max time.Duration) time.Duration { return time.Duration(rand.Int63n(int64(max) + 1)) },
	}
}

// Run executa os workers até o cancelamento do contexto (MESSAGING §5).
func (c *Consumer) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for i := 0; i < c.cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.worker(ctx)
		}()
	}
	wg.Wait()
	return nil
}

// worker recebe mensagens em long polling e entrega a cada uma ao processamento.
func (c *Consumer) worker(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		out, err := c.api.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(c.cfg.QueueURL),
			WaitTimeSeconds:     int32(c.cfg.WaitTime / time.Second),
			VisibilityTimeout:   int32(c.cfg.VisibilityTimeout / time.Second),
			MaxNumberOfMessages: 10,
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{
				types.MessageSystemAttributeNameApproximateReceiveCount,
			},
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.logger.Warn("sqs: falha ao receber", "error", err)
			c.sleep(ctx, c.jitter(c.cfg.BackoffBase))
			continue
		}
		for i := range out.Messages {
			c.handle(ctx, &out.Messages[i])
		}
	}
}

// envelope é o contrato de entrada do canal SQS (MESSAGING §2).
type envelope struct {
	MessageID  string          `json:"messageId"`
	Type       string          `json:"type"`
	OccurredAt time.Time       `json:"occurredAt"`
	Data       json.RawMessage `json:"data"`
}

// requestedData é o corpo de negócio do envelope SQS.
type requestedData struct {
	ProviderID                     string      `json:"providerId"`
	ExternalTransactionID          string      `json:"externalTransactionId"`
	PlayerID                       string      `json:"playerId"`
	WalletID                       string      `json:"walletId"`
	RoundID                        string      `json:"roundId"`
	GameID                         string      `json:"gameId"`
	Kind                           string      `json:"kind"`
	Money                          money.Money `json:"money"`
	ReferenceExternalTransactionID string      `json:"referenceExternalTransactionId"`
	IdempotencyKey                 string      `json:"idempotencyKey"`
}

// failure é uma falha permanente com motivo para a DLQ.
type failure struct {
	reason string
	detail string
}

// handle processa uma única mensagem com toda a orquestração de fila.
func (c *Consumer) handle(ctx context.Context, msg *types.Message) {
	body := aws.ToString(msg.Body)
	receipt := aws.ToString(msg.ReceiptHandle)
	if receipt == "" {
		return
	}

	// Dança de heartbeat: estende a visibilidade enquanto a mensagem está
	// sendo processada; no shutdown (ctx pai cancelado) libera para reentrega.
	hbCtx, hbCancel := context.WithCancel(ctx)
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		c.heartbeat(hbCtx, ctx, receipt)
	}()
	defer func() {
		hbCancel()
		<-hbDone
	}()

	// 1. Envelope e type.
	env, err := parseEnvelope(body)
	if err != nil {
		c.dead(ctx, msg, failure{reason: "INVALID_ENVELOPE", detail: err.Error()})
		return
	}
	if env.Type != typeWagerTransactionRequested {
		c.dead(ctx, msg, failure{reason: "UNKNOWN_TYPE", detail: env.Type})
		return
	}
	if strings.TrimSpace(env.MessageID) == "" {
		c.dead(ctx, msg, failure{reason: "INVALID_ENVELOPE", detail: "messageId ausente"})
		return
	}

	// Correlação de logs pelo messageId da mensagem SQS. Variável própria: o
	// contexto original segue sendo lido pelo goroutine de heartbeat.
	msgCtx := logging.WithCorrelation(ctx, env.MessageID)
	processCtx, processCancel := context.WithTimeout(msgCtx, c.cfg.ProcessTimeout)
	defer processCancel()

	// 2. Corpo de negócio (mesma validação estrutural do HTTP, sem autorização
	// por fornecedor — a autenticação é do produtor na fila).
	in, fail := parseRequested(env.Data)
	if fail.reason != "" {
		c.dead(ctx, msg, fail)
		return
	}
	hash, err := wager.HashPayload(wager.PayloadFields{
		ProviderID:                     in.ProviderID,
		ExternalTransactionID:          in.ExternalTransactionID,
		PlayerID:                       in.PlayerID,
		WalletID:                       in.WalletID,
		RoundID:                        in.RoundID,
		GameID:                         in.GameID,
		Kind:                           in.Kind,
		Amount:                         in.Amount,
		ReferenceExternalTransactionID: in.ReferenceExternalTransactionID,
	})
	if err != nil {
		c.dead(ctx, msg, failure{reason: "INVALID_HASH", detail: err.Error()})
		return
	}

	// 3. Transação SQL única: inbox insert (ON CONFLICT) → caso de uso → inbox
	// complete → outbox. Reentregas verificam hash e estado da inbox.
	uow, err := c.factory.Begin(processCtx)
	if err != nil {
		c.retry(msgCtx, msg, err)
		return
	}
	defer func() { _ = uow.Rollback(processCtx) }()

	inserted, err := uow.InboxRepository.TryInsert(processCtx, ConsumerName, env.MessageID, hash)
	if err != nil {
		c.retry(msgCtx, msg, err)
		return
	}
	if !inserted {
		// Reentrega: o hash deve bater (mesmo conteúdo); concluída → apaga.
		if !c.replay(msgCtx, msg, env.MessageID, hash, uow) {
			return
		}
	}

	res, err := c.service.ProcessOn(processCtx, uow, in)
	if err != nil {
		if isRetryable(err) {
			c.retry(msgCtx, msg, err)
			return
		}
		c.dead(msgCtx, msg, failure{reason: businessReason(err), detail: err.Error()})
		return
	}
	_ = res

	if err := uow.InboxRepository.MarkCompleted(processCtx, ConsumerName, env.MessageID); err != nil {
		c.retry(msgCtx, msg, err)
		return
	}
	if err := uow.Commit(processCtx); err != nil {
		c.retry(msgCtx, msg, err)
		return
	}

	// Sucesso persistido: apaga a mensagem da fila.
	c.cfg.Metrics.SQSMessages("processed")
	crashAfterCommitBeforeDelete(ctx, c.logger)
	if err := c.delete(msgCtx, msg); err != nil {
		// A inbox já está completa; na próxima reentrega será tratada como
		// replay e apagada — reentrega é idempotente.
		c.logger.WarnContext(ctx, "sqs: sucesso persistido, falha ao apagar", "messageId", env.MessageID, "error", err)
	}
}

// replay resolve reentregas de mensagens já vistas. Com hash igual e inbox
// concluída apaga e devolve false (não processa de novo); em voo por outra
// instância devolve false sem agir; hash divergente vai para a DLQ.
func (c *Consumer) replay(ctx context.Context, msg *types.Message, messageID, hash string,
	uow *postgres.UnitOfWork) bool {
	stored, err := uow.InboxRepository.GetHash(ctx, ConsumerName, messageID)
	if err != nil {
		c.retry(ctx, msg, err)
		return false
	}
	if stored != hash {
		c.dead(ctx, msg, failure{reason: "MESSAGE_HASH_MISMATCH",
			detail: "reentrega com conteúdo diferente da mensagem aceita"})
		return false
	}
	completed, err := uow.InboxRepository.IsCompleted(ctx, ConsumerName, messageID)
	if err != nil {
		c.retry(ctx, msg, err)
		return false
	}
	if completed {
		c.cfg.Metrics.SQSMessages("replay")
		if err := c.delete(ctx, msg); err != nil {
			c.logger.WarnContext(ctx, "sqs: replay concluída, falha ao apagar", "messageId", messageID, "error", err)
		}
		return false
	}
	// Em voo por outra instância (claim não concluída): deixa a fila redelivar.
	return false
}

// parseEnvelope desserializa o envelope de entrada.
func parseEnvelope(body string) (envelope, error) {
	var env envelope
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		return envelope{}, err
	}
	return env, nil
}

// parseRequested valida o corpo de negócio devolvendo o Input do caso de uso
// compartilhado (mesmas regras do HTTP, exceto OPENING e autorização).
func parseRequested(raw json.RawMessage) (processwager.Input, failure) {
	var d requestedData
	if err := json.Unmarshal(raw, &d); err != nil {
		return processwager.Input{}, failure{reason: "INVALID_DATA", detail: err.Error()}
	}
	if strings.TrimSpace(d.IdempotencyKey) == "" {
		return processwager.Input{}, failure{reason: "MISSING_IDEMPOTENCY_KEY", detail: "idempotencyKey ausente"}
	}
	if strings.TrimSpace(d.ProviderID) == "" || strings.TrimSpace(d.ExternalTransactionID) == "" ||
		strings.TrimSpace(d.PlayerID) == "" || strings.TrimSpace(d.WalletID) == "" {
		return processwager.Input{}, failure{reason: "INVALID_FIELD", detail: "campo obrigatório ausente"}
	}
	kind := wager.Kind(d.Kind)
	if kind == wager.KindOpening {
		return processwager.Input{}, failure{reason: "OPENING_NOT_ALLOWED", detail: "OPENING só pelo canal HTTP"}
	}
	switch kind {
	case wager.KindBet, wager.KindWin, wager.KindLoss, wager.KindRefund, wager.KindRollback:
	default:
		return processwager.Input{}, failure{reason: "UNKNOWN_KIND", detail: d.Kind}
	}
	if d.Money.Currency() == "" {
		return processwager.Input{}, failure{reason: "INVALID_MONEY", detail: "money ausente ou moeda inválida"}
	}
	return processwager.Input{
		ProviderID:                     d.ProviderID,
		ExternalTransactionID:          d.ExternalTransactionID,
		PlayerID:                       d.PlayerID,
		WalletID:                       d.WalletID,
		RoundID:                        d.RoundID,
		GameID:                         d.GameID,
		Kind:                           kind,
		Amount:                         d.Money,
		ReferenceExternalTransactionID: d.ReferenceExternalTransactionID,
		IdempotencyKey:                 d.IdempotencyKey,
	}, failure{}
}

// businessReason mapeia erros de negócio permanentes para o motivo da DLQ.
func businessReason(err error) string {
	switch {
	case errors.Is(err, processwager.ErrMissingIdempotencyKey):
		return "MISSING_IDEMPOTENCY_KEY"
	case errors.Is(err, processwager.ErrUnsupportedKind):
		return "UNKNOWN_KIND"
	case errors.Is(err, processwager.ErrInvalidAmountForKind):
		return "INVALID_AMOUNT_FOR_KIND"
	case errors.Is(err, processwager.ErrWalletNotFound):
		return "WALLET_NOT_FOUND"
	case errors.Is(err, processwager.ErrWalletCurrencyMismatch):
		return "WALLET_CURRENCY_MISMATCH"
	case errors.Is(err, processwager.ErrIdempotencyConflict):
		return "IDEMPOTENCY_CONFLICT"
	case errors.Is(err, processwager.ErrExternalConflict):
		return "EXTERNAL_CONFLICT"
	default:
		return "PERMANENT_FAILURE"
	}
}

// isRetryable classifica as falhas transitórias: claim concorrente desfeito e
// condições do Postgres/SQS que resolvem com reentrega (MESSAGING §3).
func isRetryable(err error) bool {
	if errors.Is(err, processwager.ErrStaleClaim) {
		return true
	}
	if errors.Is(err, postgres.ErrSerialization) || errors.Is(err, postgres.ErrDeadlock) {
		return true
	}
	if errors.Is(err, postgres.ErrUnexpected) || errors.Is(err, postgres.ErrNotFound) {
		// Erro inesperado (rede/connection) ou linha sumida em corrida: tenta
		// de novo dentro do backoff; o redrive automático decide o destino.
		return true
	}
	return false
}

// retry a falha transitória: backoff exponencial com jitter por
// ApproximateReceiveCount. Ao alcançar MaxReceiveCount, não mexe na
// visibilidade — o redrive automático da fila move para a DLQ.
func (c *Consumer) retry(ctx context.Context, msg *types.Message, err error) {
	receipt := aws.ToString(msg.ReceiptHandle)
	count := approximateReceiveCount(msg)
	c.cfg.Metrics.SQSMessages("retry")
	c.logger.WarnContext(ctx, "sqs: falha transitória", "error", err, "receiveCount", count)

	if count >= c.cfg.MaxReceiveCount {
		return
	}
	delay := c.backoff(count)
	c.logger.InfoContext(ctx, "sqs: reentrega agendada", "receiveCount", count, "delay", delay.String())
	_, changeErr := c.api.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(c.cfg.QueueURL),
		ReceiptHandle:     aws.String(receipt),
		VisibilityTimeout: int32(delay / time.Second),
	})
	if changeErr != nil && ctx.Err() == nil {
		c.logger.WarnContext(ctx, "sqs: falha ao estender visibilidade", "error", changeErr)
	}
}

// dead público falha permanente na DLQ com atributo do motivo e apaga a
// original (MESSAGING §3).
func (c *Consumer) dead(ctx context.Context, msg *types.Message, f failure) {
	body := aws.ToString(msg.Body)
	c.cfg.Metrics.SQSMessages("dead")
	c.logger.WarnContext(ctx, "sqs: falha permanente → DLQ", "reason", f.reason, "detail", f.detail)

	dedup := hashBody(body)
	if env, err := parseEnvelope(body); err == nil && env.MessageID != "" {
		dedup = env.MessageID
	}
	_, err := c.api.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(c.cfg.DLQURL),
		MessageBody:            aws.String(body),
		MessageGroupId:         aws.String(ConsumerName),
		MessageDeduplicationId: aws.String(dedup),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"failureReason": {
				DataType:    aws.String("String"),
				StringValue: aws.String(f.reason),
			},
			"failureDetail": {
				DataType:    aws.String("String"),
				StringValue: aws.String(truncate(f.detail, 512)),
			},
		},
	})
	if err != nil {
		// Não apaga a original: sem DLQ atingível, a mensagem volta na
		// reentrega e tenta de novo.
		c.logger.ErrorContext(ctx, "sqs: falha ao publicar na DLQ", "reason", f.reason, "error", err)
		return
	}
	if err := c.delete(ctx, msg); err != nil {
		c.logger.WarnContext(ctx, "sqs: DLQ ok, falha ao apagar original", "error", err)
	}
}

// delete remove a mensagem da fila de entrada.
func (c *Consumer) delete(ctx context.Context, msg *types.Message) error {
	_, err := c.api.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.cfg.QueueURL),
		ReceiptHandle: msg.ReceiptHandle,
	})
	return err
}

// heartbeat estende a visibilidade periodicamente enquanto processa. Quando o
// contexto pai (worker) é cancelado — shutdown SIGTERM — libera a visibilidade
// (0) para a reentrega segura do que não terminou (MESSAGING §5).
func (c *Consumer) heartbeat(ctx, parent context.Context, receipt string) {
	interval := c.cfg.VisibilityTimeout / 2
	if interval < time.Second {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-parent.Done():
			// Shutdown: libera mensagens em voo para reentrega segura.
			_, _ = c.api.ChangeMessageVisibility(context.Background(), &sqs.ChangeMessageVisibilityInput{
				QueueUrl:          aws.String(c.cfg.QueueURL),
				ReceiptHandle:     aws.String(receipt),
				VisibilityTimeout: 0,
			})
			return
		case <-ctx.Done():
			// Processamento concluído normalmente; o retry/delete já agiu.
			return
		case <-ticker.C:
			if _, err := c.api.ChangeMessageVisibility(context.Background(), &sqs.ChangeMessageVisibilityInput{
				QueueUrl:          aws.String(c.cfg.QueueURL),
				ReceiptHandle:     aws.String(receipt),
				VisibilityTimeout: int32(c.cfg.VisibilityTimeout / time.Second),
			}); err != nil {
				c.logger.Warn("sqs: heartbeat falhou", "error", err)
			}
		}
	}
}

// backoff calcula o atraso exponencial (base * 2^(count-1)) com jitter de 30%,
// limitado ao teto.
func (c *Consumer) backoff(count int) time.Duration {
	delay := c.cfg.BackoffBase
	for i := 1; i < count; i++ {
		if delay >= c.cfg.BackoffMax {
			break
		}
		delay *= 2
		if delay > c.cfg.BackoffMax {
			delay = c.cfg.BackoffMax
			break
		}
	}
	maxJitter := delay * 30 / 100
	if maxJitter < time.Second {
		maxJitter = time.Second
	}
	delay -= c.jitter(maxJitter)
	if delay < 0 {
		delay = 0
	}
	return delay
}

func (c *Consumer) sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

func approximateReceiveCount(msg *types.Message) int {
	for k, v := range msg.Attributes {
		if k == "ApproximateReceiveCount" {
			n := 0
			fmt.Sscanf(v, "%d", &n)
			return n
		}
	}
	return 0
}

func hashBody(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])[:32]
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
