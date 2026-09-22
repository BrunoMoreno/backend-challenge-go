// Package sqs adapta o AWS SDK (aws-sdk-go-v2) às filas FIFO da aplicação,
// operando contra o LocalStack em desenvolvimento.
package sqs

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/events"
)

// messageAPI é a fatia do cliente SQS usada pelo sender (permite injetar um
// fake em testes sem subir o LocalStack).
type messageAPI interface {
	SendMessage(ctx context.Context, in *sqs.SendMessageInput,
		optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

// Sender publica eventos da outbox na fila wager-events.fifo.
type Sender struct {
	api      messageAPI
	queueURL string
}

// NewClient monta o cliente SQS apontando para o broker configurado (código
// compartilhado por sender e consumidor). A cadeia de credenciais default do
// SDK (env vars, profile, IRSA/IAM) é respeitada — antes as credenciais
// estáticas "test/test" eram forçadas e anulavam AWS_ACCESS_KEY_ID/IRSA,
// sem caminho de produção para AWS real (docs/solve/IMPROVEMENTS.md A4).
//
// Para emulador local (endpoint setado) e sem credenciais explícitas no
// ambiente, usa a estática "test/test" apenas como fallback de assinatura —
// o LocalStack aceita qualquer credential; o SDK exige alguma.
func NewClient(ctx context.Context, endpoint, region string) (*sqs.Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(region)}
	if endpoint != "" {
		opts = append(opts, awsconfig.WithBaseEndpoint(endpoint))
		if os.Getenv("AWS_ACCESS_KEY_ID") == "" && os.Getenv("AWS_PROFILE") == "" {
			opts = append(opts, awsconfig.WithCredentialsProvider(
				aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("test", "test", ""))))
		}
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("sqs: carregar config: %w", err)
	}
	return sqs.NewFromConfig(cfg), nil
}

// NewSender cria o sender apontando para o broker SQS configurado.
func NewSender(ctx context.Context, endpoint, region, queueURL string) (*Sender, error) {
	client, err := NewClient(ctx, endpoint, region)
	if err != nil {
		return nil, err
	}
	return NewSenderFromClient(client, queueURL), nil
}

// NewSenderFromClient monta o sender sobre um cliente SQS pronto (testes).
func NewSenderFromClient(client *sqs.Client, queueURL string) *Sender {
	return &Sender{api: client, queueURL: queueURL}
}

// Send publica o envelope na fila FIFO. MessageGroupId = aggregateId preserva
// a ordem por agregado; MessageDeduplicationId = eventId torna republicações
// idempotentes no próprio broker (e o consumidor ainda deduplica por eventId).
func (s *Sender) Send(ctx context.Context, env events.Envelope) error {
	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("sqs: marshal evento: %w", err)
	}
	_, err = s.api.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:               aws.String(s.queueURL),
		MessageBody:            aws.String(string(body)),
		MessageGroupId:         aws.String(env.AggregateID),
		MessageDeduplicationId: aws.String(env.EventID),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"eventType": {
				DataType:    aws.String("String"),
				StringValue: aws.String(string(env.EventType)),
			},
			"eventId": {
				DataType:    aws.String("String"),
				StringValue: aws.String(env.EventID),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("sqs: send %s: %w", env.EventID, err)
	}
	return nil
}
