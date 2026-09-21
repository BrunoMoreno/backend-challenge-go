package sqs

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/events"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
)

var errSendFake = errors.New("sqs: broker indisponível (fake)")

// recordingAPI captura o SendMessageInput e devolve um output fixo.
type recordingAPI struct {
	in  *sqs.SendMessageInput
	err error
}

func (r *recordingAPI) SendMessage(_ context.Context, in *sqs.SendMessageInput,
	_ ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	r.in = in
	return &sqs.SendMessageOutput{MessageId: strPtr("msg-1")}, r.err
}

func strPtr(s string) *string { return &s }

func TestSenderBuildsFIFOMessage(t *testing.T) {
	env, err := events.NewWalletBalanceChanged("evt-1", "corr-1", "",
		events.WalletBalanceChangedData{
			WalletID: "w-1", TransactionID: "tx-1", Direction: "CREDIT",
			Money: money.MoneyOf(1000, "BRL"), BalanceBefore: money.MoneyOf(0, "BRL"),
			BalanceAfter: money.MoneyOf(1000, "BRL"), WalletVersion: 2,
		})
	if err != nil {
		t.Fatalf("evento: %v", err)
	}
	api := &recordingAPI{}
	s := &Sender{api: api, queueURL: "http://localhost:4566/000000000000/wager-events.fifo"}

	if err := s.Send(context.Background(), env); err != nil {
		t.Fatalf("send: %v", err)
	}

	in := api.in
	if in == nil {
		t.Fatal("nenhuma mensagem enviada")
	}
	if *in.MessageGroupId != "w-1" {
		t.Fatalf("MessageGroupId = %q, want w-1 (aggregateId)", *in.MessageGroupId)
	}
	if *in.MessageDeduplicationId != "evt-1" {
		t.Fatalf("MessageDeduplicationId = %q, want evt-1 (eventId)", *in.MessageDeduplicationId)
	}
	if *in.MessageAttributes["eventType"].StringValue != "WalletBalanceChanged" {
		t.Fatalf("attr eventType = %q", *in.MessageAttributes["eventType"].StringValue)
	}
	// Corpo é o envelope completo (JSON) com eventId e data tipada.
	var body events.Envelope
	if err := json.Unmarshal([]byte(*in.MessageBody), &body); err != nil {
		t.Fatalf("body inválido: %v", err)
	}
	if body.EventID != "evt-1" || body.AggregateID != "w-1" || body.EventType != events.TypeWalletBalanceChanged {
		t.Fatalf("body = %+v", body)
	}
}

func TestSenderFailsWhenBrokerFails(t *testing.T) {
	s := &Sender{api: &recordingAPI{err: errSendFake}, queueURL: "q"}
	if err := s.Send(context.Background(), events.Envelope{
		EventID: "e", EventType: "X", AggregateID: "a", Data: json.RawMessage(`{}`),
	}); err == nil {
		t.Fatal("esperava erro do broker")
	}
}
