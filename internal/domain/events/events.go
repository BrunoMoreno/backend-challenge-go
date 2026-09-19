// Package events define o envelope de eventos e os payloads tipados da outbox.
package events

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wager"
)

// Type identifica o tipo do evento.
type Type string

const (
	TypeWagerTransactionProcessed        Type = "WagerTransactionProcessed"
	TypeWagerTransactionRejected         Type = "WagerTransactionRejected"
	TypeWalletBalanceChanged             Type = "WalletBalanceChanged"
	TypeWagerTransactionPendingReference Type = "WagerTransactionPendingReference"
)

// VersionCurrent é a versão do schema de todos os tipos atuais.
const VersionCurrent = 1

// Erros do pacote de eventos.
var ErrEmptyEventID = errors.New("events: eventId ausente")

// Envelope é o contrato de publicação da outbox. Data é um snapshot imutável.
type Envelope struct {
	EventID       string          `json:"eventId"`
	EventType     Type            `json:"eventType"`
	AggregateID   string          `json:"aggregateId"`
	CorrelationID string          `json:"correlationId"`
	CausationID   string          `json:"causationId,omitempty"`
	OccurredAt    time.Time       `json:"occurredAt"`
	Version       int             `json:"version"`
	Data          json.RawMessage `json:"data"`
}

func build(eventID string, typ Type, aggregateID, correlationID, causationID string, data any) (Envelope, error) {
	if eventID == "" {
		return Envelope{}, ErrEmptyEventID
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return Envelope{}, err
	}
	return Envelope{
		EventID:       eventID,
		EventType:     typ,
		AggregateID:   aggregateID,
		CorrelationID: correlationID,
		CausationID:   causationID,
		OccurredAt:    time.Now().UTC(),
		Version:       VersionCurrent,
		Data:          raw,
	}, nil
}

// WagerTransactionProcessedData é o payload da conclusão bem-sucedida.
// Campos externos ficam ausentes (omitempty) em eventos de origem interna (OPENING).
type WagerTransactionProcessedData struct {
	TransactionID         string       `json:"transactionId"`
	Origin                wager.Origin `json:"origin"`
	ProviderID            string       `json:"providerId,omitempty"`
	ExternalTransactionID string       `json:"externalTransactionId,omitempty"`
	Kind                  wager.Kind   `json:"kind"`
	Money                 money.Money  `json:"money"`
	Balance               money.Money  `json:"balance"`
}

// WagerTransactionRejectedData é o payload de uma rejeição definitiva.
type WagerTransactionRejectedData struct {
	TransactionID         string            `json:"transactionId"`
	ProviderID            string            `json:"providerId,omitempty"`
	ExternalTransactionID string            `json:"externalTransactionId,omitempty"`
	Kind                  wager.Kind        `json:"kind"`
	FailureCode           wager.FailureCode `json:"failureCode"`
}

// WalletBalanceChangedData é o payload de mudança efetiva de saldo.
type WalletBalanceChangedData struct {
	WalletID      string      `json:"walletId"`
	TransactionID string      `json:"transactionId"`
	Direction     string      `json:"direction"`
	Money         money.Money `json:"money"`
	BalanceBefore money.Money `json:"balanceBefore"`
	BalanceAfter  money.Money `json:"balanceAfter"`
	WalletVersion int64       `json:"walletVersion"`
}

// WagerTransactionPendingReferenceData é o payload de espera por referência.
type WagerTransactionPendingReferenceData struct {
	TransactionID                  string `json:"transactionId"`
	ProviderID                     string `json:"providerId,omitempty"`
	ExternalTransactionID          string `json:"externalTransactionId,omitempty"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId"`
}

func NewWagerTransactionProcessed(eventID string, correlationID, causationID string,
	d WagerTransactionProcessedData) (Envelope, error) {
	return build(eventID, TypeWagerTransactionProcessed, d.TransactionID, correlationID, causationID, d)
}

func NewWagerTransactionRejected(eventID string, correlationID, causationID string,
	d WagerTransactionRejectedData) (Envelope, error) {
	return build(eventID, TypeWagerTransactionRejected, d.TransactionID, correlationID, causationID, d)
}

func NewWalletBalanceChanged(eventID string, correlationID, causationID string,
	d WalletBalanceChangedData) (Envelope, error) {
	return build(eventID, TypeWalletBalanceChanged, d.WalletID, correlationID, causationID, d)
}

func NewWagerTransactionPendingReference(eventID string, correlationID, causationID string,
	d WagerTransactionPendingReferenceData) (Envelope, error) {
	return build(eventID, TypeWagerTransactionPendingReference, d.TransactionID, correlationID, causationID, d)
}
