package events

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wager"
)

func moneyBRL(minor int64) money.Money { return money.MoneyOf(minor, "BRL") }

func TestNewWagerTransactionProcessed(t *testing.T) {
	env, err := NewWagerTransactionProcessed("evt-1", "corr-1", "",
		WagerTransactionProcessedData{
			TransactionID: "tx-1", Origin: wager.OriginExternal, ProviderID: "provider-a",
			ExternalTransactionID: "ext-1", Kind: wager.KindBet,
			Money: moneyBRL(2500), Balance: moneyBRL(97500),
		})
	if err != nil {
		t.Fatalf("NewWagerTransactionProcessed error = %v", err)
	}
	if env.EventType != TypeWagerTransactionProcessed || env.AggregateID != "tx-1" {
		t.Errorf("envelope = %s/%s", env.EventType, env.AggregateID)
	}
	if env.Version != VersionCurrent {
		t.Errorf("Version = %d, want %d", env.Version, VersionCurrent)
	}
	if env.CausationID != "" {
		t.Errorf("CausationID sem causa = %q, want vazia", env.CausationID)
	}
	var data WagerTransactionProcessedData
	if err := json.Unmarshal(env.Data, &data); err != nil {
		t.Fatalf("payload inválido: %v", err)
	}
	if data.ProviderID != "provider-a" || data.Balance.Minor() != 97500 {
		t.Errorf("payload = %+v", data)
	}
}

func TestNewWalletBalanceChanged(t *testing.T) {
	env, err := NewWalletBalanceChanged("evt-2", "corr-1", "evt-1",
		WalletBalanceChangedData{
			WalletID: "w1", TransactionID: "tx-1", Direction: "DEBIT",
			Money: moneyBRL(2500), BalanceBefore: moneyBRL(100000),
			BalanceAfter: moneyBRL(97500), WalletVersion: 2,
		})
	if err != nil {
		t.Fatalf("NewWalletBalanceChanged error = %v", err)
	}
	if env.AggregateID != "w1" {
		t.Errorf("AggregateID = %s, want w1", env.AggregateID)
	}
	if env.CausationID != "evt-1" {
		t.Errorf("CausationID = %q, want evt-1", env.CausationID)
	}
	var data WalletBalanceChangedData
	if err := json.Unmarshal(env.Data, &data); err != nil {
		t.Fatalf("payload inválido: %v", err)
	}
	if data.WalletVersion != 2 || data.Direction != "DEBIT" {
		t.Errorf("payload = %+v", data)
	}
}

func TestRejectedAndPendingReference(t *testing.T) {
	rej, err := NewWagerTransactionRejected("evt-3", "", "",
		WagerTransactionRejectedData{
			TransactionID: "tx-1", ProviderID: "provider-a", Kind: wager.KindBet,
			FailureCode: wager.FailureInsufficientFunds,
		})
	if err != nil {
		t.Fatalf("rejected error = %v", err)
	}
	var rdata WagerTransactionRejectedData
	if err := json.Unmarshal(rej.Data, &rdata); err != nil {
		t.Fatalf("rejected payload: %v", err)
	}
	if rdata.FailureCode != wager.FailureInsufficientFunds {
		t.Errorf("failureCode = %s", rdata.FailureCode)
	}

	pend, err := NewWagerTransactionPendingReference("evt-4", "", "",
		WagerTransactionPendingReferenceData{
			TransactionID: "tx-2", ReferenceExternalTransactionID: "ref-1",
		})
	if err != nil {
		t.Fatalf("pending error = %v", err)
	}
	if pend.EventType != TypeWagerTransactionPendingReference {
		t.Errorf("EventType = %s", pend.EventType)
	}
}

func TestEnvelopeRequiresEventID(t *testing.T) {
	_, err := NewWagerTransactionProcessed("", "", "",
		WagerTransactionProcessedData{TransactionID: "tx-1"})
	if !errors.Is(err, ErrEmptyEventID) {
		t.Errorf("eventID vazio = %v, want ErrEmptyEventID", err)
	}
}

func TestEnvelopeOccurredAtMarshalsRFC3339(t *testing.T) {
	env, err := NewWalletBalanceChanged("evt-5", "", "",
		WalletBalanceChangedData{WalletID: "w1"})
	if err != nil {
		t.Fatalf("error = %v", err)
	}
	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("Marshal error = %v", err)
	}
	var asJSON map[string]any
	if err := json.Unmarshal(raw, &asJSON); err != nil {
		t.Fatalf("Unmarshal envelope error = %v", err)
	}
	if _, ok := asJSON["occurredAt"]; !ok {
		t.Error("occurredAt ausente no envelope JSON")
	}
}
