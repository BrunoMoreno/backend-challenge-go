package wager

import (
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
)

func (t WagerTransaction) ID() string                    { return t.id }
func (t WagerTransaction) Origin() Origin                { return t.origin }
func (t WagerTransaction) ProviderID() string            { return t.providerID }
func (t WagerTransaction) ExternalTransactionID() string { return t.externalTransactionID }
func (t WagerTransaction) IdempotencyKey() string        { return t.idempotencyKey }
func (t WagerTransaction) PayloadHash() string           { return t.payloadHash }
func (t WagerTransaction) WalletID() string              { return t.walletID }
func (t WagerTransaction) PlayerID() string              { return t.playerID }
func (t WagerTransaction) RoundID() string               { return t.roundID }
func (t WagerTransaction) GameID() string                { return t.gameID }
func (t WagerTransaction) Kind() Kind                    { return t.kind }
func (t WagerTransaction) Amount() money.Money           { return t.amount }
func (t WagerTransaction) ReferenceExternalTransactionID() string {
	return t.referenceExternalTransactionID
}
func (t WagerTransaction) ResolvedReferenceTransactionID() string {
	return t.resolvedReferenceTransactionID
}
func (t WagerTransaction) State() State               { return t.state }
func (t WagerTransaction) FailureCode() FailureCode   { return t.failureCode }
func (t WagerTransaction) ResultBalance() money.Money { return t.resultBalance }
func (t WagerTransaction) Attempts() int              { return t.attempts }
func (t WagerTransaction) NextAttemptAt() time.Time   { return t.nextAttemptAt }
func (t WagerTransaction) CreatedAt() time.Time       { return t.createdAt }
func (t WagerTransaction) UpdatedAt() time.Time       { return t.updatedAt }

// MarkProcessed conclui com sucesso a partir de PENDING ou PENDING_REFERENCE.
func (t *WagerTransaction) MarkProcessed(resultBalance money.Money) error {
	if t.state != StatePending && t.state != StatePendingReference {
		return ErrInvalidTransition
	}
	if resultBalance.Currency() == "" {
		return ErrMissingResult
	}
	t.state = StateProcessed
	t.resultBalance = resultBalance
	t.updatedAt = time.Now().UTC()
	return nil
}

// Reject marca rejeição definitiva por regra de negócio, exigindo failureCode.
func (t *WagerTransaction) Reject(code FailureCode) error {
	if t.state != StatePending && t.state != StatePendingReference {
		return ErrInvalidTransition
	}
	if code == "" {
		return ErrMissingFailureCode
	}
	t.state = StateRejected
	t.failureCode = code
	t.updatedAt = time.Now().UTC()
	return nil
}

// PendForReference move para PENDING_REFERENCE a partir de PENDING.
func (t *WagerTransaction) PendForReference() error {
	if t.state != StatePending {
		return ErrInvalidTransition
	}
	t.state = StatePendingReference
	t.updatedAt = time.Now().UTC()
	return nil
}

// Fail marca falha permanente de infraestrutura, exigindo failureCode.
func (t *WagerTransaction) Fail(code FailureCode) error {
	if t.state != StatePending && t.state != StatePendingReference {
		return ErrInvalidTransition
	}
	if code == "" {
		return ErrMissingFailureCode
	}
	t.state = StateFailed
	t.failureCode = code
	t.updatedAt = time.Now().UTC()
	return nil
}

// NextAttempt registra mais uma tentativa de resolução da referência.
func (t *WagerTransaction) NextAttempt(next time.Time) error {
	if t.state != StatePendingReference {
		return ErrInvalidTransition
	}
	t.attempts++
	t.nextAttemptAt = next
	t.updatedAt = time.Now().UTC()
	return nil
}

// ResolveReference registra a referência interna resolvida.
func (t *WagerTransaction) ResolveReference(resolvedID string) error {
	if resolvedID == "" {
		return ErrMissingField
	}
	t.resolvedReferenceTransactionID = resolvedID
	return nil
}

// IsTerminal informa se o estado é terminal (PROCESSED, REJECTED, FAILED).
func (t WagerTransaction) IsTerminal() bool {
	switch t.state {
	case StateProcessed, StateRejected, StateFailed:
		return true
	default:
		return false
	}
}
