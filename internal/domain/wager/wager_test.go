package wager

import (
	"errors"
	"testing"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/ledger"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
)

func mm(minor int64, c string) money.Money { return money.MoneyOf(minor, money.Currency(c)) }

func newBET(t *testing.T, ref string) *WagerTransaction {
	t.Helper()
	return newExt(t, KindBet, 2500, ref)
}

func newExt(t *testing.T, kind Kind, minor int64, ref string) *WagerTransaction {
	t.Helper()
	tx, err := NewExternal("t1", "provider-a", "ext-1", "k1", "hash", "w1", "p1", "r1", "g1",
		kind, mm(minor, "BRL"), ref)
	if err != nil {
		t.Fatalf("NewExternal error = %v", err)
	}
	return &tx
}

func newRefund(t *testing.T) *WagerTransaction {
	t.Helper()
	return newExt(t, KindRefund, 2500, "ext-target")
}

func TestNewExternalValidation(t *testing.T) {
	if _, err := NewExternal("t1", "pa", "e1", "k", "h", "w1", "p1", "r", "g", KindOpening, mm(1, "BRL"), ""); !errors.Is(err, ErrInvalidKind) {
		t.Errorf("OPENING externo = %v, want ErrInvalidKind", err)
	}
	if _, err := NewExternal("t1", "pa", "e1", "k", "h", "w1", "p1", "r", "g", KindRefund, mm(1, "BRL"), ""); !errors.Is(err, ErrMissingReference) {
		t.Errorf("REFUND sem referência = %v, want ErrMissingReference", err)
	}
	if _, err := NewExternal("t1", "pa", "e1", "k", "h", "w1", "p1", "r", "g", KindRollback, mm(1, "BRL"), ""); !errors.Is(err, ErrMissingReference) {
		t.Errorf("ROLLBACK sem referência = %v, want ErrMissingReference", err)
	}
	if _, err := NewExternal("t1", "pa", "e1", "k", "h", "w1", "p1", "r", "g", KindBet, mm(1, "BRL"), "ref"); !errors.Is(err, ErrUnexpectedReference) {
		t.Errorf("BET com referência = %v, want ErrUnexpectedReference", err)
	}
	if _, err := NewExternal("t1", "pa", "e1", "k", "h", "w1", "p1", "r", "g", KindLoss, mm(0, "BRL"), "ref"); !errors.Is(err, ErrUnexpectedReference) {
		t.Errorf("LOSS com referência = %v, want ErrUnexpectedReference", err)
	}
	if _, err := NewExternal("", "pa", "e1", "k", "h", "w1", "p1", "r", "g", KindBet, mm(1, "BRL"), ""); !errors.Is(err, ErrMissingField) {
		t.Errorf("id vazio = %v, want ErrMissingField", err)
	}
	if _, err := NewExternal("t1", "pa", "e1", "", "h", "w1", "p1", "r", "g", KindBet, mm(1, "BRL"), ""); !errors.Is(err, ErrMissingField) {
		t.Errorf("idempotencyKey vazio = %v, want ErrMissingField", err)
	}
}

func TestNewExternalFields(t *testing.T) {
	tx, err := NewExternal("t1", "provider-a", "ext-1", "k1", "hash", "w1", "p1", "r1", "g1",
		KindWin, mm(500, "BRL"), "ref-1")
	if err != nil {
		t.Fatalf("NewExternal error = %v", err)
	}
	if tx.State() != StatePending {
		t.Errorf("State = %s, want PENDING", tx.State())
	}
	if tx.Origin() != OriginExternal {
		t.Errorf("Origin = %s, want EXTERNAL", tx.Origin())
	}
	if tx.ReferenceExternalTransactionID() != "ref-1" {
		t.Errorf("referência = %q, want ref-1", tx.ReferenceExternalTransactionID())
	}
	if tx.IsTerminal() {
		t.Error("PENDING não deve ser terminal")
	}
}

func TestNewOpening(t *testing.T) {
	tx, err := NewOpening("t1", "w1", "p1", mm(10000, "BRL"))
	if err != nil {
		t.Fatalf("NewOpening error = %v", err)
	}
	if tx.Origin() != OriginInternal || tx.Kind() != KindOpening || tx.State() != StatePending {
		t.Errorf("opening = %s/%s/%s", tx.Origin(), tx.Kind(), tx.State())
	}
	if tx.ProviderID() != "" || tx.ExternalTransactionID() != "" || tx.IdempotencyKey() != "" {
		t.Errorf("opening com metadados externos: %q %q %q", tx.ProviderID(), tx.ExternalTransactionID(), tx.IdempotencyKey())
	}
	if _, err := NewOpening("", "w1", "p1", mm(1, "BRL")); !errors.Is(err, ErrMissingField) {
		t.Errorf("opening id vazio = %v, want ErrMissingField", err)
	}
}

func TestRehydrateValidation(t *testing.T) {
	base := RehydrateInput{
		ID: "t1", Origin: OriginExternal, WalletID: "w1", PlayerID: "p1",
		Kind: KindBet, State: StateProcessed, Amount: mm(1, "BRL"),
	}
	if _, err := Rehydrate(base); err != nil {
		t.Fatalf("Rehydrate(base) error = %v", err)
	}
	openingWrong := base
	openingWrong.Origin = OriginExternal
	openingWrong.Kind = KindOpening
	if _, err := Rehydrate(openingWrong); !errors.Is(err, ErrInvalidOrigin) {
		t.Errorf("OPENING externo = %v, want ErrInvalidOrigin", err)
	}
	badKind := base
	badKind.Kind = "SLOT"
	if _, err := Rehydrate(badKind); !errors.Is(err, ErrInvalidKind) {
		t.Errorf("tipo inválido = %v, want ErrInvalidKind", err)
	}
	badOrigin := base
	badOrigin.Origin = "WEB"
	if _, err := Rehydrate(badOrigin); !errors.Is(err, ErrInvalidOrigin) {
		t.Errorf("origem inválida = %v, want ErrInvalidOrigin", err)
	}
}

func TestStateMachineValid(t *testing.T) {
	tx := newBET(t, "")
	if err := tx.MarkProcessed(mm(0, "BRL")); err != nil {
		t.Fatalf("MarkProcessed error = %v", err)
	}
	if tx.State() != StateProcessed || tx.ResultBalance().Minor() != 0 || !tx.IsTerminal() {
		t.Errorf("estado após processar = %s bal=%d", tx.State(), tx.ResultBalance().Minor())
	}
}

func TestTerminalImmutability(t *testing.T) {
	tx := newBET(t, "")
	if err := tx.Reject(FailureInsufficientFunds); err != nil {
		t.Fatalf("Reject error = %v", err)
	}
	if tx.State() != StateRejected || tx.FailureCode() != FailureInsufficientFunds {
		t.Errorf("estado = %s code=%s", tx.State(), tx.FailureCode())
	}
	if err := tx.MarkProcessed(mm(1, "BRL")); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("processar REJECTED = %v, want ErrInvalidTransition", err)
	}
	if err := tx.PendForReference(); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("pendurar REJECTED = %v, want ErrInvalidTransition", err)
	}
	if err := tx.Fail(FailureInternalInvariant); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("falhar REJECTED = %v, want ErrInvalidTransition", err)
	}
}

func TestPENDING_REFERENCETransitions(t *testing.T) {
	tx := newExt(t, KindRefund, 2500, "ref-1")
	if err := tx.PendForReference(); err != nil {
		t.Fatalf("PendForReference error = %v", err)
	}
	if tx.State() != StatePendingReference {
		t.Fatalf("State = %s, want PENDING_REFERENCE", tx.State())
	}
	if err := tx.NextAttempt(time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("NextAttempt error = %v", err)
	}
	if tx.Attempts() != 1 {
		t.Errorf("Attempts = %d, want 1", tx.Attempts())
	}
	if err := tx.MarkProcessed(mm(10, "BRL")); err != nil {
		t.Fatalf("MarkProcessed de PENDING_REFERENCE error = %v", err)
	}
}

func TestTransitionValidation(t *testing.T) {
	tx := newBET(t, "")
	if err := tx.Reject(""); !errors.Is(err, ErrMissingFailureCode) {
		t.Errorf("reject sem código = %v, want ErrMissingFailureCode", err)
	}
	if err := tx.MarkProcessed(money.Money{}); !errors.Is(err, ErrMissingResult) {
		t.Errorf("processed sem saldo = %v, want ErrMissingResult", err)
	}
	if err := tx.PendForReference(); err != nil {
		t.Fatalf("PendForReference error = %v", err)
	}
	if err := tx.PendForReference(); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("PendForReference duplicado = %v, want ErrInvalidTransition", err)
	}
	if err := tx.NextAttempt(time.Now()); err != nil {
		t.Errorf("NextAttempt ok deveria funcionar: %v", err)
	}
}

func TestResolveReference(t *testing.T) {
	tx := newBET(t, "")
	if err := tx.ResolveReference("target-1"); err != nil {
		t.Fatalf("ResolveReference error = %v", err)
	}
	if tx.ResolvedReferenceTransactionID() != "target-1" {
		t.Errorf("resolved = %q", tx.ResolvedReferenceTransactionID())
	}
}

func TestRehydrateRoundTrip(t *testing.T) {
	tx := newExt(t, KindRefund, 2500, "ref-9")
	if err := tx.PendForReference(); err != nil {
		t.Fatalf("PendForReference error = %v", err)
	}
	if err := tx.ResolveReference("target-9"); err != nil {
		t.Fatalf("ResolveReference error = %v", err)
	}
	if err := tx.NextAttempt(time.Unix(0, 0).UTC()); err != nil {
		t.Fatalf("NextAttempt error = %v", err)
	}
	if err := tx.MarkProcessed(mm(123, "BRL")); err != nil {
		t.Fatalf("MarkProcessed error = %v", err)
	}
	rt, err := Rehydrate(RehydrateInput{
		ID: tx.ID(), Origin: tx.Origin(), ProviderID: tx.ProviderID(),
		ExternalTransactionID: tx.ExternalTransactionID(), IdempotencyKey: tx.IdempotencyKey(),
		PayloadHash: tx.PayloadHash(), WalletID: tx.WalletID(), PlayerID: tx.PlayerID(),
		RoundID: tx.RoundID(), GameID: tx.GameID(), Kind: tx.Kind(), Amount: tx.Amount(),
		ReferenceExternalTransactionID: tx.ReferenceExternalTransactionID(),
		ResolvedReferenceTransactionID: tx.ResolvedReferenceTransactionID(),
		State:                          tx.State(), FailureCode: tx.FailureCode(), ResultBalance: tx.ResultBalance(),
		Attempts: tx.Attempts(), CreatedAt: tx.CreatedAt(), UpdatedAt: tx.UpdatedAt(),
	})
	if err != nil {
		t.Fatalf("Rehydrate roundtrip error = %v", err)
	}
	if rt.State() != StateProcessed || rt.ResultBalance().Minor() != 123 || rt.Attempts() != 1 {
		t.Errorf("roundtrip perdeu estado: %s bal=%d attempts=%d", rt.State(), rt.ResultBalance().Minor(), rt.Attempts())
	}
}

// --- Regras por tipo e reversões ---

func TestValidateAmountZeroPolicy(t *testing.T) {
	if err := ValidateAmount(KindBet, money.Zero("BRL")); !errors.Is(err, ErrAmountNotPositive) {
		t.Errorf("BET zero = %v, want ErrAmountNotPositive", err)
	}
	if err := ValidateAmount(KindWin, mm(1, "BRL")); err != nil {
		t.Errorf("WIN positivo = %v, want nil", err)
	}
	if err := ValidateAmount(KindLoss, mm(1, "BRL")); !errors.Is(err, ErrLossMustBeZero) {
		t.Errorf("LOSS 0.01 = %v, want ErrLossMustBeZero", err)
	}
	if err := ValidateAmount(KindLoss, money.Zero("BRL")); err != nil {
		t.Errorf("LOSS 0.00 = %v, want nil", err)
	}
	if err := ValidateAmount(KindRefund, money.Zero("BRL")); !errors.Is(err, ErrAmountNotPositive) {
		t.Errorf("REFUND zero = %v, want ErrAmountNotPositive", err)
	}
	if err := ValidateAmount(KindRollback, money.Zero("BRL")); !errors.Is(err, ErrAmountNotPositive) {
		t.Errorf("ROLLBACK zero = %v, want ErrAmountNotPositive", err)
	}
}

func TestMovementFor(t *testing.T) {
	bet := newBET(t, "")
	bet.amount = mm(2500, "BRL")
	bet.kind = KindBet
	m, ok, err := MovementFor(*bet)
	if err != nil || !ok || m.Direction != ledger.DirectionDebit {
		t.Errorf("BET movement = %+v ok=%v err=%v", m, ok, err)
	}
	bet.kind = KindWin
	m, ok, err = MovementFor(*bet)
	if err != nil || !ok || m.Direction != ledger.DirectionCredit {
		t.Errorf("WIN movement = %+v ok=%v err=%v", m, ok, err)
	}
	bet.kind = KindLoss
	bet.amount = money.Zero("BRL")
	m, ok, err = MovementFor(*bet)
	if err != nil || ok {
		t.Errorf("LOSS movement = %+v ok=%v err=%v (esperado sem movimento)", m, ok, err)
	}
}

func target(t *testing.T, kind Kind, minor int64, currency string) WagerTransaction {
	t.Helper()
	tx, err := Rehydrate(RehydrateInput{
		ID: "target", Origin: OriginExternal, ProviderID: "provider-a",
		ExternalTransactionID: "ext-target", WalletID: "w1", PlayerID: "p1",
		RoundID: "r1", GameID: "g1", Kind: kind, Amount: mm(minor, currency),
		State: StateProcessed,
	})
	if err != nil {
		t.Fatalf("target error = %v", err)
	}
	return tx
}

func TestResolveReversal(t *testing.T) {
	bet := target(t, KindBet, 2500, "BRL")

	op := newRefund(t)
	op.amount = mm(2500, "BRL")
	m, err := ResolveReversal(*op, bet)
	if err != nil || m.Direction != ledger.DirectionCredit {
		t.Errorf("REFUND/BET = %+v err=%v, want credit", m, err)
	}

	op.kind = KindRollback
	m, err = ResolveReversal(*op, bet)
	if err != nil || m.Direction != ledger.DirectionCredit {
		t.Errorf("ROLLBACK/BET = %+v err=%v, want credit", m, err)
	}

	win := target(t, KindWin, 5000, "BRL")
	op.kind = KindRollback
	op.amount = mm(5000, "BRL")
	m, err = ResolveReversal(*op, win)
	if err != nil || m.Direction != ledger.DirectionDebit {
		t.Errorf("ROLLBACK/WIN = %+v err=%v, want debit", m, err)
	}
}

func TestResolveReversalInvalid(t *testing.T) {
	win := target(t, KindWin, 500, "BRL")
	op := newRefund(t)
	op.amount = mm(500, "BRL")

	op.kind = KindRefund
	if _, err := ResolveReversal(*op, win); !errors.Is(err, ErrInvalidReferenceKind) {
		t.Errorf("REFUND/WIN = %v, want ErrInvalidReferenceKind", err)
	}

	loss := target(t, KindLoss, 0, "BRL")
	op.kind = KindRollback
	op.amount = money.Zero("BRL")
	if _, err := ResolveReversal(*op, loss); !errors.Is(err, ErrInvalidReferenceKind) {
		t.Errorf("ROLLBACK/LOSS = %v, want ErrInvalidReferenceKind", err)
	}

	notProcessed, err := Rehydrate(RehydrateInput{
		ID: "target", Origin: OriginExternal, ProviderID: "provider-a",
		ExternalTransactionID: "ext-target", WalletID: "w1", PlayerID: "p1",
		RoundID: "r1", GameID: "g1", Kind: KindBet, Amount: mm(2500, "BRL"),
		State: StatePending,
	})
	if err != nil {
		t.Fatalf("notProcessed error = %v", err)
	}
	op.kind = KindRefund
	op.amount = mm(2500, "BRL")
	if _, err := ResolveReversal(*op, notProcessed); !errors.Is(err, ErrReferenceNotProcessed) {
		t.Errorf("referência pendente = %v, want ErrReferenceNotProcessed", err)
	}
}

func TestResolveReversalMismatch(t *testing.T) {
	bet := target(t, KindBet, 2500, "BRL")

	op := newRefund(t)
	op.amount = mm(2400, "BRL")
	if _, err := ResolveReversal(*op, bet); !errors.Is(err, ErrReferenceMismatch) {
		t.Errorf("valor divergente = %v, want ErrReferenceMismatch", err)
	}

	op.amount = mm(2500, "BRL")
	op.roundID = "r-diferente"
	if _, err := ResolveReversal(*op, bet); !errors.Is(err, ErrReferenceMismatch) {
		t.Errorf("rodada divergente = %v, want ErrReferenceMismatch", err)
	}

	op.roundID = "r1"
	op.amount = mm(2500, "USD")
	if _, err := ResolveReversal(*op, bet); !errors.Is(err, ErrReferenceMismatch) {
		t.Errorf("moeda divergente = %v, want ErrReferenceMismatch", err)
	}
}

func TestValidateWinReference(t *testing.T) {
	bet := target(t, KindBet, 2500, "BRL")
	win := newExt(t, KindWin, 999, "ext-target")
	if err := ValidateWinReference(*win, bet); err != nil {
		t.Errorf("WIN sobre BET (valor difere) = %v, want nil", err)
	}

	winWrong := newExt(t, KindWin, 500, "ext-target")
	refund := target(t, KindRefund, 500, "BRL")
	if err := ValidateWinReference(*winWrong, refund); !errors.Is(err, ErrInvalidReferenceKind) {
		t.Errorf("WIN sobre REFUND = %v, want ErrInvalidReferenceKind", err)
	}
}
