package wager

import (
	"errors"

	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/ledger"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
)

// Erros das regras por tipo e reversões.
var (
	ErrAmountNotPositive     = errors.New("wager: tipo exige valor positivo")
	ErrLossMustBeZero        = errors.New("wager: LOSS exige valor zero")
	ErrReferenceMismatch     = errors.New("wager: referência diverge nos campos de negócio")
	ErrInvalidReferenceKind  = errors.New("wager: tipo da referência incompatível com a operação")
	ErrReferenceNotProcessed = errors.New("wager: referência não foi processada com sucesso")
)

// ValidateAmount aplica a política de zero por tipo.
// BET/WIN/REFUND/ROLLBACK exigem valor > 0; LOSS exige exatamente 0.00.
func ValidateAmount(k Kind, amount money.Money) error {
	switch k {
	case KindBet, KindWin, KindRefund, KindRollback:
		if amount.IsZero() || amount.IsNegative() {
			return ErrAmountNotPositive
		}
	case KindLoss:
		if !amount.IsZero() {
			return ErrLossMustBeZero
		}
	}
	return nil
}

// Movement descreve o efeito financeiro de uma operação sobre a carteira.
// LOSS não gera Movement (não altera saldo).
type Movement struct {
	Direction ledger.Direction
	Amount    money.Money
}

// MovementFor calcula a movimentação para tipos sem dependência (BET/WIN/LOSS/OPENING).
// Wins/r/eversões com referência usam ResolveReversal.
func MovementFor(tx WagerTransaction) (Movement, bool, error) {
	switch tx.Kind() {
	case KindBet:
		if err := ValidateAmount(tx.Kind(), tx.Amount()); err != nil {
			return Movement{}, false, err
		}
		return Movement{Direction: ledger.DirectionDebit, Amount: tx.Amount()}, true, nil
	case KindWin:
		if err := ValidateAmount(tx.Kind(), tx.Amount()); err != nil {
			return Movement{}, false, err
		}
		return Movement{Direction: ledger.DirectionCredit, Amount: tx.Amount()}, true, nil
	case KindOpening:
		return Movement{Direction: ledger.DirectionCredit, Amount: tx.Amount()}, true, nil
	case KindLoss:
		return Movement{}, false, nil
	default:
		return Movement{}, false, ErrInvalidKind
	}
}

// ReferenceAgrees verifica concordância de provedor, jogador, carteira, moeda e
// rodada entre a operação e sua referência. Com requireEqualAmount exigido em
// reversões, o valor também deve coincidir.
func ReferenceAgrees(op, target WagerTransaction, requireEqualAmount bool) error {
	switch {
	case op.ProviderID() != target.ProviderID(),
		op.PlayerID() != target.PlayerID(),
		op.WalletID() != target.WalletID(),
		op.Amount().Currency() != target.Amount().Currency(),
		op.RoundID() != target.RoundID():
		return ErrReferenceMismatch
	}
	if requireEqualAmount {
		if c, _ := op.Amount().Compare(target.Amount()); c != 0 {
			return ErrReferenceMismatch
		}
	}
	return nil
}

// ResolveReversal calcula a movimentação de REFUND/ROLLBACK sobre a referência
// resolvida (deve estar PROCESSED). REFUND reembolsa integralmente uma BET;
// ROLLBACK desfaz integralmente BET (crédito) ou WIN/REFUND (débito).
func ResolveReversal(op, target WagerTransaction) (Movement, error) {
	if target.State() != StateProcessed {
		return Movement{}, ErrReferenceNotProcessed
	}
	if err := ReferenceAgrees(op, target, true); err != nil {
		return Movement{}, err
	}
	switch op.Kind() {
	case KindRefund:
		if target.Kind() != KindBet {
			return Movement{}, ErrInvalidReferenceKind
		}
		return Movement{Direction: ledger.DirectionCredit, Amount: target.Amount()}, nil
	case KindRollback:
		switch target.Kind() {
		case KindBet:
			return Movement{Direction: ledger.DirectionCredit, Amount: target.Amount()}, nil
		case KindWin, KindRefund:
			return Movement{Direction: ledger.DirectionDebit, Amount: target.Amount()}, nil
		default:
			return Movement{}, ErrInvalidReferenceKind
		}
	default:
		return Movement{}, ErrInvalidReferenceKind
	}
}

// ValidateWinReference valida a referência opcional de um WIN (aponta BET da
// mesma rodada, PROCESSED). Valor não precisa coincidir.
func ValidateWinReference(op, target WagerTransaction) error {
	if target.State() != StateProcessed {
		return ErrReferenceNotProcessed
	}
	if target.Kind() != KindBet {
		return ErrInvalidReferenceKind
	}
	return ReferenceAgrees(op, target, false)
}
