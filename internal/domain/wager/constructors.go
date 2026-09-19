package wager

import (
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
)

// NewExternal inicia uma operação do provedor no estado PENDING.
func NewExternal(id, providerID, externalTransactionID, idempotencyKey, payloadHash,
	walletID, playerID, roundID, gameID string, kind Kind, amount money.Money,
	referenceExternalTransactionID string) (WagerTransaction, error) {
	if id == "" || walletID == "" || playerID == "" {
		return WagerTransaction{}, ErrMissingField
	}
	if !validExternalKind(kind) {
		return WagerTransaction{}, ErrInvalidKind
	}
	if amount.Currency() == "" {
		return WagerTransaction{}, ErrMissingField
	}
	switch kind {
	case KindRefund, KindRollback:
		if referenceExternalTransactionID == "" {
			return WagerTransaction{}, ErrMissingReference
		}
	case KindBet, KindLoss:
		if referenceExternalTransactionID != "" {
			return WagerTransaction{}, ErrUnexpectedReference
		}
	}
	if providerID == "" || externalTransactionID == "" || idempotencyKey == "" ||
		payloadHash == "" || roundID == "" || gameID == "" {
		return WagerTransaction{}, ErrMissingField
	}
	return WagerTransaction{
		id:                             id,
		origin:                         OriginExternal,
		providerID:                     providerID,
		externalTransactionID:          externalTransactionID,
		idempotencyKey:                 idempotencyKey,
		payloadHash:                    payloadHash,
		walletID:                       walletID,
		playerID:                       playerID,
		roundID:                        roundID,
		gameID:                         gameID,
		kind:                           kind,
		amount:                         amount,
		referenceExternalTransactionID: referenceExternalTransactionID,
		state:                          StatePending,
		createdAt:                      time.Now().UTC(),
		updatedAt:                      time.Now().UTC(),
	}, nil
}

// NewOpening inicia uma abertura interna de carteira no estado PENDING.
func NewOpening(id, walletID, playerID string, amount money.Money) (WagerTransaction, error) {
	if id == "" || walletID == "" || playerID == "" {
		return WagerTransaction{}, ErrMissingField
	}
	if amount.Currency() == "" {
		return WagerTransaction{}, ErrMissingField
	}
	return WagerTransaction{
		id:        id,
		origin:    OriginInternal,
		walletID:  walletID,
		playerID:  playerID,
		kind:      KindOpening,
		amount:    amount,
		state:     StatePending,
		createdAt: time.Now().UTC(),
		updatedAt: time.Now().UTC(),
	}, nil
}

func validExternalKind(k Kind) bool {
	switch k {
	case KindBet, KindWin, KindLoss, KindRefund, KindRollback:
		return true
	default:
		return false
	}
}
