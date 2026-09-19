package wager

import (
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
)

// RehydrateInput carrega todos os campos para reconstrução de uma transação persistida.
type RehydrateInput struct {
	ID                             string
	Origin                         Origin
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PayloadHash                    string
	WalletID                       string
	PlayerID                       string
	RoundID                        string
	GameID                         string
	Kind                           Kind
	Amount                         money.Money
	ReferenceExternalTransactionID string
	ResolvedReferenceTransactionID string
	State                          State
	FailureCode                    FailureCode
	ResultBalance                  money.Money
	Attempts                       int
	NextAttemptAt                  time.Time
	CreatedAt                      time.Time
	UpdatedAt                      time.Time
}

// Rehydrate reconstrói a transação persistida sem reaplicar transições.
func Rehydrate(in RehydrateInput) (WagerTransaction, error) {
	if in.ID == "" || in.WalletID == "" || in.PlayerID == "" {
		return WagerTransaction{}, ErrMissingField
	}
	if in.Origin != OriginInternal && in.Origin != OriginExternal {
		return WagerTransaction{}, ErrInvalidOrigin
	}
	if in.Kind == KindOpening {
		if in.Origin != OriginInternal {
			return WagerTransaction{}, ErrInvalidOrigin
		}
	} else if !validExternalKind(in.Kind) {
		return WagerTransaction{}, ErrInvalidKind
	}
	return WagerTransaction{
		id:                             in.ID,
		origin:                         in.Origin,
		providerID:                     in.ProviderID,
		externalTransactionID:          in.ExternalTransactionID,
		idempotencyKey:                 in.IdempotencyKey,
		payloadHash:                    in.PayloadHash,
		walletID:                       in.WalletID,
		playerID:                       in.PlayerID,
		roundID:                        in.RoundID,
		gameID:                         in.GameID,
		kind:                           in.Kind,
		amount:                         in.Amount,
		referenceExternalTransactionID: in.ReferenceExternalTransactionID,
		resolvedReferenceTransactionID: in.ResolvedReferenceTransactionID,
		state:                          in.State,
		failureCode:                    in.FailureCode,
		resultBalance:                  in.ResultBalance,
		attempts:                       in.Attempts,
		nextAttemptAt:                  in.NextAttemptAt,
		createdAt:                      in.CreatedAt,
		updatedAt:                      in.UpdatedAt,
	}, nil
}
