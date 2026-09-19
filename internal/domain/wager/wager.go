// Package wager define a máquina de estados da transação de aposta.
package wager

import (
	"errors"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
)

// Kind é o tipo da operação.
type Kind string

const (
	KindOpening  Kind = "OPENING"
	KindBet      Kind = "BET"
	KindWin      Kind = "WIN"
	KindLoss     Kind = "LOSS"
	KindRefund   Kind = "REFUND"
	KindRollback Kind = "ROLLBACK"
)

// Origin distingue operações internas (abertura) das externas (provedor).
type Origin string

const (
	OriginInternal Origin = "INTERNAL"
	OriginExternal Origin = "EXTERNAL"
)

// State é o estado da transação.
type State string

const (
	StatePending          State = "PENDING"
	StatePendingReference State = "PENDING_REFERENCE"
	StateProcessed        State = "PROCESSED"
	StateRejected         State = "REJECTED"
	StateFailed           State = "FAILED"
)

// FailureCode é o código persistido de rejeição/falha definitiva.
type FailureCode string

const (
	FailureInsufficientFunds         FailureCode = "INSUFFICIENT_FUNDS"
	FailureReversalInsufficientFunds FailureCode = "REVERSAL_INSUFFICIENT_FUNDS"
	FailureReferenceNotFound         FailureCode = "REFERENCE_NOT_FOUND"
	FailureReferenceNotProcessed     FailureCode = "REFERENCE_NOT_PROCESSED"
	FailureReferenceMismatch         FailureCode = "REFERENCE_MISMATCH"
	FailureInvalidReferenceKind      FailureCode = "INVALID_REFERENCE_KIND"
	FailureAlreadyReversed           FailureCode = "ALREADY_REVERSED"
	FailurePlayerMismatch            FailureCode = "PLAYER_MISMATCH"
	FailureCurrencyMismatch          FailureCode = "CURRENCY_MISMATCH"
	FailureInternalInvariant         FailureCode = "INTERNAL_INVARIANT_VIOLATION"
)

// Erros de domínio da transação.
var (
	ErrInvalidTransition   = errors.New("wager: transição inválida no estado atual")
	ErrMissingField        = errors.New("wager: campo obrigatório ausente")
	ErrInvalidKind         = errors.New("wager: tipo inválido")
	ErrOpeningNotAllowed   = errors.New("wager: OPENING não permitido em operação externa")
	ErrMissingReference    = errors.New("wager: REFUND/ROLLBACK exigem referência externa")
	ErrUnexpectedReference = errors.New("wager: referência não permitida para este tipo")
	ErrMissingFailureCode  = errors.New("wager: código de falha obrigatório")
	ErrMissingResult       = errors.New("wager: saldo resultante obrigatório")
	ErrInvalidOrigin       = errors.New("wager: origem inválida")
)

// WagerTransaction registra uma operação financeira de aposta.
type WagerTransaction struct {
	id                             string
	origin                         Origin
	providerID                     string
	externalTransactionID          string
	idempotencyKey                 string
	payloadHash                    string
	walletID                       string
	playerID                       string
	roundID                        string
	gameID                         string
	kind                           Kind
	amount                         money.Money
	referenceExternalTransactionID string
	resolvedReferenceTransactionID string
	state                          State
	failureCode                    FailureCode
	resultBalance                  money.Money
	attempts                       int
	nextAttemptAt                  time.Time
	createdAt                      time.Time
	updatedAt                      time.Time
}
