// Package storage define as interfaces que os casos de uso exigem da camada
// de persistência (um `UnitOfWork` por comando e a `Database` que os abre).
// A implementação pgx atende estruturalmente; ver compile-time assertions em
// test/integration.
package storage

import (
	"context"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/events"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/ledger"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wager"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wallet"
)

// WalletRepository agrega o acesso a carteiras dentro de uma transação.
type WalletRepository interface {
	Insert(ctx context.Context, w wallet.Wallet) error
	Get(ctx context.Context, id string) (wallet.Wallet, error)
	LockForUpdate(ctx context.Context, id string) (wallet.Wallet, error)
	UpdateBalance(ctx context.Context, w wallet.Wallet) error
}

// WagerRepository agrega o acesso às transações de aposta dentro de uma transação.
type WagerRepository interface {
	Insert(ctx context.Context, t wager.WagerTransaction) error
	InsertIfAbsent(ctx context.Context, t wager.WagerTransaction) (bool, error)
	UpdateTerminal(ctx context.Context, t wager.WagerTransaction) error
	// UpdatePendingReference registra a espera pela referência (estado
	// PENDING_REFERENCE) mantendo o IDEMPOTENCY claim — o worker de referências
	// (M6) assume a resolução posterior.
	UpdatePendingReference(ctx context.Context, t wager.WagerTransaction) error
	GetByIdempotencyKey(ctx context.Context, key string) (wager.WagerTransaction, error)
	GetByIdempotencyKeyForUpdate(ctx context.Context, key string) (wager.WagerTransaction, error)
	GetByID(ctx context.Context, id string) (wager.WagerTransaction, error)
	GetByProviderExternal(ctx context.Context, providerID, externalID string) (wager.WagerTransaction, error)
	// GetByReferenceExternal resolve (providerId, referenceExternalTransactionId),
	// a referência de reversões e de WIN com referência (ARCHITECTURE §6).
	GetByReferenceExternal(ctx context.Context, providerID, referenceExternalID string) (wager.WagerTransaction, error)
	// GetReversalForReference devolve a reversão que aponta para o alvo; usada
	// para detectar reversões já concluídas (ALREADY_REVERSED, índice parcial
	// UNIQUE (resolved_reference_id) WHERE PROCESSED).
	GetReversalForReference(ctx context.Context, targetID string) (wager.WagerTransaction, error)
	// FindPendingDue lista transações em PENDING/PENDING_REFERENCE com
	// tentativa vencida, travando-as com SKIP LOCKED (worker de referências
	// M6.1 e varredor M6.3).
	FindPendingDue(ctx context.Context, now time.Time, limit int) ([]wager.WagerTransaction, error)
}

// LedgerAggregate resume o ledger de uma carteira para a reconciliação:
// soma de créditos, soma de débitos e total de lançamentos.
type LedgerAggregate struct {
	CreditsMinor int64
	DebitsMinor  int64
	Count        int64
}

// LedgerRepository agrega o acesso ao ledger dentro de uma transação.
type LedgerRepository interface {
	Insert(ctx context.Context, e ledger.Entry) error
	// ListByWallet lista os lançamentos em ordem (created_at, id) decrescente
	// com paginação por keyset. A currency da carteira é exigida porque a
	// tabela armazena apenas minor units.
	ListByWallet(ctx context.Context, walletID string, currency money.Currency,
		afterCreatedAt time.Time, afterID string, limit int) ([]ledger.Entry, error)
	// AggregateByWallet soma créditos e débitos e conta os lançamentos da
	// carteira. Deve ser lido no mesmo snapshot do saldo (reconciliação).
	AggregateByWallet(ctx context.Context, walletID string) (LedgerAggregate, error)
}

// OutboxRepository agrega o acesso à outbox dentro de uma transação.
type OutboxRepository interface {
	Insert(ctx context.Context, env events.Envelope) error
}

// UnitOfWork é uma transação com repositórios atrelados. Commit/Rollback são
// idempotentes sob o ponto de vista do chamador (o caso de uso sempre faz
// defer Rollback e chama Commit quando tudo deu certo).
type UnitOfWork interface {
	Wallets() WalletRepository
	Wagers() WagerRepository
	Ledger() LedgerRepository
	Outbox() OutboxRepository
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// Database abre transações para os casos de uso.
type Database interface {
	Begin(ctx context.Context) (UnitOfWork, error)
	// BeginReadOnly abre uma transação somente leitura em snapshot
	// (REPEATABLE READ), usada pela reconciliação para comparar saldo e ledger
	// na mesma visão consistente dos dados.
	BeginReadOnly(ctx context.Context) (UnitOfWork, error)
}
