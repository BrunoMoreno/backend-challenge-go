// Package query agrupa os casos de uso de leitura (consultas) sobre o
// PostgreSQL. Cada consulta roda no seu próprio UnitOfWork somente-leitura;
// nada é commitado.
package query

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/app/storage"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/ledger"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wager"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wallet"
	"github.com/BrunoMoreno/backend-challenge-go/internal/infra/postgres"
)

// Erros da camada de consulta.
var (
	ErrWalletNotFound      = errors.New("query: carteira inexistente")
	ErrTransactionNotFound = errors.New("query: transação inexistente")
	ErrInvalidCursor       = errors.New("query: cursor inválido")
)

const (
	defaultLimit = 50
	maxLimit     = 200
)

// Service centraliza as consultas sobre o estado persistido.
type Service struct {
	db storage.Database
}

// NewService cria o serviço de consultas com o banco da aplicação.
func NewService(db storage.Database) *Service {
	return &Service{db: db}
}

// Wallet devolve a carteira pelo id.
func (s *Service) Wallet(ctx context.Context, id string) (wallet.Wallet, error) {
	uow, err := s.db.Begin(ctx)
	if err != nil {
		return wallet.Wallet{}, err
	}
	defer uow.Rollback(ctx)

	w, err := uow.Wallets().Get(ctx, id)
	if errors.Is(err, postgres.ErrNotFound) {
		return wallet.Wallet{}, ErrWalletNotFound
	}
	if err != nil {
		return wallet.Wallet{}, err
	}
	return w, nil
}

// LedgerPage é uma página do extrato com cursor opaco p/ a próxima.
type LedgerPage struct {
	Entries    []ledger.Entry
	NextCursor string // vazio = não há próxima página
}

// Ledger lista o extrato da carteira em ordem (created_at, id) decrescente,
// paginado por cursor opaco. Limit é 1–200 (padrão 50).
func (s *Service) Ledger(ctx context.Context, walletID, cursor string, limit int) (LedgerPage, error) {
	if limit <= 0 {
		limit = defaultLimit
	}
	if limit > maxLimit {
		limit = maxLimit
	}

	uow, err := s.db.Begin(ctx)
	if err != nil {
		return LedgerPage{}, err
	}
	defer uow.Rollback(ctx)

	w, err := uow.Wallets().Get(ctx, walletID)
	if errors.Is(err, postgres.ErrNotFound) {
		return LedgerPage{}, ErrWalletNotFound
	}
	if err != nil {
		return LedgerPage{}, err
	}

	var afterCreatedAt time.Time
	var afterID string
	if cursor != "" {
		afterCreatedAt, afterID, err = decodeCursor(cursor)
		if err != nil {
			return LedgerPage{}, ErrInvalidCursor
		}
	}

	// Lê limit+1 para detectar a próxima página.
	entries, err := uow.Ledger().ListByWallet(ctx, walletID, w.Currency(),
		afterCreatedAt, afterID, limit+1)
	if err != nil {
		return LedgerPage{}, err
	}

	var next string
	if len(entries) > limit {
		entries = entries[:limit]
		last := entries[len(entries)-1]
		next = encodeCursor(last.CreatedAt(), last.ID())
	}
	return LedgerPage{Entries: entries, NextCursor: next}, nil
}

// Transaction devolve a transação pelo id interno.
func (s *Service) Transaction(ctx context.Context, id string) (wager.WagerTransaction, error) {
	uow, err := s.db.Begin(ctx)
	if err != nil {
		return wager.WagerTransaction{}, err
	}
	defer uow.Rollback(ctx)

	t, err := uow.Wagers().GetByID(ctx, id)
	if errors.Is(err, postgres.ErrNotFound) {
		return wager.WagerTransaction{}, ErrTransactionNotFound
	}
	if err != nil {
		return wager.WagerTransaction{}, err
	}
	return t, nil
}

// ProviderTransaction devolve a transação pela identidade externa
// (providerId, externalTransactionId).
func (s *Service) ProviderTransaction(ctx context.Context, providerID, externalID string) (wager.WagerTransaction, error) {
	uow, err := s.db.Begin(ctx)
	if err != nil {
		return wager.WagerTransaction{}, err
	}
	defer uow.Rollback(ctx)

	t, err := uow.Wagers().GetByProviderExternal(ctx, providerID, externalID)
	if errors.Is(err, postgres.ErrNotFound) {
		return wager.WagerTransaction{}, ErrTransactionNotFound
	}
	if err != nil {
		return wager.WagerTransaction{}, err
	}
	return t, nil
}

// cursor é o payload opaco da paginação do extrato (keyset). JSON + base64url
// para manter opacidade no transporte e revalidar integridade na decodificação.
type cursor struct {
	T  time.Time `json:"t"`
	ID string    `json:"id"`
}

func encodeCursor(createdAt time.Time, id string) string {
	raw, _ := json.Marshal(cursor{T: createdAt, ID: id})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeCursor(s string) (time.Time, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return time.Time{}, "", err
	}
	var c cursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return time.Time{}, "", err
	}
	if c.T.IsZero() || c.ID == "" {
		return time.Time{}, "", errors.New("query: cursor incompleto")
	}
	return c.T, c.ID, nil
}
