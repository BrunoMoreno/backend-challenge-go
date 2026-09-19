// Package postgres contém acesso a dados: pool, UnitOfWork e repositórios pgx.
package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Erros de repositório mapeados a partir de condições do Postgres.
var (
	ErrNotFound       = errors.New("postgres: registro não encontrado")
	ErrDuplicate      = errors.New("postgres: registro duplicado")
	ErrOptimisticLock = errors.New("postgres: conflito de versão otimista")
	ErrPermission     = errors.New("postgres: permissão negada")
	ErrSerialization  = errors.New("postgres: falha de serialização")
	ErrDeadlock       = errors.New("postgres: deadlock")
	ErrCheck          = errors.New("postgres: violação de constraint")
	ErrForeignKey     = errors.New("postgres: violação de chave estrangeira")
	ErrUnexpected     = errors.New("postgres: erro inesperado")
)

// NewPool cria um pool pgx a partir do DSN.
func NewPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: abrir pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	return pool, nil
}

// mapError converte erros do pgx nos sentinelas deste pacote.
func mapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505":
			return fmt.Errorf("%w: %s", ErrDuplicate, pgErr.ConstraintName)
		case "23503":
			return fmt.Errorf("%w: %s", ErrForeignKey, pgErr.ConstraintName)
		case "23514":
			return fmt.Errorf("%w: %s", ErrCheck, pgErr.ConstraintName)
		case "42501":
			return fmt.Errorf("%w: %s", ErrPermission, pgErr.Message)
		case "40001":
			return ErrSerialization
		case "40P01":
			return ErrDeadlock
		case "P0001":
			// raise via trigger/função de integridade (ledger imutável,
			// terminais imutáveis, consistência deferida saldo × ledger).
			return fmt.Errorf("%w: %s", ErrCheck, pgErr.Message)
		}
	}
	return fmt.Errorf("%w: %v", ErrUnexpected, err)
}

// UnitOfWork concentra a transação e os repositórios operando nela.
type UnitOfWork struct {
	ctx context.Context
	tx  pgx.Tx

	WalletRepository *WalletRepository
	WagerRepository  *WagerTransactionRepository
	LedgerRepository *LedgerRepository
	InboxRepository  *InboxRepository
	OutboxRepository *OutboxRepository
}

func newUnitOfWork(ctx context.Context, tx pgx.Tx) *UnitOfWork {
	uow := &UnitOfWork{ctx: ctx, tx: tx}
	uow.WalletRepository = &WalletRepository{tx: tx}
	uow.WagerRepository = &WagerTransactionRepository{tx: tx}
	uow.LedgerRepository = &LedgerRepository{tx: tx}
	uow.InboxRepository = &InboxRepository{tx: tx}
	uow.OutboxRepository = &OutboxRepository{tx: tx}
	return uow
}

// Commit persiste as mudanças da transação.
func (u *UnitOfWork) Commit(ctx context.Context) error {
	return mapError(u.tx.Commit(ctx))
}

// Rollback descarta as mudanças da transação.
func (u *UnitOfWork) Rollback(ctx context.Context) error {
	return mapError(u.tx.Rollback(ctx))
}

// Tx expõe a transação bruta (para workers que precisam de queries ad-hoc).
func (u *UnitOfWork) Tx() pgx.Tx { return u.tx }

// UnitOfWorkFactory abre transações e entrega UnitOfWork consistentes.
type UnitOfWorkFactory struct {
	pool *pgxpool.Pool
}

// NewUnitOfWorkFactory cria a fábrica de transações do pool.
func NewUnitOfWorkFactory(pool *pgxpool.Pool) *UnitOfWorkFactory {
	return &UnitOfWorkFactory{pool: pool}
}

// Begin abre uma nova transação e monta os repositórios sobre ela.
func (f *UnitOfWorkFactory) Begin(ctx context.Context) (*UnitOfWork, error) {
	tx, err := f.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("postgres: begin: %w", mapError(err))
	}
	return newUnitOfWork(ctx, tx), nil
}
