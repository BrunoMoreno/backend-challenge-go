// Package processwager implementa o processamento síncrono de uma operação do
// provedor (BET/WIN/LOSS) com persistência da rejeição e idempotência de
// replay (RF-03, G2). REFUND/ROLLBACK e WIN com referência são cobertos nas
// tarefas 3.3/3.4 (PENDING_REFERENCE).
package processwager

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/app/storage"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/events"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/ledger"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wager"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wallet"
	"github.com/BrunoMoreno/backend-challenge-go/internal/infra/postgres"
)

// Erros de validação e conflito do caso de uso.
var (
	ErrMissingIdempotencyKey  = errors.New("processwager: idempotencyKey ausente")
	ErrUnsupportedKind        = errors.New("processwager: kind não suportado neste canal/fase")
	ErrWinReferencePending    = errors.New("processwager: WIN com referência aguarda PENDING_REFERENCE (3.4)")
	ErrInvalidAmountForKind   = errors.New("processwager: valor inválido para o kind")
	ErrWalletNotFound         = errors.New("processwager: carteira inexistente")
	ErrWalletCurrencyMismatch = errors.New("processwager: moeda do payload difere da carteira")
	ErrIdempotencyConflict    = errors.New("processwager: chave de idempotência reutilizada com conteúdo diferente")
	ErrExternalConflict       = errors.New("processwager: mesmo (provider, externalTransactionId) com outra chave")
	// ErrStaleClaim indica que o dono de um claim concorrente desfez a
	// transação; o Process repete o ciclo com uma transação nova.
	ErrStaleClaim = errors.New("processwager: claim concorrente desfeito, reprocessar")
	// ErrReferenceUnresolved indica referência ausente ou ainda não terminal;
	// em 3.3 nada é persistido — a persistência PENDING_REFERENCE é a 3.4.
	ErrReferenceUnresolved = errors.New("processwager: referência não resolvida (PENDING_REFERENCE em 3.4)")
)

// Input é a operação do provedor já validada estruturalmente (o transporte
// garante o formato do dinheiro e a presença dos campos).
type Input struct {
	ProviderID                     string
	ExternalTransactionID          string
	PlayerID                       string
	WalletID                       string
	RoundID                        string
	GameID                         string
	Kind                           wager.Kind
	Amount                         money.Money
	ReferenceExternalTransactionID string
	IdempotencyKey                 string
}

// Result é o desfecho síncrono da operação.
type Result struct {
	TransactionID    string
	State            wager.State
	FailureCode      wager.FailureCode
	Balance          *money.Money // definido quando PROCESSED
	IdempotentReplay bool
}

// Service processa operações dentro de transações atômicas.
type Service struct {
	db    storage.Database
	newID func() string
}

// NewService cria o caso de uso com o banco da aplicação.
func NewService(db storage.Database) *Service {
	return &Service{db: db, newID: newID}
}

// NewServiceWithIDs permite injetar o gerador de ids (testes determinísticos).
func NewServiceWithIDs(db storage.Database, newID func() string) *Service {
	return &Service{db: db, newID: newID}
}

// Process executa a operação. Corridas de claim (outro processador desfez a
// transação) são refeitas com limite de tentativas.
func (s *Service) Process(ctx context.Context, in Input) (Result, error) {
	if err := validate(in); err != nil {
		return Result{}, err
	}
	hash, err := wager.HashPayload(wager.PayloadFields{
		ProviderID:                     in.ProviderID,
		ExternalTransactionID:          in.ExternalTransactionID,
		PlayerID:                       in.PlayerID,
		WalletID:                       in.WalletID,
		RoundID:                        in.RoundID,
		GameID:                         in.GameID,
		Kind:                           in.Kind,
		Amount:                         in.Amount,
		ReferenceExternalTransactionID: in.ReferenceExternalTransactionID,
	})
	if err != nil {
		return Result{}, err
	}
	const maxAttempts = 3
	for attempt := 0; attempt < maxAttempts; attempt++ {
		res, err := s.tryProcess(ctx, in, hash)
		if errors.Is(err, ErrStaleClaim) {
			continue
		}
		return res, err
	}
	return Result{}, fmt.Errorf("%w: limite de tentativas", ErrStaleClaim)
}

// validate garante as regras antes de qualquer claim no banco.
func validate(in Input) error {
	if in.IdempotencyKey == "" {
		return ErrMissingIdempotencyKey
	}
	switch in.Kind {
	case wager.KindBet, wager.KindWin, wager.KindLoss, wager.KindRefund, wager.KindRollback:
	default:
		return ErrUnsupportedKind
	}
	if in.Kind == wager.KindWin && in.ReferenceExternalTransactionID != "" {
		return ErrWinReferencePending
	}
	if in.Kind == wager.KindLoss {
		if !in.Amount.IsZero() {
			return ErrInvalidAmountForKind
		}
	} else if in.Amount.IsZero() {
		return ErrInvalidAmountForKind
	}
	return nil
}

// tryProcess é a tentativa em uma transação nova. Retorna ErrStaleClaim quando
// o claim concorrente com a mesma chave foi desfeito (rollback) e a operação
// precisa ser reexecutada do zero.
func (s *Service) tryProcess(ctx context.Context, in Input, hash string) (Result, error) {
	uow, err := s.db.Begin(ctx)
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = uow.Rollback(ctx) }()

	tx, err := wager.NewExternal(s.newID(), in.ProviderID, in.ExternalTransactionID,
		in.IdempotencyKey, hash, in.WalletID, in.PlayerID, in.RoundID, in.GameID,
		in.Kind, in.Amount, in.ReferenceExternalTransactionID)
	if err != nil {
		return Result{}, err
	}

	inserted, err := uow.Wagers().InsertIfAbsent(ctx, tx)
	if err != nil {
		return s.conflict(ctx, in, hash, err)
	}
	if !inserted {
		return s.replayByKey(ctx, uow, in, hash)
	}
	return s.apply(ctx, uow, tx, in)
}

// conflict trata a falha do INSERT. O ON CONFLICT (idempotency_key) DO NOTHING
// não cobre o único parcial (provider, externalId): nesse caso o Postgres
// levanta 23505 e aborta a transação atual, então a confirmação é feita em uma
// transação nova (a linha conflitante já está commitada por outro processador).
// Um 23505 também não alcançaria a mesma chave de idempotência — com a mesma
// chave o ON CONFLICT teria retornado inserted=false sem erro.
func (s *Service) conflict(ctx context.Context, in Input, hash string, err error) (Result, error) {
	if !errors.Is(err, postgres.ErrDuplicate) {
		if errors.Is(err, postgres.ErrForeignKey) {
			return Result{}, ErrWalletNotFound
		}
		return Result{}, err
	}
	lookup, lerr := s.db.Begin(ctx)
	if lerr != nil {
		return Result{}, lerr
	}
	defer func() { _ = lookup.Rollback(ctx) }()
	existing, gerr := lookup.Wagers().GetByProviderExternal(ctx, in.ProviderID, in.ExternalTransactionID)
	if gerr != nil {
		return Result{}, gerr
	}
	if existing.IdempotencyKey() != in.IdempotencyKey {
		return Result{}, ErrExternalConflict
	}
	return s.replayByKey(ctx, lookup, in, hash)
}

// replayByKey devolve o resultado de uma operação já processada com a mesma
// chave de idempotência. Conteúdo diferente com a mesma chave → conflito.
func (s *Service) replayByKey(ctx context.Context, uow storage.UnitOfWork, in Input, hash string) (Result, error) {
	existing, err := uow.Wagers().GetByIdempotencyKey(ctx, in.IdempotencyKey)
	if err != nil {
		return Result{}, err
	}
	if existing.PayloadHash() != hash {
		return Result{}, ErrIdempotencyConflict
	}
	return s.replayResult(ctx, uow, existing)
}

// replayResult traduz uma transação já persistida em Result, aguardando (lock
// de linha) a conclusão/realização de um claim concorrente.
func (s *Service) replayResult(ctx context.Context, uow storage.UnitOfWork, existing wager.WagerTransaction) (Result, error) {
	final, err := uow.Wagers().GetByIdempotencyKeyForUpdate(ctx, existing.IdempotencyKey())
	if err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			return Result{}, ErrStaleClaim
		}
		return Result{}, err
	}
	if !final.IsTerminal() {
		// Claim alheio ainda em voo na transação (ou pendência futura).
		// Devolve o estado atual como replay; o HTTP mapeia para 202.
		return Result{TransactionID: final.ID(), State: final.State(), IdempotentReplay: true}, nil
	}
	if final.State() == wager.StateRejected {
		return Result{TransactionID: final.ID(), State: final.State(),
			FailureCode: final.FailureCode(), IdempotentReplay: true}, nil
	}
	b := final.ResultBalance()
	return Result{TransactionID: final.ID(), State: final.State(), Balance: &b, IdempotentReplay: true}, nil
}

// apply executa a movimentação para uma operação nova (claim próprio),
// seguindo a ordem de locks G3: claim de idempotência → lock da carteira.
func (s *Service) apply(ctx context.Context, uow storage.UnitOfWork, tx wager.WagerTransaction, in Input) (Result, error) {
	wal, err := uow.Wallets().LockForUpdate(ctx, in.WalletID)
	if err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			return Result{}, ErrWalletNotFound
		}
		return Result{}, err
	}
	if string(wal.Currency()) != string(tx.Amount().Currency()) {
		return Result{}, ErrWalletCurrencyMismatch
	}

	before := wal.Balance()
	var rejection wager.FailureCode
	var moveDir ledger.Direction
	switch in.Kind {
	case wager.KindLoss:
		if err := tx.MarkProcessed(before); err != nil {
			return Result{}, err
		}
	case wager.KindWin:
		after, err := wal.Credit(tx.Amount())
		if err != nil {
			return Result{}, err
		}
		if err := tx.MarkProcessed(after); err != nil {
			return Result{}, err
		}
		moveDir = ledger.DirectionCredit
		if err := uow.Ledger().Insert(ctx, mustLedger(s.newID(), wal.ID(), tx.ID(),
			moveDir, tx.Amount(), before, after)); err != nil {
			return Result{}, err
		}
	case wager.KindBet:
		after, err := wal.Debit(tx.Amount())
		if err != nil {
			if errors.Is(err, wallet.ErrInsufficientFunds) {
				rejection = wager.FailureInsufficientFunds
				break
			}
			return Result{}, err
		}
		if err := tx.MarkProcessed(after); err != nil {
			return Result{}, err
		}
		moveDir = ledger.DirectionDebit
		if err := uow.Ledger().Insert(ctx, mustLedger(s.newID(), wal.ID(), tx.ID(),
			moveDir, tx.Amount(), before, after)); err != nil {
			return Result{}, err
		}
	case wager.KindRefund, wager.KindRollback:
		dir, code, err := s.applyReversal(ctx, uow, &tx, &wal, before)
		if err != nil {
			return Result{}, err
		}
		if code != "" {
			rejection = code
			break
		}
		moveDir = dir
	default:
		return Result{}, ErrUnsupportedKind
	}

	if rejection != "" {
		if err := tx.Reject(rejection); err != nil {
			return Result{}, err
		}
	}

	if tx.State() == wager.StateRejected {
		if err := uow.Wagers().UpdateTerminal(ctx, tx); err != nil {
			return Result{}, err
		}
		env, err := events.NewWagerTransactionRejected(s.newID(), tx.ID(), "",
			events.WagerTransactionRejectedData{
				TransactionID:         tx.ID(),
				ProviderID:            tx.ProviderID(),
				ExternalTransactionID: tx.ExternalTransactionID(),
				Kind:                  tx.Kind(),
				FailureCode:           tx.FailureCode(),
			})
		if err != nil {
			return Result{}, err
		}
		if err := uow.Outbox().Insert(ctx, env); err != nil {
			return Result{}, err
		}
		if err := uow.Commit(ctx); err != nil {
			return Result{}, err
		}
		return Result{TransactionID: tx.ID(), State: wager.StateRejected,
			FailureCode: tx.FailureCode()}, nil
	}

	if err := uow.Wagers().UpdateTerminal(ctx, tx); err != nil {
		return Result{}, err
	}
	env, err := events.NewWagerTransactionProcessed(s.newID(), tx.ID(), "",
		events.WagerTransactionProcessedData{
			TransactionID:         tx.ID(),
			Origin:                tx.Origin(),
			ProviderID:            tx.ProviderID(),
			ExternalTransactionID: tx.ExternalTransactionID(),
			Kind:                  tx.Kind(),
			Money:                 tx.Amount(),
			Balance:               tx.ResultBalance(),
		})
	if err != nil {
		return Result{}, err
	}
	if err := uow.Outbox().Insert(ctx, env); err != nil {
		return Result{}, err
	}

	if in.Kind != wager.KindLoss && tx.State() != wager.StateRejected {
		if err := uow.Wallets().UpdateBalance(ctx, wal); err != nil {
			return Result{}, err
		}
		balEnv, err := events.NewWalletBalanceChanged(s.newID(), tx.ID(), "",
			events.WalletBalanceChangedData{
				WalletID:      wal.ID(),
				TransactionID: tx.ID(),
				Direction:     string(moveDir),
				Money:         tx.Amount(),
				BalanceBefore: before,
				BalanceAfter:  wal.Balance(),
				WalletVersion: wal.Version(),
			})
		if err != nil {
			return Result{}, err
		}
		if err := uow.Outbox().Insert(ctx, balEnv); err != nil {
			return Result{}, err
		}
	}

	if err := uow.Commit(ctx); err != nil {
		return Result{}, err
	}
	b := tx.ResultBalance()
	return Result{TransactionID: tx.ID(), State: wager.StateProcessed, Balance: &b}, nil
}

func (s *Service) applyReversal(ctx context.Context, uow storage.UnitOfWork, tx *wager.WagerTransaction,
	wal *wallet.Wallet, before money.Money) (ledger.Direction, wager.FailureCode, error) {
	target, err := uow.Wagers().GetByReferenceExternal(ctx, tx.ProviderID(), tx.ReferenceExternalTransactionID())
	if err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			return "", "", ErrReferenceUnresolved
		}
		return "", "", err
	}
	// Alvo ainda não terminal: permanece pendente (PENDING_REFERENCE em 3.4).
	if !target.IsTerminal() {
		return "", "", ErrReferenceUnresolved
	}

	// Uma única reversão processada por alvo (índice parcial no banco) — a
	// checagem aqui devolve a rejeição de negócio correta antes de movimentar.
	if rev, rerr := uow.Wagers().GetReversalForReference(ctx, target.ID()); rerr == nil && rev.State() == wager.StateProcessed {
		return "", wager.FailureAlreadyReversed, nil
	} else if rerr != nil && !errors.Is(rerr, postgres.ErrNotFound) {
		return "", "", rerr
	}

	mv, rerr := wager.ResolveReversal(*tx, target)
	if rerr != nil {
		switch {
		case errors.Is(rerr, wager.ErrReferenceNotProcessed):
			return "", wager.FailureReferenceNotProcessed, nil
		case errors.Is(rerr, wager.ErrReferenceMismatch):
			return "", wager.FailureReferenceMismatch, nil
		case errors.Is(rerr, wager.ErrInvalidReferenceKind):
			return "", wager.FailureInvalidReferenceKind, nil
		default:
			return "", "", rerr
		}
	}

	after, err := applyMovement(wal, mv)
	if err != nil {
		if errors.Is(err, wallet.ErrInsufficientFunds) {
			return "", wager.FailureReversalInsufficientFunds, nil
		}
		return "", "", err
	}
	if err := tx.ResolveReference(target.ID()); err != nil {
		return "", "", err
	}
	if err := tx.MarkProcessed(after); err != nil {
		return "", "", err
	}
	if err := uow.Ledger().Insert(ctx, mustLedger(s.newID(), wal.ID(), tx.ID(),
		mv.Direction, mv.Amount, before, after)); err != nil {
		return "", "", err
	}
	return mv.Direction, "", nil
}

// applyMovement executa o movimento calculado (débito ou crédito) na carteira.
func applyMovement(w *wallet.Wallet, mv wager.Movement) (money.Money, error) {
	switch mv.Direction {
	case ledger.DirectionCredit:
		return w.Credit(mv.Amount)
	case ledger.DirectionDebit:
		return w.Debit(mv.Amount)
	default:
		return money.Money{}, ledger.ErrInvalidDirection
	}
}

// mustLedger constrói o lançamento do ledger; erros aqui são invariantes do
// processo (valores e direções validados antes do commit).
func mustLedger(id, walletID, txID string, dir ledger.Direction, amount, before, after money.Money) ledger.Entry {
	e, err := ledger.New(id, walletID, txID, dir, amount, before, after, time.Now().UTC())
	if err != nil {
		panic(fmt.Errorf("processwager: lançamento inválido: %w", err))
	}
	return e
}

// newID gera um identificador aleatório de 16 bytes (UUID v4 hex).
func newID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Errorf("processwager: gerar id: %w", err))
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return hex.EncodeToString(b)
}
