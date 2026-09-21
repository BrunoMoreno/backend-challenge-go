// Package processwager implementa o processamento síncrono de uma operação do
// provedor (BET/WIN/LOSS/REFUND/ROLLBACK) com persistência da rejeição,
// persistência de PENDING_REFERENCE e idempotência de replay (RF-03, G2).
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
	ErrInvalidAmountForKind   = errors.New("processwager: valor inválido para o kind")
	ErrWalletNotFound         = errors.New("processwager: carteira inexistente")
	ErrWalletCurrencyMismatch = errors.New("processwager: moeda do payload difere da carteira")
	ErrIdempotencyConflict    = errors.New("processwager: chave de idempotência reutilizada com conteúdo diferente")
	ErrExternalConflict       = errors.New("processwager: mesmo (provider, externalTransactionId) com outra chave")
	// ErrStaleClaim indica que o dono de um claim concorrente desfez a
	// transação; o Process repete o ciclo com uma transação nova.
	ErrStaleClaim = errors.New("processwager: claim concorrente desfeito, reprocessar")
	// ErrPendingReference indica que a referência de uma linha PENDING_REFERENCE
	// ainda não está disponível (alvo ausente ou não-terminal). O worker de
	// referências (M6) usa o erro para reagendar a tentativa com backoff.
	ErrPendingReference = errors.New("processwager: referência ainda não disponível")
	// ErrGenerateID é falha transitória de entropia ao criar identificadores.
	ErrGenerateID = errors.New("processwager: gerar identificador")
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
	newID func() (string, error)
}

// NewService cria o caso de uso com o banco da aplicação.
func NewService(db storage.Database) *Service {
	return &Service{db: db, newID: newID}
}

// NewServiceWithIDs permite injetar o gerador de ids (testes determinísticos).
func NewServiceWithIDs(db storage.Database, newID func() string) *Service {
	return &Service{db: db, newID: func() (string, error) { return newID(), nil }}
}

// Process executa a operação abrindo a própria transação (canal HTTP). Corridas
// de claim (outro processador desfez a transação) são refeitas com limite de
// tentativas.
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
		uow, err := s.db.Begin(ctx)
		if err != nil {
			return Result{}, err
		}
		res, dirty, err := s.processOne(ctx, uow, in, hash)
		if errors.Is(err, ErrStaleClaim) || errors.Is(err, postgres.ErrDeadlock) {
			// Deadlock (40P01): o Postgres aborta a transação perdedora; a
			// operação foi totalmente revertida e pode ser reexecutada. Na
			// corrida de duas BETs (M3.6) o retry encontra o saldo já movido
			// e rejeita com INSUFFICIENT_FUNDS — o desfecho esperado.
			_ = uow.Rollback(ctx)
			continue
		}
		if err != nil {
			_ = uow.Rollback(ctx)
			return Result{}, err
		}
		if dirty {
			if cerr := uow.Commit(ctx); cerr != nil {
				if errors.Is(cerr, postgres.ErrDeadlock) {
					_ = uow.Rollback(ctx)
					continue
				}
				return Result{}, cerr
			}
		} else {
			_ = uow.Rollback(ctx)
		}
		return res, nil
	}
	return Result{}, fmt.Errorf("%w: limite de tentativas", ErrStaleClaim)
}

// ProcessOn executa a operação sobre um UnitOfWork já aberto, sem abrir nem
// cometer transação: o consumidor SQS compartilha a mesma transação SQL da
// inbox com o caso de uso (MESSAGING §3). O chamador decide Commit/Rollback e
// reprocessa ErrStaleClaim em uma transação nova. O replay (`IdempotentReplay`)
// também é devolvido sem escrever nada além do que o chamador fizer.
func (s *Service) ProcessOn(ctx context.Context, uow storage.UnitOfWork, in Input) (Result, error) {
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
	res, _, err := s.processOne(ctx, uow, in, hash)
	return res, err
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
	if in.Kind == wager.KindLoss {
		if !in.Amount.IsZero() {
			return ErrInvalidAmountForKind
		}
	} else if in.Amount.IsZero() {
		return ErrInvalidAmountForKind
	}
	return nil
}

// processOne executa uma tentativa da operação sobre uma transação já aberta.
// Não abre nem desfaz a transação — o chamador decide o destino. O booleano
// retornado (`dirty`) indica se houve escrita a commit: `true` em paths de
// movimentação/rejeição/pendência; `false` em replays (leitura) e conflitos.
// Retorna ErrStaleClaim quando o claim concorrente com a mesma chave foi
// desfeito (rollback) e a operação precisa ser reexecutada do zero.
func (s *Service) processOne(ctx context.Context, uow storage.UnitOfWork, in Input, hash string) (Result, bool, error) {
	id, err := s.nextID()
	if err != nil {
		return Result{}, false, err
	}
	tx, err := wager.NewExternal(id, in.ProviderID, in.ExternalTransactionID,
		in.IdempotencyKey, hash, in.WalletID, in.PlayerID, in.RoundID, in.GameID,
		in.Kind, in.Amount, in.ReferenceExternalTransactionID)
	if err != nil {
		return Result{}, false, err
	}

	inserted, err := uow.Wagers().InsertIfAbsent(ctx, tx)
	if err != nil {
		// O 23505 aborta a transação atual; a confirmação usa outra (conflict).
		res, cerr := s.conflict(ctx, in, hash, err)
		return res, false, cerr
	}
	if !inserted {
		res, rerr := s.replayByKey(ctx, uow, in, hash)
		return res, false, rerr
	}
	res, aerr := s.apply(ctx, uow, tx, in)
	return res, aerr == nil, aerr
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
// A resolução da referência decide entre pendência, rejeição imediata ou
// movimentação (applyResolved).
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

	// Resolução da referência (reversões e WIN com referência): alvo ausente
	// ou ainda não terminal → PENDING_REFERENCE persistido; alvo REJECTED/FAILED
	// → rejeição imediata REFERENCE_NOT_PROCESSED (ARCHITECTURE §6).
	var target wager.WagerTransaction
	var pending bool
	var rej wager.FailureCode
	var rerr error
	if in.ReferenceExternalTransactionID != "" {
		target, pending, rej, rerr = resolveReference(ctx, uow, in.ProviderID, in.ReferenceExternalTransactionID)
		if rerr != nil {
			return Result{}, rerr
		}
		if pending {
			return s.pendReference(ctx, uow, &tx)
		}
		if rej != "" {
			return s.reject(ctx, uow, &tx, rej)
		}
	}

	return s.applyResolved(ctx, uow, wal, tx, &target, in)
}

// applyResolved executa a movimentação financeira de uma transação cuja
// referência já foi resolvida (ou que não tem referência): aplica o movimento
// na carteira, grava o ledger, persiste o terminal e emite os eventos.
// Reutilizado pelo caminho síncrono e pelo worker de referências (M6).
func (s *Service) applyResolved(ctx context.Context, uow storage.UnitOfWork, wal wallet.Wallet,
	tx wager.WagerTransaction, target *wager.WagerTransaction, in Input) (Result, error) {
	before := wal.Balance()
	var rejection wager.FailureCode
	var moveDir ledger.Direction
	switch in.Kind {
	case wager.KindLoss:
		if err := tx.MarkProcessed(before); err != nil {
			return Result{}, err
		}
	case wager.KindWin:
		if in.ReferenceExternalTransactionID != "" {
			if err := wager.ValidateWinReference(tx, *target); err != nil {
				switch {
				case errors.Is(err, wager.ErrReferenceMismatch):
					return s.reject(ctx, uow, &tx, wager.FailureReferenceMismatch)
				case errors.Is(err, wager.ErrInvalidReferenceKind):
					return s.reject(ctx, uow, &tx, wager.FailureInvalidReferenceKind)
				default:
					return Result{}, err
				}
			}
			if err := tx.ResolveReference(target.ID()); err != nil {
				return Result{}, err
			}
		}
		after, err := wal.Credit(tx.Amount())
		if err != nil {
			return Result{}, err
		}
		if err := tx.MarkProcessed(after); err != nil {
			return Result{}, err
		}
		moveDir = ledger.DirectionCredit
		if err := s.insertLedger(ctx, uow, wal.ID(), tx.ID(),
			moveDir, tx.Amount(), before, after); err != nil {
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
		if err := s.insertLedger(ctx, uow, wal.ID(), tx.ID(),
			moveDir, tx.Amount(), before, after); err != nil {
			return Result{}, err
		}
	case wager.KindRefund, wager.KindRollback:
		// Alvo PROCESSED garantido na resolução acima. Slot único de reversão:
		// uma reversão processada por alvo (índice parcial no banco).
		if rev, rerr := uow.Wagers().GetReversalForReference(ctx, target.ID()); rerr == nil && rev.State() == wager.StateProcessed {
			return s.reject(ctx, uow, &tx, wager.FailureAlreadyReversed)
		} else if rerr != nil && !errors.Is(rerr, postgres.ErrNotFound) {
			return Result{}, rerr
		}
		mv, rerr := wager.ResolveReversal(tx, *target)
		if rerr != nil {
			switch {
			case errors.Is(rerr, wager.ErrReferenceMismatch):
				return s.reject(ctx, uow, &tx, wager.FailureReferenceMismatch)
			case errors.Is(rerr, wager.ErrInvalidReferenceKind):
				return s.reject(ctx, uow, &tx, wager.FailureInvalidReferenceKind)
			default:
				return Result{}, rerr
			}
		}
		after, err := applyMovement(&wal, mv)
		if err != nil {
			if errors.Is(err, wallet.ErrInsufficientFunds) {
				return s.reject(ctx, uow, &tx, wager.FailureReversalInsufficientFunds)
			}
			return Result{}, err
		}
		if err := tx.ResolveReference(target.ID()); err != nil {
			return Result{}, err
		}
		if err := tx.MarkProcessed(after); err != nil {
			return Result{}, err
		}
		moveDir = mv.Direction
		if err := s.insertLedger(ctx, uow, wal.ID(), tx.ID(),
			moveDir, mv.Amount, before, after); err != nil {
			return Result{}, err
		}
	default:
		return Result{}, ErrUnsupportedKind
	}

	if rejection != "" {
		return s.reject(ctx, uow, &tx, rejection)
	}

	if err := uow.Wagers().UpdateTerminal(ctx, tx); err != nil {
		return Result{}, err
	}
	eventID, err := s.nextID()
	if err != nil {
		return Result{}, err
	}
	env, err := events.NewWagerTransactionProcessed(eventID, tx.ID(), "",
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
		balEventID, err := s.nextID()
		if err != nil {
			return Result{}, err
		}
		balEnv, err := events.NewWalletBalanceChanged(balEventID, tx.ID(), "",
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

	b := tx.ResultBalance()
	return Result{TransactionID: tx.ID(), State: wager.StateProcessed, Balance: &b}, nil
}

// ResolvePending tenta resolver a referência de uma transação PENDING_REFERENCE
// (ou PENDING retomada pelo varredor M6.3): re-classifica o alvo pela
// referência externa e, quando terminal e PROCESSED, aplica a movimentação, o
// ledger e os eventos — o mesmo caminho do processo síncrono. Alvo ainda
// indisponível → ErrPendingReference (o worker agenda o retry com backoff).
// Não abre nem comita transação: o chamador (worker 6.1) decide o destino.
func (s *Service) ResolvePending(ctx context.Context, uow storage.UnitOfWork, pending wager.WagerTransaction) (Result, error) {
	switch pending.State() {
	case wager.StatePendingReference, wager.StatePending:
	default:
		return Result{}, fmt.Errorf("processwager: ResolvePending: estado %s", pending.State())
	}
	if pending.Kind() == wager.KindOpening {
		return Result{}, fmt.Errorf("processwager: ResolvePending: OPENING não tem referência")
	}
	in := inputOf(pending)

	wal, err := uow.Wallets().LockForUpdate(ctx, pending.WalletID())
	if err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			return Result{}, ErrWalletNotFound
		}
		return Result{}, err
	}
	if string(wal.Currency()) != string(pending.Amount().Currency()) {
		return Result{}, ErrWalletCurrencyMismatch
	}

	var target wager.WagerTransaction
	if in.ReferenceExternalTransactionID != "" {
		t, still, rej, rerr := resolveReference(ctx, uow, in.ProviderID, in.ReferenceExternalTransactionID)
		if rerr != nil {
			return Result{}, rerr
		}
		if still {
			return Result{}, ErrPendingReference
		}
		if rej != "" {
			return s.reject(ctx, uow, &pending, rej)
		}
		target = t
	}
	return s.applyResolved(ctx, uow, wal, pending, &target, in)
}

// RejectPending aplica a rejeição definitiva a uma transação ainda não-terminal
// (ex.: REFERENCE_NOT_FOUND por TTL/limite de tentativas no worker de
// referências), persistindo o desfecho e o evento de rejeição na outbox.
// Não abre nem comita transação — o chamador decide o destino.
func (s *Service) RejectPending(ctx context.Context, uow storage.UnitOfWork, pending wager.WagerTransaction, code wager.FailureCode) (Result, error) {
	return s.reject(ctx, uow, &pending, code)
}

// inputOf reconstrói o Input de uma transação já persistida (rehidratação para
// a resolução tardia da referência). Sem revalidação: a linha veio do banco
// como operação externa válida.
func inputOf(p wager.WagerTransaction) Input {
	return Input{
		ProviderID:                     p.ProviderID(),
		ExternalTransactionID:          p.ExternalTransactionID(),
		PlayerID:                       p.PlayerID(),
		WalletID:                       p.WalletID(),
		RoundID:                        p.RoundID(),
		GameID:                         p.GameID(),
		Kind:                           p.Kind(),
		Amount:                         p.Amount(),
		ReferenceExternalTransactionID: p.ReferenceExternalTransactionID(),
		IdempotencyKey:                 p.IdempotencyKey(),
	}
}

// resolveReference classifica o alvo de uma referência (ARCHITECTURE §6):
// referência inexistente ou alvo ainda não-terminal → pending, para
// PENDING_REFERENCE; alvo terminal REJECTED/FAILED → rej = REFERENCE_NOT_PROCESSED;
// alvo PROCESSED → devolvido em target para validação e movimentação.
func resolveReference(ctx context.Context, uow storage.UnitOfWork, providerID, referenceExternalID string) (wager.WagerTransaction, bool, wager.FailureCode, error) {
	target, err := uow.Wagers().GetByReferenceExternal(ctx, providerID, referenceExternalID)
	if err != nil {
		if errors.Is(err, postgres.ErrNotFound) {
			return wager.WagerTransaction{}, true, "", nil
		}
		return wager.WagerTransaction{}, false, "", err
	}
	if !target.IsTerminal() {
		return wager.WagerTransaction{}, true, "", nil
	}
	if target.State() != wager.StateProcessed {
		return wager.WagerTransaction{}, false, wager.FailureReferenceNotProcessed, nil
	}
	return target, false, "", nil
}

// pendReference persiste PENDING_REFERENCE (mantendo o claim de idempotência,
// sem movimentar saldo/ledger) e o evento de espera. Devolve o Result do 202;
// a resolução posterior fica a cargo do worker de referências (M6). Não faz
// commit: a transação é concluída pelo chamador (Process/ProcessOn).
func (s *Service) pendReference(ctx context.Context, uow storage.UnitOfWork, tx *wager.WagerTransaction) (Result, error) {
	if err := tx.PendForReference(); err != nil {
		return Result{}, err
	}
	if err := uow.Wagers().UpdatePendingReference(ctx, *tx); err != nil {
		return Result{}, err
	}
	eventID, err := s.nextID()
	if err != nil {
		return Result{}, err
	}
	env, err := events.NewWagerTransactionPendingReference(eventID, tx.ID(), "",
		events.WagerTransactionPendingReferenceData{
			TransactionID:                  tx.ID(),
			ProviderID:                     tx.ProviderID(),
			ExternalTransactionID:          tx.ExternalTransactionID(),
			ReferenceExternalTransactionID: tx.ReferenceExternalTransactionID(),
		})
	if err != nil {
		return Result{}, err
	}
	if err := uow.Outbox().Insert(ctx, env); err != nil {
		return Result{}, err
	}
	return Result{TransactionID: tx.ID(), State: wager.StatePendingReference}, nil
}

// reject persiste a rejeição definitiva e o evento correspondente. Não faz
// commit: a transação é concluída pelo chamador (Process/ProcessOn).
func (s *Service) reject(ctx context.Context, uow storage.UnitOfWork, tx *wager.WagerTransaction, code wager.FailureCode) (Result, error) {
	if err := tx.Reject(code); err != nil {
		return Result{}, err
	}
	if err := uow.Wagers().UpdateTerminal(ctx, *tx); err != nil {
		return Result{}, err
	}
	eventID, err := s.nextID()
	if err != nil {
		return Result{}, err
	}
	env, err := events.NewWagerTransactionRejected(eventID, tx.ID(), "",
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
	return Result{TransactionID: tx.ID(), State: wager.StateRejected, FailureCode: code}, nil
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

func (s *Service) insertLedger(ctx context.Context, uow storage.UnitOfWork, walletID, txID string,
	dir ledger.Direction, amount, before, after money.Money) error {
	id, err := s.nextID()
	if err != nil {
		return err
	}
	e, err := ledger.New(id, walletID, txID, dir, amount, before, after, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("processwager: lançamento inválido: %w", err)
	}
	return uow.Ledger().Insert(ctx, e)
}

func (s *Service) nextID() (string, error) {
	id, err := s.newID()
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrGenerateID, err)
	}
	return id, nil
}

// newID gera um identificador aleatório de 16 bytes (UUID v4 hex).
func newID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return hex.EncodeToString(b), nil
}
