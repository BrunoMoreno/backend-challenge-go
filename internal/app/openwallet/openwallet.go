// Package openwallet implementa o caso de uso de abertura de carteira (RF-01):
// criar a carteira e, quando o saldo inicial é positivo, persistir no mesmo
// commit o OPENING PROCESSED, o crédito no ledger e os dois eventos da outbox.
package openwallet

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
)

// Erros de validação do caso de uso (antes de tocar no banco).
var (
	ErrMissingPlayer   = errors.New("openwallet: playerId ausente")
	ErrMissingCurrency = errors.New("openwallet: moeda ausente")
	ErrNegativeBalance = errors.New("openwallet: saldo inicial negativo")
)

// Input é o comando de abertura de carteira.
type Input struct {
	PlayerID       string
	InitialBalance money.Money
}

// Service abre carteiras dentro de uma transação atômica.
type Service struct {
	db    storage.Database
	newID func() string
}

// NewService cria o caso de uso com o banco de dados da aplicação e gerador de
// identificadores aleatórios.
func NewService(db storage.Database) *Service {
	return &Service{db: db, newID: newID}
}

// NewServiceWithIDs permite injetar o gerador de ids (testes determinísticos).
func NewServiceWithIDs(db storage.Database, newID func() string) *Service {
	return &Service{db: db, newID: newID}
}

// Result descreve o que foi persistido.
type Result struct {
	Wallet wallet.Wallet
	// Opening é nil quando o saldo inicial é 0.00 (nada a registrar).
	Opening *wager.WagerTransaction
}

// Open executa o caso de uso. Erros de banco são propagados como estão
// (ex.: ErrDuplicate de (playerId, currency) da camada de persistência).
func (s *Service) Open(ctx context.Context, in Input) (Result, error) {
	cur := in.InitialBalance.Currency()
	if in.PlayerID == "" {
		return Result{}, ErrMissingPlayer
	}
	if string(cur) == "" {
		return Result{}, ErrMissingCurrency
	}
	if in.InitialBalance.IsNegative() {
		return Result{}, ErrNegativeBalance
	}

	correlationID := s.newID()
	txID := s.newID()

	uow, err := s.db.Begin(ctx)
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = uow.Rollback(ctx) }()

	// Carteira nasce zerada; o saldo inicial entra como OPENING + crédito.
	opened, err := wallet.New(s.newID(), in.PlayerID, money.Zero(cur))
	if err != nil {
		return Result{}, err
	}
	if !in.InitialBalance.IsZero() {
		if _, err := opened.Credit(in.InitialBalance); err != nil {
			return Result{}, err
		}
	}
	if err := uow.Wallets().Insert(ctx, opened); err != nil {
		return Result{}, err
	}

	if in.InitialBalance.IsZero() {
		if err := uow.Commit(ctx); err != nil {
			return Result{}, err
		}
		return Result{Wallet: opened}, nil
	}

	opening, err := wager.NewOpening(txID, opened.ID(), in.PlayerID, in.InitialBalance)
	if err != nil {
		return Result{}, err
	}
	if err := opening.MarkProcessed(in.InitialBalance); err != nil {
		return Result{}, err
	}
	if err := uow.Wagers().Insert(ctx, opening); err != nil {
		return Result{}, err
	}

	before := money.Zero(cur)
	openingLedger, err := ledger.New(s.newID(), opened.ID(), txID, ledger.DirectionCredit,
		in.InitialBalance, before, in.InitialBalance, time.Now().UTC())
	if err != nil {
		return Result{}, err
	}
	if err := uow.Ledger().Insert(ctx, openingLedger); err != nil {
		return Result{}, err
	}

	processedEnv, err := wagerProcessedEvent(s.newID(), correlationID, opening)
	if err != nil {
		return Result{}, err
	}
	if err := uow.Outbox().Insert(ctx, processedEnv); err != nil {
		return Result{}, err
	}
	balanceEnv, err := balanceChangedEvent(s.newID(), correlationID, opened, txID,
		in.InitialBalance, before)
	if err != nil {
		return Result{}, err
	}
	if err := uow.Outbox().Insert(ctx, balanceEnv); err != nil {
		return Result{}, err
	}

	if err := uow.Commit(ctx); err != nil {
		return Result{}, err
	}
	return Result{Wallet: opened, Opening: &opening}, nil
}

func wagerProcessedEvent(eventID, correlationID string, t wager.WagerTransaction) (events.Envelope, error) {
	return events.NewWagerTransactionProcessed(eventID, correlationID, "",
		events.WagerTransactionProcessedData{
			TransactionID: t.ID(),
			Origin:        t.Origin(),
			Kind:          t.Kind(),
			Money:         t.Amount(),
			Balance:       t.ResultBalance(),
		})
}

func balanceChangedEvent(eventID, correlationID string, w wallet.Wallet, txID string,
	amount, before money.Money) (events.Envelope, error) {
	return events.NewWalletBalanceChanged(eventID, correlationID, "",
		events.WalletBalanceChangedData{
			WalletID:      w.ID(),
			TransactionID: txID,
			Direction:     "CREDIT",
			Money:         amount,
			BalanceBefore: before,
			BalanceAfter:  w.Balance(),
			WalletVersion: w.Version(),
		})
}

// newID gera um identificador aleatório de 16 bytes (UUID v4 sem hiphen), usado
// para carteira, OPENING, ledger e eventos dentro do mesmo comando.
func newID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Errorf("openwallet: gerar id: %w", err))
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return hex.EncodeToString(b)
}
