package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/app/openwallet"
	"github.com/BrunoMoreno/backend-challenge-go/internal/app/processwager"
	"github.com/BrunoMoreno/backend-challenge-go/internal/app/query"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wager"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wallet"
)

const (
	defaultLedgerLimit = 50
	maxLedgerLimit     = 200
	// maxBodyBytes limita o corpo de POST a 1 MiB.
	maxBodyBytes = 1 << 20
)

// Deps agrupa os casos de uso e o verificador de tokens usados pelas rotas de
// negócio. Campos nil desativam as rotas correspondentes.
type Deps struct {
	Verifier Verifier
	// Wallets executa POST /wallets.
	Wallets WalletService
	// Wagers executa POST /wagering/transactions.
	Wagers WagerService
	// Queries atende as consultas de carteira, extrato e transações.
	Queries QueryService
	// Ready é o probe de prontidão (PostgreSQL, e SQS a partir do M7).
	Ready func(ctx context.Context) error
}

// WalletService é a porta de abertura de carteiras exigida pelo HTTP.
type WalletService interface {
	Open(ctx context.Context, in openwallet.Input) (openwallet.Result, error)
}

// WagerService é a porta de processamento de operações exigida pelo HTTP.
type WagerService interface {
	Process(ctx context.Context, in processwager.Input) (processwager.Result, error)
}

// QueryService é a porta de consultas exigida pelo HTTP.
type QueryService interface {
	Wallet(ctx context.Context, id string) (wallet.Wallet, error)
	Ledger(ctx context.Context, walletID, cursor string, limit int) (query.LedgerPage, error)
	Transaction(ctx context.Context, id string) (wager.WagerTransaction, error)
	ProviderTransaction(ctx context.Context, providerID, externalID string) (wager.WagerTransaction, error)
}

// api agrupa os handlers.
type api struct {
	deps Deps
}

// --- DTOs de resposta ---

type walletDTO struct {
	ID       string      `json:"id"`
	PlayerID string      `json:"playerId"`
	Balance  money.Money `json:"balance"`
	Version  int64       `json:"version"`
}

type ledgerEntryDTO struct {
	ID            string      `json:"id"`
	TransactionID string      `json:"transactionId"`
	Direction     string      `json:"direction"`
	Money         money.Money `json:"money"`
	BalanceBefore money.Money `json:"balanceBefore"`
	BalanceAfter  money.Money `json:"balanceAfter"`
	CreatedAt     time.Time   `json:"createdAt"`
}

type ledgerPageDTO struct {
	Items      []ledgerEntryDTO `json:"items"`
	NextCursor *string          `json:"nextCursor"`
}

type processResultDTO struct {
	TransactionID    string       `json:"transactionId"`
	Status           string       `json:"status"`
	Balance          *money.Money `json:"balance,omitempty"`
	FailureCode      string       `json:"failureCode,omitempty"`
	IdempotentReplay bool         `json:"idempotentReplay"`
}

type transactionDTO struct {
	TransactionID                  string       `json:"transactionId"`
	ProviderID                     string       `json:"providerId,omitempty"`
	ExternalTransactionID          string       `json:"externalTransactionId,omitempty"`
	Kind                           string       `json:"kind"`
	Status                         string       `json:"status"`
	FailureCode                    string       `json:"failureCode,omitempty"`
	Money                          money.Money  `json:"money"`
	ReferenceExternalTransactionID string       `json:"referenceExternalTransactionId,omitempty"`
	ResolvedReferenceTransactionID string       `json:"resolvedReferenceTransactionId,omitempty"`
	Attempts                       int          `json:"attempts"`
	NextAttemptAt                  *time.Time   `json:"nextAttemptAt,omitempty"`
	Balance                        *money.Money `json:"balance,omitempty"`
	CreatedAt                      time.Time    `json:"createdAt"`
	UpdatedAt                      time.Time    `json:"updatedAt"`
}

func newTransactionDTO(t wager.WagerTransaction) transactionDTO {
	dto := transactionDTO{
		TransactionID:                  t.ID(),
		ProviderID:                     t.ProviderID(),
		ExternalTransactionID:          t.ExternalTransactionID(),
		Kind:                           string(t.Kind()),
		Status:                         string(t.State()),
		FailureCode:                    string(t.FailureCode()),
		Money:                          t.Amount(),
		ReferenceExternalTransactionID: t.ReferenceExternalTransactionID(),
		ResolvedReferenceTransactionID: t.ResolvedReferenceTransactionID(),
		Attempts:                       t.Attempts(),
		CreatedAt:                      t.CreatedAt(),
		UpdatedAt:                      t.UpdatedAt(),
	}
	if !t.NextAttemptAt().IsZero() {
		next := t.NextAttemptAt()
		dto.NextAttemptAt = &next
	}
	if t.State() == wager.StateProcessed {
		balance := t.ResultBalance()
		dto.Balance = &balance
	}
	return dto
}

// --- Pedidos ---

type openWalletRequest struct {
	PlayerID       string
	InitialBalance money.Money
}

type submitRequest struct {
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

// rawSubmit é o corpo bruto de POST /wagering/transactions: o dinheiro fica
// como json.RawMessage para mapear formatos inválidos para INVALID_MONEY /
// INVALID_CURRENCY em vez de INVALID_JSON.
type rawSubmit struct {
	ProviderID                     string          `json:"providerId"`
	ExternalTransactionID          string          `json:"externalTransactionId"`
	PlayerID                       string          `json:"playerId"`
	WalletID                       string          `json:"walletId"`
	RoundID                        string          `json:"roundId"`
	GameID                         string          `json:"gameId"`
	Kind                           string          `json:"kind"`
	Amount                         json.RawMessage `json:"amount"`
	ReferenceExternalTransactionID string          `json:"referenceExternalTransactionId"`
}

// --- Handlers ---

// openWallet implementa POST /wallets.
func (a *api) openWallet(w http.ResponseWriter, r *http.Request) {
	var raw struct {
		PlayerID       string          `json:"playerId"`
		InitialBalance json.RawMessage `json:"initialBalance"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&raw); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_JSON", "corpo inválido")
		return
	}
	if strings.TrimSpace(raw.PlayerID) == "" {
		writeError(w, http.StatusBadRequest, "INVALID_FIELD", "playerId é obrigatório")
		return
	}
	balance, code := decodeMoney(raw.InitialBalance)
	if code != "" {
		writeError(w, http.StatusBadRequest, code, "initialBalance inválido")
		return
	}

	res, err := a.deps.Wallets.Open(r.Context(), openwallet.Input{
		PlayerID:       raw.PlayerID,
		InitialBalance: balance,
	})
	if err != nil {
		a.failOpenWallet(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, walletDTO{
		ID:       res.Wallet.ID(),
		PlayerID: res.Wallet.PlayerID(),
		Balance:  res.Wallet.Balance(),
		Version:  res.Wallet.Version(),
	})
}

// getWallet implementa GET /wallets/:walletId.
func (a *api) getWallet(w http.ResponseWriter, r *http.Request) {
	wlt, err := a.deps.Queries.Wallet(r.Context(), r.PathValue("walletId"))
	if err != nil {
		a.failQuery(w, err)
		return
	}
	writeJSON(w, http.StatusOK, walletDTO{
		ID:       wlt.ID(),
		PlayerID: wlt.PlayerID(),
		Balance:  wlt.Balance(),
		Version:  wlt.Version(),
	})
}

// ledger implementa GET /wallets/:walletId/ledger.
func (a *api) ledger(w http.ResponseWriter, r *http.Request) {
	limit := defaultLedgerLimit
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > maxLedgerLimit {
			writeError(w, http.StatusBadRequest, "INVALID_FIELD", "limit deve estar entre 1 e 200")
			return
		}
		limit = n
	}

	page, err := a.deps.Queries.Ledger(r.Context(), r.PathValue("walletId"),
		r.URL.Query().Get("cursor"), limit)
	if err != nil {
		a.failQuery(w, err)
		return
	}

	out := ledgerPageDTO{Items: make([]ledgerEntryDTO, 0, len(page.Entries))}
	for _, e := range page.Entries {
		out.Items = append(out.Items, ledgerEntryDTO{
			ID:            e.ID(),
			TransactionID: e.TransactionID(),
			Direction:     string(e.Direction()),
			Money:         e.Amount(),
			BalanceBefore: e.BalanceBefore(),
			BalanceAfter:  e.BalanceAfter(),
			CreatedAt:     e.CreatedAt(),
		})
	}
	if page.NextCursor != "" {
		next := page.NextCursor
		out.NextCursor = &next
	}
	writeJSON(w, http.StatusOK, out)
}

// submit implementa POST /wagering/transactions.
func (a *api) submit(w http.ResponseWriter, r *http.Request) {
	id, _ := IdentityFromContext(r.Context())
	var raw rawSubmit
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(&raw); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_JSON", "corpo inválido")
		return
	}

	req, code := a.parseSubmit(id, raw, r.Header.Get("Idempotency-Key"))
	if code == "FORBIDDEN" {
		writeError(w, http.StatusForbidden, code, "providerId não corresponde à identidade")
		return
	}
	if code != "" {
		writeError(w, http.StatusBadRequest, code, "entrada inválida")
		return
	}
	res, err := a.deps.Wagers.Process(r.Context(), processwager.Input{
		ProviderID:                     req.ProviderID,
		ExternalTransactionID:          req.ExternalTransactionID,
		PlayerID:                       req.PlayerID,
		WalletID:                       req.WalletID,
		RoundID:                        req.RoundID,
		GameID:                         req.GameID,
		Kind:                           req.Kind,
		Amount:                         req.Amount,
		ReferenceExternalTransactionID: req.ReferenceExternalTransactionID,
		IdempotencyKey:                 req.IdempotencyKey,
	})
	if err != nil {
		a.failSubmit(w, err)
		return
	}
	a.writeProcessResult(w, res)
}

// getTransaction implementa GET /wagering/transactions/:transactionId.
func (a *api) getTransaction(w http.ResponseWriter, r *http.Request) {
	id, _ := IdentityFromContext(r.Context())
	t, err := a.deps.Queries.Transaction(r.Context(), r.PathValue("transactionId"))
	if err != nil {
		a.failQuery(w, err)
		return
	}
	// Provedor só vê as próprias transações; divergência é 404 (não vaza
	// a existência de transações alheias).
	if !id.HasRole(RoleWageringInternal) && t.ProviderID() != id.ProviderID {
		writeError(w, http.StatusNotFound, "TRANSACTION_NOT_FOUND", "transação não encontrada")
		return
	}
	writeJSON(w, http.StatusOK, newTransactionDTO(t))
}

// getProviderTransaction implementa GET /providers/:providerId/wagering/transactions/:externalTransactionId.
func (a *api) getProviderTransaction(w http.ResponseWriter, r *http.Request) {
	t, err := a.deps.Queries.ProviderTransaction(r.Context(),
		r.PathValue("providerId"), r.PathValue("externalTransactionId"))
	if err != nil {
		a.failQuery(w, err)
		return
	}
	writeJSON(w, http.StatusOK, newTransactionDTO(t))
}

// parseSubmit valida o corpo de POST /wagering/transactions contra a matriz de
// autorização e as regras de entrada (§2/§5 da API). Devolve o `error.code` de
// entrada quando a requisição deve ser rejeitada com 400.
func (a *api) parseSubmit(id Identity, raw rawSubmit, idempotencyKey string) (submitRequest, string) {
	// Autorização: envio em nome do provedor exige provider_id da identidade.
	if !id.CanSubmitAsProvider(raw.ProviderID) {
		return submitRequest{}, "FORBIDDEN"
	}
	if strings.TrimSpace(idempotencyKey) == "" {
		return submitRequest{}, "MISSING_IDEMPOTENCY_KEY"
	}
	if strings.TrimSpace(raw.ProviderID) == "" ||
		strings.TrimSpace(raw.ExternalTransactionID) == "" ||
		strings.TrimSpace(raw.PlayerID) == "" ||
		strings.TrimSpace(raw.WalletID) == "" {
		return submitRequest{}, "INVALID_FIELD"
	}
	kind := wager.Kind(raw.Kind)
	if kind == wager.KindOpening {
		return submitRequest{}, "OPENING_NOT_ALLOWED"
	}
	switch kind {
	case wager.KindBet, wager.KindWin, wager.KindLoss, wager.KindRefund, wager.KindRollback:
	default:
		return submitRequest{}, "UNKNOWN_KIND"
	}
	amount, code := decodeMoney(raw.Amount)
	if code != "" {
		return submitRequest{}, code
	}
	return submitRequest{
		ProviderID:                     raw.ProviderID,
		ExternalTransactionID:          raw.ExternalTransactionID,
		PlayerID:                       raw.PlayerID,
		WalletID:                       raw.WalletID,
		RoundID:                        raw.RoundID,
		GameID:                         raw.GameID,
		Kind:                           kind,
		Amount:                         amount,
		ReferenceExternalTransactionID: raw.ReferenceExternalTransactionID,
		IdempotencyKey:                 idempotencyKey,
	}, ""
}

// decodeMoney interpreta o JSON de dinheiro devolvendo o `error.code` de
// entrada em falha ("" quando ok).
func decodeMoney(raw json.RawMessage) (money.Money, string) {
	var m money.Money
	if len(raw) == 0 {
		return m, "INVALID_MONEY"
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		if errors.Is(err, money.ErrInvalidCurrency) {
			return m, "INVALID_CURRENCY"
		}
		return m, "INVALID_MONEY"
	}
	return m, ""
}
