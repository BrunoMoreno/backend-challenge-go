package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/BrunoMoreno/backend-challenge-go/internal/app/openwallet"
	"github.com/BrunoMoreno/backend-challenge-go/internal/app/processwager"
	"github.com/BrunoMoreno/backend-challenge-go/internal/app/query"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wager"
	"github.com/BrunoMoreno/backend-challenge-go/internal/infra/postgres"
)

// writeJSON serializa a resposta com o envelope de sucesso.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// failOpenWallet mapeia os erros de abertura de carteira.
func (a *api) failOpenWallet(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, postgres.ErrDuplicate):
		writeError(w, http.StatusConflict, "WALLET_ALREADY_EXISTS",
			"carteira já existe para (playerId, currency)")
	case errors.Is(err, openwallet.ErrMissingPlayer), errors.Is(err, openwallet.ErrMissingCurrency):
		writeError(w, http.StatusBadRequest, "INVALID_FIELD", err.Error())
	case errors.Is(err, openwallet.ErrNegativeBalance):
		writeError(w, http.StatusBadRequest, "INVALID_MONEY", err.Error())
	default:
		a.failInternal(w, err)
	}
}

// failSubmit mapeia os erros do processamento de operações.
func (a *api) failSubmit(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, processwager.ErrMissingIdempotencyKey):
		writeError(w, http.StatusBadRequest, "MISSING_IDEMPOTENCY_KEY",
			"cabeçalho Idempotency-Key é obrigatório")
	case errors.Is(err, processwager.ErrUnsupportedKind):
		writeError(w, http.StatusBadRequest, "UNKNOWN_KIND", "kind não suportado")
	case errors.Is(err, processwager.ErrInvalidAmountForKind):
		writeError(w, http.StatusBadRequest, "INVALID_AMOUNT_FOR_KIND", "valor inválido para o kind")
	case errors.Is(err, processwager.ErrWalletNotFound):
		writeError(w, http.StatusNotFound, "WALLET_NOT_FOUND", "carteira inexistente")
	case errors.Is(err, processwager.ErrWalletCurrencyMismatch):
		writeError(w, http.StatusBadRequest, "INVALID_CURRENCY", "moeda do payload difere da carteira")
	case errors.Is(err, processwager.ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, "IDEMPOTENCY_KEY_CONFLICT",
			"chave de idempotência reutilizada com conteúdo diferente")
	case errors.Is(err, processwager.ErrExternalConflict):
		writeError(w, http.StatusConflict, "EXTERNAL_TRANSACTION_CONFLICT",
			"mesmo (provider, externalTransactionId) com outra chave")
	case errors.Is(err, processwager.ErrStaleClaim):
		// Concorrência transitória entre processadores: o cliente deve repetir.
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "UNAVAILABLE",
			"outro processador está tratando a operação; repita a requisição")
	default:
		a.failInternal(w, err)
	}
}

// failQuery mapeia os erros das consultas.
func (a *api) failQuery(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, query.ErrWalletNotFound):
		writeError(w, http.StatusNotFound, "WALLET_NOT_FOUND", "carteira inexistente")
	case errors.Is(err, query.ErrTransactionNotFound):
		writeError(w, http.StatusNotFound, "TRANSACTION_NOT_FOUND", "transação inexistente")
	case errors.Is(err, query.ErrInvalidCursor):
		writeError(w, http.StatusBadRequest, "INVALID_CURSOR", "cursor de paginação inválido")
	default:
		a.failInternal(w, err)
	}
}

// writeProcessResult serializa o desfecho síncrono de POST /wagering/transactions.
func (a *api) writeProcessResult(w http.ResponseWriter, res processwager.Result) {
	body := processResultDTO{
		TransactionID:    res.TransactionID,
		Status:           string(res.State),
		Balance:          res.Balance,
		FailureCode:      string(res.FailureCode),
		IdempotentReplay: res.IdempotentReplay,
	}
	switch {
	case res.State == wager.StateProcessed:
		status := http.StatusCreated
		if res.IdempotentReplay {
			status = http.StatusOK
		}
		writeJSON(w, status, body)
	case res.State == wager.StateRejected:
		writeJSON(w, http.StatusUnprocessableEntity, body)
	case res.State == wager.StatePendingReference || res.State == wager.StatePending:
		writeJSON(w, http.StatusAccepted, body)
	default:
		a.failInternal(w, errors.New("httpapi: estado inesperado do processamento"))
	}
}

func (a *api) failInternal(w http.ResponseWriter, err error) {
	writeError(w, http.StatusInternalServerError, "INTERNAL", "erro interno")
}
