package httpapi

import (
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/platform/config"
)

// NewServerWithDeps constrói o http.Server com as rotas de negócio
// (carteiras, operações e consultas) protegidas pela matriz de autorização.
// Deps nulas desativam as rotas correspondentes.
func NewServerWithDeps(cfg config.Config, logger *slog.Logger, deps Deps) *http.Server {
	return &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           buildHandler(logger, deps),
		ReadHeaderTimeout: 5 * time.Second,
	}
}

// buildHandler monta o mux com todas as rotas habilitadas pelos deps.
func buildHandler(logger *slog.Logger, deps Deps) http.Handler {
	mux := http.NewServeMux()
	a := &api{deps: deps}

	mux.HandleFunc("GET /health/live", handleLive(logger))
	if deps.Ready != nil {
		mux.HandleFunc("GET /health/ready", handleReady(deps))
	}

	if deps.Verifier != nil {
		walletInternal := []string{RoleWalletInternal}
		providerAny := []string{RoleProvider, RoleWageringInternal}

		if deps.Wallets != nil {
			mux.Handle("POST /wallets",
				authChain(deps.Verifier, walletInternal)(http.HandlerFunc(a.openWallet)))
		}
		if deps.Queries != nil {
			mux.Handle("GET /wallets/{walletId}",
				authChain(deps.Verifier, walletInternal)(http.HandlerFunc(a.getWallet)))
			mux.Handle("GET /wallets/{walletId}/ledger",
				authChain(deps.Verifier, walletInternal)(http.HandlerFunc(a.ledger)))
			mux.Handle("POST /wallets/{walletId}/reconciliation",
				authChain(deps.Verifier, walletInternal)(http.HandlerFunc(a.reconcile)))
		}
		if deps.Wagers != nil {
			mux.Handle("POST /wagering/transactions",
				authChain(deps.Verifier, []string{RoleProvider})(http.HandlerFunc(a.submit)))
		}
		if deps.Queries != nil {
			mux.Handle("GET /wagering/transactions/{transactionId}",
				authChain(deps.Verifier, providerAny)(http.HandlerFunc(a.getTransaction)))
			mux.Handle("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}",
				authChain(deps.Verifier, providerAny, RequireProviderPath("providerId"))(
					http.HandlerFunc(a.getProviderTransaction)))
		}
	}

	return withCorrelation(mux)
}

// authChain encadeia Authenticate + RequireRoles (+ extras, aplicados mais
// perto do handler) nas rotas de negócio.
func authChain(v Verifier, roles []string, extra ...func(http.Handler) http.Handler) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		h := next
		for i := len(extra) - 1; i >= 0; i-- {
			h = extra[i](h)
		}
		return Authenticate(v, RequireRoles(roles...)(h))
	}
}

// withCorrelation aceita X-Correlation-Id (ou gera um) e devolve na resposta.
func withCorrelation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Correlation-Id")
		if id == "" {
			id = newCorrelationID()
		}
		w.Header().Set("X-Correlation-Id", id)
		next.ServeHTTP(w, r)
	})
}

func newCorrelationID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return time.Now().Format("20060102T150405.000000000Z")
	}
	return hex.EncodeToString(b[:])
}
