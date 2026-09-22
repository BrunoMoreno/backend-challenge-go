package httpapi

import (
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/platform/config"
	"github.com/BrunoMoreno/backend-challenge-go/internal/platform/logging"
	"github.com/BrunoMoreno/backend-challenge-go/internal/platform/metrics"
)

// NewServerWithDeps constrói o http.Server com as rotas de negócio
// (carteiras, operações e consultas) protegidas pela matriz de autorização.
// Deps nulas desativam as rotas correspondentes. O logger anexa a correlação
// (X-Correlation-Id) a cada registro de requisição.
func NewServerWithDeps(cfg config.Config, logger *slog.Logger, deps Deps) *http.Server {
	handlerLog := slog.New(logging.WithCorrelationHandler(logger.Handler()))
	return &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           buildHandler(handlerLog, deps),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}
}

// buildHandler monta o mux com todas as rotas habilitadas pelos deps.
func buildHandler(logger *slog.Logger, deps Deps) http.Handler {
	mux := http.NewServeMux()
	a := &api{deps: deps}

	// A documentação é pública: permite que integradores conheçam o contrato
	// sem precisar de credenciais de uma carteira ou de um provedor.
	mux.HandleFunc("GET /openapi.yaml", handleOpenAPI)
	mux.HandleFunc("GET /swagger", handleSwaggerUI)
	mux.HandleFunc("GET /swagger/", handleSwaggerUI)
	mux.HandleFunc("GET /health/live", handleLive(logger))
	if deps.Ready != nil {
		mux.HandleFunc("GET /health/ready", handleReady(logger, deps))
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

	if deps.Metrics != nil {
		mux.Handle("GET /metrics", deps.Metrics.Handler())
	}

	// A correlação fica por fora (header + contexto); as métricas envolvem o
	// mux de perto, para o label de rota usar o r.Pattern combinado — o
	// WithContext da correlação clona a requisição, então lê-lo por fora
	// veria sempre "unmatched".
	var base http.Handler = mux
	if deps.Metrics != nil {
		base = withMetrics(deps.Metrics, base)
	}
	return withCorrelation(base)
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

// withCorrelation aceita X-Correlation-Id (ou gera um), devolve na resposta e
// carrega no contexto para os registros de log (via logging.WithCorrelation).
func withCorrelation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Correlation-Id")
		if id == "" {
			id = newCorrelationID()
		}
		w.Header().Set("X-Correlation-Id", id)
		next.ServeHTTP(w, r.WithContext(logging.WithCorrelation(r.Context(), id)))
	})
}

// withMetrics observa cada requisição (status e duração) usando a rota
// combinada (r.Pattern) como label, quando disponível.
func withMetrics(m *metrics.Metrics, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		m.HTTPObserve(r.Method, route, sw.status, time.Since(start))
	})
}

// statusWriter captura o código de resposta para as métricas.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func newCorrelationID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return time.Now().Format("20060102T150405.000000000Z")
	}
	return hex.EncodeToString(b[:])
}
