// Package metrics expõe métricas Prometheus da aplicação (HTTP, SQS, outbox e
// worker de referências) e o handler de scrape para o endpoint /metrics (M8.3).
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics agrupa os collectors de negócio registrados em um registry só.
type Metrics struct {
	registry       *prometheus.Registry
	httpRequests   *prometheus.CounterVec
	httpDuration   *prometheus.HistogramVec
	sqsMessages    *prometheus.CounterVec
	outboxEvents   *prometheus.CounterVec
	refResolutions *prometheus.CounterVec
}

// New constrói o registry com os collectors padrão. Devolve sempre um
// *Metrics utilizável; chamadas quando m é nil são ignoradas.
func New() *Metrics {
	reg := prometheus.NewRegistry()

	m := &Metrics{
		registry: reg,
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Requisições HTTP processadas, por método, rota e status.",
		}, []string{"method", "route", "status"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "Duração das requisições HTTP.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "route"}),
		sqsMessages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sqs_messages_total",
			Help: "Mensagens SQS da fila de entrada, por desfecho (processed, dead, retry, replay).",
		}, []string{"status"}),
		outboxEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "outbox_events_total",
			Help: "Eventos da outbox, por desfecho (published, failed).",
		}, []string{"status"}),
		refResolutions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "reference_resolutions_total",
			Help: "Resoluções do worker de referências, por desfecho (resolved, rejected, retry, expired, failed).",
		}, []string{"status"}),
	}
	reg.MustRegister(m.httpRequests, m.httpDuration, m.sqsMessages, m.outboxEvents, m.refResolutions)
	return m
}

// Handler devolve o /metrics (formato Prometheus) para o registry desta instância.
func (m *Metrics) Handler() http.Handler {
	if m == nil || m.registry == nil {
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// HTTPObserve registra status e duração de uma requisição na rota combinada.
func (m *Metrics) HTTPObserve(method, route string, status int, d time.Duration) {
	if m == nil {
		return
	}
	m.httpRequests.WithLabelValues(method, route, strconv.Itoa(status)).Inc()
	m.httpDuration.WithLabelValues(method, route).Observe(d.Seconds())
}

// SQSMessages incrementa o contador de desfecho de mensagens da fila de entrada.
func (m *Metrics) SQSMessages(status string) {
	if m == nil {
		return
	}
	m.sqsMessages.WithLabelValues(status).Inc()
}

// OutboxEvents incrementa o contador de desfecho de eventos da outbox.
func (m *Metrics) OutboxEvents(status string) {
	if m == nil {
		return
	}
	m.outboxEvents.WithLabelValues(status).Inc()
}

// ReferenceResolutions incrementa o contador de desfecho do worker de
// referências (resolved, rejected, retry, expired, failed) — M6.1.
func (m *Metrics) ReferenceResolutions(status string) {
	if m == nil {
		return
	}
	m.refResolutions.WithLabelValues(status).Inc()
}
