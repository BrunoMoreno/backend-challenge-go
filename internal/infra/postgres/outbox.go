package postgres

import (
	"context"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/events"
	"github.com/jackc/pgx/v5"
)

// OutboxRow é a linha pronta para publicação (claim de lease). Attempts é o
// número de tentativas anteriores de publicação (para o backoff exponencial).
type OutboxRow struct {
	EventID  string
	WaitFor  time.Time
	Attempts int
}

// OutboxRepository persiste leases de eventos a publicar.
type OutboxRepository struct {
	tx pgx.Tx
}

// Insert guarda o envelope para publicação assíncrona.
func (r *OutboxRepository) Insert(ctx context.Context, env events.Envelope) error {
	payload, err := marshalPayload(env)
	if err != nil {
		return err
	}
	_, err = r.tx.Exec(ctx,
		`INSERT INTO outbox_events
		   (event_id, aggregate_id, event_type, version, correlation_id, causation_id,
		    payload, occurred_at, next_attempt_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,now())`,
		env.EventID, env.AggregateID, env.EventType, env.Version,
		nullable(env.CorrelationID), nullable(env.CausationID),
		payload, env.OccurredAt)
	return mapError(err)
}

// ClaimPending toma até limit eventos disponíveis sob lease (SKIP LOCKED):
// instâncias concorrentes assumem apenas o que está liberado.
func (r *OutboxRepository) ClaimPending(ctx context.Context, limit int, lease time.Duration) ([]OutboxRow, error) {
	rows, err := r.tx.Query(ctx,
		`UPDATE outbox_events
		    SET locked_by = 'instance-' || pg_backend_pid(), locked_until = now() + $1,
		        next_attempt_at = now() + $1
		  WHERE event_id IN (
		        SELECT event_id FROM outbox_events
		         WHERE published_at IS NULL
		           AND (locked_until IS NULL OR locked_until <= now())
		           AND (next_attempt_at IS NULL OR next_attempt_at <= now())
		         ORDER BY occurred_at
		         LIMIT $2
		         FOR UPDATE SKIP LOCKED)
		RETURNING event_id, locked_until, attempts`, lease, limit)
	if err != nil {
		return nil, mapError(err)
	}
	defer rows.Close()

	var out []OutboxRow
	for rows.Next() {
		var r OutboxRow
		if err := rows.Scan(&r.EventID, &r.WaitFor, &r.Attempts); err != nil {
			return nil, mapError(err)
		}
		out = append(out, r)
	}
	return out, mapError(rows.Err())
}

// MarkPublished confirma a publicação do evento.
func (r *OutboxRepository) MarkPublished(ctx context.Context, eventID string) error {
	tag, err := r.tx.Exec(ctx,
		`UPDATE outbox_events
		    SET published_at = now(), locked_until = NULL, next_attempt_at = NULL
		  WHERE event_id = $1 AND published_at IS NULL`,
		eventID)
	if err != nil {
		return mapError(err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// MarkFailed registra falha de publicação (backoff exponencial no worker).
// A guarda `published_at IS NULL` impede que um publisher com lease vencido
// (late mark de um ciclo anterior) "regrida" um evento já publicado por outra
// instância, mexendo em attempts/next_attempt_at (docs/solve/IMPROVEMENTS.md A3).
// Nesse caso MarkFailed devolve ErrNotFound (o publisher trata como "já
// finalizado por outro").
func (r *OutboxRepository) MarkFailed(ctx context.Context, eventID string, next time.Time) error {
	tag, err := r.tx.Exec(ctx,
		`UPDATE outbox_events
		    SET attempts = attempts + 1, next_attempt_at = $2, locked_until = NULL
		  WHERE event_id = $1 AND published_at IS NULL`,
		eventID, next)
	if err != nil {
		return mapError(err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// GetEvent lê um envelope completo (publicação).
func (r *OutboxRepository) GetEvent(ctx context.Context, eventID string) (events.Envelope, error) {
	var (
		env  events.Envelope
		corr *string
		caus *string
	)
	err := r.tx.QueryRow(ctx,
		`SELECT event_id, aggregate_id, event_type, version, correlation_id,
		        causation_id, payload, occurred_at
		   FROM outbox_events WHERE event_id = $1`, eventID).
		Scan(&env.EventID, &env.AggregateID, &env.EventType, &env.Version,
			&corr, &caus, &env.Data, &env.OccurredAt)
	if err != nil {
		return events.Envelope{}, mapError(err)
	}
	env.CorrelationID = deref(corr)
	env.CausationID = deref(caus)
	return env, nil
}

func marshalPayload(env events.Envelope) ([]byte, error) {
	return env.Data, nil
}
