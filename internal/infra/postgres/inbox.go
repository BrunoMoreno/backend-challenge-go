package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// InboxRepository persiste o processamento de mensagens recebidas.
type InboxRepository struct {
	tx pgx.Tx
}

// TryInsert registra a recepção da mensagem se ainda não processada.
// Deve rodar na mesma transação do caso de uso (inbox + domínio + outbox
// atomicamente) — ARCHITECTURE §9.
func (r *InboxRepository) TryInsert(ctx context.Context, consumer, messageID, hash string) (bool, error) {
	tag, err := r.tx.Exec(ctx,
		`INSERT INTO inbox (consumer_name, message_id, payload_hash)
		 VALUES ($1, $2, $3)
		 ON CONFLICT (consumer_name, message_id) DO NOTHING`,
		consumer, messageID, hash)
	if err != nil {
		return false, mapError(err)
	}
	return tag.RowsAffected() == 1, nil
}

// MarkCompleted assinala que a mensagem foi processada com sucesso.
func (r *InboxRepository) MarkCompleted(ctx context.Context, consumer, messageID string) error {
	tag, err := r.tx.Exec(ctx,
		`UPDATE inbox SET completed_at = now()
		 WHERE consumer_name = $1 AND message_id = $2 AND completed_at IS NULL`,
		consumer, messageID)
	if err != nil {
		return mapError(err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// IsCompleted informa se a mensagem já foi concluída (replay idempotente).
func (r *InboxRepository) IsCompleted(ctx context.Context, consumer, messageID string) (bool, error) {
	var completedAt *time.Time
	err := r.tx.QueryRow(ctx,
		`SELECT completed_at FROM inbox WHERE consumer_name = $1 AND message_id = $2`,
		consumer, messageID).Scan(&completedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return false, nil
		}
		return false, mapError(err)
	}
	return completedAt != nil, nil
}
