-- M11: o claim da outbox ordena por (occurred_at, next_attempt_at) — o índice
-- parcial existente (outbox_pending_idx, só next_attempt_at) não servia o
-- ORDER BY primário, então o Postgres ordenava o lote candidato a cada ciclo.
-- Este índice parcial cobre o caminho quente (lê só o índice na claim).
CREATE INDEX outbox_claim_ready_idx
    ON outbox_events (occurred_at, next_attempt_at)
    WHERE published_at IS NULL;