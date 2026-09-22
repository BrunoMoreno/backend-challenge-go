-- L3: FindPendingDue do worker de referências ordena por
-- (next_attempt_at NULLS FIRST) com filtro de estado PENDING/PENDING_REFERENCE;
-- o índice parcial (state, next_attempt_at) ASC/NULLS LAST obrigava re-sort do
-- lote candidato a cada ciclo. Este índice casa exatamente o ORDER BY.
CREATE INDEX wager_pending_due_idx
    ON wager_transactions (next_attempt_at NULLS FIRST)
    WHERE state IN ('PENDING', 'PENDING_REFERENCE');