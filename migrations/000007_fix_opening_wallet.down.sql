ALTER TABLE wager_transactions DROP CONSTRAINT wager_amount_shape;
ALTER TABLE wager_transactions ADD CONSTRAINT wager_amount_shape CHECK (
    (kind = 'OPENING' AND amount_minor IS NULL)
    OR (kind = 'LOSS' AND amount_minor = 0)
    OR (kind IN ('BET', 'WIN', 'REFUND', 'ROLLBACK') AND amount_minor > 0)
);

ALTER TABLE wager_transactions DROP CONSTRAINT wager_origin_shape;
ALTER TABLE wager_transactions ADD CONSTRAINT wager_origin_shape CHECK (
    (origin = 'EXTERNAL' AND kind <> 'OPENING' AND provider_id IS NOT NULL
        AND external_transaction_id IS NOT NULL AND idempotency_key IS NOT NULL
        AND payload_hash IS NOT NULL AND wallet_id IS NOT NULL AND player_id IS NOT NULL
        AND round_id IS NOT NULL AND game_id IS NOT NULL AND currency IS NOT NULL)
    OR
    (origin = 'INTERNAL' AND kind = 'OPENING' AND provider_id IS NULL
        AND external_transaction_id IS NULL AND idempotency_key IS NULL
        AND payload_hash IS NULL AND round_id IS NULL AND game_id IS NULL
        AND wallet_id IS NULL AND player_id IS NULL AND currency IS NULL)
);