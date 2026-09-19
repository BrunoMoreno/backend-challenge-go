CREATE TABLE wager_transactions (
    id                      varchar(36) PRIMARY KEY,
    origin                  varchar(10) NOT NULL CHECK (origin IN ('INTERNAL', 'EXTERNAL')),
    kind                    varchar(10) NOT NULL CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
    state                   varchar(20) NOT NULL CHECK (state IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
    failure_code            varchar(32),
    wallet_id               varchar(36) REFERENCES wallets (id),
    player_id               varchar(64),
    round_id                varchar(64),
    game_id                 varchar(64),
    currency                varchar(3),
    amount_minor            bigint,
    reference_external_id   varchar(64),
    provider_id             varchar(64),
    external_transaction_id varchar(64),
    idempotency_key         varchar(64),
    payload_hash            varchar(64),
    resolved_reference_id   varchar(36) REFERENCES wager_transactions (id),
    result_balance_minor    bigint,
    attempt                 int          NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    next_attempt_at         timestamptz,
    version                 int          NOT NULL DEFAULT 1,
    created_at              timestamptz  NOT NULL DEFAULT now(),
    updated_at              timestamptz  NOT NULL DEFAULT now(),

    -- Forma por origem: externa exige identidade de provedor/transporte; interna é a abertura.
    CONSTRAINT wager_origin_shape CHECK (
        (origin = 'EXTERNAL' AND kind <> 'OPENING' AND provider_id IS NOT NULL
            AND external_transaction_id IS NOT NULL AND idempotency_key IS NOT NULL
            AND payload_hash IS NOT NULL AND wallet_id IS NOT NULL AND player_id IS NOT NULL
            AND round_id IS NOT NULL AND game_id IS NOT NULL AND currency IS NOT NULL)
        OR
        (origin = 'INTERNAL' AND kind = 'OPENING' AND provider_id IS NULL
            AND external_transaction_id IS NULL AND idempotency_key IS NULL
            AND payload_hash IS NULL AND round_id IS NULL AND game_id IS NULL
            AND wallet_id IS    NULL AND player_id IS    NULL AND currency IS NULL)
    ),

    -- Referência obrigatória para REFUND/ROLLBACK, proibida para BET/LOSS, opcional para WIN.
    CONSTRAINT wager_reference_shape CHECK (
        (kind IN ('REFUND', 'ROLLBACK') AND reference_external_id IS NOT NULL)
        OR (kind IN ('BET', 'LOSS') AND reference_external_id IS NULL)
        OR kind = 'OPENING' OR kind = 'WIN'
    ),

    -- Valor: OPENING sem valor, LOSS 0, demais > 0.
    CONSTRAINT wager_amount_shape CHECK (
        (kind = 'OPENING' AND amount_minor IS NULL)
        OR (kind = 'LOSS' AND amount_minor = 0)
        OR (kind IN ('BET', 'WIN', 'REFUND', 'ROLLBACK') AND amount_minor > 0)
    ),

    -- Rejeição sempre registrada com código de falha.
    CONSTRAINT wager_failure_code_required CHECK (state <> 'REJECTED' OR failure_code IS NOT NULL),

    UNIQUE (idempotency_key)
);

CREATE UNIQUE INDEX wager_provider_external_uniq
    ON wager_transactions (provider_id, external_transaction_id)
    WHERE origin = 'EXTERNAL';

CREATE UNIQUE INDEX wager_opening_per_wallet_uniq
    ON wager_transactions (wallet_id)
    WHERE kind = 'OPENING';

CREATE UNIQUE INDEX wager_resolved_reference_uniq
    ON wager_transactions (resolved_reference_id)
    WHERE state = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK');

CREATE INDEX wager_transactions_state_idx ON wager_transactions (state, next_attempt_at) WHERE state IN ('PENDING', 'PENDING_REFERENCE');