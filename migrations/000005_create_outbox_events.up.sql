CREATE TABLE outbox_events (
    event_id        varchar(36) PRIMARY KEY,
    aggregate_id    varchar(64) NOT NULL,
    event_type      varchar(48) NOT NULL,
    version         int         NOT NULL,
    correlation_id  varchar(64),
    causation_id    varchar(64),
    payload         jsonb       NOT NULL,
    occurred_at     timestamptz NOT NULL,
    attempts        int         NOT NULL DEFAULT 0,
    next_attempt_at timestamptz,
    locked_by       varchar(64),
    locked_until    timestamptz,
    published_at    timestamptz
);

CREATE INDEX outbox_pending_idx ON outbox_events (next_attempt_at) WHERE published_at IS NULL;