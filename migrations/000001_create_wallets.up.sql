CREATE TABLE wallets (
    id            varchar(36)  PRIMARY KEY,
    player_id     varchar(64)  NOT NULL,
    currency      varchar(3)   NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    balance_minor bigint       NOT NULL CHECK (balance_minor >= 0) DEFAULT 0,
    version       bigint       NOT NULL CHECK (version >= 0) DEFAULT 0,
    created_at    timestamptz  NOT NULL DEFAULT now(),
    updated_at    timestamptz  NOT NULL DEFAULT now(),
    CONSTRAINT wallets_player_currency_uniq UNIQUE (player_id, currency)
);