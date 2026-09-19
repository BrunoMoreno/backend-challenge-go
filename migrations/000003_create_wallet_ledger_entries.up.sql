CREATE TABLE wallet_ledger_entries (
    seq            bigint       GENERATED ALWAYS AS IDENTITY,
    id             varchar(36) PRIMARY KEY,
    wallet_id      varchar(36) NOT NULL REFERENCES wallets (id),
    transaction_id varchar(36) NOT NULL REFERENCES wager_transactions (id),
    direction      varchar(8)  NOT NULL CHECK (direction IN ('CREDIT', 'DEBIT')),
    amount_minor   bigint      NOT NULL CHECK (amount_minor > 0),
    balance_before bigint      NOT NULL CHECK (balance_before >= 0),
    balance_after  bigint      NOT NULL CHECK (balance_after >= 0),
    created_at     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ledger_wallet_transaction_uniq UNIQUE (wallet_id, transaction_id),
    CONSTRAINT ledger_reconciliation CHECK (
        (direction = 'CREDIT' AND balance_after = balance_before + amount_minor)
        OR (direction = 'DEBIT' AND balance_after = balance_before - amount_minor)
    )
);