-- Roles de banco.
--
-- Migration role (`app`): criado pelo POSTGRES_USER do Compose, dono das
-- tabelas e responsável por aplicar migrations (golang-migrate roda como `app`).
--
-- Application role (`wager_app`): usado pelas conexões da aplicação em
-- produção/repositórios; sem DDL e sem escrita no ledger. O ledger é
-- append-only: a aplicação pode INSERT (inbox/outbox incluídos) e SELECT,
-- mas nunca UPDATE, DELETE ou TRUNCATE em wallet_ledger_entries.

CREATE ROLE wager_app LOGIN PASSWORD 'wager_app' NOSUPERUSER NOCREATEDB NOCREATEROLE;

GRANT USAGE ON SCHEMA public TO wager_app;

-- Carteiras e transações: leitura e escrita (estados, saldos, versões).
GRANT INSERT, SELECT, UPDATE ON wallets, wager_transactions TO wager_app;

-- Ledger: append-only para a aplicação.
GRANT SELECT, INSERT ON wallet_ledger_entries TO wager_app;

-- Inbox: idempotência do consumidor (INSERT + marcar complete).
GRANT SELECT, INSERT, UPDATE ON inbox TO wager_app;

-- Outbox: o publisher só insere e marca published_at.
GRANT SELECT, INSERT, UPDATE ON outbox_events TO wager_app;