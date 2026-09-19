-- Proteções de integridade (G3, G5, G8):
--  1. ledger imutável: trigger BEFORE UPDATE/DELETE (append-only, também para o
--     role de migração; a aplicação wager_app já não tem esses privilégios).
--  2. terminais imutáveis: transação PROCESSED/REJECTED/FAILED não muda.
--  3. constraint trigger deferida no COMMIT: wallets.balance_minor = balance_after
--     do último lançamento e version = (número de lançamentos + 1).

CREATE FUNCTION ledger_entries_immutable()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'wallet_ledger_entries é append-only: UPDATE/DELETE bloqueados';
END;
$$;

CREATE TRIGGER ledger_entries_immutable
    BEFORE UPDATE OR DELETE ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION ledger_entries_immutable();

CREATE FUNCTION wager_terminal_immutable()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.state IN ('PROCESSED', 'REJECTED', 'FAILED') AND (
           NEW.state IS DISTINCT FROM OLD.state
        OR NEW.failure_code IS DISTINCT FROM OLD.failure_code
        OR NEW.result_balance_minor IS DISTINCT FROM OLD.result_balance_minor
        OR NEW.resolved_reference_id IS DISTINCT FROM OLD.resolved_reference_id
    ) THEN
        RAISE EXCEPTION 'wager_transactions % é terminal e não pode ser alterado', OLD.id;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER wager_terminal_immutable
    BEFORE UPDATE ON wager_transactions
    FOR EACH ROW EXECUTE FUNCTION wager_terminal_immutable();

CREATE FUNCTION enforce_wallet_ledger_consistency()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    last_balance bigint;
    ledger_count bigint;
BEGIN
    SELECT balance_after INTO last_balance
      FROM wallet_ledger_entries
     WHERE wallet_id = NEW.id
     ORDER BY seq DESC
     LIMIT 1;

    IF NEW.balance_minor IS DISTINCT FROM COALESCE(last_balance, 0) THEN
        RAISE EXCEPTION 'wallet % balance=% diverge do ledger (último after=%)',
            NEW.id, NEW.balance_minor, COALESCE(last_balance, 0);
    END IF;

    SELECT count(*) INTO ledger_count
      FROM wallet_ledger_entries
     WHERE wallet_id = NEW.id;

    IF NEW.version IS DISTINCT FROM ledger_count + 1 THEN
        RAISE EXCEPTION 'wallet % version=% diverge do ledger (count=%): versão esperada=%',
            NEW.id, NEW.version, ledger_count, ledger_count + 1;
    END IF;

    RETURN NEW;
END;
$$;

CREATE CONSTRAINT TRIGGER wallet_ledger_consistency
    AFTER INSERT OR UPDATE ON wallets
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION enforce_wallet_ledger_consistency();