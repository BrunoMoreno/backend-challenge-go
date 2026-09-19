DROP TRIGGER IF EXISTS wallet_ledger_consistency ON wallets;
DROP TRIGGER IF EXISTS wager_terminal_immutable ON wager_transactions;
DROP TRIGGER IF EXISTS ledger_entries_immutable ON wallet_ledger_entries;

DROP FUNCTION IF EXISTS enforce_wallet_ledger_consistency();
DROP FUNCTION IF EXISTS wager_terminal_immutable();
DROP FUNCTION IF EXISTS ledger_entries_immutable();