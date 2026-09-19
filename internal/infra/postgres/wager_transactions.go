package postgres

import (
	"context"
	"time"

	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/wager"
	"github.com/jackc/pgx/v5"
)

const wagerColumns = `id, origin, kind, state, failure_code, wallet_id, player_id, round_id,
	game_id, currency, amount_minor, reference_external_id, provider_id,
	external_transaction_id, idempotency_key, payload_hash, resolved_reference_id,
	result_balance_minor, attempt, next_attempt_at, created_at, updated_at`

type wagerRow struct {
	id, origin, kind, state string
	failureCode             *string
	walletID, playerID      string
	roundID, gameID         *string
	currency                string
	amountMinor             int64
	referenceExternalID     *string
	providerID              *string
	externalTransactionID   *string
	idempotencyKey          *string
	payloadHash             *string
	resolvedReferenceID     *string
	resultBalanceMinor      *int64
	attempt                 int
	nextAttemptAt           *time.Time
	createdAt, updatedAt    time.Time
}

func (r wagerRow) toDomain() (wager.WagerTransaction, error) {
	currency := money.Currency(r.currency)
	ref := deref(r.referenceExternalID)

	return wager.Rehydrate(wager.RehydrateInput{
		ID:                             r.id,
		Origin:                         wager.Origin(r.origin),
		ProviderID:                     deref(r.providerID),
		ExternalTransactionID:          deref(r.externalTransactionID),
		IdempotencyKey:                 deref(r.idempotencyKey),
		PayloadHash:                    deref(r.payloadHash),
		WalletID:                       r.walletID,
		PlayerID:                       r.playerID,
		RoundID:                        deref(r.roundID),
		GameID:                         deref(r.gameID),
		Kind:                           wager.Kind(r.kind),
		Amount:                         money.MoneyOf(r.amountMinor, currency),
		ReferenceExternalTransactionID: ref,
		ResolvedReferenceTransactionID: deref(r.resolvedReferenceID),
		State:                          wager.State(r.state),
		FailureCode:                    wager.FailureCode(deref(r.failureCode)),
		ResultBalance:                  resultBalanceOf(r.resultBalanceMinor, currency),
		Attempts:                       r.attempt,
		NextAttemptAt:                  timeOrNilValue(r.nextAttemptAt),
		CreatedAt:                      r.createdAt,
		UpdatedAt:                      r.updatedAt,
	})
}

func resultBalanceOf(minor *int64, currency money.Currency) money.Money {
	if minor == nil {
		return money.Money{}
	}
	return money.MoneyOf(*minor, currency)
}

func timeOrNilValue(p *time.Time) time.Time {
	if p == nil {
		return time.Time{}
	}
	return *p
}

func scanWager(row pgx.Row) (wager.WagerTransaction, error) {
	var r wagerRow
	err := row.Scan(&r.id, &r.origin, &r.kind, &r.state, &r.failureCode,
		&r.walletID, &r.playerID, &r.roundID, &r.gameID, &r.currency, &r.amountMinor,
		&r.referenceExternalID, &r.providerID, &r.externalTransactionID,
		&r.idempotencyKey, &r.payloadHash, &r.resolvedReferenceID,
		&r.resultBalanceMinor, &r.attempt, &r.nextAttemptAt, &r.createdAt, &r.updatedAt)
	if err != nil {
		return wager.WagerTransaction{}, mapError(err)
	}
	return r.toDomain()
}

// WagerTransactionRepository persiste transações de aposta.
type WagerTransactionRepository struct {
	tx pgx.Tx
}

// Insert tenta inserir a transação em PENDING. Retorna ErrDuplicate se a
// chave de idempotência já existe (a aplicação lê o registro existente).
func (r *WagerTransactionRepository) Insert(ctx context.Context, t wager.WagerTransaction) error {
	_, err := r.tx.Exec(ctx,
		`INSERT INTO wager_transactions
		   (id, origin, kind, state, failure_code, wallet_id, player_id, round_id,
		    game_id, currency, amount_minor, reference_external_id, provider_id,
		    external_transaction_id, idempotency_key, payload_hash, resolved_reference_id,
		    result_balance_minor, attempt, next_attempt_at, created_at, updated_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)`,
		t.ID(), t.Origin(), t.Kind(), t.State(), nilString(string(t.FailureCode())),
		t.WalletID(), t.PlayerID(), zeroable(t.RoundID()), zeroable(t.GameID()),
		t.Amount().Currency(), t.Amount().Minor(),
		zeroable(t.ReferenceExternalTransactionID()), zeroable(t.ProviderID()),
		zeroable(t.ExternalTransactionID()), zeroable(t.IdempotencyKey()),
		zeroable(t.PayloadHash()), zeroable(t.ResolvedReferenceTransactionID()),
		zeroableMinor(t.ResultBalance()), t.Attempts(), timeOrNil(t.NextAttemptAt()),
		t.CreatedAt(), t.UpdatedAt())
	return mapError(err)
}

// InsertIfAbsent insere idempotentemente pela chave de idempotência.
// Retorna true se a linha foi criada; false se a chave já existia (a aplicação
// então decide entre replay ou conflito de chave — ARCHITECTURE §4).
func (r *WagerTransactionRepository) InsertIfAbsent(ctx context.Context, t wager.WagerTransaction) (bool, error) {
	tag, err := r.tx.Exec(ctx,
		`INSERT INTO wager_transactions
		   (id, origin, kind, state, failure_code, wallet_id, player_id, round_id,
		    game_id, currency, amount_minor, reference_external_id, provider_id,
		    external_transaction_id, idempotency_key, payload_hash, resolved_reference_id,
		    result_balance_minor, attempt, next_attempt_at, created_at, updated_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22)
		 ON CONFLICT (idempotency_key) DO NOTHING`,
		t.ID(), t.Origin(), t.Kind(), t.State(), nilString(string(t.FailureCode())),
		t.WalletID(), t.PlayerID(), zeroable(t.RoundID()), zeroable(t.GameID()),
		t.Amount().Currency(), t.Amount().Minor(),
		zeroable(t.ReferenceExternalTransactionID()), zeroable(t.ProviderID()),
		zeroable(t.ExternalTransactionID()), zeroable(t.IdempotencyKey()),
		zeroable(t.PayloadHash()), zeroable(t.ResolvedReferenceTransactionID()),
		zeroableMinor(t.ResultBalance()), t.Attempts(), timeOrNil(t.NextAttemptAt()),
		t.CreatedAt(), t.UpdatedAt())
	if err != nil {
		return false, mapError(err)
	}
	return tag.RowsAffected() == 1, nil
}

// UpdateTerminal persiste o desfecho (PROCESSED/REJECTED/FAILED) a partir de
// PENDING/PENDING_REFERENCE. Retorna ErrOptimisticLock se a transação já saiu
// do estado não-terminal (a trigger de imutabilidade reforça no banco).
func (r *WagerTransactionRepository) UpdateTerminal(ctx context.Context, t wager.WagerTransaction) error {
	tag, err := r.tx.Exec(ctx,
		`UPDATE wager_transactions
		   SET state = $2, failure_code = $3, resolved_reference_id = $4,
		       result_balance_minor = $5, updated_at = $6
		 WHERE id = $1 AND state IN ('PENDING','PENDING_REFERENCE')`,
		t.ID(), t.State(), nilString(string(t.FailureCode())),
		zeroable(t.ResolvedReferenceTransactionID()),
		zeroableMinor(t.ResultBalance()), t.UpdatedAt())
	if err != nil {
		return mapError(err)
	}
	if tag.RowsAffected() == 0 {
		return ErrOptimisticLock
	}
	return nil
}

// UpdatePendingReference registra PENDING_REFERENCE e a agenda de tentativas.
func (r *WagerTransactionRepository) UpdatePendingReference(ctx context.Context, t wager.WagerTransaction) error {
	tag, err := r.tx.Exec(ctx,
		`UPDATE wager_transactions
		   SET state = 'PENDING_REFERENCE', attempt = $2, next_attempt_at = $3, updated_at = $4
		 WHERE id = $1 AND state IN ('PENDING','PENDING_REFERENCE')`,
		t.ID(), t.Attempts(), timeOrNil(t.NextAttemptAt()), t.UpdatedAt())
	if err != nil {
		return mapError(err)
	}
	if tag.RowsAffected() == 0 {
		return ErrOptimisticLock
	}
	return nil
}

// GetByID lê a transação pelo id.
func (r *WagerTransactionRepository) GetByID(ctx context.Context, id string) (wager.WagerTransaction, error) {
	return scanWager(r.tx.QueryRow(ctx,
		`SELECT `+wagerColumns+` FROM wager_transactions WHERE id = $1`, id))
}

// GetByIdempotencyKey lê a transação pela chave de idempotência.
func (r *WagerTransactionRepository) GetByIdempotencyKey(ctx context.Context, key string) (wager.WagerTransaction, error) {
	return scanWager(r.tx.QueryRow(ctx,
		`SELECT `+wagerColumns+` FROM wager_transactions WHERE idempotency_key = $1`, key))
}

// GetByIdempotencyKeyForUpdate aguarda a conclusão de outro processador da mesma
// chave: trava a linha até o claim alheio commitar/rollback e devolve o estado
// final já visível (idempotência com corrida, ARCHITECTURE §4).
func (r *WagerTransactionRepository) GetByIdempotencyKeyForUpdate(ctx context.Context, key string) (wager.WagerTransaction, error) {
	return scanWager(r.tx.QueryRow(ctx,
		`SELECT `+wagerColumns+` FROM wager_transactions WHERE idempotency_key = $1 FOR UPDATE`, key))
}

// GetByProviderExternal lê pela identidade externa (provider, externalTransactionId).
func (r *WagerTransactionRepository) GetByProviderExternal(ctx context.Context, providerID, externalID string) (wager.WagerTransaction, error) {
	return scanWager(r.tx.QueryRow(ctx,
		`SELECT `+wagerColumns+` FROM wager_transactions
		  WHERE provider_id = $1 AND external_transaction_id = $2 AND origin = 'EXTERNAL'`,
		providerID, externalID))
}

// GetByReferenceExternal lê a transação referenciada por uma reversão —
// resolução por (providerId, referenceExternalTransactionId), ARCHITECTURE §6.
func (r *WagerTransactionRepository) GetByReferenceExternal(ctx context.Context, providerID, referenceExternalID string) (wager.WagerTransaction, error) {
	return r.GetByProviderExternal(ctx, providerID, referenceExternalID)
}

// FindPendingDue lista transações em PENDING/PENDING_REFERENCE cujo
// next_attempt_at venceu, travando-as com SKIP LOCKED (worker multi-instância).
func (r *WagerTransactionRepository) FindPendingDue(ctx context.Context, now time.Time, limit int) ([]wager.WagerTransaction, error) {
	rows, err := r.tx.Query(ctx,
		`SELECT `+wagerColumns+` FROM wager_transactions
		  WHERE state IN ('PENDING','PENDING_REFERENCE')
		    AND next_attempt_at IS NOT NULL AND next_attempt_at <= $1
		  ORDER BY next_attempt_at
		  LIMIT $2
		  FOR UPDATE SKIP LOCKED`, now, limit)
	if err != nil {
		return nil, mapError(err)
	}
	defer rows.Close()

	var out []wager.WagerTransaction
	for rows.Next() {
		t, err := scanWager(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, mapError(rows.Err())
}

// Helpers de conversão.
func zeroable(v string) *string {
	if v == "" {
		return nil
	}
	return &v
}

func zeroableMinor(m money.Money) *int64 {
	// Resultado ausente (ex.: OPENING ainda em PENDING) → NULL.
	if m.Currency() == "" {
		return nil
	}
	v := m.Minor()
	return &v
}

func nilString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func timeOrNil(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
