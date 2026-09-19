package wager

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
)

// PayloadFields são os campos de negócio incluídos no hash de idempotência.
// Exclui a chave de idempotência, messageId, occurredAt e metadados de transporte.
type PayloadFields struct {
	ProviderID                     string
	ExternalTransactionID          string
	PlayerID                       string
	WalletID                       string
	RoundID                        string
	GameID                         string
	Kind                           Kind
	Amount                         money.Money
	ReferenceExternalTransactionID string
}

// HashPayload computa SHA-256 do JSON canônico (chaves ordenadas, sem espaços)
// dos campos de negócio. É o mesmo código usado por HTTP e SQS, garantindo a
// equivalência do hash de idempotência entre os dois canais.
func HashPayload(f PayloadFields) (string, error) {
	payload := map[string]any{
		"providerId":            f.ProviderID,
		"externalTransactionId": f.ExternalTransactionID,
		"playerId":              f.PlayerID,
		"walletId":              f.WalletID,
		"roundId":               f.RoundID,
		"gameId":                f.GameID,
		"kind":                  string(f.Kind),
		"money": map[string]any{
			"amount":   f.Amount.String(),
			"currency": f.Amount.Currency().String(),
		},
	}
	if f.ReferenceExternalTransactionID != "" {
		payload["referenceExternalTransactionId"] = f.ReferenceExternalTransactionID
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
