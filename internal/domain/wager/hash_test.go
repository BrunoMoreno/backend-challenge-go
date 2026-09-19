package wager

import (
	"testing"

	"github.com/BrunoMoreno/backend-challenge-go/internal/domain/money"
)

func baseFields() PayloadFields {
	return PayloadFields{
		ProviderID:            "provider-a",
		ExternalTransactionID: "transaction-123",
		PlayerID:              "P",
		WalletID:              "W",
		RoundID:               "R",
		GameID:                "G",
		Kind:                  KindBet,
		Amount:                mm(2500, "BRL"),
	}
}

// Vetor canônico: SHA-256 de {"externalTransactionId":...,"gameId":"G","kind":"BET",
// "money":{"amount":"25.00","currency":"BRL"},"playerId":"P","providerId":"provider-a",
// "roundId":"R","walletId":"W"} (chaves ordenadas, sem espaços).
func TestHashCanonicalVector(t *testing.T) {
	got, err := HashPayload(baseFields())
	if err != nil {
		t.Fatalf("HashPayload error = %v", err)
	}
	want := "84bcdc1354167551509dced2d8bf6995c9d24622ece759bc5b36f266b1b2ecb1"
	if got != want {
		t.Errorf("hash = %s, want %s", got, want)
	}
}

func TestHashDeterministic(t *testing.T) {
	h1, _ := HashPayload(baseFields())
	h2, _ := HashPayload(baseFields())
	if h1 != h2 {
		t.Errorf("hash não determinístico: %s != %s", h1, h2)
	}
}

// A chave de idempotência não participa do hash: duas requisições HTTP com
// chaves diferentes e mesmos campos de negócio geram o mesmo hash, pois a
// chave nunca é entrada da função.
func TestHashExcludesIdempotencyKey(t *testing.T) {
	reqA := baseFields() // Idempotency-Key: "chave-A" (nunca passada)
	reqB := baseFields() // Idempotency-Key: "chave-B" (nunca passada)
	ha, err := HashPayload(reqA)
	if err != nil {
		t.Fatalf("HashPayload(A) error = %v", err)
	}
	hb, err := HashPayload(reqB)
	if err != nil {
		t.Fatalf("HashPayload(B) error = %v", err)
	}
	if ha != hb {
		t.Errorf("chave de idempotência alterou o hash: %s != %s", ha, hb)
	}
}

func TestHashDiffersByField(t *testing.T) {
	base, _ := HashPayload(baseFields())

	f := baseFields()
	f.Amount = mm(2499, "BRL")
	if h, _ := HashPayload(f); h == base {
		t.Error("valor diferente não muda o hash")
	}

	f = baseFields()
	f.WalletID = "W-outra"
	if h, _ := HashPayload(f); h == base {
		t.Error("carteira diferente não muda o hash")
	}

	f = baseFields()
	f.Kind = KindWin
	if h, _ := HashPayload(f); h == base {
		t.Error("tipo diferente não muda o hash")
	}
}

func TestHashReferenceOptional(t *testing.T) {
	base, _ := HashPayload(baseFields())
	withRef := baseFields()
	withRef.ReferenceExternalTransactionID = "ref-1"
	h, _ := HashPayload(withRef)
	if h == base {
		t.Error("referência presente deveria mudar o hash")
	}
	withoutRef := baseFields()
	withoutRef.ReferenceExternalTransactionID = ""
	if h2, _ := HashPayload(withoutRef); h2 != base {
		t.Error("referência ausente deve ser igual ao hash base")
	}
}

func TestHashMoneyCanonical(t *testing.T) {
	// A mesma quantia representada em unidades mínimas (2500 vs 250) muda o hash
	// apenas se o valor mudar; a representação é sempre escala 2.
	a := baseFields()
	a.Amount = mm(2500, "BRL")
	b := baseFields()
	b.Amount = mm(25, "BRL")
	ha, _ := HashPayload(a)
	hb, _ := HashPayload(b)
	if ha == hb {
		t.Error("quantias diferentes geraram o mesmo hash")
	}
	_ = money.Money{}
}
