package postgres

import "time"

// nullable converte string vazia para NULL (colunas opcionais).
func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// deref desembrulha um *string de scan (NULL → "").
func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// timeOrNil converte time.Time zero para NULL (colunas ainda não agendadas).
func timeOrNil(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
