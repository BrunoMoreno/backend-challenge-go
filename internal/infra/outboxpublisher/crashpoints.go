//go:build !faultinject

package outboxpublisher

import (
	"context"
	"log/slog"
)

// crashAfterPublishBeforeMark é o ponto de falha FAULT_AFTER_PUBLISH_BEFORE_MARK
// (docs/TESTING.md §2): no binário de produção (sem a build tag `faultinject`)
// não faz nada.
func crashAfterPublishBeforeMark(_ context.Context, _ *slog.Logger) {}
