//go:build !faultinject

package sqsconsumer

import (
	"context"
	"log/slog"
)

// crashAfterCommitBeforeDelete é o ponto de falha FAULT_AFTER_COMMIT_BEFORE_DELETE
// (docs/TESTING.md §2): no binário de produção (sem a build tag `faultinject`)
// não faz nada.
func crashAfterCommitBeforeDelete(_ context.Context, _ *slog.Logger) {}
