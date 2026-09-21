//go:build faultinject

package sqsconsumer

import (
	"context"
	"fmt"
	"log/slog"
	"os"
)

// crashAfterCommitBeforeDelete simula a morte do processo (como um SIGKILL)
// imediatamente após o COMMIT da transação e antes da remoção da mensagem da
// fila — o cenário de falha entre commit e delete do desafio (§13). Acionado
// apenas quando a build tag `faultinject` está presente e a env
// FAULT_AFTER_COMMIT_BEFORE_DELETE está definida; o binário de produção não
// contém essa função.
func crashAfterCommitBeforeDelete(ctx context.Context, logger *slog.Logger) {
	val := os.Getenv("FAULT_AFTER_COMMIT_BEFORE_DELETE")
	fmt.Fprintf(os.Stderr, "FAULTINJECT-DEBUG: env=%q -> exit(1)\n", val)
	if val == "" {
		return
	}
	logger.WarnContext(ctx, "faultinject: FAULT_AFTER_COMMIT_BEFORE_DELETE — encerrando após o commit, antes do delete")
	os.Exit(1)
}
