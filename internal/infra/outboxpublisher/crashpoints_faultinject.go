//go:build faultinject

package outboxpublisher

import (
	"context"
	"log/slog"
	"os"
)

// crashAfterPublishBeforeMark simula a morte do processo (como um SIGKILL)
// imediatamente após a mensagem ser publicada no SQS e antes da confirmação
// (MarkPublished) na outbox — o cenário de falha entre publicar e marcar do
// desafio (§11). A republicação após a expiração do lease deve preservar o
// `eventId` (M5.3). Acionado apenas com a build tag `faultinject` e a env
// FAULT_AFTER_PUBLISH_BEFORE_MARK definida.
func crashAfterPublishBeforeMark(ctx context.Context, logger *slog.Logger) {
	if os.Getenv("FAULT_AFTER_PUBLISH_BEFORE_MARK") == "" {
		return
	}
	logger.WarnContext(ctx, "faultinject: FAULT_AFTER_PUBLISH_BEFORE_MARK — encerrando após publicar, antes de marcar")
	os.Exit(1)
}
