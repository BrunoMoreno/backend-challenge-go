// Command app é o ponto de entrada do serviço de processamento de apostas,
// composto com Uber Fx. O grafo completo (config, conexões, serviços e
// workers por APP_ROLES) está em internal/app/bootstrap.
package main

import (
	"go.uber.org/fx"

	"github.com/BrunoMoreno/backend-challenge-go/internal/app/bootstrap"
	"github.com/BrunoMoreno/backend-challenge-go/internal/platform/config"
)

func main() {
	// O prazo de desligamento ordenado vem da configuração (M8.2); o restante
	// do grafo, incluindo o shutdown reverso dos hooks, é do Module.
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}
	app := fx.New(bootstrap.Module(), fx.StopTimeout(cfg.ShutdownTimeout))
	app.Run()
}
