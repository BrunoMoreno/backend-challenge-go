• ## Há duas causas independentes, ambas ligadas a infraestrutura compartilhada.

> **Resolvido** (branch `fix/solve-test-integration`): as três correções de código
> abaixo foram aplicadas na suíte de integração e o pacote segue verde com
> `-race -count=1` (teste do lease 5/5):
> - `noEvent` agora faz polling estrito dentro do deadline do lease (long-poll
>   zero + deadline que nunca ultrapassa `wait`), sem leitura após a expiração;
> - pools de teste com `MaxConns` fixado (8) via `newTestPool`;
> - `runConsumer` registra `stop()` como `t.Cleanup` imediatamente após o start
>   (idempotente com `sync.Once`), garantindo o cancelamento antes do fechamento
>   do pool.
> A causa 1 (container `app` ativo) continua valendo como orientação de execução:
> `docker compose stop app` antes da suíte (README §Testes).

###  1. O teste de lease está sendo contaminado pelo container app que está ativo.

  Confirmei que backend-challenge-go-app-1 está rodando com os papéis sqs-consumer,outbox-publisher,reference-worker e usa as mesmas filas/DB dos testes. Ele pode
  publicar eventos da outbox e consumir mensagens de entrada durante a suíte.

  Além disso, o próprio noEvent tem uma janela incorreta:

  - espera 1.5s;
  - em seguida faz ReceiveMessage com long-poll de até 1s;
  - o lease é 2s.

  Logo, a leitura pode ocorrer entre 2.0s e 2.5s, já após o lease vencer. O publisher externo publica corretamente após a expiração, mas o teste acusa “durante o lease”.

###  2. O erro do PostgreSQL é saturação real de conexões, não falha de negócio.

  O banco tem max_connections = 100. Os testes criam muitos pgxpool.New(...) sem limite explícito e há testes com até 50 chamadas concorrentes. Somado ao app em execução,
  TablePlus e possivelmente outra execução simultânea de testes, o PostgreSQL recusou um novo BEGIN.

  O closed pool depois disso é efeito colateral: t.Fatalf encerra o teste, o t.Cleanup(pool.Close) fecha o pool, mas o goroutine do consumidor ainda está rodando porque
  stop() não foi registrado com defer.

  Para reproduzir de forma isolada:

  docker compose stop app
  go test -tags=integration -race ./test/integration/... -count=1

  Correções recomendadas:

  - Não subir o serviço app enquanto roda integração, ou criar filas/namespace exclusivos por execução.
  - Corrigir noEvent para fazer polling dentro de um deadline estrito, sem ReceiveMessage ultrapassar o tempo de lease.
  - Limitar MaxConns dos pools de teste (por exemplo 5–10).
  - Em testes do consumidor, registrar defer stop() imediatamente após iniciar o goroutine, garantindo cancelamento antes de fechar o pool.
  - Ao ocorrer saturação, inspecionar pg_stat_activity; no momento da minha checagem havia apenas 5 conexões ativas/ociosas, então o pico já tinha passado.