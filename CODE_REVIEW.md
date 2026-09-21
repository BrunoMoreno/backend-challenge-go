# Code Review

Data: 2026-09-21

## Visão geral

O projeto demonstra uma arquitetura bem pensada para um serviço financeiro distribuído: separação clara entre domínio, casos de uso, repositórios, infraestrutura e HTTP; uso consistente de transações e outbox; e foco em idempotência, concorrência e reconciliação. O nível de documentação e a estrutura do código são muito bons para um desafio técnico de backend.

O código está bem organizado e a intenção de negócio parece estar explícita nos comentários, especialmente nas partes de domínio, estado da aposta e worker de referência. Em termos de design, os principais pontos fortes são:

- domínio enxuto e explícito em `internal/domain/...`
- transações bem delimitadas por `UnitOfWork`
- uso de coluna/estado para idempotência e reprocessamento
- outbox e worker de referência bem alinhados com a exigência de consistência
- boa separação entre casos de uso e infraestrutura

## Pontos de atenção

### 1) Uso de `panic` em caminhos de produção

Severidade: média

Há múltiplos pontos em que erros de geração de ID e construção de objetos de domínio fazem `panic` em vez de retornar erro. Exemplos relevantes:

- `internal/app/openwallet/openwallet.go`
- `internal/app/processwager/processwager.go`

Isso aparece em funções como `newID()` e `mustLedger()`, que fazem `panic(fmt.Errorf(...))` quando falhas de `crypto/rand` ou invariantes internas ocorrem. Em um sistema financeiro, um `panic` é um comportamento muito agressivo, sobretudo em código que roda em processos de produção e em worker assíncrono.

Risco:

- processo pode morrer sem recuperação
- dificuldade de observabilidade e degradação controlada
- falha em runtime pode ser tratada como “crash do processo” em vez de “erro de domínio/integridade”

Recomendação:

- preferir retornar erro e tratar na borda de entrada/execução
- manter `panic` apenas para invariantes impossíveis de serem acionadas em produção, e não para falhas esperadas de ambiente ou geração de identificadores

### 2) Geração de IDs e entropia: dependência de `crypto/rand` sem fallback

Severidade: baixa a média

A geração de IDs usa `crypto/rand.Read`. O código trata falha como `panic`, mas em ambientes de infraestrutura isso é um ponto de fragilidade: a falha pode ser temporária, por exemplo, em contêineres com limitação de entropy ou erro excepcional do provider de rand.

A lógica do UUID v4 em hex é apropriada, mas a estratégia de tratamento de erro precisa ser mais robusta. Em sistemas críticos, falhas de “gerar ID” são tão relevantes quanto falhas de banco.

Recomendação:

- encapsular a geração em uma função que retorna `(string, error)` e que seja tratada no nível de caso de uso
- rever se há uma política explícita de retry ou fallback para erro controlado em vez de crash

### 3) `panic` em `mustLedger()` mascara o problema real da lógica

Severidade: média

`mustLedger()` foi implementado para construir um `ledger.Entry` e gerar `panic` se a construção falhar. Isso reduz a capacidade de diagnóstico, porque o erro real é transformado em crash do processo. Em uma aplicação que depende fortemente de invariantes de saldo e direção, isso cria um comportamento difícil de rastrear em produção.

A criação do lançamento do ledger não é um “evento impossível”; ela é um lugar onde muitas regras de negócio se cruzam:

- direção
- saldo antes/depois
- moeda
- validação de valores

Se alguma dessas combinações estiver incorreta, o sistema deveria reportar a falha e encerrar o comando de forma controlada, não explodir o processo.

Recomendação:

- trocar `mustLedger()` por `buildLedger()` que retorna `ledger.Entry, error`
- permitir que o caso de uso registre o problema e retorne erro em vez de panic

### 4) Risco de latent bug em `Set / metrics` com `nil` e chamadas em worker

Severidade: baixa

A implementação de `metrics.Metrics` usa `nil` check em métodos e retorna cedo. Isso é bom, mas o uso direto em vários pontos do código exige disciplina para garantir que `cfg.Metrics` sempre seja inicializado ou `nil`-safe. O código central do worker e dos consumidores faz chamadas como `w.cfg.Metrics.ReferenceResolutions(...)` sem garantir que `Metrics` tenha sido configurado.

No caso da implementação atual, a chamada funciona porque o método trata `nil`, mas esse padrão espalha a responsabilidade de segurança nula em vários pontos. Isso é aceitável para um sistema pequeno, mas pode se tornar frágil conforme o projeto cresce.

Recomendação:

- centralizar a política de métricas em um `noop` default
- evitar que o código de negócio dependa de `cfg.Metrics == nil` em vários lugares

### 5) Estratégia de reconciliação e snapshot está bem definida, mas depende de invariantes rígidas

Severidade: baixa

A reconciliação usa `BeginReadOnly` e agregação do ledger. Isso é um bom desenho para comparar saldo e ledger. No entanto, a lógica exige uma sincronização estreita entre:

- `Wallet.UpdateBalance`
- `LedgerRepository.Insert`
- `WagerRepository` terminal state transitions
- outbox events persistidos no mesmo fluxo transacional

Essa dependência é correta, mas aumenta a importância do acoplamento entre as operações. Qualquer divergência na ordem ou na persistência dessas etapas pode gerar inconsistência difícil de detectar sem uma reconciliação mais defensiva.

Recomendação:

- manter o monitoramento contínuo de reconciliação como primeiro sinal de drift
- considerar checagens extras em logs/telemetria para divergência entre saldo e ledger em produção

## Pontos positivos

### 1) Modelagem do domínio está forte

`internal/domain/wager` e `internal/domain/ledger` têm boa clareza de estados e eventos. A combinação de `State`, `FailureCode`, `Origin` e validações de transição dá muito valor para o raciocínio de negócio e para a correção de regressões.

### 2) Idempotência persistente está bem pensada

A separação entre `GetByIdempotencyKey`, `GetByProviderExternal`, `GetByReferenceExternal`, `InsertIfAbsent` e o fluxo de `WagerRepository` parece consertar a maior categoria de bugs em sistemas financeiros distribuídos: reprocessar a mesma operação sem duplicar efeitos.

### 3) Arquitetura de workers e filas está bem organizada

A combinação de outbox + SQS consumer + reference worker é uma escolha sólida. O código deixa claro que a aplicação entende a diferença entre processamento síncrono, outbox e resolução tardia de referência.

### 4) Uso de `fx` e lifecycle setup é profissional

A configuração em `internal/app/bootstrap/bootstrap.go` e a gestão de shutdown e lifecycle por `fx` deixam a aplicação pronta para multi-instância e para parada ordenada. Isso é um bom sinal de maturidade para um serviço distribuído.

## Conclusão

O projeto está claramente acima da média para um desafio de backend com entregas distribuídas. A arquitetura está coerente, a modelagem de transação e estados é boa e o foco em idempotência e consistência é excelente.

O principal ponto de atenção é a quantidade de `panic` em caminhos que deveriam ser controlados por erro, especialmente em geração de IDs e criação de lançamentos do ledger. Se esse ponto for corrigido, o sistema ficará mais robusto para produção e mais fácil de operar.

Não houve alteração de código nesta revisão; este arquivo foi criado apenas para registrar o parecer.
