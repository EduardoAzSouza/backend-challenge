# Arquitetura

Raciocínio das decisões: o que foi escolhido, por quê, e o que ficou de fora de propósito. O README
cobre a operação.

## 1. O caminho de uma transação

HTTP (`POST /wagering/transactions`) e fila (`wager-transactions.fifo`) caem no **mesmo caso de uso**,
dentro de **uma transação SQL**:

1. se veio da fila, grava a linha em `inbox_messages` (`UNIQUE consumer_name, message_id`); já existir
   significa redelivery e nada é aplicado de novo;
2. `SELECT * FROM wallets WHERE id = $1 FOR UPDATE` — trava só aquela carteira;
3. procura a transação pela `idempotency_key` e, não achando, pelo par
   `(provider_id, external_transaction_id)`: mesmo `payload_hash` é replay (devolve a decisão com
   `idempotentReplay: true`); hash diferente é `409 IDEMPOTENCY_PAYLOAD_MISMATCH` quando a key é a mesma
   e `409 EXTERNAL_TRANSACTION_CONFLICT` quando a key é nova para um id de transação já usado;
4. confere a moeda da operação contra a da carteira, senão `400 CURRENCY_MISMATCH`;
5. `REFUND`/`ROLLBACK` resolvem a referência: não existindo ainda, a transação fica
   `PENDING_REFERENCE` e a resposta é `202`; se alguma regra de negócio for violada, fica `REJECTED` com
   `failureCode`;
6. aplica o efeito: saldo da `Wallet`, lançamento no extrato, `wager_transactions`,
   `UPDATE wallets (balance, version, updated_at)` e os eventos em `outbox_messages`.

Fora da transação: o consumidor dá `ack` só depois do `COMMIT`, e o `OutboxPublisherWorker` publica os
eventos no SQS.

Três frases resumem o desenho:

- **o banco é a fonte da verdade** — idempotência, saldo e extrato são constraints e triggers, não
  convenções de código;
- **nenhum efeito externo dentro da transação** — SQS só depois do commit, via outbox;
- **um caso de uso só** atende HTTP, fila e worker de referência pendente, então a taxonomia de erro é
  idêntica em todos os caminhos.

## 2. TypeORM com SQL explícito (e `rehydrate()`)

TypeORM entra como `DataSource` + `QueryRunner` (transação, pool, migrations), não como entidades
decoradas: as linhas viram objetos de domínio por mappers e a escrita passa por SQL parametrizado.

- **Domínio puro.** `src/domain/*` não importa NestJS nem TypeORM: construtor privado, fábricas
  (`create`, `open`, `from`, `rehydrate`, `enqueue`) e testável sem banco.
- **`rehydrate()` é explícito** sobre o que o banco pode reconstruir, inclusive um saldo que veio de
  outro nó. `debit` e `credit` continuam sendo a única forma de mudar saldo, e só incrementam `version`
  quando o saldo muda.
- **A escrita está toda em `infrastructure/database/sql.ts`** (`insertWallet`, `updateWallet`,
  `insertTransaction`, `updateTransaction`, `insertLedgerEntry`, `insertOutboxMessage`,
  `enqueueEvent`). Sem lazy loading, cascata ou query montada em runtime: cada query é auditável, o que
  importa quando o assunto é dinheiro.
- **Nada de `save()` de ORM**, que faria `SELECT` + `UPDATE` de todas as colunas fora do meu controle.
  O `UPDATE wallets SET balance_amount = $2, version = $3, updated_at = $4` é literal e roda dentro da
  transação em que a trava já foi pega.

## 3. Dinheiro longe de float

- `Money` no domínio (valor `decimal.js` + moeda), `NUMERIC(18, 2)` no banco e string de 2 casas no
  JSON (`"25.00"`). Nunca `number` para dinheiro.
- A entrada é validada por formato (`/^\d+\.\d{2}$/`, sem sinal e com exatamente 2 casas): `"25"`,
  `"25.0"`, `"25.000"`, `"-10.00"`, `"1e5"` e `NaN` dão `400 INVALID_MONEY`.
- `decimal.js` trabalha em decimal, não binário: `0.10 + 0.20` dá `0.30`.
- `isPositive()` e `isNegative()` do `decimal.js` olham o **sinal**, e `isPositive()` devolve `true`
  para zero. Como o domínio usa esses métodos para decidir se pode debitar, `Money` expõe
  `isPositive()` como `greaterThan(0)` e `isNegative()` como `lessThan(0)`.
- `toJSON()` sempre passa por `toFixed(2)`, então a API não vaza `"20"` nem `"20.000000001"`.

## 4. Lock pessimista por carteira

Toda transação começa com `SELECT * FROM wallets WHERE id = $1 FOR UPDATE`.

- **A granularidade é o `wallet_id`.** O Postgres trava a linha: carteiras diferentes rodam em paralelo
  e a mesma carteira entra em fila. Lock global (tabela, advisory por moeda) travaria o sistema inteiro
  por causa de uma carteira quente.
- **Pessimista, não otimista.** Lock otimista (comparar `version` no `UPDATE` e repetir no conflito)
  joga o problema para a aplicação: N concorrentes na mesma carteira viram N-1 retries, cada um
  refazendo leitura e cálculo, e o retry passa a ser responsabilidade de quem chamou. Com lock
  pessimista o banco serializa só o necessário: a segunda aposta lê o saldo já atualizado (`20.00`),
  decide contra a realidade e commita.
- **A ordem é sempre a mesma**: travar, ler, decidir, gravar, commitar. Não existe ler e calcular fora
  da transação para gravar depois.
- **`version` só sobe quando o saldo muda.** `LOSS` não gera lançamento, não incrementa `version` e não
  gera evento de saldo, mas continua `PROCESSED` e auditável.
- **Contenção é observável**: violação de serialização (`40001`/`40P01`) incrementa
  `wallet_lock_conflicts_total`.

O teste 2 é a pergunta clássica: `100.00` com duas `BET` de `80.00` → uma passa (saldo `20.00`), a
outra é `REJECTED INSUFFICIENT_FUNDS`, e o extrato tem um só débito.

## 5. Hash canônico (`payloadHash`)

SHA-256 sobre um JSON com as chaves em ordem alfabética e **só** campos de negócio:
`externalTransactionId`, `gameId`, `kind`, `money {amount, currency}`, `playerId`, `providerId`,
`referenceExternalTransactionId` (`null` quando ausente), `roundId` e `walletId`.

- **Determinismo**: ordenar as chaves e normalizar `money` para 2 casas faz a mesma intenção gerar o
  mesmo hash, na ordem em que o cliente mandar os campos.
- **Metadado de transporte fica fora** (`occurredAt`, `correlationId`, `messageId`, headers): muda a
  cada reenvio e não faz parte da intenção — incluí-lo faria replay legítimo virar `409`.
- **Separa reenvio de conflito**: mesma `idempotency_key` com o mesmo hash é reenvio
  (`idempotentReplay: true`); com hash diferente é outro pedido usando a mesma key →
  `409 IDEMPOTENCY_PAYLOAD_MISMATCH`, porque aceitar seria aplicar duas intenções diferentes como se
  fossem uma.

É o que faz a idempotência ser semântica (mesma intenção) e não "essa string já existiu".

**A identidade da transação não é a key.** O schema torna únicos os dois: a `idempotency_key` que o
chamador mandou e o par `(provider_id, external_transaction_id)` que o provedor usa como identidade da
operação. O caso que dói é provedor que gera uma key nova a cada tentativa — a key muda, a operação não.
Por isso a busca é dupla (item 3 do caminho da transação) e o desfecho sai do payload: hash igual é a
mesma operação e vira replay; hash diferente é `409 EXTERNAL_TRANSACTION_CONFLICT`, porque dois conteúdos
não podem ocupar o mesmo id de transação do provedor. Sem essa segunda busca, esse reenvio viraria `500`
de constraint.

O `INSERT` também deixa a unicidade com o banco: quem perde uma corrida real (duas requisições
concorrentes com a mesma key, ou com o mesmo `external_transaction_id` em carteiras diferentes) recebe
`409` do mesmo jeito. O `INSERT` roda dentro de um `SAVEPOINT`, porque a violação aborta a transação
corrente e o código precisa consultar a transação vencedora para escolher o `code` certo.

## 6. Outbox e inbox

**Outbox (saída).** Nenhuma mensagem SQS sai de dentro da transação. Os eventos
(`WagerTransactionProcessed`, `WagerTransactionRejected`, `WagerTransactionPendingReference`,
`WalletBalanceChanged`) são inseridos em `outbox_messages` na mesma transação do saldo, do extrato e da
aposta. Depois do commit, o `OutboxPublisherWorker` pega as pendentes com
`ORDER BY next_attempt_at, occurred_at LIMIT 50 FOR UPDATE SKIP LOCKED`, publica e marca
`published_at`; em falha, `attempts += 1` e backoff exponencial (a linha só é pega de novo quando
`next_attempt_at` vence).

Sem outbox existiria a janela clássica de "commitou o dinheiro mas a mensagem se perdeu" (ou o
inverso). Com outbox, ou o evento e o dinheiro estão os dois no banco, ou nenhum. O `SKIP LOCKED`
permite vários publishers no mesmo banco sem travar entre si (o teste 7 roda dois ao mesmo tempo). O
lote é disjunto na reivindicação, não na publicação: se o processo morrer entre publicar e marcar, a
linha continua pendente e outra instância republica — quem absorve a repetição é o consumidor, pelo
inbox (o teste 18 cobre esse caminho).

**Inbox (entrada).** A dedup é `UNIQUE (consumer_name, message_id)` usando o `MessageId` do **SQS** —
que é o que se repete numa redelivery — gravado na mesma transação do efeito financeiro. O `ack`
acontece só depois do commit.

- redelivery → o inbox já tem a linha, o caso de uso devolve `duplicateMessage: true` e nada é debitado
  de novo (o teste 6 simula a queda entre o commit e o ack);
- erro de negócio → `REJECTED` commitada → recebe ack, porque não é falha de infraestrutura;
- erro transitório (banco ou SQS fora) → **sem** ack, a mensagem volta pelo visibility timeout (o teste 17
  derruba o caso de uso, confere que a mensagem segue na fila e que a reentrega processa);
- mensagem permanente → vai para a `wager-transactions-dlq.fifo` (o `RedrivePolicy` também manda para a
  DLQ depois de `maxReceiveCount=5`);
- `SIGTERM`/`SIGINT` → para de receber, espera a mensagem em voo e fecha o pool (o teste 21 segura uma
  mensagem em processamento e confere que o encerramento só retorna depois dela).

## 7. `PENDING_REFERENCE`: reversão que chega antes da aposta

`REFUND`/`ROLLBACK` pode chegar antes da transação referenciada (a fila não garante ordem entre
provedores). Em vez de falhar:

1. a transação é gravada como `PENDING_REFERENCE` com `next_retry_at`, o evento
   `WagerTransactionPendingReference` vai para a outbox e a resposta é `202`;
2. o `PendingReferenceWorker` pega as linhas vencidas com `FOR UPDATE SKIP LOCKED` e chama o **mesmo
   caso de uso** em modo `retryPendingReference`: não existe segunda implementação de validação;
3. achando a referência `PROCESSED`, aplica a reversão (trava na carteira, lançamento, saldo e
   `version`, eventos);
4. não achando e passando de `PENDING_REFERENCE_MAX_ATTEMPTS`, a transação vira `REJECTED
   REFERENCE_NOT_FOUND_TIMEOUT`, commitada e auditável, com o saldo intacto (teste 10).

O backoff é exponencial (`2^tentativa × PENDING_REFERENCE_BASE_DELAY_MS`) e o `next_retry_at` fica no
banco, então o backoff é compartilhado entre instâncias em vez de viver na memória de um processo.

**Por que 5 tentativas com base de 500 ms (≈31 s de janela).** O caso que motiva o recurso é a reversão
chegando segundos antes da original (dois provedores, dois caminhos de rede): 31 s cobrem com folga e
ainda falha rápido o bastante para o provedor ser notificado na mesma rodada. TTL de horas seguraria
transação aberta sem ganho — se a original nunca chegou, não chega por espera. Os dois números são
configuráveis porque a janela certa é decisão operacional, não técnica.

### 7.1 Interpretações adotadas

- **Reversão única é mais restrita que a regra literal.** A regra pede bloquear uma segunda reversão
  "pelo mesmo tipo de operação"; aqui **qualquer** segunda reversão `PROCESSED` da mesma referência é
  recusada com `ALREADY_REVERSED`, inclusive `ROLLBACK` depois de `REFUND`. É o que o índice parcial
  `uq_single_processed_reversal` (sobre `provider_id, reference_external_transaction_id`, sem distinguir
  `kind`) garante no banco: a aplicação segue o banco em vez de contradizê-lo (teste 11).
- **`Idempotency-Key` é opcional.** Sem o header, a chave vira
  `"{providerId}:{externalTransactionId}"` — a identidade natural da transação, estável entre retries
  do mesmo provedor.
- **`PENDING_REFERENCE` não é terminal**: a chegada da referência avança a transação para `PROCESSED`.
  Só `PROCESSED`, `REJECTED` e `FAILED` são terminais.
- **`FAILED` existe no domínio mas não é produzido pelo fluxo.** É o estado para erro permanente de
  infraestrutura: erro transitório não recebe ack e mensagem permanente vai para a DLQ. Fica no enum
  para quando essa política mudar.

## 8. Por que `REJECTED` não sofre rollback

Rejeitar por regra de negócio (`INSUFFICIENT_FUNDS`, `ALREADY_REVERSED`, `REFERENCE_AMOUNT_MISMATCH`…)
**commita** a transação com `status = 'REJECTED'` e o `failureCode`, de propósito:

- **auditoria**: fica registrado que aquele provedor tentou aquela aposta e qual foi a recusa;
- **idempotência de replay**: reenviar com a mesma key devolve a mesma decisão (`422`, mesmo
  `failureCode`) em vez de recalcular contra um saldo que já mudou;
- **nada de efeito**: sem `balance_amount`, sem `version`, sem linha no extrato; o evento
  `WagerTransactionRejected` vai para a outbox.

É o que o teste 2 confirma: a rejeitada existe no banco, o saldo é `20.00`, o extrato tem um lançamento
e o reenvio continua `REJECTED`.

## 9. O que o banco garante

As invariantes não ficam em código, ficam no schema:

- **`wallets`** — `CHECK chk_wallet_balance_non_negative` (saldo nunca negativo),
  `CHECK chk_wallet_currency_format` (`^[A-Z]{3}$`) e
  `UNIQUE uq_wallet_player_currency (player_id, currency)`.
- **`wager_transactions`** — `UNIQUE uq_wager_provider_external (provider_id,
  external_transaction_id)`, `UNIQUE uq_wager_idempotency_key`, `CHECK chk_wager_failure_code`
  (`REJECTED` e `FAILED` sempre com motivo, e nenhum outro status com motivo), `CHECK
  chk_wager_reference_required` (`REFUND`/`ROLLBACK` sempre com
  referência), `CHECK chk_wager_pending_reference_deadline` e o índice parcial
  `uq_single_processed_reversal`, que permite uma única reversão `PROCESSED` por referência. Os dois
  índices únicos são o que decide quem ganha uma disputa: a aplicação consulta antes e, se os dois lados
  passarem pela consulta, quem perde o `INSERT` recebe `409` (`IDEMPOTENCY_PAYLOAD_MISMATCH` ou
  `EXTERNAL_TRANSACTION_CONFLICT`) em vez de um `500` de constraint.
- **`wallet_ledger_entries`** — `UNIQUE uq_ledger_wager_transaction` (um lançamento por transação),
  `CHECK chk_ledger_amount_positive` e dois triggers: `trg_immutable_ledger` recusa `UPDATE`/`DELETE` e
  `trg_ledger_arithmetic` confere a conta na inserção.
- **`inbox_messages`** — `UNIQUE uq_inbox_consumer_message (consumer_name, message_id)`.

A aritmética do extrato é trigger, e não `CHECK`, porque a regra é relacional:
`fn_conferir_aritmetica_ledger` lê o `balance_after` do último lançamento da carteira e exige que
`balance_before` continue a série e que `balance_after` seja exatamente `balance_before ± amount`
(negativo → exceção). Mesmo que o cálculo da aplicação estivesse errado, o banco recusaria o
lançamento — e é isso que permite reconstruir o extrato e comparar com o saldo materializado em
`POST /wallets/:id/reconciliation`. O teste 8 prova o outro lado: `UPDATE` e `DELETE` no extrato falham
com *append-only*.

## 10. Multi-instância

O serviço roda em N réplicas no mesmo banco por construção:

- **carteira**: o `FOR UPDATE` garante que o saldo lido é o real, qualquer que seja a réplica;
- **outbox** e **`PENDING_REFERENCE`**: o `SKIP LOCKED` distribui as linhas — duas réplicas nunca pegam
  a mesma, e se uma morrer depois de pegar, o `next_attempt_at`/`next_retry_at` já gravado no banco
  libera a linha para outra;
- **inbox e idempotência**: as constraints `UNIQUE` decidem quem foi o primeiro, e o segundo cai no
  replay.

O teste 4 sobe 3 instâncias (contextos, portas e DataSources separados, mesmo PostgreSQL) e dispara
apostas concorrentes na mesma carteira: 12 apostas de `25.00` sobre `1000.00` terminam em `700.00` com
12 lançamentos, lidas igual pelas três.

## 11. Autenticação: `ProviderIdentityGuard`

`ProviderIdentityGuard` é um pass-through deliberado nos endpoints de negócio (`/health/*` e
`/metrics` ficam fora): registra em log a identidade declarada no header `X-Provider-Identity` e não
confia nele — o `providerId` do payload continua passando pelas validações de domínio, inclusive as
regras de referência entre provedores.

Subir um IdP (Keycloak) adiciona container, realm, client e JWKS sem tocar nas invariantes financeiras,
que são o ponto do serviço, então o gancho fica explícito no código. Em produção entraria: validação do
`Bearer` contra o JWKS (`issuer`, `audience`, `exp`), extração do
claim `provider_id` **do token validado** (nunca de header) com `401` se divergir do payload, escopos
por operação (`wagering:write`, `wallet:read`) → `403`, rotação de chave por `kid` com cache curto e
rate limit por `provider_id`/`wallet_id`.

## 12. Logs, métricas e health

- **Log JSON**, uma linha por evento, com `correlationId` (do header ou da mensagem), `messageId`
  quando a entrada é fila, `transactionId`, `walletId`, `providerId`, `status`, `failureCode` e
  `durationMs`. Sem payload financeiro e sem dado sensível. Toda decisão passa pelo mesmo `registrar()`
  do caso de uso, então HTTP, fila e worker produzem o mesmo log (`wager_transaction_decided`, em
  `info`; `warn` quando há `failureCode`).
- **Métricas em `/metrics`** (texto Prometheus): `wager_transactions_total{status}`,
  `wager_duplicates_total{source}` (replay com a mesma `Idempotency-Key` ou redelivery da mesma
  mensagem), `pending_reference_attempts_total{status}` e `outbox_publish_failures_total{eventType}`
  (retries), `sqs_dlq_total`, `wallet_lock_conflicts_total{code}`, `outbox_lag_ms`,
  `wager_processing_latency_{count,ms_total}{status}` e `reconciliation_divergences_total`.
- **Health separado**: `/health/live` não toca dependência externa; `/health/ready` consulta PostgreSQL
  e SQS e responde `503` se algum falhar.
- A reconciliação **não corrige** divergência: loga, incrementa `reconciliation_divergences_total` e
  devolve `consistent: false` com o `difference`.

## 13. O que ficou de fora (de propósito)

O que está implementado foi verificado com teste real: PostgreSQL e SQS de verdade, 57 testes unitários
e 21 cenários de integração. Ficou de fora, com o caminho de produção:

- **orquestração multi-container** — a garantia de multi-instância é do banco e o teste 4 já sobe 3
  instâncias; em produção, `--scale app=3` e um load balancer;
- **Keycloak/OIDC real** — setup pesado e nenhuma invariante financeira exercitada (item 11);
- **OpenTelemetry e dashboards** — log JSON com `correlationId` e `/metrics` em formato Prometheus
  bastam para depurar aqui; em produção, SDK do OTel, coletor e Grafana;
- **re-drive automático da DLQ** — a DLQ e a métrica existem; reenfileirar é decisão operacional (quem,
  quando, com qual idempotência);
- **retenção e particionamento do extrato** — o volume de um teste não justifica, e o schema já nasce
  pronto para particionar por mês e mandar o frio para o S3;
- **teste de carga e carteira ultraquente** — fora do escopo de correção, e a contenção já está medida
  por métrica;
- **circuit breaker no SQS** — erro transitório já é tratado com retry;
- **suíte de contrato (Pact)** — só existe um provedor de teste;
- **compensação automática de divergência** — a reconciliação detecta (`difference`, métrica, log), mas
  corrigir pede aprovação manual e trilha de auditoria.

O que não foi cortado, porque é a razão de existir do serviço: representação de dinheiro, idempotência
persistente, lock por carteira, outbox/inbox transacionais, extrato append-only e as invariantes no
banco.

## 14. Como verifiquei

- `bun run typecheck` — TypeScript `strict`, sem erros.
- `bun run test:unit` — 57 testes de domínio e hash, sem banco.
- `bun run test:integration` — 21 cenários contra PostgreSQL 16 e LocalStack reais: 50 requisições
  paralelas idênticas, disputa de `80.00 + 80.00` em `100.00`, carteiras paralelas, 3 instâncias,
  mensagem SQS real com ack depois do commit, redelivery, dois publishers concorrentes, DLQ,
  constraints e triggers no schema, referência fora de ordem, TTL esgotado, taxonomia de falhas,
  reenvio com key nova (`409 EXTERNAL_TRANSACTION_CONFLICT`), corrida de duas requisições com a mesma
  key, erro transitório sem ack, morte entre commit e publicação da outbox, paginação por cursor,
  divergência de reconciliação, encerramento por `SIGTERM` com mensagem em voo e reconciliação final
  de todas as carteiras.

Os testes de integração falham de propósito se rodados sem o ambiente certo (`WORKERS_ENABLED=false`,
`PENDING_REFERENCE_BASE_DELAY_MS=1`), em vez de dar resultado enganoso — o `package.json` já define isso
no script `test:integration`.