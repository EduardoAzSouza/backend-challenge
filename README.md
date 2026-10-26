# Processador de Apostas (wagering processor)

Serviço que processa transações de aposta (BET/WIN/LOSS/REFUND/ROLLBACK) sobre carteiras de
jogador, com três garantias que são o coração do teste:

1. **Dinheiro nunca é float** — `Money` sobre `decimal.js`, coluna `NUMERIC(18, 2)`, JSON sempre
   como string de 2 casas (`"25.00"`).
2. **Idempotência é do banco, não da memória** — `UNIQUE (idempotency_key)` + comparação de
   `payload_hash` (SHA-256 canônico). A mesma requisição repetida 50x debita uma vez.
3. **Concorrência segura por carteira** — `SELECT ... FOR UPDATE` na linha da wallet. Duas apostas
   de `80.00` numa carteira de `100.00` enviadas juntas: uma `PROCESSED`, uma `REJECTED`
   (`INSUFFICIENT_FUNDS`), saldo final `20.00`.

O extrato (`wallet_ledger_entries`) é append-only, a outbox grava os eventos na **mesma transação
SQL** do efeito financeiro, e o `payload_hash`/inbox garantem que reprocessar mensagem do SQS não
duplica dinheiro.

## Stack

Bun 1.x, TypeScript `strict`, NestJS 10, PostgreSQL 16 com TypeORM 0.3 (`DataSource`, `QueryRunner` e
SQL explícito), AWS SQS FIFO via LocalStack e `decimal.js` para dinheiro. O consumidor do SQS só é
ativado quando `SQS_ENABLED=true`.

## Subir o projeto

Pré-requisitos: **Bun** e **Docker** (com `docker compose`).

```bash
cp .env.example .env          # ajuste as portas se 5432/4566 já estiverem em uso
docker compose up -d          # postgres:16 e localstack (SQS)
bun install
bun run migration:run         # cria tabelas, constraints, índice parcial e trigger do extrato
bun run infra:init            # cria as 3 filas SQS (idempotente; a app também cria no boot)
bun run start                 # API em http://localhost:3000
```

As migrations são reversíveis: `bun run migration:revert` desfaz a última (o `down` da migration
derruba tabelas, triggers e índices na ordem inversa).

Atalho: `bun run bootstrap` = `docker compose up -d` + espera PostgreSQL e LocalStack responderem
(`scripts/wait-for-infra.sh`) + `migration:run`.

Conferindo que subiu:

```bash
curl -s localhost:3000/health/live    # {"status":"live",...}
curl -s localhost:3000/health/ready   # {"status":"ready","checks":{"database":true,"sqs":true}}
```

As variáveis têm default (veja o `.env.example`). As que importam: `DATABASE_URL`, `PORT` (3000),
`AWS_ENDPOINT_URL` (o LocalStack, `http://localhost:4566`), `SQS_ENABLED` e `WORKERS_ENABLED` (ligam o
consumidor e os workers) e `PENDING_REFERENCE_MAX_ATTEMPTS` com `PENDING_REFERENCE_BASE_DELAY_MS`
(5 e 500 ms, o backoff do worker de referência pendente).

## API

- `POST /wallets` → `201`; `409` se a carteira daquele `playerId` + `currency` já existe; `400` payload
  inválido. A carteira nasce com o lançamento de abertura no extrato e o evento na outbox, na mesma
  transação.
- `GET /wallets/:walletId` → `200` com saldo e `version`; `404` se não existe.
- `GET /wallets/:walletId/ledger?limit=&cursor=` → `200` com o extrato paginado (cursor opaco).
- `POST /wallets/:walletId/reconciliation` → `200` com `storedBalance`, `calculatedBalance`,
  `difference`, `consistent` e `checkedEntries`. Divergência não é corrigida em silêncio: é logada,
  contada em métrica e devolvida na resposta.
- `POST /wagering/transactions` → `200` `PROCESSED`, `202` `PENDING_REFERENCE` ou `422` `REJECTED` com
  `failureCode`; `400` validação (`CURRENCY_MISMATCH` etc.) e `409` conflito de idempotência ou de
  identidade da transação.
- `GET /wagering/transactions/:transactionId` e
  `GET /providers/:providerId/wagering/transactions/:externalTransactionId` → `200`; `404` se não
  existe.
- `GET /health/live` e `GET /health/ready` → `200`, com o ready respondendo `503` se PostgreSQL ou SQS
  estiverem fora; `GET /metrics` devolve texto Prometheus.

`POST /wagering/transactions` aceita o header **`Idempotency-Key`**. Sem ele, a chave é derivada do
próprio negócio: `"{providerId}:{externalTransactionId}"`. O header existe para o caso em que o provedor
quer mandar a mesma aposta duas vezes de propósito (retry do lado dele) e precisa de um identificador
próprio.

A transação é procurada pela key **e** pelo par `(providerId, externalTransactionId)`, que o schema
também mantém único. Reenviar a mesma operação com uma key nova (provedor que gera uma chave por
tentativa) devolve o replay dela — mesmo `transactionId`, `idempotentReplay: true`. Com conteúdo
diferente, a resposta é `409 EXTERNAL_TRANSACTION_CONFLICT`: um id de transação do provedor não pode
virar duas operações, e a key dele continua registrada na transação original.

### Exemplo: carteira com 100.00 e duas apostas de 80.00 ao mesmo tempo

```bash
# 1. carteira
curl -s -X POST localhost:3000/wallets -H 'Content-Type: application/json' \
  -d '{"playerId":"p-1","initialBalance":{"amount":"100.00","currency":"BRL"}}'
# {"id":"<WALLET_ID>","playerId":"p-1","balance":{"amount":"100.00","currency":"BRL"},"version":1}

# 2. duas BETs de 80.00 — envie as duas "ao mesmo tempo" (dois terminais, ou & no shell)
curl -s -X POST localhost:3000/wagering/transactions \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: prov-a:ext-1' \
  -d '{"providerId":"prov-a","externalTransactionId":"ext-1","playerId":"p-1","walletId":"<WALLET_ID>",
       "roundId":"r-1","gameId":"g-1","kind":"BET","money":{"amount":"80.00","currency":"BRL"}}'

curl -s -X POST localhost:3000/wagering/transactions \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: prov-a:ext-2' \
  -d '{"providerId":"prov-a","externalTransactionId":"ext-2","playerId":"p-1","walletId":"<WALLET_ID>",
       "roundId":"r-1","gameId":"g-1","kind":"BET","money":{"amount":"80.00","currency":"BRL"}}'

# 3. resultado: uma 200 PROCESSED e uma 422 REJECTED com failureCode INSUFFICIENT_FUNDS
curl -s localhost:3000/wallets/<WALLET_ID>          # balance 20.00, version 2
curl -s "localhost:3000/wallets/<WALLET_ID>/ledger" # 2 lançamentos: CREDIT 100.00 e DEBIT 80.00
curl -s -X POST localhost:3000/wallets/<WALLET_ID>/reconciliation
# {"storedBalance":"20.00","calculatedBalance":"20.00","difference":"0.00","consistent":true,...}
```

Resposta de uma transação aceita:

```json
{
  "transactionId": "….….….", "status": "PROCESSED",
  "balance": { "amount": "20.00", "currency": "BRL" },
  "failureCode": null, "idempotentReplay": false
}
```

Reenviar a **mesma** requisição devolve `200` com `idempotentReplay: true` e o **mesmo**
`transactionId`. Reenviar a mesma key com valor diferente devolve `409`
`IDEMPOTENCY_PAYLOAD_MISMATCH`; reenviar a mesma transação do provedor com outro conteúdo devolve `409`
`EXTERNAL_TRANSACTION_CONFLICT`. Duas requisições concorrentes que disputam a mesma key ou o mesmo
`externalTransactionId` terminam em `200` e `409` — o conflito é declarado, não vira `500` de
constraint.

Reversão (`REFUND`/`ROLLBACK`) cuja transação original ainda não chegou devolve `202`
`PENDING_REFERENCE` e é resolvida pelo worker assim que a original aparecer — se nunca aparecer,
vira `422` `REFERENCE_NOT_FOUND_TIMEOUT` depois de `PENDING_REFERENCE_MAX_ATTEMPTS` tentativas.

Corpo de erro de validação (`400`/`409`): `{ "error": "CURRENCY_MISMATCH", "message": "...", "details": {...}, "correlationId": "..." }`.
Corpo de `422`: o próprio resultado da transação (`status: "REJECTED"`, `failureCode`, `balance`, `idempotentReplay`).

### Códigos de falha (`failureCode`)

`INSUFFICIENT_FUNDS`, `INSUFFICIENT_FUNDS_FOR_REVERSAL`, `CURRENCY_MISMATCH`, `INVALID_REFERENCE_KIND`,
`REFERENCE_CONTEXT_MISMATCH`, `REFERENCE_NOT_PROCESSED`, `REFERENCE_AMOUNT_MISMATCH`, `ALREADY_REVERSED`,
`REFERENCE_NOT_FOUND_TIMEOUT`.

Uma transação `REJECTED` **é commitada** (não sofre rollback): serve de auditoria e faz o replay da
mesma key devolver a mesma decisão, sem tocar em saldo, `version` ou extrato.

## Testes

```bash
bun run test:unit         # domínio puro + hash canônico (57 testes, sem banco)
bun run test:integration  # PostgreSQL e SQS reais (21 testes)
bun run test              # os dois
```

> O `test:integration` precisa rodar pelo script: ele define `WORKERS_ENABLED=false` e
> `PENDING_REFERENCE_BASE_DELAY_MS=1` **antes** de o Bun iniciar. Rodar o arquivo direto com
> `bun test` falha de propósito com uma mensagem explicando (o `.env` tem `WORKERS_ENABLED=true`, e os
> testes disparam os workers na mão com `runOnce()`).

Precisa da infraestrutura no ar (`docker compose up -d` + `bun run migration:run`). O setup dos testes
trunca as tabelas, então **não aponte isso para um banco com dados que importam**.

Os 57 unitários cobrem as operações e validações de `Money`, as invariantes da `Wallet`, as regras de
BET/WIN/LOSS/REFUND/ROLLBACK, as transições de estado (inclusive `FAILED`), conflito de moeda e o hash
canônico.

Os 21 de integração, contra PostgreSQL e SQS reais:

- **1** — a mesma `BET` enviada 50x em paralelo: um débito no extrato, uma resposta sem replay, 49
  replays e saldo exato.
- **2** — carteira de `100.00` com duas `BET` de `80.00` simultâneas: uma `PROCESSED`, uma `REJECTED`
  `INSUFFICIENT_FUNDS`, saldo `20.00`, um lançamento, e o replay da rejeitada devolve a mesma decisão.
- **3** — carteiras distintas processando em paralelo sem interferência.
- **4** — três instâncias da aplicação (contextos NestJS separados, portas próprias) no mesmo banco.
- **5** — mensagem real no SQS processada pelo consumidor, com `ack` só depois do commit (a mensagem
  sai da fila de verdade).
- **6** — redelivery da mesma mensagem (morte depois do commit, antes do `ack`) não duplica efeito nem
  lançamento.
- **7** — dois outbox publishers concorrentes não perdem nem duplicam evento.
- **8** — o extrato é append-only: `UPDATE` e `DELETE` recusados pelo trigger do banco.
- **9** — `REFUND` chegando antes da `BET` referenciada: `202`, e o worker resolve quando ela chega.
- **10** — referência que nunca chega vira `REJECTED` `REFERENCE_NOT_FOUND_TIMEOUT`, com saldo intacto
  e evento na outbox.
- **11** — taxonomia completa de regras de negócio pela API (reversão, contexto, valor, moeda, saldo
  insuficiente para reverter).
- **12** — depois de "reiniciar" (instância nova), todas as carteiras batem com o extrato e a
  idempotência sobrevive.
- **13** — mensagem inválida (`OPENING` pela fila) vai para a DLQ, com ack na fila principal e nada
  criado no banco.
- **14** — as garantias estão no schema: `409` para carteira duplicada, `CHECK` de saldo não negativo,
  índice parcial de reversão única e os dois triggers (append-only e aritmética do extrato).
- **15** — a mesma transação do provedor reenviada com outra `Idempotency-Key`: replay quando o payload
  é idêntico, `409 EXTERNAL_TRANSACTION_CONFLICT` quando diverge — saldo, extrato e a linha original
  intactos.
- **16** — duas requisições concorrentes com a mesma key (e o mesmo `externalTransactionId`): uma
  processa e a outra recebe `409`, nunca `500`; só uma das carteiras é debitada.
- **17** — erro transitório (o caso de uso cai no meio do processamento): sem `ack`, a mensagem continua
  na fila e a reentrega processa — nada foi aplicado pela metade.
- **18** — morte entre o commit e a publicação: os eventos ficam pendentes na outbox, uma instância nova
  assume e publica uma vez cada (`attempts = 1`), a mensagem chega na fila com o `eventId` da linha e a
  republicação é absorvida pela dedupe da fila sem mover dinheiro de novo.
- **19** — o extrato pagina por cursor: estável entre chamadas, cobre todas as entradas sem repetir nem
  perder lançamento, e cursor inválido ou `limit` fora da faixa são `400`.
- **20** — divergência entre saldo e extrato: a reconciliação sinaliza, conta em métrica e **não** corrige
  nada em silêncio.
- **21** — `SIGTERM`: o encerramento espera a mensagem em processamento terminar (e o efeito commitar)
  antes de fechar.

## Ver o que aconteceu por dentro

```bash
docker compose exec postgres psql -U jungle -d wagering_db
```

```sql
SELECT external_transaction_id, kind, status, failure_code, retry_count
  FROM wager_transactions ORDER BY created_at DESC LIMIT 10;

SELECT direction, amount, balance_before, balance_after
  FROM wallet_ledger_entries ORDER BY seq DESC LIMIT 10;

SELECT event_type, attempts, published_at FROM outbox_messages ORDER BY occurred_at DESC LIMIT 10;
SELECT message_id, consumer_name, processed_at FROM inbox_messages;
```

`GET /metrics` mostra os contadores em formato Prometheus: `wager_transactions_total{status=…}`,
`wager_duplicates_total{source=...}` (replay com a mesma `Idempotency-Key` ou redelivery da mesma
mensagem do SQS), `pending_reference_attempts_total{status=...}` e
`outbox_publish_failures_total{eventType=...}` (retries), `sqs_dlq_total`,
`wallet_lock_conflicts_total{code=...}`, `outbox_lag_ms`,
`wager_processing_latency_{count,ms_total}{status=...}` e `reconciliation_divergences_total`.

## Estrutura

```
src/
  domain/          regras puras (Money, Wallet, WagerTransaction, extrato, inbox/outbox, eventos, erros)
  application/     casos de uso + hash canônico (nada de NestJS aqui)
  infrastructure/  banco (data-source, SQL, mappers, migration), mensageria (SQS, outbox, worker), logs, métricas, guard
  presentation/    controllers, filtro de exceção
test/unit            domínio e hash (sem banco)
test/integration     concorrência e fluxos (PostgreSQL + SQS reais)
scripts/             migrate, revert, init-sqs, wait-for-infra
```

## Decisões e cortes

`ARCHITECTURE.md` explica as escolhas (lock pessimista por carteira, SQL explícito, hash canônico,
outbox/inbox, por que `REJECTED` não sofre rollback) e lista **o que ficou de fora de propósito**
(orquestração multi-container, Keycloak/OIDC, OpenTelemetry, re-drive automático da DLQ) com o que
seria feito em produção.