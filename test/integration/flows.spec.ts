import { afterAll, beforeAll, describe, expect, test, spyOn } from 'bun:test';
import { randomUUID } from 'node:crypto';
import type { AddressInfo } from 'node:net';
import type { INestApplication } from '@nestjs/common';
import type { Message, SQSClient } from '@aws-sdk/client-sqs';
import type { DataSource } from 'typeorm';

// Ordem da fila (FIFO) e demais SDKs entram como import normal.
import {
  ChangeMessageVisibilityCommand,
  CreateQueueCommand,
  DeleteMessageCommand,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
} from '@aws-sdk/client-sqs';
import { NestFactory } from '@nestjs/core';

import { sha256 } from '../../src/application/canonical-hash';
import {
  ProcessWagerTransactionUseCase,
  type WagerRequest,
} from '../../src/application/process-wager-transaction.use-case';
import { AppModule } from '../../src/app.module';
import { config } from '../../src/config';
import { WagerKind, WagerStatus } from '../../src/domain/wager-transaction';
import { createDataSource } from '../../src/infrastructure/database/data-source';
import {
  EVENT_PUBLISHER,
  type EventPublisher,
  type OutboundEvent,
} from '../../src/infrastructure/messaging/event-publisher';
import { OutboxPublisherWorker } from '../../src/infrastructure/messaging/outbox-publisher.worker';
import { PendingReferenceWorker } from '../../src/infrastructure/messaging/pending-reference.worker';
import { createSqsClient, ensureQueues, SQS_CLIENT } from '../../src/infrastructure/messaging/sqs';
import { SqsConsumerService } from '../../src/infrastructure/messaging/sqs-consumer.service';
import { HttpExceptionFilter } from '../../src/presentation/http-exception.filter';

/**
 * Testes de integracao/concorrencia contra PostgreSQL e SQS REAIS (containers
 * do docker-compose). Nada de mock: as garantias que interessam (lock, constraint,
 * trigger, inbox/outbox) so existem no banco.
 *
 * IMPORTANTE: rode com `bun run test:integration` (ou `bun run test`).
 * O script define o ambiente de teste ANTES do Bun iniciar:
 *
 *   WORKERS_ENABLED=false                    - o teste dispara os workers na mao
 *                                              (runOnce), sem polling de fundo
 *                                              competindo com as assercoes
 *   PENDING_REFERENCE_BASE_DELAY_MS=1        - backoff curto: o cenario de TTL
 *                                              esgotado levaria ~30s com 500ms
 *   LOG_LEVEL=error                          - saida limpa
 *
 * Nao adianta ajustar essas variaveis dentro do arquivo (ou num `preload` do
 * bunfig): o `.env` do projeto e carregado pelo Bun e `src/config.ts` le o
 * process.env no momento do import, antes de qualquer codigo de teste rodar.
 * Por isso o guard abaixo falha alto em vez de dar resultado instavel.
 */
const ambienteDeTesteOk =
  process.env.WORKERS_ENABLED === 'false' && process.env.PENDING_REFERENCE_BASE_DELAY_MS === '1';

if (!ambienteDeTesteOk) {
  throw new Error(
    'Ambiente de teste incorreto. Rode `bun run test:integration` (WORKERS_ENABLED=false, ' +
      `PENDING_REFERENCE_BASE_DELAY_MS=1). Recebido: WORKERS_ENABLED=${process.env.WORKERS_ENABLED}, ` +
      `PENDING_REFERENCE_BASE_DELAY_MS=${process.env.PENDING_REFERENCE_BASE_DELAY_MS}`,
  );
}

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

interface Resposta {
  status: number;
  body: any;
}

let app: INestApplication;
let base: string;
let dataSource: DataSource;
let corpo50x: Record<string, unknown>;

async function subirInstancia(): Promise<{ app: INestApplication; base: string }> {
  const instancia = await NestFactory.create(AppModule, { logger: false, abortOnError: false });
  instancia.useGlobalFilters(new HttpExceptionFilter());
  await instancia.listen(0, '127.0.0.1');

  const { port } = instancia.getHttpServer().address() as AddressInfo;
  return { app: instancia, base: `http://127.0.0.1:${port}` };
}

async function api(
  metodo: string,
  caminho: string,
  corpo?: unknown,
  headers: Record<string, string> = {},
  url: string = base,
): Promise<Resposta> {
  const resposta = await fetch(`${url}${caminho}`, {
    method: metodo,
    headers: { 'Content-Type': 'application/json', ...headers },
    body: corpo === undefined ? undefined : JSON.stringify(corpo),
  });

  const texto = await resposta.text();
  return { status: resposta.status, body: texto ? JSON.parse(texto) : null };
}

async function criarCarteira(playerId: string, saldo: string): Promise<string> {
  const resposta = await api('POST', '/wallets', {
    playerId,
    initialBalance: { amount: saldo, currency: 'BRL' },
  });

  expect(resposta.status).toBe(201);
  return resposta.body.id;
}

interface Aposta {
  providerId?: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId?: string;
  gameId?: string;
  kind?: string;
  money?: { amount: string; currency: string };
  referenceExternalTransactionId?: string | null;
}

async function enviarTransacao(aposta: Aposta, url: string = base): Promise<Resposta> {
  const corpo = {
    providerId: aposta.providerId ?? 'provider-a',
    externalTransactionId: aposta.externalTransactionId,
    playerId: aposta.playerId,
    walletId: aposta.walletId,
    roundId: aposta.roundId ?? 'rodada-1',
    gameId: aposta.gameId ?? 'fortune-chimp',
    kind: aposta.kind ?? 'BET',
    money: aposta.money ?? { amount: '25.00', currency: 'BRL' },
    referenceExternalTransactionId: aposta.referenceExternalTransactionId ?? null,
  };

  return api('POST', '/wagering/transactions', corpo, {
    'Idempotency-Key': `${corpo.providerId}:${corpo.externalTransactionId}`,
  }, url);
}

async function saldoDe(walletId: string): Promise<string> {
  const resposta = await api('GET', `/wallets/${walletId}`);
  expect(resposta.status).toBe(200);
  return resposta.body.balance.amount;
}

async function contar(sql: string, params: unknown[] = []): Promise<number> {
  const linhas: Array<{ n: number }> = await dataSource.query(sql, params);
  return Number(linhas[0]!.n);
}

/**
 * Esvazia a fila recebendo e apagando as mensagens.
 *
 * Em vez de `PurgeQueue`: o purge e assincrono no SQS (pode levar ate 60s) e
 * mensagens enviadas logo depois podem ser descartadas junto, o que deixaria o
 * teste instavel.
 */
async function drenarFila(client: SQSClient, queueUrl: string): Promise<void> {
  for (let rodada = 0; rodada < 20; rodada += 1) {
    const recebidas = await client.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 0,
        VisibilityTimeout: 0,
      }),
    );

    const mensagens = recebidas.Messages ?? [];
    if (mensagens.length === 0) {
      return;
    }

    for (const mensagem of mensagens) {
      await client.send(
        new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: mensagem.ReceiptHandle! }),
      );
    }
  }
}

interface EventoDaFila {
  eventId: string;
  eventType: string;
  data: { transactionId: string; money?: { amount: string; currency: string } };
}

/**
 * Publicador de eventos igual ao de producao (SQS FIFO), mas apontado para uma
 * fila que o teste controla. O port `EventPublisher` existe justamente para isso.
 */
class PublicadorNaFila implements EventPublisher {
  constructor(
    private readonly client: SQSClient,
    private readonly queueUrl: string,
  ) {}

  async publish(event: OutboundEvent): Promise<void> {
    await this.client.send(
      new SendMessageCommand({
        QueueUrl: this.queueUrl,
        MessageBody: JSON.stringify(event.payload),
        MessageGroupId: event.aggregateId,
        MessageDeduplicationId: event.id,
      }),
    );
  }
}

/**
 * Fila de eventos exclusiva do teste.
 *
 * A `wager-events.fifo` do projeto nao tem consumidor no ambiente de teste, entao
 * ela acumula tudo que os outros testes publicaram - provar "a mensagem chegou"
 * nela custaria varrer centenas de mensagens e dependeria da ordem. Numa fila
 * vazia a chegada e provada com uma leitura.
 */
async function filaDeEventosDoTeste(client: SQSClient): Promise<string> {
  await client.send(
    new CreateQueueCommand({
      QueueName: 'wager-events-teste.fifo',
      Attributes: { FifoQueue: 'true', ContentBasedDeduplication: 'false' },
    }),
  );

  const { QueueUrl } = await client.send(new GetQueueUrlCommand({ QueueName: 'wager-events-teste.fifo' }));

  // O LocalStack devolve a URL com o host interno dele; trocamos pelo endpoint
  // configurado (mesma normalizacao do src).
  const alvo = new URL(QueueUrl!);
  const endpoint = new URL(config.sqs.endpoint);
  if (alvo.host !== endpoint.host) {
    alvo.protocol = endpoint.protocol;
    alvo.host = endpoint.host;
  }

  return alvo.toString();
}

/**
 * Drena a fila de eventos devolvendo os envelopes que estavam nela (e apagando
 * cada um). Serve para provar que o evento SAIU da outbox de verdade, e nao
 * apenas que a linha foi marcada como publicada.
 */
async function drenarEventos(client: SQSClient, queueUrl: string): Promise<EventoDaFila[]> {
  const recebidos: EventoDaFila[] = [];

  for (let rodada = 0; rodada < 30; rodada += 1) {
    const lote = await client.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 1,
        VisibilityTimeout: 30,
      }),
    );

    const mensagens = lote.Messages ?? [];
    if (mensagens.length === 0) {
      break;
    }

    for (const mensagem of mensagens) {
      recebidos.push(JSON.parse(mensagem.Body ?? '{}') as EventoDaFila);
      await client.send(
        new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: mensagem.ReceiptHandle! }),
      );
    }
  }

  return recebidos;
}

beforeAll(async () => {
  dataSource = createDataSource();
  await dataSource.initialize();
  await dataSource.runMigrations();

  // Estado limpo: os testes nao dependem de execucao anterior.
  await dataSource.query(
    'TRUNCATE wager_transactions, wallet_ledger_entries, inbox_messages, outbox_messages, wallets CASCADE',
  );

  ({ app, base } = await subirInstancia());

  const client = app.get<SQSClient>(SQS_CLIENT);
  const filas = await ensureQueues(client);
  await drenarFila(client, filas.transactions);
  await drenarFila(client, filas.transactionsDlq);
});

afterAll(async () => {
  await app?.close();
  await dataSource?.destroy();
});

/* ==================================================================== 1 */

describe('Concorrencia', () => {
  test(
    '1. a mesma BET enviada 50x em paralelo debita uma unica vez',
    async () => {
      const playerId = `p-50x-${randomUUID()}`;
      const walletId = await criarCarteira(playerId, '500.00');

      corpo50x = {
        providerId: 'provider-a',
        externalTransactionId: 'ext-50x',
        playerId,
        walletId,
        roundId: 'rodada-50x',
        gameId: 'fortune-chimp',
        kind: 'BET',
        money: { amount: '25.00', currency: 'BRL' },
        referenceExternalTransactionId: null,
      };
      const headers = { 'Idempotency-Key': 'provider-a:ext-50x' };

      const respostas = await Promise.all(
        Array.from({ length: 50 }, () => api('POST', '/wagering/transactions', corpo50x, headers)),
      );

      // todas terminam bem; 1 aplica, 49 sao replay da mesma decisao
      expect(respostas.every((r) => r.status === 200)).toBe(true);
      expect(respostas.filter((r) => r.body.idempotentReplay === false)).toHaveLength(1);
      expect(respostas.filter((r) => r.body.idempotentReplay === true)).toHaveLength(49);
      expect(new Set(respostas.map((r) => r.body.transactionId)).size).toBe(1);

      // saldo exato: 500.00 - 25.00
      expect(await saldoDe(walletId)).toBe('475.00');

      // exatamente 2 lancamentos: o CREDIT de abertura e UM debito
      const lancamentos: Array<{ direction: string; amount: string; balance_after: string }> =
        await dataSource.query(
          'SELECT direction, amount, balance_after FROM wallet_ledger_entries WHERE wallet_id = $1 ORDER BY seq',
          [walletId],
        );
      expect(lancamentos).toHaveLength(2);
      expect(lancamentos[1]).toMatchObject({ direction: 'DEBIT', amount: '25.00', balance_after: '475.00' });

      // e saiu exatamente 1 evento WalletBalanceChanged desse debito
      expect(
        await contar(
          `SELECT count(*)::int AS n FROM outbox_messages
            WHERE event_type = 'WalletBalanceChanged'
              AND payload->'data'->>'walletId' = $1`,
          [walletId],
        ),
      ).toBe(2); // abertura + aposta
    },
    60_000,
  );

  test(
    '2. carteira de 100.00 com duas BETs de 80.00 simultaneas — so uma passa',
    async () => {
      const playerId = `p-disputa-${randomUUID()}`;
      const walletId = await criarCarteira(playerId, '100.00');

      const [a, b] = await Promise.all([
        enviarTransacao({
          externalTransactionId: 'ext-80-a',
          playerId,
          walletId,
          money: { amount: '80.00', currency: 'BRL' },
        }),
        enviarTransacao({
          externalTransactionId: 'ext-80-b',
          playerId,
          walletId,
          money: { amount: '80.00', currency: 'BRL' },
        }),
      ]);

      const status = [a.body.status, b.body.status].sort();
      expect(status).toEqual(['PROCESSED', 'REJECTED']);

      const rejeitada = a.body.status === 'REJECTED' ? a : b;
      expect(rejeitada.status).toBe(422);
      expect(rejeitada.body.failureCode).toBe('INSUFFICIENT_FUNDS');
      expect(rejeitada.body.idempotentReplay).toBe(false);

      // nenhuma aposta perdida e saldo exato
      expect(await saldoDe(walletId)).toBe('20.00');
      expect(
        await contar(
          `SELECT count(*)::int AS n FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`,
          [walletId],
        ),
      ).toBe(1);

      // a REJECTED ficou auditavel no banco, sem tocar em saldo
      expect(
        await contar(
          `SELECT count(*)::int AS n FROM wager_transactions
            WHERE wallet_id = $1 AND status = 'REJECTED' AND failure_code = 'INSUFFICIENT_FUNDS'`,
          [walletId],
        ),
      ).toBe(1);

      // repetir a aposta rejeitada devolve a MESMA decisao (nao tenta de novo)
      const replay = await enviarTransacao({ externalTransactionId: 'ext-80-b', playerId, walletId, money: { amount: '80.00', currency: 'BRL' } });
      expect(replay.status).toBe(422);
      expect(replay.body.idempotentReplay).toBe(true);
      expect(await saldoDe(walletId)).toBe('20.00');
    },
    60_000,
  );

  test(
    '3. carteiras distintas processam em paralelo sem interferencia',
    async () => {
      const donos = await Promise.all([
        (async () => ({ playerId: `p-par-a-${randomUUID()}`, saldo: '300.00' }))(),
        (async () => ({ playerId: `p-par-b-${randomUUID()}`, saldo: '300.00' }))(),
        (async () => ({ playerId: `p-par-c-${randomUUID()}`, saldo: '300.00' }))(),
      ]);

      const carteiras = await Promise.all(donos.map((dono) => criarCarteira(dono.playerId, dono.saldo)));

      const apostas: Aposta[] = carteiras.flatMap((walletId, indice) => [
        {
          externalTransactionId: `ext-par-${indice}-1`,
          playerId: donos[indice]!.playerId,
          walletId,
          roundId: 'rodada-par',
          money: { amount: '100.00', currency: 'BRL' },
        },
        {
          externalTransactionId: `ext-par-${indice}-2`,
          playerId: donos[indice]!.playerId,
          walletId,
          roundId: 'rodada-par',
          money: { amount: '50.00', currency: 'BRL' },
        },
      ]);

      const respostas = await Promise.all(apostas.map((aposta) => enviarTransacao(aposta)));

      expect(respostas.every((r) => r.status === 200 && r.body.status === 'PROCESSED')).toBe(true);

      for (const walletId of carteiras) {
        expect(await saldoDe(walletId)).toBe('150.00');
        const rec = await api('POST', `/wallets/${walletId}/reconciliation`);
        expect(rec.body.consistent).toBe(true);
      }
    },
    60_000,
  );

  test(
    '4. tres instancias da aplicacao processando a mesma carteira ao mesmo tempo',
    async () => {
      const instancias = await Promise.all([subirInstancia(), subirInstancia(), subirInstancia()]);

      try {
        const playerId = `p-multi-${randomUUID()}`;
        const walletId = await criarCarteira(playerId, '1000.00');

        const apostas = Array.from({ length: 12 }, (_, indice) => ({
          url: instancias[indice % instancias.length]!.base,
          externalTransactionId: `ext-multi-${indice}`,
        }));

        // cada aposta vai para uma instancia diferente (processos/contextos distintos),
        // todas contra o MESMO banco
        const respostas = await Promise.all(
          apostas.map((aposta) =>
            enviarTransacao(
              {
                externalTransactionId: aposta.externalTransactionId,
                playerId,
                walletId,
                roundId: 'rodada-multi',
                money: { amount: '25.00', currency: 'BRL' },
              },
              aposta.url,
            ),
          ),
        );

        expect(respostas.every((r) => r.status === 200 && r.body.status === 'PROCESSED')).toBe(true);

        // 1000 - 12*25 = 700, com 12 debitos distintos
        expect(await saldoDe(walletId)).toBe('700.00');
        expect(
          await contar(
            `SELECT count(*)::int AS n FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`,
            [walletId],
          ),
        ).toBe(12);

        // as 3 instancias convergem (nenhuma tem visao propria de saldo)
        for (const instancia of instancias) {
          const carteira = await api('GET', `/wallets/${walletId}`, undefined, {}, instancia.base);
          expect(carteira.body.balance.amount).toBe('700.00');
        }
      } finally {
        await Promise.all(instancias.map((instancia) => instancia.app.close()));
      }
    },
    90_000,
  );

  test(
    '15. a mesma transacao do provedor com outra Idempotency-Key: replay se o payload e identico, 409 se diverge',
    async () => {
      const playerId = `p-chave-${randomUUID()}`;
      const walletId = await criarCarteira(playerId, '100.00');

      const corpo = (amount: string) => ({
        providerId: 'provider-a',
        externalTransactionId: 'ext-chave',
        playerId,
        walletId,
        roundId: 'rodada-chave',
        gameId: 'fortune-chimp',
        kind: 'BET',
        money: { amount, currency: 'BRL' },
        referenceExternalTransactionId: null,
      });

      const primeira = await api('POST', '/wagering/transactions', corpo('25.00'), {
        'Idempotency-Key': 'provider-a:ext-chave',
      });
      expect(primeira.status).toBe(200);
      expect(primeira.body.idempotentReplay).toBe(false);
      expect(await saldoDe(walletId)).toBe('75.00');

      // O provedor reenviou a MESMA aposta com uma key nova (ele gera uma chave
      // por tentativa): e a mesma operacao, entao a resposta e o replay dela.
      const comOutraKey = await api('POST', '/wagering/transactions', corpo('25.00'), {
        'Idempotency-Key': `key-nova-${randomUUID()}`,
      });
      expect(comOutraKey.status).toBe(200);
      expect(comOutraKey.body.idempotentReplay).toBe(true);
      expect(comOutraKey.body.transactionId).toBe(primeira.body.transactionId);
      expect(comOutraKey.body.balance.amount).toBe('75.00');
      expect(await saldoDe(walletId)).toBe('75.00');

      // Mesma transacao do provedor com OUTRO conteudo: conflito declarado, e
      // nao um 500 de constraint. Nada e aplicado e nada novo e gravado.
      const divergente = await api('POST', '/wagering/transactions', corpo('26.00'), {
        'Idempotency-Key': `key-nova-${randomUUID()}`,
      });
      expect(divergente.status).toBe(409);
      expect(divergente.body.error).toBe('EXTERNAL_TRANSACTION_CONFLICT');
      expect(await saldoDe(walletId)).toBe('75.00');

      expect(
        await contar('SELECT count(*)::int AS n FROM wager_transactions WHERE external_transaction_id = $1', [
          'ext-chave',
        ]),
      ).toBe(1);
      expect(
        await contar(
          `SELECT count(*)::int AS n FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`,
          [walletId],
        ),
      ).toBe(1);
    },
    60_000,
  );

  test(
    '16. corrida de duas requisicoes com a mesma key ou o mesmo id do provedor: 409, nunca 500',
    async () => {
      const disputa = async (
        rotulo: string,
        chaveDe: (indice: number) => string,
      ): Promise<Array<{ status: number; erro: string | null }>> => {
        const donos = [0, 1].map((indice) => `p-${rotulo}-${indice}-${randomUUID()}`);
        // carteiras distintas: os dois lados travam linhas diferentes e podem
        // chegar juntos no INSERT (a garantia final e a constraint do banco).
        const carteiras = await Promise.all(donos.map((dono) => criarCarteira(dono, '100.00')));

        const respostas = await Promise.all(
          donos.map((dono, indice) =>
            api(
              'POST',
              '/wagering/transactions',
              {
                providerId: 'provider-a',
                externalTransactionId: `ext-${rotulo}`,
                playerId: dono,
                walletId: carteiras[indice]!,
                roundId: 'rodada-corrida',
                gameId: 'fortune-chimp',
                kind: 'BET',
                money: { amount: '25.00', currency: 'BRL' },
                referenceExternalTransactionId: null,
              },
              { 'Idempotency-Key': chaveDe(indice) },
            ),
          ),
        );

        expect(respostas.filter((r) => r.status === 200)).toHaveLength(1);
        expect(respostas.filter((r) => r.status === 409)).toHaveLength(1);
        expect(respostas.every((r) => r.status < 500)).toBe(true);

        // exatamente uma das duas carteiras foi debitada
        const saldos = await Promise.all(carteiras.map((carteira) => saldoDe(carteira)));
        expect(saldos.sort()).toEqual(['100.00', '75.00']);

        return respostas.map((r) => ({ status: r.status, erro: (r.body.error as string | null) ?? null }));
      };

      // mesmo id de transacao do provedor, keys diferentes -> conflito de identidade
      const porExternalId = await disputa('disputada', (indice) => `key-${indice}-${randomUUID()}`);
      expect(porExternalId.find((r) => r.status === 409)?.erro).toBe('EXTERNAL_TRANSACTION_CONFLICT');

      // mesma key, payloads diferentes -> conflito de idempotencia
      const chaveUnica = `key-disputada-${randomUUID()}`;
      const porKey = await disputa('key-disputada', () => chaveUnica);
      expect(porKey.find((r) => r.status === 409)?.erro).toBe('IDEMPOTENCY_PAYLOAD_MISMATCH');
    },
    90_000,
  );
});

/* ==================================================================== 2 */

describe('Mensageria: SQS, inbox e outbox', () => {
  test(
    '5. mensagem real do SQS e processada, com ack so depois do commit',
    async () => {
      const client = app.get<SQSClient>(SQS_CLIENT);
      const consumidor = app.get(SqsConsumerService);
      const filas = await ensureQueues(client);

      const playerId = `p-sqs-${randomUUID()}`;
      const walletId = await criarCarteira(playerId, '50.00');
      const messageId = `msg-${randomUUID()}`;

      const dados = {
        providerId: 'provider-a',
        externalTransactionId: 'ext-sqs-1',
        idempotencyKey: 'provider-a:ext-sqs-1',
        playerId,
        walletId,
        roundId: 'rodada-sqs',
        gameId: 'fortune-chimp',
        kind: 'BET',
        money: { amount: '10.00', currency: 'BRL' },
      };

      await client.send(
        new SendMessageCommand({
          QueueUrl: filas.transactions,
          MessageBody: JSON.stringify({
            messageId,
            type: 'WagerTransactionRequested',
            occurredAt: new Date().toISOString(),
            data: dados,
          }),
          MessageGroupId: playerId,
          MessageDeduplicationId: messageId,
        }),
      );

      const recebidas = await client.send(
        new ReceiveMessageCommand({
          QueueUrl: filas.transactions,
          MaxNumberOfMessages: 1,
          WaitTimeSeconds: 5,
          VisibilityTimeout: 30,
        }),
      );

      const mensagem = recebidas.Messages?.[0] as Message;
      expect(mensagem).toBeDefined();

      expect(await consumidor.processar(mensagem)).toBe('ack');

      // o efeito financeiro aconteceu
      expect(await saldoDe(walletId)).toBe('40.00');

      // e a mensagem saiu da fila DE VERDADE (ack depois do commit).
      // O visibility timeout do receive foi 30s: se o DeleteMessage nao tivesse
      // acontecido, a mensagem continuaria contando como "em voo".
      const atributos = await client.send(
        new GetQueueAttributesCommand({
          QueueUrl: filas.transactions,
          AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'],
        }),
      );
      expect(atributos.Attributes?.ApproximateNumberOfMessages).toBe('0');
      expect(atributos.Attributes?.ApproximateNumberOfMessagesNotVisible).toBe('0');

      // o inbox registrou o processamento pela MESMA chave usada na deduplicacao:
      // o MessageId do SQS (e nao o id que veio no corpo), porque e ele que se
      // repete numa redelivery.
      expect(
        await contar(
          `SELECT count(*)::int AS n FROM inbox_messages WHERE message_id = $1 AND processed_at IS NOT NULL`,
          [mensagem.MessageId!],
        ),
      ).toBe(1);
    },
    60_000,
  );

  test(
    '6. redelivery da mesma mensagem nao duplica efeito (morte depois do commit, antes do ack)',
    async () => {
      const useCase = app.get(ProcessWagerTransactionUseCase);

      const playerId = `p-redelivery-${randomUUID()}`;
      const walletId = await criarCarteira(playerId, '100.00');
      const messageId = `msg-fixa-${randomUUID()}`;

      const dados: WagerRequest = {
        providerId: 'provider-a',
        externalTransactionId: 'ext-redelivery',
        playerId,
        walletId,
        roundId: 'rodada-redelivery',
        gameId: 'fortune-chimp',
        kind: WagerKind.Bet,
        money: { amount: '30.00', currency: 'BRL' },
        referenceExternalTransactionId: null,
        idempotencyKey: 'provider-a:ext-redelivery',
      };

      const inbox = { messageId, consumerName: 'teste-redelivery', payloadHash: sha256(dados) };

      const primeira = await useCase.execute(dados, { inbox });
      expect(primeira.status).toBe(WagerStatus.Processed);
      expect(primeira.duplicateMessage).toBeUndefined();

      // a MESMA mensagem chega de novo (a instancia anterior morreu sem dar ack)
      const segunda = await useCase.execute(dados, { inbox });
      expect(segunda.duplicateMessage).toBe(true);
      expect(segunda.idempotentReplay).toBe(true);
      expect(segunda.transactionId).toBe(primeira.transactionId);

      // efeito aplicado exatamente UMA vez
      expect(await saldoDe(walletId)).toBe('70.00');
      expect(
        await contar(
          `SELECT count(*)::int AS n FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`,
          [walletId],
        ),
      ).toBe(1);
      expect(await contar('SELECT count(*)::int AS n FROM inbox_messages WHERE message_id = $1', [messageId])).toBe(1);
    },
    60_000,
  );

  test(
    '7. dois publishers concorrentes na mesma outbox nao perdem nem duplicam eventos',
    async () => {
      const publisher = app.get(EVENT_PUBLISHER);
      const segundoPublisher = new OutboxPublisherWorker(dataSource, publisher);

      const playerId = `p-outbox-${randomUUID()}`;
      const walletId = await criarCarteira(playerId, '500.00');

      // gera carga na outbox
      await Promise.all(
        Array.from({ length: 6 }, (_, indice) =>
          enviarTransacao({
            externalTransactionId: `ext-outbox-${indice}`,
            playerId,
            walletId,
            roundId: 'rodada-outbox',
            money: { amount: '10.00', currency: 'BRL' },
          }),
        ),
      );

      // dois publishers rodando em paralelo, varias rodadas (SKIP LOCKED)
      for (let rodada = 0; rodada < 3; rodada += 1) {
        await Promise.all([app.get(OutboxPublisherWorker).runOnce(), segundoPublisher.runOnce()]);
      }

      const resumo: Array<{ total: number; publicados: number; max_attempts: number }> = await dataSource.query(
        `SELECT count(*)::int AS total,
                count(published_at)::int AS publicados,
                max(attempts)::int AS max_attempts
           FROM outbox_messages`,
      );

      const { total, publicados, max_attempts } = resumo[0]!;
      expect(total).toBeGreaterThan(0);
      expect(publicados).toBe(total); // nenhum evento perdido
      expect(Number(max_attempts)).toBe(1); // nenhum evento publicado duas vezes
    },
    60_000,
  );

  test('8. o extrato e append-only no banco (UPDATE e DELETE recusados)', async () => {
    const linhas: Array<{ id: string }> = await dataSource.query('SELECT id FROM wallet_ledger_entries LIMIT 1');
    expect(linhas).toHaveLength(1);

    await expect(
      dataSource.query('UPDATE wallet_ledger_entries SET amount = 1.00 WHERE id = $1', [linhas[0]!.id]),
    ).rejects.toThrow(/append-only/);

    await expect(
      dataSource.query('DELETE FROM wallet_ledger_entries WHERE id = $1', [linhas[0]!.id]),
    ).rejects.toThrow(/append-only/);
  });

  test(
    '13. mensagem invalida vai para a DLQ (e OPENING nao entra pela fila)',
    async () => {
      const client = app.get<SQSClient>(SQS_CLIENT);
      const consumidor = app.get(SqsConsumerService);
      const filas = await ensureQueues(client);

      const playerId = `p-dlq-${randomUUID()}`;
      const messageId = `msg-dlq-${randomUUID()}`;

      // OPENING e interno: nao pode entrar pela API nem pela fila.
      // Uma mensagem assim nunca vai processar -> tem que morrer na DLQ.
      await client.send(
        new SendMessageCommand({
          QueueUrl: filas.transactions,
          MessageBody: JSON.stringify({
            messageId,
            type: 'WagerTransactionRequested',
            occurredAt: new Date().toISOString(),
            data: {
              providerId: 'provider-a',
              externalTransactionId: 'ext-dlq-opening',
              idempotencyKey: 'provider-a:ext-dlq-opening',
              playerId,
              walletId: randomUUID(),
              roundId: 'rodada-dlq',
              gameId: 'fortune-chimp',
              kind: 'OPENING',
              money: { amount: '10.00', currency: 'BRL' },
            },
          }),
          MessageGroupId: playerId,
          MessageDeduplicationId: messageId,
        }),
      );

      const recebidas = await client.send(
        new ReceiveMessageCommand({
          QueueUrl: filas.transactions,
          MaxNumberOfMessages: 1,
          WaitTimeSeconds: 5,
          VisibilityTimeout: 30,
        }),
      );

      const mensagem = recebidas.Messages?.[0] as Message;
      expect(mensagem).toBeDefined();

      // erro permanente: manda para a DLQ e da ack na fila principal
      expect(await consumidor.processar(mensagem)).toBe('dlq');

      const naDlq = await client.send(
        new ReceiveMessageCommand({
          QueueUrl: filas.transactionsDlq,
          MaxNumberOfMessages: 10,
          WaitTimeSeconds: 5,
        }),
      );
      const corpos = (naDlq.Messages ?? []).map((m) => m.Body ?? '');
      expect(corpos.some((corpo) => corpo.includes('ext-dlq-opening'))).toBe(true);

      // a fila de entrada ficou limpa (ack depois de mandar para a DLQ)
      const atributos = await client.send(
        new GetQueueAttributesCommand({
          QueueUrl: filas.transactions,
          AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'],
        }),
      );
      expect(atributos.Attributes?.ApproximateNumberOfMessages).toBe('0');
      expect(atributos.Attributes?.ApproximateNumberOfMessagesNotVisible).toBe('0');

      // nada foi criado no banco para essa mensagem
      expect(
        await contar(`SELECT count(*)::int AS n FROM wager_transactions WHERE external_transaction_id = 'ext-dlq-opening'`),
      ).toBe(0);
    },
    60_000,
  );
});

/* ==================================================================== 2b */

describe('Migrations e constraints no banco', () => {
  test('14. as garantias estao no schema, nao so no codigo', async () => {
    const playerId = `p-schema-${randomUUID()}`;
    const walletId = await criarCarteira(playerId, '100.00');

    // --- constraint de unicidade da carteira: 409, nao 500 ---
    const duplicada = await api('POST', '/wallets', {
      playerId,
      initialBalance: { amount: '50.00', currency: 'BRL' },
    });
    expect(duplicada.status).toBe(409);

    // --- CHECK de saldo nao negativo (mesmo por SQL direto) ---
    await expect(
      dataSource.query('UPDATE wallets SET balance_amount = -1.00 WHERE id = $1', [walletId]),
    ).rejects.toThrow(/chk_wallet_balance_non_negative/);

    // --- indices e triggers que sustentam as invariantes no banco ---
    const indices: Array<{ indexname: string; indexdef: string }> = await dataSource.query(
      `SELECT indexname, indexdef FROM pg_indexes
        WHERE indexname IN ('uq_single_processed_reversal', 'uq_wager_idempotency_key', 'uq_wager_provider_external', 'uq_wallet_player_currency')
        ORDER BY indexname`,
    );
    expect(indices.map((i) => i.indexname)).toEqual([
      'uq_single_processed_reversal',
      'uq_wager_idempotency_key',
      'uq_wager_provider_external',
      'uq_wallet_player_currency',
    ]);
    // a reversao unica e indice PARCIAL: so vale para PROCESSED e REFUND/ROLLBACK
    const reversao = indices.find((i) => i.indexname === 'uq_single_processed_reversal')!;
    expect(reversao.indexdef).toContain('PROCESSED');
    expect(reversao.indexdef).toContain('REFUND');

    const triggers: Array<{ tgname: string }> = await dataSource.query(
      `SELECT tgname FROM pg_trigger WHERE NOT tgisinternal ORDER BY tgname`,
    );
    expect(triggers.map((t) => t.tgname)).toEqual(['trg_immutable_ledger', 'trg_ledger_arithmetic']);

    // --- o trigger de aritmetica recusa um lancamento que nao fecha ---
    // (a transacao rejeitada nao tem lancamento, entao serve de referencia)
    const rejeitada = await enviarTransacao({
      externalTransactionId: 'ext-schema-rejeitada',
      playerId,
      walletId,
      roundId: 'rodada-schema',
      money: { amount: '500.00', currency: 'BRL' },
    });
    expect(rejeitada.body.status).toBe('REJECTED'); // 100.00 nao paga 500.00

    // balance_before continua o extrato (100.00), mas balance_after nao fecha
    await expect(
      dataSource.query(
        `INSERT INTO wallet_ledger_entries
           (id, wallet_id, wager_transaction_id, direction, amount, currency, balance_before, balance_after)
         VALUES ($1, $2, $3, 'CREDIT', 1.00, 'BRL', 100.00, 999.00)`,
        [randomUUID(), walletId, rejeitada.body.transactionId],
      ),
    ).rejects.toThrow(/balance_after/);

    // e a rejeicao nao mexeu no saldo nem no extrato
    expect(await saldoDe(walletId)).toBe('100.00');
    expect(
      await contar('SELECT count(*)::int AS n FROM wallet_ledger_entries WHERE wallet_id = $1', [walletId]),
    ).toBe(1);
  });
});

/* ==================================================================== 3 */

describe('Referencias fora de ordem e taxonomia de falhas', () => {
  test(
    '9. REFUND antes da referencia fica 202 e o worker resolve quando a BET chega',
    async () => {
      const worker = app.get(PendingReferenceWorker);
      const playerId = `p-ref-${randomUUID()}`;
      const walletId = await criarCarteira(playerId, '200.00');

      // chega primeiro o REFUND, apontando para uma BET que ainda nao existe
      const refund = await enviarTransacao({
        externalTransactionId: 'ext-ordem-refund',
        playerId,
        walletId,
        roundId: 'rodada-ordem',
        kind: 'REFUND',
        money: { amount: '40.00', currency: 'BRL' },
        referenceExternalTransactionId: 'ext-ordem-bet',
      });

      expect(refund.status).toBe(202);
      expect(refund.body.status).toBe('PENDING_REFERENCE');
      expect(await saldoDe(walletId)).toBe('200.00');

      // a BET referenciada so chega agora
      const bet = await enviarTransacao({
        externalTransactionId: 'ext-ordem-bet',
        playerId,
        walletId,
        roundId: 'rodada-ordem',
        money: { amount: '40.00', currency: 'BRL' },
      });
      expect(bet.status).toBe(200);
      expect(await saldoDe(walletId)).toBe('160.00');

      // o worker encontra a referencia e aplica a reversao
      await sleep(30);
      expect(await worker.runOnce()).toBeGreaterThanOrEqual(1);

      const refundDepois = await api('GET', '/providers/provider-a/wagering/transactions/ext-ordem-refund');
      expect(refundDepois.body.status).toBe('PROCESSED');
      expect(await saldoDe(walletId)).toBe('200.00');

      const rec = await api('POST', `/wallets/${walletId}/reconciliation`);
      expect(rec.body.consistent).toBe(true);
    },
    60_000,
  );

  test(
    '10. referencia que nunca chega vira REJECTED com REFERENCE_NOT_FOUND_TIMEOUT',
    async () => {
      const worker = app.get(PendingReferenceWorker);
      const playerId = `p-orfa-${randomUUID()}`;
      const walletId = await criarCarteira(playerId, '50.00');

      const refund = await enviarTransacao({
        externalTransactionId: 'ext-orfa',
        playerId,
        walletId,
        roundId: 'rodada-orfa',
        kind: 'REFUND',
        money: { amount: '10.00', currency: 'BRL' },
        referenceExternalTransactionId: 'ext-fantasma',
      });

      expect(refund.status).toBe(202);

      // o worker tenta ate estourar o TTL
      for (let tentativa = 0; tentativa < 15; tentativa += 1) {
        await sleep(25);
        await worker.runOnce();

        const linhas: Array<{ status: string }> = await dataSource.query(
          `SELECT status FROM wager_transactions WHERE external_transaction_id = 'ext-orfa'`,
        );
        if (linhas[0]?.status === 'REJECTED') break;
      }

      const linhas: Array<{ status: string; failure_code: string; retry_count: number }> = await dataSource.query(
        `SELECT status, failure_code, retry_count FROM wager_transactions WHERE external_transaction_id = 'ext-orfa'`,
      );

      expect(linhas[0]!.status).toBe('REJECTED');
      expect(linhas[0]!.failure_code).toBe('REFERENCE_NOT_FOUND_TIMEOUT');
      expect(Number(linhas[0]!.retry_count)).toBe(5);

      // saldo intacto e nenhum lancamento novo
      expect(await saldoDe(walletId)).toBe('50.00');
      expect(await contar('SELECT count(*)::int AS n FROM wallet_ledger_entries WHERE wallet_id = $1', [walletId])).toBe(1);

      // e o evento de rejeicao foi para a outbox
      expect(
        await contar(
          `SELECT count(*)::int AS n FROM outbox_messages
            WHERE event_type = 'WagerTransactionRejected'
              AND payload->'data'->>'externalTransactionId' = 'ext-orfa'`,
        ),
      ).toBe(1);
    },
    60_000,
  );

  test(
    '11. taxonomia completa de regras de negocio pela API real',
    async () => {
      const playerId = `p-regras-${randomUUID()}`;
      const outroPlayer = `p-regras-outro-${randomUUID()}`;
      const walletId = await criarCarteira(playerId, '1000.00');
      const outraWallet = await criarCarteira(outroPlayer, '1000.00');
      const rodada = 'rodada-regras';

      // BET debita
      const bet1 = await enviarTransacao({
        externalTransactionId: 'ext-r-bet',
        playerId,
        walletId,
        roundId: rodada,
        money: { amount: '100.00', currency: 'BRL' },
      });
      expect(bet1.status).toBe(200);
      expect(await saldoDe(walletId)).toBe('900.00');

      // REFUND de BET credita
      const refund1 = await enviarTransacao({
        externalTransactionId: 'ext-r-refund',
        playerId,
        walletId,
        roundId: rodada,
        kind: 'REFUND',
        money: { amount: '100.00', currency: 'BRL' },
        referenceExternalTransactionId: 'ext-r-bet',
      });
      expect(refund1.status).toBe(200);
      expect(await saldoDe(walletId)).toBe('1000.00');

      // a MESMA referencia nao pode ser revertida duas vezes -> ALREADY_REVERSED
      const reversaoRepetida = await enviarTransacao({
        externalTransactionId: 'ext-r-refund-2',
        playerId,
        walletId,
        roundId: rodada,
        kind: 'REFUND',
        money: { amount: '100.00', currency: 'BRL' },
        referenceExternalTransactionId: 'ext-r-bet',
      });
      expect(reversaoRepetida.status).toBe(422);
      expect(reversaoRepetida.body.failureCode).toBe('ALREADY_REVERSED');

      // ROLLBACK de BET tambem credita (referencia ainda nao revertida)
      const bet2 = await enviarTransacao({
        externalTransactionId: 'ext-r-bet-2',
        playerId,
        walletId,
        roundId: rodada,
        money: { amount: '100.00', currency: 'BRL' },
      });
      expect(bet2.status).toBe(200);

      const rollbackBet = await enviarTransacao({
        externalTransactionId: 'ext-r-rollback-bet',
        playerId,
        walletId,
        roundId: rodada,
        kind: 'ROLLBACK',
        money: { amount: '100.00', currency: 'BRL' },
        referenceExternalTransactionId: 'ext-r-bet-2',
      });
      expect(rollbackBet.status).toBe(200);
      expect(await saldoDe(walletId)).toBe('1000.00');

      // WIN credita e LOSS nao mexe no saldo
      const win = await enviarTransacao({
        externalTransactionId: 'ext-r-win',
        playerId,
        walletId,
        roundId: rodada,
        kind: 'WIN',
        money: { amount: '250.00', currency: 'BRL' },
      });
      expect(win.status).toBe(200);
      expect(await saldoDe(walletId)).toBe('1250.00');

      const loss = await enviarTransacao({
        externalTransactionId: 'ext-r-loss',
        playerId,
        walletId,
        roundId: rodada,
        kind: 'LOSS',
        money: { amount: '250.00', currency: 'BRL' },
      });
      expect(loss.status).toBe(200);
      expect(await saldoDe(walletId)).toBe('1250.00');
      expect(
        await contar(
          `SELECT count(*)::int AS n FROM wallet_ledger_entries WHERE wallet_id = $1 AND wager_transaction_id = $2`,
          [walletId, loss.body.transactionId],
        ),
      ).toBe(0); // LOSS nao gera lancamento

      // REFUND apontando para um WIN -> INVALID_REFERENCE_KIND
      const refundDeWin = await enviarTransacao({
        externalTransactionId: 'ext-r-refund-win',
        playerId,
        walletId,
        roundId: rodada,
        kind: 'REFUND',
        money: { amount: '250.00', currency: 'BRL' },
        referenceExternalTransactionId: 'ext-r-win',
      });
      expect(refundDeWin.body.failureCode).toBe('INVALID_REFERENCE_KIND');

      // valor diferente do valor original -> REFERENCE_AMOUNT_MISMATCH
      const bet3 = await enviarTransacao({
        externalTransactionId: 'ext-r-bet-3',
        playerId,
        walletId,
        roundId: rodada,
        money: { amount: '50.00', currency: 'BRL' },
      });
      expect(bet3.status).toBe(200);

      const valorErrado = await enviarTransacao({
        externalTransactionId: 'ext-r-valor',
        playerId,
        walletId,
        roundId: rodada,
        kind: 'REFUND',
        money: { amount: '49.00', currency: 'BRL' },
        referenceExternalTransactionId: 'ext-r-bet-3',
      });
      expect(valorErrado.body.failureCode).toBe('REFERENCE_AMOUNT_MISMATCH');

      // referencia de outro player/carteira -> REFERENCE_CONTEXT_MISMATCH
      const betDoOutro = await enviarTransacao({
        externalTransactionId: 'ext-r-bet-outro',
        playerId: outroPlayer,
        walletId: outraWallet,
        roundId: rodada,
        money: { amount: '100.00', currency: 'BRL' },
      });
      expect(betDoOutro.status).toBe(200);

      const contextoErrado = await enviarTransacao({
        externalTransactionId: 'ext-r-contexto',
        playerId,
        walletId,
        roundId: rodada,
        kind: 'REFUND',
        money: { amount: '100.00', currency: 'BRL' },
        referenceExternalTransactionId: 'ext-r-bet-outro',
      });
      expect(contextoErrado.body.failureCode).toBe('REFERENCE_CONTEXT_MISMATCH');

      // moeda diferente da carteira -> 400 com CURRENCY_MISMATCH
      const moedaErrada = await enviarTransacao({
        externalTransactionId: 'ext-r-moeda',
        playerId,
        walletId,
        roundId: rodada,
        kind: 'WIN',
        money: { amount: '10.00', currency: 'USD' },
      });
      expect(moedaErrada.status).toBe(400);
      // Erros de validacao saem pelo filtro HTTP: `error` carrega o codigo.
      expect(moedaErrada.body.error).toBe('CURRENCY_MISMATCH');
      expect(await saldoDe(walletId)).toBe('1200.00');

      // ROLLBACK de WIN com saldo insuficiente -> INSUFFICIENT_FUNDS_FOR_REVERSAL
      const dreno = await enviarTransacao({
        externalTransactionId: 'ext-r-dreno',
        playerId,
        walletId,
        roundId: rodada,
        money: { amount: '1100.00', currency: 'BRL' },
      });
      expect(dreno.status).toBe(200);
      expect(await saldoDe(walletId)).toBe('100.00');

      const rollbackWin = await enviarTransacao({
        externalTransactionId: 'ext-r-rollback-win',
        playerId,
        walletId,
        roundId: rodada,
        kind: 'ROLLBACK',
        money: { amount: '250.00', currency: 'BRL' },
        referenceExternalTransactionId: 'ext-r-win',
      });
      expect(rollbackWin.status).toBe(422);
      expect(rollbackWin.body.failureCode).toBe('INSUFFICIENT_FUNDS_FOR_REVERSAL');

      // nenhuma rejeicao mexeu no saldo, e o extrato fecha
      expect(await saldoDe(walletId)).toBe('100.00');
      expect(await saldoDe(outraWallet)).toBe('900.00');

      const rec = await api('POST', `/wallets/${walletId}/reconciliation`);
      expect(rec.status).toBe(200);
      expect(rec.body.consistent).toBe(true);
    },
    90_000,
  );
});

/* ==================================================================== 4 */

describe('Invariante final', () => {
  test(
    '12. apos reinicio, todas as carteiras batem com o extrato',
    async () => {
      // "reinicio": instancia nova da aplicacao sobre o mesmo banco
      const nova = await subirInstancia();

      try {
        const carteiras: Array<{ id: string }> = await dataSource.query('SELECT id FROM wallets');
        expect(carteiras.length).toBeGreaterThan(0);

        for (const carteira of carteiras) {
          const rec = await api('POST', `/wallets/${carteira.id}/reconciliation`, undefined, {}, nova.base);
          expect(rec.status).toBe(200);
          expect(rec.body.consistent).toBe(true);
          expect(rec.body.difference.amount).toBe('0.00');
          expect(rec.body.storedBalance.amount).toBe(rec.body.calculatedBalance.amount);
        }

        // invariantes globais no banco
        expect(await contar('SELECT count(*)::int AS n FROM wallets WHERE balance_amount < 0')).toBe(0);
        expect(
          await contar(`SELECT count(*)::int AS n
                           FROM wallet_ledger_entries l
                           JOIN wager_transactions t ON t.id = l.wager_transaction_id
                          WHERE t.status <> 'PROCESSED'`),
        ).toBe(0);
        expect(
          await contar(`SELECT count(*)::int AS n
                           FROM wallets w
                           LEFT JOIN (SELECT wallet_id,
                                             SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE -amount END) AS saldo
                                        FROM wallet_ledger_entries GROUP BY wallet_id) l
                             ON l.wallet_id = w.id
                          WHERE w.balance_amount <> COALESCE(l.saldo, 0)`),
        ).toBe(0);

        // idempotencia sobrevive ao reinicio: a mesma aposta devolve a mesma decisao
        const replay = await api(
          'POST',
          '/wagering/transactions',
          corpo50x,
          { 'Idempotency-Key': 'provider-a:ext-50x' },
          nova.base,
        );
        expect(replay.status).toBe(200);
        expect(replay.body.idempotentReplay).toBe(true);
      } finally {
        await nova.app.close();
      }
    },
    90_000,
  );
});

/* ==================================================================== 5 */

describe('Caminhos de falha, paginacao e encerramento', () => {
  test(
    '17. erro transitorio nao recebe ack: a mensagem volta pelo visibility timeout',
    async () => {
      const client = app.get<SQSClient>(SQS_CLIENT);
      const consumidor = app.get(SqsConsumerService);
      const useCase = app.get(ProcessWagerTransactionUseCase);
      const filas = await ensureQueues(client);

      await drenarFila(client, filas.transactions);

      const playerId = `p-transitorio-${randomUUID()}`;
      const walletId = await criarCarteira(playerId, '100.00');
      const messageId = `msg-transitorio-${randomUUID()}`;
      const externalTransactionId = `ext-transitorio-${randomUUID()}`;

      await client.send(
        new SendMessageCommand({
          QueueUrl: filas.transactions,
          MessageBody: JSON.stringify({
            messageId,
            type: 'WagerTransactionRequested',
            occurredAt: new Date().toISOString(),
            data: {
              providerId: 'provider-a',
              externalTransactionId,
              idempotencyKey: `provider-a:${externalTransactionId}`,
              playerId,
              walletId,
              roundId: 'rodada-transitorio',
              gameId: 'fortune-chimp',
              kind: 'BET',
              money: { amount: '10.00', currency: 'BRL' },
            },
          }),
          MessageGroupId: playerId,
          MessageDeduplicationId: messageId,
        }),
      );

      const recebidas = await client.send(
        new ReceiveMessageCommand({
          QueueUrl: filas.transactions,
          MaxNumberOfMessages: 1,
          WaitTimeSeconds: 5,
          VisibilityTimeout: 30,
        }),
      );

      const mensagem = recebidas.Messages?.[0] as Message;
      expect(mensagem).toBeDefined();

      // A falha transitoria e injetada no caso de uso: e a unica forma de
      // derrubar a dependencia dentro do teste. O PostgreSQL e o SQS continuam
      // reais, e o que esta sob teste e a decisao do consumidor (sem ack ->
      // retry) - nao o banco.
      const espiao = spyOn(useCase, 'execute').mockImplementation(() => {
        throw new Error('conexao com o banco caiu');
      });

      try {
        expect(await consumidor.processar(mensagem)).toBe('retry');
      } finally {
        espiao.mockRestore();
      }

      // Sem ack: a mensagem continua EM VOO na fila (nao foi apagada, e nao foi
      // para a DLQ).
      const atributos = await client.send(
        new GetQueueAttributesCommand({
          QueueUrl: filas.transactions,
          AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'],
        }),
      );
      expect(atributos.Attributes?.ApproximateNumberOfMessagesNotVisible).toBe('1');
      expect(atributos.Attributes?.ApproximateNumberOfMessages).toBe('0');

      // E nada foi aplicado: sem transacao, sem debito, sem linha no inbox.
      expect(await saldoDe(walletId)).toBe('100.00');
      expect(
        await contar('SELECT count(*)::int AS n FROM wager_transactions WHERE external_transaction_id = $1', [
          externalTransactionId,
        ]),
      ).toBe(0);
      expect(
        await contar('SELECT count(*)::int AS n FROM inbox_messages WHERE message_id = $1', [mensagem.MessageId!]),
      ).toBe(0);

      // Devolvendo a visibilidade, a MESMA mensagem e reentregue e agora conclui.
      await client.send(
        new ChangeMessageVisibilityCommand({
          QueueUrl: filas.transactions,
          ReceiptHandle: mensagem.ReceiptHandle!,
          VisibilityTimeout: 0,
        }),
      );

      const reentrega = await client.send(
        new ReceiveMessageCommand({
          QueueUrl: filas.transactions,
          MaxNumberOfMessages: 1,
          WaitTimeSeconds: 5,
          VisibilityTimeout: 30,
        }),
      );

      const segunda = reentrega.Messages?.[0] as Message;
      expect(segunda).toBeDefined();
      expect(segunda.Body).toBe(mensagem.Body);
      expect(segunda.MessageId).toBe(mensagem.MessageId);

      expect(await consumidor.processar(segunda)).toBe('ack');
      expect(await saldoDe(walletId)).toBe('90.00');
      expect(
        await contar(
          `SELECT count(*)::int AS n FROM inbox_messages WHERE message_id = $1 AND processed_at IS NOT NULL`,
          [segunda.MessageId!],
        ),
      ).toBe(1);
    },
    60_000,
  );

  test(
    '18. commit sem publicar: outra instancia assume a outbox e o evento e publicado uma vez',
    async () => {
      const client = app.get<SQSClient>(SQS_CLIENT);

      // Publisher real (SQS FIFO), mas numa fila que o teste controla - veja
      // `filaDeEventosDoTeste` para o motivo.
      const urlDaFila = await filaDeEventosDoTeste(client);
      const publisher = new PublicadorNaFila(client, urlDaFila);
      await drenarEventos(client, urlDaFila);

      const playerId = `p-crash-${randomUUID()}`;
      const walletId = await criarCarteira(playerId, '100.00');
      const externalTransactionId = `ext-crash-${randomUUID()}`;

      // 1. o PostgreSQL confirma o commit, com os eventos ja gravados na outbox.
      const bet = await enviarTransacao({
        externalTransactionId,
        playerId,
        walletId,
        roundId: 'rodada-crash',
        money: { amount: '10.00', currency: 'BRL' },
      });
      expect(bet.status).toBe(200);
      const transactionId = bet.body.transactionId as string;
      const minhasPendentes = `SELECT id, event_type FROM outbox_messages
                                WHERE published_at IS NULL
                                  AND payload->'data'->>'transactionId' = $1
                                ORDER BY event_type`;

      const linhasNaOutbox: Array<{ id: string; event_type: string }> = await dataSource.query(minhasPendentes, [
        transactionId,
      ]);
      expect(linhasNaOutbox.map((linha) => linha.event_type)).toEqual([
        'WagerTransactionProcessed',
        'WalletBalanceChanged',
      ]);

      // 2. o processo morre aqui, antes de publicar: nada saiu. (No ambiente de
      //    teste nenhum worker fica rodando - e exatamente esse crash.)
      const idsPendentes = linhasNaOutbox.map((linha) => linha.id);
      expect(
        (await drenarEventos(client, urlDaFila)).filter((evento) => idsPendentes.includes(evento.eventId)),
      ).toHaveLength(0);

      // 3. outra instancia assume: um worker NOVO sobre a MESMA outbox.
      const outraInstancia = new OutboxPublisherWorker(dataSource, publisher);
      const pendentesAntes = await contar('SELECT count(*)::int AS n FROM outbox_messages WHERE published_at IS NULL');
      expect(pendentesAntes).toBeGreaterThan(0); // ha evento esperando (o meu inclusive)

      for (let rodada = 0; rodada < 10; rodada += 1) {
        const aindaPendentes: Array<{ id: string }> = await dataSource.query(minhasPendentes, [transactionId]);
        if (aindaPendentes.length === 0) {
          break;
        }
        await outraInstancia.runOnce();
      }

      // 4. o evento foi publicado: as duas linhas viraram mensagem, uma vez cada
      //    (attempts = 1 e nada ficou pendente).
      const publicadas: Array<{ event_type: string; attempts: number; published_at: Date | null }> =
        await dataSource.query(
          `SELECT event_type, attempts, published_at FROM outbox_messages
            WHERE payload->'data'->>'transactionId' = $1
            ORDER BY event_type`,
          [transactionId],
        );
      expect(publicadas.map((linha) => Number(linha.attempts))).toEqual([1, 1]);
      expect(publicadas.every((linha) => linha.published_at !== null)).toBe(true);

      const carga = await drenarEventos(client, urlDaFila);
      const meus = carga.filter(
        (evento) => evento.eventType === 'WagerTransactionProcessed' && evento.data.transactionId === transactionId,
      );
      expect(meus).toHaveLength(1);
      expect(meus[0]!.data.money).toEqual({ amount: '10.00', currency: 'BRL' });

      const eventId = meus[0]!.eventId;
      expect(eventId).toBe(idsPendentes[0]); // o eventId e o id da linha da outbox

      // 5. publicacao duplicada continua segura: a fila FIFO absorve a repeticao
      //    (MessageDeduplicationId = id da linha) e o envelope segue com o mesmo
      //    eventId - por onde um consumidor deduplica fora da janela da fila.
      const lancamentosAntes = await contar('SELECT count(*)::int AS n FROM wallet_ledger_entries WHERE wallet_id = $1', [
        walletId,
      ]);

      await dataSource.query(
        `UPDATE outbox_messages
            SET published_at = NULL, next_attempt_at = NOW()
          WHERE event_type = 'WagerTransactionProcessed'
            AND payload->'data'->>'transactionId' = $1`,
        [transactionId],
      );
      await outraInstancia.runOnce();

      const republicada: Array<{ attempts: number }> = await dataSource.query(
        `SELECT attempts FROM outbox_messages
          WHERE event_type = 'WagerTransactionProcessed'
            AND payload->'data'->>'transactionId' = $1`,
        [transactionId],
      );
      expect(Number(republicada[0]!.attempts)).toBe(2); // a republicacao aconteceu de verdade
      expect((await drenarEventos(client, urlDaFila)).filter((evento) => evento.eventId === eventId)).toHaveLength(0);
      expect(await saldoDe(walletId)).toBe('90.00');
      expect(
        await contar('SELECT count(*)::int AS n FROM wallet_ledger_entries WHERE wallet_id = $1', [walletId]),
      ).toBe(lancamentosAntes);
    },
    90_000,
  );

  test(
    '19. o extrato pagina por cursor, estavel entre chamadas e sem repetir nem perder lancamento',
    async () => {
      const playerId = `p-cursor-${randomUUID()}`;
      const walletId = await criarCarteira(playerId, '100.00');

      for (const indice of [1, 2, 3]) {
        const aposta = await enviarTransacao({
          externalTransactionId: `ext-cursor-${indice}-${randomUUID()}`,
          playerId,
          walletId,
          roundId: 'rodada-cursor',
          money: { amount: '10.00', currency: 'BRL' },
        });
        expect(aposta.status).toBe(200);
      }

      // abertura (CREDIT 100.00) + 3 debitos de 10.00 = 4 lancamentos
      const pagina1 = await api('GET', `/wallets/${walletId}/ledger?limit=2`);
      expect(pagina1.status).toBe(200);
      expect(pagina1.body.entries).toHaveLength(2);
      expect(typeof pagina1.body.nextCursor).toBe('string');
      expect(pagina1.body.nextCursor.length).toBeGreaterThan(0);

      // o cursor e estavel: a mesma pagina, pedida de novo, e identica
      const repetida = await api('GET', `/wallets/${walletId}/ledger?limit=2`);
      expect(repetida.body).toEqual(pagina1.body);

      const pagina2 = await api(
        'GET',
        `/wallets/${walletId}/ledger?cursor=${encodeURIComponent(pagina1.body.nextCursor)}&limit=2`,
      );
      expect(pagina2.status).toBe(200);
      expect(pagina2.body.entries).toHaveLength(2);
      expect(pagina2.body.nextCursor).toBeNull();

      // as duas paginas cobrem o extrato inteiro: sem repetir e sem perder
      const ids = [...pagina1.body.entries, ...pagina2.body.entries].map(
        (entrada: { id: string }) => entrada.id,
      );
      expect(ids).toHaveLength(4);
      expect(new Set(ids).size).toBe(4);

      // ordem do mais novo para o mais antigo, e a serie fecha na abertura
      expect(
        pagina1.body.entries.map((entrada: { balanceAfter: { amount: string } }) => entrada.balanceAfter.amount),
      ).toEqual(['70.00', '80.00']);
      expect(
        pagina2.body.entries.map((entrada: { balanceAfter: { amount: string } }) => entrada.balanceAfter.amount),
      ).toEqual(['90.00', '100.00']);

      const abertura = pagina2.body.entries[1];
      expect(abertura.direction).toBe('CREDIT');
      expect(abertura.balanceBefore.amount).toBe('0.00');

      // cursor invalido e limit fora da faixa sao 400, nao 500
      expect((await api('GET', `/wallets/${walletId}/ledger?cursor=nao-e-cursor`)).status).toBe(400);
      expect((await api('GET', `/wallets/${walletId}/ledger?limit=0`)).status).toBe(400);
    },
    60_000,
  );

  test(
    '20. a reconciliacao sinaliza divergencia e nao corrige nada em silencio',
    async () => {
      const playerId = `p-divergencia-${randomUUID()}`;
      const walletId = await criarCarteira(playerId, '100.00');
      const aposta = await enviarTransacao({
        externalTransactionId: `ext-divergencia-${randomUUID()}`,
        playerId,
        walletId,
        roundId: 'rodada-divergencia',
        money: { amount: '40.00', currency: 'BRL' },
      });
      expect(aposta.status).toBe(200);

      const antes = await api('POST', `/wallets/${walletId}/reconciliation`);
      expect(antes.body.consistent).toBe(true);
      expect(antes.body.difference.amount).toBe('0.00');
      expect(antes.body.checkedEntries).toBe(2);

      // Corrompe o saldo materializado direto no banco (o CHECK so proibe
      // negativo): e assim que uma divergencia aparece em producao.
      await dataSource.query('UPDATE wallets SET balance_amount = 61.00 WHERE id = $1', [walletId]);

      const divergente = await api('POST', `/wallets/${walletId}/reconciliation`);
      expect(divergente.status).toBe(200);
      expect(divergente.body.consistent).toBe(false);
      expect(divergente.body.storedBalance.amount).toBe('61.00');
      expect(divergente.body.calculatedBalance.amount).toBe('60.00');
      expect(divergente.body.difference.amount).toBe('1.00');

      // nao corrige em silencio: o saldo continua como estava e a leitura
      // seguinte sinaliza a mesma divergencia
      expect(await saldoDe(walletId)).toBe('61.00');
      expect((await api('POST', `/wallets/${walletId}/reconciliation`)).body.consistent).toBe(false);

      // e a divergencia ficou contada em metrica (uma por leitura divergente),
      // alem de logada
      const metricas = await (await fetch(`${base}/metrics`)).text();
      expect(metricas).toContain(`reconciliation_divergences_total{walletId="${walletId}"} 2`);

      // devolve o extrato como fonte da verdade
      await dataSource.query('UPDATE wallets SET balance_amount = 60.00 WHERE id = $1', [walletId]);
      expect((await api('POST', `/wallets/${walletId}/reconciliation`)).body.consistent).toBe(true);
    },
    60_000,
  );

  test(
    '21. o encerramento espera a mensagem em voo terminar antes de fechar',
    async () => {
      const client = app.get<SQSClient>(SQS_CLIENT);
      const useCase = app.get(ProcessWagerTransactionUseCase);
      const filas = await ensureQueues(client);

      await drenarFila(client, filas.transactions);

      const playerId = `p-encerramento-${randomUUID()}`;
      const walletId = await criarCarteira(playerId, '100.00');
      const externalTransactionId = `ext-encerramento-${randomUUID()}`;

      // Barreira: segura a mensagem em processamento para dar como provar que o
      // encerramento ESPERA por ela, em vez de fechar por baixo.
      let entrouEmVoo: () => void = () => {};
      const emVoo = new Promise<void>((resolve) => {
        entrouEmVoo = resolve;
      });
      let liberar: () => void = () => {};
      const barreira = new Promise<void>((resolve) => {
        liberar = resolve;
      });

      const original = useCase.execute.bind(useCase);
      const espiao = spyOn(useCase, 'execute').mockImplementation(async (input, options) => {
        entrouEmVoo();
        await barreira;
        return original(input, options);
      });

      // Consumidor com cliente proprio: e o loop de verdade que marca a mensagem
      // como "em voo" e que o stop() precisa drenar. O `main.ts` faz esse mesmo
      // caminho a partir de SIGTERM (app.close() -> onModuleDestroy -> stop()); o
      // sinal em si nao e entregavel neste host, entao o teste chama o metodo que
      // o sinal chama.
      const trabalhadores = config.workers as unknown as { enabled: boolean };
      const clienteDoEncerramento = createSqsClient();
      const encerrando = new SqsConsumerService(dataSource, useCase, clienteDoEncerramento);

      try {
        trabalhadores.enabled = true;
        await encerrando.onModuleInit();
        trabalhadores.enabled = false; // o loop ja foi iniciado

        const envio = await client.send(
          new SendMessageCommand({
            QueueUrl: filas.transactions,
            MessageBody: JSON.stringify({
              messageId: `msg-encerramento-${randomUUID()}`,
              type: 'WagerTransactionRequested',
              occurredAt: new Date().toISOString(),
              data: {
                providerId: 'provider-a',
                externalTransactionId,
                idempotencyKey: `provider-a:${externalTransactionId}`,
                playerId,
                walletId,
                roundId: 'rodada-encerramento',
                gameId: 'fortune-chimp',
                kind: 'BET',
                money: { amount: '10.00', currency: 'BRL' },
              },
            }),
            MessageGroupId: playerId,
            MessageDeduplicationId: `msg-encerramento-${randomUUID()}`,
          }),
        );

        await emVoo; // a mensagem esta sendo processada agora

        let encerrou = false;
        const parada = encerrando.stop().then(() => {
          encerrou = true;
        });

        await sleep(50);
        expect(encerrou).toBe(false); // stop() nao retornou: esta esperando a mensagem

        liberar();
        await parada;

        // quando o encerramento terminou, o efeito ja estava commitado e o inbox
        // marcado: nada ficou pela metade
        expect(await saldoDe(walletId)).toBe('90.00');
        expect(
          await contar(
            `SELECT count(*)::int AS n FROM inbox_messages WHERE message_id = $1 AND processed_at IS NOT NULL`,
            [envio.MessageId!],
          ),
        ).toBe(1);
      } finally {
        espiao.mockRestore();
        trabalhadores.enabled = false;
        await encerrando.stop();
      }
    },
    90_000,
  );
});