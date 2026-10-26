import {
  CreateQueueCommand,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';

import { config } from '../../config';

/**
 * Cliente SQS apontando para o LocalStack (endpoint configuravel).
 * Credenciais dummy: o LocalStack autentica qualquer coisa.
 */
export function createSqsClient(): SQSClient {
  return new SQSClient({
    region: config.sqs.region,
    endpoint: config.sqs.endpoint,
    credentials: {
      accessKeyId: config.sqs.accessKeyId,
      secretAccessKey: config.sqs.secretAccessKey,
    },
  });
}

export interface Filas {
  transactions: string;
  transactionsDlq: string;
  events: string;
}

/** Token do cliente SQS no Nest (null quando SQS_ENABLED=false). */
export const SQS_CLIENT = 'SQS_CLIENT';

let filasEmCache: Promise<Filas> | null = null;

/**
 * Cria as filas se ainda nao existirem (idempotente e memoizado no processo).
 *
 * `wager-transactions.fifo` tem RedrivePolicy para a DLQ depois de 5 recebimentos,
 * como pede a especificacao.
 */
export function ensureQueues(client: SQSClient = createSqsClient()): Promise<Filas> {
  filasEmCache ??= criarFilas(client);
  return filasEmCache;
}

async function criarFilas(client: SQSClient): Promise<Filas> {
  const dlq = await criarFila(client, config.sqs.transactionsDlq, { fifo: true });
  const transactions = await criarFila(client, config.sqs.transactionsQueue, {
    fifo: true,
    deadLetterArn: dlq.arn,
    maxReceiveCount: 5,
  });
  const events = await criarFila(client, config.sqs.eventsQueue, { fifo: true });

  return { transactions: transactions.url, transactionsDlq: dlq.url, events: events.url };
}

async function criarFila(
  client: SQSClient,
  nome: string,
  opcoes: { fifo?: boolean; deadLetterArn?: string; maxReceiveCount?: number },
): Promise<{ url: string; arn: string }> {
  const attributes: Record<string, string> = {};

  if (opcoes.fifo) {
    attributes.FifoQueue = 'true';
    attributes.ContentBasedDeduplication = 'false';
    attributes.VisibilityTimeout = String(config.sqs.visibilityTimeoutSeconds);
  }

  if (opcoes.deadLetterArn && opcoes.maxReceiveCount) {
    attributes.RedrivePolicy = JSON.stringify({
      deadLetterTargetArn: opcoes.deadLetterArn,
      maxReceiveCount: opcoes.maxReceiveCount,
    });
  }

  await client.send(new CreateQueueCommand({ QueueName: nome, Attributes: attributes }));

  const { QueueUrl } = await client.send(new GetQueueUrlCommand({ QueueName: nome }));
  const { Attributes } = await client.send(
    new GetQueueAttributesCommand({ QueueUrl: QueueUrl!, AttributeNames: ['QueueArn'] }),
  );

  return { url: normalizarUrl(QueueUrl!), arn: Attributes?.QueueArn ?? '' };
}

/**
 * O LocalStack devolve URLs com o host interno dele
 * (`sqs.us-east-1.localhost.localstack.cloud:4566`). Trocamos pelo endpoint
 * configurado para nao depender de DNS.
 */
function normalizarUrl(url: string): string {
  const alvo = new URL(url);
  const endpoint = new URL(config.sqs.endpoint);

  if (alvo.host !== endpoint.host) {
    alvo.protocol = endpoint.protocol;
    alvo.host = endpoint.host;
  }

  return alvo.toString();
}