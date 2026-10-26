import { SendMessageCommand, type SQSClient } from '@aws-sdk/client-sqs';

import { config } from '../../config';
import { logger } from '../observability/logger';

import { ensureQueues } from './sqs';

/** Evento que sai da outbox para o mundo. */
export interface OutboundEvent {
  id: string;
  aggregateId: string;
  eventType: string;
  payload: Record<string, unknown>;
}

/**
 * Porta de publicacao de eventos.
 *
 * O worker da outbox depende desta interface, nao do SDK: trocar SQS por outro
 * broker e trocar a implementacao injetada.
 */
export interface EventPublisher {
  publish(event: OutboundEvent): Promise<void>;
}

export const EVENT_PUBLISHER = 'EVENT_PUBLISHER';

/**
 * Implementacao real: SQS FIFO.
 *
 * MessageGroupId = id do agregado (mantem a ordem por carteira/transacao).
 * MessageDeduplicationId = id da linha da outbox (a dedup da fila e otimizacao:
 * a garantia real continua sendo a tabela outbox_messages).
 */
export class SqsEventPublisher implements EventPublisher {
  private fila: Promise<string> | null = null;

  constructor(private readonly client: SQSClient) {}

  async publish(event: OutboundEvent): Promise<void> {
    const queueUrl = await this.filaUrl();

    await this.client.send(
      new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: JSON.stringify(event.payload),
        MessageGroupId: event.aggregateId,
        MessageDeduplicationId: event.id,
      }),
    );
  }

  private filaUrl(): Promise<string> {
    // Resolve a fila na primeira publicacao (o boot nao depende da ordem dos
    // providers do Nest).
    this.fila ??= ensureQueues(this.client).then((filas) => filas.events);
    return this.fila;
  }
}

/**
 * Alternativa quando o SQS esta desligado (SQS_ENABLED=false): o evento e
 * apenas logado, o que permite rodar a API sem infraestrutura de fila.
 */
export class LogEventPublisher implements EventPublisher {
  async publish(event: OutboundEvent): Promise<void> {
    logger.info('event_published', { eventType: event.eventType, eventId: event.id, aggregateId: event.aggregateId });
  }
}

export function criarPublisher(client: SQSClient | null): EventPublisher {
  if (!config.sqs.enabled || client === null) {
    return new LogEventPublisher();
  }

  return new SqsEventPublisher(client);
}