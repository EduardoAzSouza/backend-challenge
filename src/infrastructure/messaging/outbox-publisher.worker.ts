import { Inject, Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import type { DataSource, QueryRunner } from 'typeorm';

import { config } from '../../config';
import { OutboxMessage } from '../../domain/outbox-message';
import { DATA_SOURCE, withTransaction } from '../database/data-source';
import { logger } from '../observability/logger';
import { metrics } from '../observability/metrics';

import { EVENT_PUBLISHER, type EventPublisher } from './event-publisher';

interface OutboxRow {
  id: string;
  aggregate_id: string;
  event_type: string;
  payload: Record<string, unknown>;
  correlation_id: string | null;
  occurred_at: Date;
  attempts: number;
  next_attempt_at: Date;
  published_at: Date | null;
}

/**
 * Publica os eventos pendentes da outbox.
 *
 * Duas ou mais instancias podem rodar este worker ao mesmo tempo: o
 * `FOR UPDATE SKIP LOCKED` garante que cada uma pega um lote DISJUNTO na
 * reivindicacao. A publicacao acontece fora da transacao, entao um crash entre
 * publicar e marcar faz outra instancia republicar - quem absorve a repeticao e
 * o consumidor, pelo inbox.
 */
@Injectable()
export class OutboxPublisherWorker implements OnModuleInit, OnModuleDestroy {
  private timer: ReturnType<typeof setInterval> | null = null;
  private parando = false;
  private executando = false;

  constructor(
    @Inject(DATA_SOURCE) private readonly dataSource: DataSource,
    @Inject(EVENT_PUBLISHER) private readonly publisher: EventPublisher,
  ) {}

  onModuleInit(): void {
    if (!config.workers.enabled) {
      return;
    }
    this.start();
  }

  start(): void {
    if (this.timer) {
      return;
    }

    this.parando = false;
    this.timer = setInterval(() => void this.tick(), config.workers.outboxPollIntervalMs);
    logger.info('outbox_publisher_started', { intervalMs: config.workers.outboxPollIntervalMs });
  }

  /** Para o loop e espera o ciclo em andamento terminar (shutdown limpo). */
  async stop(): Promise<void> {
    this.parando = true;

    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }

    while (this.executando) {
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  }

  async onModuleDestroy(): Promise<void> {
    await this.stop();
  }

  private async tick(): Promise<void> {
    if (this.parando || this.executando) {
      return;
    }

    this.executando = true;
    try {
      await this.runOnce();
    } catch (erro) {
      logger.error('outbox_publisher_error', { error: (erro as Error).message });
    } finally {
      this.executando = false;
    }
  }

  /** Um ciclo: pega o lote vencido, publica e marca (ou reagenda). */
  async runOnce(): Promise<number> {
    const linhas = await withTransaction(this.dataSource, (qr) => this.claim(qr));

    for (const linha of linhas) {
      await this.publicarLinha(linha);
    }

    await this.atualizarLag();
    return linhas.length;
  }

  private async claim(qr: QueryRunner): Promise<OutboxRow[]> {
    return qr.query(
      `SELECT id, aggregate_id, event_type, payload, correlation_id, occurred_at, attempts, next_attempt_at, published_at
         FROM outbox_messages
        WHERE published_at IS NULL
          AND next_attempt_at <= NOW()
        ORDER BY next_attempt_at, occurred_at
        LIMIT $1
        FOR UPDATE SKIP LOCKED`,
      [config.workers.outboxBatchSize],
    );
  }

  /**
   * Publica e marca a linha.
   *
   * A publicacao acontece DEPOIS do commit da transacao financeira (a linha so
   * existe porque a transacao commitou). Se o processo morrer entre publicar e
   * marcar, outra instancia republica - por isso o consumidor precisa ser
   * idempotente, via inbox.
   */
  private async publicarLinha(linha: OutboxRow): Promise<void> {
    const message = OutboxMessage.rehydrate({
      id: linha.id,
      aggregateId: linha.aggregate_id,
      eventType: linha.event_type,
      payload: linha.payload,
      correlationId: linha.correlation_id,
      occurredAt: linha.occurred_at,
      attempts: linha.attempts,
      nextAttemptAt: linha.next_attempt_at,
      publishedAt: linha.published_at,
    });

    const agora = new Date();

    try {
      await this.publisher.publish({
        id: message.id,
        aggregateId: message.aggregateId,
        eventType: message.eventType,
        payload: message.payload as Record<string, unknown>,
      });

      message.markPublished(agora);

      await this.dataSource.query(
        `UPDATE outbox_messages
            SET published_at = $2, attempts = $3, next_attempt_at = $2
          WHERE id = $1 AND published_at IS NULL`,
        [message.id, agora, message.attempts],
      );
    } catch (erro) {
      message.scheduleRetry(agora);

      await this.dataSource.query(
        `UPDATE outbox_messages SET attempts = $2, next_attempt_at = $3 WHERE id = $1 AND published_at IS NULL`,
        [message.id, message.attempts, message.nextAttemptAt],
      );

      metrics.increment('outbox_publish_failures_total', { eventType: message.eventType });
      logger.warn('outbox_publish_failed', {
        outboxId: message.id,
        eventType: message.eventType,
        attempts: message.attempts,
        error: (erro as Error).message,
      });
    }
  }

  /** Tempo (ms) do evento nao publicado mais antigo - o "atraso da outbox". */
  private async atualizarLag(): Promise<void> {
    const linhas: Array<{ lag_ms: number | null }> = await this.dataSource.query(
      `SELECT COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(occurred_at))) * 1000, 0)::int AS lag_ms
         FROM outbox_messages
        WHERE published_at IS NULL`,
    );

    metrics.gauge('outbox_lag_ms', Number(linhas[0]?.lag_ms ?? 0));
  }
}