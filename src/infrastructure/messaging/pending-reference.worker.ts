import { Inject, Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import type { DataSource } from 'typeorm';

import { config } from '../../config';
import { FailureCode } from '../../domain/errors';
import { WagerStatus } from '../../domain/wager-transaction';
import { ProcessWagerTransactionUseCase } from '../../application/process-wager-transaction.use-case';
import { DATA_SOURCE, withTransaction } from '../database/data-source';
import { logger } from '../observability/logger';
import { metrics } from '../observability/metrics';

/**
 * Resolve reversoes que chegaram ANTES da transacao referenciada
 * (PENDING_REFERENCE), com backoff exponencial e TTL.
 *
 * O worker NAO reimplementa regra de negocio: ele apenas chama o mesmo caso de
 * uso em modo `retryPendingReference`. Assim a taxonomia de erro e as
 * validacoes sao exatamente as mesmas da entrada HTTP.
 */
@Injectable()
export class PendingReferenceWorker implements OnModuleInit, OnModuleDestroy {
  private timer: ReturnType<typeof setInterval> | null = null;
  private parando = false;
  private executando = false;

  constructor(
    @Inject(DATA_SOURCE) private readonly dataSource: DataSource,
    private readonly useCase: ProcessWagerTransactionUseCase,
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
    this.timer = setInterval(() => void this.tick(), config.workers.pendingReferencePollIntervalMs);
    logger.info('pending_reference_worker_started', {
      intervalMs: config.workers.pendingReferencePollIntervalMs,
      maxAttempts: config.workers.pendingReferenceMaxAttempts,
    });
  }

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
      logger.error('pending_reference_worker_error', { error: (erro as Error).message });
    } finally {
      this.executando = false;
    }
  }

  /**
   * Um ciclo.
   *
   * O claim usa `FOR UPDATE SKIP LOCKED` para que duas instancias nao peguem as
   * mesmas linhas. A transacao de claim e CURTA (commita na hora) porque a
   * serializacao real de cada carteira acontece dentro do caso de uso, no
   * `SELECT ... FOR UPDATE` da wallet. Se duas instancias pegarem a mesma linha
   * (uma apos o claim da outra), a segunda apenas ve o estado ja decidido e nao
   * faz nada - o processamento e idempotente por construcao.
   */
  async runOnce(): Promise<number> {
    const linhas = await withTransaction(this.dataSource, (qr) =>
      qr.query(
        `SELECT id
           FROM wager_transactions
          WHERE status = 'PENDING_REFERENCE'
            AND next_retry_at <= NOW()
          ORDER BY next_retry_at
          LIMIT $1
          FOR UPDATE SKIP LOCKED`,
        [config.workers.pendingReferenceBatchSize],
      ),
    ) as Array<{ id: string }>;

    let resolvidas = 0;

    for (const linha of linhas) {
      try {
        const resultado = await this.useCase.retryPendingReference(linha.id);

        if (!resultado) {
          continue;
        }

        resolvidas += 1;
        metrics.increment('pending_reference_attempts_total', { status: resultado.status });

        if (resultado.status === WagerStatus.Processed) {
          logger.info('pending_reference_resolved', { transactionId: linha.id });
        }

        if (resultado.failureCode === FailureCode.REFERENCE_NOT_FOUND_TIMEOUT) {
          logger.warn('pending_reference_expired', { transactionId: linha.id });
        }
      } catch (erro) {
        logger.error('pending_reference_failed', {
          transactionId: linha.id,
          error: (erro as Error).message,
        });
      }
    }

    return resolvidas;
  }
}