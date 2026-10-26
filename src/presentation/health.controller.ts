import { Controller, Get, Header, HttpException, HttpStatus, Inject } from '@nestjs/common';
import { GetQueueAttributesCommand, type SQSClient } from '@aws-sdk/client-sqs';
import type { DataSource } from 'typeorm';

import { config } from '../config';
import { DATA_SOURCE } from '../infrastructure/database/data-source';
import { ensureQueues, SQS_CLIENT } from '../infrastructure/messaging/sqs';
import { logger } from '../infrastructure/observability/logger';
import { metrics } from '../infrastructure/observability/metrics';

/**
 * Health checks.
 *
 * Nao passam pelo guard de identidade e por isso nao usam @UseGuards.
 */
@Controller('health')
export class HealthController {
  constructor(
    @Inject(DATA_SOURCE) private readonly dataSource: DataSource,
    @Inject(SQS_CLIENT) private readonly sqsClient: SQSClient | null,
  ) {}

  /** Liveness: o processo esta vivo. Nao toca em dependencia externa. */
  @Get('live')
  live(): { status: string; instance: string; uptimeSeconds: number } {
    return {
      status: 'live',
      instance: config.instanceId,
      uptimeSeconds: Math.round(process.uptime()),
    };
  }

  /** Readiness: PostgreSQL e SQS utilizaveis. 503 quando algum nao estiver. */
  @Get('ready')
  async ready(): Promise<{ status: string; checks: Record<string, boolean>; instance: string }> {
    const checks = { database: false, sqs: false };

    try {
      await this.dataSource.query('SELECT 1');
      checks.database = true;
    } catch (erro) {
      logger.warn('readiness_database_failed', { error: (erro as Error).message });
    }

    if (config.sqs.enabled && this.sqsClient) {
      try {
        const filas = await ensureQueues(this.sqsClient);
        await this.sqsClient.send(
          new GetQueueAttributesCommand({ QueueUrl: filas.transactions, AttributeNames: ['QueueArn'] }),
        );
        checks.sqs = true;
      } catch (erro) {
        logger.warn('readiness_sqs_failed', { error: (erro as Error).message });
      }
    } else {
      checks.sqs = true; // SQS desligado por configuracao nao e indisponibilidade
    }

    if (!checks.database || !checks.sqs) {
      throw new HttpException({ status: 'not_ready', checks }, HttpStatus.SERVICE_UNAVAILABLE);
    }

    return { status: 'ready', checks, instance: config.instanceId };
  }
}

/** GET /metrics - formato de texto do Prometheus. */
@Controller()
export class MetricsController {
  @Get('metrics')
  @Header('Content-Type', 'text/plain; charset=utf-8')
  expor(): string {
    return metrics.toPrometheus();
  }
}