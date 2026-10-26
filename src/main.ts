import 'reflect-metadata';

import { NestFactory } from '@nestjs/core';
import type { DataSource } from 'typeorm';

import { AppModule } from './app.module';
import { config } from './config';
import { DATA_SOURCE } from './infrastructure/database/data-source';
import { logger } from './infrastructure/observability/logger';
import { HttpExceptionFilter } from './presentation/http-exception.filter';

async function bootstrap(): Promise<void> {
  // `logger: false` porque o log da aplicacao e o JSON estruturado proprio.
  // `abortOnError: false` para que uma falha de bootstrap (ex.: erro de DI)
  // chegue ao catch e seja logada em vez de matar o processo em silencio.
  const app = await NestFactory.create(AppModule, { logger: false, abortOnError: false });

  app.useGlobalFilters(new HttpExceptionFilter());
  app.enableShutdownHooks();

  await app.listen(config.port);

  logger.info('http_listening', {
    port: config.port,
    workers: config.workers.enabled,
    sqs: config.sqs.enabled,
  });

  const dataSource = app.get<DataSource>(DATA_SOURCE);

  /**
   * SIGTERM/SIGINT.
   *
   * `app.close()` dispara o onModuleDestroy dos workers: o consumidor SQS para
   * de receber e espera as mensagens em voo terminarem, e os workers de outbox
   * e PENDING_REFERENCE encerram o ciclo atual. So depois o pool e fechado.
   */
  for (const sinal of ['SIGTERM', 'SIGINT'] as const) {
    process.on(sinal, () => {
      logger.info('shutdown_started', { signal: sinal });

      void app
        .close()
        .then(() => dataSource.destroy())
        .then(() => {
          logger.info('shutdown_finished', { signal: sinal });
          process.exit(0);
        })
        .catch((erro: Error) => {
          logger.error('shutdown_failed', { error: erro.message });
          process.exit(1);
        });
    });
  }
}

bootstrap().catch((erro: Error) => {
  logger.error('bootstrap_failed', { error: erro.message, stack: erro.stack });
  // `exitCode` (e nao process.exit) para garantir que o log acima foi escrito.
  process.exitCode = 1;
});