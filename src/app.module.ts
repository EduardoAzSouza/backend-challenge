import { MiddlewareConsumer, Module, type NestModule } from '@nestjs/common';
import { SQSClient } from '@aws-sdk/client-sqs';
import type { DataSource } from 'typeorm';

import { ProcessWagerTransactionUseCase } from './application/process-wager-transaction.use-case';
import { WalletUseCases } from './application/wallet.use-cases';
import { config } from './config';
import { ProviderIdentityGuard } from './infrastructure/auth/provider-identity.guard';
import { DATA_SOURCE, createDataSource } from './infrastructure/database/data-source';
import { EVENT_PUBLISHER, criarPublisher, type EventPublisher } from './infrastructure/messaging/event-publisher';
import { OutboxPublisherWorker } from './infrastructure/messaging/outbox-publisher.worker';
import { PendingReferenceWorker } from './infrastructure/messaging/pending-reference.worker';
import { SqsConsumerService } from './infrastructure/messaging/sqs-consumer.service';
import { SQS_CLIENT, createSqsClient } from './infrastructure/messaging/sqs';
import { CorrelationMiddleware } from './infrastructure/observability/correlation.middleware';
import { HealthController, MetricsController } from './presentation/health.controller';
import { ProviderTransactionsController, WageringController } from './presentation/wagering.controller';
import { WalletsController } from './presentation/wallets.controller';

/**
 * Composicao da aplicacao.
 *
 * Os casos de uso sao criados por fábrica (e nao por `@Injectable`) para que as
 * camadas de dominio e aplicacao continuem SEM importar NestJS: quem conhece o
 * framework e so a infraestrutura e a apresentacao.
 */
@Module({
  controllers: [WalletsController, WageringController, ProviderTransactionsController, HealthController, MetricsController],
  providers: [
    {
      provide: DATA_SOURCE,
      useFactory: async (): Promise<DataSource> => {
        const dataSource = createDataSource();
        await dataSource.initialize();
        return dataSource;
      },
    },
    {
      provide: SQS_CLIENT,
      useFactory: (): SQSClient | null => (config.sqs.enabled ? createSqsClient() : null),
    },
    {
      provide: EVENT_PUBLISHER,
      useFactory: (client: SQSClient | null): EventPublisher => criarPublisher(client),
      inject: [SQS_CLIENT],
    },
    {
      provide: ProcessWagerTransactionUseCase,
      useFactory: (dataSource: DataSource) => new ProcessWagerTransactionUseCase(dataSource),
      inject: [DATA_SOURCE],
    },
    {
      provide: WalletUseCases,
      useFactory: (dataSource: DataSource) => new WalletUseCases(dataSource),
      inject: [DATA_SOURCE],
    },
    SqsConsumerService,
    OutboxPublisherWorker,
    PendingReferenceWorker,
    ProviderIdentityGuard,
    CorrelationMiddleware,
  ],
  exports: [DATA_SOURCE, ProcessWagerTransactionUseCase, WalletUseCases],
})
export class AppModule implements NestModule {
  configure(consumer: MiddlewareConsumer): void {
    consumer.apply(CorrelationMiddleware).forRoutes('*');
  }
}