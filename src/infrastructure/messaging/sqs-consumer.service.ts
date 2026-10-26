import { randomUUID } from 'node:crypto';

import { Inject, Injectable, type OnModuleDestroy, type OnModuleInit } from '@nestjs/common';
import {
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  type Message,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import type { DataSource } from 'typeorm';

import {
  ProcessWagerTransactionUseCase,
  type ProcessResult,
} from '../../application/process-wager-transaction.use-case';
import { sha256 } from '../../application/canonical-hash';
import { config } from '../../config';
import { DomainError } from '../../domain/errors';
import { WagerKind } from '../../domain/wager-transaction';
import { DATA_SOURCE } from '../database/data-source';
import { logger } from '../observability/logger';
import { metrics } from '../observability/metrics';

import { ensureQueues, SQS_CLIENT } from './sqs';

/** Formato da mensagem que chega na fila. */
export interface SqsWagerMessage {
  messageId?: string;
  type?: string;
  occurredAt?: string;
  correlationId?: string;
  data: {
    providerId: string;
    externalTransactionId: string;
    idempotencyKey?: string;
    playerId: string;
    walletId: string;
    roundId: string;
    gameId: string;
    kind: string;
    money: { amount: string; currency: string };
    referenceExternalTransactionId?: string | null;
  };
}

/**
 * Consumidor de `wager-transactions.fifo`.
 *
 * Regras:
 *  - reusa o MESMO caso de uso da entrada HTTP;
 *  - deduplica pelo inbox persistente por (consumerName, messageId);
 *  - so da ACK (DeleteMessage) DEPOIS do commit;
 *  - erro de negocio (4xx) vai para a DLQ; erro transitorio NAO recebe ACK e a
 *    mensagem volta pelo visibility timeout; REJECTED nao e erro, ja foi
 *    commitado e por isso recebe ACK;
 *  - em SIGTERM o loop para e as mensagens em voo terminam antes de fechar.
 */
@Injectable()
export class SqsConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly client: SQSClient | null;
  private filaUrl: string | null = null;
  private dlqUrl: string | null = null;

  private parando = false;
  private ciclo: Promise<void> | null = null;
  private emVoo = 0;

  constructor(
    @Inject(DATA_SOURCE) private readonly dataSource: DataSource,
    private readonly useCase: ProcessWagerTransactionUseCase,
    @Inject(SQS_CLIENT) client: SQSClient | null,
  ) {
    this.client = client;
  }

  /** Cliente garantidamente disponivel (usado depois da checagem de configuracao). */
  private get sqs(): SQSClient {
    if (!this.client) {
      throw new Error('SQS desabilitado (SQS_ENABLED=false)');
    }
    return this.client;
  }

  async onModuleInit(): Promise<void> {
    if (!config.sqs.enabled || !this.client) {
      logger.warn('sqs_consumer_disabled', { reason: 'SQS_ENABLED=false' });
      return;
    }

    const filas = await ensureQueues(this.sqs);
    this.filaUrl = filas.transactions;
    this.dlqUrl = filas.transactionsDlq;

    if (!config.workers.enabled) {
      logger.info('sqs_consumer_idle', { reason: 'WORKERS_ENABLED=false' });
      return;
    }

    this.ciclo = this.loop();
    logger.info('sqs_consumer_started', { queue: config.sqs.transactionsQueue });
  }

  async onModuleDestroy(): Promise<void> {
    await this.stop();
  }

  async stop(): Promise<void> {
    this.parando = true;
    await this.ciclo;
    this.ciclo = null;

    // Espera as mensagens em voo terminarem.
    while (this.emVoo > 0) {
      await new Promise((resolve) => setTimeout(resolve, 20));
    }

    this.client?.destroy();
  }

  private async loop(): Promise<void> {
    while (!this.parando) {
      try {
        const resposta = await this.sqs.send(
          new ReceiveMessageCommand({
            QueueUrl: this.filaUrl!,
            MaxNumberOfMessages: config.sqs.maxMessages,
            WaitTimeSeconds: config.sqs.waitTimeSeconds,
            VisibilityTimeout: config.sqs.visibilityTimeoutSeconds,
          }),
        );

        for (const mensagem of resposta.Messages ?? []) {
          this.emVoo += 1;
          try {
            await this.processar(mensagem);
          } finally {
            this.emVoo -= 1;
          }
        }
      } catch (erro) {
        logger.error('sqs_receive_failed', { error: (erro as Error).message });
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    }
  }

  /** Processa UMA mensagem. Publico para permitir teste deterministico. */
  async processar(mensagem: Message): Promise<'ack' | 'retry' | 'dlq'> {
    const messageId = mensagem.MessageId ?? randomUUID();

    let corpo: SqsWagerMessage;
    try {
      corpo = JSON.parse(mensagem.Body ?? 'null') as SqsWagerMessage;
      if (!corpo || typeof corpo !== 'object' || !corpo.data) {
        throw new Error('payload sem o campo "data"');
      }
    } catch (erro) {
      await this.enviarParaDlq(mensagem, `payload invalido: ${(erro as Error).message}`);
      return 'dlq';
    }

    const dados = corpo.data;
    const idempotencyKey = dados.idempotencyKey ?? `${dados.providerId}:${dados.externalTransactionId}`;

    try {
      const resultado: ProcessResult = await this.useCase.execute(
        {
          providerId: dados.providerId,
          externalTransactionId: dados.externalTransactionId,
          playerId: dados.playerId,
          walletId: dados.walletId,
          roundId: dados.roundId,
          gameId: dados.gameId,
          kind: dados.kind as WagerKind,
          money: dados.money,
          referenceExternalTransactionId: dados.referenceExternalTransactionId ?? null,
          idempotencyKey,
        },
        {
          // O inbox entra na MESMA transacao SQL do efeito financeiro.
          inbox: {
            messageId,
            consumerName: config.sqs.consumerName,
            payloadHash: sha256(dados),
          },
          correlationId: corpo.correlationId ?? null,
        },
      );

      // A transacao commitou (inclusive quando o resultado e REJECTED ou 202):
      // so agora a mensagem pode ser removida da fila.
      await this.confirmar(mensagem);

      return 'ack';
    } catch (erro) {
      return this.tratarErro(mensagem, erro, messageId);
    }
  }

  private async tratarErro(
    mensagem: Message,
    erro: unknown,
    messageId: string,
  ): Promise<'retry' | 'dlq'> {
    // 400/404/409: reenviar nao vai ajudar - mensagem malformada ou conflito.
    if (erro instanceof DomainError && erro.httpStatus < 500) {
      await this.enviarParaDlq(mensagem, `${erro.code}: ${erro.message}`);
      return 'dlq';
    }

    // Transitorio (banco indisponivel, timeout, deadlock): sem ACK. A mensagem
    // volta depois do visibility timeout e, passando de 5 recebimentos, a
    // RedrivePolicy da fila a joga na DLQ.
    logger.warn('sqs_message_retry', { messageId, error: (erro as Error).message });
    return 'retry';
  }

  private async confirmar(mensagem: Message): Promise<void> {
    await this.sqs.send(
      new DeleteMessageCommand({ QueueUrl: this.filaUrl!, ReceiptHandle: mensagem.ReceiptHandle! }),
    );
  }

  private async enviarParaDlq(mensagem: Message, motivo: string): Promise<void> {
    if (this.dlqUrl) {
      await this.sqs.send(
        new SendMessageCommand({
          QueueUrl: this.dlqUrl,
          MessageBody: mensagem.Body ?? '',
          MessageGroupId: 'dlq',
          MessageDeduplicationId: mensagem.MessageId ?? randomUUID(),
        }),
      );
    }

    await this.confirmar(mensagem);

    metrics.increment('sqs_dlq_total');
    logger.error('sqs_message_to_dlq', { messageId: mensagem.MessageId, reason: motivo });
  }
}