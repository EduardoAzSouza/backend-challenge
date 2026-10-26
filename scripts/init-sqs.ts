import 'reflect-metadata';

import { config } from '../src/config';
import { ensureQueues } from '../src/infrastructure/messaging/sqs';

/**
 * Cria as filas no SQS (LocalStack): `wager-transactions.fifo`,
 * `wager-transactions-dlq.fifo` (RedrivePolicy maxReceiveCount=5) e
 * `wager-events.fifo`.
 *
 * A aplicacao tambem faz isso no boot; o script existe para preparar a
 * infraestrutura antes de subir o servico.
 */
async function main(): Promise<void> {
  const filas = await ensureQueues();

  console.log(`SQS em ${config.sqs.endpoint}`);
  console.log(`  fila de entrada ............ ${config.sqs.transactionsQueue} -> ${filas.transactions}`);
  console.log(`  fila morta (dlq) ........... ${config.sqs.transactionsDlq} -> ${filas.transactionsDlq}`);
  console.log(`  fila de eventos ............ ${config.sqs.eventsQueue} -> ${filas.events}`);
}

main().catch((erro: Error) => {
  console.error('falha ao criar as filas:', erro.message);
  process.exit(1);
});