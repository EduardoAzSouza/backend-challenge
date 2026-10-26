/**
 * Configuracao lida do ambiente no bootstrap.
 * Os defaults apontam para o docker-compose do repositorio, entao
 * `bun run start` funciona sem nenhum setup extra.
 */
export const config = {
  port: Number(process.env.PORT ?? 3000),
  instanceId: process.env.APP_INSTANCE_ID ?? `instance-${process.pid}`,
  databaseUrl: process.env.DATABASE_URL ?? 'postgres://jungle:jungle_password@localhost:5432/wagering_db',
  poolMax: Number(process.env.PG_POOL_MAX ?? 20),
  logLevel: (process.env.LOG_LEVEL ?? 'info') as 'debug' | 'info' | 'warn' | 'error',

  sqs: {
    enabled: (process.env.SQS_ENABLED ?? 'true') === 'true',
    endpoint: process.env.AWS_ENDPOINT_URL ?? 'http://localhost:4566',
    region: process.env.AWS_REGION ?? 'us-east-1',
    accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test',
    transactionsQueue: process.env.SQS_QUEUE_TRANSACTIONS ?? 'wager-transactions.fifo',
    transactionsDlq: process.env.SQS_QUEUE_TRANSACTIONS_DLQ ?? 'wager-transactions-dlq.fifo',
    eventsQueue: process.env.SQS_QUEUE_EVENTS ?? 'wager-events.fifo',
    /** Nome do consumidor gravado em inbox_messages.consumer_name. */
    consumerName: process.env.SQS_CONSUMER_NAME ?? 'wager-transactions-consumer',
    waitTimeSeconds: Number(process.env.SQS_WAIT_TIME_SECONDS ?? 1),
    visibilityTimeoutSeconds: Number(process.env.SQS_VISIBILITY_TIMEOUT_SECONDS ?? 30),
    maxMessages: Number(process.env.SQS_MAX_MESSAGES ?? 10),
  },

  workers: {
    enabled: (process.env.WORKERS_ENABLED ?? 'true') === 'true',
    outboxBatchSize: Number(process.env.OUTBOX_BATCH_SIZE ?? 50),
    outboxPollIntervalMs: Number(process.env.OUTBOX_POLL_INTERVAL_MS ?? 200),
    pendingReferenceBatchSize: Number(process.env.PENDING_REFERENCE_BATCH_SIZE ?? 20),
    pendingReferencePollIntervalMs: Number(process.env.PENDING_REFERENCE_POLL_INTERVAL_MS ?? 250),
    /** Tentativas do worker antes de REJECTED com REFERENCE_NOT_FOUND_TIMEOUT. */
    pendingReferenceMaxAttempts: Number(process.env.PENDING_REFERENCE_MAX_ATTEMPTS ?? 5),
    /** Atraso da primeira tentativa e base do backoff exponencial (2^n * base). */
    pendingReferenceBaseDelayMs: Number(process.env.PENDING_REFERENCE_BASE_DELAY_MS ?? 500),
  },
} as const;