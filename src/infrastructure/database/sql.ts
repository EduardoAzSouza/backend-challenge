import type { QueryRunner } from 'typeorm';

import type { IntegrationEvent } from '../../domain/events';
import { OutboxMessage } from '../../domain/outbox-message';
import type { Wallet } from '../../domain/wallet';
import type { WalletLedgerEntry } from '../../domain/wallet-ledger-entry';
import type { WagerTransaction } from '../../domain/wager-transaction';

/**
 * SQL explicito, em um lugar so.
 *
 * Nao existem entidades mapeadas: as linhas viram objetos de dominio pelos
 * mappers (`toWallet`, `toWagerTransaction`, ...) e voltam para o banco por
 * estas funcoes. Todo parametro e vinculado ($1, $2...): nada de string
 * concatenada.
 */

/** Erro de violacao de unicidade do PostgreSQL (23505). */
export function isUniqueViolation(erro: unknown): boolean {
  return typeof erro === 'object' && erro !== null && (erro as { code?: string }).code === '23505';
}

/**
 * Nome da constraint violada (23505), quando o driver informa.
 *
 * Serve para o conflito de unicidade virar a resposta certa: reusar o
 * `Idempotency-Key` e repassar a transacao de um provedor com outro conteudo
 * sao situacoes diferentes para quem chamou, e cada uma tem seu codigo de erro.
 */
export function uniqueViolationConstraint(erro: unknown): string | null {
  if (!isUniqueViolation(erro)) {
    return null;
  }

  const detalhe = erro as { constraint?: string; detail?: string };

  if (typeof detalhe.constraint === 'string') {
    return detalhe.constraint;
  }

  return /constraint "([^"]+)"/.exec(detalhe.detail ?? '')?.[1] ?? null;
}

export async function insertWallet(qr: QueryRunner, wallet: Wallet): Promise<void> {
  await qr.query(
    `INSERT INTO wallets (id, player_id, currency, balance_amount, version, created_at, updated_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7)`,
    [
      wallet.id,
      wallet.playerId,
      wallet.currency,
      wallet.balance.toJSON().amount,
      wallet.version,
      wallet.createdAt,
      wallet.updatedAt,
    ],
  );
}

export async function updateWallet(qr: QueryRunner, wallet: Wallet): Promise<void> {
  await qr.query(`UPDATE wallets SET balance_amount = $2, version = $3, updated_at = $4 WHERE id = $1`, [
    wallet.id,
    wallet.balance.toJSON().amount,
    wallet.version,
    wallet.updatedAt,
  ]);
}

export async function insertTransaction(qr: QueryRunner, transaction: WagerTransaction): Promise<void> {
  await qr.query(
    `INSERT INTO wager_transactions
       (id, provider_id, external_transaction_id, idempotency_key, payload_hash, player_id, wallet_id,
        round_id, game_id, kind, status, amount, currency, reference_external_transaction_id,
        reference_transaction_id, failure_code, retry_count, next_retry_at, processed_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)`,
    [
      transaction.id,
      transaction.providerId,
      transaction.externalTransactionId,
      transaction.idempotencyKey,
      transaction.payloadHash,
      transaction.playerId,
      transaction.walletId,
      transaction.roundId,
      transaction.gameId,
      transaction.kind,
      transaction.status,
      transaction.money.toJSON().amount,
      transaction.money.currency,
      transaction.referenceExternalTransactionId,
      transaction.referenceTransactionId,
      transaction.failureCode,
      transaction.retryCount,
      transaction.nextRetryAt,
      transaction.processedAt,
    ],
  );
}

/** Atualiza o estado decidido (PROCESSED / REJECTED / PENDING_REFERENCE). */
export async function updateTransaction(qr: QueryRunner, transaction: WagerTransaction): Promise<void> {
  await qr.query(
    `UPDATE wager_transactions
        SET status = $2,
            reference_transaction_id = $3,
            failure_code = $4,
            retry_count = $5,
            next_retry_at = $6,
            processed_at = $7,
            updated_at = NOW()
      WHERE id = $1`,
    [
      transaction.id,
      transaction.status,
      transaction.referenceTransactionId,
      transaction.failureCode,
      transaction.retryCount,
      transaction.nextRetryAt,
      transaction.processedAt,
    ],
  );
}

export async function insertLedgerEntry(qr: QueryRunner, entry: WalletLedgerEntry): Promise<void> {
  await qr.query(
    `INSERT INTO wallet_ledger_entries
       (id, wallet_id, wager_transaction_id, direction, amount, currency, balance_before, balance_after, created_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
    [
      entry.id,
      entry.walletId,
      entry.transactionId,
      entry.direction,
      entry.money.toJSON().amount,
      entry.money.currency,
      entry.balanceBefore.toJSON().amount,
      entry.balanceAfter.toJSON().amount,
      entry.createdAt,
    ],
  );
}

export async function insertOutboxMessage(qr: QueryRunner, message: OutboxMessage): Promise<void> {
  await qr.query(
    `INSERT INTO outbox_messages
       (id, aggregate_id, event_type, payload, correlation_id, occurred_at, attempts, next_attempt_at, published_at)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
    [
      message.id,
      message.aggregateId,
      message.eventType,
      JSON.stringify(message.payload),
      message.correlationId,
      message.occurredAt,
      message.attempts,
      message.nextAttemptAt,
      message.publishedAt,
    ],
  );
}

/** Grava o envelope do evento na outbox (sempre dentro da transacao financeira). */
export async function enqueueEvent(qr: QueryRunner, event: IntegrationEvent<unknown>): Promise<void> {
  await insertOutboxMessage(qr, OutboxMessage.enqueue(event));
}