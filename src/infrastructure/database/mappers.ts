import { FailureCode } from '../../domain/errors';
import { Money } from '../../domain/money';
import { LedgerDirection, WalletLedgerEntry } from '../../domain/wallet-ledger-entry';
import { Wallet } from '../../domain/wallet';
import { WagerKind, WagerStatus, WagerTransaction } from '../../domain/wager-transaction';

/**
 * Mapeamento linha do banco -> objeto de dominio.
 *
 * E o unico lugar que conhece os nomes das colunas. O resto do sistema trabalha
 * com objetos de dominio reidratados por `.rehydrate()`.
 */

export interface WalletRow {
  id: string;
  player_id: string;
  currency: string;
  balance_amount: string;
  version: number;
  created_at: Date;
  updated_at: Date;
}

export interface WagerTransactionRow {
  id: string;
  provider_id: string;
  external_transaction_id: string;
  idempotency_key: string;
  payload_hash: string;
  player_id: string;
  wallet_id: string;
  round_id: string;
  game_id: string;
  kind: string;
  status: string;
  amount: string;
  currency: string;
  reference_external_transaction_id: string | null;
  reference_transaction_id: string | null;
  failure_code: string | null;
  retry_count: number;
  next_retry_at: Date | null;
  created_at: Date;
  processed_at?: Date | null;
}

export interface LedgerEntryRow {
  id: string;
  wallet_id: string;
  wager_transaction_id: string;
  direction: string;
  amount: string;
  currency: string;
  balance_before: string;
  balance_after: string;
  created_at: Date;
}

export function toWallet(row: WalletRow): Wallet {
  return Wallet.rehydrate({
    id: row.id,
    playerId: row.player_id,
    currency: row.currency,
    balance: Money.from({ amount: row.balance_amount, currency: row.currency }),
    version: row.version,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  });
}

export function toWagerTransaction(row: WagerTransactionRow): WagerTransaction {
  return WagerTransaction.rehydrate({
    id: row.id,
    providerId: row.provider_id,
    externalTransactionId: row.external_transaction_id,
    idempotencyKey: row.idempotency_key,
    payloadHash: row.payload_hash,
    playerId: row.player_id,
    walletId: row.wallet_id,
    roundId: row.round_id,
    gameId: row.game_id,
    kind: row.kind as WagerKind,
    money: Money.from({ amount: row.amount, currency: row.currency }),
    referenceExternalTransactionId: row.reference_external_transaction_id,
    status: row.status as WagerStatus,
    referenceTransactionId: row.reference_transaction_id,
    failureCode: (row.failure_code as FailureCode | null) ?? null,
    retryCount: row.retry_count,
    nextRetryAt: row.next_retry_at,
    createdAt: row.created_at,
    processedAt: row.processed_at ?? null,
  });
}

export function toLedgerEntry(row: LedgerEntryRow): WalletLedgerEntry {
  return WalletLedgerEntry.rehydrate({
    id: row.id,
    walletId: row.wallet_id,
    transactionId: row.wager_transaction_id,
    direction: row.direction as LedgerDirection,
    money: Money.from({ amount: row.amount, currency: row.currency }),
    balanceBefore: Money.from({ amount: row.balance_before, currency: row.currency }),
    balanceAfter: Money.from({ amount: row.balance_after, currency: row.currency }),
    createdAt: row.created_at,
  });
}