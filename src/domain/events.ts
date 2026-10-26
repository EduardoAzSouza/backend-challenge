import { randomUUID } from 'node:crypto';

import type { FailureCode } from './errors';
import type { MoneyProps } from './money';
import type { LedgerDirection } from './wallet-ledger-entry';
import type { WagerKind, WagerStatus } from './wager-transaction';

export interface EventContext {
  aggregateId: string;
  correlationId?: string | null;
  causationId?: string | null;
  occurredAt?: Date;
}

export interface IntegrationEventProps<T> {
  eventId?: string;
  aggregateId: string;
  correlationId?: string | null;
  causationId?: string | null;
  occurredAt?: Date;
  data: T;
}

/**
 * Envelope de evento de integracao.
 *
 * O `data` sempre carrega `MoneyProps` (string decimal), nunca a instancia de
 * `Money`: o payload precisa ser JSON estavel e versionavel.
 */
export abstract class IntegrationEvent<T> {
  abstract readonly eventType: string;
  abstract readonly version: number;

  readonly eventId: string;
  readonly aggregateId: string;
  readonly correlationId: string | null;
  readonly causationId: string | null;
  readonly occurredAt: Date;
  readonly data: Readonly<T>;

  protected constructor(props: IntegrationEventProps<T>) {
    this.eventId = props.eventId ?? randomUUID();
    this.aggregateId = props.aggregateId;
    this.correlationId = props.correlationId ?? null;
    this.causationId = props.causationId ?? null;
    this.occurredAt = props.occurredAt ?? new Date();
    this.data = props.data;
  }

  /** Envelope serializado, gravado em outbox_messages.payload. */
  toJSON(): {
    eventId: string;
    eventType: string;
    aggregateId: string;
    correlationId: string | null;
    causationId: string | null;
    occurredAt: string;
    version: number;
    data: T;
  } {
    return {
      eventId: this.eventId,
      eventType: this.eventType,
      aggregateId: this.aggregateId,
      correlationId: this.correlationId,
      causationId: this.causationId,
      occurredAt: this.occurredAt.toISOString(),
      version: this.version,
      data: this.data as T,
    };
  }
}

/* ------------------------------------------------------------------ dados */

export interface WagerTransactionProcessedData {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  kind: WagerKind;
  status: WagerStatus;
  money: MoneyProps;
  balanceAfter: MoneyProps;
  balanceChanged: boolean;
}

export interface WagerTransactionRejectedData {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  kind: WagerKind;
  status: WagerStatus;
  money: MoneyProps;
  failureCode: FailureCode;
  balance: MoneyProps;
}

export interface WalletBalanceChangedData {
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  walletVersion: number;
}

export interface WagerTransactionPendingReferenceData {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  referenceExternalTransactionId: string;
  playerId: string;
  walletId: string;
  kind: WagerKind;
  status: WagerStatus;
  money: MoneyProps;
  nextRetryAt: string;
}

/* ------------------------------------------------------------- subclasses */

export class WagerTransactionProcessed extends IntegrationEvent<WagerTransactionProcessedData> {
  readonly eventType = 'WagerTransactionProcessed';
  readonly version = 1;

  static from(data: WagerTransactionProcessedData, ctx: EventContext): WagerTransactionProcessed {
    return new WagerTransactionProcessed({ ...ctx, data });
  }
}

export class WagerTransactionRejected extends IntegrationEvent<WagerTransactionRejectedData> {
  readonly eventType = 'WagerTransactionRejected';
  readonly version = 1;

  static from(data: WagerTransactionRejectedData, ctx: EventContext): WagerTransactionRejected {
    return new WagerTransactionRejected({ ...ctx, data });
  }
}

export class WalletBalanceChanged extends IntegrationEvent<WalletBalanceChangedData> {
  readonly eventType = 'WalletBalanceChanged';
  readonly version = 1;

  static from(data: WalletBalanceChangedData, ctx: EventContext): WalletBalanceChanged {
    return new WalletBalanceChanged({ ...ctx, data });
  }
}

export class WagerTransactionPendingReference extends IntegrationEvent<WagerTransactionPendingReferenceData> {
  readonly eventType = 'WagerTransactionPendingReference';
  readonly version = 1;

  static from(data: WagerTransactionPendingReferenceData, ctx: EventContext): WagerTransactionPendingReference {
    return new WagerTransactionPendingReference({ ...ctx, data });
  }
}