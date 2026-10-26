import type { IntegrationEvent } from './events';

export interface OutboxMessageState {
  id: string;
  aggregateId: string;
  eventType: string;
  payload: Record<string, unknown>;
  correlationId: string | null;
  occurredAt: Date;
  attempts: number;
  nextAttemptAt: Date;
  publishedAt: Date | null;
}

const BACKOFF_BASE_MS = 500;
const BACKOFF_MAX_MS = 60_000;

/**
 * Evento pendente de publicacao (padrao transactional outbox).
 *
 * A linha e inserida NA MESMA transacao SQL que muda o saldo. Assim e
 * impossivel existir evento publicado para uma aposta que nao commitou, e
 * impossivel commit dar certo e o evento sumir.
 */
export class OutboxMessage {
  private constructor(
    readonly id: string,
    readonly aggregateId: string,
    readonly eventType: string,
    readonly payload: Readonly<Record<string, unknown>>,
    readonly correlationId: string | null,
    readonly occurredAt: Date,
    private _attempts: number,
    private _nextAttemptAt: Date,
    private _publishedAt: Date | null,
  ) {}

  static enqueue(event: IntegrationEvent<unknown>): OutboxMessage {
    return new OutboxMessage(
      event.eventId,
      event.aggregateId,
      event.eventType,
      event.toJSON() as unknown as Record<string, unknown>,
      event.correlationId,
      event.occurredAt,
      0,
      new Date(),
      null,
    );
  }

  static rehydrate(state: OutboxMessageState): OutboxMessage {
    return new OutboxMessage(
      state.id,
      state.aggregateId,
      state.eventType,
      state.payload,
      state.correlationId,
      state.occurredAt,
      state.attempts,
      state.nextAttemptAt,
      state.publishedAt,
    );
  }

  get attempts(): number {
    return this._attempts;
  }

  get nextAttemptAt(): Date {
    return this._nextAttemptAt;
  }

  get publishedAt(): Date | null {
    return this._publishedAt;
  }

  isPending(): boolean {
    return this._publishedAt === null;
  }

  isDue(now: Date): boolean {
    return this.isPending() && this._nextAttemptAt.getTime() <= now.getTime();
  }

  markPublished(at: Date): void {
    this._publishedAt = at;
    this._attempts += 1;
  }

  /** Backoff exponencial (500ms, 1s, 2s, 4s... com teto de 60s). */
  scheduleRetry(now: Date): void {
    this._attempts += 1;
    const atraso = Math.min(BACKOFF_BASE_MS * 2 ** (this._attempts - 1), BACKOFF_MAX_MS);
    this._nextAttemptAt = new Date(now.getTime() + atraso);
  }

  /** Atras da fila: ha quanto tempo o evento mais antigo nao publicado espera. */
  lagMs(now: Date): number {
    return this.isPending() ? Math.max(0, now.getTime() - this.occurredAt.getTime()) : 0;
  }
}