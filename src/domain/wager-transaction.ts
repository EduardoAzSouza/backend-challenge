import { FailureCode, InvalidTransactionStateError, ValidationError } from './errors';
import { Money } from './money';
import { LedgerDirection } from './wallet-ledger-entry';

export enum WagerKind {
  /** Interno: credito de abertura da carteira. Nunca vem da API. */
  Opening = 'OPENING',
  Bet = 'BET',
  Win = 'WIN',
  Loss = 'LOSS',
  Refund = 'REFUND',
  Rollback = 'ROLLBACK',
}

export enum WagerStatus {
  /** Aceita, ainda nao aplicada. */
  Pending = 'PENDING',
  /** Aguardando a transacao referenciada chegar. */
  PendingReference = 'PENDING_REFERENCE',
  /** Aplicada (terminal). */
  Processed = 'PROCESSED',
  /** Violacao de regra de negocio (terminal). */
  Rejected = 'REJECTED',
  /** Erro permanente de infraestrutura (terminal). */
  Failed = 'FAILED',
}

/** Kinds que o provedor pode enviar pela API ou pela fila. */
export const KINDS_DA_API: readonly WagerKind[] = [
  WagerKind.Bet,
  WagerKind.Win,
  WagerKind.Loss,
  WagerKind.Refund,
  WagerKind.Rollback,
];

/** Transicoes de estado (o resto e erro de programacao). */
const TRANSICOES_VALIDAS: Record<WagerStatus, WagerStatus[]> = {
  [WagerStatus.Pending]: [WagerStatus.PendingReference, WagerStatus.Processed, WagerStatus.Rejected, WagerStatus.Failed],
  [WagerStatus.PendingReference]: [WagerStatus.PendingReference, WagerStatus.Processed, WagerStatus.Rejected, WagerStatus.Failed],
  [WagerStatus.Processed]: [],
  [WagerStatus.Rejected]: [],
  [WagerStatus.Failed]: [],
};

export interface WagerTransactionState {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: WagerKind;
  money: Money;
  referenceExternalTransactionId: string | null;
  status: WagerStatus;
  referenceTransactionId: string | null;
  failureCode: FailureCode | null;
  retryCount: number;
  nextRetryAt: Date | null;
  createdAt: Date;
  processedAt: Date | null;
}

export interface CreateWagerTransactionProps {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: WagerKind;
  money: Money;
  referenceExternalTransactionId?: string | null;
  createdAt?: Date;
}

/**
 * Entidade central do dominio: uma operacao do provedor.
 *
 * As transicoes sao explicitas (`markProcessed`, `markPendingReference`,
 * `reject`, `fail`) e PROCESSED/REJECTED/FAILED sao terminais: tentar
 * transicionar de novo e bug de programacao e lanca erro.
 */
export class WagerTransaction {
  private constructor(
    readonly id: string,
    readonly providerId: string,
    readonly externalTransactionId: string,
    readonly idempotencyKey: string,
    readonly payloadHash: string,
    readonly playerId: string,
    readonly walletId: string,
    readonly roundId: string,
    readonly gameId: string,
    readonly kind: WagerKind,
    readonly money: Money,
    readonly referenceExternalTransactionId: string | null,
    private _status: WagerStatus,
    private _referenceTransactionId: string | null,
    private _failureCode: FailureCode | null,
    private _retryCount: number,
    private _nextRetryAt: Date | null,
    readonly createdAt: Date,
    private _processedAt: Date | null,
  ) {}

  /** Nasce em PENDING. Valida a exigencia de referencia por kind. */
  static create(props: CreateWagerTransactionProps): WagerTransaction {
    if (!props.money.isPositive()) {
      throw new ValidationError('o valor da transacao deve ser maior que zero');
    }

    const referencia = props.referenceExternalTransactionId ?? null;
    const exigeReferencia = props.kind === WagerKind.Refund || props.kind === WagerKind.Rollback;

    if (exigeReferencia && referencia === null) {
      throw new ValidationError(`referenceExternalTransactionId e obrigatorio para ${props.kind}`, {
        kind: props.kind,
      });
    }

    return new WagerTransaction(
      props.id,
      props.providerId,
      props.externalTransactionId,
      props.idempotencyKey,
      props.payloadHash,
      props.playerId,
      props.walletId,
      props.roundId,
      props.gameId,
      props.kind,
      props.money,
      referencia,
      WagerStatus.Pending,
      null,
      null,
      0,
      null,
      props.createdAt ?? new Date(),
      null,
    );
  }

  /** Reconstroi do banco (nao revalida regras de transicao). */
  static rehydrate(state: WagerTransactionState): WagerTransaction {
    return new WagerTransaction(
      state.id,
      state.providerId,
      state.externalTransactionId,
      state.idempotencyKey,
      state.payloadHash,
      state.playerId,
      state.walletId,
      state.roundId,
      state.gameId,
      state.kind,
      state.money,
      state.referenceExternalTransactionId,
      state.status,
      state.referenceTransactionId,
      state.failureCode,
      state.retryCount,
      state.nextRetryAt,
      state.createdAt,
      state.processedAt,
    );
  }

  // ---- leitura ----

  get status(): WagerStatus {
    return this._status;
  }

  get referenceTransactionId(): string | null {
    return this._referenceTransactionId;
  }

  get failureCode(): FailureCode | null {
    return this._failureCode;
  }

  get retryCount(): number {
    return this._retryCount;
  }

  get nextRetryAt(): Date | null {
    return this._nextRetryAt;
  }

  get processedAt(): Date | null {
    return this._processedAt;
  }

  isTerminal(): boolean {
    return (
      this._status === WagerStatus.Processed ||
      this._status === WagerStatus.Rejected ||
      this._status === WagerStatus.Failed
    );
  }

  /** LOSS (e qualquer transacao REJECTED) nao mexe no saldo. */
  affectsBalance(): boolean {
    return this.kind !== WagerKind.Loss && this.kind !== WagerKind.Opening;
  }

  requiresReference(): boolean {
    return this.kind === WagerKind.Refund || this.kind === WagerKind.Rollback;
  }

  matchesPayload(payloadHash: string): boolean {
    return this.payloadHash === payloadHash;
  }

  // ---- transicoes ----

  markProcessed(referenceTransactionId: string | null, at: Date = new Date()): void {
    this.transitarPara(WagerStatus.Processed);
    this._status = WagerStatus.Processed;
    this._referenceTransactionId = referenceTransactionId;
    this._processedAt = at;
    this._nextRetryAt = null;
  }

  markPendingReference(nextRetryAt: Date): void {
    this.transitarPara(WagerStatus.PendingReference);
    this._status = WagerStatus.PendingReference;
    this._nextRetryAt = nextRetryAt;
  }

  /** Usado pelo worker quando a referencia continua ausente: conta a tentativa e reagenda. */
  reagendar(tentativas: number, nextRetryAt: Date): void {
    this.transitarPara(WagerStatus.PendingReference);
    this._status = WagerStatus.PendingReference;
    this._retryCount = tentativas;
    this._nextRetryAt = nextRetryAt;
  }

  reject(code: FailureCode, at: Date = new Date()): void {
    this.transitarPara(WagerStatus.Rejected);
    this._status = WagerStatus.Rejected;
    this._failureCode = code;
    this._processedAt = at;
    this._nextRetryAt = null;
  }

  fail(code: FailureCode, at: Date = new Date()): void {
    this.transitarPara(WagerStatus.Failed);
    this._status = WagerStatus.Failed;
    this._failureCode = code;
    this._processedAt = at;
    this._nextRetryAt = null;
  }

  // ---- regras de negocio de reversao ----

  /**
   * Valida a transacao referenciada. Devolve o FailureCode da primeira regra
   * violada, ou null quando esta tudo certo.
   *
   * Ordem: contexto -> kind -> status -> valor.
   */
  checkReference(reference: WagerTransaction): FailureCode | null {
    if (!this.requiresReference() || this.referenceExternalTransactionId === null) {
      return null;
    }

    const mesmoContexto =
      reference.providerId === this.providerId &&
      reference.playerId === this.playerId &&
      reference.walletId === this.walletId &&
      reference.roundId === this.roundId &&
      reference.money.currency === this.money.currency;

    if (!mesmoContexto) {
      return FailureCode.REFERENCE_CONTEXT_MISMATCH;
    }

    if (this.kind === WagerKind.Refund && reference.kind !== WagerKind.Bet) {
      return FailureCode.INVALID_REFERENCE_KIND;
    }
    const rollbackAceita: WagerKind[] = [WagerKind.Bet, WagerKind.Win, WagerKind.Refund];
    if (this.kind === WagerKind.Rollback && !rollbackAceita.includes(reference.kind)) {
      return FailureCode.INVALID_REFERENCE_KIND;
    }

    if (reference.status !== WagerStatus.Processed) {
      return FailureCode.REFERENCE_NOT_PROCESSED;
    }

    if (!reference.money.equals(this.money)) {
      return FailureCode.REFERENCE_AMOUNT_MISMATCH;
    }

    return null;
  }

  /**
   * Direcao do lancamento no ledger.
   * `null` para LOSS (nao gera lancamento).
   */
  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection | null {
    switch (this.kind) {
      case WagerKind.Opening:
      case WagerKind.Win:
      case WagerKind.Refund:
        return LedgerDirection.Credit;
      case WagerKind.Bet:
        return LedgerDirection.Debit;
      case WagerKind.Loss:
        return null;
      case WagerKind.Rollback:
        if (!reference) {
          throw new ValidationError('ROLLBACK exige a transacao referenciada para saber a direcao');
        }
        // ROLLBACK desfaz o efeito da referencia -> direcao contraria.
        return reference.kind === WagerKind.Bet ? LedgerDirection.Credit : LedgerDirection.Debit;
      default:
        throw new InvalidTransactionStateError(`kind sem direcao de ledger: ${String(this.kind)}`);
    }
  }

  private transitarPara(destino: WagerStatus): void {
    if (!TRANSICOES_VALIDAS[this._status].includes(destino)) {
      throw new InvalidTransactionStateError(
        `transicao invalida: ${this._status} -> ${destino} (transacao ${this.id})`,
        { transactionId: this.id, from: this._status, to: destino },
      );
    }
  }
}