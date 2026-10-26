/**
 * Erros de dominio + taxonomia de falhas de negocio.
 *
 * Nada aqui importa NestJS, TypeORM, AWS SDK ou qualquer framework: o dominio
 * conhece apenas os proprios erros. Cada erro carrega o status HTTP que o
 * filtro da camada de apresentacao deve devolver, para que o mapeamento
 * erro -> HTTP exista em um lugar so.
 */

/** Motivo pelo qual uma transacao terminou como REJECTED (coluna failure_code). */
export enum FailureCode {
  /** Saldo insuficiente para uma BET. */
  INSUFFICIENT_FUNDS = 'INSUFFICIENT_FUNDS',
  /** Reverter um WIN deixaria a carteira negativa (situacao diferente de apostar sem saldo). */
  INSUFFICIENT_FUNDS_FOR_REVERSAL = 'INSUFFICIENT_FUNDS_FOR_REVERSAL',
  /** Moeda da operacao diferente da moeda da carteira. */
  CURRENCY_MISMATCH = 'CURRENCY_MISMATCH',
  /** REFUND apontando para algo que nao e BET, ou ROLLBACK apontando para LOSS/OPENING. */
  INVALID_REFERENCE_KIND = 'INVALID_REFERENCE_KIND',
  /** A transacao referenciada e de outro player/wallet/provider/moeda/rodada. */
  REFERENCE_CONTEXT_MISMATCH = 'REFERENCE_CONTEXT_MISMATCH',
  /** A transacao referenciada existe, mas esta REJECTED ou FAILED. */
  REFERENCE_NOT_PROCESSED = 'REFERENCE_NOT_PROCESSED',
  /** Valor do REFUND/ROLLBACK diferente do valor original. */
  REFERENCE_AMOUNT_MISMATCH = 'REFERENCE_AMOUNT_MISMATCH',
  /** A referencia ja possui um REFUND/ROLLBACK com status PROCESSED. */
  ALREADY_REVERSED = 'ALREADY_REVERSED',
  /** O worker esgotou as tentativas sem a transacao referenciada chegar. */
  REFERENCE_NOT_FOUND_TIMEOUT = 'REFERENCE_NOT_FOUND_TIMEOUT',
}

/** Base de todo erro de dominio. */
export class DomainError extends Error {
  constructor(
    message: string,
    readonly code: string,
    readonly httpStatus: number,
    readonly details: Record<string, unknown> = {},
  ) {
    super(message);
    this.name = new.target.name;
  }
}

/** 400 - entrada invalida (campo faltando, tipo errado, kind desconhecido...). */
export class ValidationError extends DomainError {
  constructor(message: string, details: Record<string, unknown> = {}) {
    super(message, 'VALIDATION_ERROR', 400, details);
  }
}

/** 400 - formato de dinheiro invalido ("25.0", "-10.00", "NaN", "1e5", ""). */
export class InvalidMoneyError extends DomainError {
  constructor(message: string, details: Record<string, unknown> = {}) {
    super(message, 'INVALID_MONEY', 400, details);
  }
}

/** 400 - OPENING e uma transacao interna: nao vem da API nem da fila. */
export class OpeningNotAllowedError extends DomainError {
  constructor(details: Record<string, unknown> = {}) {
    super('OPENING e uma transacao interna e nao pode ser submetida pela API.', 'OPENING_NOT_ALLOWED', 400, details);
  }
}

/** 400 - moeda da operacao diferente da moeda da carteira. Nunca gera dinheiro: e erro de payload. */
export class CurrencyMismatchError extends DomainError {
  constructor(expected: string, received: string) {
    super(`moeda da operacao (${received}) diferente da moeda da carteira (${expected})`, 'CURRENCY_MISMATCH', 400, {
      expected,
      received,
    });
  }
}

/** 404 - carteira ou transacao inexistente. */
export class NotFoundError extends DomainError {
  constructor(message: string, details: Record<string, unknown> = {}) {
    super(message, 'NOT_FOUND', 404, details);
  }
}

/** 409 - conflito: carteira duplicada ou Idempotency-Key reusada com payload diferente. */
export class ConflictError extends DomainError {
  constructor(message: string, code = 'CONFLICT', details: Record<string, unknown> = {}) {
    super(message, code, 409, details);
  }
}

/** 500 - erro de programacao (por exemplo: tentar mudar o estado de uma transacao terminal). */
export class InvalidTransactionStateError extends DomainError {
  constructor(message: string, details: Record<string, unknown> = {}) {
    super(message, 'INVALID_TRANSACTION_STATE', 500, details);
  }
}

/** 500 - o lancamento no ledger nao fecha (balanceBefore +- money != balanceAfter). */
export class InvalidLedgerEntryError extends DomainError {
  constructor(message: string, details: Record<string, unknown> = {}) {
    super(message, 'INVALID_LEDGER_ENTRY', 500, details);
  }
}

/**
 * 422 - violacao de regra de negocio.
 * Nao estorna a transacao SQL: vira uma linha REJECTED auditavel, com o
 * failureCode gravado em failure_code.
 */
export class BusinessRuleError extends DomainError {
  constructor(
    message: string,
    readonly failureCode: FailureCode,
    details: Record<string, unknown> = {},
  ) {
    super(message, failureCode, 422, details);
  }
}

/** Saldo insuficiente - de aposta (INSUFFICIENT_FUNDS) ou de reversao. */
export class InsufficientFundsError extends BusinessRuleError {
  constructor(balance: string, amount: string, failureCode: FailureCode = FailureCode.INSUFFICIENT_FUNDS) {
    super(`saldo insuficiente: ${balance} < ${amount}`, failureCode, { balance, amount });
  }
}