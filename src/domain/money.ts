import Decimal from 'decimal.js';

import { CurrencyMismatchError, InvalidMoneyError } from './errors';

/**
 * Contrato publico de dinheiro: string decimal com EXATAMENTE 2 casas.
 * Ex.: { "amount": "25.00", "currency": "BRL" }
 */
export interface MoneyProps {
  amount: string;
  currency: string;
}

/** Aceita apenas "123.45": sem sinal, sem notacao cientifica, exatamente 2 casas. */
const FORMATO_AMOUNT = /^\d+\.\d{2}$/;
const FORMATO_MOEDA = /^[A-Z]{3}$/;

/**
 * Value object de dinheiro.
 *
 * - nunca usa `number` para valores monetarios: internamente e `Decimal`;
 * - e imutavel: toda operacao devolve uma NOVA instancia;
 * - serializa sempre com 2 casas ("25.00"), nunca como float.
 */
export class Money {
  private constructor(
    private readonly valor: Decimal,
    readonly currency: string,
  ) {}

  /** Cria a partir do contrato publico. Entrada invalida -> InvalidMoneyError (HTTP 400). */
  static from(props: MoneyProps): Money {
    if (!props || typeof props !== 'object') {
      throw new InvalidMoneyError('money e obrigatorio');
    }

    const { amount, currency } = props;

    if (typeof amount !== 'string') {
      throw new InvalidMoneyError(`amount deve ser string decimal (ex.: "25.00"), recebi ${typeof amount}`);
    }
    if (!FORMATO_AMOUNT.test(amount)) {
      throw new InvalidMoneyError(
        `amount invalido: ${JSON.stringify(amount)}. Use string positiva com exatamente 2 casas (ex.: "25.00").`,
        { amount },
      );
    }
    if (typeof currency !== 'string' || !FORMATO_MOEDA.test(currency)) {
      throw new InvalidMoneyError(
        `currency invalida: ${JSON.stringify(currency)}. Use o codigo ISO-4217 em maiusculas (ex.: "BRL").`,
        { currency },
      );
    }

    return new Money(new Decimal(amount), currency);
  }

  static zero(currency: string): Money {
    return new Money(new Decimal(0), currency);
  }

  // ---- operacoes: sempre devolvem uma nova instancia (imutabilidade) ----

  add(other: Money): Money {
    this.mesmaMoeda(other);
    return new Money(this.valor.plus(other.valor), this.currency);
  }

  subtract(other: Money): Money {
    this.mesmaMoeda(other);
    return new Money(this.valor.minus(other.valor), this.currency);
  }

  /** Pode produzir valor negativo internamente (ex.: diferenca na reconciliacao). */
  negate(): Money {
    return new Money(this.valor.negated(), this.currency);
  }

  // ---- consultas ----

  // Observacao: no decimal.js `isPositive()`/`isNegative()` sao baseados em
  // SINAL, e `isPositive()` devolve true para zero. Aqui o que interessa e a
  // MAGNITUDE, porque o dominio usa essas consultas para decidir se um valor
  // pode ser debitado/creditado.
  isZero(): boolean {
    return this.valor.isZero();
  }

  isPositive(): boolean {
    return this.valor.greaterThan(0);
  }

  isNegative(): boolean {
    return this.valor.lessThan(0);
  }

  isLessThan(other: Money): boolean {
    this.mesmaMoeda(other);
    return this.valor.lessThan(other.valor);
  }

  isGreaterThan(other: Money): boolean {
    this.mesmaMoeda(other);
    return this.valor.greaterThan(other.valor);
  }

  equals(other: Money): boolean {
    this.mesmaMoeda(other);
    return this.valor.equals(other.valor);
  }

  /** Representacao oficial: string com 2 casas. E o que vai para JSON e para o banco. */
  toJSON(): MoneyProps {
    return { amount: this.valor.toFixed(2), currency: this.currency };
  }

  toString(): string {
    return `${this.valor.toFixed(2)} ${this.currency}`;
  }

  private mesmaMoeda(other: Money): void {
    if (other.currency !== this.currency) {
      throw new CurrencyMismatchError(this.currency, other.currency);
    }
  }
}