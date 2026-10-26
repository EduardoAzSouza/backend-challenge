import { CurrencyMismatchError, FailureCode, InsufficientFundsError, ValidationError } from './errors';
import { Money } from './money';

export interface WalletState {
  id: string;
  playerId: string;
  currency: string;
  balance: Money;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

/**
 * Aggregate root.
 *
 * Invariantes que esta classe protege sozinha:
 *  - o saldo nunca fica negativo;
 *  - toda mudanca de saldo incrementa `version` (e so a mudanca incrementa);
 *  - a moeda da operacao tem que ser a moeda da carteira.
 *
 * A consistencia com o ledger e garantida no caso de uso + no banco (trigger),
 * porque isso exige uma transacao SQL.
 */
export class Wallet {
  private constructor(
    readonly id: string,
    readonly playerId: string,
    readonly currency: string,
    private _balance: Money,
    private _version: number,
    readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}

  /** Carteira nova. `version` comeca em 1 (criacao nao e uma "mudanca de saldo"). */
  static open(props: { id: string; playerId: string; initialBalance: Money; now?: Date }): Wallet {
    if (props.playerId.trim().length === 0) {
      throw new ValidationError('playerId e obrigatorio');
    }
    if (props.initialBalance.isNegative()) {
      throw new ValidationError('saldo inicial nao pode ser negativo');
    }

    const agora = props.now ?? new Date();
    return new Wallet(props.id, props.playerId, props.initialBalance.currency, props.initialBalance, 1, agora, agora);
  }

  /** Reconstroi do banco. Nao revalida transicoes: o estado ja foi decidido antes. */
  static rehydrate(state: WalletState): Wallet {
    return new Wallet(state.id, state.playerId, state.currency, state.balance, state.version, state.createdAt, state.updatedAt);
  }

  get balance(): Money {
    return this._balance;
  }

  get version(): number {
    return this._version;
  }

  get updatedAt(): Date {
    return this._updatedAt;
  }

  /**
   * Debita e devolve o saldo ANTERIOR (e o que o ledger precisa para montar
   * `balance_before` / `balance_after`).
   *
   * `failureCode` existe para separar as duas situacoes: apostar sem saldo
   * (INSUFFICIENT_FUNDS) e reverter algo que deixaria a carteira negativa
   * (INSUFFICIENT_FUNDS_FOR_REVERSAL).
   */
  debit(money: Money, at: Date, failureCode: FailureCode = FailureCode.INSUFFICIENT_FUNDS): Money {
    this.mesmaMoeda(money);

    if (!money.isPositive()) {
      throw new ValidationError('o valor do debito deve ser maior que zero');
    }
    if (this._balance.isLessThan(money)) {
      throw new InsufficientFundsError(this.balance.toString(), money.toString(), failureCode);
    }

    const anterior = this._balance;
    this._balance = this._balance.subtract(money);
    this._version += 1;
    this._updatedAt = at;
    return anterior;
  }

  /** Credita e devolve o saldo anterior. */
  credit(money: Money, at: Date): Money {
    this.mesmaMoeda(money);

    if (!money.isPositive()) {
      throw new ValidationError('o valor do credito deve ser maior que zero');
    }

    const anterior = this._balance;
    this._balance = this._balance.add(money);
    this._version += 1;
    this._updatedAt = at;
    return anterior;
  }

  private mesmaMoeda(money: Money): void {
    if (money.currency !== this.currency) {
      throw new CurrencyMismatchError(this.currency, money.currency);
    }
  }
}