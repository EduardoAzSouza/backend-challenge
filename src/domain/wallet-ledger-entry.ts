import { InvalidLedgerEntryError } from './errors';
import { Money } from './money';

export enum LedgerDirection {
  Debit = 'DEBIT',
  Credit = 'CREDIT',
}

export interface LedgerEntryProps {
  id: string;
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: Money;
  balanceBefore: Money;
  balanceAfter: Money;
  createdAt: Date;
}

/**
 * Lancamento do extrato da carteira.
 *
 * A imutabilidade aqui e estrutural: nao existe setter nem metodo de transicao.
 * O banco reforca a mesma regra com o trigger `trg_immutable_ledger`
 * (qualquer UPDATE/DELETE e recusado, inclusive por um humano no psql).
 */
export class WalletLedgerEntry {
  private constructor(
    readonly id: string,
    readonly walletId: string,
    readonly transactionId: string,
    readonly direction: LedgerDirection,
    readonly money: Money,
    readonly balanceBefore: Money,
    readonly balanceAfter: Money,
    readonly createdAt: Date,
  ) {}

  /** Valida a aritmetica do lancamento antes de existir. */
  static create(props: LedgerEntryProps): WalletLedgerEntry {
    const entry = WalletLedgerEntry.rehydrate(props);

    if (!props.money.isPositive()) {
      throw new InvalidLedgerEntryError(`lancamento com valor nao positivo: ${props.money.toString()}`, {
        transactionId: props.transactionId,
      });
    }
    if (!entry.isBalanced()) {
      throw new InvalidLedgerEntryError(
        `lancamento nao fecha: ${props.balanceBefore.toString()} ${props.direction} ${props.money.toString()} != ${props.balanceAfter.toString()}`,
        { transactionId: props.transactionId, direction: props.direction },
      );
    }

    return entry;
  }

  /** Reconstroi do banco (uma linha ja gravada e imutavel, nada a revalidar). */
  static rehydrate(props: LedgerEntryProps): WalletLedgerEntry {
    return new WalletLedgerEntry(
      props.id,
      props.walletId,
      props.transactionId,
      props.direction,
      props.money,
      props.balanceBefore,
      props.balanceAfter,
      props.createdAt,
    );
  }

  /** balanceBefore +- money === balanceAfter. */
  isBalanced(): boolean {
    const esperado =
      this.direction === LedgerDirection.Credit
        ? this.balanceBefore.add(this.money)
        : this.balanceBefore.subtract(this.money);

    return esperado.equals(this.balanceAfter);
  }
}