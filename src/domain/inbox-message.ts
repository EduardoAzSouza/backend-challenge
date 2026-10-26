import { ValidationError } from './errors';

export interface InboxMessageState {
  id: string;
  consumerName: string;
  messageId: string;
  payloadHash: string;
  receivedAt: Date;
  processedAt: Date | null;
}

/**
 * Registro de mensagem ja consumida (deduplicacao persistente).
 *
 * A unicidade e do banco: UNIQUE (consumer_name, message_id). Como a linha e
 * gravada NA MESMA transacao SQL do efeito financeiro, ou as duas coisas
 * existem, ou nenhuma existe - por isso um redelivery nunca duplica debito.
 */
export class InboxMessage {
  private constructor(
    readonly id: string,
    readonly consumerName: string,
    readonly messageId: string,
    readonly payloadHash: string,
    readonly receivedAt: Date,
    private _processedAt: Date | null,
  ) {}

  static receive(props: {
    id: string;
    consumerName: string;
    messageId: string;
    payloadHash: string;
    receivedAt?: Date;
  }): InboxMessage {
    if (props.messageId.trim().length === 0) {
      throw new ValidationError('messageId e obrigatorio');
    }
    if (props.consumerName.trim().length === 0) {
      throw new ValidationError('consumerName e obrigatorio');
    }

    return new InboxMessage(
      props.id,
      props.consumerName,
      props.messageId,
      props.payloadHash,
      props.receivedAt ?? new Date(),
      null,
    );
  }

  static rehydrate(state: InboxMessageState): InboxMessage {
    return new InboxMessage(
      state.id,
      state.consumerName,
      state.messageId,
      state.payloadHash,
      state.receivedAt,
      state.processedAt,
    );
  }

  get processedAt(): Date | null {
    return this._processedAt;
  }

  isProcessed(): boolean {
    return this._processedAt !== null;
  }

  markProcessed(at: Date = new Date()): void {
    this._processedAt = at;
  }

  /** Mesmo messageId com conteudo diferente e conflito, nao replay. */
  payloadHashMatches(payloadHash: string): boolean {
    return this.payloadHash === payloadHash;
  }
}