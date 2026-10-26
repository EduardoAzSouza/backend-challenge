import { writeSync } from 'node:fs';

import { config } from '../../config';

/**
 * Log estruturado em JSON (uma linha por evento).
 *
 * Regra: nunca logamos payload financeiro completo nem dado sensivel. O que
 * interessa para diagnostico sao os identificadores (correlationId, messageId,
 * transactionId, walletId, providerId) e o resultado.
 *
 * A escrita e SINCRONA (`fs.writeSync`): um log que se perde num shutdown
 * abrupto (process.exit) nao serve para diagnosticar justamente o pior caso.
 */
type Nivel = 'debug' | 'info' | 'warn' | 'error';

const ORDEM: Record<Nivel, number> = { debug: 10, info: 20, warn: 30, error: 40 };

export interface LogFields {
  [chave: string]: unknown;
}

function emitir(nivel: Nivel, evento: string, fields: LogFields = {}): void {
  if (ORDEM[nivel] < ORDEM[config.logLevel]) {
    return;
  }

  const linha = JSON.stringify({
    ts: new Date().toISOString(),
    level: nivel,
    event: evento,
    instance: config.instanceId,
    ...fields,
  });

  try {
    writeSync(nivel === 'error' ? 2 : 1, `${linha}\n`);
  } catch {
    // Sem stdout disponivel (ex.: pipe fechado): nao vale derrubar o processo.
  }
}

export const logger = {
  debug: (evento: string, fields?: LogFields) => emitir('debug', evento, fields),
  info: (evento: string, fields?: LogFields) => emitir('info', evento, fields),
  warn: (evento: string, fields?: LogFields) => emitir('warn', evento, fields),
  error: (evento: string, fields?: LogFields) => emitir('error', evento, fields),
};