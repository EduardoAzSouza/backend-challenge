/**
 * Metricas em memoria, expostas em /metrics no formato de texto do Prometheus.
 *
 * E deliberadamente simples: contadores e medidores, sem agente externo.
 * Trocar por OpenTelemetry depois e trocar a implementacao deste arquivo.
 */
type Labels = Record<string, string | number>;

export class Metrics {
  private readonly contadores = new Map<string, number>();

  increment(nome: string, labels: Labels = {}, valor = 1): void {
    const chave = this.chave(nome, labels);
    this.contadores.set(chave, (this.contadores.get(chave) ?? 0) + valor);
  }

  /** Define um medidor (ultimo valor observado), por exemplo o atraso da outbox. */
  gauge(nome: string, valor: number, labels: Labels = {}): void {
    const chave = this.chave(nome, labels);
    this.contadores.set(chave, valor);
  }

  /** Conta uma amostra e soma a latencia em ms. */
  observeLatency(nome: string, ms: number, labels: Labels = {}): void {
    this.increment(`${nome}_count`, labels);
    this.increment(`${nome}_ms_total`, labels, ms);
  }

  snapshot(): Array<{ nome: string; valor: number }> {
    return [...this.contadores.entries()].map(([nome, valor]) => ({ nome, valor }));
  }

  reset(): void {
    this.contadores.clear();
  }

  toPrometheus(): string {
    const linhas = this.snapshot().map(({ nome, valor }) => `${nome} ${valor}`);
    return `${linhas.join('\n')}\n`;
  }

  private chave(nome: string, labels: Labels): string {
    const partes = Object.keys(labels)
      .sort()
      .map((chave) => `${chave}="${String(labels[chave])}"`);

    return partes.length > 0 ? `${nome}{${partes.join(',')}}` : nome;
  }
}

/** Instancia unica do processo. */
export const metrics = new Metrics();