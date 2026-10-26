import 'reflect-metadata';

import { createDataSource } from '../src/infrastructure/database/data-source';
import { logger } from '../src/infrastructure/observability/logger';

/** Roda as migrations versionadas (schema, constraints, triggers, indices). */
async function main(): Promise<void> {
  const dataSource = createDataSource();
  await dataSource.initialize();

  try {
    const aplicadas = await dataSource.runMigrations();
    logger.info('migrations_applied', { count: aplicadas.length });
    console.log(`migrations aplicadas: ${aplicadas.length}`);
  } finally {
    await dataSource.destroy();
  }
}

main().catch((erro: Error) => {
  console.error('falha ao rodar migrations:', erro.message);
  process.exit(1);
});