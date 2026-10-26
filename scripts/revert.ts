import 'reflect-metadata';

import { createDataSource } from '../src/infrastructure/database/data-source';
import { logger } from '../src/infrastructure/observability/logger';

/** Desfaz a ultima migration (todas as migrations sao reversiveis). */
async function main(): Promise<void> {
  const dataSource = createDataSource();
  await dataSource.initialize();

  try {
    await dataSource.undoLastMigration();
    logger.info('migration_reverted');
    console.log('ultima migration revertida');
  } finally {
    await dataSource.destroy();
  }
}

main().catch((erro: Error) => {
  console.error('falha ao reverter migration:', erro.message);
  process.exit(1);
});