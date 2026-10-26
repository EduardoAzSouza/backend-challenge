import 'reflect-metadata';

import { DataSource, type QueryRunner } from 'typeorm';

import { config } from '../../config';
import { InitialSchema1700000000000 } from './migrations/001-initial-schema';

/** Token de injecao da DataSource no Nest. */
export const DATA_SOURCE = 'DATA_SOURCE';

/**
 * O projeto usa SQL explicito (QueryRunner), nao entidades mapeadas: o
 * mapeamento linha -> objeto de dominio e feito com `.rehydrate()`.
 * Por isso `entities: []` e `synchronize: false`.
 */
export function createDataSource(url: string = config.databaseUrl): DataSource {
  return new DataSource({
    type: 'postgres',
    url,
    entities: [],
    migrations: [InitialSchema1700000000000],
    migrationsTableName: 'typeorm_migrations',
    synchronize: false,
    logging: false,
    extra: { max: config.poolMax },
  });
}

/**
 * Abre uma transacao e garante commit / rollback / release da conexao.
 * Todo caminho financeiro passa por aqui.
 */
export async function withTransaction<T>(
  dataSource: DataSource,
  work: (queryRunner: QueryRunner) => Promise<T>,
): Promise<T> {
  const queryRunner = dataSource.createQueryRunner();
  await queryRunner.connect();
  await queryRunner.startTransaction();

  try {
    const resultado = await work(queryRunner);
    await queryRunner.commitTransaction();
    return resultado;
  } catch (erro) {
    await queryRunner.rollbackTransaction();
    throw erro;
  } finally {
    await queryRunner.release();
  }
}