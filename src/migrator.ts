import type { MigrationConfig } from 'drizzle-orm/migrator';
import { readMigrationFiles } from 'drizzle-orm/migrator';
import type { DatabendDatabase } from './driver.ts';

export type DatabendMigrationConfig = MigrationConfig | string;

export async function migrate<TSchema extends Record<string, unknown>>(
  db: DatabendDatabase<TSchema>,
  config: DatabendMigrationConfig
) {
  const migrationConfig: MigrationConfig =
    typeof config === 'string' ? { migrationsFolder: config } : config;

  const migrations = readMigrationFiles(migrationConfig);

  await db.dialect.migrate(
    migrations,
    db.session,
    migrationConfig
  );
}
