import { eq, sql } from 'drizzle-orm';
import { beforeAll, describe, expect, it } from 'vitest';
import type { DatabendDatabase } from '../src/index.ts';
import { getDb, items, users } from './setup.ts';

describe('drizzle-databend', () => {
  let db: DatabendDatabase;

  beforeAll(async () => {
    db = await getDb();
  });

  it('should connect and execute raw SQL', async () => {
    const result = await db.execute(sql`SELECT 1 AS val`);
    expect(result).toBeDefined();
  });

  it('should select all rows', async () => {
    const result = await db.select().from(users);
    expect(result).toHaveLength(4);
  });

  it('should filter with where clause', async () => {
    const result = await db.select().from(users).where(eq(users.name, 'Alice'));
    expect(result).toHaveLength(1);
    expect(result[0]!.name).toBe('Alice');
    expect(result[0]!.email).toBe('alice@test.com');
  });

  it('should handle VARIANT column type', async () => {
    const result = await db.select().from(items).where(eq(items.name, 'Test Item'));
    expect(result).toHaveLength(1);
    expect(result[0]!.name).toBe('Test Item');
  });
});
