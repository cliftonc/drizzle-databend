import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { eq, sql } from 'drizzle-orm';
import {
  pgTable,
  integer,
  varchar,
  text,
  boolean,
  real,
  doublePrecision,
  bigint,
} from 'drizzle-orm/pg-core';
import { drizzle, type DatabendDatabase } from '../src/index.ts';
import { databendVariant, databendTimestamp, databendDate } from '../src/columns.ts';

const DSN = process.env.DATABEND_DSN ?? 'databend://databend:databend@localhost:8000/default?sslmode=disable';

const users = pgTable('drizzle_test_users', {
  id: integer('id').notNull(),
  name: varchar('name', { length: 256 }).notNull(),
  email: text('email'),
  active: boolean('active').default(true),
  score: real('score'),
});

const items = pgTable('drizzle_test_items', {
  id: integer('id').notNull(),
  name: varchar('name', { length: 256 }).notNull(),
  metadata: databendVariant('metadata'),
  createdAt: databendTimestamp('created_at'),
});

describe('drizzle-databend', () => {
  let db: DatabendDatabase;

  beforeAll(async () => {
    db = await drizzle(DSN);

    // Clean up any leftover tables
    await db.execute(sql`DROP TABLE IF EXISTS drizzle_test_users`);
    await db.execute(sql`DROP TABLE IF EXISTS drizzle_test_items`);

    // Create test tables
    await db.execute(sql`
      CREATE TABLE drizzle_test_users (
        id INT NOT NULL,
        name VARCHAR(256) NOT NULL,
        email VARCHAR NULL,
        active BOOLEAN DEFAULT TRUE,
        score FLOAT NULL
      )
    `);

    await db.execute(sql`
      CREATE TABLE drizzle_test_items (
        id INT NOT NULL,
        name VARCHAR(256) NOT NULL,
        metadata VARIANT NULL,
        created_at TIMESTAMP NULL
      )
    `);
  });

  afterAll(async () => {
    await db.execute(sql`DROP TABLE IF EXISTS drizzle_test_users`);
    await db.execute(sql`DROP TABLE IF EXISTS drizzle_test_items`);
    await db.close();
  });

  it('should connect and execute raw SQL', async () => {
    const result = await db.execute(sql`SELECT 1 AS val`);
    expect(result).toBeDefined();
  });

  it('should insert and select rows', async () => {
    await db.insert(users).values([
      { id: 1, name: 'Alice', email: 'alice@test.com', score: 9.5 },
      { id: 2, name: 'Bob', email: 'bob@test.com', score: 8.0 },
      { id: 3, name: 'Charlie', email: null, score: 7.5 },
    ]);

    const result = await db.select().from(users);
    expect(result.length).toBe(3);
  });

  it('should filter with where clause', async () => {
    const result = await db
      .select()
      .from(users)
      .where(eq(users.name, 'Alice'));
    expect(result.length).toBe(1);
    expect(result[0]!.name).toBe('Alice');
    expect(result[0]!.email).toBe('alice@test.com');
  });

  it('should update rows', async () => {
    await db
      .update(users)
      .set({ score: 10.0 })
      .where(eq(users.name, 'Alice'));

    const result = await db
      .select()
      .from(users)
      .where(eq(users.name, 'Alice'));
    expect(result[0]!.score).toBe(10.0);
  });

  it('should delete rows', async () => {
    await db.delete(users).where(eq(users.name, 'Charlie'));

    const result = await db.select().from(users);
    expect(result.length).toBe(2);
  });

  it('should handle VARIANT column type', async () => {
    const meta = { tags: ['important', 'urgent'], priority: 1 };
    await db.insert(items).values({
      id: 1,
      name: 'Test Item',
      metadata: meta,
      createdAt: new Date('2024-01-01T00:00:00Z'),
    });

    const result = await db
      .select()
      .from(items)
      .where(eq(items.name, 'Test Item'));
    expect(result.length).toBe(1);
    expect(result[0]!.name).toBe('Test Item');
  });

  it('should handle transactions', async () => {
    await db.transaction(async (tx) => {
      await tx.insert(users).values({
        id: 4,
        name: 'TransactionUser',
        email: 'tx@test.com',
        score: 5.0,
      });
    });

    const result = await db
      .select()
      .from(users)
      .where(eq(users.name, 'TransactionUser'));
    expect(result.length).toBe(1);
  });
});
