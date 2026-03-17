import { avg, count, eq, max, min, sum } from 'drizzle-orm';
import { beforeAll, describe, expect, it } from 'vitest';
import type { DatabendDatabase } from '../src/index.ts';
import { drizzle } from '../src/index.ts';
import { DSN, getDb, products, users } from './setup.ts';

describe('concurrent queries', () => {
  let db: DatabendDatabase;

  beforeAll(async () => {
    db = await getDb();
  });

  it('should handle multiple ORM selects via Promise.all', async () => {
    const [usersResult, productsResult, countResult] = await Promise.all([
      db.select().from(users),
      db.select().from(products),
      db.select({ cnt: count() }).from(users),
    ]);

    expect(usersResult).toHaveLength(4);
    expect(productsResult).toHaveLength(5);
    expect(Number(countResult[0]!.cnt)).toBe(4);
  });

  it('should handle many concurrent filtered reads', async () => {
    const promises = Array.from({ length: 10 }, (_, i) =>
      db.select().from(users).where(eq(users.id, (i % 4) + 1))
    );

    const results = await Promise.all(promises);
    for (const result of results) {
      expect(result).toHaveLength(1);
    }
  });

  it('should handle concurrent aggregates', async () => {
    const results = await Promise.all([
      db.select({ cnt: count() }).from(users),
      db.select({ total: sum(products.price) }).from(products),
      db.select({ average: avg(products.quantity) }).from(products),
      db.select({ highest: max(users.score) }).from(users),
      db.select({ lowest: min(products.price) }).from(products),
    ]);

    for (const result of results) {
      expect(result).toHaveLength(1);
    }
    expect(Number(results[0]![0]!.cnt)).toBe(4);
  });

  it('should handle concurrent mixed reads with filters', async () => {
    const results = await Promise.all([
      db.select().from(users).where(eq(users.active, true)),
      db.select({ cnt: count() }).from(products),
      db.select().from(products).where(eq(products.active, true)),
      db.select().from(users).where(eq(users.name, 'Alice')),
    ]);

    expect(results[0]).toHaveLength(3);
    expect(Number(results[1]![0]!.cnt)).toBe(5);
    expect(results[2]).toHaveLength(4);
    expect(results[3]).toHaveLength(1);
  });
});

describe('pool stress under load', () => {
  let db: DatabendDatabase;

  beforeAll(async () => {
    // Create a pool with size=2 to maximize contention
    db = await drizzle(DSN, { pool: { size: 2 } });
  });

  it('should handle 20 concurrent queries on pool size 2', async () => {
    const promises = Array.from({ length: 20 }, (_, i) =>
      db.select().from(users).where(eq(users.id, (i % 4) + 1))
    );

    const results = await Promise.all(promises);
    expect(results).toHaveLength(20);
    for (const result of results) {
      expect(result).toHaveLength(1);
    }
  });

  it('should handle 50 concurrent queries on pool size 2', async () => {
    const promises = Array.from({ length: 50 }, (_, i) => {
      if (i % 3 === 0) return db.select({ cnt: count() }).from(products);
      if (i % 3 === 1) return db.select().from(users).where(eq(users.id, (i % 4) + 1));
      return db.select({ total: sum(products.price) }).from(products);
    });

    const results = await Promise.all(promises);
    expect(results).toHaveLength(50);
    for (const result of results) {
      expect(result).toHaveLength(1);
    }
  });

  it('should handle concurrent queries with parameterized strings', async () => {
    const names = ['Alice', 'Bob', "O'Brien", 'Charlie'];
    const promises = Array.from({ length: 40 }, (_, i) =>
      db.select().from(users).where(eq(users.name, names[i % names.length]!))
    );

    const results = await Promise.all(promises);
    expect(results).toHaveLength(40);
    for (const result of results) {
      expect(result).toHaveLength(1);
    }
  });

  it('should handle concurrent mixed selects and aggregates', async () => {
    const promises = Array.from({ length: 30 }, (_, i) => {
      switch (i % 5) {
        case 0: return db.select().from(users);
        case 1: return db.select().from(products);
        case 2: return db.select({ cnt: count() }).from(users);
        case 3: return db.select({ avg: avg(products.price) }).from(products);
        default: return db.select().from(users).where(eq(users.active, true));
      }
    });

    const results = await Promise.all(promises);
    expect(results).toHaveLength(30);
    for (const result of results) {
      expect(result.length).toBeGreaterThan(0);
    }
  });

  it('should handle sequential batches of concurrent queries', async () => {
    for (let batch = 0; batch < 5; batch++) {
      const promises = Array.from({ length: 10 }, (_, i) =>
        db.select().from(users).where(eq(users.id, (i % 4) + 1))
      );

      const results = await Promise.all(promises);
      expect(results).toHaveLength(10);
      for (const result of results) {
        expect(result).toHaveLength(1);
      }
    }
  });
});
