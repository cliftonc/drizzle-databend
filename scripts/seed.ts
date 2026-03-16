import { sql } from 'drizzle-orm';
import { pgTable, integer, varchar, text, boolean, real } from 'drizzle-orm/pg-core';
import { drizzle } from '../src/index.ts';
import { databendVariant, databendTimestamp } from '../src/columns.ts';

const DSN = process.env.DATABEND_DSN ?? 'databend://databend:databend@localhost:8000/default?sslmode=disable';

const users = pgTable('users', {
  id: integer('id').notNull(),
  name: varchar('name', { length: 256 }).notNull(),
  email: text('email'),
  role: varchar('role', { length: 64 }).notNull(),
  active: boolean('active').default(true),
  score: real('score'),
});

const events = pgTable('events', {
  id: integer('id').notNull(),
  userId: integer('user_id').notNull(),
  action: varchar('action', { length: 128 }).notNull(),
  payload: databendVariant('payload'),
  createdAt: databendTimestamp('created_at'),
});

async function seed() {
  const db = await drizzle(DSN);

  console.log('Dropping existing tables...');
  await db.execute(sql`DROP TABLE IF EXISTS events`);
  await db.execute(sql`DROP TABLE IF EXISTS users`);

  console.log('Creating tables...');
  await db.execute(sql`
    CREATE TABLE users (
      id INT NOT NULL,
      name VARCHAR(256) NOT NULL,
      email VARCHAR NULL,
      role VARCHAR(64) NOT NULL,
      active BOOLEAN DEFAULT TRUE,
      score FLOAT NULL
    )
  `);

  await db.execute(sql`
    CREATE TABLE events (
      id INT NOT NULL,
      user_id INT NOT NULL,
      action VARCHAR(128) NOT NULL,
      payload VARIANT NULL,
      created_at TIMESTAMP NULL
    )
  `);

  console.log('Inserting users...');
  await db.insert(users).values([
    { id: 1, name: 'Alice Chen', email: 'alice@example.com', role: 'admin', active: true, score: 95.5 },
    { id: 2, name: 'Bob Smith', email: 'bob@example.com', role: 'editor', active: true, score: 82.0 },
    { id: 3, name: 'Carol Wu', email: 'carol@example.com', role: 'viewer', active: true, score: 78.3 },
    { id: 4, name: 'Dave Jones', email: 'dave@example.com', role: 'editor', active: false, score: 64.1 },
    { id: 5, name: 'Eve Park', email: 'eve@example.com', role: 'admin', active: true, score: 91.7 },
  ]);

  console.log('Inserting events...');
  await db.insert(events).values([
    { id: 1, userId: 1, action: 'login', payload: { ip: '192.168.1.10', browser: 'Chrome' }, createdAt: new Date('2024-12-01T09:00:00Z') },
    { id: 2, userId: 1, action: 'page_view', payload: { path: '/dashboard', duration_ms: 4500 }, createdAt: new Date('2024-12-01T09:01:00Z') },
    { id: 3, userId: 2, action: 'login', payload: { ip: '10.0.0.5', browser: 'Firefox' }, createdAt: new Date('2024-12-01T10:30:00Z') },
    { id: 4, userId: 2, action: 'edit', payload: { resource: 'post', resourceId: 42, changes: ['title', 'body'] }, createdAt: new Date('2024-12-01T10:35:00Z') },
    { id: 5, userId: 3, action: 'login', payload: { ip: '172.16.0.1', browser: 'Safari' }, createdAt: new Date('2024-12-01T11:00:00Z') },
    { id: 6, userId: 3, action: 'page_view', payload: { path: '/reports', duration_ms: 12000 }, createdAt: new Date('2024-12-01T11:05:00Z') },
    { id: 7, userId: 1, action: 'edit', payload: { resource: 'user', resourceId: 4, changes: ['active'] }, createdAt: new Date('2024-12-01T14:00:00Z') },
    { id: 8, userId: 5, action: 'login', payload: { ip: '192.168.1.20', browser: 'Chrome' }, createdAt: new Date('2024-12-02T08:00:00Z') },
    { id: 9, userId: 5, action: 'page_view', payload: { path: '/settings', duration_ms: 3200 }, createdAt: new Date('2024-12-02T08:05:00Z') },
    { id: 10, userId: 4, action: 'login', payload: { ip: '10.0.0.12', browser: 'Edge' }, createdAt: new Date('2024-12-02T09:00:00Z') },
  ]);

  const userCount = await db.select().from(users);
  const eventCount = await db.select().from(events);
  console.log(`Seeded ${userCount.length} users and ${eventCount.length} events.`);

  await db.close();
}

seed().catch((err) => {
  console.error('Seed failed:', err);
  process.exit(1);
});
