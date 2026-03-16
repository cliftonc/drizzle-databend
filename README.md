# drizzle-databend

A [Drizzle ORM](https://orm.drizzle.team/) driver for [Databend](https://databend.com/). Built on Drizzle's Postgres driver surface (`pg-core`) since Databend supports `$1` positional parameters and double-quote identifier quoting.

Uses [`databend-driver`](https://github.com/databendlabs/bendsql/tree/main/bindings/nodejs) (NAPI-RS bindings) as the underlying client.

## Install

```sh
bun add drizzle-databend drizzle-orm databend-driver
```

## Quick start

```ts
import { drizzle } from 'drizzle-databend';
import { pgTable, integer, varchar, text } from 'drizzle-orm/pg-core';
import { eq } from 'drizzle-orm';

const users = pgTable('users', {
  id: integer('id').notNull(),
  name: varchar('name', { length: 256 }).notNull(),
  email: text('email'),
});

const db = await drizzle('databend://databend:databend@localhost:8000/default?sslmode=disable');

// Insert
await db.insert(users).values({ id: 1, name: 'Alice', email: 'alice@example.com' });

// Select
const rows = await db.select().from(users).where(eq(users.name, 'Alice'));
```

## Databend-specific column types

```ts
import { databendVariant, databendArray, databendTuple, databendMap, databendTimestamp, databendDate } from 'drizzle-databend';

const events = pgTable('events', {
  id: integer('id').notNull(),
  payload: databendVariant('payload'),         // VARIANT (semi-structured JSON)
  tags: databendArray('tags', 'VARCHAR'),       // ARRAY(VARCHAR)
  coords: databendTuple('coords', ['FLOAT', 'FLOAT']), // TUPLE(FLOAT, FLOAT)
  attrs: databendMap('attrs', 'VARCHAR', 'VARCHAR'),    // MAP(VARCHAR, VARCHAR)
  createdAt: databendTimestamp('created_at'),   // TIMESTAMP
  eventDate: databendDate('event_date'),        // DATE
});
```

## Connection options

```ts
// DSN string (async, auto-pools with 4 connections)
const db = await drizzle('databend://user:pass@host:8000/db?sslmode=disable');

// With config
const db = await drizzle('databend://...', { pool: { size: 8 }, logger: true });

// Config object
const db = await drizzle({ connection: 'databend://...', schema: mySchema });

// Explicit client (sync, no pooling)
import { Client } from 'databend-driver';
const conn = await new Client('databend://...').getConn();
const db = drizzle({ client: conn });

// Disable pooling
const db = await drizzle('databend://...', { pool: false });
```

## Transactions

```ts
await db.transaction(async (tx) => {
  await tx.insert(users).values({ id: 2, name: 'Bob', email: 'bob@example.com' });
  await tx.update(users).set({ name: 'Robert' }).where(eq(users.id, 2));
});
```

Note: Databend does not support savepoints. Nested transactions use a rollback-only fallback.

## Migrations

```ts
import { migrate } from 'drizzle-databend';

await migrate(db, { migrationsFolder: './drizzle' });
```

## Development

### Prerequisites

You need a running Databend instance. The quickest way is Docker:

```sh
docker run -d \
    --name databend \
    --network host \
    -e MINIO_ENABLED=true \
    -e QUERY_DEFAULT_USER=databend \
    -e QUERY_DEFAULT_PASSWORD=databend \
    -v minio_data_dir:/var/lib/minio \
    --restart unless-stopped \
    datafuselabs/databend
```

See the [Databend self-hosted quickstart](https://docs.databend.com/guides/self-hosted/quickstart) for more options.

Verify the connection:

```sh
bendsql -udatabend -pdatabend
```

### Commands

```sh
bun install           # Install dependencies
bun run build         # Build (dist/index.mjs + type declarations)
bun test              # Run integration tests (requires running Databend)
```

### Seed data

Populate two sample tables (`users` and `events`) for manual testing:

```sh
bun run scripts/seed.ts
```

This creates 5 users and 10 events with VARIANT payloads and timestamps.

## Architecture

Built on the same pattern as [drizzle-duckdb](https://github.com/leonardovida/drizzle-duckdb):

- **`driver.ts`** -- `drizzle()` factory and `DatabendDatabase` extending `PgDatabase`
- **`session.ts`** -- `DatabendSession` and `DatabendPreparedQuery` for query execution
- **`dialect.ts`** -- `DatabendDialect` extending `PgDialect` with Databend-specific migrations and type mapping
- **`client.ts`** -- Low-level execution wrapping `databend-driver`'s `Connection` API
- **`pool.ts`** -- Connection pooling via `Client.getConn()`
- **`columns.ts`** -- Custom column types (VARIANT, ARRAY, TUPLE, MAP, TIMESTAMP, DATE)
- **`sql/result-mapper.ts`** -- Maps Databend results to Drizzle's expected format

## License

MIT
