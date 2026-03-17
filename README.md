# drizzle-databend

> This driver was created to power [drizzle-cube](https://try.drizzle-cube.dev) (an embeddable semantic layer built on Drizzle) and [drizby](https://github.com/cliftonc/drizby) (an open source BI platform built on drizzle-cube). It enables both projects to query Databend natively via Drizzle ORM.

A [Drizzle ORM](https://orm.drizzle.team/) driver for [Databend](https://databend.com/). Built on a standalone `databend-core` dialect module with `$1` positional parameters and double-quote identifier quoting.

Uses [`databend-driver`](https://github.com/databendlabs/bendsql/tree/main/bindings/nodejs) (NAPI-RS bindings) as the underlying client.

## Install

```sh
npm install drizzle-databend drizzle-orm databend-driver
```

## Quick start

```ts
import { drizzle, databendTable, integer, varchar, text } from 'drizzle-databend';
import { eq } from 'drizzle-orm';

const users = databendTable('users', {
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
import {
  databendTable,
  databendVariant,
  databendArray,
  databendTuple,
  databendMap,
  databendTimestamp,
  databendDate,
  integer,
} from 'drizzle-databend';

const events = databendTable('events', {
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
npm install           # Install dependencies
npm run build         # Build (dist/index.mjs + type declarations)
npm run typecheck     # Type-check with tsc
npm run lint          # Lint with biome
npm test              # Run all tests (requires running Databend)
```

## Architecture

- **`src/databend-core/`** -- Standalone dialect module (ported from drizzle-orm's gel-core)
  - `dialect.ts` -- SQL generation with `$1` params and `"` identifier quoting
  - `session.ts` -- Abstract session, prepared query, and transaction base classes
  - `db.ts` -- `DatabendDatabase` with select/insert/update/delete/execute/CTE support
  - `table.ts` -- `databendTable()` table definition function
  - `columns/` -- Column type builders (integer, varchar, boolean, timestamp, etc.)
  - `query-builders/` -- SELECT, INSERT, UPDATE, DELETE query builders
- **`src/driver.ts`** -- `drizzle()` factory, connection management, pool creation
- **`src/session.ts`** -- Concrete session with databend-driver query execution
- **`src/client.ts`** -- Low-level execution wrapping `databend-driver`'s `Connection` API
- **`src/pool.ts`** -- Connection pooling via `Client.getConn()`
- **`src/columns.ts`** -- Custom column types (VARIANT, ARRAY, TUPLE, MAP, TIMESTAMP, DATE)
- **`src/sql/result-mapper.ts`** -- Maps Databend results to Drizzle's expected format

## License

MIT
