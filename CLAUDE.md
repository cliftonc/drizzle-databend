# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is `drizzle-databend`, a Databend dialect adapter for drizzle-orm. It builds on Drizzle's Postgres driver surface but targets Databend, providing query building, migrations, and type inference for Databend's Node runtime (`databend-driver`).

## Commands

- **Install dependencies:** `npm install`
- **Run all tests:** `npm test`
- **Run a single test file:** `npx vitest run test/<filename>.test.ts`
- **Build:** `npm run build` (emits `dist/index.mjs` and type declarations)
- **Build declarations only:** `npm run build:declarations`
- **Start Databend:** `npm run db:up`
- **Stop Databend:** `npm run db:down`
- **Reset Databend (drop volumes):** `npm run db:destroy`

## Architecture

### Core Module Structure (`src/`)

The package exports from `src/index.ts` which re-exports:

- `driver.ts` - Main entry point with `drizzle()` factory and `DatabendDatabase` class extending `PgDatabase`
- `session.ts` - `DatabendSession` and `DatabendPreparedQuery` for query execution, transaction handling
- `dialect.ts` - `DatabendDialect` extending `PgDialect` with Databend-specific SQL generation
- `columns.ts` - Databend-specific column helpers (`databendVariant`, `databendArray`, `databendTuple`, `databendMap`, `databendTimestamp`, etc.)
- `pool.ts` - Connection pooling with `createDatabendConnectionPool()` for concurrent query execution
- `client.ts` - Low-level client utilities
- `migrator.ts` - `migrate()` function for applying SQL migrations

### SQL Transformation Pipeline (`src/sql/`)

- `result-mapper.ts` - Converts Databend query results to Drizzle's expected format
- `selection.ts` - Selection/projection handling with alias deduplication

### Key Design Decisions

1. **Built on Postgres Driver**: Extends `PgDialect`, `PgSession`, `PgDatabase` from `drizzle-orm/pg-core` since Databend supports double-quote identifier quoting. Note the dialect emits `?` placeholders (not `$1`): Databend reads `$N` as a column-position reference, which forces `databend-driver` onto client-side interpolation, whereas `?` is bound server-side.

5. **Server-side parameter binding**: With `databend-driver >= 0.34.0` against a Databend server `> 1.2.900`, parameters are bound server-side via the `/v1/query` `params` field. The driver owns escaping (server-side binding, or its own client-side escaping against older servers), so `src/client.ts::prepareParams` must NOT pre-escape values - it only coerces JS types (bigint, Date, structured values) the JSON params array cannot carry verbatim.

2. **Custom Column Types**: Databend-specific types (VARIANT, ARRAY, TUPLE, MAP) use custom type builders

3. **Connection Pooling**: Databend connections are stateful server-side sessions; the pool enables concurrent queries

4. **No Savepoints**: Databend does not support savepoints; nested transactions use rollback-only fallback

### Testing

Tests are in `test/` using Vitest. Requires a running Databend instance at `localhost:8124` (mapped from the container's `8000`; start it with `npm run db:up`). Exercising the server-side parameter-binding path requires a server `> 1.2.900` - the `docker-compose.yml` image is pinned to a nightly above that threshold.

## Important Conventions

- ESM only with explicit `.ts` extensions in imports
- Source uses `moduleResolution: bundler`
- Never edit files in `dist/` - they are generated
- Never use emojis in comments or code
- Be concise and to the point
