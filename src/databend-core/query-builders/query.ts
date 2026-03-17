import { entityKind } from 'drizzle-orm/entity';
import { QueryPromise } from 'drizzle-orm/query-promise';
import { mapRelationalRow } from 'drizzle-orm/relations';
// @ts-expect-error - tracer is exported at runtime but not in .d.ts
import { tracer } from 'drizzle-orm/tracing';

export class RelationalQueryBuilder {
  static readonly [entityKind]: string = 'DatabendRelationalQueryBuilder';

  constructor(
    private fullSchema: any,
    private schema: any,
    private tableNamesMap: any,
    private table: any,
    private tableConfig: any,
    private dialect: any,
    private session: any
  ) {}

  findMany(config?: any) {
    return new DatabendRelationalQuery(
      this.fullSchema,
      this.schema,
      this.tableNamesMap,
      this.table,
      this.tableConfig,
      this.dialect,
      this.session,
      config ? config : {},
      'many'
    );
  }

  findFirst(config?: any) {
    return new DatabendRelationalQuery(
      this.fullSchema,
      this.schema,
      this.tableNamesMap,
      this.table,
      this.tableConfig,
      this.dialect,
      this.session,
      config ? { ...config, limit: 1 } : { limit: 1 },
      'first'
    );
  }
}

export class DatabendRelationalQuery extends QueryPromise<any> {
  static readonly [entityKind]: string = 'DatabendRelationalQuery';

  constructor(
    private fullSchema: any,
    private schema: any,
    private tableNamesMap: any,
    private table: any,
    private tableConfig: any,
    private dialect: any,
    private session: any,
    private config: any,
    private mode: 'many' | 'first'
  ) {
    super();
  }

  /** @internal */
  _prepare(name?: string) {
    return tracer.startActiveSpan('drizzle.prepareQuery', () => {
      const { query, builtQuery } = this._toSQL();
      return this.session.prepareQuery(
        builtQuery,
        undefined,
        name,
        true,
        (rawRows: any, mapColumnValue: any) => {
          const rows = rawRows.map(
            (row: any) => mapRelationalRow(this.schema, this.tableConfig, row, query.selection, mapColumnValue)
          );
          if (this.mode === 'first') {
            return rows[0];
          }
          return rows;
        }
      );
    });
  }

  prepare(name: string) {
    return this._prepare(name);
  }

  private _getQuery() {
    return this.dialect.buildRelationalQueryWithoutPK({
      fullSchema: this.fullSchema,
      schema: this.schema,
      tableNamesMap: this.tableNamesMap,
      table: this.table,
      tableConfig: this.tableConfig,
      queryConfig: this.config,
      tableAlias: this.tableConfig.tsName,
    });
  }

  /** @internal */
  getSQL() {
    return this._getQuery().sql;
  }

  private _toSQL() {
    const query = this._getQuery();
    const builtQuery = this.dialect.sqlToQuery(query.sql);
    return { query, builtQuery };
  }

  toSQL() {
    return this._toSQL().builtQuery;
  }

  execute() {
    return tracer.startActiveSpan('drizzle.operation', () => {
      return this._prepare().execute(undefined);
    });
  }
}
