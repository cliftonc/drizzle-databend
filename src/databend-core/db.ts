import { entityKind } from 'drizzle-orm/entity';
import { SelectionProxyHandler } from 'drizzle-orm/selection-proxy';
import { sql } from 'drizzle-orm/sql/sql';
import { WithSubquery } from 'drizzle-orm/subquery';
import { DatabendCountBuilder } from './query-builders/count.ts';
import {
  DatabendDeleteBase,
  DatabendInsertBuilder,
  DatabendSelectBuilder,
  DatabendUpdateBuilder,
  QueryBuilder,
} from './query-builders/index.ts';
import { RelationalQueryBuilder } from './query-builders/query.ts';
import { DatabendRaw } from './query-builders/raw.ts';

export class DatabendDatabase {
  static readonly [entityKind]: string = 'DatabendDatabase';

  _: any;
  query: any;

  constructor(
    public dialect: any,
    public session: any,
    schema?: any
  ) {
    this._ = schema
      ? {
          schema: schema.schema,
          fullSchema: schema.fullSchema,
          tableNamesMap: schema.tableNamesMap,
          session,
        }
      : {
          schema: undefined,
          fullSchema: {},
          tableNamesMap: {},
          session,
        };

    this.query = {};
    if (this._.schema) {
      for (const [tableName, columns] of Object.entries(this._.schema)) {
        (this.query as any)[tableName] = new RelationalQueryBuilder(
          schema.fullSchema,
          this._.schema,
          this._.tableNamesMap,
          schema.fullSchema[tableName],
          columns,
          dialect,
          session
        );
      }
    }
  }

  $with(alias: string) {
    const self = this;
    return {
      as(qb: any) {
        if (typeof qb === 'function') {
          qb = qb(new QueryBuilder(self.dialect));
        }
        const sqlObj = typeof qb.getSQL === 'function' ? qb.getSQL() : qb;
        const fields = typeof qb.getSelectedFields === 'function' ? qb.getSelectedFields() : {};
        return new Proxy(
          new WithSubquery(sqlObj, fields, alias, true),
          new SelectionProxyHandler({ alias, sqlAliasedBehavior: 'alias', sqlBehavior: 'error' })
        );
      },
    };
  }

  $count(source: any, filters?: any) {
    return new DatabendCountBuilder({ source, filters, session: this.session });
  }

  with(...queries: any[]) {
    const self = this;
    function select(fields?: any) {
      return new DatabendSelectBuilder({
        fields: fields ?? undefined,
        session: self.session,
        dialect: self.dialect,
        withList: queries,
      });
    }
    function selectDistinct(fields?: any) {
      return new DatabendSelectBuilder({
        fields: fields ?? undefined,
        session: self.session,
        dialect: self.dialect,
        withList: queries,
        distinct: true,
      });
    }
    function update(table: any) {
      return new DatabendUpdateBuilder(table, self.session, self.dialect, queries);
    }
    function insert(table: any) {
      return new DatabendInsertBuilder(table, self.session, self.dialect, queries);
    }
    function delete_(table: any) {
      return new DatabendDeleteBase(table, self.session, self.dialect, queries);
    }
    return { select, selectDistinct, update, insert, delete: delete_ };
  }

  select(fields?: any) {
    return new DatabendSelectBuilder({
      fields: fields ?? undefined,
      session: this.session,
      dialect: this.dialect,
    });
  }

  selectDistinct(fields?: any) {
    return new DatabendSelectBuilder({
      fields: fields ?? undefined,
      session: this.session,
      dialect: this.dialect,
      distinct: true,
    });
  }

  update(table: any) {
    return new DatabendUpdateBuilder(table, this.session, this.dialect);
  }

  insert(table: any) {
    return new DatabendInsertBuilder(table, this.session, this.dialect);
  }

  delete(table: any) {
    return new DatabendDeleteBase(table, this.session, this.dialect);
  }

  execute(query: any) {
    const sequel = typeof query === 'string' ? sql.raw(query) : query.getSQL();
    const builtQuery = this.dialect.sqlToQuery(sequel);
    const prepared = this.session.prepareQuery(builtQuery, undefined, undefined, false);
    return new DatabendRaw(
      () => prepared.execute(undefined),
      sequel,
      builtQuery,
      (result: any) => prepared.mapResult(result, true)
    );
  }

  transaction(transaction: any) {
    return this.session.transaction(transaction);
  }
}
