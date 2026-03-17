import { entityKind, is } from 'drizzle-orm/entity';
import { QueryPromise } from 'drizzle-orm/query-promise';
import { SQL } from 'drizzle-orm/sql/sql';
import { Table } from 'drizzle-orm/table';
// @ts-expect-error - tracer is exported at runtime but not in .d.ts
import { tracer } from 'drizzle-orm/tracing';
// @ts-expect-error - mapUpdateSet is exported at runtime but not in .d.ts
import { mapUpdateSet } from 'drizzle-orm/utils';

export class DatabendUpdateBuilder {
  static readonly [entityKind]: string = 'DatabendUpdateBuilder';

  constructor(
    private table: any,
    private session: any,
    private dialect: any,
    private withList?: any[]
  ) {}

  set(values: any) {
    return new DatabendUpdateBase(
      this.table,
      mapUpdateSet(this.table, values),
      this.session,
      this.dialect,
      this.withList
    );
  }
}

export class DatabendUpdateBase extends QueryPromise<any> {
  static readonly [entityKind]: string = 'DatabendUpdate';

  config: any;

  constructor(
    table: any,
    set: any,
    private session: any,
    private dialect: any,
    withList?: any[]
  ) {
    super();
    this.config = { set, table, withList };
  }

  where(where: any) {
    this.config.where = where;
    return this;
  }

  /** @internal */
  getSQL() {
    return this.dialect.buildUpdateQuery(this.config);
  }

  toSQL() {
    const { typings: _typings, ...rest } = this.dialect.sqlToQuery(this.getSQL());
    return rest;
  }

  /** @internal */
  _prepare(name?: string) {
    return tracer.startActiveSpan('drizzle.prepareQuery', () => {
      return this.session.prepareQuery(this.dialect.sqlToQuery(this.getSQL()), undefined, name, true);
    });
  }

  prepare(name: string) {
    return this._prepare(name);
  }

  execute = (placeholderValues?: any) => {
    return tracer.startActiveSpan('drizzle.operation', () => {
      return this._prepare().execute(placeholderValues);
    });
  };

  $dynamic() {
    return this;
  }
}
