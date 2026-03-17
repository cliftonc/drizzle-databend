import { entityKind } from 'drizzle-orm/entity';
import { QueryPromise } from 'drizzle-orm/query-promise';
// @ts-expect-error - tracer is exported at runtime but not in .d.ts
import { tracer } from 'drizzle-orm/tracing';

export class DatabendDeleteBase extends QueryPromise<any> {
  static readonly [entityKind]: string = 'DatabendDelete';

  config: any;

  constructor(
    table: any,
    private session: any,
    private dialect: any,
    withList?: any[]
  ) {
    super();
    this.config = { table, withList };
  }

  where(where: any) {
    this.config.where = where;
    return this;
  }

  /** @internal */
  getSQL() {
    return this.dialect.buildDeleteQuery(this.config);
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
