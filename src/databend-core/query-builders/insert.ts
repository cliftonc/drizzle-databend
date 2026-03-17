import { entityKind, is } from 'drizzle-orm/entity';
import { QueryPromise } from 'drizzle-orm/query-promise';
import { Param, SQL } from 'drizzle-orm/sql/sql';
import { Table } from 'drizzle-orm/table';
// @ts-expect-error - tracer is exported at runtime but not in .d.ts
import { tracer } from 'drizzle-orm/tracing';
import { haveSameKeys } from 'drizzle-orm/utils';
import { QueryBuilder } from './query-builder.ts';

export class DatabendInsertBuilder {
  static readonly [entityKind]: string = 'DatabendInsertBuilder';

  constructor(
    private table: any,
    private session: any,
    private dialect: any,
    private withList?: any[]
  ) {}

  values(values: any) {
    values = Array.isArray(values) ? values : [values];
    if (values.length === 0) {
      throw new Error('values() must be called with at least one value');
    }
    const mappedValues = values.map((entry: any) => {
      const result: any = {};
      const cols = this.table[(Table as any).Symbol.Columns];
      for (const colKey of Object.keys(entry)) {
        const colValue = entry[colKey];
        result[colKey] = is(colValue, SQL) ? colValue : new Param(colValue, cols[colKey]);
      }
      return result;
    });

    return new DatabendInsertBase(
      this.table,
      mappedValues,
      this.session,
      this.dialect,
      this.withList,
      false
    );
  }

  select(selectQuery: any) {
    const select = typeof selectQuery === 'function' ? selectQuery(new QueryBuilder()) : selectQuery;

    if (!is(select, SQL) && !haveSameKeys(this.table[(Table as any).Symbol.Columns], select._.selectedFields)) {
      throw new Error(
        'Insert select error: selected fields are not the same or are in a different order compared to the table definition'
      );
    }

    return new DatabendInsertBase(this.table, select, this.session, this.dialect, this.withList, true);
  }
}

export class DatabendInsertBase extends QueryPromise<any> {
  static readonly [entityKind]: string = 'DatabendInsert';

  config: any;

  constructor(
    table: any,
    values: any,
    private session: any,
    private dialect: any,
    withList?: any[],
    select?: boolean
  ) {
    super();
    this.config = { table, values, withList, select };
  }

  /** @internal */
  getSQL() {
    return this.dialect.buildInsertQuery(this.config);
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
