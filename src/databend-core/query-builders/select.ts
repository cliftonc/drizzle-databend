import { entityKind, is } from 'drizzle-orm/entity';
import { TypedQueryBuilder } from 'drizzle-orm/query-builders/query-builder';
import { QueryPromise } from 'drizzle-orm/query-promise';
import { SelectionProxyHandler } from 'drizzle-orm/selection-proxy';
import { SQL, View } from 'drizzle-orm/sql/sql';
import { Subquery } from 'drizzle-orm/subquery';
import { Table } from 'drizzle-orm/table';
// @ts-expect-error - tracer is exported at runtime but not in .d.ts
import { tracer } from 'drizzle-orm/tracing';
import {
  // @ts-expect-error - exported at runtime but not in .d.ts
  applyMixins,
  getTableColumns,
  // @ts-expect-error - exported at runtime but not in .d.ts
  getTableLikeName,
  haveSameKeys,
  // @ts-expect-error - exported at runtime but not in .d.ts
  orderSelectedFields,
} from 'drizzle-orm/utils';
import { ViewBaseConfig } from 'drizzle-orm/view-common';
import { DatabendColumn } from '../columns/common.ts';
import { DatabendViewBase } from '../view-base.ts';

export class DatabendSelectBuilder {
  static readonly [entityKind]: string = 'DatabendSelectBuilder';

  fields: any;
  session: any;
  dialect: any;
  withList: any[] = [];
  distinct: any;

  constructor(config: any) {
    this.fields = config.fields;
    this.session = config.session;
    this.dialect = config.dialect;
    if (config.withList) {
      this.withList = config.withList;
    }
    this.distinct = config.distinct;
  }

  from(source: any): any {
    const isPartialSelect = !!this.fields;

    let fields: any;
    if (this.fields) {
      fields = this.fields;
    } else if (is(source, Subquery)) {
      fields = Object.fromEntries(
        Object.keys(source._.selectedFields).map((key) => [key, (source as any)[key]])
      );
    } else if (is(source, DatabendViewBase)) {
      fields = (source as any)[ViewBaseConfig].selectedFields;
    } else if (is(source, SQL)) {
      fields = {};
    } else {
      fields = getTableColumns(source);
    }

    return new DatabendSelectBase({
      table: source,
      fields,
      isPartialSelect,
      session: this.session,
      dialect: this.dialect,
      withList: this.withList,
      distinct: this.distinct,
    });
  }
}

class DatabendSelectQueryBuilderBase extends TypedQueryBuilder<any, any> {
  static readonly [entityKind]: string = 'DatabendSelectQueryBuilder';

  _: any;
  config: any;
  joinsNotNullableMap: any;
  tableName: any;
  isPartialSelect: any;
  session: any;
  dialect: any;

  constructor({ table, fields, isPartialSelect, session, dialect, withList, distinct }: any) {
    super();
    this.config = {
      withList,
      table,
      fields: { ...fields },
      distinct,
      setOperators: [],
    };
    this.isPartialSelect = isPartialSelect;
    this.session = session;
    this.dialect = dialect;
    this._ = {
      selectedFields: fields,
    };
    this.tableName = getTableLikeName(table);
    this.joinsNotNullableMap = typeof this.tableName === 'string' ? { [this.tableName]: true } : {};
  }

  private createJoin(joinType: string) {
    return (table: any, on: any) => {
      const baseTableName = this.tableName;
      const tableName = getTableLikeName(table);

      if (typeof tableName === 'string' && this.config.joins?.some((join: any) => join.alias === tableName)) {
        throw new Error(`Alias "${tableName}" is already used in this query`);
      }

      if (!this.isPartialSelect) {
        if (Object.keys(this.joinsNotNullableMap).length === 1 && typeof baseTableName === 'string') {
          this.config.fields = {
            [baseTableName]: this.config.fields,
          };
        }
        if (typeof tableName === 'string' && !is(table, SQL)) {
          const selection = is(table, Subquery)
            ? table._.selectedFields
            : is(table, View)
              ? (table as any)[ViewBaseConfig].selectedFields
              : table[(Table as any).Symbol.Columns];
          this.config.fields[tableName] = selection;
        }
      }

      if (typeof on === 'function') {
        on = on(
          new Proxy(
            this.config.fields,
            new SelectionProxyHandler({ sqlAliasedBehavior: 'sql', sqlBehavior: 'sql' })
          )
        );
      }

      if (!this.config.joins) {
        this.config.joins = [];
      }
      this.config.joins.push({ on, table, joinType, alias: tableName });

      if (typeof tableName === 'string') {
        switch (joinType) {
          case 'left':
            this.joinsNotNullableMap[tableName] = false;
            break;
          case 'right':
            this.joinsNotNullableMap = Object.fromEntries(
              Object.entries(this.joinsNotNullableMap).map(([key]) => [key, false])
            );
            this.joinsNotNullableMap[tableName] = true;
            break;
          case 'inner':
            this.joinsNotNullableMap[tableName] = true;
            break;
          case 'full':
            this.joinsNotNullableMap = Object.fromEntries(
              Object.entries(this.joinsNotNullableMap).map(([key]) => [key, false])
            );
            this.joinsNotNullableMap[tableName] = false;
            break;
        }
      }
      return this;
    };
  }

  leftJoin = this.createJoin('left');
  rightJoin = this.createJoin('right');
  innerJoin = this.createJoin('inner');
  fullJoin = this.createJoin('full');

  private createSetOperator(type: string, isAll: boolean) {
    return (rightSelection: any) => {
      const rightSelect = typeof rightSelection === 'function'
        ? rightSelection(getDatabendSetOperators())
        : rightSelection;
      if (!haveSameKeys(this.getSelectedFields(), rightSelect.getSelectedFields())) {
        throw new Error(
          'Set operator error (union / intersect / except): selected fields are not the same or are in a different order'
        );
      }
      this.config.setOperators.push({ type, isAll, rightSelect });
      return this;
    };
  }

  union = this.createSetOperator('union', false);
  unionAll = this.createSetOperator('union', true);
  intersect = this.createSetOperator('intersect', false);
  intersectAll = this.createSetOperator('intersect', true);
  except = this.createSetOperator('except', false);
  exceptAll = this.createSetOperator('except', true);

  /** @internal */
  addSetOperators(setOperators: any[]) {
    this.config.setOperators.push(...setOperators);
    return this;
  }

  where(where: any) {
    if (typeof where === 'function') {
      where = where(
        new Proxy(
          this.config.fields,
          new SelectionProxyHandler({ sqlAliasedBehavior: 'sql', sqlBehavior: 'sql' })
        )
      );
    }
    this.config.where = where;
    return this;
  }

  having(having: any) {
    if (typeof having === 'function') {
      having = having(
        new Proxy(
          this.config.fields,
          new SelectionProxyHandler({ sqlAliasedBehavior: 'sql', sqlBehavior: 'sql' })
        )
      );
    }
    this.config.having = having;
    return this;
  }

  groupBy(...columns: any[]) {
    if (typeof columns[0] === 'function') {
      const groupBy = columns[0](
        new Proxy(
          this.config.fields,
          new SelectionProxyHandler({ sqlAliasedBehavior: 'alias', sqlBehavior: 'sql' })
        )
      );
      this.config.groupBy = Array.isArray(groupBy) ? groupBy : [groupBy];
    } else {
      this.config.groupBy = columns;
    }
    return this;
  }

  orderBy(...columns: any[]) {
    if (typeof columns[0] === 'function') {
      const orderBy = columns[0](
        new Proxy(
          this.config.fields,
          new SelectionProxyHandler({ sqlAliasedBehavior: 'alias', sqlBehavior: 'sql' })
        )
      );
      const orderByArray = Array.isArray(orderBy) ? orderBy : [orderBy];
      if (this.config.setOperators.length > 0) {
        this.config.setOperators.at(-1).orderBy = orderByArray;
      } else {
        this.config.orderBy = orderByArray;
      }
    } else {
      const orderByArray = columns;
      if (this.config.setOperators.length > 0) {
        this.config.setOperators.at(-1).orderBy = orderByArray;
      } else {
        this.config.orderBy = orderByArray;
      }
    }
    return this;
  }

  limit(limit: any) {
    if (this.config.setOperators.length > 0) {
      this.config.setOperators.at(-1).limit = limit;
    } else {
      this.config.limit = limit;
    }
    return this;
  }

  offset(offset: any) {
    if (this.config.setOperators.length > 0) {
      this.config.setOperators.at(-1).offset = offset;
    } else {
      this.config.offset = offset;
    }
    return this;
  }

  /** @internal */
  getSQL() {
    return this.dialect.buildSelectQuery(this.config);
  }

  toSQL() {
    const { typings: _typings, ...rest } = this.dialect.sqlToQuery(this.getSQL());
    return rest;
  }

  as(alias: string) {
    return new Proxy(
      new Subquery(this.getSQL(), this.config.fields, alias),
      new SelectionProxyHandler({ alias, sqlAliasedBehavior: 'alias', sqlBehavior: 'error' })
    );
  }

  /** @internal */
  getSelectedFields() {
    return new Proxy(
      this.config.fields,
      new SelectionProxyHandler({ alias: this.tableName, sqlAliasedBehavior: 'alias', sqlBehavior: 'error' })
    );
  }

  $dynamic() {
    return this;
  }
}

export class DatabendSelectBase extends DatabendSelectQueryBuilderBase {
  static override readonly [entityKind]: string = 'DatabendSelect';

  /** @internal */
  _prepare(name?: string) {
    const { session, config, dialect, joinsNotNullableMap } = this;
    if (!session) {
      throw new Error('Cannot execute a query on a query builder. Please use a database instance instead.');
    }
    return tracer.startActiveSpan('drizzle.prepareQuery', () => {
      const fieldsList = orderSelectedFields(config.fields);
      const query = session.prepareQuery(dialect.sqlToQuery(this.getSQL()), fieldsList, name, true);
      query.joinsNotNullableMap = joinsNotNullableMap;
      return query;
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
}

applyMixins(DatabendSelectBase, [QueryPromise]);

function createSetOperator(type: string, isAll: boolean) {
  return (leftSelect: any, rightSelect: any, ...restSelects: any[]) => {
    const setOperators = [rightSelect, ...restSelects].map((select) => ({
      type,
      isAll,
      rightSelect: select,
    }));
    for (const setOperator of setOperators) {
      if (!haveSameKeys(leftSelect.getSelectedFields(), setOperator.rightSelect.getSelectedFields())) {
        throw new Error(
          'Set operator error (union / intersect / except): selected fields are not the same or are in a different order'
        );
      }
    }
    return leftSelect.addSetOperators(setOperators);
  };
}

const getDatabendSetOperators = () => ({
  union,
  unionAll,
  intersect,
  intersectAll,
  except,
  exceptAll,
});

export const union = createSetOperator('union', false);
export const unionAll = createSetOperator('union', true);
export const intersect = createSetOperator('intersect', false);
export const intersectAll = createSetOperator('intersect', true);
export const except = createSetOperator('except', false);
export const exceptAll = createSetOperator('except', true);
