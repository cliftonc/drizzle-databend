import { entityKind, is } from 'drizzle-orm/entity';
import { SelectionProxyHandler } from 'drizzle-orm/selection-proxy';
import { WithSubquery } from 'drizzle-orm/subquery';
import { DatabendDialect } from '../dialect.ts';
import { DatabendSelectBuilder } from './select.ts';

export class QueryBuilder {
  static readonly [entityKind]: string = 'DatabendQueryBuilder';

  dialect: any;
  dialectConfig: any;

  constructor(dialect?: any) {
    this.dialect = is(dialect, DatabendDialect) ? dialect : undefined;
    this.dialectConfig = is(dialect, DatabendDialect) ? undefined : dialect;
  }

  $with(alias: string) {
    const queryBuilder = this;
    return {
      as(qb: any) {
        if (typeof qb === 'function') {
          qb = qb(queryBuilder);
        }
        return new Proxy(
          new WithSubquery(qb.getSQL(), qb.getSelectedFields(), alias, true),
          new SelectionProxyHandler({ alias, sqlAliasedBehavior: 'alias', sqlBehavior: 'error' })
        );
      },
    };
  }

  with(...queries: any[]) {
    const self = this;
    function select(fields?: any) {
      return new DatabendSelectBuilder({
        fields: fields ?? undefined,
        session: undefined,
        dialect: self.getDialect(),
        withList: queries,
      });
    }
    function selectDistinct(fields?: any) {
      return new DatabendSelectBuilder({
        fields: fields ?? undefined,
        session: undefined,
        dialect: self.getDialect(),
        distinct: true,
      });
    }
    return { select, selectDistinct };
  }

  select(fields?: any) {
    return new DatabendSelectBuilder({
      fields: fields ?? undefined,
      session: undefined,
      dialect: this.getDialect(),
    });
  }

  selectDistinct(fields?: any) {
    return new DatabendSelectBuilder({
      fields: fields ?? undefined,
      session: undefined,
      dialect: this.getDialect(),
      distinct: true,
    });
  }

  private getDialect() {
    if (!this.dialect) {
      this.dialect = new DatabendDialect(this.dialectConfig);
    }
    return this.dialect;
  }
}
