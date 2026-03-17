import { entityKind } from 'drizzle-orm/entity';
import { TransactionRollbackError } from 'drizzle-orm/errors';
// @ts-expect-error - tracer is exported at runtime but not in .d.ts
import { tracer } from 'drizzle-orm/tracing';
import { DatabendDatabase } from './db.ts';

export class DatabendPreparedQuery {
  static readonly [entityKind]: string = 'DatabendPreparedQuery';

  /** @internal */
  joinsNotNullableMap: Record<string, boolean> | undefined;

  constructor(public query: any) {}

  getQuery() {
    return this.query;
  }

  mapResult(response: any, _isFromBatch?: boolean) {
    return response;
  }
}

export class DatabendSession {
  static readonly [entityKind]: string = 'DatabendSession';

  constructor(public dialect: any) {}

  execute(query: any): any {
    return tracer.startActiveSpan('drizzle.operation', () => {
      const prepared = tracer.startActiveSpan('drizzle.prepareQuery', () => {
        return this.prepareQuery(
          this.dialect.sqlToQuery(query),
          undefined,
          undefined,
          false
        );
      });
      return prepared.execute(undefined);
    });
  }

  all(query: any): any {
    return this.prepareQuery(
      this.dialect.sqlToQuery(query),
      undefined,
      undefined,
      false
    ).all();
  }

  async count(sql: any): Promise<number> {
    const res = await this.execute(sql);
    return Number(res[0]['count']);
  }

  prepareQuery(
    _query: any,
    _fields: any,
    _name: any,
    _isResponseInArrayMode: boolean,
    _customResultMapper?: any
  ): any {
    throw new Error('prepareQuery not implemented');
  }

  transaction(_transaction: any, _config?: any): Promise<any> {
    throw new Error('transaction not implemented');
  }
}

export class DatabendTransaction extends DatabendDatabase {
  static override readonly [entityKind]: string = 'DatabendTransaction';

  declare readonly schema: any;

  constructor(dialect: any, session: any, schema: any, public nestedIndex = 0) {
    super(dialect, session, schema);
    this.schema = schema;
  }

  rollback(): never {
    throw new TransactionRollbackError();
  }
}
