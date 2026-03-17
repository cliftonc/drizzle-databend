import { entityKind } from 'drizzle-orm/entity';
import { QueryPromise } from 'drizzle-orm/query-promise';

export class DatabendRaw extends QueryPromise<any> {
  static readonly [entityKind]: string = 'DatabendRaw';

  constructor(
    public execute: () => Promise<any>,
    public sql: any,
    public query: any,
    public mapBatchResult: (result: any) => any
  ) {
    super();
  }

  /** @internal */
  getSQL() {
    return this.sql;
  }

  getQuery() {
    return this.query;
  }

  mapResult(result: any, isFromBatch: boolean) {
    return isFromBatch ? this.mapBatchResult(result) : result;
  }

  _prepare() {
    return this;
  }

  /** @internal */
  isResponseInArrayMode() {
    return false;
  }
}
