import { entityKind } from 'drizzle-orm/entity';
import { SQL, sql } from 'drizzle-orm/sql/sql';

export class DatabendCountBuilder extends SQL {
  static override readonly [entityKind]: string = 'DatabendCountBuilder';
  readonly [Symbol.toStringTag] = 'DatabendCountBuilder';

  private sql: SQL;
  private session: any;

  constructor(params: { source: any; filters?: any; session: any }) {
    super(DatabendCountBuilder.buildEmbeddedCount(params.source, params.filters).queryChunks);
    this.mapWith(Number);
    this.session = params.session;
    this.sql = DatabendCountBuilder.buildCount(params.source, params.filters);
  }

  private static buildEmbeddedCount(source: any, filters?: any) {
    return sql`(select count(*) from ${source}${sql.raw(' where ').if(filters)}${filters})`;
  }

  private static buildCount(source: any, filters?: any) {
    return sql`select count(*) as count from ${source}${sql.raw(' where ').if(filters)}${filters};`;
  }

  // biome-ignore lint/suspicious/noThenProperty: Promise-like interface required for await support
  then(onfulfilled?: any, onrejected?: any) {
    return Promise.resolve(this.session.count(this.sql)).then(onfulfilled, onrejected);
  }

  catch(onRejected?: any) {
    return this.then(undefined, onRejected);
  }

  finally(onFinally?: any) {
    return this.then(
      (value: any) => {
        onFinally?.();
        return value;
      },
      (reason: any) => {
        onFinally?.();
        throw reason;
      }
    );
  }
}
