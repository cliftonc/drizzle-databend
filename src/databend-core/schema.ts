import { entityKind, is } from 'drizzle-orm/entity';
import { SQL, sql } from 'drizzle-orm/sql/sql';
import { databendTableWithSchema } from './table.ts';

export class DatabendSchema {
  static readonly [entityKind]: string = 'DatabendSchema';

  constructor(public schemaName: string) {}

  table = (name: string, columns: any, extraConfig?: any): any => {
    return databendTableWithSchema(name, columns, extraConfig, this.schemaName);
  };

  getSQL(): SQL {
    return new SQL([sql.identifier(this.schemaName)]);
  }

  shouldOmitSQLParens() {
    return true;
  }
}

export function isDatabendSchema(obj: unknown): obj is DatabendSchema {
  return is(obj, DatabendSchema);
}

export function databendSchema(name: string) {
  return new DatabendSchema(name);
}
