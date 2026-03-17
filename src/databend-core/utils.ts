import { is } from 'drizzle-orm/entity';
import { Table } from 'drizzle-orm/table';
import { IndexBuilder } from './indexes.ts';
import { PrimaryKeyBuilder } from './primary-keys.ts';
import { DatabendTable } from './table.ts';

export function getTableConfig(table: any) {
  const columns = Object.values(table[(Table as any).Symbol.Columns]);
  const indexes: any[] = [];
  const primaryKeys: any[] = [];
  const name = table[(Table as any).Symbol.Name];
  const schema = table[(Table as any).Symbol.Schema];

  const extraConfigBuilder = table[(DatabendTable as any).Symbol.ExtraConfigBuilder];
  if (extraConfigBuilder !== undefined) {
    const extraConfig = extraConfigBuilder(table[(Table as any).Symbol.ExtraConfigColumns]);
    const extraValues = Array.isArray(extraConfig) ? extraConfig.flat(1) : Object.values(extraConfig);
    for (const builder of extraValues) {
      if (is(builder, IndexBuilder)) {
        indexes.push(builder.build(table));
      } else if (is(builder, PrimaryKeyBuilder)) {
        primaryKeys.push(builder.build(table));
      }
    }
  }

  return {
    columns,
    indexes,
    primaryKeys,
    name,
    schema,
  };
}
