import { entityKind } from 'drizzle-orm/entity';
import { Table } from 'drizzle-orm/table';
import { getDatabendColumnBuilders } from './columns/all.ts';

export class DatabendTable extends Table {
  static readonly [entityKind]: string = 'DatabendTable';

  /** @internal */
  static Symbol = Object.assign({}, (Table as any).Symbol, {});

  constructor(name: string, schema: string | undefined, baseName: string) {
    super(name, schema, baseName);
    (this as any)[(Table as any).Symbol.ExtraConfigBuilder] = undefined;
    (this as any)[(Table as any).Symbol.ExtraConfigColumns] = {};
  }
}

function databendTableWithSchema(
  name: string,
  columns: any,
  extraConfig: any,
  schema: string | undefined,
  baseName = name
): any {
  const rawTable = new DatabendTable(name, schema, baseName);
  const parsedColumns = typeof columns === 'function' ? columns(getDatabendColumnBuilders()) : columns;

  const builtColumns = Object.fromEntries(
    Object.entries(parsedColumns).map(([name, colBuilderBase]: [string, any]) => {
      const colBuilder = colBuilderBase;
      colBuilder.setName(name);
      const column = colBuilder.build(rawTable);
      return [name, column];
    })
  );

  const builtColumnsForExtraConfig = Object.fromEntries(
    Object.entries(parsedColumns).map(([name, colBuilderBase]: [string, any]) => {
      const colBuilder = colBuilderBase;
      colBuilder.setName(name);
      const column = colBuilder.buildExtraConfigColumn(rawTable);
      return [name, column];
    })
  );

  const table = Object.assign(rawTable, builtColumns);
  table[(Table as any).Symbol.Columns] = builtColumns;
  table[(Table as any).Symbol.ExtraConfigColumns] = builtColumnsForExtraConfig;

  if (extraConfig) {
    table[(DatabendTable as any).Symbol.ExtraConfigBuilder] = extraConfig;
  }

  return table;
}

export const databendTable = (name: string, columns: any, extraConfig?: any): any => {
  return databendTableWithSchema(name, columns, extraConfig, undefined);
};

export function databendTableCreator(customizeTableName: (name: string) => string) {
  return (name: string, columns: any, extraConfig?: any) => {
    return databendTableWithSchema(customizeTableName(name), columns, extraConfig, undefined, name);
  };
}

export { databendTableWithSchema };
