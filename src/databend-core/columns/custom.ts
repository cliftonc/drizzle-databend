import { entityKind } from 'drizzle-orm/entity';
import { DatabendColumn, DatabendColumnBuilder } from './common.ts';

export interface CustomTypeValues {
  data: unknown;
  driverData?: unknown;
  notNull: boolean;
  default: boolean;
}

export interface CustomTypeParams<T extends CustomTypeValues> {
  dataType: (config: any) => string;
  toDriver?: (value: T['data']) => T['driverData'] | undefined;
  fromDriver?: (value: T['driverData']) => T['data'];
}

export class DatabendCustomColumnBuilder<T extends CustomTypeValues> extends DatabendColumnBuilder {
  static readonly [entityKind]: string = 'DatabendCustomColumnBuilder';

  private sqlDataType: string;
  private mapTo?: (value: T['data']) => T['driverData'];
  private mapFrom?: (value: T['driverData']) => T['data'];

  constructor(
    name: string,
    fieldConfig: any,
    customTypeParams: CustomTypeParams<T>
  ) {
    super(name, 'custom', 'DatabendCustomColumn');
    this.sqlDataType = customTypeParams.dataType(fieldConfig);
    this.mapTo = customTypeParams.toDriver;
    this.mapFrom = customTypeParams.fromDriver;
  }

  /** @internal */
  build(table: any): DatabendCustomColumn<T> {
    return new DatabendCustomColumn<T>(
      table,
      this.config,
      this.sqlDataType,
      this.mapTo,
      this.mapFrom
    );
  }
}

export class DatabendCustomColumn<T extends CustomTypeValues> extends DatabendColumn {
  static readonly [entityKind]: string = 'DatabendCustomColumn';

  private sqlDataType: string;
  private mapTo?: (value: T['data']) => T['driverData'];
  private mapFrom?: (value: T['driverData']) => T['data'];

  constructor(
    table: any,
    config: any,
    sqlDataType: string,
    mapTo?: (value: T['data']) => T['driverData'],
    mapFrom?: (value: T['driverData']) => T['data']
  ) {
    super(table, config);
    this.sqlDataType = sqlDataType;
    this.mapTo = mapTo;
    this.mapFrom = mapFrom;
  }

  getSQLType(): string {
    return this.sqlDataType;
  }

  override mapFromDriverValue(value: any): T['data'] {
    return this.mapFrom ? this.mapFrom(value) : value;
  }

  override mapToDriverValue(value: T['data']): T['driverData'] {
    return this.mapTo ? this.mapTo(value) : value;
  }
}

export function customType<T extends CustomTypeValues>(
  customTypeParams: CustomTypeParams<T>
): (dbName: string, fieldConfig?: any) => DatabendCustomColumnBuilder<T> {
  return (dbName: string, fieldConfig?: any) => {
    return new DatabendCustomColumnBuilder(dbName, fieldConfig, customTypeParams);
  };
}
