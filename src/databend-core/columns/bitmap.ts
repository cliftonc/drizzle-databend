import { entityKind } from 'drizzle-orm/entity';
import { DatabendColumn, DatabendColumnBuilder } from './common.ts';

export class DatabendBitmapBuilder extends DatabendColumnBuilder {
  static readonly [entityKind]: string = 'DatabendBitmapBuilder';

  constructor(name: string) {
    super(name, 'string', 'DatabendBitmap');
  }

  /** @internal */
  build(table: any): DatabendBitmap {
    return new DatabendBitmap(table, this.config);
  }
}

export class DatabendBitmap extends DatabendColumn {
  static readonly [entityKind]: string = 'DatabendBitmap';

  getSQLType(): string {
    return 'bitmap';
  }
}

export function bitmap(name?: string) {
  return new DatabendBitmapBuilder(name ?? '');
}
