import { entityKind } from 'drizzle-orm/entity';
import { DatabendColumn, DatabendColumnBuilder } from './common.ts';

export class DatabendTextBuilder extends DatabendColumnBuilder {
  static readonly [entityKind]: string = 'DatabendTextBuilder';

  constructor(name: string) {
    super(name, 'string', 'DatabendText');
  }

  /** @internal */
  build(table: any): DatabendText {
    return new DatabendText(table, this.config);
  }
}

export class DatabendText extends DatabendColumn {
  static readonly [entityKind]: string = 'DatabendText';

  getSQLType(): string {
    return 'text';
  }
}

export function text(name?: string) {
  return new DatabendTextBuilder(name ?? '');
}
