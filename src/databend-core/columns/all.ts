import { bigint } from './bigint.ts';
import { binary } from './binary.ts';
import { bitmap } from './bitmap.ts';
import { boolean } from './boolean.ts';
import { customType } from './custom.ts';
import { date } from './date.ts';
import { decimal } from './decimal.ts';
import { doublePrecision } from './double.ts';
import { float, real } from './float.ts';
import { integer } from './integer.ts';
import { smallint } from './smallint.ts';
import { text } from './text.ts';
import { timestamp } from './timestamp.ts';
import { tinyint } from './tinyint.ts';
import { varchar } from './varchar.ts';
import { variant } from './variant.ts';

export function getDatabendColumnBuilders() {
  return {
    bigint,
    binary,
    bitmap,
    boolean,
    customType,
    date,
    decimal,
    doublePrecision,
    float,
    integer,
    real,
    smallint,
    text,
    timestamp,
    tinyint,
    varchar,
    variant,
  };
}
