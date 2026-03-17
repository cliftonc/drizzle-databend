import { TableAliasProxyHandler } from 'drizzle-orm/alias';

export function alias(table: any, alias: string): any {
  return new Proxy(table, new TableAliasProxyHandler(alias, false));
}
