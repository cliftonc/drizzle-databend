import { describe, expect, it } from 'vitest';
import { prepareParams } from '../src/client.ts';

describe('prepareParams', () => {
  describe('string passthrough (no client-side escaping)', () => {
    // As of databend-driver 0.34.0 the driver owns escaping (server-side binding, or its
    // own client-side escaping against older servers). prepareParams must NOT pre-escape,
    // otherwise values are double-escaped.
    it('should pass single quotes through unchanged', () => {
      expect(prepareParams(["O'Brien"])).toEqual(["O'Brien"]);
    });

    it('should pass multiple single quotes through unchanged', () => {
      expect(prepareParams(["it's a 'test'"])).toEqual(["it's a 'test'"]);
    });

    it('should pass SQL injection attempts through verbatim (driver binds them safely)', () => {
      const result = prepareParams(["'; DROP TABLE users; --"]);
      expect(result).toEqual(["'; DROP TABLE users; --"]);
    });

    it('should pass strings without quotes unchanged', () => {
      expect(prepareParams(['hello'])).toEqual(['hello']);
    });

    it('should handle empty string', () => {
      expect(prepareParams([''])).toEqual(['']);
    });
  });

  describe('date handling', () => {
    it('should convert Date to ISO string', () => {
      const date = new Date('2024-01-15T10:00:00Z');
      const result = prepareParams([date]);
      expect(result[0]).toBe('2024-01-15T10:00:00.000Z');
    });

    it('should produce a plain ISO string for the driver to bind', () => {
      const result = prepareParams([new Date('2024-01-15T10:00:00Z')]);
      expect(typeof result[0]).toBe('string');
    });
  });

  describe('bigint handling', () => {
    it('should convert bigint to string', () => {
      expect(prepareParams([BigInt(42)])).toEqual(['42']);
    });

    it('should handle large bigint values', () => {
      expect(prepareParams([BigInt('9007199254740993')])).toEqual(['9007199254740993']);
    });
  });

  describe('object/array handling', () => {
    it('should JSON-stringify objects', () => {
      const result = prepareParams([{ key: 'value' }]);
      expect(result[0]).toBe('{"key":"value"}');
    });

    it('should JSON-stringify arrays', () => {
      const result = prepareParams([[1, 2, 3]]);
      expect(result[0]).toBe('[1,2,3]');
    });

    it('should JSON-stringify objects with quotes without extra escaping', () => {
      const result = prepareParams([{ note: "it's a test" }]);
      expect(result[0]).toBe('{"note":"it\'s a test"}');
    });

    it('should handle nested objects with quotes', () => {
      const result = prepareParams([{ tags: ["won't break"], name: "O'Brien" }]);
      expect(result[0]).toBe('{"tags":["won\'t break"],"name":"O\'Brien"}');
    });
  });

  describe('null/undefined handling', () => {
    it('should convert undefined to null', () => {
      expect(prepareParams([undefined])).toEqual([null]);
    });

    it('should pass null through', () => {
      expect(prepareParams([null])).toEqual([null]);
    });
  });

  describe('number/boolean passthrough', () => {
    it('should pass numbers through unchanged', () => {
      expect(prepareParams([42, 3.14, 0, -1])).toEqual([42, 3.14, 0, -1]);
    });

    it('should pass booleans through unchanged', () => {
      expect(prepareParams([true, false])).toEqual([true, false]);
    });
  });

  describe('type coercion with typings', () => {
    it('should convert numeric string to number when typing is decimal', () => {
      const result = prepareParams(['5'], ['decimal']);
      expect(result[0]).toBe(5);
    });

    it('should convert decimal string to number when typing is decimal', () => {
      const result = prepareParams(['3.14'], ['decimal']);
      expect(result[0]).toBe(3.14);
    });

    it('should convert negative numeric string to number', () => {
      const result = prepareParams(['-42'], ['decimal']);
      expect(result[0]).toBe(-42);
    });

    it('should convert scientific notation string to number', () => {
      const result = prepareParams(['1.5e3'], ['decimal']);
      expect(result[0]).toBe(1500);
    });

    it('should NOT convert non-numeric string even with decimal typing', () => {
      const result = prepareParams(['hello'], ['decimal']);
      expect(result[0]).toBe('hello');
    });

    it('should NOT convert string with spaces even with decimal typing', () => {
      const result = prepareParams([' 5 '], ['decimal']);
      expect(result[0]).toBe(' 5 ');
    });

    it('should pass quoted strings through unchanged when typing is not decimal', () => {
      const result = prepareParams(["O'Brien"], ['none']);
      expect(result[0]).toBe("O'Brien");
    });

    it('should handle mixed typings', () => {
      const result = prepareParams(['5', "O'Brien", '3.14'], ['decimal', 'none', 'decimal']);
      expect(result).toEqual([5, "O'Brien", 3.14]);
    });

    it('should handle missing typings gracefully', () => {
      const result = prepareParams(['5', 'hello'], ['decimal']);
      expect(result[0]).toBe(5);
      expect(result[1]).toBe('hello'); // no typing for index 1, treated as string
    });
  });
});
