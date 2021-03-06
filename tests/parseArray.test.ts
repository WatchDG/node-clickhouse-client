import { parseArray } from "../src/streams/tsv";

describe('parseArray', function () {
    it('empty', function () {
        const arrayString = '[]';
        const arrayBuffer = Buffer.from(arrayString);
        const array = parseArray(arrayBuffer, 'Int8');

        expect(array).toBeInstanceOf(Array);
        expect(array.length).toBe(0);
    });
    it('Int8', function () {
        const arrayString = '[1,2,3]';
        const arrayBuffer = Buffer.from(arrayString);
        const array = parseArray(arrayBuffer, 'Int8');

        expect(array).toBeInstanceOf(Array);
        expect(array.length).toBe(3);
        expect(array).toStrictEqual(expect.arrayContaining([1, 2, 3]));
    });
    it('Int16', function () {
        const arrayString = '[1,2,3]';
        const arrayBuffer = Buffer.from(arrayString);
        const array = parseArray(arrayBuffer, 'Int16');

        expect(array).toBeInstanceOf(Array);
        expect(array.length).toBe(3);
        expect(array).toStrictEqual(expect.arrayContaining([1, 2, 3]));
    });
    it('Int32', function () {
        const arrayString = '[1,2,3]';
        const arrayBuffer = Buffer.from(arrayString);
        const array = parseArray(arrayBuffer, 'Int32');

        expect(array).toBeInstanceOf(Array);
        expect(array.length).toBe(3);
        expect(array).toStrictEqual(expect.arrayContaining([1, 2, 3]));
    });
    it('Int64', function () {
        const arrayString = '[1,2,3]';
        const arrayBuffer = Buffer.from(arrayString);
        const array = parseArray(arrayBuffer, 'Int64');

        expect(array).toBeInstanceOf(Array);
        expect(array.length).toBe(3);
        expect(array).toStrictEqual(expect.arrayContaining([1n, 2n, 3n]));
    });
    it('Int128', function () {
        const arrayString = '[1,2,3]';
        const arrayBuffer = Buffer.from(arrayString);
        const array = parseArray(arrayBuffer, 'Int128');

        expect(array).toBeInstanceOf(Array);
        expect(array.length).toBe(3);
        expect(array).toStrictEqual(expect.arrayContaining([1n, 2n, 3n]));
    });
    it('Int256', function () {
        const arrayString = '[1,2,3]';
        const arrayBuffer = Buffer.from(arrayString);
        const array = parseArray(arrayBuffer, 'Int256');

        expect(array).toBeInstanceOf(Array);
        expect(array.length).toBe(3);
        expect(array).toStrictEqual(expect.arrayContaining([1n, 2n, 3n]));
    });
    it('UInt8', function () {
        const arrayString = '[1,2,3]';
        const arrayBuffer = Buffer.from(arrayString);
        const array = parseArray(arrayBuffer, 'UInt8');

        expect(array).toBeInstanceOf(Array);
        expect(array.length).toBe(3);
        expect(array).toStrictEqual(expect.arrayContaining([1, 2, 3]));
    });
    it('UInt16', function () {
        const arrayString = '[1,2,3]';
        const arrayBuffer = Buffer.from(arrayString);
        const array = parseArray(arrayBuffer, 'UInt16');

        expect(array).toBeInstanceOf(Array);
        expect(array.length).toBe(3);
        expect(array).toStrictEqual(expect.arrayContaining([1, 2, 3]));
    });
    it('UInt32', function () {
        const arrayString = '[1,2,3]';
        const arrayBuffer = Buffer.from(arrayString);
        const array = parseArray(arrayBuffer, 'UInt32');

        expect(array).toBeInstanceOf(Array);
        expect(array.length).toBe(3);
        expect(array).toStrictEqual(expect.arrayContaining([1, 2, 3]));
    });
    it('UInt64', function () {
        const arrayString = '[1,2,3]';
        const arrayBuffer = Buffer.from(arrayString);
        const array = parseArray(arrayBuffer, 'UInt64');

        expect(array).toBeInstanceOf(Array);
        expect(array.length).toBe(3);
        expect(array).toStrictEqual(expect.arrayContaining([1n, 2n, 3n]));
    });
    it('UInt128', function () {
        const arrayString = '[1,2,3]';
        const arrayBuffer = Buffer.from(arrayString);
        const array = parseArray(arrayBuffer, 'UInt128');

        expect(array).toBeInstanceOf(Array);
        expect(array.length).toBe(3);
        expect(array).toStrictEqual(expect.arrayContaining([1n, 2n, 3n]));
    });
    it('UInt256', function () {
        const arrayString = '[1,2,3]';
        const arrayBuffer = Buffer.from(arrayString);
        const array = parseArray(arrayBuffer, 'UInt256');

        expect(array).toBeInstanceOf(Array);
        expect(array.length).toBe(3);
        expect(array).toStrictEqual(expect.arrayContaining([1n, 2n, 3n]));
    });
    it('Float32', function () {
        const arrayString = '[1.2,2.3,3.4]';
        const arrayBuffer = Buffer.from(arrayString);
        const array = parseArray(arrayBuffer, 'Float32');

        expect(array).toBeInstanceOf(Array);
        expect(array.length).toBe(3);
        expect(array).toStrictEqual(expect.arrayContaining([1.2, 2.3, 3.4]));
    });
    it('Float64', function () {
        const arrayString = '[1.2,2.3,3.4]';
        const arrayBuffer = Buffer.from(arrayString);
        const array = parseArray(arrayBuffer, 'Float32');

        expect(array).toBeInstanceOf(Array);
        expect(array.length).toBe(3);
        expect(array).toStrictEqual(expect.arrayContaining([1.2, 2.3, 3.4]));
    });
});