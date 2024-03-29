import { ClickhouseClient } from "../src";
import { Blob } from "buffer";
import { Readable } from "stream";
import { createBrotliCompress, createDeflate, createGzip } from "zlib";

describe('clickhouse client', function () {
    const DATABASE = "test";

    let clickhouseClient: ClickhouseClient;

    beforeAll(async function () {
        clickhouseClient = new ClickhouseClient({
            user: 'new_user',
            password: 'new_password'
        });
        await clickhouseClient.query(`
            CREATE DATABASE IF NOT EXISTS "${DATABASE}";
        `);
        await clickhouseClient.query(`
            DROP TABLE IF EXISTS "${DATABASE}"."test_insert";
        `);
        await clickhouseClient.query(`
            CREATE TABLE "${DATABASE}"."test_insert"
            (
                time  DateTime DEFAULT now(),
                key   String,
                value Int32
            ) ENGINE MergeTree() ORDER BY time;
        `);
    });

    afterAll(async function () {
        await clickhouseClient.close();
    });

    beforeEach(async function () {
        await clickhouseClient.query(`
            TRUNCATE TABLE IF EXISTS "${DATABASE}"."test_insert";
        `);
    });

    it('init', function () {
        expect(clickhouseClient).toBeInstanceOf(ClickhouseClient);
    });

    it('ping', async function () {
        const result = await clickhouseClient.ping();
        expect(result).toBe(true);
    });

    describe('query', function () {

        describe('SELECT', function () {
            describe('JSON', function () {
                it('select', async function () {
                    const result = await clickhouseClient.query(`
                SELECT  1           AS num,
                        '2'         AS str,
                        [3, 4]      AS numArr,
                        ['5', '6']  AS strArr
                FORMAT JSON
                `);
                    expect(result).toHaveProperty('data');
                    expect(result.rows).toBe(1);
                    expect(result.data).toBeInstanceOf(Array);
                    expect(result.data).toEqual(expect.arrayContaining([{
                        num: 1,
                        str: '2',
                        numArr: [3, 4],
                        strArr: ['5', '6']
                    }]));
                });
                it('UUID', async function () {
                    const result = await clickhouseClient.query(`
                        SELECT toUUIDOrZero('') as value
                        FORMAT JSON
                    `);
                    expect(result).toHaveProperty('data');
                    expect(result.rows).toBe(1);
                    expect(result.data).toBeInstanceOf(Array);
                    expect(result.data).toEqual(expect.arrayContaining([{
                        value: '00000000-0000-0000-0000-000000000000'
                    }]));
                });
            });

            describe('TabSeparated', function () {
                const format = 'TabSeparated';
                it('select', async function () {
                    const result = await clickhouseClient.query(`
                SELECT  1,
                        '2',
                        [3, 4],
                        ['5','6'],
                        ['7\\'8',',]9']
                FORMAT TabSeparated
                `);
                    expect(result).toHaveProperty('data');
                    expect(result.rows).toBe(1);
                    expect(result.data).toBeInstanceOf(Array);
                    expect(result.data).toEqual(expect.arrayContaining([['1', '2', '[3,4]', `['5','6']`, `['7'8',',]9']`]]));
                });
                it('UUID', async function () {
                    const result = await clickhouseClient.query(`
                        SELECT toUUIDOrZero('') as value
                        FORMAT ${format}
                    `);
                    expect(result).toHaveProperty('data');
                    expect(result.rows).toBe(1);
                    expect(result.data).toBeInstanceOf(Array);
                    expect(result.data).toEqual(expect.arrayContaining([['00000000-0000-0000-0000-000000000000']]));
                });
            });

            describe('TabSeparatedRaw', function () {
                const format = 'TabSeparatedRaw';
                it('select', async function () {
                    const result = await clickhouseClient.query(`
                SELECT  1,
                        '2',
                        [3, 4],
                        ['5','6']
                FORMAT TabSeparatedRaw
                `);
                    expect(result).toHaveProperty('data');
                    expect(result.rows).toBe(1);
                    expect(result.data).toBeInstanceOf(Array);
                    expect(result.data).toEqual(expect.arrayContaining([['1', '2', '[3,4]', `['5','6']`]]));
                });
                it('UUID', async function () {
                    const result = await clickhouseClient.query(`
                        SELECT toUUIDOrZero('') as value
                        FORMAT ${format}
                    `);
                    expect(result).toHaveProperty('data');
                    expect(result.rows).toBe(1);
                    expect(result.data).toBeInstanceOf(Array);
                    expect(result.data).toEqual(expect.arrayContaining([['00000000-0000-0000-0000-000000000000']]));
                });
            });

            describe('TabSeparatedWithNames', function () {
                const format = 'TabSeparatedWithNames';
                describe('select', function () {
                    it('Int8', async function () {
                        const result = await clickhouseClient.query(`
                    SELECT toInt8(1) as value
                    FORMAT TabSeparatedWithNames
                    `);
                        expect(result.meta).toEqual(expect.arrayContaining([{ name: 'value' }]));
                        expect(result.rows).toBe(1);
                        expect(result).toHaveProperty('data');
                        expect(result.data).toBeInstanceOf(Array);
                        expect(result.data).toEqual(expect.arrayContaining([{ value: '1' }]));
                    });
                    it('Int64', async function () {
                        const result = await clickhouseClient.query(`
                    SELECT toInt64(1) as value
                    FORMAT TabSeparatedWithNames
                    `);
                        expect(result).toHaveProperty('data');
                        expect(result.meta).toEqual(expect.arrayContaining([{ name: 'value' }]));
                        expect(result.rows).toBe(1);
                        expect(result.data).toBeInstanceOf(Array);
                        expect(result.data).toEqual(expect.arrayContaining([{ value: '1' }]));
                    });
                    describe('Float64', function () {
                        it('NaN', async function () {
                            const result = await clickhouseClient.query(`
                        SELECT toFloat64(nan) AS value
                        FORMAT TabSeparatedWithNames`
                            );
                            expect(result).toHaveProperty('meta');
                            expect(result.meta).toBeInstanceOf(Array);
                            expect(result.meta).toEqual(expect.arrayContaining([{ name: 'value' }]));
                            expect(result).toHaveProperty('rows');
                            expect(result.rows).toBe(1);
                            expect(result).toHaveProperty('data');
                            expect(result.data).toBeInstanceOf(Array);
                            expect(result.data).toEqual(expect.arrayContaining([{ value: 'nan' }]));
                        });
                        it('positive Infinity', async function () {
                            const result = await clickhouseClient.query(`
                        SELECT inf AS value
                        FORMAT TabSeparatedWithNames`
                            );
                            expect(result).toHaveProperty('meta');
                            expect(result.meta).toBeInstanceOf(Array);
                            expect(result.meta).toEqual(expect.arrayContaining([{ name: 'value' }]));
                            expect(result).toHaveProperty('rows');
                            expect(result.rows).toBe(1);
                            expect(result).toHaveProperty('data');
                            expect(result.data).toBeInstanceOf(Array);
                            expect(result.data).toEqual(expect.arrayContaining([{ value: 'inf' }]));
                        });
                        it('negative Infinity', async function () {
                            const result = await clickhouseClient.query(`
                        SELECT -inf AS value
                        FORMAT TabSeparatedWithNames`
                            );
                            expect(result).toHaveProperty('meta');
                            expect(result.meta).toBeInstanceOf(Array);
                            expect(result.meta).toEqual(expect.arrayContaining([{ name: 'value' }]));
                            expect(result).toHaveProperty('rows');
                            expect(result.rows).toBe(1);
                            expect(result).toHaveProperty('data');
                            expect(result.data).toBeInstanceOf(Array);
                            expect(result.data).toEqual(expect.arrayContaining([{ value: '-inf' }]));
                        });
                    });
                });
                it('select 100k rows', async function () {
                    const result = await clickhouseClient.query(`
                        SELECT number AS value
                        FROM numbers(100000)
                            LIMIT 100000
                            FORMAT
                            TabSeparatedWithNames;
                    `);
                    expect(result).toBeInstanceOf(Object);
                    expect(result).toHaveProperty('meta');
                    expect(result.meta).toEqual(expect.arrayContaining([{ name: 'value' }]));
                    expect(result).toHaveProperty('rows');
                    expect(result.rows).toBe(100000);
                    expect(result).toHaveProperty('data');
                    expect(result.data).toBeInstanceOf(Array);
                    expect(result.data.length).toBe(result.rows);
                });
                it('UUID', async function () {
                    const result = await clickhouseClient.query(`
                        SELECT toUUIDOrZero('') as value
                        FORMAT ${format}
                    `);
                    expect(result).toHaveProperty('data');
                    expect(result.rows).toBe(1);
                    expect(result.data).toBeInstanceOf(Array);
                    expect(result.data).toEqual(expect.arrayContaining([
                        { value: '00000000-0000-0000-0000-000000000000' }
                    ]));
                });
            });

            describe('TabSeparatedWithNamesAndTypes', function () {
                const format = 'TabSeparatedWithNamesAndTypes';
                describe('select', function () {
                    describe('Int8', function () {
                        it('positive', async function () {
                            const result = await clickhouseClient.query(`
                        SELECT toInt8(1) AS value
                        FORMAT TabSeparatedWithNamesAndTypes`
                            );
                            expect(result).toHaveProperty('data');
                            expect(result.meta).toBeInstanceOf(Array);
                            expect(result.meta).toEqual(expect.arrayContaining([{ name: 'value', type: 'Int8' }]));
                            expect(result.rows).toBe(1);
                            expect(result.data).toBeInstanceOf(Array);
                            expect(result.data).toEqual(expect.arrayContaining([{ value: 1 }]));
                            expect(result).toHaveProperty('meta');
                        });
                        it('negative', async function () {
                            const result = await clickhouseClient.query(`
                        SELECT toInt8(-1) AS value
                        FORMAT TabSeparatedWithNamesAndTypes`
                            );
                            expect(result).toHaveProperty('data');
                            expect(result.rows).toBe(1);
                            expect(result.data).toBeInstanceOf(Array);
                            expect(result.data).toEqual(expect.arrayContaining([{ value: -1 }]));
                            expect(result).toHaveProperty('meta');
                            expect(result.meta).toBeInstanceOf(Array);
                            expect(result.meta).toEqual(expect.arrayContaining([{ name: 'value', type: 'Int8' }]));
                        });
                    });
                    it('Int16', async function () {
                        const result = await clickhouseClient.query(`
                        SELECT toInt16(1) AS value
                        FORMAT TabSeparatedWithNamesAndTypes`
                        );
                        expect(result).toHaveProperty('data');
                        expect(result.rows).toBe(1);
                        expect(result.data).toBeInstanceOf(Array);
                        expect(result.data).toEqual(expect.arrayContaining([{ value: 1 }]));
                        expect(result).toHaveProperty('meta');
                        expect(result.meta).toBeInstanceOf(Array);
                        expect(result.meta).toEqual(expect.arrayContaining([{ name: 'value', type: 'Int16' }]));
                    });
                    it('Int32', async function () {
                        const result = await clickhouseClient.query(`
                        SELECT toInt32(1) AS value
                        FORMAT TabSeparatedWithNamesAndTypes`
                        );
                        expect(result).toHaveProperty('data');
                        expect(result.rows).toBe(1);
                        expect(result.data).toBeInstanceOf(Array);
                        expect(result.data).toEqual(expect.arrayContaining([{ value: 1 }]));
                        expect(result).toHaveProperty('meta');
                        expect(result.meta).toBeInstanceOf(Array);
                        expect(result.meta).toEqual(expect.arrayContaining([{ name: 'value', type: 'Int32' }]));
                    });
                    it('Int64', async function () {
                        const result = await clickhouseClient.query(`
                        SELECT toInt64(1) AS value
                        FORMAT TabSeparatedWithNamesAndTypes`
                        );
                        expect(result).toHaveProperty('data');
                        expect(result.rows).toBe(1);
                        expect(result.data).toBeInstanceOf(Array);
                        expect(result.data).toEqual(expect.arrayContaining([{ value: 1n }]));
                        expect(result).toHaveProperty('meta');
                        expect(result.meta).toBeInstanceOf(Array);
                        expect(result.meta).toEqual(expect.arrayContaining([{ name: 'value', type: 'Int64' }]));
                    });
                    it('Int128', async function () {
                        const result = await clickhouseClient.query(`
                        SELECT toInt128(1) AS value
                        FORMAT TabSeparatedWithNamesAndTypes`
                        );
                        expect(result).toHaveProperty('data');
                        expect(result.rows).toBe(1);
                        expect(result.data).toBeInstanceOf(Array);
                        expect(result.data).toEqual(expect.arrayContaining([{ value: 1n }]));
                        expect(result).toHaveProperty('meta');
                        expect(result.meta).toBeInstanceOf(Array);
                        expect(result.meta).toEqual(expect.arrayContaining([{ name: 'value', type: 'Int128' }]));
                    });
                    it('Int256', async function () {
                        const result = await clickhouseClient.query(`
                        SELECT toInt256(1) AS value
                        FORMAT TabSeparatedWithNamesAndTypes`
                        );
                        expect(result).toHaveProperty('data');
                        expect(result.rows).toBe(1);
                        expect(result.data).toBeInstanceOf(Array);
                        expect(result.data).toEqual(expect.arrayContaining([{ value: 1n }]));
                        expect(result).toHaveProperty('meta');
                        expect(result.meta).toBeInstanceOf(Array);
                        expect(result.meta).toEqual(expect.arrayContaining([{ name: 'value', type: 'Int256' }]));
                    });
                    it('UInt8', async function () {
                        const result = await clickhouseClient.query(`
                        SELECT toUInt8(1) AS value
                        FORMAT TabSeparatedWithNamesAndTypes`
                        );
                        expect(result).toHaveProperty('data');
                        expect(result.rows).toBe(1);
                        expect(result.data).toBeInstanceOf(Array);
                        expect(result.data).toEqual(expect.arrayContaining([{ value: 1 }]));
                        expect(result).toHaveProperty('meta');
                        expect(result.meta).toBeInstanceOf(Array);
                        expect(result.meta).toEqual(expect.arrayContaining([{ name: 'value', type: 'UInt8' }]));
                    });
                    it('UInt16', async function () {
                        const result = await clickhouseClient.query(`
                        SELECT toUInt16(1) AS value
                        FORMAT TabSeparatedWithNamesAndTypes`
                        );
                        expect(result).toHaveProperty('data');
                        expect(result.rows).toBe(1);
                        expect(result.data).toBeInstanceOf(Array);
                        expect(result.data).toEqual(expect.arrayContaining([{ value: 1 }]));
                        expect(result).toHaveProperty('meta');
                        expect(result.meta).toBeInstanceOf(Array);
                        expect(result.meta).toEqual(expect.arrayContaining([{ name: 'value', type: 'UInt16' }]));
                    });
                    it('UInt32', async function () {
                        const result = await clickhouseClient.query(`
                        SELECT toUInt32(1) AS value
                        FORMAT TabSeparatedWithNamesAndTypes`
                        );
                        expect(result).toHaveProperty('data');
                        expect(result.rows).toBe(1);
                        expect(result.data).toBeInstanceOf(Array);
                        expect(result.data).toEqual(expect.arrayContaining([{ value: 1 }]));
                        expect(result).toHaveProperty('meta');
                        expect(result.meta).toBeInstanceOf(Array);
                        expect(result.meta).toEqual(expect.arrayContaining([{ name: 'value', type: 'UInt32' }]));
                    });
                    it('UInt64', async function () {
                        const result = await clickhouseClient.query(`
                        SELECT toUInt64(1) AS value
                        FORMAT TabSeparatedWithNamesAndTypes`
                        );
                        expect(result).toHaveProperty('data');
                        expect(result.rows).toBe(1);
                        expect(result.data).toBeInstanceOf(Array);
                        expect(result.data).toEqual(expect.arrayContaining([{ value: 1n }]));
                        expect(result).toHaveProperty('meta');
                        expect(result.meta).toBeInstanceOf(Array);
                        expect(result.meta).toEqual(expect.arrayContaining([{ name: 'value', type: 'UInt64' }]));
                    });
                    it('UInt128', async function () {
                        const result = await clickhouseClient.query(`
                        SELECT toUInt128(1) AS value
                        FORMAT TabSeparatedWithNamesAndTypes`
                        );
                        expect(result).toHaveProperty('data');
                        expect(result.rows).toBe(1);
                        expect(result.data).toBeInstanceOf(Array);
                        expect(result.data).toEqual(expect.arrayContaining([{ value: 1n }]));
                        expect(result).toHaveProperty('meta');
                        expect(result.meta).toBeInstanceOf(Array);
                        expect(result.meta).toEqual(expect.arrayContaining([{ name: 'value', type: 'UInt128' }]));
                    });
                    it('UInt256', async function () {
                        const result = await clickhouseClient.query(`
                        SELECT toUInt256(1) AS value
                        FORMAT TabSeparatedWithNamesAndTypes`
                        );
                        expect(result).toHaveProperty('data');
                        expect(result.rows).toBe(1);
                        expect(result.data).toBeInstanceOf(Array);
                        expect(result.data).toEqual(expect.arrayContaining([{ value: 1n }]));
                        expect(result).toHaveProperty('meta');
                        expect(result.meta).toBeInstanceOf(Array);
                        expect(result.meta).toEqual(expect.arrayContaining([{ name: 'value', type: 'UInt256' }]));
                    });
                    it('Float32', async function () {
                        const result = await clickhouseClient.query(`
                        SELECT toFloat32(1.2) AS value
                        FORMAT TabSeparatedWithNamesAndTypes`
                        );
                        expect(result).toHaveProperty('data');
                        expect(result.rows).toBe(1);
                        expect(result.data).toBeInstanceOf(Array);
                        expect(result.data).toEqual(expect.arrayContaining([{ value: 1.2 }]));
                        expect(result).toHaveProperty('meta');
                        expect(result.meta).toBeInstanceOf(Array);
                        expect(result.meta).toEqual(expect.arrayContaining([{ name: 'value', type: 'Float32' }]));
                    });
                    it('JSON', async function () {
                        const result = await clickhouseClient.query({
                                query: `SELECT '{"a": 1}'::JSON AS value FORMAT TabSeparatedWithNamesAndTypes`,
                                params: {
                                    allow_experimental_object_type: '1'
                                }
                            }
                        );
                        expect(result).toHaveProperty('data');
                        expect(result.rows).toBe(1);
                        expect(result.data).toBeInstanceOf(Array);
                        expect(result.data).toEqual(expect.arrayContaining([{ value: { a: 1 } }]));
                        expect(result).toHaveProperty('meta');
                        expect(result.meta).toBeInstanceOf(Array);
                        expect(result.meta).toEqual(expect.arrayContaining([{
                            name: 'value',
                            type: `Object('json')`
                        }]));
                    });
                    describe('Float64', function () {
                        it('positive', async function () {
                            const result = await clickhouseClient.query(`
                        SELECT toFloat64(1.2) AS value
                        FORMAT TabSeparatedWithNamesAndTypes`
                            );
                            expect(result).toHaveProperty('data');
                            expect(result.rows).toBe(1);
                            expect(result.data).toBeInstanceOf(Array);
                            expect(result.data).toEqual(expect.arrayContaining([{ value: 1.2 }]));
                            expect(result).toHaveProperty('meta');
                            expect(result.meta).toBeInstanceOf(Array);
                            expect(result.meta).toEqual(expect.arrayContaining([{ name: 'value', type: 'Float64' }]));
                        });
                        it('negative', async function () {
                            const result = await clickhouseClient.query(`
                        SELECT toFloat64(-1.2) AS value
                        FORMAT TabSeparatedWithNamesAndTypes`
                            );
                            expect(result).toHaveProperty('data');
                            expect(result.rows).toBe(1);
                            expect(result.data).toBeInstanceOf(Array);
                            expect(result.data).toEqual(expect.arrayContaining([{ value: -1.2 }]));
                            expect(result).toHaveProperty('meta');
                            expect(result.meta).toBeInstanceOf(Array);
                            expect(result.meta).toEqual(expect.arrayContaining([{ name: 'value', type: 'Float64' }]));
                        });
                        it('NaN', async function () {
                            const result = await clickhouseClient.query(`
                        SELECT toFloat64(nan) AS value
                        FORMAT TabSeparatedWithNamesAndTypes`
                            );
                            expect(result).toHaveProperty('data');
                            expect(result.rows).toBe(1);
                            expect(result.data).toBeInstanceOf(Array);
                            expect(result.data).toEqual(expect.arrayContaining([{ value: NaN }]));
                            expect(result).toHaveProperty('meta');
                            expect(result.meta).toBeInstanceOf(Array);
                            expect(result.meta).toEqual(expect.arrayContaining([{ name: 'value', type: 'Float64' }]));
                        });
                        it('positive Infinity', async function () {
                            const result = await clickhouseClient.query(`
                        SELECT inf AS value
                        FORMAT TabSeparatedWithNamesAndTypes`
                            );
                            expect(result).toHaveProperty('data');
                            expect(result.rows).toBe(1);
                            expect(result.data).toBeInstanceOf(Array);
                            expect(result.data).toEqual(expect.arrayContaining([{ value: Infinity }]));
                            expect(result).toHaveProperty('meta');
                            expect(result.meta).toBeInstanceOf(Array);
                            expect(result.meta).toEqual(expect.arrayContaining([{ name: 'value', type: 'Float64' }]));
                        });
                        it('negative Infinity', async function () {
                            const result = await clickhouseClient.query(`
                        SELECT -inf AS value
                        FORMAT TabSeparatedWithNamesAndTypes`
                            );
                            expect(result).toHaveProperty('data');
                            expect(result.rows).toBe(1);
                            expect(result.data).toBeInstanceOf(Array);
                            expect(result.data).toEqual(expect.arrayContaining([{ value: -Infinity }]));
                            expect(result).toHaveProperty('meta');
                            expect(result.meta).toBeInstanceOf(Array);
                            expect(result.meta).toEqual(expect.arrayContaining([{ name: 'value', type: 'Float64' }]));
                        });
                    });
                    describe('String', function () {
                        it('empty', async function () {
                            const result = await clickhouseClient.query(`
                            SELECT '' AS emptyValue
                            FORMAT TabSeparatedWithNamesAndTypes`
                            );
                            expect(result).toHaveProperty('data');
                            expect(result.rows).toBe(1);
                            expect(result.data).toBeInstanceOf(Array);
                            expect(result.data).toEqual(expect.arrayContaining([{ emptyValue: '' }]));
                            expect(result).toHaveProperty('meta');
                            expect(result.meta).toBeInstanceOf(Array);
                            expect(result.meta).toEqual(expect.arrayContaining([
                                { name: 'emptyValue', type: 'String' }
                            ]));
                        });
                        it('empty 2', async function () {
                            const result = await clickhouseClient.query(`
                            SELECT 'a' AS value, '' AS emptyValue
                            FORMAT TabSeparatedWithNamesAndTypes`
                            );
                            expect(result).toHaveProperty('data');
                            expect(result.rows).toBe(1);
                            expect(result.data).toBeInstanceOf(Array);
                            expect(result.data).toEqual(expect.arrayContaining([{ value: 'a', emptyValue: '' }]));
                            expect(result).toHaveProperty('meta');
                            expect(result.meta).toBeInstanceOf(Array);
                            expect(result.meta).toEqual(expect.arrayContaining([
                                { name: 'value', type: 'String' },
                                { name: 'emptyValue', type: 'String' }
                            ]));
                        });
                        it('back slash', async function () {
                            const result = await clickhouseClient.query(`
                            SELECT '\\\\' AS backSlash
                            FORMAT TabSeparatedWithNamesAndTypes`
                            );
                            expect(result).toHaveProperty('data');
                            expect(result.rows).toBe(1);
                            expect(result.data).toBeInstanceOf(Array);
                            expect(result.data).toEqual(expect.arrayContaining([{ backSlash: '\\' }]));
                            expect(result).toHaveProperty('meta');
                            expect(result.meta).toBeInstanceOf(Array);
                            expect(result.meta).toEqual(expect.arrayContaining([
                                { name: 'backSlash', type: 'String' }
                            ]));
                        });
                        it('single quote', async function () {
                            const result = await clickhouseClient.query(`
                            SELECT '''' AS singleQuote
                            FORMAT TabSeparatedWithNamesAndTypes`
                            );
                            expect(result).toHaveProperty('data');
                            expect(result.rows).toBe(1);
                            expect(result.data).toBeInstanceOf(Array);
                            expect(result.data).toEqual(expect.arrayContaining([{ singleQuote: `'` }]));
                            expect(result).toHaveProperty('meta');
                            expect(result.meta).toBeInstanceOf(Array);
                            expect(result.meta).toEqual(expect.arrayContaining([
                                { name: 'singleQuote', type: 'String' }
                            ]));
                        });
                    });
                    it('DateTime', async function () {
                        const result = await clickhouseClient.query(`
                            SELECT toDateTime('2016-06-15 23:00:00') AS value
                            FORMAT TabSeparatedWithNamesAndTypes`
                        );
                        expect(result).toHaveProperty('data');
                        expect(result.rows).toBe(1);
                        expect(result.data).toBeInstanceOf(Array);
                        expect(result.data).toEqual(expect.arrayContaining([{ value: new Date(2016, 5, 15, 23, 0, 0) }]));
                        expect(result).toHaveProperty('meta');
                        expect(result.meta).toBeInstanceOf(Array);
                        expect(result.meta).toEqual(expect.arrayContaining([
                            { name: 'value', type: 'DateTime' }
                        ]));
                    });
                    it('Bool', async function () {
                        const result = await clickhouseClient.query(`
                            SELECT True AS value
                            FORMAT TabSeparatedWithNamesAndTypes`
                        );
                        expect(result).toHaveProperty('data');
                        expect(result.rows).toBe(1);
                        expect(result.data).toBeInstanceOf(Array);
                        expect(result.data).toEqual(expect.arrayContaining([{ value: true }]));
                        expect(result).toHaveProperty('meta');
                        expect(result.meta).toBeInstanceOf(Array);
                        expect(result.meta).toEqual(expect.arrayContaining([
                            { name: 'value', type: 'Bool' }
                        ]));
                    });
                });
                describe('nullable', function () {
                    it('null', async function () {
                        const result = await clickhouseClient.query(`
                        SELECT nullIf('', '') AS value
                        FORMAT TabSeparatedWithNamesAndTypes`
                        );
                        expect(result).toHaveProperty('data');
                        expect(result.rows).toBe(1);
                        expect(result.data).toBeInstanceOf(Array);
                        expect(result.data).toEqual(expect.arrayContaining([{ value: null }]));
                        expect(result).toHaveProperty('meta');
                        expect(result.meta).toBeInstanceOf(Array);
                        expect(result.meta).toEqual(expect.arrayContaining([{
                            name: 'value',
                            type: 'Nullable(String)'
                        }]));
                    });
                    it('not null', async function () {
                        const result = await clickhouseClient.query(`
                        SELECT nullIf(toUInt32(10), 2) AS value
                        FORMAT TabSeparatedWithNamesAndTypes`
                        );
                        expect(result).toHaveProperty('data');
                        expect(result.rows).toBe(1);
                        expect(result.data).toBeInstanceOf(Array);
                        expect(result.data).toEqual(expect.arrayContaining([{ value: 10 }]));
                        expect(result).toHaveProperty('meta');
                        expect(result.meta).toBeInstanceOf(Array);
                        expect(result.meta).toEqual(expect.arrayContaining([{
                            name: 'value',
                            type: 'Nullable(UInt32)'
                        }]));
                    });
                });
                describe('array', function () {
                    it('UInt32', async function () {
                        const result = await clickhouseClient.query(`
                        SELECT [toUInt32(1), toUInt32(2), toUInt32(3)] AS value
                        FORMAT TabSeparatedWithNamesAndTypes`
                        );
                        expect(result).toHaveProperty('data');
                        expect(result.rows).toBe(1);
                        expect(result.data).toBeInstanceOf(Array);
                        expect(result.data).toEqual(expect.arrayContaining([{ value: [1, 2, 3] }]));
                        expect(result).toHaveProperty('meta');
                        expect(result.meta).toBeInstanceOf(Array);
                        expect(result.meta).toEqual(expect.arrayContaining([{
                            name: 'value',
                            type: 'Array(UInt32)'
                        }]));
                    });
                    it('String', async function () {
                        const result = await clickhouseClient.query(`
                            SELECT ['a', 'b\nb', 'c,c', 'd\\'d'] AS value
                            FORMAT TabSeparatedWithNamesAndTypes`
                        );
                        expect(result).toHaveProperty('data');
                        expect(result.rows).toBe(1);
                        expect(result.data).toBeInstanceOf(Array);
                        expect(result.data).toEqual(expect.arrayContaining([{ value: ['a', 'b\nb', 'c,c', 'd\'d'] }]));
                        expect(result).toHaveProperty('meta');
                        expect(result.meta).toBeInstanceOf(Array);
                        expect(result.meta).toEqual(expect.arrayContaining([{
                            name: 'value',
                            type: 'Array(String)'
                        }]));
                    });
                });
                it('UUID', async function () {
                    const result = await clickhouseClient.query(`
                        SELECT toUUIDOrZero('') as value
                        FORMAT ${format}
                    `);
                    expect(result).toHaveProperty('rows');
                    expect(result.rows).toBe(1);
                    expect(result).toHaveProperty('meta');
                    expect(result.meta).toBeInstanceOf(Array);
                    expect(result.meta).toEqual(expect.arrayContaining([
                        { name: 'value', type: 'UUID' }
                    ]));
                    expect(result).toHaveProperty('data');
                    expect(result.data).toBeInstanceOf(Array);
                    expect(result.data).toEqual(expect.arrayContaining([
                        { value: '00000000-0000-0000-0000-000000000000' }
                    ]));
                });
                it('IPv4', async function(){
                    const result = await clickhouseClient.query(`
                        SELECT '0.0.0.0'::IPv4 as value
                        FORMAT ${format}
                    `);
                    expect(result).toHaveProperty('rows');
                    expect(result.rows).toBe(1);
                    expect(result).toHaveProperty('meta');
                    expect(result.meta).toBeInstanceOf(Array);
                    expect(result.meta).toEqual(expect.arrayContaining([
                        { name: 'value', type: 'IPv4' }
                    ]));
                    expect(result).toHaveProperty('data');
                    expect(result.data).toBeInstanceOf(Array);
                    expect(result.data).toEqual(expect.arrayContaining([
                        { value: '0.0.0.0' }
                    ]));
                });
                it('IPv6', async function(){
                    const result = await clickhouseClient.query(`
                        SELECT '::'::IPv6 as value
                        FORMAT ${format}
                    `);
                    expect(result).toHaveProperty('rows');
                    expect(result.rows).toBe(1);
                    expect(result).toHaveProperty('meta');
                    expect(result.meta).toBeInstanceOf(Array);
                    expect(result.meta).toEqual(expect.arrayContaining([
                        { name: 'value', type: 'IPv6' }
                    ]));
                    expect(result).toHaveProperty('data');
                    expect(result.data).toBeInstanceOf(Array);
                    expect(result.data).toEqual(expect.arrayContaining([
                        { value: '::' }
                    ]));
                });
            });

            it('insert ans select values', async function () {
                await clickhouseClient.query(`
                    INSERT INTO "${DATABASE}"."test_insert" (key, value)
                    VALUES ('a', 1),
                           ('b', 2),
                           ('c', 3);
                `);
                const result = await clickhouseClient.query(`
                    SELECT *
                    FROM "${DATABASE}"."test_insert" FORMAT TSVWithNamesAndTypes;
                `);
                expect(result).toBeInstanceOf(Object);
                expect(result).toHaveProperty('meta');
                expect(result).toHaveProperty('rows');
                expect(result.rows).toBe(3);
                expect(result).toHaveProperty('data');
                expect(result.data).toBeInstanceOf(Array);
                expect(result.data.length).toBe(result.rows);
            });

            it('select with limit', async function () {
                await clickhouseClient.query(`
                    INSERT INTO "${DATABASE}"."test_insert" (key, value)
                    VALUES ('a', 1),
                           ('b', 2),
                           ('c', 3);
                `);
                const result = await clickhouseClient.query(`
                    SELECT *
                    FROM "${DATABASE}"."test_insert"
                    ORDER BY time
                        LIMIT 1
                        FORMAT
                        TSVWithNamesAndTypes;
                `);
                expect(result).toBeInstanceOf(Object);
                expect(result).toHaveProperty('meta');
                expect(result).toHaveProperty('rows');
                expect(result.rows).toBe(1);
                expect(result).toHaveProperty('data');
                expect(result.data).toBeInstanceOf(Array);
                expect(result.data.length).toBe(result.rows);
                expect(result).toHaveProperty('headers');
                expect(result.headers).toBeInstanceOf(Object);
            });

            describe('WITH TOTALS', function () {

                beforeAll(async function () {
                    await clickhouseClient.query(`
                        DROP TABLE IF EXISTS "${DATABASE}".test_with_totals;
                    `);
                    await clickhouseClient.query(`
                        CREATE TABLE "${DATABASE}".test_with_totals
                        (
                            a UInt8,
                            b UInt8
                        ) ENGINE MergeTree() ORDER BY a;
                    `);
                    await clickhouseClient.query(`
                        INSERT INTO "${DATABASE}".test_with_totals
                        SELECT number % 7, number % 11
                        FROM numbers(100);
                    `);
                });

                afterAll(async function () {
                    await clickhouseClient.query(`
                        DROP TABLE "${DATABASE}".test_with_totals;
                    `);
                });

                it('JSON', async function () {
                    const result = await clickhouseClient.query(`
                        SELECT a AS value, count(DISTINCT b) AS count
                        FROM "${DATABASE}".test_with_totals
                        GROUP BY a
                        WITH TOTALS
                            FORMAT JSON;
                    `);
                    expect(result).toBeInstanceOf(Object);
                    expect(result).toHaveProperty('rows');
                    expect(result.rows).toBe(7);
                    expect(result).toHaveProperty('totals');
                    expect(result.totals).toBeInstanceOf(Object);
                    expect(result.totals).toHaveProperty('value');
                    expect(result.totals).toHaveProperty('count');
                });

                it('TabSeparatedWithNamesAndTypes', async function () {
                    const result = await clickhouseClient.query(`
                        SELECT a AS value, count(DISTINCT b) AS count
                        FROM "${DATABASE}".test_with_totals
                        GROUP BY a
                        WITH TOTALS
                            FORMAT TabSeparatedWithNamesAndTypes;
                    `);
                    expect(result).toBeInstanceOf(Object);
                    expect(result).toHaveProperty('rows');
                    expect(result.rows).toBe(7);
                    expect(result).toHaveProperty('totals');
                    expect(result.totals).toBeInstanceOf(Object);
                    expect(result.totals).toHaveProperty('value');
                    expect(result.totals).toHaveProperty('count');
                });

            });

        });

        describe('INSERT', function () {
            it("stream data", async function () {
                const blob = new Blob([JSON.stringify(['a', 0])]);
                const blobStream = blob.stream();
                await clickhouseClient.query({
                    query: `INSERT INTO "${DATABASE}"."test_insert" ("key", "value") FORMAT JSONCompactEachRow`,
                    data: Readable.from(blobStream)
                });
                const result = await clickhouseClient.query(`
                    SELECT *
                    FROM "${DATABASE}"."test_insert" FORMAT TabSeparatedWithNamesAndTypes
                `);
                expect(result.rows).toEqual(1);
            });
            it("compressed stream data: gzip", async function () {
                const blob = new Blob([JSON.stringify(['a', 0])]);
                const blobStream = blob.stream();
                const compressedStream = Readable.from(blobStream).pipe(createGzip());
                await clickhouseClient.query({
                    query: `INSERT INTO "${DATABASE}"."test_insert" ("key", "value") FORMAT JSONCompactEachRow`,
                    data: compressedStream,
                    compressed: 'gzip'
                });
                const result = await clickhouseClient.query(`
                    SELECT *
                    FROM "${DATABASE}"."test_insert" FORMAT TabSeparatedWithNamesAndTypes
                `);
                expect(result.rows).toEqual(1);
            });
            it("compressed stream data: br", async function () {
                const blob = new Blob([JSON.stringify(['a', 0])]);
                const blobStream = blob.stream();
                const compressedStream = Readable.from(blobStream).pipe(createBrotliCompress());
                await clickhouseClient.query({
                    query: `INSERT INTO "${DATABASE}"."test_insert" ("key", "value") FORMAT JSONCompactEachRow`,
                    data: compressedStream,
                    compressed: 'br'
                });
                const result = await clickhouseClient.query(`
                    SELECT *
                    FROM "${DATABASE}"."test_insert" FORMAT TabSeparatedWithNamesAndTypes
                `);
                expect(result.rows).toEqual(1);
            });
            it("compressed stream data: deflate", async function () {
                const blob = new Blob([JSON.stringify(['a', 0])]);
                const blobStream = blob.stream();
                const compressedStream = Readable.from(blobStream).pipe(createDeflate());
                await clickhouseClient.query({
                    query: `INSERT INTO "${DATABASE}"."test_insert" ("key", "value") FORMAT JSONCompactEachRow`,
                    data: compressedStream,
                    compressed: 'deflate'
                });
                const result = await clickhouseClient.query(`
                    SELECT *
                    FROM "${DATABASE}"."test_insert" FORMAT TabSeparatedWithNamesAndTypes
                `);
                expect(result.rows).toEqual(1);
            });
        });
    });
});