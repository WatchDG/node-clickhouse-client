import { ClickhouseClient } from "../src";
import { Readable } from "stream";

describe('clickhouse client', function () {
    let clickhouseClient: ClickhouseClient;

    beforeAll(async function () {
        clickhouseClient = new ClickhouseClient();
    });

    afterAll(async function () {
        await clickhouseClient.close();
    });

    it('init', function () {
        expect(clickhouseClient).toBeInstanceOf(ClickhouseClient);
    });

    it('ping', async function () {
        const result = await clickhouseClient.ping();
        expect(result).toBe(true);
    });

    describe('query', function () {
        describe('JSON', function () {
            it('select', async function () {
                const clickhouseClient = new ClickhouseClient();
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
        });

        describe('TabSeparated', function () {
            it('select', async function () {
                const clickhouseClient = new ClickhouseClient();
                const result = await clickhouseClient.query(`
                SELECT  1,
                        '2',
                        [3, 4],
                        ['5','6']
                FORMAT TabSeparated
                `);
                expect(result).toHaveProperty('data');
                expect(result.rows).toBe(1);
                expect(result.data).toBeInstanceOf(Array);
                expect(result.data).toEqual(expect.arrayContaining([['1', '2', '[3,4]', `['5','6']`]]));
            });
        });

        describe('TabSeparatedRaw', function () {
            it('select', async function () {
                const clickhouseClient = new ClickhouseClient();
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
        });

        describe('TabSeparatedWithNames', function () {
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
        });

        describe('TabSeparatedWithNamesAndTypes', function () {
            describe('select', function () {
                describe('Int8', function () {
                    it('positive', async function () {
                        const clickhouseClient = new ClickhouseClient();
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
                        const clickhouseClient = new ClickhouseClient();
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
                    const clickhouseClient = new ClickhouseClient();
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
                    const clickhouseClient = new ClickhouseClient();
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
                    const clickhouseClient = new ClickhouseClient();
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
                    const clickhouseClient = new ClickhouseClient();
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
                    const clickhouseClient = new ClickhouseClient();
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
                    const clickhouseClient = new ClickhouseClient();
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
                    const clickhouseClient = new ClickhouseClient();
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
                    const clickhouseClient = new ClickhouseClient();
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
                    const clickhouseClient = new ClickhouseClient();
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
                    const clickhouseClient = new ClickhouseClient();
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
                    const clickhouseClient = new ClickhouseClient();
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
                    const clickhouseClient = new ClickhouseClient();
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
                describe('Float64', function () {
                    it('positive', async function () {
                        const clickhouseClient = new ClickhouseClient();
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
                        const clickhouseClient = new ClickhouseClient();
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
                        const clickhouseClient = new ClickhouseClient();
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
                        const clickhouseClient = new ClickhouseClient();
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
                        const clickhouseClient = new ClickhouseClient();
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
            });
        });

        it("create and drop table", async function () {
            const resultCreate = await clickhouseClient.query(`
                CREATE TABLE IF NOT EXISTS "default"."user"
                (
                    id         UInt32,
                    first_name String,
                    last_name  String
                ) ENGINE MergeTree() ORDER BY id;
            `);
            expect(resultCreate).toBeUndefined();
            const resultDrop = await clickhouseClient.query(`DROP TABLE "default"."user";`);
            expect(resultDrop).toBeUndefined();
        });
    });
});