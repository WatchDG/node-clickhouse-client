import { ClickhouseClient } from "../src";

describe('clickhouse client params', function () {
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
    });

    afterAll(async function () {
        await clickhouseClient.close();
    });

    beforeEach(async function () {
        await clickhouseClient.query(`
            TRUNCATE TABLE IF EXISTS "${DATABASE}"."test_insert";
        `);
    });

    describe('query', function () {
        describe('extremes', function () {
            const TABLE = "test_extremes";
            beforeAll(async function () {
                await clickhouseClient.query(`DROP TABLE IF EXISTS "${DATABASE}"."${TABLE}"`);
                await clickhouseClient.query(`
                    CREATE TABLE "${DATABASE}"."${TABLE}"
                    (
                        id    UInt64,
                        value UInt64
                    ) ENGINE Memory();
                `);
                await clickhouseClient.query(`
                    INSERT INTO "${DATABASE}"."${TABLE}"
                    VALUES (0, 1),
                           (0, 2),
                           (1, 3),
                           (1, 4);
                `);
            });

            it("JSON", async function () {
                const result = await clickhouseClient.query({
                    query: `
                        SELECT *
                        FROM "${DATABASE}"."${TABLE}" FORMAT JSON;
                    `,
                    params: {
                        extremes: '1'
                    }
                });
                expect(result).toHaveProperty('extremes');
                expect(result.extremes).toHaveProperty('min');
                expect(result.extremes).toHaveProperty('max');
                expect(result.extremes.min).toStrictEqual({ id: '0', value: '1' });
                expect(result.extremes.max).toStrictEqual({ id: '1', value: '4' });
            });

            it("TabSeparatedWithNamesAndTypes", async function () {
                const result = await clickhouseClient.query({
                    query: `
                        SELECT *
                        FROM "${DATABASE}"."${TABLE}" FORMAT TabSeparatedWithNamesAndTypes;
                    `,
                    params: {
                        extremes: '1'
                    }
                });
                expect(result).toHaveProperty('extremes');
                expect(result.extremes).toHaveProperty('min');
                expect(result.extremes).toHaveProperty('max');
                expect(result.extremes.min).toStrictEqual({ id: 0n, value: 1n });
                expect(result.extremes.max).toStrictEqual({ id: 1n, value: 4n });
            });

            describe('WITH TOTALS', function () {
                it("JSON", async function () {
                    const result = await clickhouseClient.query({
                        query: `
                            SELECT id, count() as value
                            FROM "${DATABASE}"."${TABLE}"
                            GROUP BY id
                            WITH TOTALS FORMAT JSON;
                        `,
                        params: {
                            extremes: '1'
                        }
                    });
                    expect(result).toHaveProperty('extremes');
                    expect(result.extremes).toHaveProperty('min');
                    expect(result.extremes).toHaveProperty('max');
                    expect(result.extremes.min).toStrictEqual({ id: '0', value: '2' });
                    expect(result.extremes.max).toStrictEqual({ id: '1', value: '2' });
                    expect(result).toHaveProperty('totals');
                    expect(result.totals).toStrictEqual({ id: '0', value: '4' });
                });
                it("TabSeparatedWithNamesAndTypes", async function () {
                    const result = await clickhouseClient.query({
                        query: `
                            SELECT id, count() as value
                            FROM "${DATABASE}"."${TABLE}"
                            GROUP BY id
                            WITH TOTALS FORMAT TabSeparatedWithNamesAndTypes;
                        `,
                        params: {
                            extremes: '1'
                        }
                    });
                    expect(result).toHaveProperty('extremes');
                    expect(result.extremes).toHaveProperty('min');
                    expect(result.extremes).toHaveProperty('max');
                    expect(result.extremes.min).toStrictEqual({ id: 0n, value: 2n });
                    expect(result.extremes.max).toStrictEqual({ id: 1n, value: 2n });
                    expect(result).toHaveProperty('totals');
                    expect(result.totals).toStrictEqual({ id: 0n, value: 4n });
                });
            });

        });
        describe('enable_http_compression', function () {
            const TABLE = "test_enable_http_compression";
            beforeAll(async function () {
                await clickhouseClient.query(`DROP TABLE IF EXISTS "${DATABASE}"."${TABLE}"`);
                await clickhouseClient.query(`
                    CREATE TABLE "${DATABASE}"."${TABLE}"
                    (
                        id    UInt64,
                        value UInt64
                    ) ENGINE Memory();
                `);
                await clickhouseClient.query(`
                    INSERT INTO "${DATABASE}"."${TABLE}"
                    VALUES (0, 1),
                           (0, 2),
                           (1, 3),
                           (1, 4);
                `);
            });

            it("TabSeparatedWithNamesAndTypes", async function () {
                const result = await clickhouseClient.query({
                    query: `
                        SELECT *
                        FROM "${DATABASE}"."${TABLE}" FORMAT TabSeparatedWithNamesAndTypes;
                    `,
                    params: {
                        enable_http_compression: '1'
                    }
                });
                expect(result.rows).toBe(4);
            });

        });
    });

});