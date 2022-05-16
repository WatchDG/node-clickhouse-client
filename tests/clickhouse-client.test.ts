import { ClickhouseClient } from "../src";
import { Readable } from "stream";

describe('clickhouse client', function () {
    it('init', function () {
        const clickhouseClient = new ClickhouseClient();
        expect(clickhouseClient).toBeInstanceOf(ClickhouseClient);
    });

    it('ping', async function () {
        const clickhouseClient = new ClickhouseClient();
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
            it('select', async function () {
                const clickhouseClient = new ClickhouseClient();
                const result = await clickhouseClient.query(`
                SELECT  1 AS v1,
                        '2' AS v2,
                        [3, 4] AS v3,
                        ['5','6'] AS v4
                FORMAT TabSeparatedWithNames
                `);
                expect(result).toHaveProperty('data');
                expect(result.rows).toBe(1);
                expect(result.data).toBeInstanceOf(Array);
                expect(result.data).toEqual(expect.arrayContaining([{
                    'v1': '1',
                    'v2': '2',
                    'v3': '[3,4]',
                    'v4': `['5','6']`
                }]));
                expect(result).toHaveProperty('meta');
                expect(result.meta).toBeInstanceOf(Object);
                expect(result.meta).toHaveProperty('names');
                expect(result.meta.names).toEqual(['v1', 'v2', 'v3', 'v4']);
            });
        });

        describe('TabSeparatedWithNamesAndTypes', function () {
            it('select', async function () {
                const clickhouseClient = new ClickhouseClient();
                const result = await clickhouseClient.query(`
                SELECT  1 AS v1,
                        '2' AS v2,
                        [3, 4] AS v3,
                        ['5','6'] AS v4,
                        false AS v5
                FORMAT TabSeparatedWithNamesAndTypes
                `);
                expect(result).toHaveProperty('data');
                expect(result.rows).toBe(1);
                expect(result.data).toBeInstanceOf(Array);
                expect(result.data).toEqual(expect.arrayContaining([{
                    'v1': 1,
                    'v2': '2',
                    'v3': '[3,4]',
                    'v4': `['5','6']`,
                    'v5': false
                }]));
                expect(result).toHaveProperty('meta');
                expect(result.meta).toBeInstanceOf(Object);
                expect(result.meta).toHaveProperty('names');
                expect(result.meta.names).toEqual(['v1', 'v2', 'v3', 'v4', 'v5']);
                expect(result.meta).toHaveProperty('types');
                console.log(result.meta);
            });
        });
    });

    describe('stream', function () {
        describe('TabSeparated', function () {
            it('select', function (done) {
                const clickhouseClient = new ClickhouseClient();

                clickhouseClient.stream(`
                SELECT  1,
                        '2',
                        [3, 4],
                        ['5','6']
                FORMAT TabSeparated
                `).then(function (stream) {
                    expect(stream).toBeInstanceOf(Readable);
                    const data: any[] = [];
                    stream.on('data', function (chunk) {
                        data.push(...chunk);
                    });
                    stream.on('end', function () {
                        expect(data.length).toBe(1);
                        expect(data).toEqual(expect.arrayContaining([['1', '2', '[3,4]', `['5','6']`]]));
                        done();
                    });
                });
            });
        });

        describe('TabSeparatedRaw', function () {
            it('select', function (done) {
                const clickhouseClient = new ClickhouseClient();

                clickhouseClient.stream(`
                SELECT  1,
                        '2',
                        [3, 4],
                        ['5','6']
                FORMAT TabSeparatedRaw
                `).then(function (stream) {
                    expect(stream).toBeInstanceOf(Readable);
                    const data: any[] = [];
                    stream.on('data', function (chunk) {
                        data.push(...chunk);
                    });
                    stream.on('end', function () {
                        expect(data.length).toBe(1);
                        expect(data).toEqual(expect.arrayContaining([['1', '2', '[3,4]', `['5','6']`]]));
                        done();
                    });
                });
            });
        });
    });
});