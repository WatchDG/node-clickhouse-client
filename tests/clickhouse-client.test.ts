import { ClickhouseClient } from "../src";

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

    it('select 1', async function () {
        const clickhouseClient = new ClickhouseClient();
        const result = await clickhouseClient.query('SELECT 1 AS value format JSON');
        expect(result).toHaveProperty('data');
        expect(result.rows).toBe(1);
        expect(result.data).toBeInstanceOf(Array);
        expect(result.data).toEqual(expect.arrayContaining([{ value: 1 }]));
    });
});