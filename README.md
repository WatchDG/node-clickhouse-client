# clickhouse-client

nodejs clickhouse client

## Install

```shell
yarn add @watchdg/clickhouse-client
# or
npm install @watchdg/clickhouse-client
```

## How to use

```javascript
import {ClickhouseClient} from '@watchdg/clickhouse-client';

(async function () {
    const clickhouseClient = new ClickhouseClient();
    const {rows, data} = await clickhouseClient.query('select 1');
    console.log(data);
})();
```

## Supported formats

* TabSeparated (TSV)
* TabSeparatedRaw (TSVRaw)
* TabSeparatedWithNames (TSVWithNames)
* TabSeparatedWithNamesAndTypes (TSVWithNamesAndTypes)

## Supported types

| Clickhouse | NodeJS  |
|------------|---------|
| Int8       | Number  |
| Int16      | Number  |
| Int32      | Number  |
| UInt8      | Number  |
| UInt16     | Number  |
| UInt32     | Number  |
| Float32    | Number  |
| Float64    | Number  |
| Int64      | BigInt  |
| Int128     | BigInt  |
| Int256     | BigInt  |
| UInt64     | BigInt  |
| UInt128    | BigInt  |
| UInt256    | BigInt  |
| Bool       | Boolean |
| DateTime   | Date    |
| String     | String  |
| UUID       | String  |
| IPv4       | String  |
| IPv6       | String  |

* Nullable(T)
* Array(T)