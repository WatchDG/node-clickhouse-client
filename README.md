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