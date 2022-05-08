import { Client } from "undici";

import type { Dispatcher } from 'undici';
import type { Readable } from "stream";

export interface ClickhouseClientOptions {
    protocol?: 'http' | 'https';
    user?: string;
    password?: string;
    host?: string;
    port?: string;
}

export interface ClickhouseClientRequest {
    query: string;
    data?: Readable;
}

export class ClickhouseClient {
    private readonly httpClient: Client;

    static getBaseUrl(options?: ClickhouseClientOptions): URL {
        const url = new URL(`http://localhost:8123`);
        if (!options) {
            return url;
        }
        if (options.protocol) {
            url.protocol = options.protocol;
        }
        if (options.user) {
            url.username = options.user;
        }
        if (options.password) {
            url.password = options.password;
        }
        if (options.host) {
            url.host = options.host;
        }
        if (options.port) {
            url.port = options.port;
        }
        return url;
    }

    static prepareRequest(request: ClickhouseClientRequest | string): ClickhouseClientRequest {
        if (typeof request === "string") {
            return {
                query: request
            };
        }
        return request;
    }

    constructor(options?: ClickhouseClientOptions) {
        const baseUrl = ClickhouseClient.getBaseUrl(options);
        this.httpClient = new Client(baseUrl);
    }

    private request(request: ClickhouseClientRequest): Promise<Dispatcher.ResponseData> {
        const requestOptions: Dispatcher.RequestOptions = {
            method: 'POST',
            path: '/'
        };
        if (request.data) {
            requestOptions.path = `/?query=${encodeURIComponent(request.query)}`;
            requestOptions.body = request.data;
        } else {
            requestOptions.body = request.query;
        }
        return this.httpClient.request(requestOptions);
    }

    async ping(): Promise<boolean> {
        const { statusCode, body } = await this.httpClient.request({
            method: 'GET',
            path: '/ping'
        });
        return statusCode == 200 && (await body.text()) === 'Ok.\n';
    }

    async query(request: ClickhouseClientRequest | string) {
        const _request = ClickhouseClient.prepareRequest(request);
        const { statusCode, headers, body } = await this.request(_request);
        if (statusCode != 200) {
            throw new Error(await body.text());
        }
        if (headers['x-clickhouse-format'] === 'JSON') {
            return await body.json();
        }
    }

    async close(): Promise<void> {
        await this.httpClient.close();
    }
}
