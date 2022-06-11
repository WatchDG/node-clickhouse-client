import { Client } from "undici";

import { TSVTransform } from "./streams/tsv";

import type { Dispatcher } from 'undici';
import type { Readable } from "stream";

export const DEFAULT_DATABASE = 'default';

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

export type Request = ClickhouseClientRequest | string;

export interface MetadataItem {
    name: string;
    type?: string;
}

export type Metadata = MetadataItem[];

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

    static prepareRequest(request: Request): ClickhouseClientRequest {
        if (typeof request === "string") {
            return {
                query: request
            };
        }
        return request;
    }

    static getMetadata(names?: string[], types?: string[]): Metadata {
        const metadata: Metadata = [];
        if (names) {
            if (types) {
                names.forEach(function (name, index) {
                    metadata.push({
                        name,
                        type: types[index]
                    });
                });
            } else {
                names.forEach(function (name) {
                    metadata.push({
                        name
                    });
                });
            }
        }
        return metadata;
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

    async query(request: Request) {
        const _request = ClickhouseClient.prepareRequest(request);
        const { statusCode, headers, body } = await this.request(_request);
        if (statusCode != 200) {
            throw new Error(await body.text());
        }
        const clickhouseHeaders = Object.fromEntries(
            Object.entries(headers).filter(function ([value]) {
                return /^x-clickhouse*/i.test(value);
            })
        );
        const clickhouseFormat = headers['x-clickhouse-format'];
        if (clickhouseFormat === 'JSON') {
            return await body.json();
        }
        if (clickhouseFormat === 'TabSeparated' ||
            clickhouseFormat === 'TabSeparatedRaw' ||
            clickhouseFormat === 'TabSeparatedWithNames' ||
            clickhouseFormat === 'TabSeparatedWithNamesAndTypes' ||
            clickhouseFormat === 'TSV' ||
            clickhouseFormat === 'TSVRaw' ||
            clickhouseFormat === 'TSVWithNames' ||
            clickhouseFormat === 'TSVWithNamesAndTypes'
        ) {
            return await new Promise((resolve, reject) => {
                const data: any[] = [];
                const metadata: Record<'names' | 'types' | string, any> = {};
                const tsvStream = new TSVTransform({
                    clickhouseFormat
                });
                const stream = body.pipe(tsvStream);
                stream.on('data', function (chunk) {
                    data.push(...chunk);
                });
                stream.on('metadata', function (eventMetadata) {
                    metadata[eventMetadata.type] = eventMetadata.value;
                });
                stream.on('error', function (reason) {
                    reject(reason);
                });
                stream.on('end', function () {
                    resolve({
                        data,
                        meta: ClickhouseClient.getMetadata(metadata.names, metadata.types),
                        totals: metadata.totals,
                        rows: data.length,
                        headers: clickhouseHeaders
                    });
                });
            });
        }
        if (clickhouseFormat) {
            throw new Error(`Unsupported clickhouse format: ${clickhouseFormat}`);
        }
        return {
            headers: clickhouseHeaders
        };
    }

    async stream(request: Request) {
        const _request = ClickhouseClient.prepareRequest(request);
        const { statusCode, headers, body } = await this.request(_request);
        if (statusCode != 200) {
            throw new Error(await body.text());
        }
        const clickhouseHeaders = Object.fromEntries(
            Object.entries(headers).filter(function ([value]) {
                return /^x-clickhouse*/i.test(value);
            })
        );
        const clickhouseFormat = headers['x-clickhouse-format'];
        if (clickhouseFormat === 'TabSeparated' ||
            clickhouseFormat === 'TabSeparatedRaw' ||
            clickhouseFormat === 'TabSeparatedWithNames' ||
            clickhouseFormat === 'TabSeparatedWithNamesAndTypes' ||
            clickhouseFormat === 'TSV' ||
            clickhouseFormat === 'TSVRaw' ||
            clickhouseFormat === 'TSVWithNames' ||
            clickhouseFormat === 'TSVWithNamesAndTypes'
        ) {
            const tsvStream = new TSVTransform({
                clickhouseFormat
            });
            return {
                stream: tsvStream,
                headers: clickhouseHeaders
            };
        }
        if (clickhouseFormat) {
            throw new Error(`Unsupported clickhouse format: ${clickhouseFormat}`);
        }
    }

    async close(): Promise<void> {
        await this.httpClient.close();
    }
}