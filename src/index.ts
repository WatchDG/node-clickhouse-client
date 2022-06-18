import { createBrotliDecompress, createGunzip, createInflate, gzip } from "zlib";
import { Client } from "undici";

import { TSVTransform } from "./streams/tsv";

import type { Dispatcher } from 'undici';
import type { Readable } from "stream";
import type { IncomingHttpHeaders } from "http";

export const DEFAULT_DATABASE = 'default';

export type ClickhouseParams = 'extremes' | 'enable_http_compression' | string;

export interface ClickhouseClientOptions {
    protocol?: 'http' | 'https';
    user?: string;
    password?: string;
    host?: string;
    port?: string;
    params?: Record<ClickhouseParams, string>;
}

export interface ClickhouseClientRequest {
    query: string;
    params?: Record<ClickhouseParams, string>;
    data?: Readable;
    compressed?: 'gzip' | 'br' | 'deflate';
    headers?: IncomingHttpHeaders;
}

export type Request = ClickhouseClientRequest | string;

export interface MetadataItem {
    name: string;
    type?: string;
}

export type Metadata = MetadataItem[];

export class ClickhouseClient {
    private readonly httpClient: Client;
    private readonly params: Record<string, string> = {};

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
        if (options?.params) {
            this.params = options.params;
        }
    }

    private request(request: ClickhouseClientRequest): Promise<Dispatcher.ResponseData> {
        const headers: IncomingHttpHeaders = {
            'accept-encoding': 'br, gzip, deflate'
        };
        if (request.compressed) {
            headers['content-encoding'] = request.compressed;
        }
        Object.assign(headers, request.headers);
        const requestOptions: Dispatcher.RequestOptions = {
            method: 'POST',
            path: '/',
            headers
        };
        const params: Record<string, string> = Object.assign({}, this.params, request.params);
        if (request.data) {
            params.query = request.query;
            requestOptions.body = request.data;
            requestOptions.query = params;
        } else {
            requestOptions.body = request.query;
            requestOptions.query = params;
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
        const contentEncoding = headers['content-encoding'];

        if (contentEncoding && contentEncoding != 'gzip' && contentEncoding != 'br' && contentEncoding != 'deflate') {
            throw new Error(`Unsupported content encoding: ${contentEncoding}`);
        }

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
                const metadata: Record<'names' | 'types' | 'extremes' | string, any> = {};

                const tsvStream = new TSVTransform({
                    clickhouseFormat
                });

                let stream;
                if (contentEncoding === 'gzip') {
                    stream = body.pipe(createGunzip()).pipe(tsvStream);
                } else if (contentEncoding === 'br') {
                    stream = body.pipe(createBrotliDecompress()).pipe(tsvStream);
                } else if (contentEncoding === 'deflate') {
                    stream = body.pipe(createInflate());
                } else {
                    stream = body.pipe(tsvStream);
                }

                stream.on('data', function (chunk) {
                    data.push(...chunk);
                });
                stream.once('meta-names', function (names) {
                    metadata['names'] = names;
                });
                stream.once('meta-types', function (types) {
                    metadata['types'] = types;
                });
                stream.on('meta-extra', function (extra) {
                    Object.assign(metadata, extra);
                });
                stream.on('error', function (reason) {
                    reject(reason);
                });
                stream.on('end', function () {
                    const response: {
                        data: any[];
                        meta: MetadataItem[];
                        rows: number;
                        totals?: any[];
                        extremes?: { min: any, max: any },
                        headers: Record<string, any>;
                    } = {
                        data,
                        meta: ClickhouseClient.getMetadata(metadata.names, metadata.types),
                        rows: data.length,
                        headers: clickhouseHeaders
                    };
                    if (metadata.totals) {
                        response.totals = metadata.totals;
                    }
                    if (metadata.extremes) {
                        response.extremes = metadata.extremes;
                    }
                    resolve(response);
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