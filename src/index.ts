import { createBrotliDecompress, createGunzip, createInflate } from "zlib";
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

    private static getStreamDecoder(contentEncoding: string) {
        switch (contentEncoding) {
            case 'gzip':
                return createGunzip();
            case 'br':
                return createBrotliDecompress();
            case 'deflate':
                return createInflate();
            default:
                throw new Error(`Unsupported content encoding: ${contentEncoding}`);
        }
    }

    private static getStreamTransformer(clickhouseFormat: string) {
        switch (clickhouseFormat) {
            case 'TabSeparated':
            case 'TabSeparatedRaw':
            case 'TabSeparatedWithNames':
            case 'TabSeparatedWithNamesAndTypes':
            case 'TSV':
            case 'TSVRaw':
            case 'TSVWithNames':
            case 'TSVWithNamesAndTypes':
                return new TSVTransform({
                    clickhouseFormat
                });
            default:
                throw new Error(`Unsupported clickhouse format: ${clickhouseFormat}`);
        }
    }

    private static getClickhouseHeaders(headers: Record<string, any>) {
        const entries = Object.entries(headers).filter(function ([value]) {
            return /^x-clickhouse*/i.test(value);
        });
        return Object.fromEntries(entries);
    }

    private static getStreamData(stream: Readable, headers: Record<string, any>) {
        return new Promise(function (resolve, reject) {
            const data: any[] = [];
            const metadata: Record<'names' | 'types' | 'extremes' | string, any> = {};
            stream.on('data', function (chunk) {
                data.push(...chunk);
            });
            stream.once('meta-names', function (names) {
                metadata['names'] = names;
            });
            stream.once('meta-types', function (types) {
                metadata['types'] = types;
            });
            stream.once('meta-extra', function (extra) {
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
                    extremes?: { min: any, max: any };
                    headers: Record<string, any>;
                } = {
                    data,
                    meta: ClickhouseClient.getMetadata(metadata.names, metadata.types),
                    rows: data.length,
                    headers
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

    constructor(options?: ClickhouseClientOptions) {
        const baseUrl = ClickhouseClient.getBaseUrl(options);
        this.httpClient = new Client(baseUrl);
        if (options?.params) {
            this.params = options.params;
        }
    }

    private async request(request: ClickhouseClientRequest): Promise<Dispatcher.ResponseData> {
        const incomingHeaders: IncomingHttpHeaders = {
            'accept-encoding': 'br, gzip, deflate'
        };
        if (request.compressed) {
            incomingHeaders['content-encoding'] = request.compressed;
        }
        Object.assign(incomingHeaders, request.headers);
        const requestOptions: Dispatcher.RequestOptions = {
            method: 'POST',
            path: '/',
            headers: incomingHeaders as any // TODO: resolve IncomingHttpHeaders
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
        const { statusCode, body, ...other } = await this.httpClient.request(requestOptions);
        if (statusCode != 200) {
            throw new Error(await body.text());
        }
        return { statusCode, body, ...other };
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
        const { headers, body } = await this.request(_request);

        const clickhouseHeaders = ClickhouseClient.getClickhouseHeaders(headers);

        const contentEncoding = headers['content-encoding'] as string | undefined;
        const clickhouseFormat = headers['x-clickhouse-format'] as string | undefined;

        if (clickhouseFormat === 'JSON') {
            return await body.json();
        }

        const decoder = contentEncoding ? ClickhouseClient.getStreamDecoder(contentEncoding) : null;
        const transformer = clickhouseFormat ? ClickhouseClient.getStreamTransformer(clickhouseFormat) : null;

        if (!transformer) {
            return {
                headers: clickhouseHeaders
            };
        }

        const stream = decoder ? body.pipe(decoder).pipe(transformer) : body.pipe(transformer);

        return await ClickhouseClient.getStreamData(stream, clickhouseHeaders);
    }

    async stream(request: Request) {
        const _request = ClickhouseClient.prepareRequest(request);
        const { headers, body } = await this.request(_request);

        const clickhouseHeaders = ClickhouseClient.getClickhouseHeaders(headers);

        const contentEncoding = headers['content-encoding'] as string | undefined;
        const clickhouseFormat = headers['x-clickhouse-format'] as string | undefined;

        const decoder = contentEncoding ? ClickhouseClient.getStreamDecoder(contentEncoding) : null;
        const transformer = clickhouseFormat ? ClickhouseClient.getStreamTransformer(clickhouseFormat) : null;

        if (!transformer) {
            return {
                headers: clickhouseHeaders
            };
        }

        const stream = decoder ? body.pipe(decoder).pipe(transformer) : body.pipe(transformer);

        return {
            stream,
            headers: clickhouseHeaders
        };
    }

    async close(): Promise<void> {
        await this.httpClient.close();
    }
}