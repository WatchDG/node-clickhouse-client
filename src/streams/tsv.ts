import { Transform } from "stream";

import type { TransformCallback, TransformOptions } from "stream";

const DOES_NOT_CONTAIN = -1;

const END_LINE_CHAR_CODE = 0x0a;
const TAB_CHAR_CODE = 0x09;

function lastIndexOf(buffer: Buffer, value: number, start: number, end: number): number {
    for (let lastIndex = end - 1; lastIndex > 0 && lastIndex >= start; lastIndex--) {
        if (buffer[lastIndex] == value) {
            return lastIndex;
        }
    }
    return DOES_NOT_CONTAIN;
}

function parseColumn(column: Buffer, type?: string) {
    if (type === 'Int8' ||
        type == 'Int16' ||
        type == 'Int32' ||
        type == 'UInt8' ||
        type == 'UInt16' ||
        type == 'UInt32') {
        return Number(column);
    }
    if (type === 'Bool') {
        return column.toString() === 'true';
    }
    return column.toString();
}

function parseRow(row: Buffer[], names?: string[], types?: string[]): string[] | Record<string, any> {
    if (names) {
        const object: Record<any, any> = {};
        if (types) {
            row.forEach(function (value, index) {
                object[names[index]] = parseColumn(value, types[index]);
            });
        } else {
            row.forEach(function (value, index) {
                object[names[index]] = parseColumn(value);
            });
        }
        return object;
    }
    return row.map(function (column) {
        return parseColumn(column);
    });
}

type ClickhouseTSVFormat =
    'TabSeparated'
    | 'TabSeparatedRaw'
    | 'TabSeparatedWithNames'
    | 'TabSeparatedWithNamesAndTypes';

export interface TSVTransformOptions {
    transform?: {
        writableHighWaterMark?: number;
        readableHighWaterMark?: number;
    };
    clickhouseFormat?: ClickhouseTSVFormat;
}

export class TSVTransform extends Transform {
    private buffer: Buffer = Buffer.alloc(0);
    private lastCheckedOffset = 0;
    private readonly clickhouseFormat: ClickhouseTSVFormat = 'TabSeparated';

    private readonly withNames: boolean = false;
    private readonly withTypes: boolean = false;
    private names?: string[];
    private types?: string[];

    constructor(options?: TSVTransformOptions) {
        const _options: TransformOptions = Object.assign({
            readableObjectMode: true
        }, options?.transform);
        super(_options);
        if (options?.clickhouseFormat) {
            this.clickhouseFormat = options.clickhouseFormat;
        }
        if (this.clickhouseFormat === 'TabSeparatedWithNames') {
            this.withNames = true;
        } else if (this.clickhouseFormat === 'TabSeparatedWithNamesAndTypes') {
            this.withNames = true;
            this.withTypes = true;
        }
    }

    _transform(chunk: any, encoding: BufferEncoding, callback: TransformCallback) {
        this.buffer = Buffer.concat([this.buffer, chunk], this.buffer.byteLength + chunk.byteLength);

        const lastEndLineIndex = lastIndexOf(this.buffer, END_LINE_CHAR_CODE, this.lastCheckedOffset, this.buffer.byteLength);

        if (lastEndLineIndex === DOES_NOT_CONTAIN) {
            this.lastCheckedOffset = this.buffer.byteLength;
            callback(null);
            return;
        }

        const buffer = this.buffer.subarray(0, lastEndLineIndex + 1);
        this.buffer = this.buffer.subarray(lastEndLineIndex + 1);
        this.lastCheckedOffset = 0;

        const bufferLength = buffer.length;

        const rows = [];

        let row = [];

        let startValueIndex = 0;
        let endValueIndex = startValueIndex;
        while (endValueIndex < bufferLength) {
            while (endValueIndex < bufferLength && buffer[endValueIndex] != TAB_CHAR_CODE && buffer[endValueIndex] != END_LINE_CHAR_CODE) {
                endValueIndex++;
            }
            const column = buffer.subarray(startValueIndex, endValueIndex);
            row.push(column);
            if (buffer[endValueIndex] === END_LINE_CHAR_CODE) {
                if (this.withNames && !this.names) {
                    const names = parseRow(row) as string[];
                    this.emit('metadata', {
                        type: 'names',
                        value: names
                    });
                    this.names = names;
                } else if (this.withTypes && !this.types) {
                    const types = parseRow(row) as string[];
                    this.emit('metadata', {
                        type: 'types',
                        value: types
                    });
                    this.types = types;
                } else {
                    rows.push(parseRow(row, this.names, this.types));
                }
                row = [];
            }
            startValueIndex = endValueIndex + 1;
            endValueIndex = startValueIndex;
        }
        this.push(rows);
        callback(null);
    }
}