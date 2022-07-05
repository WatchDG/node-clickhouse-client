import { Transform } from "stream";

import type { TransformCallback, TransformOptions } from "stream";

const DOES_NOT_CONTAIN = -1;

const END_LINE_CHAR_CODE = 0x0a;
const TAB_CHAR_CODE = 0x09;

function lastIndexOf(buffer: Buffer, value: number, start: number, end: number): number {
    for (let lastIndex = end - 1; lastIndex >= 0 && lastIndex >= start; lastIndex--) {
        if (buffer[lastIndex] == value) {
            return lastIndex;
        }
    }
    return DOES_NOT_CONTAIN;
}

function parseColumn(column: Buffer, type?: string): any {
    if (type === 'Int8' ||
        type === 'Int16' ||
        type === 'Int32' ||
        type === 'UInt8' ||
        type === 'UInt16' ||
        type === 'UInt32' ||
        type === 'Float32' ||
        type === 'Float64') {
        if (Buffer.compare(column, Buffer.from('inf')) === 0) {
            return Infinity;
        } else if (Buffer.compare(column, Buffer.from('-inf')) === 0) {
            return -Infinity;
        } else if (Buffer.compare(column, Buffer.from('nan')) === 0) {
            return NaN;
        } else {
            return Number(column);
        }
    }
    if (type === 'Int64' ||
        type === 'Int128' ||
        type === 'Int256' ||
        type === 'UInt64' ||
        type === 'UInt128' ||
        type === 'UInt256') {
        return BigInt(column.toString());
    }
    if (type === 'Bool') {
        return column.toString() === 'true';
    }
    if (type === 'DateTime') {
        return new Date(column.toString());
    }
    if (type === 'String') {
        return column.toString();
    }
    const match = type?.match(/Nullable\((.*)\)/);
    if (match) {
        if (column.length == 2 && column.toString() === '\\N') {
            return null;
        }
        const innerType = match[1];
        return parseColumn(column, innerType);
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
    | 'TabSeparatedWithNamesAndTypes'
    | 'TSV'
    | 'TSVRaw'
    | 'TSVWithNames'
    | 'TSVWithNamesAndTypes';

type ColumnPoint = [number, number];
type RowPoints = ColumnPoint[];

enum TSVRowType {
    Unknown,
    Names,
    Types,
    Data,
    Empty,
    Extra
}

export interface TSVTransformOptions {
    transform?: {
        writableHighWaterMark?: number;
        readableHighWaterMark?: number;
    };
    clickhouseFormat?: ClickhouseTSVFormat;
    extremes?: boolean;
}

export class TSVTransform extends Transform {
    private buffer: Buffer = Buffer.alloc(0);
    private lastCheckedOffset = 0;
    private readonly clickhouseFormat: ClickhouseTSVFormat = 'TabSeparated';

    private readonly withExtremes: boolean = false;

    private numOfColumns = 0;
    private previousRowType: TSVRowType = TSVRowType.Unknown;
    private currentRowType: TSVRowType = TSVRowType.Unknown;
    private nextRowType: TSVRowType = TSVRowType.Unknown;

    private names?: string[];
    private types?: string[];

    private readonly extraRows: Record<string, any>[] = [];

    constructor(options?: TSVTransformOptions) {
        const _options: TransformOptions = Object.assign({
            readableObjectMode: true
        }, options?.transform);

        super(_options);

        if (options?.clickhouseFormat) {
            this.clickhouseFormat = options.clickhouseFormat;
        }
        if (options?.extremes) {
            this.withExtremes = options.extremes;
        }

        switch (this.clickhouseFormat) {
            case 'TabSeparatedWithNames':
            case 'TSVWithNames':
                this.setCursor(TSVRowType.Unknown, TSVRowType.Names, TSVRowType.Data);
                break;
            case 'TabSeparatedWithNamesAndTypes':
            case 'TSVWithNamesAndTypes':
                this.setCursor(TSVRowType.Unknown, TSVRowType.Names, TSVRowType.Types);
                break;
            case 'TabSeparated':
            case 'TSV':
            case 'TabSeparatedRaw':
            case 'TSVRaw':
                this.setCursor(TSVRowType.Unknown, TSVRowType.Data, TSVRowType.Data);
        }

        this.once('end', () => {
            if (this.extraRows.length > 0) {
                let extra: { totals?: any, extremes?: { min: any, max: any } } = {};
                if (this.extraRows.length === 1) {
                    extra = {
                        totals: this.extraRows[0]
                    };
                } else if (this.extraRows.length === 2) {
                    extra = {
                        extremes: {
                            min: this.extraRows[0],
                            max: this.extraRows[1]
                        }
                    };
                } else if (this.extraRows.length === 3) {
                    extra = {
                        totals: this.extraRows[0],
                        extremes: {
                            min: this.extraRows[1],
                            max: this.extraRows[2]
                        }
                    };
                }
                this.emit('meta-extra', extra);
            }
        });
    }

    private setCursor(previousRowType: TSVRowType, currentRowType: TSVRowType, nextRowType: TSVRowType) {
        this.previousRowType = previousRowType;
        this.currentRowType = currentRowType;
        this.nextRowType = nextRowType;
    }

    private currentCursor(currentRowType: TSVRowType) {
        this.currentRowType = currentRowType;
    }

    private nextCursor(nextRowType: TSVRowType = TSVRowType.Unknown) {
        if (this.previousRowType === TSVRowType.Data && this.currentRowType === TSVRowType.Empty ||
            this.currentRowType === TSVRowType.Extra ||
            this.previousRowType === TSVRowType.Extra && this.currentRowType === TSVRowType.Empty
        ) {
            this.nextRowType = TSVRowType.Extra;
        }

        this.previousRowType = this.currentRowType;
        this.currentRowType = this.nextRowType;
        this.nextRowType = nextRowType;
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
        const bufferLength = buffer.length;

        this.buffer = this.buffer.subarray(lastEndLineIndex + 1);
        this.lastCheckedOffset = 0;

        const rowsPoints: RowPoints[] = [];

        let startColumnIndex = 0;
        let endColumnIndex = startColumnIndex;
        while (endColumnIndex < bufferLength) {
            const rowColumns: RowPoints = [];
            while (buffer[endColumnIndex] !== END_LINE_CHAR_CODE) {
                if (buffer[endColumnIndex] === TAB_CHAR_CODE) {
                    rowColumns.push([startColumnIndex, endColumnIndex]);
                    startColumnIndex = endColumnIndex + 1;
                }
                endColumnIndex++;
            }
            rowColumns.push([startColumnIndex, endColumnIndex]);
            rowsPoints.push(rowColumns);
            endColumnIndex++;
            startColumnIndex = endColumnIndex;
        }

        if (this.numOfColumns === 0) {
            this.numOfColumns = rowsPoints[0].length;
        }

        const rows = [];

        for (const rowPoint of rowsPoints) {
            if (rowPoint.length === 1 && rowPoint[0][0] === rowPoint[0][1] && this.numOfColumns >= 2) {
                this.currentCursor(TSVRowType.Empty);
                this.nextCursor();
                continue;
            }
            const row = [];
            for (const columnPoint of rowPoint) {
                const column = buffer.subarray(columnPoint[0], columnPoint[1]);
                row.push(column);
            }
            if (this.currentRowType === TSVRowType.Names) {
                const names = parseRow(row) as string[];
                this.emit('meta-names', names);
                this.names = names;
            } else if (this.currentRowType === TSVRowType.Types) {
                const types = parseRow(row) as string[];
                this.emit('meta-types', types);
                this.types = types;
            } else if (this.currentRowType === TSVRowType.Extra) {
                const parsedRow = parseRow(row, this.names, this.types);
                this.extraRows.push(parsedRow);
            } else {
                this.currentCursor(TSVRowType.Data);
                rows.push(parseRow(row, this.names, this.types));
            }
            this.nextCursor();
        }

        this.push(rows);
        callback(null);
    }
}