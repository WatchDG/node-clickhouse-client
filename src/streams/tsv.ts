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

function parseValue(buffer: Buffer): { parsed: boolean, value: any } {
    return {
        parsed: true,
        value: buffer.toString()
    };
}

export interface TSVTransformOptions {
    writableHighWaterMark?: number;
    readableHighWaterMark?: number;
}

export class TSVTransform extends Transform {
    private buffer: Buffer = Buffer.alloc(0);
    private lastCheckedOffset = 0;

    constructor(options?: TSVTransformOptions) {
        const _options: TransformOptions = Object.assign({
            readableObjectMode: true
        }, options);
        super(_options);
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

        const records = [];

        let record = [];

        let startValueIndex = 0;
        let endValueIndex = startValueIndex;
        while (endValueIndex < bufferLength) {
            while (endValueIndex < bufferLength && buffer[endValueIndex] != TAB_CHAR_CODE && buffer[endValueIndex] != END_LINE_CHAR_CODE) {
                endValueIndex++;
            }
            const valueBuffer = buffer.subarray(startValueIndex, endValueIndex);
            const parsedResult = parseValue(valueBuffer);
            if (!parsedResult.parsed) {
                callback(new Error(`Can not parse value: ${JSON.stringify(valueBuffer)}`));
                return;
            }
            record.push(parsedResult.value);
            if (buffer[endValueIndex] === END_LINE_CHAR_CODE) {
                records.push(record);
                record = [];
            }
            startValueIndex = endValueIndex + 1;
            endValueIndex = startValueIndex;
        }
        this.push(records);
        callback(null);
    }
}