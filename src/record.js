// Copyright (c) 2018-present, GM Cruise LLC

// This source code is licensed under the Apache License, Version 2.0,
// found in the LICENSE file in the root directory of this source tree.
// You may not use this file except in compliance with the License.

// @flow

import int53 from "int53";

import { extractFields, extractTime, composeTime } from "./fields";
import { MessageReader } from "./MessageReader";
import type { Time } from "./types";

import { composeHeader } from "./header";

const HEADER_READAHEAD = 4096;

const readUInt64LE = (buffer: Buffer) => {
  return int53.readUInt64LE(buffer, 0);
};

const writeUInt64LE = (num: number) => {
  const buffer = Buffer.alloc(8);
  int53.writeUInt64LE(num, buffer, 0);
  return buffer;
};

const writeUInt32LE = (num: number) => {
  const buffer = Buffer.alloc(4);
  buffer.writeUInt32LE(num, 0);
  return buffer;
};

const writeUInt8 = (num: number) => {
  const buffer = Buffer.alloc(1);
  buffer.writeUInt8(num, 0);
  return buffer;
};

export class Record {
  offset: number;
  dataOffset: number;
  end: number;
  length: number;

  constructor(_fields: { [key: string]: any }) {}

  parseData(_buffer: Buffer) {}

  composeRecord() {}
}

export class BagHeader extends Record {
  static opcode = 3;
  indexPosition: number;
  connectionCount: number;
  chunkCount: number;

  constructor(fields: { [key: string]: Buffer }) {
    super(fields);
    this.indexPosition = readUInt64LE(fields.index_pos);
    this.connectionCount = fields.conn_count.readInt32LE(0);
    this.chunkCount = fields.chunk_count.readInt32LE(0);
  }

  composeRecord() {
    const headers = [
      {
        name: "index_pos",
        value: writeUInt64LE(this.indexPosition)
      },
      {
        name: "conn_count",
        value: writeUInt32LE(this.connectionCount)
      },
      {
        name: "chunk_count",
        value: writeUInt32LE(this.chunkCount)
      },
      {
        name: "op",
        value: writeUInt8(BagHeader.opcode)
      }
    ];

    const headersBuffer = composeHeader(headers);

    let data = "";

    for (let i = 0; i < HEADER_READAHEAD - headersBuffer.length; i++) {
      data += " ";
    }
    const dataBuffer = Buffer.alloc(HEADER_READAHEAD - headersBuffer.length + 4, data, "ascii");
    const dataLengthBuffer = writeUInt32LE(dataBuffer.length);

    return Buffer.concat([headersBuffer, dataLengthBuffer, dataBuffer], HEADER_READAHEAD + 8);

  }

}

export class Chunk extends Record {
  static opcode = 5;
  compression: string;
  size: number;
  data: Buffer;

  constructor(fields: { [key: string]: Buffer }) {
    super(fields);
    this.compression = fields.compression.toString();
    this.size = fields.size.readUInt32LE(0);
  }

  parseData(buffer: Buffer) {
    this.data = buffer;
  }

  composeRecord() {
    const headers = [
      {
        name: "compression",
        value: Buffer.alloc(this.compression.length, this.compression, "ascii")
      },
      {
        name: "size",
        value: writeUInt32LE(this.size)
      },
      {
        name: "op",
        value: writeUInt8(Chunk.opcode)
      }
    ];

    const headersBuffer = composeHeader(headers);
    const dataLengthBuffer = writeUInt32LE(this.size);

    return Buffer.concat([headersBuffer, dataLengthBuffer,  this.data], headersBuffer.length + dataLengthBuffer.length + this.size);

  }

}

const getField = (fields: { [key: string]: Buffer }, key: string) => {
  if (fields[key] === undefined) {
    throw new Error(`Connection header is missing ${key}.`);
  }
  return fields[key].toString();
};

export class Connection extends Record {
  static opcode = 7;
  conn: number;
  topic: string;
  type: ?string;
  md5sum: ?string;
  messageDefinition: string;
  callerid: ?string;
  latching: ?boolean;
  reader: ?MessageReader;

  constructor(fields: { [key: string]: Buffer }) {
    super(fields);
    this.conn = fields.conn.readUInt32LE(0);
    this.topic = fields.topic.toString();
    this.type = undefined;
    this.md5sum = undefined;
    this.messageDefinition = "";
    this.callerid = undefined;
    this.latching = undefined;
  }

  parseData(buffer: Buffer) {
    const fields = extractFields(buffer);
    this.type = getField(fields, "type");
    this.md5sum = getField(fields, "md5sum");
    this.messageDefinition = getField(fields, "message_definition");
    if (fields.callerid !== undefined) {
      this.callerid = fields.callerid.toString();
    }
    if (fields.latching !== undefined) {
      this.latching = fields.latching.toString() === "1";
    }
  }

  composeRecord() {
    const headers = [
      {
        name: "conn",
        value: writeUInt32LE(this.conn)
      },
      {
        name: "topic",
        value: Buffer.alloc(this.topic.length, this.topic, "ascii")
      },
      {
        name: "op",
        value: writeUInt8(Connection.opcode)
      }
    ];
    const headersBuffer = composeHeader(headers);

    // connection data is composed similar to header
    const data = [];

    if (this.type === undefined || this.md5sum === undefined || this.messageDefinition === undefined) {
      throw new Error("Connection data is undefined");
    } else {

      const type = this.type ? this.type : "";
      const md5sum = this.md5sum ? this.md5sum : "";

      data.push({
        name: "topic",
        value: Buffer.alloc(this.topic.length, this.topic, "ascii")
      });
      data.push({
        name: "type",
        value: Buffer.alloc(type.length, type, "ascii")
      });
      data.push({
        name: "md5sum",
        value: Buffer.alloc(md5sum.length, md5sum, "ascii")
      });
      data.push({
        name: "message_definition",
        value: Buffer.alloc(this.messageDefinition.length, this.messageDefinition, "ascii")
      });
    }

    if (this.callerid !== undefined) {
      const callerid = this.callerid ? this.callerid : "";
      data.push({
        name: "callerid",
        value: Buffer.alloc(callerid.length, callerid, "ascii")
      });
    }
    if (this.latching !== undefined) {
      data.push({
        name: "latching",
        value: Buffer.alloc(1, this.latching ? "1" : "0", "ascii")
      });
    }
    const composedData = composeHeader(data);
    return Buffer.concat([headersBuffer, composedData], headersBuffer.length +  composedData.length);
  }
}

export class MessageData extends Record {
  static opcode = 2;
  conn: number;
  time: Time;
  data: Buffer;

  constructor(fields: { [key: string]: Buffer }) {
    super(fields);
    this.conn = fields.conn.readUInt32LE(0);
    this.time = extractTime(fields.time, 0);
  }

  parseData(buffer: Buffer) {
    this.data = buffer;
  }
}

export class IndexData extends Record {
  static opcode = 4;
  ver: number;
  conn: number;
  count: number;
  indices: Array<{ time: Time, offset: number }>;

  constructor(fields: { [key: string]: Buffer }) {
    super(fields);
    this.ver = fields.ver.readUInt32LE(0);
    this.conn = fields.conn.readUInt32LE(0);
    this.count = fields.count.readUInt32LE(0);
  }

  parseData(buffer: Buffer) {
    this.indices = [];
    for (let i = 0; i < this.count; i++) {
      this.indices.push({
        time: extractTime(buffer, i * 12),
        offset: buffer.readUInt32LE(i * 12 + 8),
      });
    }
  }

  composeRecord() {
    const headers = [
      {
        name: "ver",
        value: writeUInt32LE(this.ver)
      },
      {
        name: "conn",
        value: writeUInt32LE(this.conn)
      },
      {
        name: "count",
        value: writeUInt32LE(this.count)
      },
      {
        name: "op",
        value: writeUInt8(IndexData.opcode)
      }
    ];
    const headersBuffer = composeHeader(headers);

    let dataBuffer = Buffer.alloc(0);
    this.indices.forEach( (indexData) => {
      const indexDataBuffer = Buffer.alloc(12);
      indexDataBuffer.writeUInt32LE(indexData.time.sec, 0);
      indexDataBuffer.writeUInt32LE(indexData.time.nsec, 4);
      indexDataBuffer.writeUInt32LE(indexData.offset, 8);
      dataBuffer = Buffer.concat([dataBuffer, indexDataBuffer], dataBuffer.length + indexDataBuffer.length);
    });

    const dataLengthBuffer = writeUInt32LE(dataBuffer.length);

    return Buffer.concat([headersBuffer, dataLengthBuffer, dataBuffer], headersBuffer.length + dataLengthBuffer.length + dataBuffer.length);

  }
}

export class ChunkInfo extends Record {
  static opcode = 6;
  ver: number;
  chunkPosition: number;
  startTime: Time;
  endTime: Time;
  count: number;
  connections: Array<{ conn: number, count: number }>;
  nextChunk: ?ChunkInfo;

  constructor(fields: { [key: string]: Buffer }) {
    super(fields);
    this.ver = fields.ver.readUInt32LE(0);
    this.chunkPosition = readUInt64LE(fields.chunk_pos);
    this.startTime = extractTime(fields.start_time, 0);
    this.endTime = extractTime(fields.end_time, 0);
    this.count = fields.count.readUInt32LE(0);
  }

  parseData(buffer: Buffer) {
    this.connections = [];
    for (let i = 0; i < this.count; i++) {
      this.connections.push({
        conn: buffer.readUInt32LE(i * 8),
        count: buffer.readUInt32LE(i * 8 + 4),
      });
    }
  }

  composeRecord() {
    const headers = [
      {
        name: "ver",
        value: writeUInt32LE(this.ver)
      },
      {
        name: "chunk_pos",
        value: writeUInt64LE(this.chunkPosition)
      },
      {
        name: "start_time",
        value: composeTime(this.startTime)
      },
      {
        name: "end_time",
        value: composeTime(this.endTime)
      },
      {
        name: "count",
        value: writeUInt32LE(this.count)
      },
      {
        name: "op",
        value: writeUInt8(ChunkInfo.opcode)
      }
    ];
    const headersBuffer = composeHeader(headers);
    let dataBuffer = Buffer.alloc(0);

    this.connections.forEach((connection) => {
      const connBuffer = Buffer.alloc(8);
      connBuffer.writeUInt32LE(connection.conn, 0);
      connBuffer.writeUInt32LE(connection.count, 4);
      dataBuffer = Buffer.concat([dataBuffer, connBuffer], dataBuffer.length + connBuffer.length);
    });

    const dataLengthBuffer = writeUInt32LE(dataBuffer.length);

    return Buffer.concat([headersBuffer, dataLengthBuffer, dataBuffer], headersBuffer.length + dataLengthBuffer.length + dataBuffer.length);

  }
}
