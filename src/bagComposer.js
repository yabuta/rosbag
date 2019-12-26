// Copyright (c) 2018-present, GM Cruise LLC

// This source code is licensed under the Apache License, Version 2.0,
// found in the LICENSE file in the root directory of this source tree.
// You may not use this file except in compliance with the License.

// @flow


import type { ReadOptions } from "./bag";
import { BagHeader, Chunk, ChunkInfo, Connection, IndexData, MessageData } from "./record";
import BagReader from "./BagReader";

interface ComposeBuffer {buffer: Array<Buffer>, length: number}
const HEADER_OFFSET = 13;

interface ChunkReadResult {
  chunk: Chunk;
  indices: IndexData[];
}

// BagReader is a lower level interface for reading specific sections & chunks
// from a rosbag file - generally it is consumed through the Bag class, but
// can be useful to use directly for efficiently accessing raw pieces from
// within the bag
export default class BagComposer {

  reader: BagReader;
  header: BagHeader;
  connections: { [conn: number]: Connection };
  chunkInfos: ChunkInfo[];

  constructor(reader: BagReader, header: BagHeader, connections: { [conn: number]: Connection }, chunkInfos: ChunkInfo[]) {
    this.reader = reader;
    this.header = header;
    this.connections = connections;
    this.chunkInfos = chunkInfos;
  }

  createChunk(messageDatas: MessageData[]) {
    const indices = {};
    let offset = 0;
    const messageDataBuffers = messageDatas.map( (messageData) => {
      if (Array.isArray(indices[messageData.conn])) {
        indices[messageData.conn].indices.push({
          time: messageData.time,
          offset: offset
        });
      } else {
        indices[messageData.conn] = {
          conn: messageData.conn,
          indices: [{
            time: messageData.time,
            offset: offset
          }]};
      }
      const buffer =  messageData.composeRecord();
      offset += buffer.length;
      return buffer;
    });

    const chunkDataBuffer =  Buffer.concat(messageDataBuffers);

    const chunkBuffer = Chunk.ComposeRecordFromValue("none", offset, chunkDataBuffer);

    const indicesBuffers = [];
    for (const conn in indices) {
      if (indices.hasOwnProperty(conn)) {
        indicesBuffers.push(IndexData.ComposeRecordFromValue(1, indices[conn].conn, indices[conn].indices.length, indices[conn].indices));
      }
    }
    return [chunkBuffer, indicesBuffers];
  }


  composeRosbagHeader(): ComposeBuffer {
    const bagHeaderBuffer =  this.header.composeRecord();
    return { buffer: [bagHeaderBuffer], length: bagHeaderBuffer.length};
  }

  composeChunks(chunkResults: Array<ChunkReadResult>): ComposeBuffer {
    let percentage = 0;
    const chunkBuffers = [];
    let chunkBufferSize = 0;
    chunkResults.forEach( (result, index) => {
      if (Math.floor((index * 100) / chunkResults.length ) !== percentage) {
        percentage = Math.floor((index * 100) / chunkResults.length );
        console.log(percentage);
      }
      const { chunk, indices } = result;
      const chunkBuffer = chunk.composeRecord();
      chunkBufferSize += chunkBuffer.length;
      chunkBuffers.push(chunkBuffer);
      indices.forEach((indexData) => {
        const indexDataBuffer = indexData.composeRecord();
        chunkBufferSize += indexDataBuffer.length;
        chunkBuffers.push(indexDataBuffer);
      });
    });

    return {buffer: chunkBuffers, length: chunkBufferSize};
  }

  composeConnections(): ComposeBuffer {
    const connectionBuffers = [];
    let connectionBuffersSize = 0;
    Object.keys(this.connections)
      .forEach((conn: any) => {
        const connectionBuffer = this.connections[conn].composeRecord();
        // const connections = this.reader.readRecordFromBuffer(connectionBuffer, 0, Connection);
        connectionBuffersSize += connectionBuffer.length;
        connectionBuffers.push(connectionBuffer);
      });
    return { buffer: connectionBuffers, length: connectionBuffersSize };
  }


  composeChunkInfos(): ComposeBuffer {
    const chunkInfoBuffers = [];
    let chunkInfoBuffersSize = 0;
    this.chunkInfos.forEach( (chunkInfo) => {
      const chunkInfoBuffer = chunkInfo.composeRecord();
      chunkInfoBuffersSize += chunkInfoBuffer.length;
      chunkInfoBuffers.push(chunkInfoBuffer);
    });
    return { buffer: chunkInfoBuffers, length: chunkInfoBuffersSize };
  }

  async getRosbagBuffer(opts: ReadOptions): Promise<Buffer> {

    let rosbagBuffers = [];
    let bufferSize = 0;

    const versionBuffer = Buffer.alloc(HEADER_OFFSET);
    // write version
    const versionLength = versionBuffer.write("#ROSBAG V2.0\n", 0, HEADER_OFFSET);
    if (versionLength !== HEADER_OFFSET) {
      throw new Error("Missing to write version to buffer.");
    }
    rosbagBuffers.push(versionBuffer);
    bufferSize += versionBuffer.length;

    const rosbagHeaderBuffer = this.composeRosbagHeader();
    rosbagBuffers = rosbagBuffers.concat(rosbagHeaderBuffer.buffer);
    bufferSize += rosbagHeaderBuffer.length;

    const chunkResults = await this.readChunk(opts);
    const chunkBuffer = this.composeChunks(chunkResults);
    rosbagBuffers = rosbagBuffers.concat(chunkBuffer.buffer);
    bufferSize += chunkBuffer.length;

    const connectionsBuffer = this.composeConnections();
    rosbagBuffers = rosbagBuffers.concat(connectionsBuffer.buffer);
    bufferSize += connectionsBuffer.length;

    const chunkInfosBuffer = this.composeChunkInfos();
    rosbagBuffers = rosbagBuffers.concat(chunkInfosBuffer.buffer);
    bufferSize += chunkInfosBuffer.length;

    return Buffer.concat(rosbagBuffers, bufferSize);
  }


  async readChunk(opts: ReadOptions): Promise<Array<ChunkReadResult>> {
    const { decompress = {} } = opts;
    const chunkResults = [];

    for (let i = 0; i < this.chunkInfos.length; i++) {
      const info = this.chunkInfos[i];
      const chunkResult = await this.reader.readChunkAsync(
        info,
        decompress
      );
      chunkResults.push(chunkResult);
    }

    return chunkResults;
  }




}
