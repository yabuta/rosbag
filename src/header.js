// Copyright (c) 2018-present, GM Cruise LLC

// This source code is licensed under the Apache License, Version 2.0,
// found in the LICENSE file in the root directory of this source tree.
// You may not use this file except in compliance with the License.

// @flow

import { extractFields, composeFields } from "./fields";
import { Record } from "./record";

// given a buffer parses out the record within the buffer
// based on the opcode type bit
export function parseHeader<T: Record>(buffer: Buffer, cls: Class<T> & { opcode: number }): T {
  const fields = extractFields(buffer);
  if (fields.op === undefined) {
    throw new Error("Header is missing 'op' field.");
  }
  const opcode = fields.op.readUInt8(0);
  if (opcode !== cls.opcode) {
    throw new Error(`Expected ${cls.name} (${cls.opcode}) but found ${opcode}`);
  }

  return new cls(fields);
}

export function composeHeader (headers: Array<any>): Buffer {
  const buffer = composeFields(headers);
  if (buffer.length === 0) {
    throw new Error("Fields is 0 byte");
  }

  const headerLengthBuffer = Buffer.alloc(4);
  headerLengthBuffer.writeUInt32LE(buffer.length, 0);

  return Buffer.concat([headerLengthBuffer, buffer], 4 + buffer.length);
}
