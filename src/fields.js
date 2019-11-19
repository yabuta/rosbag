// Copyright (c) 2018-present, GM Cruise LLC

// This source code is licensed under the Apache License, Version 2.0,
// found in the LICENSE file in the root directory of this source tree.
// You may not use this file except in compliance with the License.

// @flow

import type { Time } from "./types";

// reads through a buffer and extracts { [key: string]: value: string }
// pairs - the buffer is expected to have length prefixed utf8 strings
// with a '=' separating the key and value
export function extractFields(buffer: Buffer) {
  if (buffer.length < 4) {
    throw new Error("Header fields are truncated.");
  }

  let i = 0;
  const fields: { [key: string]: Buffer } = {};

  while (i < buffer.length) {
    const length = buffer.readInt32LE(i);
    i += 4;

    if (i + length > buffer.length) {
      throw new Error("Header fields are corrupt.");
    }

    const field = buffer.slice(i, i + length);
    const index = field.indexOf("=");
    if (index === -1) {
      throw new Error("Header field is missing equals sign.");
    }

    fields[field.slice(0, index).toString()] = field.slice(index + 1);
    i += length;
  }

  return fields;
}

// reads a Time object out of a buffer at the given offset
export function extractTime(buffer: Buffer, offset: number): Time {
  const sec = buffer.readUInt32LE(offset);
  const nsec = buffer.readUInt32LE(offset + 4);
  return { sec, nsec };
}


// compose header data to bite
export function composeFields(headers: Array<any>) {

  let fieldsLength = 0;

  headers.forEach( (header) => {
    fieldsLength += 4 + header.name.length + 1 + header.value.length;
  });

  const buffer = Buffer.alloc(fieldsLength);
  let offset = 0;

  headers.forEach( (header) => {
    const fieldLength = 4 + header.name.length + 1 + header.value.length;

    const index = buffer.writeUInt32LE(fieldLength - 4, offset);
    if (index !== offset + 4) {
      throw new Error("Missing to write Header length to buffer." + index);
    }

    const nameLength = buffer.write(header.name, offset + 4, header.name.length);
    if (nameLength !== header.name.length) {
      throw new Error("Missing to write Header name to buffer.");
    }

    const equalLength = buffer.write("=", offset + 4 + header.name.length, 1);
    if (equalLength !== 1) {
      throw new Error("Missing to write '=' to buffer.");
    }

    const valueLength = header.value.copy(buffer, offset + 4 + header.name.length + 1, 0, header.value.length);
    if (valueLength !== header.value.length) {
      throw new Error("Missing to write Header value to buffer.");
    }
    offset += fieldLength;
  });

  if (fieldsLength !== offset) {
    throw new Error("Written buffer size is not equal to calculated size.");
  }

  return { buffer, fieldsLength };

}
