// Copyright (c) 2018-present, GM Cruise LLC

// This source code is licensed under the Apache License, Version 2.0,
// found in the LICENSE file in the root directory of this source tree.
// You may not use this file except in compliance with the License.

// @flow

import type { Filelike } from "./types";
import { MessageData } from "./record";




// the high level rosbag interface
// create a new bag by calling:
// `const bag = await Bag.open('./path-to-file.bag')` in node or
// `const bag = await Bag.open(files[0])` in the browser
//
// after that you can consume messages by calling
// `await bag.readMessages({ topics: ['/foo'] },
//    (result) => console.log(result.topic, result.message))`
export default class BagCopy {
  _file: Filelike;

  // you can optionally create a bag manually passing in a bagReader instance
  constructor(filelike: Filelike) {
    this._file = filelike;
  }

  async initializeWriter(fileHandler) {
    this._writer = await fileHandler.createWriter({
      keepExistingData: true
    });
  }


  fileRead(startPos, length, callback) {
    this._file.read(startPos, length, (error: Error | null, buffer?: Buffer) => {
      if (error || !buffer) {
        throw new Error("Missing read rosbag. start position: " + startPos + "  length: " + length);
      }
      return callback(error, buffer);
    });
  }

  fileReadAsync(startPos, length) {
    return new Promise((resolve, reject) => {
      this.fileRead(startPos, length, (err: Error | null, buffer?: Buffer) => (err || !buffer ? reject(err) : resolve(buffer)));
    });
  }

  async writeBagToNewFile(buffer, startPos) {
    await this._writer.write(startPos, buffer);
    await this._writer.close();
  }

  async bagCopy() {
    const fileSize = this._file.size() || 0;
    const length  = 100;
    for (let startPos = 0; startPos < fileSize; startPos += length) {
      const buffer = await this.fileReadAsync(startPos, length);
      await this.writeBagToNewFile(buffer, startPos);
    }
  }
}
