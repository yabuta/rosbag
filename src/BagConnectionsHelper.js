// @flow
//
//  Copyright (c) 2018-present, GM Cruise LLC
//
//  This source code is licensed under the Apache License, Version 2.0,
//  found in the LICENSE file in the root directory of this source tree.
//  You may not use this file except in compliance with the License.

import { parseMessageDefinition } from "./parseMessageDefinition";
import { Connection, ChunkInfo } from "./record";

export type Topic = {|
  // Of ROS topic format, i.e. "/some/topic". We currently depend on this slashes format a bit in
  // `<MessageHistroy>`, though we could relax this and support arbitrary strings. It's nice to have
  // a consistent representation for topics that people recognize though.
  name: string,
  // Name of the datatype (see `type PlayerStateActiveData` for details).
  datatype: string,
  // The original topic name, if the topic name was at some point renamed, e.g. in
  // CombinedDataProvider.
  originalTopic?: string,
|};

type DatatypeDescription = {
  messageDefinition: string,
  type: ?string,
};

// Extract one big list of datatypes from the individual connections.
export function bagConnectionsToDatatypes(connections: $ReadOnlyArray<DatatypeDescription>) {
  const datatypes = {};
  connections.forEach((connection) => {
    const connectionTypes = parseMessageDefinition(connection.messageDefinition);
    connectionTypes.forEach(({ name, definitions }, index) => {
      // The first definition usually doesn't have an explicit name,
      // so we get the name from the connection.
      if (index === 0) {
        if (!connection.type) {
          throw new Error(`connection ${connection.messageDefinition} has no type`);
        }
        datatypes[connection.type] = definitions;
      } else if (name) {
        datatypes[name] = definitions;
      }
    });
  });
  return datatypes;
}

// Extract one big list of topics from the individual connections.
export function bagConnectionsToTopics(connections: $ReadOnlyArray<Connection>): Topic[] {
  // Use an object to deduplicate topics.
  const topics: { [string]: Topic } = {};
  connections.forEach((connection) => {
    const existingTopic = topics[connection.topic];
    const dataType = connection.type || "";
    if (existingTopic && existingTopic.datatype !== connection.type) {
      throw new Error(
        `duplicate topic with differing datatype.exist topic type is  ${existingTopic.datatype}, another type is ${dataType}`
      );
    }
    topics[connection.topic] = {
      name: connection.topic,
      datatype: dataType,
    };
  });
  // Satisfy flow by using `Object.keys` instead of `Object.values`
  return Object.keys(topics).map((topic) => topics[topic]);
}

export function bagConnectionsToMessageCount(
  chunkInfos: $ReadOnlyArray<ChunkInfo>,
  connections: $ReadOnlyArray<Connection>
) {
  const topics = {};
  let totalNum: number = 0;
  chunkInfos.forEach((chunkInfo) => {
    chunkInfo.connections.forEach((connection) => {
      const topicName = connections[connection.conn].topic;
      const existingTopic = topics[topicName];
      const dataType = connections[connection.conn].type || "";
      let topicCount = 0;
      if (existingTopic && existingTopic.datatype !== dataType) {
        throw new Error(
          `duplicate topic with differing datatype.exist topic type is  ${existingTopic.datatype}, another type is ${dataType}`
        );
      }
      if (existingTopic && Object.prototype.hasOwnProperty.call(existingTopic, "count")) {
        topicCount = existingTopic.count + connection.count;
        topics[topicName] += connection.count;
      } else {
        topicCount = connection.count;
      }
      topics[topicName] = {
        datatype: dataType,
        count: topicCount,
      };
      totalNum += connection.count;
    });
  });

  return { topics, totalNum };
}
