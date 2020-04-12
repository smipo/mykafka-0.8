/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package kafka.consumer;


import java.util.List;
import java.util.Map;

import kafka.serializer.Decoder;

public interface ConsumerConnector {


  /**
   *  Create a list of MessageStreams for each topic.
   *
   *  @param topicCountMap  a map of (topic, #streams) pair
   *  @return a map of (topic, list of  KafkaStream) pairs.
   *          The number of items in the list is #streams. Each stream supports
   *          an iterator over message/metadata pairs.
   */
  Map<String, List<KafkaStream<byte[], byte[]>>> createMessageStreams(Map<String, Integer> topicCountMap) throws InterruptedException;


  /**
   *  Create a list of MessageStreams for each topic.
   *
   *  @param topicCountMap  a map of (topic, #streams) pair
   *  @param keyDecoder Decoder to decode the key portion of the message
   *  @param valueDecoder Decoder to decode the value portion of the message
   *  @return a map of (topic, list of  KafkaStream) pairs.
   *          The number of items in the list is #streams. Each stream supports
   *          an iterator over message/metadata pairs.
   */
  public <K,V> Map<String, List<KafkaStream<K,V>>> createMessageStreams(
          Map<String, Integer> topicCountMap,  Decoder<K> keyDecoder,
          Decoder<V>  valueDecoder)throws InterruptedException;



  /**
   *  Create a list of message streams for all topics that match a given filter.
   *
   *  @param topicFilter Either a Whitelist or Blacklist TopicFilter object.
   *  @param numStreams Number of streams to return
   *  @param keyDecoder Decoder to decode the key portion of the message
   *  @param valueDecoder Decoder to decode the value portion of the message
   *  @return a list of KafkaStream each of which provides an
   *          iterator over message/metadata pairs over allowed topics.
   */
  public <K,V> List<KafkaStream<K,V>> createMessageStreamsByFilter(
          TopicFilter topicFilter, int numStreams, Decoder<K> keyDecoder,
          Decoder<V>  valueDecoder) throws Exception;

  /**
   *  Commit the offsets of all broker partitions connected by this connector.
   */
  public void commitOffsets();

  /**
   *  Shut down the connector
   */
  public void shutdown();
}
