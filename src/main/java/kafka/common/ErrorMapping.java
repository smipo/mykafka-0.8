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

package kafka.common;

import kafka.message.InvalidMessageException;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class ErrorMapping{

  public static final  ByteBuffer EmptyByteBuffer = ByteBuffer.allocate(0);

  public static final short UnknownCode = -1;
  public static final short NoError = 0;
  public static final short OffsetOutOfRangeCode = 1;
  public static final short InvalidMessageCode = 2;
  public static final short UnknownTopicOrPartitionCode = 3;
  public static final short InvalidFetchSizeCode = 4;
  public static final short LeaderNotAvailableCode  = 5;
  public static final short NotLeaderForPartitionCode  = 6;
  public static final short RequestTimedOutCode = 7;
  public static final short BrokerNotAvailableCode = 8;
  public static final short ReplicaNotAvailableCode = 9;
  public static final short MessageSizeTooLargeCode = 10;
  public static final short StaleControllerEpochCode = 11;


  public static Map<String,Short> errorMap = new HashMap<>();
  static {
    errorMap.put(OffsetOutOfRangeException.class.getName(),OffsetOutOfRangeCode);
    errorMap.put(InvalidMessageException.class.getName(),InvalidMessageCode);
    errorMap.put(InvalidPartitionException.class.getName(),UnknownTopicOrPartitionCode);
    errorMap.put(InvalidMessageSizeException.class.getName(),InvalidFetchSizeCode);

    errorMap.put(NotLeaderForPartitionException.class.getName(),LeaderNotAvailableCode);
    errorMap.put(LeaderNotAvailableException.class.getName(),NotLeaderForPartitionCode);
    errorMap.put(RequestTimedOutException.class.getName(),RequestTimedOutCode);
    errorMap.put(BrokerNotAvailableException.class.getName(),BrokerNotAvailableCode);
    errorMap.put(ReplicaNotAvailableException.class.getName(),ReplicaNotAvailableCode);
    errorMap.put(MessageSizeTooLargeException.class.getName(),MessageSizeTooLargeCode);
    errorMap.put(ControllerMovedException.class.getName(),StaleControllerEpochCode);
  }

  public static short codeFor(String className){
    Short code = errorMap.get(className);
    if(code == null) return UnknownCode;
    return code;
  }
  public static Throwable exceptionFor(Short code) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    for(Map.Entry<String, Short> entry : errorMap.entrySet()){
        if(entry.getValue().equals(code)){
          return (Throwable)Class.forName(entry.getKey()).newInstance();
        }
    }
    return new UnknownException(String.valueOf(code));
  }
}