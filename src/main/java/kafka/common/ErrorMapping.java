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

  public static final int UnknownCode = -1;
  public static final int NoError = 0;
  public static final int OffsetOutOfRangeCode = 1;
  public static final int InvalidMessageCode = 2;
  public static final int WrongPartitionCode = 3;
  public static final int InvalidFetchSizeCode = 4;

  public static Map<String,Integer> errorMap = new HashMap<>();
  static {
    errorMap.put(OffsetOutOfRangeException.class.getName(),OffsetOutOfRangeCode);
    errorMap.put(InvalidMessageException.class.getName(),InvalidMessageCode);
    errorMap.put(InvalidPartitionException.class.getName(),WrongPartitionCode);
    errorMap.put(InvalidMessageSizeException.class.getName(),InvalidFetchSizeCode);
  }

  public static int codeFor(String className){

    Integer code = errorMap.get(className);
    if(code == null) return UnknownCode;
    return code;
  }
}