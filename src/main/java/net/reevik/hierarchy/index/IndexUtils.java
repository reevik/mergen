/*
 * Copyright (c) 2024 Erhan Bagdemir. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.reevik.hierarchy.index;

import java.nio.ByteBuffer;

public class IndexUtils {

  public static long bytesToLong(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.put(bytes);
    buffer.flip();
    return buffer.getLong();
  }

  public static short bytesToShort(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES);
    buffer.put(bytes);
    buffer.flip();
    return buffer.getShort();
  }

  public static int bytesToInt(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
    buffer.put(bytes);
    buffer.flip();
    return buffer.getInt();
  }

  public static byte[] getBytesOf(long total) {
    var buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(total);
    return buffer.array();
  }

  public static byte[] getBytesOf(short value) {
    var buffer = ByteBuffer.allocate(Short.BYTES);
    buffer.putShort(value);
    return buffer.array();
  }

  public static byte[] getBytesOf(int value) {
    var buffer = ByteBuffer.allocate(Integer.BYTES);
    buffer.putInt(value);
    return buffer.array();
  }

  public static int append(byte[] buffer, byte[] source, int start) {
    System.arraycopy(source, 0, buffer, start, source.length);
    return start + source.length;
  }

  public static int appendBackward(byte[] buffer, byte[] source, int start) {
    System.arraycopy(source, 0, buffer, start, source.length);
    return start - source.length;
  }
}
