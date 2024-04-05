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
package net.reevik.hierarchy.io;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class DiskFile implements FileIO, Closeable {
  public static final int PAGE_SIZE = 1024 * 16;
  private final String fileName;
  private RandomAccessFile randomAccessFile;
  private FileChannel channel;
  private volatile long currentOffset;

  public DiskFile(String fileName) {
    this.fileName = fileName;
  }

  public void init() {
    try {
      String mode = "rw";
      randomAccessFile = new RandomAccessFile(fileName, mode);
      randomAccessFile.seek(randomAccessFile.length());
      channel = randomAccessFile.getChannel();
      this.currentOffset = randomAccessFile.length();
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public long writeAt(byte[] data, long offset) {
    try {
      synchronized (this) {
        randomAccessFile.seek(offset);
        var buffer = ByteBuffer.allocate(data.length);
        buffer.put(data);
        buffer.flip();
        channel.write(buffer);
        currentOffset += data.length;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return currentOffset;
  }

  public long writeAt(byte[] data) {
    try {
      return writeAt(data, randomAccessFile.length());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public byte[] readBytes(long pageOffset, int pageSize) {
    try (var out = new ByteArrayOutputStream()) {
      channel.position(pageOffset);
      var bufferSize = pageSize;
      if (bufferSize > channel.size()) {
        bufferSize = (int) channel.size();
      }
      var buffer = ByteBuffer.allocate(bufferSize);
      while (channel.read(buffer) > 0) {
        out.write(buffer.array(), 0, buffer.position());
        buffer.clear();
      }
      return out.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public String getFileName() {
    return fileName;
  }

  @Override
  public void close() throws IOException {
    randomAccessFile.close();
  }
}
