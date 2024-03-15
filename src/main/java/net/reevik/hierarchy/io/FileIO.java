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

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileIO implements Closeable {

  private final String fileName;
  private RandomAccessFile randomAccessFile;
  private FileChannel channel;
  private volatile long currentOffset;

  public FileIO(String fileName) {
    this.fileName = fileName;
    this.currentOffset = open();
  }

  private long open() {
    try {
      String mode = "rw";
      randomAccessFile = new RandomAccessFile(fileName, mode);
      channel = randomAccessFile.getChannel();
      return randomAccessFile.length();
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public long write(byte[] data) {
    try {
      synchronized (this) {
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

  public String getFileName() {
    return fileName;
  }

  @Override
  public void close() throws IOException {
    randomAccessFile.close();
  }
}
