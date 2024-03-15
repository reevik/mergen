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

public enum DiskManager implements Closeable {
  DATA(new FileIO("Datafile")),
  INDEX(new FileIO("Indexfile"));

  DiskManager(FileIO file) {
    this.file = file;
  }

  private final FileIO file;

  @Override
  public void close() {
    INDEX.close();
    DATA.close();
  }

  public long write(byte[] buffer) {
    return file.write(buffer);
  }
}
