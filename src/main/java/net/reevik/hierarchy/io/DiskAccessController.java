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

import static net.reevik.hierarchy.io.DiskFile.PAGE_SIZE;

import java.io.Closeable;
import java.io.IOException;
import net.reevik.mikron.annotation.Configurable;
import net.reevik.mikron.annotation.Initialize;
import net.reevik.mikron.annotation.Managed;

@Managed(name = "diskAccessController")
public class DiskAccessController implements Closeable {

  @Configurable(name = "fileName")
  private String fileName;

  private FileIO file;

  @Initialize
  public void init() {
    this.file = new DiskFile(fileName);
  }

  public long append(Page page) {
    return file.writeAt(page.getPageBuffer());
  }

  public Page read(PageRef pageRef) {
    var bytes = file.readBytes(pageRef.pageOffset(), PAGE_SIZE);
    return new Page(bytes, pageRef);
  }

  public long write(Page page) {
    return file.writeAt(page.getPageBuffer(), page.getPageRef().pageOffset());
  }

  @Override
  public void close() throws IOException {

  }
}
