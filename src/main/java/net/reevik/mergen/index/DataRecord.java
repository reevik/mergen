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
package net.reevik.mergen.index;

import java.nio.ByteBuffer;
import java.util.Iterator;
import net.reevik.mergen.io.DiskController;
import net.reevik.mergen.io.Page;
import net.reevik.mergen.io.Page.PageType;
import net.reevik.mergen.io.PageRef;
import net.reevik.mergen.io.SerializableObject;

public class DataRecord extends SerializableObject {
  private byte[] payload = new byte[0];
  private int size;

  public DataRecord(byte[] payload, PageRef pageRef, DiskController diskAccessController) {
    super(pageRef, diskAccessController);
    this.payload = payload;
    markSynced();
  }

  public DataRecord(PageRef pageRef, DiskController diskAccessController) {
    super(pageRef, diskAccessController);
    markUnsynced();
  }

  public DataRecord(byte[] payload, DiskController diskAccessController) {
    super(new PageRef(-1), diskAccessController);
    this.payload = payload;
    markDirty();
  }

  public byte[] getPayload() {
    return payload;
  }

  @Override
  public PageRef persist() {
    markSynced();
    return PageRef.empty();
  }

  @Override
  public Page serialize() {
    var page = new Page(this);
    page.appendCell(ByteBuffer.wrap(payload));
    return page;
  }

  @Override
  public PageType getPageType() {
    return PageType.DATA_RECORD;
  }

  @Override
  public String toString() {
    return "DataRecord{" + "payload=" + new String(payload) + '}';
  }

  public static DataRecord createSynced(PageRef ref, byte[] payload, DiskController controller) {
    var dataRecord = new DataRecord(payload, ref, controller);
    dataRecord.markSynced();
    return dataRecord;
  }

  public static DataRecord createNew(DataEntity dataEntity, DiskController controller) {
    var dataRecord = new DataRecord(dataEntity.payload(), controller);
    dataRecord.markDirty();
    return dataRecord;
  }

  public static DataRecord createNew(byte[] entityPayload, DiskController controller) {
    var dataRecord = new DataRecord(entityPayload, controller);
    dataRecord.markDirty();
    return dataRecord;
  }

  public static DataRecord deserialize(Page page, DiskController controller) {
    Iterator<ByteBuffer> iterator = page.iterator();
    if (iterator.hasNext()) {
      return new DataRecord(iterator.next().array(), page.getPageRef(), controller);
    }
    return null;
  }
}
