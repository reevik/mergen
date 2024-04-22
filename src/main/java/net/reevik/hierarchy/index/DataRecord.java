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

import net.reevik.hierarchy.io.DiskAccessController;
import net.reevik.hierarchy.io.Page;
import net.reevik.hierarchy.io.Page.PageType;
import net.reevik.hierarchy.io.PageRef;
import net.reevik.hierarchy.io.SerializableObject;

public class DataRecord extends SerializableObject {
  private byte[] payload = new byte[0];
  private int size;

  public DataRecord(byte[] payload, PageRef pageRef, DiskAccessController diskAccessController) {
    super(pageRef, diskAccessController);
    this.payload = payload;
    markSynced();
  }

  public DataRecord(PageRef pageRef, DiskAccessController diskAccessController) {
    super(pageRef, diskAccessController);
    markUnsynced();
  }

  public DataRecord(byte[] payload, DiskAccessController diskAccessController) {
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
    return null;
  }

  @Override
  public PageType getPageType() {
    return PageType.DATA_RECORD;
  }

  @Override
  public String toString() {
    return "DataRecord{" + "payload=" + new String(payload) + '}';
  }

  public static DataRecord createSynced(PageRef ref, byte[] payload, DiskAccessController controller) {
    var dataRecord = new DataRecord(payload, ref, controller);
    dataRecord.markSynced();
    return dataRecord;
  }

  public static DataRecord createNew(DataEntity dataEntity, DiskAccessController controller) {
    var dataRecord = new DataRecord(dataEntity.payload(), controller);
    dataRecord.markDirty();
    return dataRecord;
  }

  public static DataRecord createNew(byte[] entityPayload, DiskAccessController controller) {
    var dataRecord = new DataRecord(entityPayload, controller);
    dataRecord.markDirty();
    return dataRecord;
  }
}
