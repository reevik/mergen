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

import static net.reevik.hierarchy.index.IndexUtils.append;
import static net.reevik.hierarchy.index.IndexUtils.getBytesOf;

import net.reevik.hierarchy.io.PageRef;
import net.reevik.hierarchy.io.SerializableObject;

public class DataRecord extends SerializableObject {
  private byte[] payload = new byte[0];
  private int size;

  public DataRecord(byte[] payload, PageRef pageRef) {
    super(pageRef);
    this.payload = payload;
    markSynced();
  }

  public DataRecord(PageRef pageRef) {
    super(pageRef);
    markUnsynced();
  }

  public DataRecord(byte[] payload) {
    super(new PageRef(-1));
    this.payload = payload;
    markDirty();
  }

  public byte[] getPayload() {
    if (isUnsynced()) {
      load();
    }
    return payload;
  }

  public byte[] getBytes() {
    var totalRecordSize = getPayload().length;
    byte[] totalRecordSizeInBytes = getBytesOf(totalRecordSize);
    byte[] byteRepresentation = new byte[totalRecordSizeInBytes.length + totalRecordSize];
    int newStartPos = append(byteRepresentation, totalRecordSizeInBytes, 0);
    append(byteRepresentation, payload, newStartPos);
    return byteRepresentation;
  }

  @Override
  public void load() {
    markSynced();
  }

  @Override
  public long persist() {
    markSynced();
    return 0;
  }

  @Override
  public String toString() {
    return "DataRecord{" + "payload=" + new String(payload) + '}';
  }

  public static DataRecord createUnloaded(PageRef ref) {
    var dataRecord = new DataRecord(ref);
    dataRecord.markUnsynced();
    return dataRecord;
  }

  public static DataRecord createSynced(PageRef ref, byte[] payload) {
    var dataRecord = new DataRecord(payload, ref);
    dataRecord.markSynced();
    return dataRecord;
  }

  public static DataRecord createNew(DataEntity dataEntity) {
    var dataRecord = new DataRecord(dataEntity.payload());
    dataRecord.markDirty();
    return dataRecord;
  }

  public static DataRecord createNew(byte[] entityPayload) {
    var dataRecord = new DataRecord(entityPayload);
    dataRecord.markDirty();
    return dataRecord;
  }
}
