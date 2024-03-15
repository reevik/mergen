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
import java.util.Objects;
import net.reevik.hierarchy.io.DiskManager;
import net.reevik.hierarchy.io.SerializableObject;

public class DataRecord implements Comparable<DataRecord>, SerializableObject {
  private final Object indexKey;
  private final byte[] payload;
  private final long offset;

  public DataRecord(Object indexKey, byte[] payload) {
    this.indexKey = indexKey;
    this.payload = payload;
    this.offset = DiskManager.DATA.write(serialize());
  }


  @Override
  public int compareTo(DataRecord o) {
    return indexKey.toString().compareTo(o.indexKey.toString());
  }

  public Object getIndexKey() {
    return indexKey;
  }

  public byte[] getPayload() {
    return payload;
  }

  @Override
  public long offset() {
    return offset;
  }

  @Override
  public byte[] serialize() {
    var indexKeyInBytes = indexKey.toString().getBytes();
    var totalRecordSize = indexKeyInBytes.length + payload.length;
    byte[] totalRecordSizeInBytes = getBytesOf(totalRecordSize);
    byte[] byteRepresentation = new byte[totalRecordSizeInBytes.length + totalRecordSize];

    System.arraycopy(totalRecordSizeInBytes, 0, byteRepresentation, 0,
        totalRecordSizeInBytes.length);
    System.arraycopy(indexKeyInBytes, 0, byteRepresentation, totalRecordSizeInBytes.length,
        indexKeyInBytes.length);
    System.arraycopy(payload, 0, byteRepresentation, indexKeyInBytes.length, payload.length);

    return byteRepresentation;
  }

  private static byte[] getBytesOf(long totalRecordSize) {
    var buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(totalRecordSize);
    return buffer.array();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DataRecord that = (DataRecord) o;
    return Objects.equals(indexKey, that.indexKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(indexKey);
  }

  @Override
  public String toString() {
    return "DataRecord{" + "indexKey=" + indexKey + '}';
  }
}
