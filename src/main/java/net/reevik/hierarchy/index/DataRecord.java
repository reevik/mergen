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

import net.reevik.hierarchy.io.DiskManager;
import net.reevik.hierarchy.io.SerializableObject;

public class DataRecord implements SerializableObject {

  private final byte[] payload;
  private long offset;

  public DataRecord(byte[] payload) {
    this.payload = payload;
    persist();
  }

  public byte[] getPayload() {
    return payload;
  }

  @Override
  public long getOffset() {
    return offset;
  }

  @Override
  public byte[] getBytes() {
    var totalRecordSize = payload.length;
    byte[] totalRecordSizeInBytes = getBytesOf(totalRecordSize);
    byte[] byteRepresentation = new byte[totalRecordSizeInBytes.length + totalRecordSize];
    int newStartPos = append(byteRepresentation, totalRecordSizeInBytes, 0);
    append(byteRepresentation, payload, newStartPos);
    return byteRepresentation;
  }

  @Override
  public void load() {
  }

  @Override
  public void persist() {
    offset = DiskManager.DATA.append(getBytes());
  }

  @Override
  public String toString() {
    return "DataRecord{" + "payload=" + new String(payload) + '}';
  }
}
