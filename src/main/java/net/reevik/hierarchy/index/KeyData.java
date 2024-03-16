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

import java.util.Objects;

public class KeyData implements Comparable<KeyData> {

  private final Object indexKey;
  private final DataRecord dataRecord;

  public KeyData(Object indexKey, DataRecord dataRecord) {
    this.indexKey = indexKey;
    this.dataRecord = dataRecord;
  }

  public byte[] getBytes() {
    var dataRecordOffsetInBytes = getBytesOf(dataRecord.getOffset());
    var indexKeyInBytes = indexKey.toString().getBytes();
    int totalRecordSize = indexKeyInBytes.length;
    totalRecordSize += dataRecordOffsetInBytes.length;
    var totalRecordSizeInBytes = getBytesOf(totalRecordSize);
    totalRecordSize += totalRecordSizeInBytes.length;
    var payload = new byte[totalRecordSize];
    int newStart = append(payload, totalRecordSizeInBytes, 0);
    newStart = append(payload, dataRecordOffsetInBytes, newStart);
    append(payload, indexKeyInBytes, newStart);
    return payload;
  }

  @Override
  public int compareTo(KeyData o) {
    return indexKey.toString().compareTo(o.indexKey.toString());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    KeyData keyData = (KeyData) o;
    return Objects.equals(indexKey, keyData.indexKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(indexKey);
  }

  @Override
  public String toString() {
    return "KeyData{" +
        "indexKey=" + indexKey +
        ", dataRecord=" + dataRecord +
        '}';
  }

  public Object getIndexKey() {
    return indexKey;
  }

  public DataRecord getDataRecord() {
    return dataRecord;
  }
}
