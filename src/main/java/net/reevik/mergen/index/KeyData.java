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
import net.reevik.mergen.io.DiskController;
import net.reevik.mergen.io.PageRef;

public record KeyData(Object indexKey, DataRecord dataRecord) implements Comparable<KeyData> {

  public ByteBuffer serialize() {
    var indexKeyInBytes = indexKey.toString().getBytes();
    var buffer = ByteBuffer.allocate(indexKeyInBytes.length + Long.BYTES);
    buffer.putLong(dataRecord.getPageRef().pageOffset());
    buffer.put(indexKeyInBytes);
    return buffer;
  }

  public static KeyData deserialize(ByteBuffer byteBuffer, DiskController controller) {
    long dataRecordOffset = byteBuffer.getLong();
    int indeyKeySize = byteBuffer.capacity() - Long.BYTES;
    byte[] indexKey = new byte[indeyKeySize];
    byteBuffer.get(indexKey, 0, indeyKeySize);
    return new KeyData(new String(indexKey), new DataRecord(new PageRef(dataRecordOffset),
        controller));
  }

  @Override
  public int compareTo(KeyData o) {
    return indexKey.toString().compareTo(o.indexKey.toString());
  }
}
