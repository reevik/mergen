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

import java.nio.ByteBuffer;
import java.util.Objects;

public class Key implements Comparable<Key> {

  private final Object indexKey;
  private final Node node;

  public Key(Object indexKey, Node node) {
    this.indexKey = indexKey;
    this.node = node;
  }

  @Override
  public int compareTo(Key o) {
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
    Key key = (Key) o;
    return Objects.equals(indexKey, key.indexKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(indexKey);
  }

  @Override
  public String toString() {
    return "Key{" +
        "indexKey=" + indexKey +
        '}';
  }

  public Object indexKey() {
    return indexKey;
  }

  public Node node() {
    return node;
  }

  public byte[] getBytes() {
    var nodeOffsetInBytes = getBytesOf(node.getPageOffset());
    var indexKeyInBytes = indexKey.toString().getBytes();
    int totalRecordSize = indexKeyInBytes.length;
    totalRecordSize += nodeOffsetInBytes.length;
    var totalRecordSizeInBytes = getBytesOf(totalRecordSize);
    totalRecordSize += totalRecordSizeInBytes.length;
    var payload = new byte[totalRecordSize];
    int newStartPos = append(payload, totalRecordSizeInBytes, 0);
    newStartPos = append(payload, nodeOffsetInBytes, newStartPos);
    append(payload, nodeOffsetInBytes, newStartPos);
    return payload;
  }
}
