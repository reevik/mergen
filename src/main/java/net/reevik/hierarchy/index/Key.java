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

import static net.reevik.hierarchy.index.Key.KeyType.RMN;

import java.nio.ByteBuffer;
import java.util.Objects;
import net.reevik.hierarchy.index.Node.Type;
import net.reevik.hierarchy.io.DiskAccessController;
import net.reevik.hierarchy.io.PageRef;

public class Key implements Comparable<Key> {

  enum KeyType {
    RMN
  }

  private final Object indexKey;
  private final Node node;

  public Key(Object indexKey, Node node) {
    this.indexKey = indexKey;
    this.node = node;
  }

  public Key(Node node) {
    this.indexKey = RMN;
    this.node = node;
  }

  public boolean isRightMost() {
    return indexKey.equals(RMN);
  }

  public ByteBuffer serialize() {
    var indexKeyInBytes = indexKey.toString().getBytes();
    var buffer = ByteBuffer.allocate(indexKeyInBytes.length + Long.BYTES + Integer.BYTES);
    buffer.putInt(node.getNodeType().ordinal());
    buffer.putLong(node.getPageRef().pageOffset());
    buffer.put(indexKeyInBytes);
    return buffer;
  }

  public static Key deserialize(ByteBuffer byteBuffer, DiskAccessController controller) {
    var nodeType = Node.Type.values()[byteBuffer.getInt()];
    var nodeOffset = byteBuffer.getLong();
    var indeyKeySize = byteBuffer.capacity() - Long.BYTES - Integer.BYTES;
    var indexKey = new byte[indeyKeySize];
    byteBuffer.get(indexKey, 0, indeyKeySize);
    Node node;
    if (nodeType.equals(Type.INNER)) {
      node = new InnerNode(PageRef.of(nodeOffset), controller);
    } else {
      node = new DataNode(PageRef.of(nodeOffset), controller);
    }
    return new Key(new String(indexKey), node);
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
}
