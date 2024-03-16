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

import static net.reevik.hierarchy.index.IndexUtils.getBytesOf;

import java.util.Set;
import java.util.TreeSet;
import net.reevik.hierarchy.io.DiskManager;
import net.reevik.hierarchy.io.SerializableObject;

public class DataNode extends Node implements SerializableObject {

  public static final long NON_EXISTING_OFFSET = -1L;
  private final TreeSet<KeyData> keyDataSet = new TreeSet<>();
  private long offset = -1;

  public void add(DataEntity dataEntity) {
    add(new KeyData(dataEntity.indexKey(), new DataRecord(dataEntity.payload())));
  }

  public void add(KeyData dataRecord) {
    keyDataSet.add(dataRecord);
    if (keyDataSet.size() >= BTreeIndex.ORDER) {
      split();
    }
  }

  private void split() {
    var leftNode = newLeftNode();
    removeItems(leftNode);
    createRootIfNotExists();
    getParent().add(newLeftNodeKey(leftNode));
  }

  private void createRootIfNotExists() {
    if (!hasParent()) {
      var root = newRoot();
      setParent(root);
      root.add(this.toRightMostKey());
    }
  }

  private void removeItems(DataNode leftNode) {
    keyDataSet.removeAll(leftNode.keyDataSet);
  }

  // Splitting the existing node into two parts at the mid-point.
  private DataNode newLeftNode() {
    var midPoint = getMidPoint();
    var leftNode = new DataNode();
    var c = 0;
    for (var dataRecord : keyDataSet) {
      if (++c < midPoint) {
        leftNode.add(dataRecord);
      } else {
        break;
      }
    }
    return leftNode;
  }

  public Key toRightMostKey() {
    return new Key(keyDataSet.iterator().next().getIndexKey(), this);
  }

  private Key newLeftNodeKey(DataNode leftNode) {
    leftNode.setParent(getParent());
    return new Key(keyDataSet.iterator().next().getIndexKey(), leftNode);
  }

  private InnerNode newRoot() {
    var root = new InnerNode();
    root.registerObservers(getNodeObservers());
    notifyObservers(root);
    return root;
  }

  private int getMidPoint() {
    return (int) Math.ceil(keyDataSet.size() / 2.0d);
  }

  @Override
  Object firstIndexKey() {
    return keyDataSet.first().getIndexKey();
  }

  @Override
  void upsert(DataEntity dataEntity) {
    add(dataEntity);
  }

  @Override
  Set<DataRecord> query(String query) {
    for (var dataKey : keyDataSet) {
      if (dataKey.getIndexKey().equals(query)) {
        return Set.of(dataKey.getDataRecord());
      }
    }
    return Set.of();
  }

  @Override
  int getSize() {
    return keyDataSet.size();
  }

  @Override
  public long getOffset() {
    return offset;
  }

  public Set<KeyData> getKeyDataSet() {
    return keyDataSet;
  }

  @Override
  public byte[] getBytes() {
    return new byte[0];
  }

  private byte[] getParentsInBytes(InnerNode parent) {
    if (parent != null) {
      return getBytesOf(parent.getOffset());
    }
    return getBytesOf(NON_EXISTING_OFFSET);
  }


  @Override
  public void load() {
    if (offset < 0) {
      throw new IllegalStateException("The node haven't been persisted yet.");
    }
  }

  @Override
  public void persist() {
    if (offset < 0) {
      offset = DiskManager.INDEX.append(getBytes());
    } else {
      DiskManager.INDEX.writeAt(getBytes(), offset);
    }
  }
}
