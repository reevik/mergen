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

import java.util.Set;
import java.util.TreeSet;

public class InnerNode extends Node {

  private final TreeSet<Key> keySet = new TreeSet<>();
  private Key rightMost;

  public void upsert(DataRecord dataRecord) {
    var indexKeyAsStr = dataRecord.indexKey().toString();
    for (var key : keySet) {
      if (indexKeyAsStr.compareTo(key.indexRangeKey().toString()) <= 0) {
        key.node().upsert(dataRecord);
        return;
      }
    }
    rightMost.node().upsert(dataRecord);
  }

  @Override
  Set<DataRecord> query(String record) {
    for (var key : keySet) {
      if (record.compareTo(key.indexRangeKey().toString()) < 0) {
        return key.node().query(record);
      }
    }
    return rightMost.node().query(record);
  }

  void add(Key key) {
    if (key.isRightMost()) {
      rightMost = key;
    } else {
      keySet.add(key);
    }

    int totalSize = getTotalSize();
    if (totalSize > BTreeIndex.ORDER) {
      split();
    }
  }

  private int getTotalSize() {
    int totalSize = keySet.size();
    if (rightMost != null) {
      totalSize++;
    }
    return totalSize;
  }

  private void split() {
    var midPoint = getMidPoint();
    var leftNode = new InnerNode();
    var counter = 0;
    for (var key : keySet) {
      if (++counter < midPoint) {
        leftNode.add(key);
      } else {
        break;
      }
    }
    keySet.removeAll(leftNode.keySet);
    createParentIfNotExists();
    getParent().add(newLeftNodeKey(leftNode));
    reattachFirstNodeTo(leftNode);
  }

  private void createParentIfNotExists() {
    if (!hasParent()) {
      createRoot();
      getParent().add(asRightMostKey());
    }
  }

  private void reattachFirstNodeTo(InnerNode leftNode) {
    var keyToRemoveFromRightTree = keySet.iterator().next();
    leftNode.setRightMost(keyToRemoveFromRightTree);
    keySet.remove(keyToRemoveFromRightTree);
  }

  public Key asRightMostKey() {
    return new Key(keySet.iterator().next().indexRangeKey(), this);
  }

  private Key newLeftNodeKey(InnerNode leftNode) {
    leftNode.setParent(getParent());
    return new Key(keySet.iterator().next(), leftNode);
  }

  private void createRoot() {
    var root = new InnerNode();
    root.registerObservers(getNodeObservers());
    setParent(root);
    root.notifyObservers(root);
  }

  private int getMidPoint() {
    return (int) Math.ceil(getTotalSize() / 2.0d);
  }

  @Override
  Object firstIndexKey() {
    return keySet.first().indexRangeKey();
  }

  @Override
  int getSize() {
    return getTotalSize();
  }

  public Set<Key> getKeySet() {
    return keySet;
  }

  public Key getRightMost() {
    return rightMost;
  }

  public void setRightMost(Key rightMost) {
    this.rightMost = rightMost;
  }
}