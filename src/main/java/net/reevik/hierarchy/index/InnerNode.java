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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import net.reevik.hierarchy.io.SerializableObject;

public class InnerNode extends Node implements SerializableObject, Iterable<Key> {
  private final TreeSet<Key> keySet = new TreeSet<>();
  private Key rightMost;

  public InnerNode(long pageOffset) {
    super(pageOffset);
  }

  public InnerNode() {
    super(-1L);
  }

  public void _upsert(DataEntity dataEntity) {
    var indexKeyAsStr = dataEntity.indexKey().toString();
    for (var key : keySet) {
      if (indexKeyAsStr.compareTo(key.indexKey().toString()) <= 0) {
        key.node()._upsert(dataEntity);
        return;
      }
    }
    rightMost.node()._upsert(dataEntity);
  }

  @Override
  Set<DataRecord> _query(String dataRecord) {
    for (var key : keySet) {
      if (dataRecord.compareTo(key.indexKey().toString()) < 0) {
        return key.node()._query(dataRecord);
      }
    }
    return rightMost.node()._query(dataRecord);
  }

  void add(Key key) {
    if (isRightMost(key)) {
      rightMost = key;
    } else {
      keySet.add(key);
    }

    int totalSize = getTotalSize();
    if (totalSize > BTreeIndex.ORDER) {
      split();
    }
  }

  boolean isRightMost(Key key) {
    return key.indexKey().toString().compareTo(key.node().firstIndexKey().toString()) <= 0;
  }

  private int getTotalSize() {
    int totalSize = keySet.size();
    if (rightMost != null) {
      totalSize++;
    }
    return totalSize;
  }

  private void split() {
    InnerNode leftNode = newLeftNode();
    removeItems(leftNode);
    createParentIfNotExists();
    getParent().add(newLeftNodeKey(leftNode));
    reattachFirstNodeTo(leftNode);
  }

  private InnerNode newLeftNode() {
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
    return leftNode;
  }

  private void removeItems(InnerNode leftNode) {
    keySet.removeAll(leftNode.keySet);
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
    return new Key(keySet.iterator().next().indexKey(), this);
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
  Object _firstIndexKey() {
    return keySet.first().indexKey();
  }

  @Override
  int _getSize() {
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

  @Override
  public void load() {
  }

  @Override
  public Iterator<Key> iterator() {
    var listView = new ArrayList<>(Arrays.asList(keySet.toArray(new Key[keySet.size()])));
    listView.add(rightMost);
    return listView.iterator();
  }
}
