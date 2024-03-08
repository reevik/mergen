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

public class DataNode extends Node {

  private final TreeSet<DataRecord> dataRecordSet = new TreeSet<>();

  public void add(DataRecord dataRecord) {
    dataRecordSet.add(dataRecord);
    if (dataRecordSet.size() >= BTreeIndex.ORDER) {
      split();
    }
  }

  private void split() {
    var midPoint = getMidPoint();
    var leftNode = new DataNode();
    var counter = 0;
    for (var dataRecord : dataRecordSet) {
      if (++counter < midPoint) {
        leftNode.add(dataRecord);
      } else {
        break;
      }
    }
    dataRecordSet.removeAll(leftNode.dataRecordSet);
    if (!hasParent()) {
      createRoot();
      getParent().add(asRightMostKey());
    }
    getParent().add(newLeftNodeKey(leftNode));
  }

  public Key asRightMostKey() {
    return new Key(dataRecordSet.iterator().next().indexKey(), this);
  }

  private Key newLeftNodeKey(DataNode leftNode) {
    leftNode.setParent(getParent());
    return new Key(dataRecordSet.iterator().next().indexKey(), leftNode);
  }

  private void createRoot() {
    var root = new InnerNode();
    root.registerObservers(getNodeObservers());
    setParent(root);
    notifyObservers(root);
  }

  private int getMidPoint() {
    return (int) Math.ceil(dataRecordSet.size() / 2.0d);
  }

  @Override
  Object firstIndexKey() {
    return dataRecordSet.first().indexKey();
  }

  @Override
  void upsert(DataRecord dataRecord) {
    add(dataRecord);
  }

  @Override
  Set<DataRecord> query(String query) {
    for (var record : dataRecordSet) {
      if (record.indexKey().equals(query)) {
        return Set.of(record);
      }
    }
    return Set.of();
  }

  @Override
  int getSize() {
    return dataRecordSet.size();
  }

  public Set<DataRecord> getDataRecordSet() {
    return dataRecordSet;
  }
}