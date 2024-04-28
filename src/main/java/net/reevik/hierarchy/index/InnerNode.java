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
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import net.reevik.hierarchy.io.DiskAccessController;
import net.reevik.hierarchy.io.Page;
import net.reevik.hierarchy.io.Page.PageType;
import net.reevik.hierarchy.io.PageRef;

public class InnerNode extends Node implements Iterable<Key> {

  private final TreeSet<Key> keySet = new TreeSet<>();
  private Key rightMost;

  public InnerNode(PageRef pageRef, DiskAccessController diskAccessController) {
    super(pageRef, diskAccessController);
  }

  public InnerNode(DiskAccessController diskAccessController) {
    super(PageRef.empty(), diskAccessController);
  }

  public static InnerNode deserialize(Page page, DiskAccessController controller) {
    InnerNode dataNode = new InnerNode(page.getPageRef(), controller);
    page.forEach(nextCell -> dataNode.add(Key.deserialize(nextCell, controller)));
    return dataNode;
  }

  @Override
  public void doUpsert(DataEntity dataEntity) {
    String indexKeyAsStr = dataEntity.indexKey().toString();
    for (var key : keySet) {
      if (indexKeyAsStr.compareTo(key.indexKey().toString()) <= 0) {
        key.node().doUpsert(dataEntity);
        return;
      }
    }
    rightMost.node().doUpsert(dataEntity);
  }

  @Override
  Set<DataRecord> doQuery(String dataRecord) {
    for (var key : keySet) {
      if (dataRecord.compareTo(key.indexKey().toString()) < 0) {
        return key.node().doQuery(dataRecord);
      }
    }
    return rightMost.node().doQuery(dataRecord);
  }

  void add(Key key) {
    if (key.isRightMost()) {
      rightMost = key;
    } else {
      keySet.add(key);
    }

    if (getTotalSize() >= BTreeIndex.ORDER) {
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
    createParentIfNotExists();
    extractLeftNode();
  }

  private void extractLeftNode() {
    InnerNode leftNode = new InnerNode(getDiskAccessController());
    int midPoint = getMidPoint();
    int counter = 0;
    for (var key : keySet) {
      if (++counter < midPoint) {
        leftNode.add(key);
      } else {
        break;
      }
    }
    removeItems(leftNode);
    attachToParent(leftNode);
  }

  private void attachToParent(InnerNode leftNode) {
    Object leftNodeParentKey = initRightmost(leftNode);
    leftNode.setParent(getParent());
    Key key = new Key(leftNodeParentKey, leftNode);
    getParent().add(key);
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

  private Object initRightmost(InnerNode leftNode) {
    Key keyToRemoveFromRightTree = keySet.getFirst();
    Key keyOftheLeftsRightMostNode = new Key(keyToRemoveFromRightTree.node());
    leftNode.setRightMost(keyOftheLeftsRightMostNode);
    keySet.remove(keyToRemoveFromRightTree);
    return keyToRemoveFromRightTree.indexKey();
  }

  private Key asRightMostKey() {
    return new Key(this);
  }

  private void createRoot() {
    InnerNode root = new InnerNode(getDiskAccessController());
    root.registerObservers(getNodeObservers());
    setParent(root);
    root.notifyObservers(root);
  }

  private int getMidPoint() {
    return (int) Math.ceil(getTotalSize() / 2.0d);
  }

  @Override
  Object getFirstIndexKey() {
    return keySet.first().indexKey();
  }

  @Override
  int doGetSize() {
    return getTotalSize();
  }

  @Override
  Type getNodeType() {
    return Type.INNER;
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
  public PageRef persist() {
    return PageRef.empty();
  }

  @Override
  public Page serialize() {
    var page = new Page(this);
    keySet.forEach(key -> page.appendCell(key.serialize()));
    page.appendCell(rightMost.serialize());
    return page;
  }

  @Override
  public Iterator<Key> iterator() {
    var listView = new ArrayList<>(Arrays.asList(keySet.toArray(new Key[keySet.size()])));
    listView.add(rightMost);
    return listView.iterator();
  }

  @Override
  public PageType getPageType() {
    return PageType.INNER_NODE;
  }

  public List<String> getIndexKeys() {
    return keySet.stream().map(Key::indexKey).map(Object::toString).toList();
  }
}
