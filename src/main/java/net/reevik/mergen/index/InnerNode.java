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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiFunction;
import net.reevik.mergen.io.DiskAccessController;
import net.reevik.mergen.io.Page;
import net.reevik.mergen.io.Page.PageType;
import net.reevik.mergen.io.PageRef;

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
  List<DataRecord> doQuery(String indexQuery, BiFunction<List<KeyData>, DataNode, List<DataRecord>> operation) {
    for (var key : keySet) {
      if (indexQuery.compareTo(key.indexKey().toString()) < 0) {
        return key.node().doQuery(indexQuery, operation);
      }
    }
    return rightMost.node().doQuery(indexQuery, operation);
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

  void delete(String indexKey) {
    keySet.removeIf(key -> indexKey.compareTo(key.indexKey().toString()) < 0);
    if (indexKey.compareTo(rightMost.indexKey().toString()) >= 0) {
      rightMost = null;
      if (keySet.size() >= 2) {
        var lastKey = keySet.last();
        rightMost = lastKey;
        keySet.remove(lastKey);
      }
    }
    // If the current node has a single child.
    if (isUnbalanced() && hasParent()) {
      Key remainingKey = getLastKeyOrRightmost();
      if (getParent().isBinaryNode()) {
        Key parentKey = getParent().getLastKeyOrRightmost();
        if ((remainingKey.isRightMost() &&
            parentKey.indexKey().toString().compareTo(remainingKey.node().firstIndexKey().toString()) >= 0) ||
            parentKey.compareTo(remainingKey) > 0) {
          // merge the parent to the right branch.
          var rightBranchInner = (InnerNode) getParent().getRightMost().node();
          rightBranchInner.add(new Key(parentKey.indexKey(), remainingKey.node()));
          if (getParent().getParent() != null) {
            rightBranchInner.setParent(getParent().getParent());
          } else {
            rightBranchInner.setParent(null);
            notifyObservers(rightBranchInner);
          }
        } else {
          // merge the parent to the left branch.
          var leftBranchInner = (InnerNode) getParent().getLastKeyOrRightmost().node();
          leftBranchInner.add(new Key(parentKey.indexKey(), remainingKey.node()));
          if (getParent().getParent() != null) {
            leftBranchInner.setParent(getParent().getParent());
          } else {
            leftBranchInner.setParent(null);
            notifyObservers(leftBranchInner);
          }
          leftBranchInner.setParent(null);
          notifyObservers(leftBranchInner);
        }
      } else {
        Iterator<Key> iterator = getParent().getKeySet().iterator();
        while (iterator.hasNext()) {
          Key nextOnParent = iterator.next();
          if (nextOnParent.compareTo(remainingKey) > 0) {
            InnerNode rightBranchInner = iterator.hasNext() ?
                (InnerNode) iterator.next().node() :
                (InnerNode) getParent().getRightMost().node();
            rightBranchInner.add(new Key(nextOnParent.indexKey(), remainingKey.node()));
            getParent().getKeySet().remove(nextOnParent);
          }
        }
        if (remainingKey.compareTo(getParent().getRightMost()) >= 0) {
          Key parentLastKey = getParent().getLastChild();
          getParent().getKeySet().remove(parentLastKey);
          InnerNode leftBranch = (InnerNode) parentLastKey.node();
          getParent().setRightMost(new Key(leftBranch));
          leftBranch.add(new Key(parentLastKey.indexKey(), leftBranch.getRightMost().node()));
          leftBranch.setRightMost(new Key(remainingKey.node()));
        }
      }
    }
  }

  private boolean isBinaryNode() {
    return getTotalSize() == 2 && rightMost != null;
  }

  private Key getLastKeyOrRightmost() {
    return keySet.isEmpty() ? rightMost : keySet.first();
  }

  private boolean isUnbalanced() {
    return getTotalSize() == 1;
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
        key.node().setParent(leftNode);
      } else {
        break;
      }
    }
    leftNode.registerObservers(getNodeObservers());
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

  public Key getLastChild() {
    if (keySet.isEmpty()) { return null; }
    return keySet.last();
  }
}
