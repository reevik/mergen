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
import net.reevik.mergen.io.DiskController;
import net.reevik.mergen.io.Page;
import net.reevik.mergen.io.Page.PageType;
import net.reevik.mergen.io.PageRef;

/**
 * <p>
 * Inner nodes are the ones, which are the ascendants of leaf nodes including the root node. If
 * their capacity is reached, they will split into two equal-sized inner node while increasing the
 * number of children in their parents. If the parent also reaches its max. capacity, it also splits
 * into two equal-sized inner node, and the split process will be terminated, either one of the
 * ascendants have enough capacity for the new inner-child or a new root has to be created.
 * </p>
 * <p>
 * An inner node is either a root node or one of the descendants of the current root. If the root
 * node needs to split, then a new root node will be created and two inner nodes, which are the
 * product of the split, become the direct children of the new root node.
 * </p>
 * <p>
 * Every inner node keeps track of its direct children in a {@link Set} of {@link Key}, which are
 * references to the children for an index range key. The set is sorted by the index range key, and
 * each reference is a link to a child, of which index range keys are smaller than reference's.
 * Children of an inner node is either another inner node or a {@link DataNode}.
 * </p>
 * <p>
 * The right most reference points to the child node, of which index range key is equals and bigger
 * than the previous sibling. It means, the number of children of an inner node is one greater than
 * the index range key. All index range keys are maintaining references to children, of which index
 * range keys are smaller than their parents, but the last children that is referenced by the right
 * most reference, is greater than all other siblings.
 * </p>
 *
 * @author Erhan Bagdemir
 * @version 1.0
 */
public class InnerNode extends Node implements Iterable<Key> {

  /**
   * A sorted set of references to the children.
   */
  private final TreeSet<Key> keySet = new TreeSet<>();

  /**
   * The right most key, which is a reference to a child, of which index range keys are greater than
   * its sibling.
   */
  private Key rightMost;

  public InnerNode(PageRef pageRef, DiskController diskAccessController) {
    super(pageRef, diskAccessController);
  }

  public InnerNode(DiskController diskAccessController) {
    super(PageRef.empty(), diskAccessController);
  }

  public static InnerNode deserialize(Page page, DiskController controller) {
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
  List<DataRecord> doQuery(String indexQuery,
      BiFunction<List<KeyData>, DataNode, List<DataRecord>> operation) {
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

  /**
   * <p>
   * The method deletes a node by index key and rebalances the tree if a node remains unbalanced.
   * Unbalanced nodes need to be eliminated by reassigning their remaining child to the unbalanced
   * one's parent and moving the parent to the sibling:
   * </p>
   * <pre>
   *      100                  50  100              100                 100 150
   *    50   150 (*)   =>     A   B   160     (*) 50   150     =>     40   A   B
   *   A  B     160                             40    A   B
   * </pre>
   * <p>
   * The nodes which are marked with "*" are unbalanced ones with a single child, so they are to be
   * eliminated. Unbalanced nodes' children will be assigned to their parent, and the parent is to
   * join the sibling.
   * </p>
   *
   * @param indexKey Index key of the node to be deleted.
   */
  void deleteNodeAndRebalanceBy(String indexKey) {
    // Remove the node by the index key in the current node. The current now may be left in
    // unbalanced state, which will be handled below.
    if (removeNodeKeyBy(indexKey) && hasParent()) {
      var remainingKey = getLastKeyOrRightmost();
      var parent = getParent();
      if (parent.isBinaryNode()) {
        var parentKey = parent.getLastKeyOrRightmost();
        if (isRemainingInTheLeftBranch(remainingKey, parentKey)) {
          mergeParentToRightSibling(parentKey, remainingKey);
        } else {
          mergeParentToLeftSibling(parentKey, remainingKey);
        }
      } else {
        removeKeyOnABalancedNode(remainingKey, parent);
      }
    }
  }

  private void removeKeyOnABalancedNode(Key remainingKey, InnerNode node) {
    if (node.isUnbalanced()) {
      throw new IllegalArgumentException("The node must be balanced.");
    }
    Iterator<Key> iterator = node.getKeySet().iterator();
    while (iterator.hasNext()) {
      Key nextKey = iterator.next();
      if (nextKey.compareTo(remainingKey) > 0) {
        InnerNode rightBranchInner = iterator.hasNext() ?
            (InnerNode) iterator.next().node() :
            (InnerNode) node.getRightMost().node();
        rightBranchInner.add(new Key(nextKey.indexKey(), remainingKey.node()));
        node.getKeySet().remove(nextKey);
      }
    }
    if (remainingKey.compareTo(node.getRightMost()) >= 0) {
      Key lastKey = node.getLastChild();
      node.getKeySet().remove(lastKey);
      InnerNode leftBranch = (InnerNode) lastKey.node();
      node.setRightMost(new Key(leftBranch));
      leftBranch.add(new Key(lastKey.indexKey(), leftBranch.getRightMost().node()));
      leftBranch.setRightMost(new Key(remainingKey.node()));
    }
  }

  // There are two possibilities here, 1) remaining key is the right most. In this case, the
  // first child of the right most must be compared with the parent because the right most key
  // doesn't contain index keys to compare. 2) remaining key is a left-side branch with an index
  // key, so we can compare it with the parent to determine if the remaining node resides in the
  // left branch of the parent.
  private boolean isRemainingInTheLeftBranch(Key remainingKey, Key parentKey) {
    return (remainingKey.isRightMost() &&
        parentKey.indexKey().toString().compareTo(remainingKey.node().firstIndexKey().toString())
            >= 0) ||
        parentKey.compareTo(remainingKey) > 0;
  }

  // Move the parent node to the left sibling of the remaining, because the unbalanced node is in
  // the right branch of the parent, i.e., the unbalanced node mustn't survive on its own. The
  // parent becomes the new parent of the unbalanced one's child while moving it to the left
  // sibling.
  private void mergeParentToLeftSibling(Key parentKey, Key remainingKey) {
    InnerNode leftBranchInner = (InnerNode) getParent().getLastKeyOrRightmost().node();
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

  // Same as mergeParentToLeftSibling but moves the parent to the right branch.
  private void mergeParentToRightSibling(Key parentKey, Key remainingKey) {
    InnerNode rightBranchInner = (InnerNode) getParent().getRightMost().node();
    rightBranchInner.add(new Key(parentKey.indexKey(), remainingKey.node()));
    if (getParent().getParent() != null) {
      rightBranchInner.setParent(getParent().getParent());
    } else {
      rightBranchInner.setParent(null);
      notifyObservers(rightBranchInner);
    }
  }

  private boolean removeNodeKeyBy(String indexKey) {
    keySet.removeIf(key -> indexKey.compareTo(key.indexKey().toString()) < 0);
    if (indexKey.compareTo(rightMost.indexKey().toString()) >= 0) {
      rightMost = null;
      if (keySet.size() >= 2) {
        var lastKey = keySet.last();
        rightMost = lastKey;
        keySet.remove(lastKey);
      }
    }

    return isUnbalanced();
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
    if (keySet.isEmpty()) {
      return null;
    }
    return keySet.last();
  }
}
