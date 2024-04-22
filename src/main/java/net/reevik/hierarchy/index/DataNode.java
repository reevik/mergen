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

import static net.reevik.hierarchy.index.DataRecord.createNew;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import net.reevik.hierarchy.io.DiskAccessController;
import net.reevik.hierarchy.io.Page;
import net.reevik.hierarchy.io.Page.PageType;
import net.reevik.hierarchy.io.PageRef;

public class DataNode extends Node implements Iterable<KeyData> {

  private final TreeSet<KeyData> keyDataSet = new TreeSet<>();

  public DataNode(PageRef pageRef, DiskAccessController diskAccessController) {
    super(pageRef, diskAccessController);
  }

  public DataNode(Page page, DiskAccessController diskAccessController) {
    super(page.getPageRef(), diskAccessController);
  }

  public static DataNode deserialize(Page page, DiskAccessController controller) {
    var dataNode = new DataNode(page.getPageRef(), controller);
    for (var nextCell : page) {
      var deserialize = KeyData.deserialize(nextCell, controller);
      dataNode.add(deserialize);
    }
    return dataNode;
  }

  @Override
  public Page serialize() {
    var page = new Page(this);
    keyDataSet.forEach(keyData -> page.appendCell(keyData.serialize()));
    return page;
  }

  public DataNode(DiskAccessController diskAccessController) {
    super(PageRef.empty(), diskAccessController);
  }

  public void add(DataEntity dataEntity) {
    add(new KeyData(dataEntity.indexKey(), createNew(dataEntity, getDiskAccessController())));
    markDirty();
  }

  public void add(KeyData dataRecord) {
    keyDataSet.add(dataRecord);
    markDirty();
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
    var leftNode = new DataNode(getDiskAccessController());
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
    return new Key(keyDataSet.getFirst().getIndexKey(), this);
  }

  private Key newLeftNodeKey(DataNode leftNode) {
    leftNode.setParent(getParent());
    return new Key(keyDataSet.getFirst().getIndexKey(), leftNode);
  }

  private InnerNode newRoot() {
    var root = new InnerNode(getDiskAccessController());
    root.registerObservers(getNodeObservers());
    notifyObservers(root);
    return root;
  }

  private int getMidPoint() {
    return (int) Math.ceil(keyDataSet.size() / 2.0d);
  }

  @Override
  Object getFirstIndexKey() {
    return keyDataSet.first().getIndexKey();
  }

  @Override
  void doUpsert(DataEntity dataEntity) {
    add(dataEntity);
  }

  @Override
  Set<DataRecord> doQuery(String query) {
    for (var dataKey : keyDataSet) {
      if (dataKey.getIndexKey().equals(query)) {
        return Set.of(dataKey.getDataRecord());
      }
    }
    return Set.of();
  }

  @Override
  int doGetSize() {
    return keyDataSet.size();
  }

  @Override
  Type getNodeType() {
    return Type.DATA;
  }

  public Set<KeyData> getKeyDataSet() {
    return keyDataSet;
  }

  @Override
  public PageRef persist() {
    var page = new Page(this);
    var offset = getDiskAccessController().write(page);
    var pageRef = new PageRef(offset);
    markSynced();
    return pageRef;
  }

  @Override
  public PageType getPageType() {
    return PageType.DATA_NODE;
  }

  @Override
  public Iterator<KeyData> iterator() {
    return keyDataSet.iterator();
  }

  public boolean contains(KeyData keyData) {
    return keyDataSet.contains(keyData);
  }
}
