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

import static net.reevik.mergen.index.DataRecord.createNew;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiFunction;
import net.reevik.mergen.io.DiskController;
import net.reevik.mergen.io.Page;
import net.reevik.mergen.io.Page.PageType;
import net.reevik.mergen.io.PageRef;

public class DataNode extends Node implements Iterable<KeyData> {

  private final TreeSet<KeyData> keyDataSet = new TreeSet<>();

  public DataNode(PageRef pageRef, DiskController diskAccessController) {
    super(pageRef, diskAccessController);
  }

  public DataNode(Page page, DiskController diskAccessController) {
    super(page.getPageRef(), diskAccessController);
  }

  public DataNode(DiskController diskAccessController) {
    super(PageRef.empty(), diskAccessController);
  }

  public static DataNode deserialize(Page page, DiskController controller) {
    DataNode dataNode = new DataNode(page.getPageRef(), controller);
    page.forEach(nextCell -> dataNode.add(KeyData.deserialize(nextCell, controller)));
    return dataNode;
  }

  public Page serialize() {
    Page page = new Page(this);
    for (var keyData : keyDataSet) {
      ByteBuffer serialize = keyData.serialize();
      if (!page.hasSpace(serialize.capacity())) {
        getDiskAccessController().append(page);
        page = new Page(this);
      }
      page.appendCell(keyData.serialize());
    }
    return page;
  }

  public DataNode add(DataEntity dataEntity) {
    add(new KeyData(dataEntity.indexKey(), createNew(dataEntity, getDiskAccessController())));
    markDirty();
    return this;
  }

  public DataNode add(KeyData keyData) {
    keyDataSet.add(keyData);
    markDirty();
    if (keyDataSet.size() >= BTreeIndex.ORDER - 1) {
      split();
    }
    return this;
  }

  public DataNode add(Object indexKey, DataRecord dataRecord) {
    return add(new KeyData(indexKey, dataRecord));
  }

  DataRecord delete(String indexKey) {
    KeyData deletedKeyData = null;
    for (var keyData : keyDataSet) {
      if (keyData.indexKey().equals(indexKey)) {
        keyDataSet.remove(keyData);
        deletedKeyData = keyData;
        break;
      }
    }
    if (keyDataSet.isEmpty() && hasParent()) {
      getParent().deleteNodeAndBalanceBy(indexKey);
    }
    return deletedKeyData != null ? deletedKeyData.dataRecord() : null;
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
      root.add(toRightMostKey());
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
    return new Key(this);
  }

  private Key newLeftNodeKey(DataNode leftNode) {
    leftNode.setParent(getParent());
    leftNode.registerObservers(getNodeObservers());
    return new Key(keyDataSet.getFirst().indexKey(), leftNode);
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
    return keyDataSet.first().indexKey();
  }

  @Override
  void doUpsert(DataEntity dataEntity) {
    add(dataEntity);
  }

  @Override
  List<DataRecord> doQuery(String query,
      BiFunction<List<KeyData>, DataNode, List<DataRecord>> operation) {
    List<KeyData> results = keyDataSet.stream().filter(keyData -> keyData.indexKey().equals(query))
        .toList();
    return operation.apply(results, this);
  }

  @Override
  int doGetSize() {
    return keyDataSet.size();
  }

  @Override
  Type getNodeType() {
    return Type.DATA;
  }

  @Override
  public PageRef persist() {
    // TODO Precondition checks for the cells bigger than page size.
    // Alternatively we can split the cell into multiple pages.
    Page page = new Page(this);
    for (var keyData : keyDataSet) {
      ByteBuffer serialize = keyData.serialize();
      if (!page.hasSpace(serialize.capacity())) {
        PageRef pageRef = getDiskAccessController().append(page);
        page = new Page(this);
        // we persist the slotted pages backwards, i.e., the last cell's offset (the right most)
        // will be the node's page offset.
        page.setNextSlottedPage(pageRef);
      }
      page.appendCell(keyData.serialize());
    }
    PageRef pageRef = getDiskAccessController().append(page);
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

  public Set<KeyData> getKeyDataSet() {
    return keyDataSet;
  }

  public boolean contains(KeyData keyData) {
    return keyDataSet.contains(keyData);
  }
}
