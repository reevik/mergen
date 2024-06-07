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

import java.util.List;
import net.reevik.mergen.io.DiskController;
import net.reevik.mikron.annotation.Configurable;
import net.reevik.mikron.annotation.Initialize;
import net.reevik.mikron.annotation.Managed;
import net.reevik.mikron.annotation.Wire;

/**
 * A B+Tree implementation of indexing. The order is the number of index keys each inner or data
 * node can hold. {@link BTreeIndex} is a {@link NodeObserver} implementation. Whenever the root
 * node needs to be updated, e.g. in case of node split, the {@link BTreeIndex} will be notified.
 *
 * @author Erhan Bagdemir
 */
@Managed(name = "index")
public class BTreeIndex implements NodeObserver {

  static final int ORDER = 4;

  /**
   * Root node of the B+Tree index.
   */
  private Node root;

  @Configurable(name = "order")
  private int order;

  @Wire
  private DiskController diskAccessController;

  @Initialize
  public void init() {
    this.root = resolveRoot();
  }

  private Node resolveRoot() {
    // TODO When the index file is loaded to the main memory, we first resolve the root node,
    //  which is used to process index queries.
    return null;
  }

  /**
   * Inserts or update an existing record.
   *
   * @param dataEntity An instance of {@link DataEntity}.
   */
  public void upsert(DataEntity dataEntity) {
    if (root == null) {
      root = new DataNode(diskAccessController);
      root.registerObserver(this);
    }
    root.doUpsert(dataEntity);
  }

  /**
   * Query the index by index key.
   *
   * @param indexKey An index key.
   * @return {@link DataRecord} instances found for the index key.
   */
  public List<DataRecord> query(String indexKey) {
    return root.doQuery(indexKey,
        (keyData, dataNode) -> keyData.stream().map(KeyData::dataRecord).toList());
  }

  /**
   * Delete the data records by index key.
   *
   * @param indexKey An index key.
   * @return A list of {@link DataRecord}s.
   */
  public List<DataRecord> delete(String indexKey) {
    return root.doQuery(indexKey, (keyData, dataNode) ->
        keyData.stream().map(kd -> dataNode.delete(kd.indexKey().toString())).toList());
  }

  /**
   * A callback method which is called, whenever a new root get created.
   *
   * @param newRoot {@link Node}, which is the new root of the index.
   */
  @Override
  public void onNewRoot(Node newRoot) {
    root = newRoot;
  }
}
