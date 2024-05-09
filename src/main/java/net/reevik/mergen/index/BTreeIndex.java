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
import java.util.Set;
import net.reevik.mergen.io.DiskAccessController;
import net.reevik.mikron.annotation.Dynamic;
import net.reevik.mikron.annotation.Initialize;
import net.reevik.mikron.annotation.Managed;
import net.reevik.mikron.annotation.Wire;

@Managed
public class BTreeIndex implements NodeObserver {

  static final int ORDER = 4;
  private Node root;

  @Wire
  private DiskAccessController diskAccessController;

  @Initialize
  public void init() {
    this.root = resolveRoot();
  }

  private Node resolveRoot() {
    return null;
  }

  public void upsert(DataEntity dataEntity) {
    if (root == null) {
      root = new DataNode(diskAccessController);
      root.registerObserver(this);
    }
    root.doUpsert(dataEntity);
  }

  public List<DataRecord> query(String indexKey) {
    return root.doQuery(indexKey, (keyData, dataNode) -> keyData.dataRecord());
  }

  public List<DataRecord> delete(String indexKey) {
    return root.doQuery(indexKey, (keyData, dataNode) -> dataNode.delete(indexKey));
  }

  @Override
  public void onNewRoot(Node newRoot) {
    root = newRoot;
  }

  public void load() {
  }
}
