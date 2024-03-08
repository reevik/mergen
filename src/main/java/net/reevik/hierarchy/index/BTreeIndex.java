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

public class BTreeIndex implements NodeObserver {

  static final int ORDER = 3;

  private Node root;

  public void upsert(DataRecord dataRecord) {
    if (root == null) {
      root = new DataNode();
      root.registerObserver(this);
    }
    root.upsert(dataRecord);
  }

  public Set<DataRecord> query(String indexKey) {
    return root.query(indexKey);
  }

  @Override
  public void onNewRoot(Node newRoot) {
    root = newRoot;
  }
}
