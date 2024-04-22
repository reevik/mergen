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
import net.reevik.hierarchy.io.DiskAccessController;
import net.reevik.mikron.annotation.ManagedApplication;
import net.reevik.mikron.annotation.Wire;
import org.junit.jupiter.api.Test;

@ManagedApplication(packages = "net.reevik.hierarchy.index.*")
class BTreeIndexTest {

  @Wire
  private BTreeIndex bTreeIndex;

  @Test
  void indexQuery() {
    bTreeIndex.upsert(createRecord("500", "500"));
    bTreeIndex.upsert(createRecord("400", "400"));
    bTreeIndex.upsert(createRecord("600", "600"));
    bTreeIndex.upsert(createRecord("700", "700"));
    bTreeIndex.upsert(createRecord("300", "300"));
    bTreeIndex.upsert(createRecord("450", "450"));
    Set<DataRecord> query = bTreeIndex.query("450");
  }

  static DataEntity createRecord(String indexKey, String payload) {
    return new DataEntity(indexKey, payload.getBytes());
  }
}
