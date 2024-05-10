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

import static org.assertj.core.api.Assertions.assertThat;

import net.reevik.mikron.annotation.ManagedApplication;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@ManagedApplication(packages = "net.reevik.hierarchy.index.*")
class BTreeIndexTest {

  private final BTreeIndex bTreeIndex = new BTreeIndex();

  @BeforeEach
  public void setUp() {
    bTreeIndex.upsert(createRecord("500", "500"));
    bTreeIndex.upsert(createRecord("400", "400"));
    bTreeIndex.upsert(createRecord("600", "600"));
    bTreeIndex.upsert(createRecord("700", "700"));
    bTreeIndex.upsert(createRecord("300", "300"));
    bTreeIndex.upsert(createRecord("450", "450"));
  }

  @Test
  void testIndexQuery() {
    assertThat(bTreeIndex.query("450").stream()
        .map(dr -> new String(dr.getPayload())).toList()).contains("450");
    assertThat(bTreeIndex.query("700").stream()
        .map(dr -> new String(dr.getPayload())).toList()).contains("700");
  }

  @Test
  void testIndexDelete() {
    assertThat(bTreeIndex.query("450").stream()
        .map(dr -> new String(dr.getPayload())).toList()).contains("450");
    bTreeIndex.delete("450");
    bTreeIndex.delete("400");
    assertThat(bTreeIndex.query("450")).isEmpty();
    assertThat(bTreeIndex.query("400")).isEmpty();
  }

  @Test
  void testIndexDeleteAllExceptOne() {
    assertThat(bTreeIndex.query("450").stream()
        .map(dr -> new String(dr.getPayload())).toList()).contains("450");
    bTreeIndex.delete("300");
    assertThat(bTreeIndex.query("300")).isEmpty();
    bTreeIndex.delete("400");
    assertThat(bTreeIndex.query("400")).isEmpty();
    bTreeIndex.delete("500");
    assertThat(bTreeIndex.query("500")).isEmpty();
    bTreeIndex.delete("600");
    assertThat(bTreeIndex.query("600")).isEmpty();
    bTreeIndex.delete("700");
    assertThat(bTreeIndex.query("700")).isEmpty();
    assertThat(bTreeIndex.query("450").stream()
        .map(dr -> new String(dr.getPayload())).toList()).contains("450");
  }

  static DataEntity createRecord(String indexKey, String payload) {
    return new DataEntity(indexKey, payload.getBytes());
  }
}
