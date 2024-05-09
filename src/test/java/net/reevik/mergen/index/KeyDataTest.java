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

import java.nio.ByteBuffer;
import net.reevik.mergen.io.DiskAccessController;
import net.reevik.mergen.io.PageRef;
import net.reevik.mikron.annotation.ManagedApplication;
import net.reevik.mikron.annotation.Wire;
import org.junit.jupiter.api.Test;

@ManagedApplication(packages = "net.reevik.hierarchy.index.*")
class KeyDataTest {
  private static final int OFFSET = 666;
  private static final String INDEX_KEY = "index-key";

  @Wire
  private DiskAccessController diskAccessController;

  @Test
  void testSerializeKeyData() {
    var dataRecord = new DataRecord(PageRef.of(OFFSET), diskAccessController);
    var keyData = new KeyData(INDEX_KEY, dataRecord);
    var serializedKeyData = keyData.serialize();
    var keyDataInBytes = serializedKeyData.array();
    var deserializedKeyData = KeyData.deserialize(ByteBuffer.wrap(keyDataInBytes),
        diskAccessController);
    assertThat(deserializedKeyData).isNotNull();
    assertThat(deserializedKeyData.indexKey()).isEqualTo(INDEX_KEY);
    assertThat(deserializedKeyData.dataRecord()).isNotNull();
    assertThat(deserializedKeyData.dataRecord().getPageRef()).isEqualTo(PageRef.of(OFFSET));
  }
}
