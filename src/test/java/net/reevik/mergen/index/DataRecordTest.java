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

import net.reevik.mergen.io.DiskController;
import net.reevik.mergen.io.Page;
import net.reevik.mergen.io.PageRef;
import net.reevik.mikron.annotation.ManagedApplication;
import net.reevik.mikron.annotation.Wire;
import org.junit.jupiter.api.Test;

@ManagedApplication(packages = "net.reevik.hierarchy.*")
class DataRecordTest {

  @Wire
  private DiskController diskAccessController;

  @Test
  void testSerializeDataRecords() {
    String payload = "test123";
    DataRecord dataRecord = new DataRecord(payload.getBytes(), PageRef.of(-1),
        diskAccessController);
    Page dataRecordPage = dataRecord.serialize();
    DataRecord deserializedDataRecord = DataRecord.deserialize(dataRecordPage,
        diskAccessController);
    assertThat(deserializedDataRecord.getPayload()).isNotNull();
    String deserializedPayload = new String(deserializedDataRecord.getPayload());
    assertThat(deserializedPayload).isEqualTo(payload);
  }
}
