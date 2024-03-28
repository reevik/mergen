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
package net.reevik.hierarchy.io;

import static net.reevik.hierarchy.index.DataRecord.createNew;
import static net.reevik.hierarchy.index.DataRecord.createSynced;
import static net.reevik.hierarchy.io.FileIO.PAGE_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

import net.reevik.hierarchy.index.DataNode;
import net.reevik.hierarchy.index.DataRecord;
import net.reevik.hierarchy.index.KeyData;
import org.junit.jupiter.api.Test;

public class PageTest {

  @Test
  void testSerializeAndDeserialize() {
    var dataNode = new DataNode();
    var dataRecord500 = createDataRecord("500");
    var dataRecord600 = createDataRecord("600");
    dataNode.add(new KeyData("500", dataRecord500));
    dataNode.add(new KeyData("600", dataRecord600));
    var page0 = new Page(dataNode);
    byte[] pageBuffer = page0.getPageBuffer();
    assertThat(pageBuffer.length).isEqualTo(PAGE_SIZE);
    var page1 = new Page(pageBuffer);
    var deserializedDataNode = page1.deserialize();
    assertThat(deserializedDataNode.contains(new KeyData("500", dataRecord500))).isTrue();
    assertThat(deserializedDataNode.contains(new KeyData("600", dataRecord600))).isTrue();
  }

  private DataRecord createDataRecord(Object number) {
    return createNew(number.toString().getBytes());
  }
}
