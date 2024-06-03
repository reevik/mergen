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
package net.reevik.mergen.io;

import static net.reevik.mergen.index.DataRecord.createNew;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import net.reevik.mergen.index.DataNode;
import net.reevik.mergen.index.DataRecord;
import net.reevik.mergen.index.InnerNode;
import net.reevik.mergen.index.Key;
import net.reevik.mergen.index.KeyData;
import net.reevik.mikron.annotation.ManagedApplication;
import net.reevik.mikron.annotation.ManagedTest;
import net.reevik.mikron.annotation.Wire;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

@ManagedApplication(packages = {"net.reevik.mergen.*"})
@ManagedTest
public class PageTest {

  @Wire(name = "diskAccessController")
  private DiskController diskAccessController;

  @AfterEach
  void tearDown() {
    diskAccessController.purge();
  }

  @Test
  void testSerializeAndDeserializeDataNodes() {
    var dataNode = new DataNode(diskAccessController);
    var dataRecord500 = createDataRecord("500");
    var dataRecord600 = createDataRecord("600");
    var dataRecord700 = createDataRecord("700");
    // first item will be moved into the left split as the order=3.
    dataNode.add(new KeyData("500", dataRecord500));
    dataNode.add(new KeyData("600", dataRecord600));
    dataNode.add(new KeyData("700", dataRecord700));
    PageRef persistedPageRef = dataNode.persist();
    assertThat(persistedPageRef.pageOffset()).isEqualTo(0);
    Page readPage = diskAccessController.read(persistedPageRef);
    DataNode deserializedDataNode = DataNode.deserialize(readPage, diskAccessController);
    assertThat(deserializedDataNode.getSize()).isEqualTo(2);
    List<String> listOfIndexKeys =
            deserializedDataNode.getKeyDataSet().stream()
                .map(KeyData::indexKey)
                .map(Object::toString)
                .toList();
    assertThat(listOfIndexKeys).contains("600");
    assertThat(listOfIndexKeys).contains("700");
  }

  @Test
  void testSerializeAndDeserializeInnerNodes() {
    var innerNode = new InnerNode(diskAccessController);
    // first item will be moved into the left split as the order=3.
    innerNode.setRightMost(new Key(new InnerNode(diskAccessController)));
    innerNode.add(new Key("600", new InnerNode(diskAccessController)));
    innerNode.add(new Key("700", new InnerNode(diskAccessController)));
    PageRef persistedPageRef = innerNode.persist();
    assertThat(persistedPageRef.pageOffset()).isEqualTo(0);
    var readPage = diskAccessController.read(persistedPageRef);
    var deserializedNode = InnerNode.deserialize(readPage, diskAccessController);
    assertThat(deserializedNode.getSize()).isEqualTo(2);
    assertThat(deserializedNode.getIndexKeys()).contains("600");
    assertThat(deserializedNode.getIndexKeys()).contains("700");
  }

  private DataRecord createDataRecord(Object number) {
    return createNew(number.toString().getBytes(), diskAccessController);
  }
}
