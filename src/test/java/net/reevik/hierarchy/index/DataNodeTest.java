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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

class DataNodeTest {

  @Test
  void testSplitDataWithoutParent() {
    var dataNode = new DataNode();
    var dataRecord100 = new KeyData("100", createDataRecord("100"));
    var dataRecord200 = new KeyData("200", createDataRecord("200"));
    var dataRecord300 = new KeyData("300", createDataRecord("300"));
    dataNode.add(dataRecord100);
    dataNode.add(dataRecord200);
    dataNode.add(dataRecord300);

    var parent = dataNode.getParent();
    assertThat(parent).isNotNull();
    ifDataNodeThenRun(parent.getRightMost().node(), (n) -> {
      assertThat(n.getKeyDataSet()).contains(dataRecord200);
      assertThat(n.getKeyDataSet()).contains(dataRecord300);
    });
    assertThat(parent.getKeySet()).hasSize(1);
    var leftKey = parent.getKeySet().iterator().next();
    assertThat(leftKey.indexKey()).isEqualTo("200");
    ifDataNodeThenRun(leftKey.node(), (n) -> {
      var dataRecordSet = n.getKeyDataSet();
      assertThat(dataRecordSet).hasSize(1);
      assertThat(dataRecordSet).contains(dataRecord100);
    });
  }

  @Test
  void testLeftNodeSplitDataWithParent() {
    var dataNode = new DataNode();
    var dataRecord500 = createDataRecord("500");
    var dataRecord600 = createDataRecord("600");
    var dataRecord700 = createDataRecord("700");
    dataNode.add(new KeyData("500", dataRecord500));
    dataNode.add(new KeyData("600", dataRecord600));
    dataNode.add(new KeyData("700", dataRecord700));
    var parent = dataNode.getParent();
    var parentKeySet = parent.getKeySet();
    assertThat(parentKeySet).hasSize(1);
    var firstKeyInParentKeySet = parentKeySet.iterator().next();
    assertThat(firstKeyInParentKeySet.node()).isInstanceOf(DataNode.class);
    if (firstKeyInParentKeySet.node() instanceof DataNode dataNodeChild) {
      var dataRecordSet = dataNodeChild.getKeyDataSet();
      assertThat(dataRecordSet).hasSize(1);
      var dataRecord450 = createDataRecord("450");
      var dataRecord400 = createDataRecord("400");
      var dataRecord300 = createDataRecord("300");
      dataNodeChild.add(new KeyData("400", dataRecord400));
      dataNodeChild.add(new KeyData("300", dataRecord300));
      dataNodeChild.add(new KeyData("450", dataRecord450));
      assertThat(parent.getSize()).isEqualTo(2);
      assertThat(parent.getParent().getSize()).isEqualTo(2);
      assertThat(parentKeySet.stream().map(Key::indexKey).toList()).contains("600");
    }
  }

  private void ifDataNodeThenRun(Node node, Consumer<DataNode> consumer) {
    assertThat(node).isInstanceOf(DataNode.class);
    if (node instanceof DataNode dataNode) {
      consumer.accept(dataNode);
    }
  }

  private DataRecord createDataRecord(Object number) {
    return new DataRecord(number.toString().getBytes());
  }
}
