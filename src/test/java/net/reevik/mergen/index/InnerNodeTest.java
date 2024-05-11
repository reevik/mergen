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

import static net.reevik.mergen.index.Key.KeyType.RMN;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import net.reevik.mergen.io.DiskController;
import net.reevik.mergen.io.PageRef;
import net.reevik.mikron.annotation.ManagedApplication;
import net.reevik.mikron.annotation.Wire;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

@ManagedApplication(packages = "net.reevik.hierarchy.*")
class InnerNodeTest {

  @Wire
  private DiskController diskAccessController;

  /*
   * It ensures that the inner node split happens after it reaches its the capacity. Post split,
   * we expect a new parent inner, which has two children nodes.
   *
   * Initial inner node looks as follows,
   *
   *                [100, 200, -]
   *                /    /     \
   *            [90]   [190]   [600]
   *
   * post that we add another inner node with the index key, [300]:
   *
   *              [100, 200, 300, -]
   *                /    /    /     \
   *            [90] [190] [290]    [600]
   *
   * which results in,
   *
   *                 [200]
   *                 /   \
   *              [100]   [300]
   *            /  \       /   \
   *        [90]  [190] [290]  [600]
   */
  @Test
  void testSplitInnerNodeAndEnsureParentAndRightMost() {
    var innerNode = createAndSplitInnerNode();
    var parent = innerNode.getParent();
    assertThat(parent).isNotNull();
    var keys = parent.getIndexKeys();
    Assertions.assertThat(keys).contains("200").hasSize(1);
    var node = parent.getRightMost().node();
    Assertions.assertThat(node).isInstanceOf(InnerNode.class);
    var parentRightMost = (InnerNode) node;
    var parentRightMostChildren = parentRightMost.getIndexKeys();
    Assertions.assertThat(parentRightMostChildren).contains("300").hasSize(1);
    Assertions.assertThat(parentRightMost.getRightMost()).isNotNull();
    var parentRightMostRightMostNode = parentRightMost.getRightMost().node();
    var parentRightMostInnerRightMostNode = (InnerNode) parentRightMostRightMostNode;
    var parentRightMostInnerRightMostChildren = parentRightMostInnerRightMostNode.getIndexKeys();
    Assertions.assertThat(parentRightMostInnerRightMostChildren).contains("600");
  }

  @Test
  void testSerizalizeInnenNode() {
    var splitInnerNode = createAndSplitInnerNode();
    var page = splitInnerNode.serialize();
    var deserializedPage = InnerNode.deserialize(page, diskAccessController);
    Assertions.assertThat(deserializedPage).isNotNull();
    Assertions.assertThat(deserializedPage.getKeySet()).hasSize(2);
  }

  private InnerNode createAndSplitInnerNode() {
    var inner = createInnerNode();
    inner.add(new Key("100", createInnerNode()));
    inner.add(new Key(RMN, createInnerNodeWithChild("600")));
    inner.add(new Key("200", createInnerNode()));
    inner.add(new Key("300", createInnerNode()));
    return inner;
  }


  private InnerNode createInnerNodeWithChild(String childKey) {
    var innerNode = new InnerNode(PageRef.empty(), diskAccessController);
    innerNode.add(new Key(childKey, null));
    return innerNode;
  }

  private InnerNode createInnerNode() {
    return new InnerNode(PageRef.empty(), diskAccessController);
  }
}
