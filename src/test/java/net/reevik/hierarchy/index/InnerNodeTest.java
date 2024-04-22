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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.when;

import net.reevik.hierarchy.io.DiskAccessController;
import net.reevik.hierarchy.io.PageRef;
import net.reevik.mikron.annotation.ManagedApplication;
import net.reevik.mikron.annotation.Wire;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@ManagedApplication(packages = "net.reevik.hierarchy.*")
class InnerNodeTest {

  @Wire
  private DiskAccessController diskAccessController;

  @Test
  void testSplit() {
    var innerNode = createSplitInnerNode();
    var parent = innerNode.getParent();
    assertThat(parent).isNotNull();
  }

  private InnerNode createSplitInnerNode() {
    var inner = createInnerNode();
    var subInner1Key = Mockito.mock(Key.class);
    when(subInner1Key.isRightMost()).thenReturn(false);
    when(subInner1Key.indexKey()).thenReturn("100");
    var subInner2Key = Mockito.mock(Key.class);
    when(subInner2Key.isRightMost()).thenReturn(false);
    when(subInner2Key.indexKey()).thenReturn("200");
    var subInner3Key = Mockito.mock(Key.class);
    when(subInner3Key.isRightMost()).thenReturn(true);
    when(subInner3Key.indexKey()).thenReturn("300");
    var subInner4Key = Mockito.mock(Key.class);
    when(subInner4Key.isRightMost()).thenReturn(false);
    when(subInner4Key.indexKey()).thenReturn("300");
    inner.add(subInner1Key);
    inner.add(subInner2Key);
    inner.add(subInner3Key);
    inner.add(subInner4Key);
    return inner;
  }

  private InnerNode createInnerNode(Object childKey) {
    InnerNode innerNode = new InnerNode(PageRef.empty(), diskAccessController);
    innerNode.add(new Key(childKey, null));
    return innerNode;
  }

  private InnerNode createInnerNode() {
    return new InnerNode(PageRef.empty(), diskAccessController);
  }
}
