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

import java.util.LinkedList;
import java.util.List;
import java.util.function.BiFunction;
import net.reevik.mergen.io.DiskController;
import net.reevik.mergen.io.PageRef;
import net.reevik.mergen.io.SerializableObject;

public abstract class Node extends SerializableObject {

  enum Type {

    /**
     * Inner node type.
     */
    INNER,

    /**
     * Data node type.
     */
    DATA
  }

  private final List<NodeObserver> nodeObservers = new LinkedList<>();
  private InnerNode parent;

  protected Node(PageRef pageRef, DiskController diskAccessController) {
    super(pageRef, diskAccessController);
    markUnsynced();
  }

  protected Node(DiskController diskAccessController) {
    super(PageRef.empty(), diskAccessController);
    markDirty();
  }

  public Object firstIndexKey() {
    return getFirstIndexKey();
  }

  abstract Object getFirstIndexKey();

  public void upsert(DataEntity entity) {
    doUpsert(entity);
  }

  abstract void doUpsert(DataEntity entity);

  public List<DataRecord> query(String queryString,
      BiFunction<List<KeyData>, DataNode, List<DataRecord>> operation) {
    return doQuery(queryString, operation);
  }

  abstract List<DataRecord> doQuery(String queryString,
      BiFunction<List<KeyData>, DataNode, List<DataRecord>> operation);

  public int getSize() {
    return doGetSize();
  }

  abstract int doGetSize();

  abstract Type getNodeType();

  public InnerNode getParent() {
    return parent;
  }

  public void setParent(InnerNode parent) {
    this.parent = parent;
  }

  public boolean hasParent() {
    return parent != null;
  }

  public void registerObserver(NodeObserver nodeObserver) {
    nodeObservers.add(nodeObserver);
  }

  public void registerObservers(List<NodeObserver> nodeObservers) {
    this.nodeObservers.addAll(nodeObservers);
  }

  public void notifyObservers(Node newRoot) {
    nodeObservers.forEach(nodeObserver -> nodeObserver.onNewRoot(newRoot));
  }

  public List<NodeObserver> getNodeObservers() {
    return nodeObservers;
  }

  public PageRef getParentPageOffset() {
    if (hasParent()) {
      return getParent().getPageRef();
    }
    return new PageRef(-1);
  }
}
