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

import static net.reevik.hierarchy.index.NodeState.NEW;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import net.reevik.hierarchy.io.SerializableObject;

public abstract class Node implements SerializableObject {
  private final List<NodeObserver> nodeObservers = new LinkedList<>();
  private InnerNode parent;
  private long pageOffset = -1;
  private NodeState nodeState;

  protected Node(long pageOffset) {
    this.pageOffset = pageOffset;
    this.nodeState = NodeState.UNLOADED;
  }

  protected Node() {
    this.nodeState = NEW;
  }

  Object firstIndexKey() {
    load();
    return _firstIndexKey();
  }

  abstract Object _firstIndexKey();

  void upsert(DataEntity entity) {
    load();
    _upsert(entity);
  }

  abstract void _upsert(DataEntity entity);

  Set<DataRecord> query(String queryString) {
    load();
    return _query(queryString);
  }

  abstract Set<DataRecord> _query(String queryString);

  int getSize() {
    load();
    return _getSize();
  }

  abstract int _getSize();

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

  public long getPageOffset() {
    return pageOffset;
  }
}
