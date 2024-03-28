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

import static net.reevik.hierarchy.index.SyncState.DIRTY;
import static net.reevik.hierarchy.index.SyncState.SYNCED;
import static net.reevik.hierarchy.index.SyncState.UNSYNCED;

import net.reevik.hierarchy.index.SyncState;

public abstract class SerializableObject {

  private SyncState syncState;
  private PageRef pageRef;

  public SerializableObject(PageRef pageRef) {
    this.pageRef = pageRef;
  }

  public void markDirty() {
    syncState = DIRTY;
  }

  public void markSynced() {
    syncState = SYNCED;
  }

  public void markUnsynced() {
    syncState = UNSYNCED;
  }

  public boolean isUnsynced() {
    return syncState == UNSYNCED;
  }

  public boolean isSynced() {
    return syncState == SYNCED;
  }

  public boolean isDirty() {
    return syncState == DIRTY;
  }

  public PageRef getPageRef() {
    if (isUnsynced()) {
      load();
    }
    return pageRef;
  }

  public abstract void load();

  public abstract long persist();
}
