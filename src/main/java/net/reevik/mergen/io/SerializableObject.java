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

import static net.reevik.mergen.index.SyncState.DIRTY;
import static net.reevik.mergen.index.SyncState.SYNCED;
import static net.reevik.mergen.index.SyncState.UNSYNCED;

import net.reevik.mergen.index.SyncState;
import net.reevik.mergen.io.Page.PageType;

/**
 * Serializable object is used to persist the data into disk.
 */
public abstract class SerializableObject {

  private final PageRef pageRef;
  private final DiskController diskAccessController;
  private SyncState syncState;
  private PageRef parentPageRef = PageRef.empty();
  private PageRef nextSlottedPageRef = PageRef.empty();

  protected SerializableObject(PageRef pageRef, DiskController diskAccessController) {
    this.pageRef = pageRef;
    this.diskAccessController = diskAccessController;
  }

  protected SerializableObject(PageRef pageRef, PageRef parentPageRef,
      DiskController diskAccessController) {
    this.pageRef = pageRef;
    this.parentPageRef = parentPageRef;
    this.diskAccessController = diskAccessController;
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
    return pageRef;
  }

  public PageRef getParentPageRef() {
    return parentPageRef;
  }

  public void assignParent(PageRef parentPageRef) {
    this.parentPageRef = parentPageRef;
  }

  public void setNextSlottedPageRef(PageRef nextSlottedPageRef) {
    this.nextSlottedPageRef = nextSlottedPageRef;
  }

  public PageRef getNextSlottedPageRef() {
    return nextSlottedPageRef;
  }

  public DiskController getDiskAccessController() {
    return diskAccessController;
  }

  /**
   * Persists the serializable object to index file.
   * <p>
   *
   * @return the page reference including the offset in the file.
   */
  public abstract PageRef persist();

  public abstract PageType getPageType();
}
