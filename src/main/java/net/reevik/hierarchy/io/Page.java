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

import static net.reevik.hierarchy.index.IndexUtils.append;
import static net.reevik.hierarchy.index.IndexUtils.getBytesOf;

import net.reevik.hierarchy.index.DataNode;

public class Page {
  public static final int PAGE_SIZE = 1024 * 16;
  private long pageOffset;
  private int pageSize;
  private short pageType;
  private long parentPageOffset;
  private long siblingOffset = -1L;
  private int cellCount = 0;
  private final byte[] pageBuffer = new byte[PAGE_SIZE];
  private final DataNode dataNode;

  public Page(DataNode dataNode) {
    this.dataNode = dataNode;
    this.pageSize = PAGE_SIZE;
    this.pageType = 1;
    this.parentPageOffset = dataNode.getParent().getPageOffset();
    serialize();
  }

  public void serialize() {
    var nextOffset = append(pageBuffer, getBytesOf(pageOffset), 0);
    nextOffset = append(pageBuffer, getBytesOf(pageSize), nextOffset);
    nextOffset = append(pageBuffer, getBytesOf(pageType), nextOffset);
    nextOffset = append(pageBuffer, getBytesOf(parentPageOffset), nextOffset);
    var cellCountOffset = append(pageBuffer, getBytesOf(siblingOffset), nextOffset);
    nextOffset = append(pageBuffer, getBytesOf(cellCount), cellCountOffset);
    int count = 0;
    var slotOffset = pageSize - (Integer.BYTES * (count + 1));
    for (var keyData : dataNode) {
      var keyDataView = keyData.toByteView();
      slotOffset = append(pageBuffer, getBytesOf(keyDataView.size()), slotOffset);
      nextOffset = append(pageBuffer, keyDataView.payload(), nextOffset);
      count++;
    }
    updateTotalCount(cellCountOffset, count);
  }

  private void updateTotalCount(int cellCountOffset, int count) {
    byte[] totalCountInBytes = getBytesOf(count);
    System.arraycopy(totalCountInBytes, 0, pageBuffer, cellCountOffset, totalCountInBytes.length);
  }

  public byte[] getPageBuffer() {
    return pageBuffer;
  }
}
