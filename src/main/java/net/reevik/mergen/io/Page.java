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

import static net.reevik.mergen.io.DiskFile.PAGE_SIZE;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

public class Page implements Iterable<ByteBuffer> {


  @Override
  public Iterator<ByteBuffer> iterator() {
    return new Iterator<>() {
      @Override
      public boolean hasNext() {
        return cursor.get() < cellCount;
      }

      @Override
      public ByteBuffer next() {
        int currentIndex = cursor.get();
        int cellSizeAtIndex = getCellSize(currentIndex);
        int cellOffsetAtIndex = getCellOffset(currentIndex);
        byte[] dest = new byte[cellSizeAtIndex];
        pageBuffer.position(cellOffsetAtIndex);
        pageBuffer.get(dest, 0, cellSizeAtIndex);
        cursor.incrementAndGet();
        return ByteBuffer.wrap(dest);
      }
    };
  }

  enum PageHeader {
    OFFSET(Long.BYTES, 0),
    P_OFFSET(Long.BYTES, OFFSET.nextOffset()),
    NEXT_PAGE(Long.BYTES, P_OFFSET.nextOffset()),
    SIZE(Integer.BYTES, NEXT_PAGE.nextOffset()),
    AVAILABLE(Integer.BYTES, SIZE.nextOffset()),
    TYPE(Short.BYTES, AVAILABLE.nextOffset()),
    SIBLING_OFFSET(Long.BYTES, TYPE.nextOffset()),
    CELL_COUNT(Integer.BYTES, SIBLING_OFFSET.nextOffset());

    private final int size;
    private final int start;

    PageHeader(int size, int start) {
      this.size = size;
      this.start = start;
    }

    int nextOffset() {
      return size + start;
    }

    int getSize() {
      return CELL_COUNT.nextOffset();
    }
  }

  public enum PageType {
    DATA_NODE((short) 1),
    INNER_NODE((short) 2),
    DATA_RECORD((short) 3);

    private final short pageTypeShort;

    PageType(short pageTypeShort) {
      this.pageTypeShort = pageTypeShort;
    }

    public static PageType from(short pageType) {
      return switch (pageType) {
        case 1 -> DATA_NODE;
        case 2 -> INNER_NODE;
        case 3 -> DATA_RECORD;
        default -> throw new RuntimeException("Unknown record type.");
      };
    }

    public short toShort() {
      return pageTypeShort;
    }
  }

  private final PageRef pageRef;
  private int pageSize;
  private PageRef nextPage;
  private PageType pageType;
  private PageRef parentPageRef;
  private int availableSpace;
  private long siblingOffset = -1L;
  private int cellCount = 0;
  private ByteBuffer pageBuffer;
  private SerializableObject serializableObject;
  private final AtomicInteger cursor = new AtomicInteger(0);

  public Page(SerializableObject serializableObject) {
    this.pageBuffer = ByteBuffer.allocate(PAGE_SIZE);
    this.pageSize = PAGE_SIZE;
    this.pageType = PageType.DATA_NODE;
    this.siblingOffset = -1;
    this.pageRef = serializableObject.getPageRef();
    this.serializableObject = serializableObject;
    this.parentPageRef = serializableObject.getParentPageRef();
    this.siblingOffset = serializableObject.getSiblingPageRef().pageOffset();
    this.nextPage = PageRef.empty();
    appendHeader();
  }

  public Page(byte[] buffer, PageRef pageRef) {
    this.pageBuffer = ByteBuffer.wrap(buffer);
    this.pageRef = pageRef;
  }

  private int getCellSize(int index) {
    int cellIndexPos = pageBuffer.capacity() - ((index + 1) * Integer.BYTES);
    return pageBuffer.position(cellIndexPos).getInt();
  }

  public Page appendCell(ByteBuffer cellBuffer) {
    int occupiedSpace = getOccupiedSpace();
    pageBuffer = pageBuffer.position(getHeadPosition())
        .put(cellBuffer.array())
        .position(PageHeader.SIBLING_OFFSET.nextOffset())
        .putInt(++cellCount)
        .position(pageBuffer.capacity() - (Integer.BYTES * cellCount))
        .putInt(cellBuffer.capacity())
        .position(PageHeader.AVAILABLE.start)
        .putInt(occupiedSpace + cellBuffer.capacity());
    return this;
  }

  private int getHeadPosition() {
    var currentCellSize = PageHeader.CELL_COUNT.nextOffset();
    pageBuffer.position(pageBuffer.capacity());
    for (int i = 0; i < cellCount; i++) {
      pageBuffer.position(pageBuffer.capacity() - (Integer.BYTES * (i + 1)));
      currentCellSize += pageBuffer.getInt();
    }
    return currentCellSize;
  }

  private int getCellOffset(int index) {
    if (index < cellCount) {
      var currentCellSize = PageHeader.CELL_COUNT.nextOffset();
      pageBuffer.position(pageBuffer.capacity());
      for (int i = 0; i < index; i++) {
        pageBuffer.position(pageBuffer.capacity() - (Integer.BYTES * (i + 1)));
        currentCellSize += pageBuffer.getInt();
      }
      return currentCellSize;
    }
    throw new IndexOutOfBoundsException();
  }


  private int getOccupiedSpace() {
    pageBuffer.position(pageBuffer.capacity());
    var totalOccupied = 0;
    for (int i = 0; i < cellCount; i++) {
      pageBuffer.position(pageBuffer.capacity() - (Integer.BYTES * (i + 1)));
      totalOccupied += pageBuffer.getInt();
    }
    return totalOccupied;
  }

  private int getSpaceAvailable() {
    return PAGE_SIZE - PageHeader.CELL_COUNT.nextOffset() - getOccupiedSpace();
  }

  public Page appendHeader() {
    pageBuffer.clear();
    pageBuffer = pageBuffer.putLong(serializableObject.getPageRef().pageOffset())
        .putLong(serializableObject.getParentPageRef().pageOffset())
        .putLong(nextPage.pageOffset())
        .putInt(pageSize)
        .putInt(availableSpace)
        .putShort(pageType.toShort())
        .putLong(serializableObject.getSiblingPageRef().pageOffset())
        .putInt(cellCount);
    return this;
  }

  public byte[] getPageBuffer() {
    return pageBuffer.clear().array();
  }

  public PageRef getPageRef() {
    return pageRef;
  }
}