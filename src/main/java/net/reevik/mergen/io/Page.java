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

  /**
   * Maximum size in bytes of a page can accept to store a cell:
   * <pre>
   * Maximum cell space = Page Size  - Header size - Cell pointer.
   * </pre>
   */
  public static final int MAX_CELL_SPACE = PAGE_SIZE - PageHeader.getSize() - Integer.BYTES;

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
    P_OFFSET(Long.BYTES, OFFSET.nextHeaderOffset()),
    NEXT_PAGE(Long.BYTES, P_OFFSET.nextHeaderOffset()),
    SIZE(Integer.BYTES, NEXT_PAGE.nextHeaderOffset()),
    AVAILABLE(Integer.BYTES, SIZE.nextHeaderOffset()),
    TYPE(Short.BYTES, AVAILABLE.nextHeaderOffset()),
    SIBLING_OFFSET(Long.BYTES, TYPE.nextHeaderOffset()),
    CELL_COUNT(Integer.BYTES, SIBLING_OFFSET.nextHeaderOffset());

    private final int size;
    private final int start;

    PageHeader(int size, int start) {
      this.size = size;
      this.start = start;
    }

    int nextHeaderOffset() {
      return size + start;
    }

    int offset() {
      return start;
    }

    static int getSize() {
      return CELL_COUNT.nextHeaderOffset();
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
  private PageRef nextSlottedPage;
  private PageType pageType;
  private PageRef parentNodePageRef;
  private int availableSpace;
  private long siblingNodeOffset = -1L;
  private int cellCount = 0;
  private ByteBuffer pageBuffer;
  private SerializableObject serializableObject;
  private final AtomicInteger cursor = new AtomicInteger(0);

  public Page(SerializableObject serializableObject) {
    this.pageBuffer = ByteBuffer.allocate(PAGE_SIZE);
    this.pageSize = PAGE_SIZE;
    this.pageType = PageType.DATA_NODE;
    this.pageRef = serializableObject.getPageRef();
    this.serializableObject = serializableObject;
    this.parentNodePageRef = serializableObject.getParentPageRef();
    this.siblingNodeOffset = serializableObject.getNextSlottedPageRef().pageOffset();
    this.nextSlottedPage = PageRef.empty();
    appendHeader();
  }

  public Page(byte[] buffer, PageRef pageRef) {
    this.pageBuffer = ByteBuffer.wrap(buffer);
    this.pageRef = pageRef;
    readHeader();
  }

  private int getCellSize(int index) {
    int cellIndexPos = pageBuffer.capacity() - ((index + 1) * Integer.BYTES);
    return pageBuffer.position(cellIndexPos).getInt();
  }

  public boolean hasSpace(long askedSize) {
    return getSpaceAvailable() + Integer.BYTES > askedSize;
  }

  public Page appendCell(ByteBuffer cellBuffer) {
    pageBuffer = pageBuffer.position(getHeadPosition())
        .put(cellBuffer.array())
        .position(PageHeader.CELL_COUNT.offset())
        .putInt(++cellCount)
        .position(getNextCellPointerOffset())
        .putInt(cellBuffer.capacity());
    // space availability check request to move cursor position.
    // we determine the available space after the cell is written, check available space and
    // reset the cursor position.
    int spaceAvailable = getSpaceAvailable();
    pageBuffer
        .position(PageHeader.AVAILABLE.offset())
        .putInt(spaceAvailable);
    return this;
  }

  private int getNextCellPointerOffset() {
    return pageBuffer.capacity() - (Integer.BYTES * cellCount);
  }

  private int getHeadPosition() {
    var firstCellOffset = PageHeader.CELL_COUNT.nextHeaderOffset();
    // wind to the end of the page.
    pageBuffer.position(pageBuffer.capacity());
    // while going back one int byte for each cell pointer, add cell size to the current cell
    // head offset.
    for (int i = 0; i < cellCount; i++) {
      pageBuffer.position(pageBuffer.capacity() - (Integer.BYTES * (i + 1)));
      int nextCellSize = pageBuffer.getInt();
      firstCellOffset += nextCellSize;
    }
    return firstCellOffset;
  }

  private int getCellOffset(int index) {
    if (index < cellCount) {
      var currentCellSize = PageHeader.CELL_COUNT.nextHeaderOffset();
      pageBuffer.position(pageBuffer.capacity());
      for (int i = 0; i < index; i++) {
        pageBuffer.position(pageBuffer.capacity() - (Integer.BYTES * (i + 1)));
        currentCellSize += pageBuffer.getInt();
      }
      return currentCellSize;
    }
    throw new IndexOutOfBoundsException();
  }


  private int getOccupiedCellSpace() {
    pageBuffer.position(pageBuffer.capacity());
    var totalOccupied = 0;
    var cellHeaderSize = Integer.BYTES;
    for (int i = 0; i < cellCount; i++) {
      pageBuffer.position(pageBuffer.capacity() - (cellHeaderSize * (i + 1)));
      int currentCellSize = pageBuffer.getInt();
      totalOccupied += currentCellSize + cellHeaderSize;
    }
    return totalOccupied;
  }

  private int getSpaceAvailable() {
    return PAGE_SIZE - PageHeader.getSize() - getOccupiedCellSpace();
  }

  public Page appendHeader() {
    pageBuffer.clear();
    pageBuffer = pageBuffer
        .putLong(serializableObject.getPageRef().pageOffset()) // Page reference.
        .putLong(serializableObject.getParentPageRef().pageOffset()) // Parent page reference.
        .putLong(nextSlottedPage.pageOffset())
        .putInt(pageSize) // Page size.
        .putInt(PAGE_SIZE - PageHeader.getSize() - Long.BYTES) // Available size.
        .putShort(pageType.toShort()) // Page type.
        .putLong(serializableObject.getNextSlottedPageRef().pageOffset())
        .putInt(cellCount);
    return this;
  }

  private void readHeader() {
    if (pageBuffer.capacity() >= PageHeader.getSize()) {
      pageBuffer.position(0);
      long pageOffset = pageBuffer.getLong();
      parentNodePageRef = PageRef.of(pageBuffer.getLong());
      nextSlottedPage = PageRef.of(pageBuffer.getLong());
      pageSize = pageBuffer.getInt();
      availableSpace = pageBuffer.getInt();
      pageType = PageType.from(pageBuffer.getShort());
      siblingNodeOffset = pageBuffer.getLong();
      cellCount = pageBuffer.getInt();
    } else {
      throw new IllegalStateException("Page header is missing.");
    }
  }

  public byte[] getPageBuffer() {
    return pageBuffer.clear().array();
  }

  public PageRef getPageRef() {
    return pageRef;
  }

  public void setNextSlottedPage(PageRef nextSlottedPage) {
    this.nextSlottedPage = nextSlottedPage;
  }
}
