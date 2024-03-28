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

import static java.util.Arrays.copyOfRange;
import static net.reevik.hierarchy.index.DataRecord.createSynced;
import static net.reevik.hierarchy.index.DataRecord.createUnloaded;
import static net.reevik.hierarchy.index.IndexUtils.bytesToLong;
import static net.reevik.hierarchy.io.FileIO.PAGE_SIZE;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import net.reevik.hierarchy.index.DataNode;
import net.reevik.hierarchy.index.InnerNode;
import net.reevik.hierarchy.index.KeyData;

public class Page {

  enum PageHeader {
    OFFSET(Long.BYTES, 0),
    SIZE(Long.BYTES, OFFSET.nextOffset()),
    TYPE(Short.SIZE, SIZE.nextOffset()),
    PARENT_OFFSET(Long.BYTES, TYPE.nextOffset()),
    SIBLING_OFFSET(Long.BYTES, PARENT_OFFSET.nextOffset()),
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

    public byte[] from(byte[] buffer) {
      return copyOfRange(buffer, start, size);
    }
  }

  enum PageType {
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

  private PageRef pageRef;
  private int pageSize;
  private PageType pageType;
  private PageRef parentPageRef;
  private long siblingOffset = -1L;
  private int cellCount = 0;
  private final ByteBuffer pageBuffer;

  public Page(DataNode dataNode) {
    this.pageBuffer = ByteBuffer.allocate(PAGE_SIZE);
    this.pageSize = PAGE_SIZE;
    this.pageType = PageType.DATA_NODE;
    this.parentPageRef = dataNode.getParentPageOffset();
    this.siblingOffset = -1;
    this.pageRef = dataNode.getPageRef();
    serialize(dataNode);
  }

  public Page(byte[] buffer) {
    this.pageBuffer = ByteBuffer.wrap(buffer);
    deserialize();
  }

  public DataNode deserialize() {
    var sizes = readHeader();
    var entity = new DataNode(parentPageRef);
    entity.setParent(new InnerNode(parentPageRef)); // TODO
    sizes.stream()
        .map(this::read)
        .map(KeyData::from)
        .forEach(entity::add);
    return entity;
  }

  private byte[] read(Integer size) {
    var bytes = new byte[size];
    pageBuffer.get(bytes, 0, size);
    return bytes;
  }

  private List<Integer> readHeader() {
    pageBuffer.clear();
    this.pageRef = new PageRef(pageBuffer.getLong());
    this.pageSize = pageBuffer.getInt();
    this.pageType = PageType.from(pageBuffer.getShort());
    this.parentPageRef = new PageRef(pageBuffer.getLong());
    this.siblingOffset = pageBuffer.getLong();
    this.cellCount = pageBuffer.getInt();
    pageBuffer.mark();
    pageBuffer.position(pageBuffer.capacity() - (cellCount * Integer.BYTES));
    List<Integer> sizes = new ArrayList<>();
    for (int i = 0; i < cellCount; i++) {
      sizes.add(pageBuffer.getInt());
    }
    pageBuffer.reset();
    return sizes;
  }

  private void serialize(DataNode dataEntity) {
    appendHeader(dataEntity);
    List<Integer> sizes = new ArrayList<>();
    for (final var keyData : dataEntity) {
      var keyDataByteBuffer = keyData.toByteBuffer();
      pageBuffer.put(keyDataByteBuffer.array());
      sizes.add(keyDataByteBuffer.capacity());
    }
    cellCount = sizes.size();
    pageBuffer.position(pageBuffer.capacity() - (Integer.BYTES * sizes.size()));
    sizes.forEach(pageBuffer::putInt);
    pageBuffer.reset().putInt(sizes.size());
  }

  private ByteBuffer appendHeader(DataNode dataEntity) {
    return pageBuffer.putLong(dataEntity.getPageRef().pageOffset())
        .putInt(pageSize)
        .putShort(pageType.toShort())
        .putLong(dataEntity.getParentPageOffset().pageOffset())
        .putLong(siblingOffset)
        .mark()
        .putInt(cellCount);
  }

  public byte[] getPageBuffer() {
    return pageBuffer.clear().array();
  }
}
