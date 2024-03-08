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

import java.util.Objects;

public record Key(Object indexRangeKey, Node node) implements Comparable<Key> {

  @Override
  public int compareTo(Key o) {
    return indexRangeKey.toString().compareTo(o.indexRangeKey.toString());
  }

  public boolean isRightMost() {
    return indexRangeKey.toString().compareTo(node.firstIndexKey().toString()) <= 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Key key = (Key) o;
    return Objects.equals(indexRangeKey, key.indexRangeKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(indexRangeKey);
  }

  @Override
  public String toString() {
    return "Key{" +
        "indexRangeKey=" + indexRangeKey +
        '}';
  }

  @Override
  public Object indexRangeKey() {
    return indexRangeKey;
  }

  @Override
  public Node node() {
    return node;
  }
}
