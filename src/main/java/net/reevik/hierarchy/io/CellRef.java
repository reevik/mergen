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

public record CellRef(int offset, int size) {

  byte[] toBytes() {
    byte[] bytesOffset = getBytesOf(offset);
    byte[] bytesSize = getBytesOf(size);
    var result = new byte[2 * Integer.BYTES];
    append(result, bytesOffset, 0);
    append(result, bytesSize, Integer.BYTES);
    return result;
  }
}
