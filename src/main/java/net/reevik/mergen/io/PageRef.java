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

import static net.reevik.mergen.index.IndexUtils.getBytesOf;

public record PageRef(long pageOffset) {

  public static PageRef empty() {
    return new PageRef(-1L);
  }

  public static PageRef of(long offset) {
    return new PageRef(offset);
  }

  byte[] toBytes() {
    return getBytesOf(pageOffset);
  }

  public boolean hasNoOffset() {
    return pageOffset == -1L;
  }
}
