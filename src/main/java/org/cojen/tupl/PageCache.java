/*
 *  Copyright 2014-2015 Cojen.org
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.cojen.tupl;

import java.io.Closeable;

/**
 * Simple cache for storing fixed sized pages.
 *
 * @author Brian S O'Neill
 */
interface PageCache extends Closeable {
    /**
     * Add or replace an entry as most recent.
     *
     * @param pageId page identifier
     * @param page copy source
     * @param canEvict true if another page can be evicted to make room
     * @return false if close or if cannot evict and cache is full
     */
    public boolean add(long pageId, byte[] page, int offset, boolean canEvict);

    public boolean add(long pageId, long pagePtr, int offset, boolean canEvict);

    /**
     * Copy all or part of an entry if it exists, without affecting eviction priority.
     *
     * @param pageId page identifier
     * @param start start of page to copy
     * @param page copy destination
     * @return false if not found
     */
    public boolean copy(long pageId, int start, byte[] page, int offset);

    public boolean copy(long pageId, int start, long pagePtr, int offset);

    /**
     * Get and remove an entry if it exists.
     *
     * @param pageId page identifier
     * @param page copy destination; pass null for no copy
     * @return false if not found
     */
    public boolean remove(long pageId, byte[] page, int offset, int length);

    public boolean remove(long pageId, long pagePtr, int offset, int length);

    /**
     * @return maximum number of bytes in the cache
     */
    public long capacity();

    /**
     * @return maximum supported number of entries
     */
    public long maxEntryCount();

    @Override
    public void close();
}
