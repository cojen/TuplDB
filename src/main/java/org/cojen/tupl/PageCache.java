/*
 *  Copyright (C) 2011-2017 Cojen.org
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
