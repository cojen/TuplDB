/*
 *  Copyright 2014 Brian S O'Neill
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
     * Add an entry as most recent. Caller must ensure that no duplicate entries are created.
     *
     * @param pageId non-zero page identifier
     * @param page copy source
     */
    public void add(long pageId, byte[] page);

    /**
     * Get and remove an entry if it exists.
     *
     * @param pageId non-zero page identifier
     * @param page copy destination
     * @return false if not found
     */
    public boolean remove(long pageId, byte[] page);

    /**
     * @return maximum supported number of entries
     */
    public int capacity();

    @Override
    public void close();

    static class Non implements PageCache {
        public void add(long pageId, byte[] page) {}
        public boolean remove(long pageId, byte[] page) {return false;}
        public int capacity() {return 0;};
        public void close() {}
    }
}
