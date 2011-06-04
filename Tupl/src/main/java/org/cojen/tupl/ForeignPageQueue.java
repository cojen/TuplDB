/*
 *  Copyright 2011 Brian S O'Neill
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

import java.io.IOException;

import java.util.concurrent.locks.Lock;

/**
 * Stores freed pages for use by another page array. For this reason, this
 * class doesn't allocate or create any pages except for the internal queue.
 *
 * @author Brian S O'Neill
 */
class ForeignPageQueue extends PageQueue {
    private final PageQueue mAllocator;

    ForeignPageQueue(PageQueue allocator) throws IOException {
        super(allocator.pageArray());
        mAllocator = allocator;
    }

    ForeignPageQueue(PageQueue allocator, byte[] header, int offset) throws IOException {
        super(allocator.pageArray(), header, offset);
        mAllocator = allocator;
    }

    @Override
    long createPage() throws IOException {
        return 0;
    }

    @Override
    boolean isPageOutOfBounds(long id) throws IOException {
        return false;
    }

    @Override
    long allocQueuePage() throws IOException {
        return mAllocator.allocQueuePage();
    }

    @Override
    void deleteQueuePage(long id) throws IOException {
        mAllocator.deleteQueuePage(id);
    }
}
