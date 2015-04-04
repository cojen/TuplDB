/*
 *  Copyright 2011-2013 Brian S O'Neill
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

/**
 * Pool of spare page buffers not currently in use by nodes.
 *
 * @author Brian S O'Neill
 */
final class PagePool extends Latch {
    private final transient WaitQueue mQueue;
    private final byte[][] mPool;
    private int mPos;

    PagePool(int pageSize, int poolSize) {
        mQueue = new WaitQueue();
        byte[][] pool = new byte[poolSize][];
        for (int i=0; i<poolSize; i++) {
            pool[i] = new byte[pageSize];
        }
        mPool = pool;
        mPos = poolSize;
    }

    /**
     * Remove a page from the pool, waiting for one to become available if necessary.
     */
    byte[] remove() {
        acquireExclusive();
        try {
            int pos;
            while ((pos = mPos) == 0) {
                mQueue.await(this, new WaitQueue.Node(), -1, 0);
            }
            return mPool[mPos = pos - 1];
        } finally {
            releaseExclusive();
        }
    }

    /**
     * Add a previously removed page back into the pool.
     */
    void add(byte[] page) {
        acquireExclusive();
        try {
            int pos = mPos;
            mPool[pos] = page;
            // Adjust pos after assignment to prevent harm if an array bounds exception was thrown.
            mPos = pos + 1;
            mQueue.signal();
        } finally {
            releaseExclusive();
        }
    }
}
