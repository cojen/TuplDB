/*
 *  Copyright 2011-2012 Brian S O'Neill
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

import java.io.InterruptedIOException;

/**
 * Pool of spare page buffers not currently in use by nodes.
 *
 * @author Brian S O'Neill
 */
final class BufferPool {
    private final byte[][] mPool;
    private int mPos;

    BufferPool(int pageSize, int poolSize) {
        byte[][] pool = new byte[poolSize][];
        for (int i=0; i<poolSize; i++) {
            pool[i] = new byte[pageSize];
        }
        mPool = pool;
        mPos = poolSize;
    }

    /**
     * Remove a buffer from the pool, waiting for one to become available if necessary.
     */
    synchronized byte[] remove() throws InterruptedIOException {
        try {
            int pos;
            while ((pos = mPos) == 0) {
                wait();
            }
            return mPool[mPos = pos - 1];
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        }
    }

    /**
     * Add a previously removed buffer back into the pool.
     */
    synchronized void add(byte[] buffer) {
        int pos = mPos;
        mPool[pos] = buffer;
        // Adjust pos after assignment to prevent harm if an array bounds exception was thrown.
        mPos = pos + 1;
        notify();
    }
}
