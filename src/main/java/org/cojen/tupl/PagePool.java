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

import org.cojen.tupl.util.Latch;
import org.cojen.tupl.util.LatchCondition;

/**
 * Pool of spare page buffers not currently in use by nodes.
 *
 * @author Brian S O'Neill
 */
final class PagePool extends Latch {
    private final transient LatchCondition mQueue;
    private final /*P*/ byte[][] mPool;
    private int mPos;

    PagePool(int pageSize, int poolSize, boolean aligned) {
        mQueue = new LatchCondition();
        /*P*/ byte[][] pool = PageOps.p_allocArray(poolSize);
        for (int i=0; i<poolSize; i++) {
            pool[i] = PageOps.p_calloc(pageSize, aligned);
        }
        mPool = pool;
        mPos = poolSize;
    }

    /**
     * Remove a page from the pool, waiting for one to become available if necessary.
     */
    /*P*/ byte[] remove() {
        acquireExclusive();
        try {
            int pos;
            while ((pos = mPos) == 0) {
                mQueue.await(this, -1, 0);
            }
            return mPool[mPos = pos - 1];
        } finally {
            releaseExclusive();
        }
    }

    /**
     * Add a previously removed page back into the pool.
     */
    void add(/*P*/ byte[] page) {
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

    /**
     * Must be called when object is no longer referenced.
     */
    void delete() {
        acquireExclusive();
        try {
            for (int i=0; i<mPos; i++) {
                /*P*/ byte[] page = mPool[i];
                mPool[i] = PageOps.p_null();
                PageOps.p_delete(page);
            }
        } finally {
            releaseExclusive();
        }
    }
}
