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

import java.util.NoSuchElementException;

/**
 * Allows long ids to be added any order, but removed in sorted order. Data structure is a
 * binary heap.
 *
 * @author Brian S O'Neill
 */
final class IdHeap {
    private final long[] mIds;
    private int mSize;

    public IdHeap(int maxSize) {
        // Pad one more id to account for delete requiring an extra alloc if
        // free list node is deleted.
        mIds = new long[maxSize + 1];
    }

    public int size() {
        return mSize;
    }

    /**
     * @param id non-zero id
     */
    public void add(long id) {
        long[] ids = mIds;
        int pos = mSize;
        while (pos > 0) {
            int parentPos = (pos - 1) >>> 1;
            long parentId = ids[parentPos];
            if (id >= parentId) {
                break;
            }
            ids[pos] = parentId;
            pos = parentPos;
        }
        ids[pos] = id;
        // Increment only if no array bounds exception.
        mSize++;
    }

    /*
    public long peek() {
        if (mSize <= 0) {
            throw new NoSuchElementException();
        }
        return mIds[0];
    }
    */

    /**
     * @throws NoSuchElementException if empty
     */
    public long remove() {
        long id = tryRemove();
        if (id == 0) {
            throw new NoSuchElementException();
        }
        return id;
    }

    /**
     * @return 0 if empty
     */
    public long tryRemove() {
        final int size = mSize;
        if (size <= 0) {
            return 0;
        }
        int pos = size - 1;
        long[] ids = mIds;
        long result = ids[0];
        if (pos != 0) {
            long id = ids[pos];
            pos = 0;
            int half = size >>> 1;
            while (pos < half) {
                int childPos = (pos << 1) + 1;
                long child = ids[childPos];
                int rightPos = childPos + 1;
                if (rightPos < size && child > ids[rightPos]) {
                    child = ids[childPos = rightPos];
                }
                if (id <= child) {
                    break;
                }
                ids[pos] = child;
                pos = childPos;
            }
            ids[pos] = id;
        }
        mSize = size - 1;
        return result;
    }

    public boolean shouldDrain() {
        // Compare and ignore padding added by constructor.
        return mSize >= mIds.length - 1;
    }

    /**
     * Remove and encode all remaining ids, up to the maximum possible. Each id
     * is encoded as a difference from the previous.
     *
     * @return new offset
     */
    public int drain(long prevId, /*P*/ byte[] buffer, int offset, int length) {
        int end = offset + length;
        while (mSize > 0 && offset < end) {
            if (offset > (end - 9)) {
                long id = mIds[0];
                if (offset + PageOps.p_ulongVarSize(id - prevId) > end) {
                    break;
                }
            }
            long id = remove();
            offset = PageOps.p_ulongPutVar(buffer, offset, id - prevId);
            prevId = id;
        }
        return offset;
    }
}
