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

import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * Allows long ids to be added any order, but removed in sorted order. Data structure is a
 * binary heap.
 *
 * @author Brian S O'Neill
 */
final class IdHeap {
    private final int mDrainSize;
    private long[] mIds;
    private int mSize;

    public IdHeap(int drainSize) {
        mDrainSize = drainSize;
        // Pad one more id to account for delete requiring an extra alloc if
        // free list node is deleted.
        mIds = new long[drainSize + 1];
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
        if (pos >= ids.length) {
            // Usually a single padding element is sufficient, but sometimes the free list
            // contains many nodes which only have a single element. This can cause additional
            // deletions during a drain operation and overflow the heap. This is usually caused
            // by aggressive forced checkpoints, typical of a database compaction operation.
            // Grow the heap capacity incrementally.
            mIds = ids = Arrays.copyOf(ids, ids.length + 1);
        }
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

    /**
     * Removes a specific id, intended for recovering from exceptions.
     */
    public void remove(long id) {
        long[] copy = new long[mIds.length];
        int pos = 0;
        while (true) {
            long removed = tryRemove();
            if (removed == 0) {
                break;
            }
            if (removed != id) {
                copy[pos++] = removed;
            }
        }
        while (--pos >= 0) {
            add(copy[pos]);
        }
    }

    public boolean shouldDrain() {
        return mSize >= mDrainSize;
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

    /**
     * @param id first id; was prevId for drain method call
     * @param endOffset must be return offset from drain
     */
    public void undrain(long id, /*P*/ byte[] buffer, int offset, int endOffset) {
        add(id);
        IntegerRef offsetRef = new IntegerRef.Value();
        offsetRef.set(offset);
        while (offsetRef.get() < endOffset) {
            id += PageOps.p_ulongGetVar(buffer, offsetRef);
            add(id);
        }
    }
}
