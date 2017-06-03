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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;

import org.cojen.tupl.util.Latch;

/**
 * Page cache which uses direct buffers and very few Java objects, eliminating garbage
 * collection overhead. Caller should scramble page ids to reduce hash collisions.
 *
 * Note: Might need to set -XX:MaxDirectMemorySize=99999m
 *
 * @author Brian S O'Neill
 */
final class BasicPageCache extends Latch implements PageCache {
    /*
      Node format:

      long: pageId
      int:  lessRecentPtr
      int:  moreRecentPtr
      int:  chainNextPtr
    */

    private static final int PAGE_ID_FIELD         = 0;
    private static final int LESS_RECENT_PTR_FIELD = 2;
    private static final int MORE_RECENT_PTR_FIELD = 3;
    private static final int CHAIN_NEXT_PTR_FIELD  = 4;

    private static final int NODE_SIZE_IN_INTS = CHAIN_NEXT_PTR_FIELD + 1;

    private static final int NO_NEXT_ENTRY = -1;
    private static final int UNUSED_NODE = -2;

    private final int mPageSize;
    private final int[] mHashTable;

    private ByteBuffer mNodesByteBuffer;
    private IntBuffer mNodes;
    private ByteBuffer mData;

    private int mLeastRecentPtr;
    private int mMostRecentPtr;

    /**
     * @param capacity capacity in bytes
     */
    BasicPageCache(int capacity, int pageSize) {
        int entryCount = entryCountFor(capacity, pageSize);

        mPageSize = pageSize;
        mHashTable = new int[entryCount];

        acquireExclusive();

        mNodesByteBuffer = ByteBuffer
            .allocateDirect(entryCount * (NODE_SIZE_IN_INTS * 4))
            .order(ByteOrder.nativeOrder());
        mNodes = mNodesByteBuffer.asIntBuffer();
        mData = ByteBuffer.allocateDirect(entryCount * pageSize);

        // Initialize the nodes, all linked together.
        int ptr = 0;
        for (; ptr < entryCount * NODE_SIZE_IN_INTS; ptr += NODE_SIZE_IN_INTS) {
            mNodes.put(ptr + LESS_RECENT_PTR_FIELD, ptr - NODE_SIZE_IN_INTS);
            mNodes.put(ptr + MORE_RECENT_PTR_FIELD, ptr + NODE_SIZE_IN_INTS);
            mNodes.put(ptr + CHAIN_NEXT_PTR_FIELD, UNUSED_NODE);
        }

        mLeastRecentPtr = 0;
        mMostRecentPtr = ptr - NODE_SIZE_IN_INTS;

        for (int i=0; i<mHashTable.length; i++) {
            mHashTable[i] = NO_NEXT_ENTRY;
        }

        releaseExclusive();
    }

    static int entryCountFor(int capacity, int pageSize) {
        return Math.max(2, capacity / ((NODE_SIZE_IN_INTS * 4) + pageSize));
    }

    static int capacityFor(int entryCount, int pageSize) {
        return (entryCount * (NODE_SIZE_IN_INTS * 4)) + (entryCount * pageSize);
    }

    @Override
    public boolean add(final long pageId,
                       final byte[] page, final int offset,
                       final boolean canEvict)
    {
        return add(pageId, canEvict, (dst, pageSize) -> {
            dst.put(page, offset, pageSize);
        });
    }

    @Override
    public boolean add(long pageId, long pagePtr, int offset, boolean canEvict) {
        return add(pageId, canEvict, (dst, pageSize) -> {
            DirectPageOps.p_copyToBB(pagePtr, offset, dst, pageSize);
        });
    }

    private boolean add(final long pageId, final boolean canEvict, final CopyToBB copier) {
        acquireExclusive();
        try {
            final IntBuffer nodes = mNodes;
            if (nodes == null) {
                // Closed.
                return false;
            }

            final int[] hashTable = mHashTable;
            final int index = hash(pageId) % hashTable.length;

            // Try to replace existing entry.
            int ptr = hashTable[index];
            if (ptr >= 0) {
                while (true) {
                    final int chainNextPtr = nodes.get(ptr + CHAIN_NEXT_PTR_FIELD);
                    if (getPageId(nodes, ptr) == pageId) {
                        // Found it.
                        mData.position((ptr / NODE_SIZE_IN_INTS) * mPageSize);
                        copier.copyToBB(mData, mPageSize);

                        if (ptr != mMostRecentPtr) {
                            // Move to most recent.
                            int morePtr = nodes.get(ptr + MORE_RECENT_PTR_FIELD);
                            if (ptr == mLeastRecentPtr) {
                                mLeastRecentPtr = morePtr;
                            } else {
                                int lessPtr = nodes.get(ptr + LESS_RECENT_PTR_FIELD);
                                nodes.put(morePtr + LESS_RECENT_PTR_FIELD, lessPtr);
                                nodes.put(lessPtr + MORE_RECENT_PTR_FIELD, morePtr);
                            }
                            nodes.put(mMostRecentPtr + MORE_RECENT_PTR_FIELD, ptr);
                            nodes.put(ptr + LESS_RECENT_PTR_FIELD, mMostRecentPtr);
                            mMostRecentPtr = ptr;
                        }

                        return true;
                    }
                    if (chainNextPtr < 0) {
                        break;
                    }
                    ptr = chainNextPtr;
                }
            }

            // Select the least recent entry.
            ptr = mLeastRecentPtr;

            if (nodes.get(ptr + CHAIN_NEXT_PTR_FIELD) != UNUSED_NODE) {
                if (!canEvict) {
                    return false;
                }

                // Evict old entry from hashtable.
                final long evictedPageId = getPageId(nodes, ptr);
                final int evictedIndex = hash(evictedPageId) % hashTable.length;

                int entryPtr = hashTable[evictedIndex];
                if (entryPtr >= 0) {
                    int prevPtr = NO_NEXT_ENTRY;
                    while (true) {
                        final int chainNextPtr = nodes.get(entryPtr + CHAIN_NEXT_PTR_FIELD);
                        if (getPageId(nodes, entryPtr) == evictedPageId) {
                            if (prevPtr < 0) {
                                hashTable[evictedIndex] = chainNextPtr;
                            } else {
                                nodes.put(prevPtr + CHAIN_NEXT_PTR_FIELD, chainNextPtr);
                            }
                            break;
                        }
                        if (chainNextPtr < 0) {
                            break;
                        }
                        prevPtr = entryPtr;
                        entryPtr = chainNextPtr;
                    }
                }
            }

            // Move to most recent entry.
            mLeastRecentPtr = nodes.get(ptr + MORE_RECENT_PTR_FIELD);
            nodes.put(mMostRecentPtr + MORE_RECENT_PTR_FIELD, ptr);
            nodes.put(ptr + LESS_RECENT_PTR_FIELD, mMostRecentPtr);
            mMostRecentPtr = ptr;

            // Copy page into the data buffer.
            mData.position((ptr / NODE_SIZE_IN_INTS) * mPageSize);
            copier.copyToBB(mData, mPageSize);

            // Add new entry into the hashtable.
            nodes.put(ptr + CHAIN_NEXT_PTR_FIELD, hashTable[index]);
            hashTable[index] = ptr;
            setPageId(nodes, ptr, pageId);

            return true;
        } finally {
            releaseExclusive();
        }
    }

    @Override
    public boolean copy(final long pageId, final int start,
                        final byte[] page, final int offset)
    {
        return copy(pageId, start, (src, pageSize) -> {
            src.get(page, offset, pageSize);
        });
    }

    @Override
    public boolean copy(final long pageId, final int start,
                        final long pagePtr, final int offset)
    {
        return copy(pageId, start, (src, pageSize) -> {
            DirectPageOps.p_copyFromBB(src, pagePtr, offset, pageSize);
        });
    }

    private boolean copy(final long pageId, final int start, final CopyFromBB copier) {
        acquireShared();
        try {
            final IntBuffer nodes = mNodes;
            if (nodes == null) {
                // Closed.
                return false;
            }

            final int[] hashTable = mHashTable;
            final int index = hash(pageId) % hashTable.length;

            int ptr = hashTable[index];
            if (ptr >= 0) {
                while (true) {
                    final int chainNextPtr = nodes.get(ptr + CHAIN_NEXT_PTR_FIELD);
                    if (getPageId(nodes, ptr) == pageId) {
                        // Found it.
                        mData.position(((ptr / NODE_SIZE_IN_INTS) * mPageSize) + start);
                        copier.copyFromBB(mData, mPageSize);
                        return true;
                    }
                    if (chainNextPtr < 0) {
                        break;
                    }
                    ptr = chainNextPtr;
                }
            }

            return false;
        } finally {
            releaseShared();
        }
    }

    public boolean remove(final long pageId,
                          final byte[] page, final int offset, final int length)
    {
        return remove(pageId, page == null ? null : (src, pageSize) -> {
            src.get(page, offset, length);
        });
    }

    @Override
    public boolean remove(long pageId, long pagePtr, int offset, int length) {
        return remove(pageId, pagePtr == DirectPageOps.p_null() ? null : (src, pageSize) -> {
            DirectPageOps.p_copyFromBB(src, pagePtr, offset, length);
        });
    }

    private boolean remove(final long pageId, final CopyFromBB copier) {
        acquireExclusive();
        try {
            final IntBuffer nodes = mNodes;
            if (nodes == null) {
                // Closed.
                return false;
            }

            final int[] hashTable = mHashTable;
            final int index = hash(pageId) % hashTable.length;

            int ptr = hashTable[index];
            if (ptr >= 0) {
                int prevPtr = NO_NEXT_ENTRY;
                while (true) {
                    final int chainNextPtr = nodes.get(ptr + CHAIN_NEXT_PTR_FIELD);
                    if (getPageId(nodes, ptr) == pageId) {
                        // Found it.

                        if (copier != null) {
                            // Copy data buffer into the page.
                            mData.position((ptr / NODE_SIZE_IN_INTS) * mPageSize);
                            copier.copyFromBB(mData, mPageSize);
                        }

                        if (ptr != mLeastRecentPtr) {
                            // Move to least recent.
                            int lessPtr = nodes.get(ptr + LESS_RECENT_PTR_FIELD);
                            if (ptr == mMostRecentPtr) {
                                mMostRecentPtr = lessPtr;
                            } else {
                                int morePtr = nodes.get(ptr + MORE_RECENT_PTR_FIELD);
                                nodes.put(lessPtr + MORE_RECENT_PTR_FIELD, morePtr);
                                nodes.put(morePtr + LESS_RECENT_PTR_FIELD, lessPtr);
                            }
                            nodes.put(mLeastRecentPtr + LESS_RECENT_PTR_FIELD, ptr);
                            nodes.put(ptr + MORE_RECENT_PTR_FIELD, mLeastRecentPtr);
                            mLeastRecentPtr = ptr;
                        }

                        // Remove from hashtable.
                        if (prevPtr < 0) {
                            hashTable[index] = chainNextPtr;
                        } else {
                            nodes.put(prevPtr + CHAIN_NEXT_PTR_FIELD, chainNextPtr);
                        }

                        // Node is unused.
                        nodes.put(ptr + CHAIN_NEXT_PTR_FIELD, UNUSED_NODE);

                        return true;
                    }

                    if (chainNextPtr < 0) {
                        break;
                    }

                    prevPtr = ptr;
                    ptr = chainNextPtr;
                }
            }

            return false;
        } finally {
            releaseExclusive();
        }
    }

    @Override
    public long capacity() {
        return mNodesByteBuffer.capacity() + mData.capacity();
    }

    @Override
    public long maxEntryCount() {
        return mHashTable.length;
    }

    @Override
    public void close() {
        acquireExclusive();
        try {
            if (mNodes != null) {
                try {
                    Utils.delete(mNodesByteBuffer);
                    Utils.delete(mData);
                } finally {
                    mNodes = null;
                    mData = null;
                }
            }
        } finally {
            releaseExclusive();
        }
    }

    @FunctionalInterface
    static interface CopyToBB {
        void copyToBB(ByteBuffer dst, int pageSize);
    }

    @FunctionalInterface
    static interface CopyFromBB {
        void copyFromBB(ByteBuffer src, int pageSize);
    }

    private static long getPageId(IntBuffer nodes, int ptr) {
        return (nodes.get(ptr + PAGE_ID_FIELD) & 0xffffffffL)
            | (((long) nodes.get(ptr + (PAGE_ID_FIELD + 1))) << 32);
    }

    private static void setPageId(IntBuffer nodes, int ptr, long pageId) {
        nodes.put(ptr + PAGE_ID_FIELD, (int) pageId);
        nodes.put(ptr + (PAGE_ID_FIELD + 1), (int) (pageId >>> 32));
    }

    private static int hash(long pageId) {
        return Long.hashCode(pageId) & 0x7fffffff;
    }
}
