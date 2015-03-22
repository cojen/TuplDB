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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;

import static org.cojen.tupl.PageOps.*;

/**
 * Page cache which uses direct buffers and very few Java objects, eliminating garbage
 * collection overhead. Caller should scramble page ids to reduce hash collisions.
 *
 * Note: Might need to set -XX:MaxDirectMemorySize=99999m
 *
 * @author Brian S O'Neill
 */
final class DirectPageCache extends Latch implements PageCache {
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
    DirectPageCache(int capacity, int pageSize) {
        int entryCount = entryCountFor(capacity, pageSize);

        mPageSize = pageSize;
        mHashTable = new int[entryCount];

        acquireExclusive();

        mNodesByteBuffer = ByteBuffer
            .allocateDirect(entryCount * (NODE_SIZE_IN_INTS * 4))
            .order(ByteOrder.nativeOrder());
        mNodes = mNodesByteBuffer.asIntBuffer();
        mData = ByteBuffer.allocateDirect(entryCount * pageSize);

        // Initalize the nodes, all linked together.
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
                       final /*P*/ byte[] page, final int offset, final int length,
                       final boolean canEvict)
    {
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
                int prevPtr = NO_NEXT_ENTRY;
                while (true) {
                    final int chainNextPtr = nodes.get(ptr + CHAIN_NEXT_PTR_FIELD);
                    if (getPageId(nodes, ptr) == pageId) {
                        // Found it.
                        mData.position((ptr / NODE_SIZE_IN_INTS) * mPageSize);
                        p_copyToBB(page, offset, mData, length);

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
                    prevPtr = ptr;
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
            p_copyToBB(page, offset, mData, length);

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
                        final /*P*/ byte[] page, final int offset, final int length)
    {
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
                int prevPtr = NO_NEXT_ENTRY;
                while (true) {
                    final int chainNextPtr = nodes.get(ptr + CHAIN_NEXT_PTR_FIELD);
                    if (getPageId(nodes, ptr) == pageId) {
                        // Found it.
                        mData.position(((ptr / NODE_SIZE_IN_INTS) * mPageSize) + start);
                        p_copyFromBB(mData, page, offset, length);
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
            releaseShared();
        }
    }

    @Override
    public boolean remove(final long pageId,
                          final /*P*/ byte[] page, final int offset, final int length)
    {
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

                        if (page != PageOps.p_null()) {
                            // Copy data buffer into the page.
                            mData.position((ptr / NODE_SIZE_IN_INTS) * mPageSize);
                            p_copyFromBB(mData, page, offset, length);
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

    private static long getPageId(IntBuffer nodes, int ptr) {
        return (nodes.get(ptr + PAGE_ID_FIELD) & 0xffffffffL)
            | (((long) nodes.get(ptr + (PAGE_ID_FIELD + 1))) << 32);
    }

    private static void setPageId(IntBuffer nodes, int ptr, long pageId) {
        nodes.put(ptr + PAGE_ID_FIELD, (int) pageId);
        nodes.put(ptr + (PAGE_ID_FIELD + 1), (int) (pageId >>> 32));
    }

    private static int hash(long pageId) {
        return (((int) pageId) ^ ((int) (pageId >>> 32))) & 0x7fffffff;
    }
}
