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

/**
 * Page cache which is spread across several direct page caches, improving concurrency and
 * supporting a much larger maximum capacity.
 *
 * @author Brian S O'Neill
 */
class PartitionedPageCache implements PageCache {
    private final DirectPageCache[] mPartitions;
    private final long mPartitionShift;
    private final long mCapacity;
    private final long mMaxEntryCount;

    /**
     * @param capacity capacity in bytes
     */
    PartitionedPageCache(long capacity, int pageSize) {
        this(capacity, pageSize, Runtime.getRuntime().availableProcessors() * 4);
    }

    /**
     * @param capacity capacity in bytes
     */
    PartitionedPageCache(long capacity, int pageSize, int minPartitions) {
        capacity = Math.min(capacity, 0x1000_0000_0000_0000L);
        minPartitions = (int) Math.max(minPartitions, (capacity + 0x3fff_ffffL) / 0x4000_0000L);

        final int pcount = Utils.roundUpPower2(minPartitions);
        final double psize = capacity / (double) pcount;
        final long zeroId = Utils.scramble(0);

        mPartitions = new DirectPageCache[pcount];

        capacity = 0;
        long maxEntryCount = 0;

        try {
            for (int i=0; i<mPartitions.length; i++) {
                int pcapacity = (int) (((long) ((i + 1) * psize)) - capacity);
                DirectPageCache partition = new DirectPageCache(pcapacity, pageSize);
                mPartitions[i] = partition;
                capacity += partition.capacity();
                maxEntryCount += partition.maxEntryCount();
            }
        } catch (Throwable e) {
            close();
            throw e;
        }

        mPartitionShift = Long.numberOfLeadingZeros(pcount - 1);
        mCapacity = capacity;
        mMaxEntryCount = maxEntryCount;
    }

    @Override
    public boolean add(long pageId, byte[] page, int offset, int length, boolean canEvict) {
        pageId = Utils.scramble(pageId);
        return mPartitions[(int) (pageId >>> mPartitionShift)]
            .add(pageId, page, offset, length, canEvict);
    }

    @Override
    public boolean copy(long pageId, int start, byte[] page, int offset, int length) {
        pageId = Utils.scramble(pageId);
        return mPartitions[(int) (pageId >>> mPartitionShift)]
            .copy(pageId, start, page, offset, length);
    }

    @Override
    public boolean remove(long pageId, byte[] page, int offset, int length) {
        pageId = Utils.scramble(pageId);
        return mPartitions[(int) (pageId >>> mPartitionShift)]
            .remove(pageId, page, offset, length);
    }

    @Override
    public long capacity() {
        return mCapacity;
    }

    @Override
    public long maxEntryCount() {
        return mMaxEntryCount;
    }

    @Override
    public void close() {
        for (DirectPageCache cache : mPartitions) {
            if (cache != null) {
                cache.close();
            }
        }
    }
}
