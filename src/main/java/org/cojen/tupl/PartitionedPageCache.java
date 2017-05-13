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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Page cache which is spread across several direct page caches, improving concurrency and
 * supporting a much larger maximum capacity.
 *
 * @author Brian S O'Neill
 */
final class PartitionedPageCache implements PageCache {
    private final PageCache[] mPartitions;
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
    PartitionedPageCache(long capacity, final int pageSize, int minPartitions) {
        capacity = Math.min(capacity, 0x1000_0000_0000_0000L);
        minPartitions = (int) Math.max(minPartitions, (capacity + 0x3fff_ffffL) / 0x4000_0000L);

        final int pcount = Utils.roundUpPower2(minPartitions);
        final double psize = capacity / (double) pcount;
        final int[] pcapacities = new int[pcount];

        capacity = 0;
        long maxEntryCount = 0;

        for (int i=0; i<pcount; i++) {
            int pcapacity = (int) (((long) ((i + 1) * psize)) - capacity);
            int pentryCount = BasicPageCache.entryCountFor(pcapacity, pageSize);
            pcapacities[i] = BasicPageCache.capacityFor(pentryCount, pageSize);
            capacity += pcapacities[i];
            maxEntryCount += pentryCount;
        }

        mPartitions = new PageCache[pcount];
        mPartitionShift = Long.numberOfLeadingZeros(pcount - 1);
        mCapacity = capacity;
        mMaxEntryCount = maxEntryCount;

        int procCount = Runtime.getRuntime().availableProcessors();

        try {
            if (capacity <= 0x1_000_000L || procCount <= 1) {
                for (int i=pcount; --i>=0; ) {
                    mPartitions[i] = new BasicPageCache(pcapacities[i], pageSize);
                }
            } else {
                // Initializing the buffers takes a long time, so do in parallel.

                final AtomicInteger slot = new AtomicInteger(pcount);

                class Init extends Thread {
                    volatile Throwable mEx;

                    public void run() {
                        try {
                            while (true) {
                                int i = slot.get();
                                if (i <= 0) {
                                    break;
                                }
                                if (!slot.compareAndSet(i, --i)) {
                                    continue;
                                }
                                mPartitions[i] = new BasicPageCache(pcapacities[i], pageSize);
                            }
                        } catch (Throwable e) {
                            mEx = e;
                        }
                    }
                }

                Init[] inits = new Init[procCount];

                for (int i=0; i<inits.length; i++) {
                    if (slot.get() <= 0) {
                        break;
                    }
                    (inits[i] = new Init()).start();
                }

                try {
                    for (int i=0; i<inits.length; i++) {
                        Init init = inits[i];
                        if (init != null) {
                            init.join();
                        }
                    }

                    for (int i=0; i<inits.length; i++) {
                        Init init = inits[i];
                        if (init != null) {
                            Throwable e = init.mEx;
                            if (e != null) {
                                throw e;
                            }
                        }
                    }
                } catch (Throwable e) {
                    throw Utils.rethrow(e);
                }
            }
        } catch (Throwable e) {
            close();
            throw e;
        }
    }

    @Override
    public boolean add(long pageId, byte[] page, int offset, boolean canEvict) {
        pageId = Utils.scramble(pageId);
        return mPartitions[(int) (pageId >>> mPartitionShift)]
            .add(pageId, page, offset, canEvict);
    }

    @Override
    public boolean add(long pageId, long pagePtr, int offset, boolean canEvict) {
        pageId = Utils.scramble(pageId);
        return mPartitions[(int) (pageId >>> mPartitionShift)]
            .add(pageId, pagePtr, offset, canEvict);
    }

    @Override
    public boolean copy(long pageId, int start, byte[] page, int offset) {
        pageId = Utils.scramble(pageId);
        return mPartitions[(int) (pageId >>> mPartitionShift)]
            .copy(pageId, start, page, offset);
    }

    @Override
    public boolean copy(long pageId, int start, long pagePtr, int offset) {
        pageId = Utils.scramble(pageId);
        return mPartitions[(int) (pageId >>> mPartitionShift)]
            .copy(pageId, start, pagePtr, offset);
    }

    @Override
    public boolean remove(long pageId, byte[] page, int offset, int length) {
        pageId = Utils.scramble(pageId);
        return mPartitions[(int) (pageId >>> mPartitionShift)]
            .remove(pageId, page, offset, length);
    }

    @Override
    public boolean remove(long pageId, long pagePtr, int offset, int length) {
        pageId = Utils.scramble(pageId);
        return mPartitions[(int) (pageId >>> mPartitionShift)]
            .remove(pageId, pagePtr, offset, length);
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
        for (PageCache cache : mPartitions) {
            if (cache != null) {
                cache.close();
            }
        }
    }
}
