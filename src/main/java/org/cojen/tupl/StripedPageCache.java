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
final class StripedPageCache implements PageCache {
    private final PageCache[] mStripes;
    private final long mStripeShift;
    private final long mCapacity;
    private final long mMaxEntryCount;

    /**
     * @param capacity capacity in bytes
     */
    StripedPageCache(long capacity, int pageSize) {
        this(capacity, pageSize, Runtime.getRuntime().availableProcessors() * 4);
    }

    /**
     * @param capacity capacity in bytes
     */
    StripedPageCache(long capacity, final int pageSize, int minStripes) {
        capacity = Math.min(capacity, 0x1000_0000_0000_0000L);
        minStripes = (int) Math.max(minStripes, (capacity + 0x3fff_ffffL) / 0x4000_0000L);

        final int s_count = Utils.roundUpPower2(minStripes);
        final double s_size = capacity / (double) s_count;
        final int[] s_capacities = new int[s_count];

        capacity = 0;
        long maxEntryCount = 0;

        for (int i=0; i<s_count; i++) {
            int s_capacity = (int) (((long) ((i + 1) * s_size)) - capacity);
            int s_entryCount = BasicPageCache.entryCountFor(s_capacity, pageSize);
            s_capacities[i] = BasicPageCache.capacityFor(s_entryCount, pageSize);
            capacity += s_capacities[i];
            maxEntryCount += s_entryCount;
        }

        mStripes = new PageCache[s_count];
        mStripeShift = Long.numberOfLeadingZeros(s_count - 1);
        mCapacity = capacity;
        mMaxEntryCount = maxEntryCount;

        int procCount = Runtime.getRuntime().availableProcessors();

        try {
            if (capacity <= 0x1_000_000L || procCount <= 1) {
                for (int i=s_count; --i>=0; ) {
                    mStripes[i] = new BasicPageCache(s_capacities[i], pageSize);
                }
            } else {
                // Initializing the buffers takes a long time, so do in parallel.

                final AtomicInteger slot = new AtomicInteger(s_count);

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
                                mStripes[i] = new BasicPageCache(s_capacities[i], pageSize);
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
        return mStripes[(int) (pageId >>> mStripeShift)].add(pageId, page, offset, canEvict);
    }

    @Override
    public boolean add(long pageId, long pagePtr, int offset, boolean canEvict) {
        pageId = Utils.scramble(pageId);
        return mStripes[(int) (pageId >>> mStripeShift)].add(pageId, pagePtr, offset, canEvict);
    }

    @Override
    public boolean copy(long pageId, int start, byte[] page, int offset) {
        pageId = Utils.scramble(pageId);
        return mStripes[(int) (pageId >>> mStripeShift)].copy(pageId, start, page, offset);
    }

    @Override
    public boolean copy(long pageId, int start, long pagePtr, int offset) {
        pageId = Utils.scramble(pageId);
        return mStripes[(int) (pageId >>> mStripeShift)].copy(pageId, start, pagePtr, offset);
    }

    @Override
    public boolean remove(long pageId, byte[] page, int offset, int length) {
        pageId = Utils.scramble(pageId);
        return mStripes[(int) (pageId >>> mStripeShift)].remove(pageId, page, offset, length);
    }

    @Override
    public boolean remove(long pageId, long pagePtr, int offset, int length) {
        pageId = Utils.scramble(pageId);
        return mStripes[(int) (pageId >>> mStripeShift)].remove(pageId, pagePtr, offset, length);
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
        for (PageCache cache : mStripes) {
            if (cache != null) {
                cache.close();
            }
        }
    }
}
