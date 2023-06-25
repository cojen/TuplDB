/*
 *  Copyright 2020 Cojen.org
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

package org.cojen.tupl.util;

import java.util.Arrays;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.util.concurrent.ThreadLocalRandom;

import java.util.function.Consumer;
import java.util.function.Supplier;

import org.cojen.tupl.io.Utils;

/**
 * Utility for sharing a small set of poolable objects (like buffers) which have thread
 * affinity. A thread should only request a single object from the pool to prevent deadlocks.
 *
 * @author Brian S O'Neill
 */
public final class LocalPool<B> {
    private static final VarHandle cNumEntriesHandle;

    static {
        try {
            cNumEntriesHandle = MethodHandles.lookup().findVarHandle
                (LocalPool.class, "mNumEntries", int.class);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    private final Latch mFullLatch;
    private final ThreadLocal<TheEntry<B>> mLocalEntry;
    private final Supplier<B> mSupplier;
    private final TheEntry<B>[] mAllEntries;

    private int mNumEntries;

    /**
     * Construct a pool with a maximum size equal to the number of available processors.
     *
     * @param supplier can pass null to create entries which initially reference null
     */
    public LocalPool(Supplier<B> supplier) {
        this(supplier, -1);
    }

    /**
     * Construct a pool with the given maximum size. If negative, then this is multiplied by
     * the number of available processors and negated.
     *
     * @param supplier can pass null to create entries which initially reference null
     */
    @SuppressWarnings("unchecked")
    public LocalPool(Supplier<B> supplier, int maxSize) {
        if (maxSize <= 0) {
            if (maxSize == 0) {
                throw new IllegalArgumentException();
            }
            maxSize = -(maxSize * Runtime.getRuntime().availableProcessors());
        }
        mFullLatch = new Latch();
        mLocalEntry = new ThreadLocal<>();
        mSupplier = supplier;
        mAllEntries = (TheEntry<B>[]) new TheEntry[maxSize];
    }

    /**
     * Returns an available pooled object, blocking if necessary.
     */
    public Entry<B> access() {
        TheEntry<B> entry = mLocalEntry.get();
        if (entry != null && entry.tryAcquireExclusive()) {
            return entry;
        }
        return accessSlow();
    }

    private Entry<B> accessSlow() {
        TheEntry<B> entry;
        doAccess: {
            var rnd = ThreadLocalRandom.current();
            TheEntry<B>[] allEntries = mAllEntries;
            int numEntries = (int) cNumEntriesHandle.getAcquire(this);

            if (numEntries > 0) {
                entry = allEntries[rnd.nextInt(numEntries)];
                if (entry != null && entry.tryAcquireExclusive()) {
                    break doAccess;
                }
            }

            mFullLatch.acquireShared();
            numEntries = mNumEntries;

            if (numEntries < allEntries.length) expand: {
                if (!mFullLatch.tryUpgrade()) {
                    mFullLatch.releaseShared();
                    mFullLatch.acquireExclusive();
                    numEntries = mNumEntries;
                    if (numEntries >= allEntries.length) {
                        mFullLatch.downgrade();
                        break expand;
                    }
                }

                try {
                    entry = new TheEntry<>(mSupplier == null ? null : mSupplier.get());
                    VarHandle.storeStoreFence();
                    allEntries[numEntries] = entry;
                    mNumEntries = numEntries + 1;
                    break doAccess;
                } finally {
                    mFullLatch.releaseExclusive();
                }
            }

            try {
                entry = allEntries[rnd.nextInt(numEntries)];
                entry.acquireExclusive();
            } finally {
                mFullLatch.releaseShared();
            }
        }

        mLocalEntry.set(entry);
        return entry;
    }

    /**
     * Removes all pooled objects and passes them to the optional consumer for cleanup. Calling
     * this method doesn't prevent new pooled objects from being created concurrently by the
     * original supplier.
     */
    public void clear(Consumer<B> consumer) {
        TheEntry<B>[] removed;

        mFullLatch.acquireExclusive();
        try {
            int num = mNumEntries;
            if (num <= 0) {
                return;
            }
            removed = Arrays.copyOfRange(mAllEntries, 0, num);
            Arrays.fill(mAllEntries, null);
            mNumEntries = 0;
        } finally {
            mFullLatch.releaseExclusive();
        }

        for (var e : removed) {
            e.acquireExclusive();
            if (consumer != null) {
                consumer.accept(e.get());
            }
        }
    }

    /**
     * Entry within a {@link LocalPool}.
     */
    public static sealed interface Entry<B> {
        /**
         * Return the pooled object, which is locked exclusively.
         */
        public B get();

        /**
         * Replace the pooled object. This should only be called while the entry is locked
         * exclusively.
         */
        public void replace(B instance);

        /**
         * Release the entry such that another thread can use the pooled object.
         */
        public void release();
    }

    private static final class TheEntry<B> extends Latch implements Entry<B> {
        private B mInstance;

        private TheEntry(B instance) {
            super(EXCLUSIVE);
            mInstance = instance;
        }

        @Override
        public B get() {
            return mInstance;
        }

        @Override
        public void replace(B instance) {
            mInstance = instance;
        }

        @Override
        public void release() {
            releaseExclusive();
        }
    }
}
