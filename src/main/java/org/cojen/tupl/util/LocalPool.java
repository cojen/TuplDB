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
 * afinity. A thread should only request a single object from the pool to prevent deadlocks.
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

    private final ThreadLocal<TheEntry<B>> mLocalEntry;
    private final Supplier<B> mSupplier;
    private final TheEntry<B>[] mAllEntries;

    private int mNumEntries;

    /**
     * Construct a pool with a maximum size equal to the number of available processors.
     */
    public LocalPool(Supplier<B> supplier) {
        this(supplier, -1);
    }

    /**
     * Construct a pool with the given maximum size. If negative, then this is multiplied by
     * the number of available processors and negated.
     */
    @SuppressWarnings("unchecked")
    public LocalPool(Supplier<B> supplier, int maxSize) {
        if (maxSize <= 0) {
            if (maxSize == 0) {
                throw new IllegalArgumentException();
            }
            maxSize = -(maxSize * Runtime.getRuntime().availableProcessors());
        }
        mLocalEntry = new ThreadLocal<>();
        mSupplier = supplier;
        mAllEntries = (TheEntry<B>[]) new TheEntry[maxSize];
        for (int i=0; i<maxSize; i++) {
            mAllEntries[i] = new TheEntry<>(supplier.get());
            mAllEntries[i].release();
        }
        mNumEntries = maxSize;
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
            TheEntry<B>[] allEntries = mAllEntries;
            int numEntries = (int) cNumEntriesHandle.getOpaque(this);

            if (numEntries < allEntries.length) expand: {
                synchronized (this) {
                    numEntries = mNumEntries;
                    if (numEntries >= allEntries.length) {
                        break expand;
                    }
                    entry = new TheEntry<B>(mSupplier.get());
                    allEntries[numEntries] = entry;
                    mNumEntries = numEntries + 1;
                }
                break doAccess;
            }

            entry = allEntries[ThreadLocalRandom.current().nextInt(allEntries.length)];
            entry.acquireExclusive();
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

        synchronized (this) {
            int num = mNumEntries;
            if (num <= 0) {
                return;
            }
            removed = Arrays.copyOfRange(mAllEntries, 0, num);
            mNumEntries = 0;
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
    public static interface Entry<B> {
        /**
         * Return the pooled instance, which is locked exclusively.
         */
        public B get();

        /**
         * Release the entry such that another thread can use the pooled object.
         */
        public void release();
    }

    private static class TheEntry<B> extends Latch implements Entry<B> {
        private final B mInstance;

        private TheEntry(B instance) {
            super(EXCLUSIVE);
            mInstance = instance;
        }

        @Override
        public B get() {
            return mInstance;
        }

        @Override
        public void release() {
            releaseExclusive();
        }
    }
}
