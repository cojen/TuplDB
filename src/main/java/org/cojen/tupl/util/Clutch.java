/*
 *  Copyright (C) 2017 Cojen.org
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

import java.util.concurrent.ThreadLocalRandom;

import java.util.concurrent.locks.LockSupport;

/**
 * A clutch is a specialized latch which can support highly concurrent shared requests, under
 * the assumption that exclusive requests are infrequent. When too many shared requests are
 * denied due to high contention, the clutch switches to a special contended mode. Later, when
 * an exclusive clutch is acquired, the mode switches back to non-contended mode. This design
 * allows the clutch to be adaptive, by relying on the exclusive clutch as a signal that access
 * patterns have changed.
 *
 * <p>Note: Shared access should not be held by any thread indefinitely. If another thread
 * attempts to switch to contended mode, it first needs to acquire exlusive access in order to
 * make the switch. The thread will block even though shared access could have been granted if
 * it just kept trying. This behavior holds true for downgrades as well. Another thread cannot
 * switch to contended mode until after the downgraded latch is fully released.
 *
 * @author Brian S O'Neill
 */
public abstract class Clutch extends Latch {
    // Inherited latch methods are used for non-contended mode, and for switching to it.

    // Is >=0 when in contended mode.
    private volatile int mContendedSlot = -1;

    public Clutch() {
    }

    /**
     * @param initialState UNLATCHED, EXCLUSIVE, or SHARED
     */
    public Clutch(int initialState) {
        super(initialState);
    }

    /**
     * Returns true if clutch is operating in contended mode.
     */
    public final boolean isContended() {
        return mContendedSlot >= 0;
    }

    @Override
    public final boolean tryAcquireExclusive() {
        if (!super.tryAcquireExclusive()) {
            return false;
        }
        int slot = mContendedSlot;
        if (slot >= 0) {
            if (!getPack().tryUnregisterExclusive(slot, this)) {
                super.releaseExclusive();
                return false;
            }
            mContendedSlot = -1;
        }
        return true;
    }

    @Override
    public final boolean tryAcquireExclusiveNanos(long nanosTimeout) throws InterruptedException {
        if (nanosTimeout < 0) {
            acquireExclusiveInterruptibly();
            return true;
        }

        long start = System.nanoTime();
        if (!super.tryAcquireExclusiveNanos(nanosTimeout)) {
            return false;
        }

        nanosTimeout -= (System.nanoTime() - start);
        if (nanosTimeout < 0) {
            nanosTimeout = 0;
        }

        int slot = mContendedSlot;
        if (slot >= 0) {
            if (!getPack().tryUnregisterExclusiveNanos(slot, this, nanosTimeout)) {
                super.releaseExclusive();
                return false;
            }
            mContendedSlot = -1;
        }

        return true;
    }

    @Override
    public final void acquireExclusive() {
        super.acquireExclusive();
        int slot = mContendedSlot;
        if (slot >= 0) {
            getPack().unregisterExclusive(slot);
            mContendedSlot = -1;
        }
    }

    @Override
    public final void acquireExclusiveInterruptibly() throws InterruptedException {
        super.acquireExclusiveInterruptibly();
        int slot = mContendedSlot;
        if (slot >= 0) {
            getPack().tryUnregisterExclusiveNanos(slot, this, -1);
            mContendedSlot = -1;
        }
    }

    /**
     * Downgrade the held exclusive latch with the option to try switching to contended mode.
     * Caller must later call releaseShared instead of releaseExclusive.
     *
     * @param contended pass true to try switching to contended mode
     */
    public final void downgrade(boolean contended) {
        if (contended) {
            Pack pack = getPack();
            int slot = pack.tryRegister(this);
            if (slot >= 0) {
                mContendedSlot = slot;
                if (!pack.tryAcquireShared(slot, this)) {
                    throw new AssertionError();
                }
                super.releaseExclusive();
                return;
            }
        }

        super.downgrade();
    }

    /**
     * Release the held exclusive latch with the option to try switching to contended mode.
     *
     * @param contended pass true to try switching to contended mode
     */
    public final void releaseExclusive(boolean contended) {
        if (contended) {
            mContendedSlot = getPack().tryRegister(this);
        }
        super.releaseExclusive();
    }

    @Override
    public final boolean tryAcquireShared() {
        int slot = mContendedSlot;
        if (slot < 0 || !getPack().tryAcquireShared(slot, this)) {
            if (!super.tryAcquireShared()) {
                return false;
            }
            uncontendedMode();
        }
        return true;
    }

    @Override
    public final boolean tryAcquireSharedNanos(long nanosTimeout) throws InterruptedException {
        if (nanosTimeout < 0) {
            acquireSharedInterruptibly();
            return true;
        }

        doAcquire: {
            int slot = mContendedSlot;
            if (slot >= 0) {
                if (getPack().tryAcquireShared(slot, this)) {
                    return true;
                }
            } else {
                long start = System.nanoTime();
                int result = acquireSharedUncontendedNanos(nanosTimeout);
                if (result > 0) {
                    break doAcquire;
                } else if (result == 0) {
                    return false;
                }
                nanosTimeout -= (System.nanoTime() - start);
                if (nanosTimeout < 0) {
                    nanosTimeout = 0;
                }
                if (shouldSwitchToContendedMode()) {
                    if (super.tryAcquireShared()) {
                        break doAcquire;
                    }
                    if (!super.tryAcquireExclusiveNanos(nanosTimeout)) {
                        return false;
                    }
                    contendedMode();
                    return true;
                }
            }

            if (!super.tryAcquireSharedNanos(nanosTimeout)) {
                return false;
            }
        }

        uncontendedMode();
        return true;
    }

    @Override
    public final boolean acquireSharedUncontended() {
        int slot = mContendedSlot;
        if (slot < 0 || !getPack().tryAcquireShared(slot, this)) {
            if (!super.acquireSharedUncontended()) {
                return false;
            }
            uncontendedMode();
        }
        return true;
    }

    @Override
    public final int acquireSharedUncontendedNanos(long nanosTimeout) throws InterruptedException {
        int slot = mContendedSlot;
        if (slot < 0 || !getPack().tryAcquireShared(slot, this)) {
            int result = super.acquireSharedUncontendedNanos(nanosTimeout);
            if (result <= 0) {
                return result;
            }
            uncontendedMode();
        }
        return 1;
    }

    @Override
    public final void acquireShared() {
        doAcquire: {
            int slot = mContendedSlot;
            if (slot >= 0) {
                if (getPack().tryAcquireShared(slot, this)) {
                    return;
                }
            } else {
                if (super.acquireSharedUncontended()) {
                    break doAcquire;
                }
                if (shouldSwitchToContendedMode()) {
                    if (super.tryAcquireShared()) {
                        break doAcquire;
                    }
                    super.acquireExclusive();
                    contendedMode();
                    return;
                }
            }

            super.acquireShared();
        }

        uncontendedMode();
    }

    @Override
    public final void acquireSharedInterruptibly() throws InterruptedException {
        doAcquire: {
            int slot = mContendedSlot;
            if (slot >= 0) {
                if (getPack().tryAcquireShared(slot, this)) {
                    return;
                }
            } else {
                if (super.acquireSharedUncontendedNanos(-1) > 0) {
                    break doAcquire;
                }
                if (shouldSwitchToContendedMode()) {
                    if (super.tryAcquireShared()) {
                        break doAcquire;
                    }
                    super.acquireExclusiveInterruptibly();
                    contendedMode();
                    return;
                }
            }

            super.acquireSharedInterruptibly();
        }

        uncontendedMode();
    }

    private static boolean shouldSwitchToContendedMode() {
        // Switching to contended mode can be expensive, so do it infrequently.
        return (ThreadLocalRandom.current().nextInt() & 0x0ff) == 0;
    }

    /**
     * Attempt to switch to contended mode and retain a shared acquisition. Caller must have
     * acquired the exclusive latch, which is released or downgraded by this method.
     */
    private void contendedMode() {
        Pack pack = getPack();
        int slot = mContendedSlot;
        if (slot < 0) {
            slot = pack.tryRegister(this);
            if (slot < 0) {
                // No slots are available.
                super.downgrade();
                return;
            }
            mContendedSlot = slot;
        }

        if (!pack.tryAcquireShared(slot, this)) {
            throw new AssertionError();
        }

        super.releaseExclusive();
    }

    /**
     * Called after a shared latch has been acquired in uncontended mode.
     */
    private void uncontendedMode() {
        int slot = mContendedSlot;
        if (slot >= 0) {
            if (!getPack().tryAcquireShared(slot, this)) {
                throw new AssertionError();
            }
            super.releaseShared();
        }
    }

    @Override
    public final boolean tryUpgrade() {
        // With shared clutch held, another thread cannot switch to contended mode. Hence, no
        // double check is required here.
        return mContendedSlot < 0 && super.tryUpgrade();
    }

    @Override
    public final void releaseShared() {
        // TODO: can be non-volatile read
        int slot = mContendedSlot;
        if (slot < 0) {
            super.releaseShared();
        } else {
            getPack().releaseShared(slot);
        }
    }

    @Override
    public String toString() {
        if (mContendedSlot < 0) {
            return super.toString();
        }
        StringBuilder b = new StringBuilder();
        appendMiniString(b, this);
        return b.append(" {state=").append("contended").append('}').toString();
    }

    /**
     * Returns the pack associated with this clutch, which should be shared to reduce the
     * overall memory footprint.
     */
    protected abstract Pack getPack();

    /**
     * Sharable object for supporting contended clutches. Memory overhead (in bytes) is
     * proportional to {@code (number of slots) * (number of cores)}. The number of slots
     * should be at least 16, to minimize cache line contention. As a convenience, this class
     * also extends the Latch class, but the latching features are not used here.
     */
    @SuppressWarnings("restriction")
    public static class Pack extends Latch {
        private static final int OBJECT_ARRAY_BASE;
        private static final int OBJECT_ARRAY_SHIFT;
        private static final int INT_ARRAY_BASE;
        private static final int INT_ARRAY_SHIFT;

        static {
            OBJECT_ARRAY_BASE = UNSAFE.arrayBaseOffset(Object[].class);
            OBJECT_ARRAY_SHIFT = computeShift(UNSAFE.arrayIndexScale(Object[].class));
            INT_ARRAY_BASE = UNSAFE.arrayBaseOffset(int[].class);
            INT_ARRAY_SHIFT = computeShift(UNSAFE.arrayIndexScale(int[].class));
        }

        private static int computeShift(int scale) {
            if ((scale & (scale - 1)) != 0) {
                throw new Error("data type scale not a power of two");
            }
            return 31 - Integer.numberOfLeadingZeros(scale);
        }

        private final int mCores;
        private final Object[] mSlots;
        private final int[] mCounters;
        private final int[] mThreadStripes;

        /**
         * @param numSlots amount of contended clutches that this pack can support
         */
        public Pack(int numSlots) {
            this(numSlots, Runtime.getRuntime().availableProcessors());
        }

        /**
         * @param numSlots amount of contended clutches that this pack can support
         * @param cores must be at least 1 CPU core
         */
        public Pack(int numSlots, int cores) {
            if (cores < 1) {
                throw new IllegalArgumentException();
            }
            mCores = cores;
            mSlots = new Object[numSlots];
            mCounters = new int[numSlots * cores];
            mThreadStripes = new int[cores * 4];
        }

        /**
         * @return selected slot or -1 if none are available at the moment
         */
        final int tryRegister(Clutch clutch) {
            Object[] slots = mSlots;
            int slot = ThreadLocalRandom.current().nextInt(slots.length);

            Object existing = get(slots, slot);
            if (existing == null) {
                if (compareAndSet(slots, slot, null, clutch)) {
                    return slot;
                }
                existing = get(slots, slot);
            }

            // Try to steal the slot.
            if (existing instanceof Clutch) {
                Clutch existingClutch = (Clutch) existing;
                if (existingClutch.tryAcquireExclusive()) {
                    // If acquired exclusive, then slot has been potentially freed.
                    existingClutch.releaseExclusive();
                    if (compareAndSet(slots, slot, null, clutch)) {
                        return slot;
                    }
                }
            }

            return -1;
        }

        /**
         * Tries to acquire the slot exclusively and then unregister the associated
         * clutch. Caller must hold the clutch latch exclusively.
         */
        final boolean tryUnregisterExclusive(int slot, Clutch clutch) {
            if (isZero(slot)) {
                // Store marker to signal that exclusive is requested, but don't unpark.
                set(mSlots, slot, this);
                if (isZero(slot)) {
                    set(mSlots, slot, null);
                    return true;
                }
                set(mSlots, slot, clutch);
            }
            return false;
        }

        /**
         * Tries to acquire the slot exclusively and then unregister the associated
         * clutch. Caller must hold the clutch latch exclusively.
         *
         * @param nanosTimeout is infinite if negative
         */
        final boolean tryUnregisterExclusiveNanos(int slot, Clutch clutch, long nanosTimeout)
            throws InterruptedException
        {
            // Store thread to signal that exclusive is requested, and to call unpark.
            set(mSlots, slot, Thread.currentThread());

            if (nanosTimeout < 0) {
                while (!isZero(slot)) {
                    LockSupport.park(this);
                    if (Thread.interrupted()) {
                        set(mSlots, slot, clutch);
                        throw new InterruptedException();
                    }
                }
            } else if (!isZero(slot)) {
                long start = System.nanoTime();
                while (true) {
                    LockSupport.parkNanos(this, nanosTimeout);
                    if (Thread.interrupted()) {
                        set(mSlots, slot, clutch);
                        throw new InterruptedException();
                    }
                    if (isZero(slot)) {
                        break;
                    }
                    long now = System.nanoTime();
                    nanosTimeout -= (now - start);
                    if (nanosTimeout <= 0) {
                        set(mSlots, slot, clutch);
                        return false;
                    }
                    start = now;
                }
            }

            set(mSlots, slot, null);
            return true;
        }

        /**
         * Acquires the slot exclusively and then unregisters the associated clutch. Caller
         * must hold the clutch latch exclusively.
         */
        final void unregisterExclusive(int slot) {
            // Store thread to signal that exclusive is requested, and to call unpark.
            set(mSlots, slot, Thread.currentThread());
            while (!isZero(slot)) {
                LockSupport.park(this);
                // Clear the interrupted flag.
                Thread.interrupted();
            }
            set(mSlots, slot, null);
        }

        /**
         * @return false if exclusive is requested, or if clutch has been evicted
         */
        final boolean tryAcquireShared(int slot, Clutch clutch) {
            int stripe = add(slot, +1);
            if (get(mSlots, slot) == clutch) {
                return true;
            }
            releaseShared(slot, stripe);
            return false;
        }

        final void releaseShared(int slot) {
            releaseShared(slot, threadStripe());
        }

        private void releaseShared(int slot, int stripe) {
            add(slot, -1, stripe);
            Object entry = get(mSlots, slot);
            if (entry instanceof Thread && isZero(slot)) {
                LockSupport.unpark((Thread) entry);
            }
        }

        /**
         * @return selected stripe
         */
        private int add(int slot, int delta) {
            int stripe = threadStripe();
            long offset = intArrayByteOffset(slot + stripe);
            int[] counters = mCounters;
            int cv = UNSAFE.getIntVolatile(counters, offset);

            if (!UNSAFE.compareAndSwapInt(counters, offset, cv, cv + delta)) {
                stripe = ThreadLocalRandom.current().nextInt(mCores) * mSlots.length;
                int id = xorshift((int) Thread.currentThread().getId());
                mThreadStripes[id & (mThreadStripes.length - 1)] = stripe;
                add(slot, delta, stripe);
            }

            return stripe;
        }

        private void add(int slot, int delta, int stripe) {
            UNSAFE.getAndAddInt(mCounters, intArrayByteOffset(slot + stripe), delta);
        }

        private int threadStripe() {
            int id = xorshift((int) Thread.currentThread().getId());
            return mThreadStripes[id & (mThreadStripes.length - 1)];
        }

        private static int xorshift(int v) {
            v ^= v << 13;
            v ^= v >>> 17;
            v ^= v << 5;
            return v;
        }

        private boolean isZero(int slot) {
            int[] counters = mCounters;
            int stride = mSlots.length;
            long sum = 0;
            for (int i = 0; i < mCores; i++, slot += stride) {
                sum += UNSAFE.getIntVolatile(counters, intArrayByteOffset(slot));
            }
            return sum == 0;
        }

        private static Object get(Object[] array, int i) {
            return UNSAFE.getObjectVolatile(array, objectArrayByteOffset(i));
        }

        private static void set(Object[] array, int i, Object value) {
            UNSAFE.putObjectVolatile(array, objectArrayByteOffset(i), value);
        }

        private static boolean compareAndSet(Object[] array, int i, Object expect, Object update) {
            return UNSAFE.compareAndSwapObject(array, objectArrayByteOffset(i), expect, update);
        }

        private static long objectArrayByteOffset(int i) {
            return ((long) i << OBJECT_ARRAY_SHIFT) + OBJECT_ARRAY_BASE;
        }

        private static long intArrayByteOffset(int i) {
            return ((long) i << INT_ARRAY_SHIFT) + INT_ARRAY_BASE;
        }
    }
}
