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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.util.concurrent.ThreadLocalRandom;

import static org.cojen.tupl.io.Utils.rethrow;

/**
 * A clutch is a specialized latch which can support highly concurrent shared requests, under
 * the assumption that exclusive requests are infrequent. When too many shared requests are
 * denied due to high contention, the clutch switches to a special contended mode. Later, when
 * an exclusive clutch is acquired, the mode switches back to non-contended mode. This design
 * allows the clutch to be adaptive, by relying on the exclusive clutch as a signal that access
 * patterns have changed.
 *
 * <p>Note: Shared access should not be held by any thread indefinitely. If another thread
 * attempts to switch to contended mode, it first needs to acquire exclusive access in order to
 * make the switch. The thread will block even though shared access could have been granted if
 * it just kept trying. This behavior holds true for downgrades as well. Another thread cannot
 * switch to contended mode until after the downgraded latch is fully released.
 *
 * @author Brian S O'Neill
 */
public abstract class Clutch extends Latch {
    // Inherited latch methods are used for non-contended mode, and for switching to it.

    private static final VarHandle cContendedSlotHandle;

    static {
        try {
            cContendedSlotHandle =
                MethodHandles.lookup().findVarHandle
                (Clutch.class, "mContendedSlot", int.class);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    // Is >=0 when in contended mode.
    private volatile int mContendedSlot = -1;

    /**
     * Return a new Clutch instance, which might share a pack with other instances returned
     * from this method.
     */
    public static Clutch make() {
        return new Impl(Impl.sharedPack());
    }

    /**
     * Return a new Clutch instance, which might share a pack with other instances returned
     * from this method.
     *
     * @param initialState UNLATCHED, EXCLUSIVE, or SHARED
     */
    public static Clutch make(int initialState) {
        return new Impl(Impl.sharedPack(), initialState);
    }

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

    @Override
    public final void uponExclusive(Runnable cont) {
        super.uponExclusive(() -> {
            int slot = mContendedSlot;
            if (slot >= 0) {
                getPack().unregisterExclusive(slot);
                mContendedSlot = -1;
            }
            cont.run();
        });
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
        return ((int) cContendedSlotHandle.get(this)) < 0 && super.tryUpgrade();
    }

    @Override
    public final void releaseShared() {
        int slot = (int) cContendedSlotHandle.get(this);
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
        var b = new StringBuilder();
        appendMiniString(b, this);
        return b.append('{').append("state=").append("contended").append('}').toString();
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
     * also extends the Latch class, but the latching features aren't used here.
     */
    public static class Pack extends Latch {
        private static final VarHandle cObjectArrayHandle, cIntArrayHandle;

        static {
            cObjectArrayHandle = MethodHandles.arrayElementVarHandle(Object[].class);
            cIntArrayHandle = MethodHandles.arrayElementVarHandle(int[].class);
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

            Object existing = cObjectArrayHandle.getVolatile(slots, slot);
            if (existing == null) {
                if (cObjectArrayHandle.compareAndSet(slots, slot, null, clutch)) {
                    return slot;
                }
                existing = cObjectArrayHandle.getVolatile(slots, slot);
            }

            // Try to steal the slot.
            if (existing instanceof Clutch existingClutch) {
                if (existingClutch.tryAcquireExclusive()) {
                    // If acquired exclusive, then slot has been potentially freed.
                    existingClutch.releaseExclusive();
                    if (cObjectArrayHandle.compareAndSet(slots, slot, null, clutch)) {
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
                cObjectArrayHandle.setVolatile(mSlots, slot, this);
                if (isZero(slot)) {
                    cObjectArrayHandle.setVolatile(mSlots, slot, null);
                    return true;
                }
                cObjectArrayHandle.setVolatile(mSlots, slot, clutch);
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
            cObjectArrayHandle.setVolatile(mSlots, slot, Thread.currentThread());

            if (nanosTimeout < 0) {
                while (!isZero(slot)) {
                    Parker.park(this);
                    if (Thread.interrupted()) {
                        cObjectArrayHandle.setVolatile(mSlots, slot, clutch);
                        throw new InterruptedException();
                    }
                }
            } else if (!isZero(slot)) {
                long start = System.nanoTime();
                while (true) {
                    Parker.parkNanos(this, nanosTimeout);
                    if (Thread.interrupted()) {
                        cObjectArrayHandle.setVolatile(mSlots, slot, clutch);
                        throw new InterruptedException();
                    }
                    if (isZero(slot)) {
                        break;
                    }
                    long now = System.nanoTime();
                    nanosTimeout -= (now - start);
                    if (nanosTimeout <= 0) {
                        cObjectArrayHandle.setVolatile(mSlots, slot, clutch);
                        return false;
                    }
                    start = now;
                }
            }

            cObjectArrayHandle.setVolatile(mSlots, slot, null);
            return true;
        }

        /**
         * Acquires the slot exclusively and then unregisters the associated clutch. Caller
         * must hold the clutch latch exclusively.
         */
        final void unregisterExclusive(int slot) {
            // Store thread to signal that exclusive is requested, and to call unpark.
            cObjectArrayHandle.setVolatile(mSlots, slot, Thread.currentThread());
            while (!isZero(slot)) {
                Parker.park(this);
                // Clear the interrupted flag.
                Thread.interrupted();
            }
            cObjectArrayHandle.setVolatile(mSlots, slot, null);
        }

        /**
         * @return false if exclusive is requested, or if clutch has been evicted
         */
        final boolean tryAcquireShared(int slot, Clutch clutch) {
            int stripe = add(slot, +1);
            if (cObjectArrayHandle.getVolatile(mSlots, slot) == clutch) {
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
            Object entry = cObjectArrayHandle.getVolatile(mSlots, slot);
            if (entry instanceof Thread t && isZero(slot)) {
                Parker.unpark(t);
            }
        }

        /**
         * @return selected stripe
         */
        private int add(int slot, int delta) {
            int stripe = threadStripe();
            int[] counters = mCounters;
            int cv = (int) cIntArrayHandle.getVolatile(counters, slot + stripe);

            if (!cIntArrayHandle.compareAndSet(counters, slot + stripe, cv, cv + delta)) {
                stripe = ThreadLocalRandom.current().nextInt(mCores) * mSlots.length;
                int id = xorshift((int) Thread.currentThread().threadId());
                mThreadStripes[id & (mThreadStripes.length - 1)] = stripe;
                add(slot, delta, stripe);
            }

            return stripe;
        }

        private void add(int slot, int delta, int stripe) {
            cIntArrayHandle.getAndAdd(mCounters, slot + stripe, delta);
        }

        private int threadStripe() {
            int id = xorshift((int) Thread.currentThread().threadId());
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
                sum += (int) cIntArrayHandle.getVolatile(counters, slot);
            }
            return sum == 0;
        }
    }

    private static class Impl extends Clutch {
        private static final int PACK_LIMIT = 16;
        private static Pack cSharedPack;
        private static int cShareCount;

        static synchronized Pack sharedPack() {
            Pack pack = cSharedPack;
            if (pack == null || cShareCount >= PACK_LIMIT) {
                cSharedPack = pack = new Pack(PACK_LIMIT);
                cShareCount = 1;
            } else {
                cShareCount++;
            }
            return pack;
        }

        private final Pack mPack;

        private Impl(Pack pack) {
            mPack = pack;
        }

        private Impl(Pack pack, int initialState) {
            super(initialState);
            mPack = pack;
        }

        @Override
        protected Pack getPack() {
            return mPack;
        }
    }
}
