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

package org.cojen.tupl.core;

import org.cojen.tupl.util.Latch;
import org.cojen.tupl.util.Parker;

/**
 * Special kind of condition variable in which threads wait for a sequence to reach a specific
 * value. This allows multiple threads to perform work out of order but apply their results in
 * order.
 *
 * @author Brian S O'Neill
 */
final class Sequencer extends Latch {
    // Hashtable of waiters.
    private Waiter[] mWaiters;
    private int mWaitersSize;

    private long mValue;

    /**
     * @param initial initial sequence value
     * @param numWaiters initial number of potential waiters
     * @throws IllegalArgumentException if initial is max value
     */
    public Sequencer(long initial, int numWaiters) {
        if (initial == Long.MAX_VALUE) {
            throw new IllegalArgumentException();
        }
        mWaiters = new Waiter[Utils.roundUpPower2(numWaiters)];
        mValue = initial;
    }

    /**
     * Wait until the sequence value is exactly equal to the given value.
     *
     * @param waiter thread-local instance; pass null to create a new instance automatically
     * @return false if aborted
     * @throws IllegalStateException if another thread is waiting on the same value or if the
     * sequence value is higher than the given value
     */
    public boolean await(long value, Waiter waiter) throws InterruptedException {
        boolean registered = false;
        while (true) {
            acquireExclusive();
            try {
                if (mValue == Long.MAX_VALUE) {
                    return false;
                }
                if (mValue == value) {
                    return true;
                }
                if (mValue > value) {
                    throw new IllegalStateException();
                }

                if (!registered) {
                    if (waiter == null) {
                        waiter = new Waiter();
                    }

                    Waiter[] waiters = mWaiters;
                    int index = ((int) value) & (waiters.length - 1);
                    for (Waiter w = waiters[index]; w != null; w = w.mNext) {
                        if (w.mValue == value) {
                            throw new IllegalStateException();
                        }
                    }

                    if (mWaitersSize > waiters.length) {
                        grow();
                        waiters = mWaiters;
                        index = ((int) value) & (waiters.length - 1);
                    }

                    waiter.mValue = value;
                    waiter.mNext = waiters[index];
                    waiters[index] = waiter;
                    mWaitersSize++;
                    registered = true;
                }
            } finally {
                releaseExclusive();
            }

            // The Sequencer is expected to be used to coordinate threads which are blocked on
            // file I/O, and so parking immediately is the most efficient thing to do. As long
            // as enough threads are running, there's little risk of a CPU core going to sleep.
            Parker.parkNow(this);

            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        }
    }

    private void grow() {
        Waiter[] waiters = mWaiters;

        int capacity = waiters.length << 1;
        var newWaiters = new Waiter[capacity];
        int newMask = capacity - 1;

        for (int i=waiters.length; --i>=0 ;) {
            for (Waiter w = waiters[i]; w != null; ) {
                Waiter next = w.mNext;
                int ix = ((int) w.mValue) & newMask;
                w.mNext = newWaiters[ix];
                newWaiters[ix] = w;
                w = next;
            }
        }

        mWaiters = newWaiters;
    }

    /**
     * Updates the sequence value and wakes up any thread waiting for it.
     *
     * @return false if aborted
     * @throws IllegalStateException if the given value isn't higher than the current sequence
     * value
     */
    public boolean signal(long value) {
        Waiter w;
        findWaiter: {
            acquireExclusive();
            try {
                if (value <= mValue) {
                    if (mValue == Long.MAX_VALUE) {
                        return false;
                    }
                    throw new IllegalStateException();
                }

                mValue = value;

                Waiter[] waiters = mWaiters;
                int index = ((int) value) & (waiters.length - 1);
                Waiter prev = null;
                for (w = waiters[index]; w != null; w = w.mNext) {
                    if (w.mValue == value) {
                        if (prev == null) {
                            waiters[index] = w.mNext;
                        } else {
                            prev.mNext = w.mNext;
                        }
                        mWaitersSize--;
                        break findWaiter;
                    } else {
                        prev = w;
                    }
                }

                return true;
            } finally {
                releaseExclusive();
            }
        }

        Parker.unpark(w.mThread);

        w.mNext = null;

        return true;
    }

    /**
     * Wakes up all waiting threads.
     */
    public void abort() {
        acquireExclusive();
        try {
            mValue = Long.MAX_VALUE;

            Waiter[] waiters = mWaiters;
            for (int i=0; i<waiters.length; i++) {
                for (Waiter w = waiters[i]; w != null; ) {
                    Parker.unpark(w.mThread);
                    Waiter next = w.mNext;
                    w.mNext = null;
                    w = next;
                }
                waiters[i] = null;
            }

            mWaitersSize = 0;
        } finally {
            releaseExclusive();
        }
    }

    public static class Waiter {
        private final Thread mThread;

        private long mValue;
        private Waiter mNext;

        public Waiter() {
            mThread = Thread.currentThread();
        }
    }
}
