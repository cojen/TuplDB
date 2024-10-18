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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.util.concurrent.ThreadLocalRandom;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.util.Latch;
import org.cojen.tupl.util.Runner;

/**
 * 
 *
 * @author Brian S O'Neill
 */
/*P*/
final class PendingTxnFinisher extends Latch {
    private final Worker[] mWorkers;
    private final long mMaxLag;
    private int mLastSelected;

    PendingTxnFinisher(int maxThreads) {
        mWorkers = new Worker[maxThreads];
        for (int i=0; i<mWorkers.length; i++) {
            mWorkers[i] = new Worker();
        }
        mMaxLag = 1_000_000; // TODO: configurable?
    }

    void enqueue(PendingTxn first, PendingTxn last) {
        long lastCommitPos = last.commitPos();

        acquireExclusive();
        try {
            // Start the search just lower than the last one selected, to drive tasks towards the
            // lower workers. The higher workers can then idle and allow their threads to exit.
            int slot = Math.max(0, mLastSelected - 1);

            for (int i=0; i<mWorkers.length; i++) {
                Worker w = mWorkers[slot];
                if (lastCommitPos - w.commitPos() <= mMaxLag) {
                    w.enqueue(first, last);
                    mLastSelected = slot;
                    return;
                }
                slot++;
                if (slot >= mWorkers.length) {
                    slot = 0;
                }
            }

            mLastSelected = slot = ThreadLocalRandom.current().nextInt(mWorkers.length);

            mWorkers[slot].enqueue(first, last);
        } finally {
            releaseExclusive();
        }
    }

    /**
     * Signal all threads, to help them exit sooner.
     */
    void interrupt() {
        for (Worker w : mWorkers) {
            w.interrupt();
        }
    }

    private static class Worker extends Latch implements Runnable {
        private static final VarHandle cCommitPosHandle;

        static {
            try {
                var lookup = MethodHandles.lookup();
                cCommitPosHandle = lookup.findVarHandle(Worker.class, "mCommitPos", long.class);
            } catch (Throwable e) {
                throw Utils.rethrow(e);
            }
        }

        private final Latch.Condition mIdleCondition;

        private PendingTxn mFirst, mLast;
        private long mCommitPos;
        private boolean mRunning;

        private Worker() {
            mIdleCondition = new Latch.Condition();
            mCommitPos = Long.MAX_VALUE;
        }

        long commitPos() {
            return (long) cCommitPosHandle.getOpaque(this);
        }

        void enqueue(PendingTxn first, PendingTxn last) {
            acquireExclusive();
            try {
                if (mLast == null) {
                    mFirst = first;
                    cCommitPosHandle.setOpaque(this, first.commitPos());
                } else {
                    mLast.setNextPlain(first);
                }

                mLast = last;

                if (!mRunning) {
                    try {
                        Runner.start("PendingTxnFinisher", this);
                        mRunning = true;
                    } catch (Throwable e) {
                        // Possibly out of memory. Try again later.
                    }
                } else {
                    mIdleCondition.signal(this);
                }
            } finally {
                releaseExclusive();
            }
        }

        void interrupt() {
            acquireExclusive();
            mIdleCondition.signal(this);
            releaseExclusive();
        }

        @Override
        public void run() {
            while (true) {
                int awaitResult = 1; // signaled
                long delta = 0;

                PendingTxn first, last;

                acquireExclusive();
                try {
                    while (true) {
                        first = mFirst;
                        if (first != null) {
                            last = mLast;
                            mFirst = null;
                            mLast = null;
                            cCommitPosHandle.setOpaque(this, last.commitPos());
                            break;
                        }

                        // Indicate that this worker is all caught up.
                        cCommitPosHandle.setOpaque(this, Long.MAX_VALUE);

                        if (awaitResult <= 0) { // interrupted or timed out
                            mRunning = false;
                            return;
                        }

                        long nanosTimeout = 10_000_000_000L;
                        long nanosEnd = System.nanoTime() + nanosTimeout;
                        awaitResult = mIdleCondition.await(this, nanosTimeout, nanosEnd);
                    }
                } finally {
                    releaseExclusive();
                }

                delta = last.commitPos() - first.commitPos();

                while (true) {
                    try {
                        first.run();
                    } catch (Throwable e) {
                        // PendingTxn should catch and report any exceptions, but just in case
                        // something leaks out, ignore it and move on.
                    }
                    if (first == last) {
                        break;
                    }
                    first = first.getNextPlain();
                }
            }
        }
    }
}
