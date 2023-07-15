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
import org.cojen.tupl.util.Runner;

/**
 * 
 *
 * @author Brian S O'Neill
 */
/*P*/
final class PendingTxnFinisher extends Latch implements Runnable {
    private final int mMaxThreads;
    private final Latch.Condition mIdleCondition;

    private PendingTxn mFirst, mLast;

    private int mTotalThreads;

    PendingTxnFinisher(int maxThreads) {
        mMaxThreads = maxThreads;
        mIdleCondition = new Latch.Condition();
    }

    void enqueue(PendingTxn first, PendingTxn last) {
        acquireExclusive();
        try {
            if (mLast == null) {
                mFirst = first;
            } else {
                mLast.setNextPlain(first);
            }
            mLast = last;
            if (mIdleCondition.isEmpty() && mTotalThreads < mMaxThreads) {
                Runner.start("PendingTxnFinisher", this);
                mTotalThreads++;
            } else {
                mIdleCondition.signal(this);
            }
        } finally {
            releaseExclusive();
        }
    }

    /**
     * Signal up all threads, to help them exit sooner.
     */
    void interrupt() {
        acquireExclusive();
        try {
            mIdleCondition.signalAll(this);
        } finally {
            releaseExclusive();
        }
    }

    @Override
    public void run() {
        while (true) {
            boolean waited = false;
            PendingTxn pending;
            acquireExclusive();
            try {
                while (true) {
                    pending = mFirst;
                    if (pending != null) {
                        if (pending == mLast) {
                            mFirst = null;
                            mLast = null;
                        } else {
                            mFirst = pending.getNextPlain();
                        }
                        break;
                    }
                    if (waited) {
                        mTotalThreads--;
                        return;
                    }

                    // Use priorityAwait in order to force some threads to do less work,
                    // allowing them to exit when idle. The total amount of threads will more
                    // closely match the amount that's needed.
                    long nanosTimeout = 10_000_000_000L;
                    long nanosEnd = System.nanoTime() + nanosTimeout;
                    mIdleCondition.priorityAwait(this, nanosTimeout, nanosEnd);

                    waited = true;
                }
            } finally {
                releaseExclusive();
            }

            pending.run();
        }
    }
}
