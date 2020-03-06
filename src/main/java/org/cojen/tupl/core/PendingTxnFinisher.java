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

import org.cojen.tupl.util.Runner;

/**
 * 
 *
 * @author Brian S O'Neill
 */
/*P*/
final class PendingTxnFinisher implements Runnable {
    // FIXME: Actually spin up more threads.
    private final int mMaxThreads;

    private PendingTxn mFirst, mLast;

    private boolean mRunning;

    PendingTxnFinisher() {
        this(Runtime.getRuntime().availableProcessors());
    }

    PendingTxnFinisher(int maxThreads) {
        mMaxThreads = maxThreads;
    }

    synchronized void enqueue(int count, PendingTxn first, PendingTxn last) {
        if (mLast == null) {
            mFirst = first;
        } else {
            mLast.mNext = first;
        }
        mLast = last;
        if (!mRunning) {
            Runner.start("PendingTxnFinisher", this);
            mRunning = true;
        } else {
            notify();
        }
    }

    @Override
    public void run() {
        while (true) {
            boolean waited = false;
            PendingTxn pending;
            synchronized (this) {
                while (true) {
                    pending = mFirst;
                    if (pending != null) {
                        if ((mFirst = pending.mNext) == null) {
                            mLast = null;
                        }
                        break;
                    }
                    if (waited) {
                        mRunning = false;
                        return;
                    }
                    try {
                        wait(10_000);
                    } catch (InterruptedException e) {
                    }
                    waited = true;
                }
            }

            pending.run();
        }
    }
}
