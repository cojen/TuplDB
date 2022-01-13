/*
 *  Copyright (C) 2022 Cojen.org
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
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.util;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.util.concurrent.TimeUnit;

import org.cojen.tupl.io.Utils;

/**
 * Lightweight condition object which can be signaled at most once, and only one thread can be
 * waiting for the signal.
 *
 * @author Brian S O'Neill
 * @see LatchCondition
 */
public class OneShot {
    private static final VarHandle cWaiterHandle;

    static {
        try {
            var lookup = MethodHandles.lookup();
            cWaiterHandle = lookup.findVarHandle(OneShot.class, "mWaiter", Object.class);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    private volatile Object mWaiter;

    /**
     * Blocks the current thread indefinitely until this object is signaled.
     *
     * @return -1 if interrupted, or 1 if signaled
     * @throws IllegalStateException if another thread is waiting
     */
    public final int await() {
        return await(-1, 0);
    }

    /**
     * Blocks the current thread until this object is signaled.
     *
     * @param timeout relative time to wait; infinite if {@literal <0}
     * @param unit timeout unit
     * @return -1 if interrupted, 0 if timed out, 1 if signaled
     * @throws IllegalStateException if another thread is waiting
     */
    public final int await(long timeout, TimeUnit unit) {
        long nanosTimeout, nanosEnd;
        if (timeout <= 0) {
            nanosTimeout = timeout;
            nanosEnd = 0;
        } else {
            nanosTimeout = unit.toNanos(timeout);
            nanosEnd = System.nanoTime() + nanosTimeout;
        }
        return await(nanosTimeout, nanosEnd);
    }

    /**
     * Blocks the current thread until this object is signaled.
     *
     * @param nanosTimeout relative nanosecond time to wait; infinite if {@literal <0}
     * @return -1 if interrupted, 0 if timed out, 1 if signaled
     * @throws IllegalStateException if another thread is waiting
     */
    public final int await(long nanosTimeout) {
        long nanosEnd = nanosTimeout <= 0 ? 0 : (System.nanoTime() + nanosTimeout);
        return await(nanosTimeout, nanosEnd);
    }

    /**
     * Blocks the current thread until this object is signaled.
     *
     * @param nanosTimeout relative nanosecond time to wait; infinite if {@literal <0}
     * @param nanosEnd absolute nanosecond time to wait until; used only with {@literal >0} timeout
     * @return -1 if interrupted, 0 if timed out, 1 if signaled
     * @throws IllegalStateException if another thread is waiting
     */
    public final int await(long nanosTimeout, long nanosEnd) {
        if (mWaiter == this) {
            return 1;
        }

        if (nanosTimeout == 0) {
            return 0;
        }

        Object waiter = cWaiterHandle.compareAndExchange(this, null, Thread.currentThread());

        if (waiter != null) {
            if (waiter == this) {
                return 1;
            }
            throw new IllegalStateException();
        }

        int result;

        while (true) {
            if (nanosTimeout < 0) {
                Parker.park(this);
                if (Thread.interrupted()) {
                    result = -1;
                    break;
                }
            } else {
                Parker.parkNanos(this, nanosTimeout);
                if (Thread.interrupted()) {
                    result = -1;
                    break;
                }
                if ((nanosTimeout = nanosEnd - System.nanoTime()) <= 0) {
                    result = 0;
                    break;
                }
            }

            if (mWaiter == this) {
                return 1;
            }
        }

        // Timed out or interrupted.

        cWaiterHandle.compareAndExchangeRelease(this, Thread.currentThread(), null);

        return result;
    }

    /**
     * Signals this object, waking up a blocked thread.
     */
    public final void signal() {
        Object waiter = cWaiterHandle.getAndSet(this, this);
        if (waiter instanceof Thread t) {
            Parker.unpark(t);
        }
    }
}
