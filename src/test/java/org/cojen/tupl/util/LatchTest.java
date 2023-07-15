/*
 *  Copyright 2019 Cojen.org
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

import java.util.ArrayList;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.TestUtils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class LatchTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(LatchTest.class.getName());
    }

    @Test
    public void clearThread() throws Exception {
        // Must interrupt a Thread.

        var latch = new Latch();
        var cond = new Latch.Condition();

        var waiter = new Thread() {
            volatile int result;

            @Override
            public void run() {
                latch.acquireExclusive();
                result = cond.await(latch);
                latch.releaseExclusive();
            }
        };

        TestUtils.startAndWaitUntilBlocked(waiter);

        latch.acquireExclusive();
        cond.clear();
        latch.releaseExclusive();

        waiter.join();
        assertEquals(-1, waiter.result);
    }

    @Test
    public void clearContinuation() throws Exception {
        // Can't interrupt a continuation.

        var latch = new Latch();
        var cond = new Latch.Condition();

        var waiter = new Runnable() {
            volatile boolean called;

            @Override
            public void run() {
                called = true;
            }
        };

        latch.acquireExclusive();
        cond.uponSignal(waiter);

        cond.clear();
        latch.releaseExclusive();

        assertFalse(waiter.called);
    }

    @Test
    public void signalSharedWaiterAfterTimeout() throws Exception {
        var latch = new Latch();
        latch.acquireShared();

        // Exclusive request is blocked because a shared lock is held.
        Thread t1 = TestUtils.startAndWaitUntilBlocked(new Thread(() -> {
            try {
                latch.tryAcquireExclusiveNanos(1_000_000_000L);
            } catch (InterruptedException e) {
            }
        }));

        // Shared request is blocked by the exclusive latch request, but it should be granted
        // once the exclusive request times out.
        assertTrue(latch.tryAcquireSharedNanos(60_000_000_000L));
    }
    
    @Test
    public void priorityAwait() throws Exception {
        var latch = new Latch();
        var condition = new Latch.Condition();

        var finished = new ArrayList<Thread>();

        class Waiter extends Thread {
            @Override
            public void run() {
                latch.acquireExclusive();
                condition.priorityAwait(latch, -1, 0);
                finished.add(this);
                latch.releaseExclusive();
            }
        }

        Thread t1 = TestUtils.startAndWaitUntilBlocked(new Waiter());
        Thread t2 = TestUtils.startAndWaitUntilBlocked(new Waiter());

        latch.acquireExclusive();
        condition.signalAll(latch);
        latch.releaseExclusive();

        t1.join();
        t2.join();

        // Verify that threads finished in LIFO order.
        assertEquals(2, finished.size());
        assertEquals(t2, finished.get(0));
        assertEquals(t1, finished.get(1));
    }
}
