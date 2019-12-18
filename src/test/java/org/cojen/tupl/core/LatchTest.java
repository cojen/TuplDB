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

package org.cojen.tupl.core;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.util.Continuation;
import org.cojen.tupl.util.Latch;
import org.cojen.tupl.util.LatchCondition;

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
        var cond = new LatchCondition();

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
        // Can't interrupt a Continuation.

        var latch = new Latch();
        var cond = new LatchCondition();

        var waiter = new Continuation() {
            volatile boolean called;

            @Override
            public boolean run() {
                called = true;
                return true;
            }
        };

        latch.acquireExclusive();
        cond.uponSignal(latch, waiter);

        cond.clear();
        latch.releaseExclusive();

        assertFalse(waiter.called);
    }
}
