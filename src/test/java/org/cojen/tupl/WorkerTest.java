/*
 *  Copyright (C) 2011-2017 Cojen.org
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

package org.cojen.tupl;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.util.Worker;
import org.cojen.tupl.util.WorkerGroup;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class WorkerTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(WorkerTest.class.getName());
    }

    @Test
    public void queueFill() {
        // Call the non-blocking enqueue method.

        final int max = 100;
        Worker w = Worker.make(max, 60, TimeUnit.SECONDS, null);
        LongAdder total = new LongAdder();

        for (int i=1; i<=max; i++) {
            assertTrue(w.tryEnqueue(new Counter(total, i, 10)));
        }

        Counter task = new Counter(total, 1000, 0);
        assertFalse(w.tryEnqueue(task));

        w.join(false);

        assertEquals((max + 1) * (max / 2), total.sum());

        assertTrue(w.tryEnqueue(task));

        w.join(false);

        assertEquals(1000 + ((max + 1) * (max / 2)), total.sum());
    }

    @Test
    public void queueFill2() {
        // Call the blocking enqueue method.

        final int max = 100;
        Worker w = Worker.make(max, 60, TimeUnit.SECONDS, null);
        LongAdder total = new LongAdder();

        final int limit = max * 2;
        for (int i=1; i<=limit; i++) {
            w.enqueue(new Counter(total, i, 10));
        }

        w.join(false);

        assertEquals((limit + 1) * (limit / 2), total.sum());
    }

    @Test
    public void idle() {
        // Permit the worker thread to exit and be restarted many times.

        final int max = 100;
        Worker w = Worker.make(max, 1, TimeUnit.MILLISECONDS, null);
        LongAdder total = new LongAdder();

        final int limit = max * 10;
        for (int i=1; i<=limit; i++) {
            w.enqueue(new Counter(total, i, 0));
            TestUtils.sleep(2);
        }

        w.join(false);

        assertEquals((limit + 1) * (limit / 2), total.sum());
    }

    @Test
    public void interrupt() {
        // Interrupt a waiting worker thread.

        Worker w = Worker.make(100, 60, TimeUnit.SECONDS, null);

        AtomicReference<Thread> threadRef = new AtomicReference<>();

        w.enqueue(new Worker.Task() {
            @Override
            public void run() {
                threadRef.set(Thread.currentThread());
            }
        });

        Thread thread;
        while ((thread = threadRef.get()) == null);

        w.join(true);

        long start = System.currentTimeMillis();
        while (thread.getState() != Thread.State.TERMINATED) {
            TestUtils.sleep(1);
        }
        long end = System.currentTimeMillis();

        assertTrue((end - start) < 1_000);

        // Launch a few new tasks.

        LongAdder total = new LongAdder();
        w.enqueue(new Counter(total, 123, 0));
        w.join(false);
        assertEquals(123, total.sum());

        w.enqueue(new Counter(total, 1, 0));
        w.join(true);
        assertEquals(124, total.sum());
    }

    @Test
    public void singletonGroup() {
        // Test a worker group with one worker.
        
        final int max = 100;
        WorkerGroup group = WorkerGroup.make(1, max, 60, TimeUnit.SECONDS, null);
        LongAdder total = new LongAdder();

        Worker w = null;

        final int limit = max * 2;
        for (int i=1; i<=limit; i++) {
            Worker actual = group.enqueue(new Counter(total, i, 10));
            assertTrue(actual != null);
            if (w == null) {
                w = actual;
            } else {
                assertEquals(w, actual);
            }
        }

        group.join(false);

        assertEquals((limit + 1) * (limit / 2), total.sum());
    }

    @Test
    public void smallGroup() {
        // Test a worker group with several workers.
        
        final int max = 100;
        WorkerGroup group = WorkerGroup.make(10, max, 60, TimeUnit.SECONDS, null);
        LongAdder total = new LongAdder();

        Map<Worker, Integer> counts = new HashMap<>();

        final int limit = max * 2;
        for (int i=1; i<=limit; i++) {
            Worker w = group.enqueue(new Counter(total, i, 10));
            Integer count = counts.get(w);
            if (count == null) {
                counts.put(w, 1);
            } else {
                counts.put(w, count + 1);
            }
        }

        group.join(false);

        assertEquals((limit + 1) * (limit / 2), total.sum());

        assertEquals(2, counts.size());

        long sum = 0;
        int prev = Integer.MAX_VALUE;
        for (int count : counts.values()) {
            sum += count;
            assertTrue(count + " <= " + prev, count <= prev);
            prev = count;
        }

        assertEquals(max * counts.size(), sum);
    }

    @Test
    public void stress() {
        final int max = 100;
        WorkerGroup group = WorkerGroup.make(10, max, 60, TimeUnit.SECONDS, null);

        long seed = new Random().nextLong();
        Random rnd = new Random(seed);
        long expected = 0;
        LongAdder total = new LongAdder();

        for (int i=0; i<100_000_000; i++) {
            long amt = rnd.nextLong();
            expected += amt;
            group.enqueue(new Counter(total, amt, 0));
        }

        group.join(true);

        assertEquals(expected, total.sum());
    }

    private static class Counter extends Worker.Task {
        private final LongAdder mTotal;
        private final long mAmt;
        private final long mDelayMillis;

        Counter(LongAdder total, long amt, long delayMillis) {
            mTotal = total;
            mAmt = amt;
            mDelayMillis = delayMillis;
        }

        @Override
        public void run() {
            mTotal.add(mAmt);
            TestUtils.sleep(mDelayMillis);
        }
    }
}
