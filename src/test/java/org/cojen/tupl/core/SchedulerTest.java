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

package org.cojen.tupl.core;

import java.util.Random;

import java.util.concurrent.Executors;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.TestUtils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class SchedulerTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(SchedulerTest.class.getName());
    }

    @Before
    public void setup() {
        mScheduler = new Scheduler(Executors.newCachedThreadPool(r -> {
            var t = new Thread(r);
            t.setPriority(Thread.MAX_PRIORITY);
            return t;
        }));
    }

    @After
    public void teardown() {
        mScheduler.shutdown();
    }

    private Scheduler mScheduler;

    @Test
    public void basic() {
        try {
            doBasic();
        } catch (AssertionError e) {
            // Try again.
            doBasic();
        }
    }

    private void doBasic() {
        var task = new Task();
        assertTrue(mScheduler.execute(task));
        task.runCheck();

        task = new Task();
        long start = System.nanoTime();
        assertTrue(mScheduler.scheduleMillis(task, 100));
        task.runCheck();
        long delayMillis = (System.nanoTime() - start) / 1_000_000L;
        assertTrue("" + delayMillis, delayMillis >= 100 && delayMillis <= 1000);

        task = new Task();
        assertTrue(mScheduler.scheduleMillis(task, 100));
        mScheduler.shutdown();
        try {
            task.runCheck();
        } catch (AssertionError e) {
            // Expected.
        }

        assertFalse(mScheduler.execute(new Task()));
    }

    @Test
    public void fuzz() {
        try {
            doFuzz();
        } catch (AssertionError e) {
            // Try again.
            doFuzz();
        }
    }

    private void doFuzz() {
        var tasks = new Task[1000];
        for (int i=0; i<tasks.length; i++) {
            tasks[i] = new Task();
        }

        var rnd = new Random(309458);

        for (int i=0; i<tasks.length; i++) {
            long delay = rnd.nextInt(1000);
            tasks[i].expectedTime = System.nanoTime() + delay * 1_000_000L;
            mScheduler.scheduleMillis(tasks[i], delay);
            TestUtils.sleep(rnd.nextInt(10));
        }

        for (Task task : tasks) {
            task.runCheck();
            long deviationMillis = (task.actualTime - task.expectedTime) / 1_000_000L;
            assertTrue("" + deviationMillis, deviationMillis >= 0 && deviationMillis <= 1000); 
        }
    }

    static class Task implements Runnable {
        long expectedTime;
        volatile long actualTime;
        volatile boolean ran;

        @Override
        public void run() {
            actualTime = System.nanoTime();
            ran = true;
        }

        void runCheck() {
            for (int i=0; i<2000; i++) {
                if (ran) {
                    return;
                }
                TestUtils.sleep(1);
            }
            fail();
        }
    }
}
