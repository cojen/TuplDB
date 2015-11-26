/*
 *  Copyright 2011-2015 Cojen.org
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.cojen.tupl;

import java.util.*;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.sleep;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class DeadlockTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(DeadlockTest.class.getName());
    }

    volatile LockManager mManager;

    private List<Task> mTasks;

    @Before
    public void setup() {
        mManager = new LockManager(null, -1);
        mTasks = new ArrayList<Task>();
    }

    @Test
    public void test_1() throws Throwable {
        // Simple deadlock caused by two threads and two keys. Another thread
        // is a victim which timed out.

        final long timeout = 5L * 1000 * 1000 * 1000;
        final byte[][] keys = {"k0".getBytes(), "k1".getBytes(), "k2".getBytes()};

        // Current thread locks k0, and helps cause the deadlock.
        Locker locker = new Locker(mManager);
        locker.lockShared(1, keys[0], timeout);

        // Victim thread.
        mTasks.add(new Task() {
                void doRun() throws Throwable {
                    Locker locker = new Locker(mManager);
                    try {
                        // Lock k2 has does not participate in deadlock.
                        locker.lockExclusive(1, keys[2], timeout / 2);
                        sleep(1000);
                        try {
                            locker.lockExclusive(1, keys[0], timeout / 2);
                            fail();
                        } catch (DeadlockException e) {
                            // Deadlock observed, but this thread didn't create it.
                            assertFalse(e.isGuilty());
                        }
                    } finally {
                        locker.scopeUnlockAll();
                    }
                }
            });

        // Culprit thread.
        mTasks.add(new Task() {
                void doRun() throws Throwable {
                    Locker locker = new Locker(mManager);
                    try {
                        // Lock k1 and then k0, which is the opposite order of main thread.
                        locker.lockShared(1, keys[1], timeout);
                        sleep(500);
                        try {
                            locker.lockExclusive(1, keys[0], timeout);
                            fail();
                        } catch (DeadlockException e) {
                            // This thread helped create the deadlock.
                            assertTrue(e.isGuilty());
                        }
                    } finally {
                        locker.scopeUnlockAll();
                    }
                }
            });

        startTasks();

        sleep(250);

        // Lock k1, creating a deadlock. Timeout is longer, and so deadlock
        // will not be detected here.
        try {
            locker.lockExclusive(1, keys[1], timeout * 2);
        } finally {
            locker.scopeUnlockAll();
        }

        joinTasks();
    }

    @Test
    public void test_2() throws Throwable {
        // Deadlock caused by three threads and three keys.

        final long timeout = 3L * 1000 * 1000 * 1000;

        class TheTask extends Task {
            private final long mTimeout;
            private final boolean mExpectDeadlock;
            private final byte[][] mKeys;

            TheTask(long timeout, boolean expectDeadlock, String... keys) {
                mTimeout = timeout;
                mExpectDeadlock = expectDeadlock;
                mKeys = new byte[keys.length][];
                for (int i=0; i<keys.length; i++) {
                    mKeys[i] = keys[i].getBytes();
                }
            }

            void doRun() throws Throwable {
                try {
                    Locker locker = new Locker(mManager);
                    try {
                        for (byte[] key : mKeys) {
                            locker.lockUpgradable(1, key, mTimeout);
                            sleep(100);
                        }
                    } finally {
                        locker.scopeUnlockAll();
                    }
                    assertFalse(mExpectDeadlock);
                } catch (DeadlockException e) {
                    assertTrue(mExpectDeadlock);
                }
            }
        };

        // All three threads must be waiting for a deadlock to occur. As soon
        // as one times out, the rest can proceed because the dependency cycle
        // is broken.

        mTasks.add(new TheTask(timeout,     true,  "k1", "k2"));
        mTasks.add(new TheTask(timeout * 2, false, "k2", "k3"));
        mTasks.add(new TheTask(timeout * 2, false, "k3", "k1"));

        startTasks();
        joinTasks();
    }

    private void startTasks() {
        for (Task t : mTasks) {
            t.start();
        }
    }

    private void joinTasks() throws Throwable {
        for (Task t : mTasks) {
            t.join();
        }
        for (Task t : mTasks) {
            t.check();
        }
    }

    static abstract class Task extends Thread {
        private volatile Throwable mFailure;

        @Override
        public final void run() {
            try {
                doRun();
            } catch (Throwable t) {
                mFailure = t;
            }
        }

        void check() throws Throwable {
            Throwable t = mFailure;
            if (t != null) {
                throw t;
            }
        }

        abstract void doRun() throws Throwable;
    }
}
