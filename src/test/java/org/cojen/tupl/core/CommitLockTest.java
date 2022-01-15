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

package org.cojen.tupl.core;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.TestUtils;

import org.cojen.tupl.util.Latch;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class CommitLockTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(CommitLockTest.class.getName());
    }

    @Test
    public void reentrant() throws Exception {
        var lock = new CommitLock();

        lock.lock();
        lock.lock();
        assertTrue(lock.tryLock());

        // Shared lock can be upgraded to exclusive.
        lock.acquireExclusive();
        lock.releaseExclusive();

        // Release all the shared locks.
        lock.unlock();
        lock.unlock();
        lock.unlock();

        lock.acquireExclusive();

        // Can still acquire shared.
        assertTrue(lock.tryLock());

        // Release all the locks.
        lock.releaseExclusive();
        lock.unlock();

        // Lock shared locally.
        lock.lock();

        // Upgrade isn't possible when held by another thread.
        var holder = new Thread(() -> {
            lock.lock();
            try {
                while (true) {
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                lock.unlock();
            }
        });

        TestUtils.startAndWaitUntilBlocked(holder);

        holder.interrupt();

        // Can lock now.
        lock.acquireExclusive();
        lock.releaseExclusive();
        lock.unlock();
    }

    @Test
    public void blocked() throws Exception {
        var lock = new CommitLock();

        var waiter = new Latch(Latch.EXCLUSIVE);

        var ex = new Thread(() -> {
            try {
                lock.acquireExclusive();
                waiter.releaseExclusive();
                while (true) {
                    Thread.sleep(100_000);
                }
            } catch (InterruptedException e) {
                lock.releaseExclusive();
            }
        });

        ex.start();

        // Wait for exclusive lock to be held.
        waiter.acquireExclusive();

        assertFalse(lock.tryLock());

        LockTest.selfInterrupt(1000);
        try {
            lock.lockInterruptibly();
            fail();
        } catch (InterruptedException e) {
            // Good.
        }

        ex.interrupt();

        assertTrue(lock.tryLock(10, TimeUnit.SECONDS));
    }

    @Test
    public void contention() throws Exception {
        var lock = new CommitLock();

        var sharedThreads = new Thread[100];
        var waiter = new CountDownLatch(sharedThreads.length);

        for (int i=0; i<sharedThreads.length; i++) {
            sharedThreads[i] = new Thread(() -> {
                try {
                    lock.lockInterruptibly();
                    waiter.countDown();
                    while (true) {
                        lock.unlock();
                        lock.lockInterruptibly();
                    }
                } catch (InterruptedException e) {
                    // Done.
                }
            });

            sharedThreads[i].start();
        }

        // Wait for threads to start.
        waiter.await();

        // Even with all those threads running, the exclusive lock can be acquired.
        lock.acquireExclusive();

        for (Thread t : sharedThreads) {
            t.interrupt();
        }

        lock.releaseExclusive();
        lock.acquireExclusive();

        for (Thread t : sharedThreads) {
            t.join();
        }

        lock.releaseExclusive();
    }
}
