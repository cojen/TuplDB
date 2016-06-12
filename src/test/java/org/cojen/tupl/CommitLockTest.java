/*
 *  Copyright 2016 Cojen.org
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

import java.io.InterruptedIOException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.*;
import static org.junit.Assert.*;

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
        CommitLock lock = new CommitLock();

        lock.lock();
        lock.lock();
        assertTrue(lock.tryLock());

        // Cannot upgrade shared lock to exclusive.
        LockTest.selfInterrupt(1000);
        try {
            lock.acquireExclusive();
            fail();
        } catch (InterruptedIOException e) {
            // Good.
        }

        // Release all the shared locks.
        lock.unlock();
        lock.unlock();
        lock.unlock();

        lock.acquireExclusive();

        // Now can acquire shared.
        assertTrue(lock.tryLock());

        // Exclusive isn't reentrant.
        LockTest.selfInterrupt(1000);
        try {
            lock.acquireExclusive();
            fail();
        } catch (InterruptedIOException e) {
            // Good.
        }

        // Release all the locks.
        lock.releaseExclusive();
        lock.unlock();
    }

    @Test
    public void blocked() throws Exception {
        CommitLock lock = new CommitLock();

        CountDownLatch waiter = new CountDownLatch(1);

        Thread ex = new Thread(() -> {
            try {
                lock.acquireExclusive();
                waiter.countDown();
                while (true) {
                    Thread.sleep(100_000);
                }
            } catch (InterruptedException | InterruptedIOException e) {
                lock.releaseExclusive();
            }
        });

        ex.start();

        // Wait for exclusive lock to be held.
        waiter.await();

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
        CommitLock lock = new CommitLock();

        Thread[] sharedThreads = new Thread[100];
        CountDownLatch waiter = new CountDownLatch(sharedThreads.length);

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
