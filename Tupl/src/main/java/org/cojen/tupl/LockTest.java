/*
 *  Copyright 2011 Brian S O'Neill
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

import org.junit.*;
import static org.junit.Assert.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class LockTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(LockTest.class.getName());
    }

    private static final byte[] k1, k2;

    private static final long ONE_MILLIS_IN_NANOS = 1000000L;

    private static final long SHORT_TIMEOUT = ONE_MILLIS_IN_NANOS;
    private static final long MEDIUM_TIMEOUT = ONE_MILLIS_IN_NANOS * 10000;

    static {
        k1 = key("hello");
        k2 = key("world");
    }

    public static byte[] key(String str) {
        return str.getBytes();
    }

    public static void sleep(long millis) {
        if (millis == 0) {
            return;
        }
        long start = System.nanoTime();
        do {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {
            }
            millis -= (System.nanoTime() - start) / 1000000;
        } while (millis > 0);
    }

    public static void selfInterrupt(final long delayMillis) {
        final Thread thread = Thread.currentThread();
        new Thread() {
            public void run() {
                LockTest.sleep(delayMillis);
                thread.interrupt();
            }
        }.start();
    }

    private LockManager mManager;

    @Before
    public void setup() {
        mManager = new LockManager();
    }

    @Test
    public void basicShared() {
        Locker locker = new Locker(mManager);
        assertEquals(LockResult.ACQUIRED, locker.lockShared(k1, -1));
        assertEquals(1, mManager.numLocksHeld());
        assertEquals(LockResult.OWNED_SHARED, locker.lockShared(k1, -1));
        assertEquals(1, mManager.numLocksHeld());
        assertEquals(LockResult.ACQUIRED, locker.lockShared(k2, -1));
        assertEquals(LockResult.OWNED_SHARED, locker.lockShared(k1, -1));
        assertEquals(LockResult.OWNED_SHARED, locker.lockShared(k2, -1));
        assertEquals(2, mManager.numLocksHeld());
        locker.unlockAll();
        assertEquals(0, mManager.numLocksHeld());

        assertEquals(LockResult.ACQUIRED, locker.lockShared(k1, -1));
        assertEquals(LockResult.ACQUIRED, locker.lockShared(k2, -1));
        assertEquals(k2, locker.unlock());
        assertEquals(k1, locker.unlock());
        assertEquals(LockResult.ACQUIRED, locker.lockShared(k1, -1));
        assertEquals(LockResult.ACQUIRED, locker.lockShared(k2, -1));
        assertEquals(k2, locker.unlock());

        Locker locker2 = new Locker(mManager);
        assertEquals(LockResult.ACQUIRED, locker2.lockShared(k1, -1));
        assertEquals(LockResult.ACQUIRED, locker2.lockShared(k2, -1));
        assertEquals(LockResult.OWNED_SHARED, locker.lockShared(k1, -1));
        assertEquals(LockResult.ACQUIRED, locker.lockShared(k2, -1));

        assertEquals(k2, locker.unlockToShared());
        assertEquals(k2, locker2.unlockToShared());
        assertEquals(k2, locker2.unlockToShared());

        assertEquals(LockResult.OWNED_SHARED, locker2.lockShared(k1, -1));
        assertEquals(LockResult.OWNED_SHARED, locker2.lockShared(k2, -1));
        assertEquals(LockResult.OWNED_SHARED, locker.lockShared(k1, -1));
        assertEquals(LockResult.OWNED_SHARED, locker.lockShared(k2, -1));

        locker.unlockAll();
        locker2.unlockAll();

        try {
            locker.unlock();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(LockResult.ACQUIRED, locker.lockShared(k1, -1));

        assertEquals(LockResult.ILLEGAL, locker.lockUpgradable(k1, -1));
        assertEquals(LockResult.ILLEGAL, locker.lockExclusive(k1, -1));

        locker.unlockAll();
        locker2.unlockAll();
    }

    @Test
    public void basicUpgradable() {
        Locker locker = new Locker(mManager);
        assertEquals(LockResult.ACQUIRED, locker.lockUpgradable(k1, -1));
        assertEquals(1, mManager.numLocksHeld());
        assertEquals(LockResult.OWNED_UPGRADABLE, locker.lockUpgradable(k1, -1));
        assertEquals(1, mManager.numLocksHeld());
        assertEquals(LockResult.ACQUIRED, locker.lockUpgradable(k2, -1));
        assertEquals(LockResult.OWNED_UPGRADABLE, locker.lockUpgradable(k1, -1));
        assertEquals(LockResult.OWNED_UPGRADABLE, locker.lockUpgradable(k2, -1));
        assertEquals(2, mManager.numLocksHeld());
        locker.unlockAll();
        assertEquals(0, mManager.numLocksHeld());

        assertEquals(LockResult.ACQUIRED, locker.lockUpgradable(k1, -1));
        assertEquals(LockResult.ACQUIRED, locker.lockUpgradable(k2, -1));
        assertEquals(k2, locker.unlock());
        assertEquals(k1, locker.unlock());
        assertEquals(LockResult.ACQUIRED, locker.lockUpgradable(k1, -1));
        assertEquals(LockResult.ACQUIRED, locker.lockUpgradable(k2, -1));
        assertEquals(k2, locker.unlock());

        Locker locker2 = new Locker(mManager);
        assertEquals(LockResult.TIMED_OUT_LOCK, locker2.lockUpgradable(k1, SHORT_TIMEOUT));
        assertEquals(LockResult.ACQUIRED, locker2.lockUpgradable(k2, -1));
        assertEquals(LockResult.OWNED_UPGRADABLE, locker.lockUpgradable(k1, -1));
        assertEquals(LockResult.TIMED_OUT_LOCK, locker.lockUpgradable(k2, SHORT_TIMEOUT));
        assertEquals(LockResult.ACQUIRED, locker2.lockShared(k1, -1));
        assertEquals(LockResult.OWNED_SHARED, locker2.lockShared(k1, -1));
        assertEquals(LockResult.OWNED_UPGRADABLE, locker2.lockShared(k2, -1));
        assertEquals(LockResult.OWNED_UPGRADABLE, locker.lockShared(k1, -1));
        assertEquals(LockResult.ACQUIRED, locker.lockShared(k2, -1));
        assertEquals(LockResult.OWNED_SHARED, locker.lockShared(k2, -1));

        try {
            locker.unlockToUpgradable();
            fail();
        } catch (IllegalStateException e) {
        }
        assertEquals(k2, locker.unlockToShared());
        try {
            locker2.unlockToUpgradable();
            fail();
        } catch (IllegalStateException e) {
        }
        assertEquals(k1, locker2.unlockToShared());
        assertEquals(k1, locker2.unlockToShared());

        assertEquals(LockResult.ILLEGAL, locker2.lockUpgradable(k1, -1));
        assertEquals(LockResult.OWNED_UPGRADABLE, locker2.lockUpgradable(k2, -1));
        assertEquals(LockResult.OWNED_UPGRADABLE, locker.lockUpgradable(k1, -1));
        assertEquals(LockResult.ILLEGAL, locker.lockUpgradable(k2, -1));

        locker.unlockAll();
        locker2.unlockAll();

        try {
            locker.unlock();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(LockResult.ACQUIRED, locker.lockUpgradable(k1, -1));

        assertEquals(k1, locker.unlockToUpgradable());
        assertEquals(LockResult.OWNED_UPGRADABLE, locker.lockShared(k1, -1));
        assertEquals(k1, locker.unlockToShared());
        
        assertEquals(LockResult.OWNED_SHARED, locker.lockShared(k1, -1));
        assertEquals(LockResult.ILLEGAL, locker.lockUpgradable(k1, -1));
        assertEquals(LockResult.ILLEGAL, locker.lockExclusive(k1, -1));

        locker.unlockAll();
        locker2.unlockAll();
    }

    @Test
    public void basicExclusive() {
        Locker locker = new Locker(mManager);
        assertEquals(LockResult.ACQUIRED, locker.lockExclusive(k1, -1));
        assertEquals(1, mManager.numLocksHeld());
        assertEquals(LockResult.OWNED_EXCLUSIVE, locker.lockExclusive(k1, -1));
        assertEquals(1, mManager.numLocksHeld());
        assertEquals(LockResult.ACQUIRED, locker.lockExclusive(k2, -1));
        assertEquals(LockResult.OWNED_EXCLUSIVE, locker.lockExclusive(k1, -1));
        assertEquals(LockResult.OWNED_EXCLUSIVE, locker.lockExclusive(k2, -1));
        assertEquals(2, mManager.numLocksHeld());
        locker.unlockAll();
        assertEquals(0, mManager.numLocksHeld());

        assertEquals(LockResult.ACQUIRED, locker.lockExclusive(k1, -1));
        assertEquals(LockResult.ACQUIRED, locker.lockExclusive(k2, -1));
        assertEquals(k2, locker.unlock());
        assertEquals(k1, locker.unlock());
        assertEquals(LockResult.ACQUIRED, locker.lockExclusive(k1, -1));
        assertEquals(LockResult.ACQUIRED, locker.lockExclusive(k2, -1));
        assertEquals(k2, locker.unlock());

        Locker locker2 = new Locker(mManager);
        assertEquals(LockResult.TIMED_OUT_LOCK, locker2.lockShared(k1, SHORT_TIMEOUT));
        assertEquals(LockResult.TIMED_OUT_LOCK, locker2.lockUpgradable(k1, SHORT_TIMEOUT));
        assertEquals(LockResult.TIMED_OUT_LOCK, locker2.lockExclusive(k1, SHORT_TIMEOUT));
        assertEquals(LockResult.ACQUIRED, locker2.lockExclusive(k2, -1));
        assertEquals(LockResult.OWNED_EXCLUSIVE, locker.lockExclusive(k1, -1));
        assertEquals(LockResult.TIMED_OUT_LOCK, locker.lockShared(k2, SHORT_TIMEOUT));
        assertEquals(LockResult.TIMED_OUT_LOCK, locker.lockUpgradable(k2, SHORT_TIMEOUT));
        assertEquals(LockResult.TIMED_OUT_LOCK, locker.lockExclusive(k2, SHORT_TIMEOUT));

        assertEquals(k1, locker.unlockToUpgradable());
        assertEquals(LockResult.OWNED_UPGRADABLE, locker.lockShared(k1, -1));
        assertEquals(LockResult.OWNED_UPGRADABLE, locker.lockUpgradable(k1, -1));
        assertEquals(k1, locker.unlockToShared());
        assertEquals(k2, locker2.unlockToUpgradable());
        assertEquals(k2, locker2.unlockToShared());
        assertEquals(LockResult.OWNED_SHARED, locker2.lockShared(k2, -1));

        assertEquals(LockResult.ILLEGAL, locker.lockExclusive(k1, -1));
        assertEquals(LockResult.ILLEGAL, locker2.lockExclusive(k2, -1));

        locker.unlockAll();
        locker2.unlockAll();
        locker2.unlockAll();

        try {
            locker.unlock();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(LockResult.ACQUIRED, locker.lockExclusive(k1, -1));

        assertEquals(k1, locker.unlockToUpgradable());
        assertEquals(k1, locker.unlockToUpgradable());
        assertEquals(LockResult.OWNED_UPGRADABLE, locker.lockShared(k1, -1));
        assertEquals(k1, locker.unlockToShared());
        assertEquals(k1, locker.unlockToShared());
        
        assertEquals(LockResult.OWNED_SHARED, locker.lockShared(k1, -1));
        assertEquals(LockResult.ILLEGAL, locker.lockExclusive(k1, -1));

        locker.unlockAll();
        locker2.unlockAll();
    }

    @Test
    public void pileOfLocks() {
        Locker locker = new Locker(mManager);
        for (int i=0; i<1000; i++) {
            assertEquals(LockResult.ACQUIRED, locker.lockExclusive(key("k" + i), -1));
        }
        assertEquals(1000, mManager.numLocksHeld());
        locker.unlockAll();
        for (int i=0; i<1000; i++) {
            assertEquals(LockResult.ACQUIRED, locker.lockExclusive(key("k" + i), -1));
        }
        for (int i=0; i<1000; i++) {
            assertEquals(LockResult.OWNED_EXCLUSIVE, locker.lockExclusive(key("k" + i), -1));
        }
        for (int i=1000; --i>=0; ) {
            assertArrayEquals(key("k" + i), locker.unlock());
        }
        for (int i=0; i<1000; i++) {
            assertEquals(LockResult.ACQUIRED, locker.lockExclusive(key("k" + i), -1));
        }
        locker.unlockAll();
    }

    @Test
    public void blockedNoWait() {
        blocked(0);
    }

    @Test
    public void blockedTimedWait() {
        blocked(SHORT_TIMEOUT);
    }

    private void blocked(long nanosTimeout) {
        Locker locker = new Locker(mManager);
        Locker locker2 = new Locker(mManager);

        locker.lockShared(k1, -1);

        assertEquals(LockResult.TIMED_OUT_LOCK, locker2.lockExclusive(k1, nanosTimeout));

        locker.unlock();

        assertEquals(LockResult.ACQUIRED, locker2.lockExclusive(k1, -1));
        locker2.unlock();

        locker.lockUpgradable(k1, -1);

        assertEquals(LockResult.TIMED_OUT_LOCK, locker2.lockUpgradable(k1, nanosTimeout));
        assertEquals(LockResult.TIMED_OUT_LOCK, locker2.lockExclusive(k1, nanosTimeout));

        locker.unlock();

        assertEquals(LockResult.ACQUIRED, locker2.lockUpgradable(k1, -1));
        locker2.unlock();

        locker.lockExclusive(k1, -1);

        assertEquals(LockResult.TIMED_OUT_LOCK, locker2.lockShared(k1, nanosTimeout));
        assertEquals(LockResult.TIMED_OUT_LOCK, locker2.lockUpgradable(k1, nanosTimeout));
        assertEquals(LockResult.TIMED_OUT_LOCK, locker2.lockExclusive(k1, nanosTimeout));

        locker.unlock();

        assertEquals(LockResult.ACQUIRED, locker2.lockShared(k1, -1));
        locker2.unlock();

        locker.unlockAll();
        locker2.unlockAll();
    }

    @Test
    public void interrupts() {
        interrupts(-1);
    }

    @Test
    public void interruptsTimedWait() {
        interrupts(10000 * ONE_MILLIS_IN_NANOS);
    }

    private void interrupts(long nanosTimeout) {
        Locker locker = new Locker(mManager);
        Locker locker2 = new Locker(mManager);

        locker.lockShared(k1, -1);

        selfInterrupt(1000);
        assertEquals(LockResult.INTERRUPTED, locker2.lockExclusive(k1, nanosTimeout));
        assertFalse(Thread.interrupted());

        locker.unlock();

        assertEquals(LockResult.ACQUIRED, locker2.lockExclusive(k1, -1));
        locker2.unlock();

        locker.lockUpgradable(k1, -1);

        selfInterrupt(1000);
        assertEquals(LockResult.INTERRUPTED, locker2.lockUpgradable(k1, nanosTimeout));
        assertFalse(Thread.interrupted());
        selfInterrupt(1000);
        assertEquals(LockResult.INTERRUPTED, locker2.lockExclusive(k1, nanosTimeout));
        assertFalse(Thread.interrupted());

        locker.unlock();

        assertEquals(LockResult.ACQUIRED, locker2.lockUpgradable(k1, -1));
        locker2.unlock();

        locker.lockExclusive(k1, -1);

        selfInterrupt(1000);
        assertEquals(LockResult.INTERRUPTED, locker2.lockShared(k1, nanosTimeout));
        assertFalse(Thread.interrupted());
        selfInterrupt(1000);
        assertEquals(LockResult.INTERRUPTED, locker2.lockUpgradable(k1, nanosTimeout));
        assertFalse(Thread.interrupted());
        selfInterrupt(1000);
        assertEquals(LockResult.INTERRUPTED, locker2.lockExclusive(k1, nanosTimeout));
        assertFalse(Thread.interrupted());

        locker.unlock();

        assertEquals(LockResult.ACQUIRED, locker2.lockShared(k1, -1));
        locker2.unlock();

        locker.unlockAll();
        locker2.unlockAll();
    }

    @Test
    public void delayedAcquire() {
        Locker locker = new Locker(mManager);
        Locker locker2 = new Locker(mManager);

        assertEquals(LockResult.ACQUIRED, locker.lockUpgradable(k1, -1));
        long end = scheduleUnlock(locker, 1000);
        assertEquals(LockResult.ACQUIRED, locker2.lockUpgradable(k1, MEDIUM_TIMEOUT));
        assertEquals(LockResult.TIMED_OUT_LOCK, locker.lockUpgradable(k1, 0));
        locker2.unlock();
        assertTrue(System.nanoTime() >= end);

        assertEquals(LockResult.ACQUIRED, locker.lockUpgradable(k1, -1));
        end = scheduleUnlock(locker, 1000);
        assertEquals(LockResult.ACQUIRED, locker2.lockExclusive(k1, MEDIUM_TIMEOUT));
        assertEquals(LockResult.TIMED_OUT_LOCK, locker.lockUpgradable(k1, 0));
        locker2.unlock();
        assertTrue(System.nanoTime() >= end);

        assertEquals(LockResult.ACQUIRED, locker.lockExclusive(k1, -1));
        end = scheduleUnlock(locker, 1000);
        assertEquals(LockResult.ACQUIRED, locker2.lockUpgradable(k1, MEDIUM_TIMEOUT));
        assertEquals(LockResult.TIMED_OUT_LOCK, locker.lockUpgradable(k1, 0));
        locker2.unlock();
        assertTrue(System.nanoTime() >= end);

        assertEquals(LockResult.ACQUIRED, locker.lockExclusive(k1, -1));
        end = scheduleUnlock(locker, 1000);
        assertEquals(LockResult.ACQUIRED, locker2.lockExclusive(k1, MEDIUM_TIMEOUT));
        assertEquals(LockResult.TIMED_OUT_LOCK, locker.lockUpgradable(k1, 0));
        locker2.unlock();
        assertTrue(System.nanoTime() >= end);

        assertEquals(LockResult.ACQUIRED, locker.lockExclusive(k1, -1));
        end = scheduleUnlockToShared(locker, 1000);
        assertEquals(LockResult.ACQUIRED, locker2.lockUpgradable(k1, MEDIUM_TIMEOUT));
        assertEquals(LockResult.OWNED_SHARED, locker.lockShared(k1, 0));
        locker2.unlock();
        locker.unlock();
        assertTrue(System.nanoTime() >= end);

        assertEquals(LockResult.ACQUIRED, locker.lockExclusive(k1, -1));
        end = scheduleUnlockToShared(locker, 1000);
        assertEquals(LockResult.ACQUIRED, locker2.lockShared(k1, MEDIUM_TIMEOUT));
        assertEquals(LockResult.OWNED_SHARED, locker.lockShared(k1, 0));
        locker.unlock();
        locker2.unlock();
        assertTrue(System.nanoTime() >= end);

        assertEquals(LockResult.ACQUIRED, locker.lockExclusive(k1, -1));
        end = scheduleUnlockToUpgradable(locker, 1000);
        assertEquals(LockResult.ACQUIRED, locker2.lockShared(k1, MEDIUM_TIMEOUT));
        assertEquals(LockResult.OWNED_UPGRADABLE, locker.lockShared(k1, 0));
        locker.unlock();
        locker2.unlock();
        assertTrue(System.nanoTime() >= end);

        assertEquals(LockResult.ACQUIRED, locker.lockShared(k1, -1));
        end = scheduleUnlock(locker, 1000);
        assertEquals(LockResult.ACQUIRED, locker2.lockExclusive(k1, MEDIUM_TIMEOUT));
        assertEquals(LockResult.TIMED_OUT_LOCK, locker.lockShared(k1, 0));
        locker2.unlock();
        assertTrue(System.nanoTime() >= end);
    }

    private long scheduleUnlock(final Locker locker, final long delayMillis) {
        return schedule(locker, delayMillis, 0);
    }

    private long scheduleUnlockToShared(final Locker locker, final long delayMillis) {
        return schedule(locker, delayMillis, 1);
    }

    private long scheduleUnlockToUpgradable(final Locker locker, final long delayMillis) {
        return schedule(locker, delayMillis, 2);
    }

    private long schedule(final Locker locker, final long delayMillis, final int type) {
        long end = System.nanoTime() + delayMillis * ONE_MILLIS_IN_NANOS;
        new Thread() {
            public void run() {
                LockTest.sleep(delayMillis);
                switch (type) {
                default:
                    locker.unlock();
                    break;
                case 1:
                    locker.unlockToShared();
                    break;
                case 2:
                    locker.unlockToUpgradable();
                    break;
                }
            }
        }.start();
        return end;
    }
}
