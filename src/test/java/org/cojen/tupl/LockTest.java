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

import java.nio.charset.StandardCharsets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.LockResult.*;
import static org.cojen.tupl.TestUtils.startAndWaitUntilBlocked;

/**
 * 
 *
 * @author Brian S O'Neill
 * @author anandsa
 */
public class LockTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(LockTest.class.getName());
    }

    private static final byte[] k1, k2, k3, k4;

    private static final long ONE_MILLIS_IN_NANOS = 1000000L;

    private static final long SHORT_TIMEOUT = ONE_MILLIS_IN_NANOS;
    private static final long MEDIUM_TIMEOUT = ONE_MILLIS_IN_NANOS * 10000;

    static {
        k1 = key("hello");
        k2 = key("world");
        k3 = key("akey");
        k4 = key("bkey");
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
        new Thread(() -> {
            LockTest.sleep(delayMillis);
            thread.interrupt();
        }).start();
    }

    private LockManager mManager;
    private ExecutorService mExecutor;

    @Before
    public void setup() {
        mManager = new LockManager(null, null, -1);
    }

    @After
    public void teardown() {
        if (mExecutor != null) {
            mExecutor.shutdown();
        }
    }

    @Test
    public void basicShared() throws Exception {
        Locker locker = new Locker(mManager);
        assertEquals(ACQUIRED, locker.tryLockShared(0, k1, -1));
        assertEquals(1, mManager.numLocksHeld());
        assertEquals(OWNED_SHARED, locker.tryLockShared(0, k1, -1));
        assertEquals(1, mManager.numLocksHeld());
        assertEquals(ACQUIRED, locker.tryLockShared(0, k2, -1));
        assertEquals(OWNED_SHARED, locker.tryLockShared(0, k1, -1));
        assertEquals(OWNED_SHARED, locker.tryLockShared(0, k2, -1));
        assertEquals(2, mManager.numLocksHeld());
        locker.scopeExitAll();
        assertEquals(0, mManager.numLocksHeld());

        assertEquals(ACQUIRED, locker.tryLockShared(0, k1, -1));
        assertEquals(ACQUIRED, locker.tryLockShared(0, k2, -1));
        assertEquals(k2, locker.lastLockedKey());
        locker.unlock();
        assertEquals(k1, locker.lastLockedKey());
        locker.unlock();
        assertEquals(ACQUIRED, locker.tryLockShared(0, k1, -1));
        assertEquals(ACQUIRED, locker.tryLockShared(0, k2, -1));
        assertEquals(k2, locker.lastLockedKey());
        locker.unlock();

        Locker locker2 = new Locker(mManager);
        assertEquals(ACQUIRED, locker2.tryLockShared(0, k1, -1));
        assertEquals(ACQUIRED, locker2.tryLockShared(0, k2, -1));
        assertEquals(OWNED_SHARED, locker.tryLockShared(0, k1, -1));
        assertEquals(ACQUIRED, locker.tryLockShared(0, k2, -1));

        assertEquals(k2, locker.lastLockedKey());
        locker.unlockToShared();
        assertEquals(k2, locker2.lastLockedKey());
        locker2.unlockToShared();
        assertEquals(k2, locker2.lastLockedKey());
        locker2.unlockToShared();

        assertEquals(OWNED_SHARED, locker2.tryLockShared(0, k1, -1));
        assertEquals(OWNED_SHARED, locker2.tryLockShared(0, k2, -1));
        assertEquals(OWNED_SHARED, locker.tryLockShared(0, k1, -1));
        assertEquals(OWNED_SHARED, locker.tryLockShared(0, k2, -1));

        locker.scopeExitAll();
        locker2.scopeExitAll();

        try {
            locker.unlock();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(ACQUIRED, locker.tryLockShared(0, k1, -1));

        assertEquals(ILLEGAL, locker.tryLockUpgradable(0, k1, -1));
        assertEquals(ILLEGAL, locker.tryLockExclusive(0, k1, -1));

        locker.scopeExitAll();
        locker2.scopeExitAll();
    }

    @Test
    public void basicUpgradable() throws Exception {
        Locker locker = new Locker(mManager);
        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k1, -1));
        assertEquals(1, mManager.numLocksHeld());
        assertEquals(OWNED_UPGRADABLE, locker.tryLockUpgradable(0, k1, -1));
        assertEquals(1, mManager.numLocksHeld());
        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k2, -1));
        assertEquals(OWNED_UPGRADABLE, locker.tryLockUpgradable(0, k1, -1));
        assertEquals(OWNED_UPGRADABLE, locker.tryLockUpgradable(0, k2, -1));
        assertEquals(2, mManager.numLocksHeld());
        locker.scopeExitAll();
        assertEquals(0, mManager.numLocksHeld());

        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k1, -1));
        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k2, -1));
        assertEquals(k2, locker.lastLockedKey());
        locker.unlock();
        assertEquals(k1, locker.lastLockedKey());
        locker.unlock();
        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k1, -1));
        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k2, -1));
        assertEquals(k2, locker.lastLockedKey());
        locker.unlock();

        Locker locker2 = new Locker(mManager);
        assertEquals(TIMED_OUT_LOCK, locker2.tryLockUpgradable(0, k1, SHORT_TIMEOUT));
        assertEquals(ACQUIRED, locker2.tryLockUpgradable(0, k2, -1));
        assertEquals(OWNED_UPGRADABLE, locker.tryLockUpgradable(0, k1, -1));
        assertEquals(TIMED_OUT_LOCK, locker.tryLockUpgradable(0, k2, SHORT_TIMEOUT));
        assertEquals(ACQUIRED, locker2.tryLockShared(0, k1, -1));
        assertEquals(OWNED_SHARED, locker2.tryLockShared(0, k1, -1));
        assertEquals(OWNED_UPGRADABLE, locker2.tryLockShared(0, k2, -1));
        assertEquals(OWNED_UPGRADABLE, locker.tryLockShared(0, k1, -1));
        assertEquals(ACQUIRED, locker.tryLockShared(0, k2, -1));
        assertEquals(OWNED_SHARED, locker.tryLockShared(0, k2, -1));

        try {
            locker.unlockToUpgradable();
            fail();
        } catch (IllegalStateException e) {
        }
        assertEquals(k2, locker.lastLockedKey());
        locker.unlockToShared();
        try {
            locker2.unlockToUpgradable();
            fail();
        } catch (IllegalStateException e) {
        }
        assertEquals(k1, locker2.lastLockedKey());
        locker2.unlockToShared();
        assertEquals(k1, locker2.lastLockedKey());
        locker2.unlockToShared();

        assertEquals(ILLEGAL, locker2.tryLockUpgradable(0, k1, -1));
        assertEquals(OWNED_UPGRADABLE, locker2.tryLockUpgradable(0, k2, -1));
        assertEquals(OWNED_UPGRADABLE, locker.tryLockUpgradable(0, k1, -1));
        assertEquals(ILLEGAL, locker.tryLockUpgradable(0, k2, -1));

        locker.scopeExitAll();
        locker2.scopeExitAll();

        try {
            locker.unlock();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k1, -1));

        assertEquals(k1, locker.lastLockedKey());
        locker.unlockToUpgradable();
        assertEquals(OWNED_UPGRADABLE, locker.tryLockShared(0, k1, -1));
        assertEquals(k1, locker.lastLockedKey());
        locker.unlockToShared();
        
        assertEquals(OWNED_SHARED, locker.tryLockShared(0, k1, -1));
        assertEquals(ILLEGAL, locker.tryLockUpgradable(0, k1, -1));
        assertEquals(ILLEGAL, locker.tryLockExclusive(0, k1, -1));

        locker.scopeExitAll();
        locker2.scopeExitAll();
    }

    @Test
    public void basicExclusive() throws Exception {
        Locker locker = new Locker(mManager);
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        assertEquals(1, mManager.numLocksHeld());
        assertEquals(OWNED_EXCLUSIVE, locker.tryLockExclusive(0, k1, -1));
        assertEquals(1, mManager.numLocksHeld());
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k2, -1));
        assertEquals(OWNED_EXCLUSIVE, locker.tryLockExclusive(0, k1, -1));
        assertEquals(OWNED_EXCLUSIVE, locker.tryLockExclusive(0, k2, -1));
        assertEquals(2, mManager.numLocksHeld());
        locker.scopeExitAll();
        assertEquals(0, mManager.numLocksHeld());

        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k2, -1));
        assertEquals(k2, locker.lastLockedKey());
        locker.unlock();
        assertEquals(k1, locker.lastLockedKey());
        locker.unlock();
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k2, -1));
        assertEquals(k2, locker.lastLockedKey());
        locker.unlock();

        Locker locker2 = new Locker(mManager);
        assertEquals(TIMED_OUT_LOCK, locker2.tryLockShared(0, k1, SHORT_TIMEOUT));
        assertEquals(TIMED_OUT_LOCK, locker2.tryLockUpgradable(0, k1, SHORT_TIMEOUT));
        assertEquals(TIMED_OUT_LOCK, locker2.tryLockExclusive(0, k1, SHORT_TIMEOUT));
        assertEquals(ACQUIRED, locker2.tryLockExclusive(0, k2, -1));
        assertEquals(OWNED_EXCLUSIVE, locker.tryLockExclusive(0, k1, -1));
        assertEquals(TIMED_OUT_LOCK, locker.tryLockShared(0, k2, SHORT_TIMEOUT));
        assertEquals(TIMED_OUT_LOCK, locker.tryLockUpgradable(0, k2, SHORT_TIMEOUT));
        assertEquals(TIMED_OUT_LOCK, locker.tryLockExclusive(0, k2, SHORT_TIMEOUT));

        assertEquals(k1, locker.lastLockedKey());
        locker.unlockToUpgradable();
        assertEquals(OWNED_UPGRADABLE, locker.tryLockShared(0, k1, -1));
        assertEquals(OWNED_UPGRADABLE, locker.tryLockUpgradable(0, k1, -1));
        assertEquals(k1, locker.lastLockedKey());
        locker.unlockToShared();
        assertEquals(k2, locker2.lastLockedKey());
        locker2.unlockToUpgradable();
        assertEquals(k2, locker2.lastLockedKey());
        locker2.unlockToShared();
        assertEquals(OWNED_SHARED, locker2.tryLockShared(0, k2, -1));

        assertEquals(ILLEGAL, locker.tryLockExclusive(0, k1, -1));
        assertEquals(ILLEGAL, locker2.tryLockExclusive(0, k2, -1));

        locker.scopeExitAll();
        locker2.scopeExitAll();
        locker2.scopeExitAll();

        try {
            locker.unlock();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));

        assertEquals(k1, locker.lastLockedKey());
        locker.unlockToUpgradable();
        assertEquals(k1, locker.lastLockedKey());
        locker.unlockToUpgradable();
        assertEquals(OWNED_UPGRADABLE, locker.tryLockShared(0, k1, -1));
        assertEquals(k1, locker.lastLockedKey());
        locker.unlockToShared();
        assertEquals(k1, locker.lastLockedKey());
        locker.unlockToShared();
        
        assertEquals(OWNED_SHARED, locker.tryLockShared(0, k1, -1));
        assertEquals(ILLEGAL, locker.tryLockExclusive(0, k1, -1));

        locker.scopeExitAll();
        locker2.scopeExitAll();
    }

    @Test
    public void lenientUpgradeRule() throws Exception {
        LockManager manager = new LockManager(null, LockUpgradeRule.LENIENT, -1);

        Locker locker1 = new Locker(manager);
        Locker locker2 = new Locker(manager);

        assertEquals(ACQUIRED, locker1.tryLockShared(0, k1, -1));
        assertEquals(OWNED_UPGRADABLE, locker1.tryLockUpgradable(0, k1, -1));
        locker1.scopeExitAll();

        assertEquals(ACQUIRED, locker1.tryLockShared(0, k1, -1));
        assertEquals(ACQUIRED, locker2.tryLockShared(0, k1, -1));

        assertEquals(ILLEGAL, locker1.tryLockUpgradable(0, k1, -1));
        assertEquals(ILLEGAL, locker2.tryLockExclusive(0, k1, -1));

        locker1.scopeExitAll();

        assertEquals(UPGRADED, locker2.tryLockExclusive(0, k1, -1));
        locker2.unlockToUpgradable();
        assertEquals(UPGRADED, locker2.tryLockExclusive(0, k1, -1));
        locker2.unlockToShared();
        assertEquals(OWNED_UPGRADABLE, locker2.tryLockUpgradable(0, k1, -1));
        locker2.unlock();

        assertEquals(ACQUIRED, locker1.tryLockExclusive(0, k1, -1));
        locker1.unlock();
    }

    @Test
    public void uncheckedUpgradeRule() throws Exception {
        LockManager manager = new LockManager(null, LockUpgradeRule.UNCHECKED, -1);

        Locker locker1 = new Locker(manager);
        Locker locker2 = new Locker(manager);

        assertEquals(ACQUIRED, locker1.tryLockShared(0, k1, -1));
        assertEquals(OWNED_UPGRADABLE, locker1.tryLockUpgradable(0, k1, -1));
        locker1.scopeExitAll();

        assertEquals(ACQUIRED, locker1.tryLockShared(0, k1, -1));
        assertEquals(ACQUIRED, locker2.tryLockShared(0, k1, -1));

        assertEquals(OWNED_UPGRADABLE, locker1.tryLockUpgradable(0, k1, -1));
        assertEquals(TIMED_OUT_LOCK, locker2.tryLockExclusive(0, k1, 0));

        try {
            locker1.tryLockExclusive(0, k1, 10);
            fail();
        } catch (DeadlockException e) {
        }

        locker1.unlockToShared();

        assertEquals(OWNED_UPGRADABLE, locker2.tryLockUpgradable(0, k1, -1));
        assertEquals(TIMED_OUT_LOCK, locker2.tryLockExclusive(0, k1, 0));
        try {
            locker2.tryLockExclusive(0, k1, 1);
            fail();
        } catch (DeadlockException e) {
        }

        locker1.unlock();

        assertEquals(UPGRADED, locker2.tryLockExclusive(0, k1, -1));
        locker2.unlockToUpgradable();
        assertEquals(UPGRADED, locker2.tryLockExclusive(0, k1, -1));
        locker2.unlockToShared();
        assertEquals(OWNED_UPGRADABLE, locker2.tryLockUpgradable(0, k1, -1));
        locker2.unlock();

        assertEquals(ACQUIRED, locker1.tryLockExclusive(0, k1, -1));
        locker1.unlock();
    }

    @Test
    public void isolatedIndexes() throws Exception {
        Locker locker = new Locker(mManager);
        assertEquals(ACQUIRED, locker.tryLockExclusive(1, k1, -1));
        assertEquals(ACQUIRED, locker.tryLockExclusive(2, k1, -1));
        assertEquals(2, mManager.numLocksHeld());
        assertEquals(OWNED_EXCLUSIVE, locker.tryLockExclusive(1, k1, -1));
        assertEquals(OWNED_EXCLUSIVE, locker.tryLockExclusive(2, k1, -1));
        assertEquals(2, mManager.numLocksHeld());
        assertEquals(2, locker.lastLockedIndex());
        assertEquals(k1, locker.lastLockedKey());
        locker.scopeExitAll();
        assertEquals(0, mManager.numLocksHeld());
    }

    @Test
    public void upgrade() throws Exception {
        Locker locker = new Locker(mManager);
        Locker locker2 = new Locker(mManager);

        assertEquals(ACQUIRED, locker.tryLockShared(0, k1, -1));
        assertEquals(ACQUIRED, locker2.tryLockUpgradable(0, k1, -1));
        assertEquals(TIMED_OUT_LOCK, locker2.tryLockExclusive(0, k1, SHORT_TIMEOUT));
        scheduleUnlock(locker, 1000);
        assertEquals(UPGRADED, locker2.tryLockExclusive(0, k1, MEDIUM_TIMEOUT));
        assertEquals(OWNED_EXCLUSIVE, locker2.tryLockExclusive(0, k1, -1));
        assertEquals(TIMED_OUT_LOCK, locker.tryLockShared(0, k1, SHORT_TIMEOUT));
        assertEquals(TIMED_OUT_LOCK, locker.tryLockUpgradable(0, k1, SHORT_TIMEOUT));
        scheduleUnlockToUpgradable(locker2, 1000);
        assertEquals(ACQUIRED, locker.tryLockShared(0, k1, MEDIUM_TIMEOUT));
        assertEquals(OWNED_UPGRADABLE, locker2.tryLockUpgradable(0, k1, -1));

        locker.scopeExitAll();
        locker2.scopeExitAll();
    }

    @Test
    public void downgrade() throws Exception {
        Locker locker = new Locker(mManager);

        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k1, -1));
        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k2, -1));
        assertEquals(UPGRADED, locker.tryLockExclusive(0, k1, -1));
        try {
            locker.unlock();
            fail();
        } catch (IllegalStateException e) {
            // Non-immediate upgrade.
        }
        try {
            locker.unlockToShared();
            fail();
        } catch (IllegalStateException e) {
            // Non-immediate upgrade.
        }
        assertArrayEquals(k1, locker.lastLockedKey());
        locker.unlockToUpgradable();
        assertArrayEquals(k2, locker.lastLockedKey());
        locker.unlock();
        assertArrayEquals(k1, locker.lastLockedKey());
        locker.unlock();

        // Do again, but with another upgrade in between.

        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k1, -1));
        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k2, -1));
        assertEquals(UPGRADED, locker.tryLockExclusive(0, k2, -1));
        assertEquals(UPGRADED, locker.tryLockExclusive(0, k1, -1));
        try {
            locker.unlock();
            fail();
        } catch (IllegalStateException e) {
            // Non-immediate upgrade.
        }
        try {
            locker.unlockToShared();
            fail();
        } catch (IllegalStateException e) {
            // Non-immediate upgrade.
        }
        assertArrayEquals(k1, locker.lastLockedKey());
        locker.unlockToUpgradable();
        assertArrayEquals(k2, locker.lastLockedKey());
        locker.unlock();
        assertArrayEquals(k1, locker.lastLockedKey());
        locker.unlock();

        try {
            locker.unlockToShared();
            fail();
        } catch (IllegalStateException e) {
            // No locks held.
        }
        try {
            locker.unlockToUpgradable();
            fail();
        } catch (IllegalStateException e) {
            // No locks held.
        }
    }

    @Test
    public void downgrade2() throws Exception {
        // Owner holds exclusive lock, one waiter is upgradable, the other is shared.
        // Downgrading the owner allows both waiters to acquire the lock.
        release2(true);
    }

    @Test
    public void release2() throws Exception {
        // Owner holds exclusive lock, one waiter is upgradable, the other is shared.
        // Releasing the owner allows both waiters to acquire the lock.
        release2(false);
    }

    private void release2(boolean downgrade) throws Exception {
        Locker locker1 = new Locker(mManager);
        Locker locker2 = new Locker(mManager);
        Locker locker3 = new Locker(mManager);

        assertEquals(LockResult.ACQUIRED, locker1.lockExclusive(0, k1, -1));

        class Upgradable extends Thread {
            volatile Throwable failed;
            volatile LockResult result;

            @Override
            public void run() {
                try {
                    result = locker2.lockUpgradable(0, k1, -1);
                } catch (Throwable e) {
                    failed = e;
                }
            }
        }

        class Shared extends Thread {
            volatile Throwable failed;
            volatile LockResult result;

            @Override
            public void run() {
                try {
                    result = locker3.lockShared(0, k1, -1);
                } catch (Throwable e) {
                    failed = e;
                }
            }
        }

        Upgradable w1 = startAndWaitUntilBlocked(new Upgradable());
        Shared w2 = startAndWaitUntilBlocked(new Shared());

        assertNull(w1.result);
        assertNull(w2.result);

        if (downgrade) {
            locker1.unlockToShared();
        } else {
            locker1.unlock();
        }

        w1.join();
        w2.join();

        assertNull(w1.failed);
        assertNull(w2.failed);

        assertEquals(LockResult.ACQUIRED, w1.result);
        assertEquals(LockResult.ACQUIRED, w2.result);

        int hash = LockManager.hash(0, k1);

        if (downgrade) {
            assertEquals(LockResult.OWNED_SHARED, mManager.check(locker1, 0, k1, hash));
        } else {
            assertEquals(LockResult.UNOWNED, mManager.check(locker1, 0, k1, hash));
        }

        assertEquals(LockResult.OWNED_UPGRADABLE, mManager.check(locker2, 0, k1, hash));
        assertEquals(LockResult.OWNED_SHARED, mManager.check(locker3, 0, k1, hash));
    }

    @Test
    public void pileOfLocks() throws Exception {
        Locker locker = new Locker(mManager);
        for (int i=0; i<1000; i++) {
            assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("k" + i), -1));
        }
        assertEquals(1000, mManager.numLocksHeld());
        locker.scopeExitAll();
        for (int i=0; i<1000; i++) {
            assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("k" + i), -1));
        }
        for (int i=0; i<1000; i++) {
            assertEquals(OWNED_EXCLUSIVE, locker.tryLockExclusive(0, key("k" + i), -1));
        }
        for (int i=1000; --i>=0; ) {
            assertArrayEquals(key("k" + i), locker.lastLockedKey());
            locker.unlock();
        }
        for (int i=0; i<1000; i++) {
            assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("k" + i), -1));
        }
        locker.scopeExitAll();
    }

    @Test
    public void blockedNoWait() throws Exception {
        blocked(0);
    }

    @Test
    public void blockedTimedWait() throws Exception {
        blocked(SHORT_TIMEOUT);
    }

    private void blocked(long nanosTimeout) throws Exception {
        Locker locker = new Locker(mManager);
        Locker locker2 = new Locker(mManager);

        locker.tryLockShared(0, k1, -1);

        assertEquals(TIMED_OUT_LOCK, locker2.tryLockExclusive(0, k1, nanosTimeout));

        locker.unlock();

        assertEquals(ACQUIRED, locker2.tryLockExclusive(0, k1, -1));
        locker2.unlock();

        locker.tryLockUpgradable(0, k1, -1);

        assertEquals(TIMED_OUT_LOCK, locker2.tryLockUpgradable(0, k1, nanosTimeout));
        assertEquals(TIMED_OUT_LOCK, locker2.tryLockExclusive(0, k1, nanosTimeout));

        locker.unlock();

        assertEquals(ACQUIRED, locker2.tryLockUpgradable(0, k1, -1));
        locker2.unlock();

        locker.tryLockExclusive(0, k1, -1);

        assertEquals(TIMED_OUT_LOCK, locker2.tryLockShared(0, k1, nanosTimeout));
        assertEquals(TIMED_OUT_LOCK, locker2.tryLockUpgradable(0, k1, nanosTimeout));
        assertEquals(TIMED_OUT_LOCK, locker2.tryLockExclusive(0, k1, nanosTimeout));

        locker.unlock();

        assertEquals(ACQUIRED, locker2.tryLockShared(0, k1, -1));
        locker2.unlock();

        locker.scopeExitAll();
        locker2.scopeExitAll();
    }

    @Test
    public void interrupts() throws Exception {
        interrupts(-1);
    }

    @Test
    public void interruptsTimedWait() throws Exception {
        interrupts(10000 * ONE_MILLIS_IN_NANOS);
    }

    private void interrupts(long nanosTimeout) throws Exception {
        Locker locker = new Locker(mManager);
        Locker locker2 = new Locker(mManager);

        locker.tryLockShared(0, k1, -1);

        selfInterrupt(1000);
        assertEquals(INTERRUPTED, locker2.tryLockExclusive(0, k1, nanosTimeout));
        assertFalse(Thread.interrupted());

        locker.unlock();

        assertEquals(ACQUIRED, locker2.tryLockExclusive(0, k1, -1));
        locker2.unlock();

        locker.tryLockUpgradable(0, k1, -1);

        selfInterrupt(1000);
        assertEquals(INTERRUPTED, locker2.tryLockUpgradable(0, k1, nanosTimeout));
        assertFalse(Thread.interrupted());
        selfInterrupt(1000);
        assertEquals(INTERRUPTED, locker2.tryLockExclusive(0, k1, nanosTimeout));
        assertFalse(Thread.interrupted());

        locker.unlock();

        assertEquals(ACQUIRED, locker2.tryLockUpgradable(0, k1, -1));
        locker2.unlock();

        locker.tryLockExclusive(0, k1, -1);

        selfInterrupt(1000);
        assertEquals(INTERRUPTED, locker2.tryLockShared(0, k1, nanosTimeout));
        assertFalse(Thread.interrupted());
        selfInterrupt(1000);
        assertEquals(INTERRUPTED, locker2.tryLockUpgradable(0, k1, nanosTimeout));
        assertFalse(Thread.interrupted());
        selfInterrupt(1000);
        assertEquals(INTERRUPTED, locker2.tryLockExclusive(0, k1, nanosTimeout));
        assertFalse(Thread.interrupted());

        locker.unlock();

        assertEquals(ACQUIRED, locker2.tryLockShared(0, k1, -1));
        locker2.unlock();

        locker.scopeExitAll();
        locker2.scopeExitAll();
    }

    @Test
    public void delayedAcquire() throws Exception {
        Locker locker = new Locker(mManager);
        Locker locker2 = new Locker(mManager);
        long end;

        // Exclusive locks blocked...

        // Exclusive lock blocked by shared lock.
        assertEquals(ACQUIRED, locker.tryLockShared(0, k1, -1));
        end = scheduleUnlock(locker, 1000);
        assertEquals(ACQUIRED, locker2.tryLockExclusive(0, k1, MEDIUM_TIMEOUT));
        assertEquals(TIMED_OUT_LOCK, locker.tryLockShared(0, k1, 0));
        locker2.unlock();
        assertTrue(System.nanoTime() >= end);

        // Exclusive lock blocked by upgradable lock.
        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k1, -1));
        end = scheduleUnlock(locker, 1000);
        assertEquals(ACQUIRED, locker2.tryLockExclusive(0, k1, MEDIUM_TIMEOUT));
        assertEquals(TIMED_OUT_LOCK, locker.tryLockUpgradable(0, k1, 0));
        locker2.unlock();
        assertTrue(System.nanoTime() >= end);

        // Exclusive lock blocked by exclusive lock.
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        end = scheduleUnlock(locker, 1000);
        assertEquals(ACQUIRED, locker2.tryLockExclusive(0, k1, MEDIUM_TIMEOUT));
        assertEquals(TIMED_OUT_LOCK, locker.tryLockUpgradable(0, k1, 0));
        locker2.unlock();
        assertTrue(System.nanoTime() >= end);

        // Upgradable locks blocked...

        // Upgradable lock blocked by upgradable lock.
        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k1, -1));
        end = scheduleUnlock(locker, 1000);
        assertEquals(ACQUIRED, locker2.tryLockUpgradable(0, k1, MEDIUM_TIMEOUT));
        assertEquals(TIMED_OUT_LOCK, locker.tryLockUpgradable(0, k1, 0));
        locker2.unlock();
        assertTrue(System.nanoTime() >= end);

        // Upgradable lock blocked by upgradable lock, granted via downgrade to shared.
        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k1, -1));
        end = scheduleUnlockToShared(locker, 1000);
        assertEquals(ACQUIRED, locker2.tryLockUpgradable(0, k1, MEDIUM_TIMEOUT));
        assertEquals(OWNED_SHARED, locker.tryLockShared(0, k1, 0));
        locker2.unlock();
        locker.unlock();
        assertTrue(System.nanoTime() >= end);

        // Upgradable lock blocked by exclusive lock.
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        end = scheduleUnlock(locker, 1000);
        assertEquals(ACQUIRED, locker2.tryLockUpgradable(0, k1, MEDIUM_TIMEOUT));
        assertEquals(TIMED_OUT_LOCK, locker.tryLockUpgradable(0, k1, 0));
        locker2.unlock();
        assertTrue(System.nanoTime() >= end);

        // Upgradable lock blocked by exclusive lock, granted via downgrade to shared.
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        end = scheduleUnlockToShared(locker, 1000);
        assertEquals(ACQUIRED, locker2.tryLockUpgradable(0, k1, MEDIUM_TIMEOUT));
        assertEquals(OWNED_SHARED, locker.tryLockShared(0, k1, 0));
        locker2.unlock();
        locker.unlock();
        assertTrue(System.nanoTime() >= end);

        // Shared locks blocked...

        // Shared lock blocked by exclusive lock.
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        end = scheduleUnlock(locker, 1000);
        assertEquals(ACQUIRED, locker2.tryLockShared(0, k1, MEDIUM_TIMEOUT));
        assertEquals(ACQUIRED, locker.tryLockShared(0, k1, 0));
        locker.unlock();
        locker2.unlock();
        assertTrue(System.nanoTime() >= end);

        // Shared lock blocked by exclusive lock, granted via downgrade to shared.
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        end = scheduleUnlockToShared(locker, 1000);
        assertEquals(ACQUIRED, locker2.tryLockShared(0, k1, MEDIUM_TIMEOUT));
        assertEquals(OWNED_SHARED, locker.tryLockShared(0, k1, 0));
        locker.unlock();
        locker2.unlock();
        assertTrue(System.nanoTime() >= end);

        // Shared lock blocked by exclusive lock, granted via downgrade to upgradable.
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        end = scheduleUnlockToUpgradable(locker, 1000);
        assertEquals(ACQUIRED, locker2.tryLockShared(0, k1, MEDIUM_TIMEOUT));
        assertEquals(OWNED_UPGRADABLE, locker.tryLockShared(0, k1, 0));
        locker.unlock();
        locker2.unlock();
        assertTrue(System.nanoTime() >= end);
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void fifo() throws Exception {
        for (int i=1;; i++) {
            long start = System.nanoTime();
            try {
                fifo(10);
                return;
            } catch (AssertionError e) {
                if (i == 10 || e.getMessage().indexOf("TIMED") < 0) {
                    throw e;
                }
                // Tolerate unexpected long stalls which cause timeouts.
                long end = System.nanoTime();
                if ((end - start) < MEDIUM_TIMEOUT) {
                    throw e;
                }
                teardown();
                setup();
            }
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private void fifo(final int count) throws Exception {
        mExecutor = Executors.newCachedThreadPool();

        Locker[] lockers = new Locker[count];
        for (int i=0; i<count; i++) {
            lockers[i] = new Locker(mManager);
        }
        Future<LockResult>[] futures = new Future[count];

        // Upgradable locks acquired in fifo order.
        synchronized (lockers[0]) {
            lockers[0].tryLockUpgradable(0, k1, -1);
        }
        for (int i=1; i<lockers.length; i++) {
            // Sleep between attempts, to ensure proper enqueue ordering while
            // threads are concurrently starting.
            sleep(100);
            futures[i] = tryLockUpgradable(lockers[i], 0, k1);
        }
        for (int i=1; i<lockers.length; i++) {
            Locker last = lockers[i - 1];
            synchronized (last) {
                last.unlock();
            }
            assertEquals(ACQUIRED, futures[i].get());
        }

        // Clean up.
        for (int i=0; i<count; i++) {
            synchronized (lockers[i]) {
                lockers[i].scopeExitAll();
            }
        }

        // Shared lock, enqueue exclusive lock, remaining shared locks must wait.
        synchronized (lockers[0]) {
            lockers[0].tryLockShared(0, k1, -1);
        }
        synchronized (lockers[1]) {
            assertEquals(TIMED_OUT_LOCK, lockers[1].tryLockExclusive(0, k1, SHORT_TIMEOUT));
        }
        futures[1] = tryLockExclusive(lockers[1], 0, k1);
        sleep(100);
        synchronized (lockers[2]) {
            assertEquals(TIMED_OUT_LOCK, lockers[2].tryLockShared(0, k1, SHORT_TIMEOUT));
        }
        for (int i=2; i<lockers.length; i++) {
            sleep(100);
            futures[i] = tryLockShared(lockers[i], 0, k1);
        }
        // Now release first shared lock.
        synchronized (lockers[0]) {
            lockers[0].unlock();
        }
        // Exclusive lock is now available.
        assertEquals(ACQUIRED, futures[1].get());
        // Verify shared locks not held.
        sleep(100);
        for (int i=2; i<lockers.length; i++) {
            assertFalse(futures[i].isDone());
        }
        // Release exclusive and let shared in.
        synchronized (lockers[1]) {
            lockers[1].unlock();
        }
        // Shared locks all acquired now.
        for (int i=2; i<lockers.length; i++) {
            assertEquals(ACQUIRED, futures[i].get());
        }

        // Clean up.
        for (int i=0; i<count; i++) {
            synchronized (lockers[i]) {
                lockers[i].scopeExitAll();
            }
        }
    }

    @Test
    public void scoping() throws Exception {
        // Lots o' sub tests, many of which were created to improve code coverage.

        Locker locker = new Locker(mManager);

        assertNull(locker.scopeExit());

        locker.scopeEnter();
        assertNotNull(locker.scopeExit());

        locker.scopeEnter();
        locker.scopeEnter();
        assertNotNull(locker.scopeExit());
        assertNotNull(locker.scopeExit());
        assertNull(locker.scopeExit());

        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        assertEquals(OWNED_EXCLUSIVE, locker.tryLockExclusive(0, k1, -1));
        assertNotNull(locker.scopeExit());
        assertEquals(OWNED_EXCLUSIVE, locker.tryLockExclusive(0, k1, -1));
        assertNull(locker.scopeExit());
        assertEquals(UNOWNED, locker.lockCheck(0, k1));

        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k2, -1));
        assertNotNull(locker.scopeExit());
        assertEquals(UNOWNED, locker.lockCheck(0, k2));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
        locker.scopeExitAll();

        // Upgrade lock within scope, and then exit scope.
        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k1, -1));
        locker.scopeEnter();
        assertEquals(UPGRADED, locker.tryLockExclusive(0, k1, -1));
        assertNotNull(locker.scopeExit());
        // Outer scope was downgraded to original lock strength.
        assertEquals(OWNED_UPGRADABLE, locker.lockCheck(0, k1));
        locker.scopeExitAll();

        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k2, -1));
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k3, -1));
        // ScopeExitAll and unlock all scopes.
        locker.scopeExitAll();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));
        assertEquals(UNOWNED, locker.lockCheck(0, k2));
        assertEquals(UNOWNED, locker.lockCheck(0, k3));
        assertNull(locker.scopeExit());

        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k2, -1));
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k3, -1));
        // ScopeExitAll and unlock all scopes.
        locker.scopeExitAll();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));
        assertEquals(UNOWNED, locker.lockCheck(0, k2));
        assertEquals(UNOWNED, locker.lockCheck(0, k3));
        assertNull(locker.scopeExit());

        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k2, -1));
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k3, -1));
        locker.promote();
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k3));
        assertNotNull(locker.scopeExit());
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k3));
        locker.promote();
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k2));
        assertNotNull(locker.scopeExit());
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k2));
        assertNull(locker.scopeExit());
        assertEquals(UNOWNED, locker.lockCheck(0, k1));
        assertEquals(UNOWNED, locker.lockCheck(0, k2));
        assertEquals(UNOWNED, locker.lockCheck(0, k3));
        
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k2, -1));
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k3, -1));
        locker.promote();
        assertNotNull(locker.scopeExit());
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k3));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k2));
        assertNull(locker.scopeExit());
        assertEquals(UNOWNED, locker.lockCheck(0, k1));
        assertEquals(UNOWNED, locker.lockCheck(0, k2));
        assertEquals(UNOWNED, locker.lockCheck(0, k3));

        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k2, -1));
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k3, -1));
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k4, -1));
        locker.promote();
        assertNotNull(locker.scopeExit());
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k3));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k4));
        locker.promote();
        assertNotNull(locker.scopeExit());
        assertNull(locker.scopeExit());
        assertEquals(UNOWNED, locker.lockCheck(0, k1));
        assertEquals(UNOWNED, locker.lockCheck(0, k2));
        assertEquals(UNOWNED, locker.lockCheck(0, k3));
        assertEquals(UNOWNED, locker.lockCheck(0, k4));

        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        // Fill up first block of locks.
        for (int i=0; i<8; i++) {
            assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("k" + i), -1));
        }
        locker.promote();
        assertNotNull(locker.scopeExit());
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
        for (int i=0; i<8; i++) {
            assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, key("k" + i)));
        }
        assertNull(locker.scopeExit());
        assertEquals(UNOWNED, locker.lockCheck(0, k1));
        for (int i=0; i<8; i++) {
            assertEquals(UNOWNED, locker.lockCheck(0, key("k" + i)));
        }

        // Fill up first block of locks.
        for (int i=0; i<8; i++) {
            assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("k" + i), -1));
        }
        locker.scopeEnter();
        // Fill up another first block of locks.
        for (int i=0; i<8; i++) {
            assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("a" + i), -1));
        }
        locker.promote();
        assertNotNull(locker.scopeExit());
        for (int i=0; i<8; i++) {
            assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, key("k" + i)));
            assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, key("a" + i)));
        }
        assertNull(locker.scopeExit());
        for (int i=0; i<8; i++) {
            assertEquals(UNOWNED, locker.lockCheck(0, key("k" + i)));
            assertEquals(UNOWNED, locker.lockCheck(0, key("a" + i)));
        }

        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k2, -1));
        locker.scopeEnter();
        // Fill up first block of locks.
        for (int i=0; i<8; i++) {
            assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("k" + i), -1));
        }
        for (int q=0; q<2; q++) {
            locker.promote();
            assertNotNull(locker.scopeExit());
            assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
            assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k2));
            for (int i=0; i<8; i++) {
                assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, key("k" + i)));
            }
        }
        assertNull(locker.scopeExit());
        assertEquals(UNOWNED, locker.lockCheck(0, k1));
        assertEquals(UNOWNED, locker.lockCheck(0, k2));
        for (int i=0; i<8; i++) {
            assertEquals(UNOWNED, locker.lockCheck(0, key("k" + i)));
        }

        // Deep scoping.
        for (int i=0; i<10; i++) {
            locker.scopeEnter();
            assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("k" + i), -1));
        }
        for (int i=10; --i>=0; ) {
            locker.scopeExit();
            assertEquals(UNOWNED, locker.lockCheck(0, key("k" + i)));
        }
        locker.scopeExitAll();

        for (int q=0; q<3; q++) {
            for (int w=0; w<2; w++) {
                if (w != 0) {
                    for (int i=0; i<100; i++) {
                        assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("v" + i), -1));
                    }
                }

                assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k1, -1));
                locker.scopeEnter();
                // Fill up first block of locks.
                for (int i=0; i<8; i++) {
                    assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("k" + i), -1));
                }
                locker.promote();
                assertNotNull(locker.scopeExit());
                for (int i=0; i<8; i++) {
                    locker.unlock();
                }
                assertEquals(UPGRADED, locker.tryLockExclusive(0, k1, -1));
                if (q == 0) {
                    locker.unlockToShared();
                    assertEquals(OWNED_SHARED, locker.lockCheck(0, k1));
                } else if (q == 1) {
                    locker.unlockToUpgradable();
                    assertEquals(OWNED_UPGRADABLE, locker.lockCheck(0, k1));
                } else {
                    locker.unlock();
                    assertEquals(UNOWNED, locker.lockCheck(0, k1));
                }
                assertEquals(ACQUIRED, locker.tryLockExclusive(0, k2, -1));
                locker.scopeExitAll();
                assertEquals(UNOWNED, locker.lockCheck(0, k1));
                for (int i=0; i<8; i++) {
                    assertEquals(UNOWNED, locker.lockCheck(0, key("k" + i)));
                }
            }
        }

        // Create a chain with alternating tiny blocks.
        for (int q=0; q<4; q++) {
            locker.scopeEnter();
            assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("q" + q), -1));
            locker.scopeEnter();
            for (int i=0; i<8; i++) {
                assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("k" + q + "_" + i), -1));
            }
        }
        for (int q=0; q<8; q++) {
            locker.promote();
            locker.scopeExit();
        }
        for (int q=4; --q>=0; ) {
            for (int i=8; --i>=0; ) {
                assertArrayEquals(key("k" + q + "_" + i), locker.lastLockedKey());
                locker.unlock();
            }
            assertArrayEquals(key("q" + q), locker.lastLockedKey());
            locker.unlock();
        }

        for (int q=0; q<2; q++) {
            assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k1, -1));
            locker.scopeEnter();
            if (q != 0) {
                for (int i=0; i<(8 + 16); i++) {
                    assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("v" + i), -1));
                }
            }
            assertEquals(UPGRADED, locker.tryLockExclusive(0, k1, -1));
            assertEquals(ACQUIRED, locker.tryLockShared(0, k2, -1));
            locker.unlock();
            assertEquals(UNOWNED, locker.lockCheck(0, k2));
            assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
            try {
                locker.unlockToUpgradable();
                fail();
            } catch (IllegalStateException e) {
            }
            assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
            locker.scopeExitAll();
            assertEquals(UNOWNED, locker.lockCheck(0, k1));
        }

        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        for (int i=0; i<8; i++) {
            assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("k" + i), -1));
        }
        locker.promote();
        assertNotNull(locker.scopeExit());
        for (int i=8; --i>=0; ) {
            locker.unlock();
            assertEquals(UNOWNED, locker.lockCheck(0, key("k" + i)));
        }
        locker.scopeEnter();
        for (int i=0; i<4; i++) {
            assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("v" + i), -1));
        }
        locker.promote();
        assertNotNull(locker.scopeExit());
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
        for (int i=0; i<4; i++) {
            assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, key("v" + i)));
        }
        for (int i=4; --i>=0; ) {
            locker.unlock();
            assertEquals(UNOWNED, locker.lockCheck(0, key("v" + i)));
        }
        locker.unlock();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));
        for (int i=0; i<8; i++) {
            assertEquals(UNOWNED, locker.lockCheck(0, key("k" + i)));
        }
        assertNotNull(locker.scopeExit());
        assertNull(locker.scopeExit());

        for (int i=0; i<9; i++) {
            assertEquals(ACQUIRED, locker.tryLockExclusive(0, key("k" + i), -1));
        }
        locker.scopeEnter();
        for (int i=0; i<4; i++) {
            assertEquals(ACQUIRED, locker.tryLockUpgradable(0, key("v" + i), -1));
        }
        locker.promote();
        locker.scopeExit();
        for (int i=0; i<9; i++) {
            assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, key("k" + i)));
        }
        for (int i=0; i<4; i++) {
            assertEquals(OWNED_UPGRADABLE, locker.lockCheck(0, key("v" + i)));
        }
        assertNull(locker.scopeExit());
        for (int i=0; i<9; i++) {
            assertEquals(UNOWNED, locker.lockCheck(0, key("k" + i)));
        }
        for (int i=0; i<4; i++) {
            assertEquals(UNOWNED, locker.lockCheck(0, key("v" + i)));
        }
    }

    @Test
    public void unlockSavepoint() throws Exception {
        Locker locker = new Locker(mManager);

        locker.scopeEnter();
        locker.lockExclusive(0, k1, -1);
        locker.lockExclusive(0, k2, -1);
        locker.scopeUnlockAll();

        try {
            locker.lastLockedKey();
        } catch (IllegalStateException e) {
            // Good.
        }

        try {
            locker.unlock();
        } catch (IllegalStateException e) {
            // Good.
        }
    }

    @Test
    public void promote() throws Exception {
        Locker locker = new Locker(mManager);

        locker.scopeEnter();
        locker.tryLockExclusive(0, k1, -1);
        locker.promote();
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
        locker.scopeExit();
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
        locker.unlock();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));

        locker.scopeEnter();
        locker.tryLockShared(0, k2, -1);
        locker.promote();
        assertEquals(OWNED_SHARED, locker.lockCheck(0, k2));
        locker.scopeExit();
        assertEquals(OWNED_SHARED, locker.lockCheck(0, k2));
        locker.scopeExit();
        assertEquals(UNOWNED, locker.lockCheck(0, k2));

        locker.tryLockExclusive(0, k3, -1);
        locker.tryLockExclusive(0, k4, -1);
        locker.scopeEnter();
        locker.tryLockExclusive(0, key("e"), -1);
        locker.tryLockExclusive(0, key("f"), -1);
        locker.promote();
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, key("e")));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, key("f")));
        assertEquals(UNOWNED, locker.lockCheck(0, key("g")));
        locker.scopeExit();
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k3));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k4));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, key("e")));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, key("f")));
        locker.unlock();
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k3));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k4));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, key("e")));
        assertEquals(UNOWNED, locker.lockCheck(0, key("f")));
    }

    @Test
    public void promoteExitAll() throws Exception {
        Locker locker = new Locker(mManager);
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        locker.promote();
        locker.scopeExitAll();

        Locker locker2 = new Locker(mManager);
        assertEquals(ACQUIRED, locker2.tryLockExclusive(0, k1, SHORT_TIMEOUT));
    }

    @Test
    public void blockDiscard() throws Exception {
        // Test for a bug which caused a parent scope to reference a Block, but the current
        // (child) scope referenced a Lock. This is an illegal combination, resulting in a
        // class cast exception when rolling back.

        Locker locker = new Locker(mManager);

        locker.lockExclusive(0, k1, -1);
        locker.lockExclusive(0, k2, -1);

        // Must not leave an empty Block behind, which is illegal.
        locker.scopeUnlockAll();

        locker.scopeEnter();
        locker.lockExclusive(0, k3, -1);

        // If current reference is a Block, it gets null'd as a side-effect.
        locker.unlock();

        // If reference was null'd, this creates a Lock reference.
        locker.lockExclusive(0, k4, -1);

        // If parent references a Block and current reference is a Lock, this call fails.
        locker.scopeUnlockAll();
    }

    @Test
    public void promoteUnlockUpgraded() throws Exception {
        Locker locker = new Locker(mManager);

        // Enter, lock, promote...
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k1, -1));
        locker.promote();

        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k2, -1));
        assertEquals(UPGRADED, locker.tryLockExclusive(0, k2, -1));
        locker.unlock();

        assertEquals(OWNED_UPGRADABLE, locker.lockCheck(0, k1));
        assertEquals(UNOWNED, locker.lockCheck(0, k2));

        locker.scopeExitAll();

        // Same initial state but without promote.
        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k1, -1));
        locker.scopeEnter();

        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k2, -1));
        assertEquals(UPGRADED, locker.tryLockExclusive(0, k2, -1));
        locker.unlock();

        assertEquals(OWNED_UPGRADABLE, locker.lockCheck(0, k1));
        assertEquals(UNOWNED, locker.lockCheck(0, k2));
    }

    @Test
    public void reverseScan() throws Exception {
        // Test which discovered a race condition in LockOwner.hashCode method.

        Database db = Database.open(new DatabaseConfig().directPageAccess(false));
        View view = db.openIndex("index1");

        int numAttempts = 100;
        int batchSize = 20;

        int numConcurrentReads = 4;
        int numConcurrentWrites = 1;
        ExecutorService executor = Executors.newFixedThreadPool
            (numConcurrentReads + numConcurrentWrites);

        for (int attempt=0; attempt<numAttempts; attempt++) {
            final List<byte[]> keys = new ArrayList<>();
            final List<byte[]> vals = new ArrayList<>();

            // Prepare request
            for (int id=0; id<batchSize; id++) {
                int suffix = attempt*batchSize+id;

                byte[] key = ("key" + suffix).getBytes(StandardCharsets.UTF_8);
                byte[] val = ("value" + suffix).getBytes(StandardCharsets.UTF_8);

                keys.add(key);
                vals.add(val);
            }

            // Write
            Future[] writeFutures = new Future[numConcurrentWrites];
            for (int i=0; i<numConcurrentWrites; i++) {
                writeFutures[i] = executor.submit(() -> {
                    try {
                        for(int id=0; id<batchSize; id++) {
                            view.store(null, keys.get(id), vals.get(id));
                        }
                    } catch (Exception e) {
                        fail("Unexpected exception " + e);
                    }
                });
            };

            final List<byte[]> reversedKeys = new ArrayList<>(keys);
            Collections.reverse(reversedKeys);
            final List<byte[]> reversedVals = new ArrayList<>(vals);
            Collections.reverse(reversedVals);

            for (Future f : writeFutures) {
                f.get();
            }

            // Read
            Future[] readFutures = new Future[numConcurrentReads];
            for (int i=0; i<numConcurrentReads; i++) {
                final boolean reverse = (i % 2 == 1);
                readFutures[i] = executor.submit(() -> {
                    try {
                        final List<byte[]> ks = reverse ? reversedKeys : keys;
                        final List<byte[]> vs = reverse ? reversedVals : vals;
                        Transaction txn = db.newTransaction();
                        txn.lockMode(LockMode.REPEATABLE_READ);
                        for(int id=0; id<batchSize; id++) {
                            final byte[] k = ks.get(id);
                            final byte[] v = vs.get(id);
                            byte[] a = view.load(txn, k);
                            assertArrayEquals(a, v);
                        }
                        txn.reset();
                    } catch (Exception e) {
                        fail("Unexpected exception " + e);
                    }
                });
            }

            for (Future f : readFutures) {
                f.get();
            }
        }

        executor.shutdown();
    }

    @Test
    public void doubleLockSharedWithExclusiveWaiter() throws Exception {
        // Shared lock request must always check for existing ownership before blocking.

        Locker locker1 = new Locker(mManager);

        assertEquals(ACQUIRED, locker1.tryLockShared(0, k1, -1));

        Thread t = new Thread(() -> {
            try {
                Locker locker2 = new Locker(mManager);
                locker2.tryLockExclusive(0, k1, -1);
            } catch (Exception e) {
                // Bail.
            }
        });

        t.start();

        wait: {
            for (int i=0; i<100; i++) {
                if (t.getState() == Thread.State.WAITING) {
                    break wait;
                }
                Thread.sleep(100);
            }

            fail("Thread not blocked waiting for lock");
        }

        assertEquals(LockResult.OWNED_SHARED, locker1.tryLockShared(0, k1, 10_000_000_000L));

        locker1.scopeExitAll();
        t.join();
    }

    @Test
    public void illegalUnlock() throws Exception {
        Locker locker = new Locker(mManager);

        locker.lockExclusive(0, k1, -1);
        locker.scopeEnter();
        try {
            locker.unlock();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));

        locker.scopeExit();
        locker.unlock();

        assertEquals(UNOWNED, locker.lockCheck(0, k1));
    }

    @Test
    public void illegalUnlock2() throws Exception {
        Locker locker = new Locker(mManager);

        locker.lockExclusive(0, k1, -1);
        locker.scopeEnter();
        locker.lockExclusive(0, k2, -1);
        locker.unlock();

        try {
            locker.unlock();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
        assertEquals(UNOWNED, locker.lockCheck(0, k2));
    }

    @Test
    public void illegalUnlock3() throws Exception {
        Locker locker = new Locker(mManager);

        locker.lockExclusive(0, k1, -1);
        locker.lockExclusive(0, k2, -1);
        locker.scopeEnter();
        locker.lockExclusive(0, k3, -1);
        locker.unlock();

        try {
            locker.unlock();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k2));
        assertEquals(UNOWNED, locker.lockCheck(0, k3));
    }

    @Test
    public void illegalUnlock4() throws Exception {
        Locker locker = new Locker(mManager);

        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));

        try {
            locker.unlockToShared();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
    }

    @Test
    public void illegalUnlock5() throws Exception {
        Locker locker = new Locker(mManager);

        locker.lockExclusive(0, k1, -1);
        locker.scopeEnter();
        locker.lockExclusive(0, k2, -1);
        locker.unlock();

        try {
            locker.unlockToShared();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
        assertEquals(UNOWNED, locker.lockCheck(0, k2));
    }

    @Test
    public void illegalUnlock6() throws Exception {
        Locker locker = new Locker(mManager);

        locker.lockExclusive(0, k1, -1);
        locker.lockExclusive(0, k2, -1);
        locker.scopeEnter();
        locker.lockExclusive(0, k3, -1);
        locker.unlock();

        try {
            locker.unlockToShared();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k2));
        assertEquals(UNOWNED, locker.lockCheck(0, k3));
    }

    @Test
    public void illegalUnlock7() throws Exception {
        Locker locker = new Locker(mManager);

        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));

        try {
            locker.unlockToUpgradable();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
    }

    @Test
    public void illegalUnlock8() throws Exception {
        Locker locker = new Locker(mManager);

        locker.lockExclusive(0, k1, -1);
        locker.scopeEnter();
        locker.lockExclusive(0, k2, -1);
        locker.unlock();

        try {
            locker.unlockToUpgradable();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
        assertEquals(UNOWNED, locker.lockCheck(0, k2));
    }

    @Test
    public void illegalUnlock9() throws Exception {
        Locker locker = new Locker(mManager);

        locker.lockExclusive(0, k1, -1);
        locker.lockExclusive(0, k2, -1);
        locker.scopeEnter();
        locker.lockExclusive(0, k3, -1);
        locker.unlock();

        try {
            locker.unlockToUpgradable();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k2));
        assertEquals(UNOWNED, locker.lockCheck(0, k3));
    }

    @Test
    public void illegalUnlock10() throws Exception {
        Locker locker = new Locker(mManager);

        assertEquals(ACQUIRED, locker.tryLockUpgradable(0, k1, -1));
        assertEquals(ACQUIRED, locker.tryLockExclusive(0, k2, -1));
        assertEquals(UPGRADED, locker.tryLockExclusive(0, k1, -1));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
        locker.scopeEnter();
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));

        try {
            locker.unlockToUpgradable();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(OWNED_EXCLUSIVE, locker.tryLockUpgradable(0, k1, -1));
        locker.scopeEnter();
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
        assertEquals(OWNED_EXCLUSIVE, locker.tryLockExclusive(0, k1, -1));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));

        try {
            locker.unlockToUpgradable();
            fail();
        } catch (IllegalStateException e) {
        }

        try {
            locker.unlock();
            fail();
        } catch (IllegalStateException e) {
        }
    }

    @Test
    public void unlockCombine() throws Exception {
        Locker locker = new Locker(mManager);

        try {
            locker.unlockCombine();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(ACQUIRED, locker.lockShared(0, k1, -1));
        locker.unlockCombine();
        locker.unlock();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));

        // This pairing of acquire and upgrade works because its for the same lock, immediately
        // upgraded.
        assertEquals(ACQUIRED, locker.lockUpgradable(0, k1, -1));
        assertEquals(UPGRADED, locker.lockExclusive(0, k1, -1));
        locker.unlockCombine();
        locker.unlock();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));

        // This pairing fails because another lock is in between.
        assertEquals(ACQUIRED, locker.lockUpgradable(0, k1, -1));
        assertEquals(ACQUIRED, locker.lockUpgradable(0, k2, -1));
        assertEquals(UPGRADED, locker.lockExclusive(0, k1, -1));
        try {
            locker.unlockCombine();
            fail();
        } catch (IllegalStateException e) {
            // Cannot combine an acquire with an upgrade
        }
        try {
            locker.unlock();
            fail();
        } catch (IllegalStateException e) {
            // Cannot unlock non-immediate upgrade
        }
        locker.scopeExit();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));
        assertEquals(UNOWNED, locker.lockCheck(0, k2));

        // This pairing works.
        assertEquals(ACQUIRED, locker.lockUpgradable(0, k1, -1));
        assertEquals(ACQUIRED, locker.lockUpgradable(0, k2, -1));
        locker.unlockCombine();
        locker.unlock();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));
        assertEquals(UNOWNED, locker.lockCheck(0, k2));

        // This pairing of upgrades works.
        assertEquals(ACQUIRED, locker.lockUpgradable(0, k1, -1));
        assertEquals(UPGRADED, locker.lockExclusive(0, k1, -1));
        assertEquals(ACQUIRED, locker.lockUpgradable(0, k2, -1));
        assertEquals(UPGRADED, locker.lockExclusive(0, k2, -1));
        locker.unlockCombine();
        locker.unlock();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));
        assertEquals(UNOWNED, locker.lockCheck(0, k2));

        // This pairing of upgrades doesn't work.
        assertEquals(ACQUIRED, locker.lockUpgradable(0, k1, -1));
        assertEquals(ACQUIRED, locker.lockUpgradable(0, k2, -1));
        assertEquals(UPGRADED, locker.lockExclusive(0, k1, -1));
        assertEquals(UPGRADED, locker.lockExclusive(0, k2, -1));
        locker.unlockCombine();
        try {
            locker.unlock();
            fail();
        } catch (IllegalStateException e) {
            // Cannot unlock non-immediate upgrade
        }
        locker.scopeExit();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));
        assertEquals(UNOWNED, locker.lockCheck(0, k2));
    }

    @Test
    public void unlockCombineMany() throws Exception {
        Locker locker = new Locker(mManager);

        for (int i=0; i<1000; i++) {
            assertEquals(ACQUIRED, locker.lockShared(0, key("k" + i), -1));
            locker.unlockCombine();
        }
        locker.unlock();
        for (int i=0; i<1000; i++) {
            assertEquals(UNOWNED, locker.lockCheck(0, key("k" + i)));
        }

        // Again, but with a lock which should remain.
        assertEquals(ACQUIRED, locker.lockShared(0, k1, -1));
        for (int i=0; i<1000; i++) {
            assertEquals(ACQUIRED, locker.lockShared(0, key("k" + i), -1));
            if (i != 0) {
                locker.unlockCombine();
            }
        }
        assertEquals(OWNED_SHARED, locker.lockCheck(0, k1));
        locker.unlock();
        assertEquals(OWNED_SHARED, locker.lockCheck(0, k1));
        for (int i=0; i<1000; i++) {
            assertEquals(UNOWNED, locker.lockCheck(0, key("k" + i)));
        }
        locker.unlock();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));

        // Again, with a lock which should remain, and a combined downgrade.
        assertEquals(ACQUIRED, locker.lockShared(0, k1, -1));
        for (int i=0; i<1000; i++) {
            assertEquals(ACQUIRED, locker.lockExclusive(0, key("k" + i), -1));
            if (i != 0) {
                locker.unlockCombine();
            }
        }
        assertEquals(OWNED_SHARED, locker.lockCheck(0, k1));
        locker.unlockToUpgradable();
        assertEquals(OWNED_SHARED, locker.lockCheck(0, k1));
        for (int i=0; i<1000; i++) {
            assertEquals(OWNED_UPGRADABLE, locker.lockCheck(0, key("k" + i)));
        }
        locker.unlock();
        for (int i=0; i<1000; i++) {
            assertEquals(UNOWNED, locker.lockCheck(0, key("k" + i)));
        }
        assertEquals(OWNED_SHARED, locker.lockCheck(0, k1));
        locker.unlock();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));

        // Again, with a lock which should remain, and a combined downgrade to shared.
        assertEquals(ACQUIRED, locker.lockShared(0, k1, -1));
        for (int i=0; i<1000; i++) {
            assertEquals(ACQUIRED, locker.lockExclusive(0, key("k" + i), -1));
            if (i != 0) {
                locker.unlockCombine();
            }
        }
        assertEquals(OWNED_SHARED, locker.lockCheck(0, k1));
        locker.unlockToShared();
        assertEquals(OWNED_SHARED, locker.lockCheck(0, k1));
        for (int i=0; i<1000; i++) {
            assertEquals(OWNED_SHARED, locker.lockCheck(0, key("k" + i)));
        }
        locker.unlock();
        for (int i=0; i<1000; i++) {
            assertEquals(UNOWNED, locker.lockCheck(0, key("k" + i)));
        }
        assertEquals(OWNED_SHARED, locker.lockCheck(0, k1));
        locker.unlock();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));

        // Again, with a lock which should remain, a bunch of upgrades, and a combined downgrade.
        assertEquals(ACQUIRED, locker.lockShared(0, k1, -1));
        for (int i=0; i<1000; i++) {
            assertEquals(ACQUIRED, locker.lockUpgradable(0, key("k" + i), -1));
            assertEquals(UPGRADED, locker.lockExclusive(0, key("k" + i), -1));
            if (i != 0) {
                locker.unlockCombine();
            }
        }
        assertEquals(OWNED_SHARED, locker.lockCheck(0, k1));
        locker.unlockToUpgradable();
        assertEquals(OWNED_SHARED, locker.lockCheck(0, k1));
        for (int i=0; i<1000; i++) {
            assertEquals(OWNED_UPGRADABLE, locker.lockCheck(0, key("k" + i)));
        }
        locker.unlock();
        for (int i=0; i<1000; i++) {
            assertEquals(UNOWNED, locker.lockCheck(0, key("k" + i)));
        }
        assertEquals(OWNED_SHARED, locker.lockCheck(0, k1));
        locker.unlock();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));
    }

    @Test
    public void unlockCombineBlockOfOne() throws Exception {
        // Tests combine logic for a lock block with one entry.

        Locker locker = new Locker(mManager);

        // This forms a block of two.
        assertEquals(ACQUIRED, locker.lockShared(0, k1, -1));
        assertEquals(ACQUIRED, locker.lockShared(0, k2, -1));

        // Unlock the last, leaving a block with one entry.
        locker.unlock();

        // Combine shouldn't do anything.
        locker.unlockCombine();
        
        assertEquals(OWNED_SHARED, locker.lockCheck(0, k1));
        assertEquals(UNOWNED, locker.lockCheck(0, k2));

        locker.unlock();
    }

    @Test
    public void unlockDowngradeBlockOfOne() throws Exception {
        Locker locker = new Locker(mManager);

        assertEquals(ACQUIRED, locker.lockUpgradable(0, k1, -1));

        // After this, a block of 8 (initial size) should exist.
        for (int i=0; i<7; i++) {
            assertEquals(ACQUIRED, locker.lockShared(0, key("k" + i), -1));
        }

        // After this, a block of 1 should be stacked.
        assertEquals(UPGRADED, locker.lockExclusive(0, k1, -1));

        // This will unlock from a block of size 1, popping it off the stack.
        locker.unlockToUpgradable();

        assertEquals(OWNED_UPGRADABLE, locker.lockCheck(0, k1));

        // Now unlock everything and verify along the way.

        for (int i=7; --i>=0; ) {
            assertEquals(OWNED_SHARED, locker.lockCheck(0, key("k" + i)));
            locker.unlock();
            assertEquals(UNOWNED, locker.lockCheck(0, key("k" + i)));
        }

        assertEquals(OWNED_UPGRADABLE, locker.lockCheck(0, k1));
        locker.unlock();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));
    }

    @Test
    public void unlockDowngradeGroup() throws Exception {
        Locker locker = new Locker(mManager);

        assertEquals(ACQUIRED, locker.lockShared(0, k1, -1));
        assertEquals(ACQUIRED, locker.lockUpgradable(0, k2, -1));
        assertEquals(ACQUIRED, locker.lockUpgradable(0, k3, -1));
        assertEquals(ACQUIRED, locker.lockUpgradable(0, k4, -1));

        assertEquals(UPGRADED, locker.lockExclusive(0, k2, -1));
        assertEquals(UPGRADED, locker.lockExclusive(0, k3, -1));
        locker.unlockCombine();
        assertEquals(UPGRADED, locker.lockExclusive(0, k4, -1));
        locker.unlockCombine();

        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k2));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k3));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k4));

        locker.unlockToUpgradable();

        assertEquals(OWNED_SHARED, locker.lockCheck(0, k1));
        assertEquals(OWNED_UPGRADABLE, locker.lockCheck(0, k2));
        assertEquals(OWNED_UPGRADABLE, locker.lockCheck(0, k3));
        assertEquals(OWNED_UPGRADABLE, locker.lockCheck(0, k4));

        locker.scopeExit();

        // Again with various sizes.
        for (int size=1; size<=30; size++) {
            for (int i=0; i<size; i++) {
                assertEquals(ACQUIRED, locker.lockUpgradable(0, key("k" + i), -1));
            }

            for (int i=0; i<size; i++) {
                assertEquals(UPGRADED, locker.lockExclusive(0, key("k" + i), -1));
                if (i != 0) {
                    locker.unlockCombine();
                }
            }

            for (int i=0; i<size; i++) {
                assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, key("k" + i)));
            }

            locker.unlockToUpgradable();

            for (int i=0; i<size; i++) {
                assertEquals(OWNED_UPGRADABLE, locker.lockCheck(0, key("k" + i)));
            }

            for (int i=1; i<size; i++) {
                locker.unlockCombine();
            }

            locker.unlock();

            for (int i=0; i<size; i++) {
                assertEquals(UNOWNED, locker.lockCheck(0, key("k" + i)));
            }
        }
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
        new Thread(() -> {
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
        }).start();
        return end;
    }

    private Future<LockResult> tryLockShared(final Locker locker,
                                             final long indexId, final byte[] key)
    {
        return lockAsync(locker, indexId, key, 0);
    }

    private Future<LockResult> tryLockUpgradable(final Locker locker,
                                                 final long indexId, final byte[] key)
    {
        return lockAsync(locker, indexId, key, 1);
    }

    private Future<LockResult> tryLockExclusive(final Locker locker,
                                                final long indexId, final byte[] key)
    {
        return lockAsync(locker, indexId, key, 2);
    }

    private Future<LockResult> lockAsync(final Locker locker,
                                         final long indexId, final byte[] key,
                                         final int type)
    {
        return mExecutor.submit(new Callable<LockResult>() {
            public LockResult call() throws Exception {
                LockResult result;
                synchronized (locker) {
                    switch (type) {
                    default:
                        result = locker.tryLockShared(indexId, key, MEDIUM_TIMEOUT);
                        break;
                    case 1:
                        result = locker.tryLockUpgradable(indexId, key, MEDIUM_TIMEOUT);
                        break;
                    case 2:
                        result = locker.tryLockExclusive(indexId, key, MEDIUM_TIMEOUT);
                        break;
                    }
                }
                return result;
            }
        });
    }
}
