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

import java.nio.charset.StandardCharsets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

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
        startAndWaitUntilBlocked(new Thread(() -> {
            LockTest.sleep(delayMillis);
            thread.interrupt();
        }));
    }

    private LockManager mManager;
    private ExecutorService mExecutor;

    private Thread mScheduledUnlock;

    @Before
    public void setup() {
        mManager = new LockManager(null, null, -1);
    }

    @After
    public void teardown() {
        if (mExecutor != null) {
            mExecutor.shutdown();
        }
        mManager.close();
    }

    @Test
    public void basicShared() throws Exception {
        var locker = new Locker(mManager);
        assertEquals(ACQUIRED, locker.doTryLockShared(0, k1, -1));
        assertEquals(1, mManager.numLocksHeld());
        assertEquals(OWNED_SHARED, locker.doTryLockShared(0, k1, -1));
        assertEquals(1, mManager.numLocksHeld());
        assertEquals(ACQUIRED, locker.doTryLockShared(0, k2, -1));
        assertEquals(OWNED_SHARED, locker.doTryLockShared(0, k1, -1));
        assertEquals(OWNED_SHARED, locker.doTryLockShared(0, k2, -1));
        assertEquals(2, mManager.numLocksHeld());
        locker.scopeExitAll();
        assertEquals(0, mManager.numLocksHeld());

        assertEquals(ACQUIRED, locker.doTryLockShared(0, k1, -1));
        assertEquals(ACQUIRED, locker.doTryLockShared(0, k2, -1));
        assertEquals(k2, locker.lastLockedKey());
        locker.doUnlock();
        assertEquals(k1, locker.lastLockedKey());
        locker.doUnlock();
        assertEquals(ACQUIRED, locker.doTryLockShared(0, k1, -1));
        assertEquals(ACQUIRED, locker.doTryLockShared(0, k2, -1));
        assertEquals(k2, locker.lastLockedKey());
        locker.doUnlock();

        var locker2 = new Locker(mManager);
        assertEquals(ACQUIRED, locker2.doTryLockShared(0, k1, -1));
        assertEquals(ACQUIRED, locker2.doTryLockShared(0, k2, -1));
        assertEquals(OWNED_SHARED, locker.doTryLockShared(0, k1, -1));
        assertEquals(ACQUIRED, locker.doTryLockShared(0, k2, -1));

        assertEquals(k2, locker.lastLockedKey());
        locker.doUnlockToShared();
        assertEquals(k2, locker2.lastLockedKey());
        locker2.doUnlockToShared();
        assertEquals(k2, locker2.lastLockedKey());
        locker2.doUnlockToShared();

        assertEquals(OWNED_SHARED, locker2.doTryLockShared(0, k1, -1));
        assertEquals(OWNED_SHARED, locker2.doTryLockShared(0, k2, -1));
        assertEquals(OWNED_SHARED, locker.doTryLockShared(0, k1, -1));
        assertEquals(OWNED_SHARED, locker.doTryLockShared(0, k2, -1));

        locker.scopeExitAll();
        locker2.scopeExitAll();

        try {
            locker.doUnlock();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(ACQUIRED, locker.doTryLockShared(0, k1, -1));

        assertEquals(ILLEGAL, locker.doTryLockUpgradable(0, k1, -1));
        assertEquals(ILLEGAL, locker.doTryLockExclusive(0, k1, -1));

        locker.scopeExitAll();
        locker2.scopeExitAll();
    }

    @Test
    public void basicUpgradable() throws Exception {
        var locker = new Locker(mManager);
        assertEquals(ACQUIRED, locker.doTryLockUpgradable(0, k1, -1));
        assertEquals(1, mManager.numLocksHeld());
        assertEquals(OWNED_UPGRADABLE, locker.doTryLockUpgradable(0, k1, -1));
        assertEquals(1, mManager.numLocksHeld());
        assertEquals(ACQUIRED, locker.doTryLockUpgradable(0, k2, -1));
        assertEquals(OWNED_UPGRADABLE, locker.doTryLockUpgradable(0, k1, -1));
        assertEquals(OWNED_UPGRADABLE, locker.doTryLockUpgradable(0, k2, -1));
        assertEquals(2, mManager.numLocksHeld());
        locker.scopeExitAll();
        assertEquals(0, mManager.numLocksHeld());

        assertEquals(ACQUIRED, locker.doTryLockUpgradable(0, k1, -1));
        assertEquals(ACQUIRED, locker.doTryLockUpgradable(0, k2, -1));
        assertEquals(k2, locker.lastLockedKey());
        locker.doUnlock();
        assertEquals(k1, locker.lastLockedKey());
        locker.doUnlock();
        assertEquals(ACQUIRED, locker.doTryLockUpgradable(0, k1, -1));
        assertEquals(ACQUIRED, locker.doTryLockUpgradable(0, k2, -1));
        assertEquals(k2, locker.lastLockedKey());
        locker.doUnlock();

        var locker2 = new Locker(mManager);
        assertEquals(TIMED_OUT_LOCK, locker2.doTryLockUpgradable(0, k1, SHORT_TIMEOUT));
        assertEquals(ACQUIRED, locker2.doTryLockUpgradable(0, k2, -1));
        assertEquals(OWNED_UPGRADABLE, locker.doTryLockUpgradable(0, k1, -1));
        assertEquals(TIMED_OUT_LOCK, locker.doTryLockUpgradable(0, k2, SHORT_TIMEOUT));
        assertEquals(ACQUIRED, locker2.doTryLockShared(0, k1, -1));
        assertEquals(OWNED_SHARED, locker2.doTryLockShared(0, k1, -1));
        assertEquals(OWNED_UPGRADABLE, locker2.doTryLockShared(0, k2, -1));
        assertEquals(OWNED_UPGRADABLE, locker.doTryLockShared(0, k1, -1));
        assertEquals(ACQUIRED, locker.doTryLockShared(0, k2, -1));
        assertEquals(OWNED_SHARED, locker.doTryLockShared(0, k2, -1));

        try {
            locker.doUnlockToUpgradable();
            fail();
        } catch (IllegalStateException e) {
        }
        assertEquals(k2, locker.lastLockedKey());
        locker.doUnlockToShared();
        try {
            locker2.doUnlockToUpgradable();
            fail();
        } catch (IllegalStateException e) {
        }
        assertEquals(k1, locker2.lastLockedKey());
        locker2.doUnlockToShared();
        assertEquals(k1, locker2.lastLockedKey());
        locker2.doUnlockToShared();

        assertEquals(ILLEGAL, locker2.doTryLockUpgradable(0, k1, -1));
        assertEquals(OWNED_UPGRADABLE, locker2.doTryLockUpgradable(0, k2, -1));
        assertEquals(OWNED_UPGRADABLE, locker.doTryLockUpgradable(0, k1, -1));
        assertEquals(ILLEGAL, locker.doTryLockUpgradable(0, k2, -1));

        locker.scopeExitAll();
        locker2.scopeExitAll();

        try {
            locker.doUnlock();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(ACQUIRED, locker.doTryLockUpgradable(0, k1, -1));

        assertEquals(k1, locker.lastLockedKey());
        locker.doUnlockToUpgradable();
        assertEquals(OWNED_UPGRADABLE, locker.doTryLockShared(0, k1, -1));
        assertEquals(k1, locker.lastLockedKey());
        locker.doUnlockToShared();
        
        assertEquals(OWNED_SHARED, locker.doTryLockShared(0, k1, -1));
        assertEquals(ILLEGAL, locker.doTryLockUpgradable(0, k1, -1));
        assertEquals(ILLEGAL, locker.doTryLockExclusive(0, k1, -1));

        locker.scopeExitAll();
        locker2.scopeExitAll();
    }

    @Test
    public void basicExclusive() throws Exception {
        var locker = new Locker(mManager);
        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k1, -1));
        assertEquals(1, mManager.numLocksHeld());
        assertEquals(OWNED_EXCLUSIVE, locker.doTryLockExclusive(0, k1, -1));
        assertEquals(1, mManager.numLocksHeld());
        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k2, -1));
        assertEquals(OWNED_EXCLUSIVE, locker.doTryLockExclusive(0, k1, -1));
        assertEquals(OWNED_EXCLUSIVE, locker.doTryLockExclusive(0, k2, -1));
        assertEquals(2, mManager.numLocksHeld());
        locker.scopeExitAll();
        assertEquals(0, mManager.numLocksHeld());

        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k1, -1));
        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k2, -1));
        assertEquals(k2, locker.lastLockedKey());
        locker.doUnlock();
        assertEquals(k1, locker.lastLockedKey());
        locker.doUnlock();
        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k1, -1));
        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k2, -1));
        assertEquals(k2, locker.lastLockedKey());
        locker.doUnlock();

        var locker2 = new Locker(mManager);
        assertEquals(TIMED_OUT_LOCK, locker2.doTryLockShared(0, k1, SHORT_TIMEOUT));
        assertEquals(TIMED_OUT_LOCK, locker2.doTryLockUpgradable(0, k1, SHORT_TIMEOUT));
        assertEquals(TIMED_OUT_LOCK, locker2.doTryLockExclusive(0, k1, SHORT_TIMEOUT));
        assertEquals(ACQUIRED, locker2.doTryLockExclusive(0, k2, -1));
        assertEquals(OWNED_EXCLUSIVE, locker.doTryLockExclusive(0, k1, -1));
        assertEquals(TIMED_OUT_LOCK, locker.doTryLockShared(0, k2, SHORT_TIMEOUT));
        assertEquals(TIMED_OUT_LOCK, locker.doTryLockUpgradable(0, k2, SHORT_TIMEOUT));
        assertEquals(TIMED_OUT_LOCK, locker.doTryLockExclusive(0, k2, SHORT_TIMEOUT));

        assertEquals(k1, locker.lastLockedKey());
        locker.doUnlockToUpgradable();
        assertEquals(OWNED_UPGRADABLE, locker.doTryLockShared(0, k1, -1));
        assertEquals(OWNED_UPGRADABLE, locker.doTryLockUpgradable(0, k1, -1));
        assertEquals(k1, locker.lastLockedKey());
        locker.doUnlockToShared();
        assertEquals(k2, locker2.lastLockedKey());
        locker2.doUnlockToUpgradable();
        assertEquals(k2, locker2.lastLockedKey());
        locker2.doUnlockToShared();
        assertEquals(OWNED_SHARED, locker2.doTryLockShared(0, k2, -1));

        assertEquals(ILLEGAL, locker.doTryLockExclusive(0, k1, -1));
        assertEquals(ILLEGAL, locker2.doTryLockExclusive(0, k2, -1));

        locker.scopeExitAll();
        locker2.scopeExitAll();
        locker2.scopeExitAll();

        try {
            locker.doUnlock();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k1, -1));
        assertEquals(OWNED_EXCLUSIVE, locker.doTryLockShared(0, k1, 0));

        assertEquals(k1, locker.lastLockedKey());
        locker.doUnlockToUpgradable();
        assertEquals(k1, locker.lastLockedKey());
        locker.doUnlockToUpgradable();
        assertEquals(OWNED_UPGRADABLE, locker.doTryLockShared(0, k1, -1));
        assertEquals(k1, locker.lastLockedKey());
        locker.doUnlockToShared();
        assertEquals(k1, locker.lastLockedKey());
        locker.doUnlockToShared();
        
        assertEquals(OWNED_SHARED, locker.doTryLockShared(0, k1, -1));
        assertEquals(ILLEGAL, locker.doTryLockExclusive(0, k1, -1));

        locker.scopeExitAll();
        locker2.scopeExitAll();
    }

    @Test
    public void lenientUpgradeRule() throws Exception {
        LockManager manager = new LockManager(null, LockUpgradeRule.LENIENT, -1);

        var locker1 = new Locker(manager);
        var locker2 = new Locker(manager);

        assertEquals(ACQUIRED, locker1.doTryLockShared(0, k1, -1));
        assertEquals(OWNED_UPGRADABLE, locker1.doTryLockUpgradable(0, k1, -1));
        locker1.scopeExitAll();

        assertEquals(ACQUIRED, locker1.doTryLockShared(0, k1, -1));
        assertEquals(ACQUIRED, locker2.doTryLockShared(0, k1, -1));

        assertEquals(ILLEGAL, locker1.doTryLockUpgradable(0, k1, -1));
        assertEquals(ILLEGAL, locker2.doTryLockExclusive(0, k1, -1));

        locker1.scopeExitAll();

        assertEquals(UPGRADED, locker2.doTryLockExclusive(0, k1, -1));
        locker2.doUnlockToUpgradable();
        assertEquals(UPGRADED, locker2.doTryLockExclusive(0, k1, -1));
        locker2.doUnlockToShared();
        assertEquals(OWNED_UPGRADABLE, locker2.doTryLockUpgradable(0, k1, -1));
        locker2.doUnlock();

        assertEquals(ACQUIRED, locker1.doTryLockExclusive(0, k1, -1));
        locker1.doUnlock();
    }

    @Test
    public void uncheckedUpgradeRule() throws Exception {
        var manager = new LockManager(null, LockUpgradeRule.UNCHECKED, -1);

        var locker1 = new Locker(manager);
        var locker2 = new Locker(manager);

        assertEquals(ACQUIRED, locker1.doTryLockShared(0, k1, -1));
        assertEquals(OWNED_UPGRADABLE, locker1.doTryLockUpgradable(0, k1, -1));
        locker1.scopeExitAll();

        assertEquals(ACQUIRED, locker1.doTryLockShared(0, k1, -1));
        assertEquals(ACQUIRED, locker2.doTryLockShared(0, k1, -1));

        assertEquals(OWNED_UPGRADABLE, locker1.doTryLockUpgradable(0, k1, -1));
        assertEquals(TIMED_OUT_LOCK, locker2.doTryLockExclusive(0, k1, 0));

        try {
            locker1.doTryLockExclusive(0, k1, 10);
            fail();
        } catch (DeadlockException e) {
        }

        locker1.doUnlockToShared();

        assertEquals(OWNED_UPGRADABLE, locker2.doTryLockUpgradable(0, k1, -1));
        assertEquals(TIMED_OUT_LOCK, locker2.doTryLockExclusive(0, k1, 0));
        try {
            locker2.doTryLockExclusive(0, k1, 1);
            fail();
        } catch (DeadlockException e) {
        }

        locker1.doUnlock();

        assertEquals(UPGRADED, locker2.doTryLockExclusive(0, k1, -1));
        locker2.doUnlockToUpgradable();
        assertEquals(UPGRADED, locker2.doTryLockExclusive(0, k1, -1));
        locker2.doUnlockToShared();
        assertEquals(OWNED_UPGRADABLE, locker2.doTryLockUpgradable(0, k1, -1));
        locker2.doUnlock();

        assertEquals(ACQUIRED, locker1.doTryLockExclusive(0, k1, -1));
        locker1.doUnlock();
    }

    @Test
    public void isolatedIndexes() throws Exception {
        var locker = new Locker(mManager);
        assertEquals(ACQUIRED, locker.doTryLockExclusive(1, k1, -1));
        assertEquals(ACQUIRED, locker.doTryLockExclusive(2, k1, -1));
        assertEquals(2, mManager.numLocksHeld());
        assertEquals(OWNED_EXCLUSIVE, locker.doTryLockExclusive(1, k1, -1));
        assertEquals(OWNED_EXCLUSIVE, locker.doTryLockExclusive(2, k1, -1));
        assertEquals(2, mManager.numLocksHeld());
        assertEquals(2, locker.lastLockedIndex());
        assertEquals(k1, locker.lastLockedKey());
        assertTrue(locker.wasAcquired(2, k1));
        locker.scopeExitAll();
        assertEquals(0, mManager.numLocksHeld());
    }

    @Test
    public void upgrade() throws Exception {
        var locker = new Locker(mManager);
        var locker2 = new Locker(mManager);

        assertEquals(ACQUIRED, locker.doTryLockShared(0, k1, -1));
        assertEquals(ACQUIRED, locker2.doTryLockUpgradable(0, k1, -1));
        assertEquals(TIMED_OUT_LOCK, locker2.doTryLockExclusive(0, k1, SHORT_TIMEOUT));
        scheduleUnlock(locker, 1000);
        assertEquals(UPGRADED, locker2.doTryLockExclusive(0, k1, MEDIUM_TIMEOUT));
        assertEquals(OWNED_EXCLUSIVE, locker2.doTryLockExclusive(0, k1, -1));
        mScheduledUnlock.join();
        assertEquals(TIMED_OUT_LOCK, locker.doTryLockShared(0, k1, SHORT_TIMEOUT));
        assertEquals(TIMED_OUT_LOCK, locker.doTryLockUpgradable(0, k1, SHORT_TIMEOUT));
        scheduleUnlockToUpgradable(locker2, 1000);
        assertEquals(ACQUIRED, locker.doTryLockShared(0, k1, MEDIUM_TIMEOUT));
        mScheduledUnlock.join();
        assertEquals(OWNED_UPGRADABLE, locker2.doTryLockUpgradable(0, k1, -1));

        locker.scopeExitAll();
        locker2.scopeExitAll();
    }

    @Test
    public void downgrade() throws Exception {
        var locker = new Locker(mManager);

        assertEquals(ACQUIRED, locker.doTryLockUpgradable(0, k1, -1));
        assertEquals(ACQUIRED, locker.doTryLockUpgradable(0, k2, -1));
        assertEquals(UPGRADED, locker.doTryLockExclusive(0, k1, -1));
        try {
            locker.doUnlock();
            fail();
        } catch (IllegalStateException e) {
            // Non-immediate upgrade.
        }
        try {
            locker.doUnlockToShared();
            fail();
        } catch (IllegalStateException e) {
            // Non-immediate upgrade.
        }
        assertArrayEquals(k1, locker.lastLockedKey());
        locker.doUnlockToUpgradable();
        assertArrayEquals(k2, locker.lastLockedKey());
        locker.doUnlock();
        assertArrayEquals(k1, locker.lastLockedKey());
        locker.doUnlock();

        // Do again, but with another upgrade in between.

        assertEquals(ACQUIRED, locker.doTryLockUpgradable(0, k1, -1));
        assertEquals(ACQUIRED, locker.doTryLockUpgradable(0, k2, -1));
        assertEquals(UPGRADED, locker.doTryLockExclusive(0, k2, -1));
        assertEquals(UPGRADED, locker.doTryLockExclusive(0, k1, -1));
        try {
            locker.doUnlock();
            fail();
        } catch (IllegalStateException e) {
            // Non-immediate upgrade.
        }
        try {
            locker.doUnlockToShared();
            fail();
        } catch (IllegalStateException e) {
            // Non-immediate upgrade.
        }
        assertArrayEquals(k1, locker.lastLockedKey());
        locker.doUnlockToUpgradable();
        assertArrayEquals(k2, locker.lastLockedKey());
        locker.doUnlock();
        assertArrayEquals(k1, locker.lastLockedKey());
        locker.doUnlock();

        try {
            locker.doUnlockToShared();
            fail();
        } catch (IllegalStateException e) {
            // No locks held.
        }
        try {
            locker.doUnlockToUpgradable();
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
        var locker1 = new Locker(mManager);
        var locker2 = new Locker(mManager);
        var locker3 = new Locker(mManager);

        assertEquals(LockResult.ACQUIRED, locker1.doLockExclusive(0, k1, -1));

        class Upgradable extends Thread {
            volatile Throwable failed;
            volatile LockResult result;

            @Override
            public void run() {
                try {
                    result = locker2.doLockUpgradable(0, k1, -1);
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
                    result = locker3.doLockShared(0, k1, -1);
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
            locker1.doUnlockToShared();
        } else {
            locker1.doUnlock();
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
        var locker = new Locker(mManager);
        for (int i=0; i<1000; i++) {
            assertEquals(ACQUIRED, locker.doTryLockExclusive(0, key("k" + i), -1));
        }
        assertEquals(1000, mManager.numLocksHeld());
        locker.scopeExitAll();
        for (int i=0; i<1000; i++) {
            assertEquals(ACQUIRED, locker.doTryLockExclusive(0, key("k" + i), -1));
        }
        for (int i=0; i<1000; i++) {
            assertEquals(OWNED_EXCLUSIVE, locker.doTryLockExclusive(0, key("k" + i), -1));
        }
        for (int i=1000; --i>=0; ) {
            assertArrayEquals(key("k" + i), locker.lastLockedKey());
            locker.doUnlock();
        }
        for (int i=0; i<1000; i++) {
            assertEquals(ACQUIRED, locker.doTryLockExclusive(0, key("k" + i), -1));
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
        var locker = new Locker(mManager);
        var locker2 = new Locker(mManager);

        locker.doTryLockShared(0, k1, -1);

        assertEquals(TIMED_OUT_LOCK, locker2.doTryLockExclusive(0, k1, nanosTimeout));

        locker.doUnlock();

        assertEquals(ACQUIRED, locker2.doTryLockExclusive(0, k1, -1));
        locker2.doUnlock();

        locker.doTryLockUpgradable(0, k1, -1);

        assertEquals(TIMED_OUT_LOCK, locker2.doTryLockUpgradable(0, k1, nanosTimeout));
        assertEquals(TIMED_OUT_LOCK, locker2.doTryLockExclusive(0, k1, nanosTimeout));

        locker.doUnlock();

        assertEquals(ACQUIRED, locker2.doTryLockUpgradable(0, k1, -1));
        locker2.doUnlock();

        locker.doTryLockExclusive(0, k1, -1);

        assertEquals(TIMED_OUT_LOCK, locker2.doTryLockShared(0, k1, nanosTimeout));
        assertEquals(TIMED_OUT_LOCK, locker2.doTryLockUpgradable(0, k1, nanosTimeout));
        assertEquals(TIMED_OUT_LOCK, locker2.doTryLockExclusive(0, k1, nanosTimeout));

        locker.doUnlock();

        assertEquals(ACQUIRED, locker2.doTryLockShared(0, k1, -1));
        locker2.doUnlock();

        locker.scopeExitAll();
        locker2.scopeExitAll();
    }

    @Test
    public void blockedNoWait2() throws Exception {
        var locker = new Locker(mManager);

        assertEquals(ACQUIRED, locker.doTryLockShared(0, k1, -1));

        Thread t = startAndWaitUntilBlocked(new Thread(() -> {
            try {
                var locker2 = new Locker(mManager);
                locker2.doTryLockExclusive(0, k1, -1);
            } catch (Exception e) {
                Utils.uncaught(e);
            }
        }));

        var locker3 = new Locker(mManager);

        assertEquals(TIMED_OUT_LOCK, locker3.doTryLockShared(0, k1, 0));
        assertEquals(TIMED_OUT_LOCK, locker3.doTryLockUpgradable(0, k1, 0));
        assertEquals(TIMED_OUT_LOCK, locker3.doTryLockExclusive(0, k1, 0));
    }

    @Test
    public void blockedNoWait3() throws Exception {
        var locker = new Locker(mManager);

        assertEquals(ACQUIRED, locker.doTryLockUpgradable(0, k1, -1));

        Thread t = startAndWaitUntilBlocked(new Thread(() -> {
            try {
                var locker2 = new Locker(mManager);
                locker2.doTryLockUpgradable(0, k1, -1);
            } catch (Exception e) {
                Utils.uncaught(e);
            }
        }));

        var locker3 = new Locker(mManager);

        assertEquals(TIMED_OUT_LOCK, locker3.doTryLockUpgradable(0, k1, 0));
        assertEquals(TIMED_OUT_LOCK, locker3.doTryLockExclusive(0, k1, 0));
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
        var locker = new Locker(mManager);
        var locker2 = new Locker(mManager);

        locker.doTryLockShared(0, k1, -1);

        selfInterrupt(1000);
        assertEquals(INTERRUPTED, locker2.doTryLockExclusive(0, k1, nanosTimeout));
        assertFalse(Thread.interrupted());

        locker.doUnlock();

        assertEquals(ACQUIRED, locker2.doTryLockExclusive(0, k1, -1));
        locker2.doUnlock();

        locker.doTryLockUpgradable(0, k1, -1);

        selfInterrupt(1000);
        assertEquals(INTERRUPTED, locker2.doTryLockUpgradable(0, k1, nanosTimeout));
        assertFalse(Thread.interrupted());
        selfInterrupt(1000);
        assertEquals(INTERRUPTED, locker2.doTryLockExclusive(0, k1, nanosTimeout));
        assertFalse(Thread.interrupted());

        locker.doUnlock();

        assertEquals(ACQUIRED, locker2.doTryLockUpgradable(0, k1, -1));
        locker2.doUnlock();

        locker.doTryLockExclusive(0, k1, -1);

        selfInterrupt(1000);
        assertEquals(INTERRUPTED, locker2.doTryLockShared(0, k1, nanosTimeout));
        assertFalse(Thread.interrupted());
        selfInterrupt(1000);
        assertEquals(INTERRUPTED, locker2.doTryLockUpgradable(0, k1, nanosTimeout));
        assertFalse(Thread.interrupted());
        selfInterrupt(1000);
        assertEquals(INTERRUPTED, locker2.doTryLockExclusive(0, k1, nanosTimeout));
        assertFalse(Thread.interrupted());

        locker.doUnlock();

        assertEquals(ACQUIRED, locker2.doTryLockShared(0, k1, -1));
        locker2.doUnlock();

        locker.scopeExitAll();
        locker2.scopeExitAll();
    }

    @Test
    public void delayedAcquire() throws Exception {
        var locker = new Locker(mManager);
        var locker2 = new Locker(mManager);
        long end;

        // Exclusive locks blocked...

        // Exclusive lock blocked by shared lock.
        assertEquals(ACQUIRED, locker.doTryLockShared(0, k1, -1));
        end = scheduleUnlock(locker, 1000);
        assertEquals(ACQUIRED, locker2.doTryLockExclusive(0, k1, MEDIUM_TIMEOUT));
        mScheduledUnlock.join();
        assertEquals(TIMED_OUT_LOCK, locker.doTryLockShared(0, k1, 0));
        locker2.doUnlock();
        assertTrue(System.nanoTime() >= end);

        // Exclusive lock blocked by upgradable lock.
        assertEquals(ACQUIRED, locker.doTryLockUpgradable(0, k1, -1));
        end = scheduleUnlock(locker, 1000);
        assertEquals(ACQUIRED, locker2.doTryLockExclusive(0, k1, MEDIUM_TIMEOUT));
        mScheduledUnlock.join();
        assertEquals(TIMED_OUT_LOCK, locker.doTryLockUpgradable(0, k1, 0));
        locker2.doUnlock();
        assertTrue(System.nanoTime() >= end);

        // Exclusive lock blocked by exclusive lock.
        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k1, -1));
        end = scheduleUnlock(locker, 1000);
        assertEquals(ACQUIRED, locker2.doTryLockExclusive(0, k1, MEDIUM_TIMEOUT));
        mScheduledUnlock.join();
        assertEquals(TIMED_OUT_LOCK, locker.doTryLockUpgradable(0, k1, 0));
        locker2.doUnlock();
        assertTrue(System.nanoTime() >= end);

        // Upgradable locks blocked...

        // Upgradable lock blocked by upgradable lock.
        assertEquals(ACQUIRED, locker.doTryLockUpgradable(0, k1, -1));
        end = scheduleUnlock(locker, 1000);
        assertEquals(ACQUIRED, locker2.doTryLockUpgradable(0, k1, MEDIUM_TIMEOUT));
        mScheduledUnlock.join();
        assertEquals(TIMED_OUT_LOCK, locker.doTryLockUpgradable(0, k1, 0));
        locker2.doUnlock();
        assertTrue(System.nanoTime() >= end);

        // Upgradable lock blocked by upgradable lock, granted via downgrade to shared.
        assertEquals(ACQUIRED, locker.doTryLockUpgradable(0, k1, -1));
        end = scheduleUnlockToShared(locker, 1000);
        assertEquals(ACQUIRED, locker2.doTryLockUpgradable(0, k1, MEDIUM_TIMEOUT));
        mScheduledUnlock.join();
        assertEquals(OWNED_SHARED, locker.doTryLockShared(0, k1, 0));
        locker2.doUnlock();
        locker.doUnlock();
        assertTrue(System.nanoTime() >= end);

        // Upgradable lock blocked by exclusive lock.
        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k1, -1));
        end = scheduleUnlock(locker, 1000);
        assertEquals(ACQUIRED, locker2.doTryLockUpgradable(0, k1, MEDIUM_TIMEOUT));
        mScheduledUnlock.join();
        assertEquals(TIMED_OUT_LOCK, locker.doTryLockUpgradable(0, k1, 0));
        locker2.doUnlock();
        assertTrue(System.nanoTime() >= end);

        // Upgradable lock blocked by exclusive lock, granted via downgrade to shared.
        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k1, -1));
        end = scheduleUnlockToShared(locker, 1000);
        assertEquals(ACQUIRED, locker2.doTryLockUpgradable(0, k1, MEDIUM_TIMEOUT));
        mScheduledUnlock.join();
        assertEquals(OWNED_SHARED, locker.doTryLockShared(0, k1, 0));
        locker2.doUnlock();
        locker.doUnlock();
        assertTrue(System.nanoTime() >= end);

        // Shared locks blocked...

        // Shared lock blocked by exclusive lock.
        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k1, -1));
        end = scheduleUnlock(locker, 1000);
        assertEquals(ACQUIRED, locker2.doTryLockShared(0, k1, MEDIUM_TIMEOUT));
        mScheduledUnlock.join();
        assertEquals(ACQUIRED, locker.doTryLockShared(0, k1, 0));
        locker.doUnlock();
        locker2.doUnlock();
        assertTrue(System.nanoTime() >= end);

        // Shared lock blocked by exclusive lock, granted via downgrade to shared.
        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k1, -1));
        end = scheduleUnlockToShared(locker, 1000);
        assertEquals(ACQUIRED, locker2.doTryLockShared(0, k1, MEDIUM_TIMEOUT));
        mScheduledUnlock.join();
        assertEquals(OWNED_SHARED, locker.doTryLockShared(0, k1, 0));
        locker.doUnlock();
        locker2.doUnlock();
        assertTrue(System.nanoTime() >= end);

        // Shared lock blocked by exclusive lock, granted via downgrade to upgradable.
        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k1, -1));
        end = scheduleUnlockToUpgradable(locker, 1000);
        assertEquals(ACQUIRED, locker2.doTryLockShared(0, k1, MEDIUM_TIMEOUT));
        mScheduledUnlock.join();
        assertEquals(OWNED_UPGRADABLE, locker.doTryLockShared(0, k1, 0));
        locker.doUnlock();
        locker2.doUnlock();
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
                if (i == 10 || !e.getMessage().contains("TIMED")) {
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

        var lockers = new Locker[count];
        for (int i=0; i<count; i++) {
            lockers[i] = new Locker(mManager);
        }
        Future<LockResult>[] futures = new Future[count];

        // Upgradable locks acquired in fifo order.
        synchronized (lockers[0]) {
            lockers[0].doTryLockUpgradable(0, k1, -1);
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
                last.doUnlock();
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
            lockers[0].doTryLockShared(0, k1, -1);
        }
        synchronized (lockers[1]) {
            assertEquals(TIMED_OUT_LOCK, lockers[1].doTryLockExclusive(0, k1, SHORT_TIMEOUT));
        }
        futures[1] = tryLockExclusive(lockers[1], 0, k1);
        sleep(100);
        synchronized (lockers[2]) {
            assertEquals(TIMED_OUT_LOCK, lockers[2].doTryLockShared(0, k1, SHORT_TIMEOUT));
        }
        for (int i=2; i<lockers.length; i++) {
            sleep(100);
            futures[i] = tryLockShared(lockers[i], 0, k1);
        }
        // Now release first shared lock.
        synchronized (lockers[0]) {
            lockers[0].doUnlock();
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
            lockers[1].doUnlock();
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

        var locker = new Locker(mManager);

        assertNull(locker.scopeExit());

        locker.scopeEnter();
        assertNotNull(locker.scopeExit());

        locker.scopeEnter();
        locker.scopeEnter();
        assertNotNull(locker.scopeExit());
        assertNotNull(locker.scopeExit());
        assertNull(locker.scopeExit());

        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        assertEquals(OWNED_EXCLUSIVE, locker.doTryLockExclusive(0, k1, -1));
        assertNotNull(locker.scopeExit());
        assertEquals(OWNED_EXCLUSIVE, locker.doTryLockExclusive(0, k1, -1));
        assertNull(locker.scopeExit());
        assertEquals(UNOWNED, locker.lockCheck(0, k1));

        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k2, -1));
        assertNotNull(locker.scopeExit());
        assertEquals(UNOWNED, locker.lockCheck(0, k2));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
        locker.scopeExitAll();

        // Upgrade lock within scope, and then exit scope.
        assertEquals(ACQUIRED, locker.doTryLockUpgradable(0, k1, -1));
        locker.scopeEnter();
        assertEquals(UPGRADED, locker.doTryLockExclusive(0, k1, -1));
        assertNotNull(locker.scopeExit());
        // Outer scope was downgraded to original lock strength.
        assertEquals(OWNED_UPGRADABLE, locker.lockCheck(0, k1));
        locker.scopeExitAll();

        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k2, -1));
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k3, -1));
        // ScopeExitAll and unlock all scopes.
        locker.scopeExitAll();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));
        assertEquals(UNOWNED, locker.lockCheck(0, k2));
        assertEquals(UNOWNED, locker.lockCheck(0, k3));
        assertNull(locker.scopeExit());

        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k2, -1));
        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k3, -1));
        // ScopeExitAll and unlock all scopes.
        locker.scopeExitAll();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));
        assertEquals(UNOWNED, locker.lockCheck(0, k2));
        assertEquals(UNOWNED, locker.lockCheck(0, k3));
        assertNull(locker.scopeExit());

        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k2, -1));
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k3, -1));
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
        
        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k2, -1));
        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k3, -1));
        locker.promote();
        assertNotNull(locker.scopeExit());
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k3));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k2));
        assertNull(locker.scopeExit());
        assertEquals(UNOWNED, locker.lockCheck(0, k1));
        assertEquals(UNOWNED, locker.lockCheck(0, k2));
        assertEquals(UNOWNED, locker.lockCheck(0, k3));

        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k1, -1));
        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k2, -1));
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k3, -1));
        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k4, -1));
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

        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        // Fill up first block of locks.
        for (int i=0; i<8; i++) {
            assertEquals(ACQUIRED, locker.doTryLockExclusive(0, key("k" + i), -1));
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
            assertEquals(ACQUIRED, locker.doTryLockExclusive(0, key("k" + i), -1));
        }
        locker.scopeEnter();
        // Fill up another first block of locks.
        for (int i=0; i<8; i++) {
            assertEquals(ACQUIRED, locker.doTryLockExclusive(0, key("a" + i), -1));
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

        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k2, -1));
        locker.scopeEnter();
        // Fill up first block of locks.
        for (int i=0; i<8; i++) {
            assertEquals(ACQUIRED, locker.doTryLockExclusive(0, key("k" + i), -1));
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
            assertEquals(ACQUIRED, locker.doTryLockExclusive(0, key("k" + i), -1));
        }
        for (int i=10; --i>=0; ) {
            locker.scopeExit();
            assertEquals(UNOWNED, locker.lockCheck(0, key("k" + i)));
        }
        locker.scopeExitAll();

        // Deep scoping with big scopes.
        for (int i=0; i<10; i++) {
            assertEquals(ACQUIRED, locker.doTryLockExclusive(0, key("k" + i), -1));
            locker.scopeEnter();
            for (int j=0; j<1000; j++) {
                assertEquals(ACQUIRED, locker.doTryLockExclusive(0, key("k" + i + ", " + j), -1));
            }
        }
        for (int i=10; --i>=0; ) {
            locker.scopeExit();
            assertEquals(UNOWNED, locker.lockCheck(0, key("k" + (i + 1))));
            for (int j=0; j<1000; j++) {
                assertEquals(UNOWNED, locker.lockCheck(0, key("k" + i + ", " + j)));
            }
        }
        locker.scopeExitAll();
        assertEquals(UNOWNED, locker.lockCheck(0, key("k" + 0)));

        for (int q=0; q<3; q++) {
            for (int w=0; w<2; w++) {
                if (w != 0) {
                    for (int i=0; i<100; i++) {
                        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, key("v" + i), -1));
                    }
                }

                assertEquals(ACQUIRED, locker.doTryLockUpgradable(0, k1, -1));
                locker.scopeEnter();
                // Fill up first block of locks.
                for (int i=0; i<8; i++) {
                    assertEquals(ACQUIRED, locker.doTryLockExclusive(0, key("k" + i), -1));
                }
                locker.promote();
                assertNotNull(locker.scopeExit());
                for (int i=0; i<8; i++) {
                    locker.doUnlock();
                }
                assertEquals(UPGRADED, locker.doTryLockExclusive(0, k1, -1));
                if (q == 0) {
                    locker.doUnlockToShared();
                    assertEquals(OWNED_SHARED, locker.lockCheck(0, k1));
                } else if (q == 1) {
                    locker.doUnlockToUpgradable();
                    assertEquals(OWNED_UPGRADABLE, locker.lockCheck(0, k1));
                } else {
                    locker.doUnlock();
                    assertEquals(UNOWNED, locker.lockCheck(0, k1));
                }
                assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k2, -1));
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
            assertEquals(ACQUIRED, locker.doTryLockExclusive(0, key("q" + q), -1));
            locker.scopeEnter();
            for (int i=0; i<8; i++) {
                assertEquals(ACQUIRED, locker.doTryLockExclusive(0, key("k" + q + "_" + i), -1));
            }
        }
        for (int q=0; q<8; q++) {
            locker.promote();
            locker.scopeExit();
        }
        for (int q=4; --q>=0; ) {
            for (int i=8; --i>=0; ) {
                assertArrayEquals(key("k" + q + "_" + i), locker.lastLockedKey());
                locker.doUnlock();
            }
            assertArrayEquals(key("q" + q), locker.lastLockedKey());
            locker.doUnlock();
        }

        for (int q=0; q<2; q++) {
            assertEquals(ACQUIRED, locker.doTryLockUpgradable(0, k1, -1));
            locker.scopeEnter();
            if (q != 0) {
                for (int i=0; i<(8 + 16); i++) {
                    assertEquals(ACQUIRED, locker.doTryLockExclusive(0, key("v" + i), -1));
                }
            }
            assertEquals(UPGRADED, locker.doTryLockExclusive(0, k1, -1));
            assertEquals(ACQUIRED, locker.doTryLockShared(0, k2, -1));
            locker.doUnlock();
            assertEquals(UNOWNED, locker.lockCheck(0, k2));
            assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
            try {
                locker.doUnlockToUpgradable();
                fail();
            } catch (IllegalStateException e) {
            }
            assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
            locker.scopeExitAll();
            assertEquals(UNOWNED, locker.lockCheck(0, k1));
        }

        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        for (int i=0; i<8; i++) {
            assertEquals(ACQUIRED, locker.doTryLockExclusive(0, key("k" + i), -1));
        }
        locker.promote();
        assertNotNull(locker.scopeExit());
        for (int i=8; --i>=0; ) {
            locker.doUnlock();
            assertEquals(UNOWNED, locker.lockCheck(0, key("k" + i)));
        }
        locker.scopeEnter();
        for (int i=0; i<4; i++) {
            assertEquals(ACQUIRED, locker.doTryLockExclusive(0, key("v" + i), -1));
        }
        locker.promote();
        assertNotNull(locker.scopeExit());
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
        for (int i=0; i<4; i++) {
            assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, key("v" + i)));
        }
        for (int i=4; --i>=0; ) {
            locker.doUnlock();
            assertEquals(UNOWNED, locker.lockCheck(0, key("v" + i)));
        }
        locker.doUnlock();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));
        for (int i=0; i<8; i++) {
            assertEquals(UNOWNED, locker.lockCheck(0, key("k" + i)));
        }
        assertNotNull(locker.scopeExit());
        assertNull(locker.scopeExit());

        for (int i=0; i<9; i++) {
            assertEquals(ACQUIRED, locker.doTryLockExclusive(0, key("k" + i), -1));
        }
        locker.scopeEnter();
        for (int i=0; i<4; i++) {
            assertEquals(ACQUIRED, locker.doTryLockUpgradable(0, key("v" + i), -1));
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
        var locker = new Locker(mManager);

        locker.scopeEnter();
        locker.doLockExclusive(0, k1, -1);
        locker.doLockExclusive(0, k2, -1);
        locker.scopeUnlockAll();

        try {
            locker.lastLockedKey();
        } catch (IllegalStateException e) {
            // Good.
        }

        try {
            locker.doUnlock();
        } catch (IllegalStateException e) {
            // Good.
        }
    }

    @Test
    public void promote() throws Exception {
        var locker = new Locker(mManager);

        locker.scopeEnter();
        locker.doTryLockExclusive(0, k1, -1);
        locker.promote();
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
        locker.scopeExit();
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
        locker.doUnlock();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));

        locker.scopeEnter();
        locker.doTryLockShared(0, k2, -1);
        locker.promote();
        assertEquals(OWNED_SHARED, locker.lockCheck(0, k2));
        locker.scopeExit();
        assertEquals(OWNED_SHARED, locker.lockCheck(0, k2));
        locker.scopeExit();
        assertEquals(UNOWNED, locker.lockCheck(0, k2));

        locker.doTryLockExclusive(0, k3, -1);
        locker.doTryLockExclusive(0, k4, -1);
        locker.scopeEnter();
        locker.doTryLockExclusive(0, key("e"), -1);
        locker.doTryLockExclusive(0, key("f"), -1);
        locker.promote();
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, key("e")));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, key("f")));
        assertEquals(UNOWNED, locker.lockCheck(0, key("g")));
        locker.scopeExit();
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k3));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k4));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, key("e")));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, key("f")));
        locker.doUnlock();
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k3));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k4));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, key("e")));
        assertEquals(UNOWNED, locker.lockCheck(0, key("f")));
    }

    @Test
    public void promoteExitAll() throws Exception {
        var locker = new Locker(mManager);
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k1, -1));
        locker.promote();
        locker.scopeExitAll();

        var locker2 = new Locker(mManager);
        assertEquals(ACQUIRED, locker2.doTryLockExclusive(0, k1, SHORT_TIMEOUT));
    }

    @Test
    public void blockDiscard() throws Exception {
        // Test for a bug which caused a parent scope to reference a Block, but the current
        // (child) scope referenced a Lock. This is an illegal combination, resulting in a
        // class cast exception when rolling back.

        var locker = new Locker(mManager);

        locker.doLockExclusive(0, k1, -1);
        locker.doLockExclusive(0, k2, -1);

        // Must not leave an empty Block behind, which is illegal.
        locker.scopeUnlockAll();

        locker.scopeEnter();
        locker.doLockExclusive(0, k3, -1);

        // If current reference is a Block, it gets null'd as a side-effect.
        locker.doUnlock();

        // If reference was null'd, this creates a Lock reference.
        locker.doLockExclusive(0, k4, -1);

        // If parent references a Block and current reference is a Lock, this call fails.
        locker.scopeUnlockAll();
    }

    @Test
    public void promoteUnlockUpgraded() throws Exception {
        var locker = new Locker(mManager);

        // Enter, lock, promote...
        locker.scopeEnter();
        assertEquals(ACQUIRED, locker.doTryLockUpgradable(0, k1, -1));
        locker.promote();

        assertEquals(ACQUIRED, locker.doTryLockUpgradable(0, k2, -1));
        assertEquals(UPGRADED, locker.doTryLockExclusive(0, k2, -1));
        locker.doUnlock();

        assertEquals(OWNED_UPGRADABLE, locker.lockCheck(0, k1));
        assertEquals(UNOWNED, locker.lockCheck(0, k2));

        locker.scopeExitAll();

        // Same initial state but without promote.
        assertEquals(ACQUIRED, locker.doTryLockUpgradable(0, k1, -1));
        locker.scopeEnter();

        assertEquals(ACQUIRED, locker.doTryLockUpgradable(0, k2, -1));
        assertEquals(UPGRADED, locker.doTryLockExclusive(0, k2, -1));
        locker.doUnlock();

        assertEquals(OWNED_UPGRADABLE, locker.lockCheck(0, k1));
        assertEquals(UNOWNED, locker.lockCheck(0, k2));
    }

    @Test
    public void reverseScan() throws Exception {
        // Test which discovered a race condition in LockOwner.hashCode method.

        Database db = Database.open(new DatabaseConfig());
        View view = db.openIndex("index1");

        int numAttempts = 100;
        int batchSize = 20;

        int numConcurrentReads = 4;
        int numConcurrentWrites = 1;
        ExecutorService executor = Executors.newFixedThreadPool
            (numConcurrentReads + numConcurrentWrites);

        for (int attempt=0; attempt<numAttempts; attempt++) {
            final var keys = new ArrayList<byte[]>();
            final var vals = new ArrayList<byte[]>();

            // Prepare request
            for (int id=0; id<batchSize; id++) {
                int suffix = attempt*batchSize+id;

                byte[] key = ("key" + suffix).getBytes(StandardCharsets.UTF_8);
                byte[] val = ("value" + suffix).getBytes(StandardCharsets.UTF_8);

                keys.add(key);
                vals.add(val);
            }

            // Write
            var writeFutures = new Future[numConcurrentWrites];
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
            }

            final var reversedKeys = new ArrayList<byte[]>(keys);
            Collections.reverse(reversedKeys);
            final var reversedVals = new ArrayList<byte[]>(vals);
            Collections.reverse(reversedVals);

            for (Future f : writeFutures) {
                f.get();
            }

            // Read
            var readFutures = new Future[numConcurrentReads];
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

        db.close();
    }

    @Test
    public void doubleLockSharedWithExclusiveWaiter() throws Exception {
        // Shared lock request must always check for existing ownership before blocking.

        var locker1 = new Locker(mManager);

        assertEquals(ACQUIRED, locker1.doTryLockShared(0, k1, -1));

        var t = new Thread(() -> {
            try {
                var locker2 = new Locker(mManager);
                locker2.doTryLockExclusive(0, k1, -1);
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

        assertEquals(LockResult.OWNED_SHARED, locker1.doTryLockShared(0, k1, 10_000_000_000L));

        locker1.scopeExitAll();
        t.join();
    }

    @Test
    public void illegalUnlock() throws Exception {
        var locker = new Locker(mManager);

        locker.doLockExclusive(0, k1, -1);
        locker.scopeEnter();
        try {
            locker.doUnlock();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));

        locker.scopeExit();
        locker.doUnlock();

        assertEquals(UNOWNED, locker.lockCheck(0, k1));
    }

    @Test
    public void illegalUnlock2() throws Exception {
        var locker = new Locker(mManager);

        locker.doLockExclusive(0, k1, -1);
        locker.scopeEnter();
        locker.doLockExclusive(0, k2, -1);
        locker.doUnlock();

        try {
            locker.doUnlock();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
        assertEquals(UNOWNED, locker.lockCheck(0, k2));
    }

    @Test
    public void illegalUnlock3() throws Exception {
        var locker = new Locker(mManager);

        locker.doLockExclusive(0, k1, -1);
        locker.doLockExclusive(0, k2, -1);
        locker.scopeEnter();
        locker.doLockExclusive(0, k3, -1);
        locker.doUnlock();

        try {
            locker.doUnlock();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k2));
        assertEquals(UNOWNED, locker.lockCheck(0, k3));
    }

    @Test
    public void illegalUnlock4() throws Exception {
        var locker = new Locker(mManager);

        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));

        try {
            locker.doUnlockToShared();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
    }

    @Test
    public void illegalUnlock5() throws Exception {
        var locker = new Locker(mManager);

        locker.doLockExclusive(0, k1, -1);
        locker.scopeEnter();
        locker.doLockExclusive(0, k2, -1);
        locker.doUnlock();

        try {
            locker.doUnlockToShared();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
        assertEquals(UNOWNED, locker.lockCheck(0, k2));
    }

    @Test
    public void illegalUnlock6() throws Exception {
        var locker = new Locker(mManager);

        locker.doLockExclusive(0, k1, -1);
        locker.doLockExclusive(0, k2, -1);
        locker.scopeEnter();
        locker.doLockExclusive(0, k3, -1);
        locker.doUnlock();

        try {
            locker.doUnlockToShared();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k2));
        assertEquals(UNOWNED, locker.lockCheck(0, k3));
    }

    @Test
    public void illegalUnlock7() throws Exception {
        var locker = new Locker(mManager);

        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k1, -1));
        locker.scopeEnter();
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));

        try {
            locker.doUnlockToUpgradable();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
    }

    @Test
    public void illegalUnlock8() throws Exception {
        var locker = new Locker(mManager);

        locker.doLockExclusive(0, k1, -1);
        locker.scopeEnter();
        locker.doLockExclusive(0, k2, -1);
        locker.doUnlock();

        try {
            locker.doUnlockToUpgradable();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
        assertEquals(UNOWNED, locker.lockCheck(0, k2));
    }

    @Test
    public void illegalUnlock9() throws Exception {
        var locker = new Locker(mManager);

        locker.doLockExclusive(0, k1, -1);
        locker.doLockExclusive(0, k2, -1);
        locker.scopeEnter();
        locker.doLockExclusive(0, k3, -1);
        locker.doUnlock();

        try {
            locker.doUnlockToUpgradable();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k2));
        assertEquals(UNOWNED, locker.lockCheck(0, k3));
    }

    @Test
    public void illegalUnlock10() throws Exception {
        var locker = new Locker(mManager);

        assertEquals(ACQUIRED, locker.doTryLockUpgradable(0, k1, -1));
        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k2, -1));
        assertEquals(UPGRADED, locker.doTryLockExclusive(0, k1, -1));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
        locker.scopeEnter();
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));

        try {
            locker.doUnlockToUpgradable();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(OWNED_EXCLUSIVE, locker.doTryLockUpgradable(0, k1, -1));
        locker.scopeEnter();
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
        assertEquals(OWNED_EXCLUSIVE, locker.doTryLockExclusive(0, k1, -1));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));

        try {
            locker.doUnlockToUpgradable();
            fail();
        } catch (IllegalStateException e) {
        }

        try {
            locker.doUnlock();
            fail();
        } catch (IllegalStateException e) {
        }
    }

    @Test
    public void illegalUnlock11() throws Exception {
        var locker = new Locker(mManager);

        Lock lock = locker.doLockSharedNoPush(0, k1);
        locker.push(lock);
        locker.scopeExitAll();

        try {
            mManager.doUnlock(locker, lock);
            fail();
        } catch (IllegalStateException e) {
        }
    }

    @Test
    public void illegalUnlock12() throws Exception {
        var locker = new Locker(mManager);

        Lock lock = locker.doLockSharedNoPush(0, k1);
        locker.push(lock);
        locker.scopeExitAll();

        try {
            mManager.doUnlockToShared(locker, lock);
            fail();
        } catch (IllegalStateException e) {
        }
    }

    @Test
    public void illegalUnlock13() throws Exception {
        var locker = new Locker(mManager);

        Lock lock = locker.doLockSharedNoPush(0, k1);
        locker.push(lock);
        locker.scopeExitAll();

        try {
            mManager.doUnlockToUpgradable(locker, lock);
            fail();
        } catch (IllegalStateException e) {
        }
    }

    @Test
    public void illegalUnlock14() throws Exception {
        var locker1 = new Locker(mManager);
        var locker2 = new Locker(mManager);
        var locker3 = new Locker(mManager);

        assertEquals(ACQUIRED, locker1.doTryLockShared(0, k1, -1));
        assertEquals(ACQUIRED, locker2.doTryLockShared(0, k1, -1));

        Lock lock = locker3.doLockSharedNoPush(0, k1);
        locker3.push(lock);
        locker3.scopeExitAll();

        try {
            mManager.doUnlock(locker3, lock);
            fail();
        } catch (IllegalStateException e) {
        }
    }

    @Test
    public void illegalUnlock15() throws Exception {
        // Cannot unlock or downgrade exclusive lock via the public API.

        var locker = new Locker(mManager);
        assertEquals(ACQUIRED, locker.doTryLockExclusive(0, k1, -1));
        assertEquals(ACQUIRED, locker.doTryLockUpgradable(0, k2, -1));
        assertEquals(ACQUIRED, locker.doTryLockShared(0, k3, -1));

        locker.unlock();
        locker.unlock();

        try {
            locker.unlock();
            fail();
        } catch (IllegalStateException e) {
        }

        try {
            locker.unlockToShared();
            fail();
        } catch (IllegalStateException e) {
        }

        // Can't be sneaky either and combine locks.

        assertEquals(ACQUIRED, locker.doTryLockUpgradable(0, k2, -1));
        locker.unlockCombine();

        try {
            locker.unlock();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(ACQUIRED, locker.doTryLockUpgradable(0, k2, -1));
        locker.unlockCombine();

        try {
            locker.unlockToShared();
            fail();
        } catch (IllegalStateException e) {
        }
    }

    @Test
    public void doubleTimeout() throws Exception {
        // Verify that a timeout of a lock doesn't falsely signal the next waiter.

        var locker1 = new Locker(mManager);
        var locker2 = new Locker(mManager);
        var locker3 = new Locker(mManager);

        assertEquals(LockResult.ACQUIRED, locker1.doLockExclusive(0, k1, -1));

        class Waiter extends Thread {
            volatile Throwable failed;
            volatile LockResult result;

            @Override
            public void run() {
                try {
                    result = locker2.doTryLockShared(0, k1, ONE_MILLIS_IN_NANOS * 1000);
                } catch (Throwable e) {
                    failed = e;
                }
            }
        }

        Waiter w1 = startAndWaitUntilBlocked(new Waiter());

        LockResult result = locker3.doTryLockShared(0, k1, ONE_MILLIS_IN_NANOS * 2_000);
        assertEquals(LockResult.TIMED_OUT_LOCK, result);

        w1.join();

        assertNull(w1.failed);
        assertEquals(LockResult.TIMED_OUT_LOCK, w1.result);
    }

    @Test
    public void closedLocker() throws Exception {
        LocalDatabase db = LocalDatabase.open(new Launcher());
        var manager = new LockManager(db, null, -1);

        var locker = new Locker(manager);

        Lock lock = locker.doLockSharedNoPush(0, k1);
        locker.push(lock);
        locker.scopeExitAll();
        db.close();
        manager.doUnlockToUpgradable(locker, lock);
    }

    @Test
    public void closedLocker2() throws Exception {
        var locker = new Locker(mManager);

        locker.doLockShared(0, k1, -1);

        var resultRef = new AtomicReference<LockResult>();

        Thread t = startAndWaitUntilBlocked(new Thread(() -> {
            try {
                var locker2 = new Locker(mManager);
                resultRef.set(locker2.doTryLockExclusive(0, k1, -1));
            } catch (Exception e) {
                System.out.println(e);
            }
        }));

        mManager.close();

        t.join();

        assertEquals(INTERRUPTED, resultRef.get());
    }

    @Test
    public void attachment() throws Exception {
        var locker = new Locker(mManager);
        try {
            locker.attach("hello");
            fail();
        } catch (UnsupportedOperationException e) {
            // Expected.
        }
    }

    @Test
    public void unlockCombine() throws Exception {
        var locker = new Locker(mManager);

        // Should do nothing.
        locker.unlockCombine();

        assertEquals(ACQUIRED, locker.doLockShared(0, k1, -1));
        locker.unlockCombine();
        locker.doUnlock();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));

        // This pairing of acquire and upgrade works because it's for the same lock, immediately
        // upgraded.
        assertEquals(ACQUIRED, locker.doLockUpgradable(0, k1, -1));
        assertEquals(UPGRADED, locker.doLockExclusive(0, k1, -1));
        locker.unlockCombine();
        locker.doUnlock();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));

        // This pairing fails because another lock is in between.
        assertEquals(ACQUIRED, locker.doLockUpgradable(0, k1, -1));
        assertEquals(ACQUIRED, locker.doLockUpgradable(0, k2, -1));
        assertEquals(UPGRADED, locker.doLockExclusive(0, k1, -1));
        try {
            locker.unlockCombine();
            fail();
        } catch (IllegalStateException e) {
            // Cannot combine an acquire with an upgrade
        }
        try {
            locker.doUnlock();
            fail();
        } catch (IllegalStateException e) {
            // Cannot unlock non-immediate upgrade
        }
        locker.scopeExit();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));
        assertEquals(UNOWNED, locker.lockCheck(0, k2));

        // This pairing works.
        assertEquals(ACQUIRED, locker.doLockUpgradable(0, k1, -1));
        assertEquals(ACQUIRED, locker.doLockUpgradable(0, k2, -1));
        locker.unlockCombine();
        locker.doUnlock();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));
        assertEquals(UNOWNED, locker.lockCheck(0, k2));

        // This pairing of upgrades works.
        assertEquals(ACQUIRED, locker.doLockUpgradable(0, k1, -1));
        assertEquals(UPGRADED, locker.doLockExclusive(0, k1, -1));
        assertEquals(ACQUIRED, locker.doLockUpgradable(0, k2, -1));
        assertEquals(UPGRADED, locker.doLockExclusive(0, k2, -1));
        locker.unlockCombine();
        locker.doUnlock();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));
        assertEquals(UNOWNED, locker.lockCheck(0, k2));

        // This pairing of upgrades doesn't work.
        assertEquals(ACQUIRED, locker.doLockUpgradable(0, k1, -1));
        assertEquals(ACQUIRED, locker.doLockUpgradable(0, k2, -1));
        assertEquals(UPGRADED, locker.doLockExclusive(0, k1, -1));
        assertEquals(UPGRADED, locker.doLockExclusive(0, k2, -1));
        locker.unlockCombine();
        try {
            locker.doUnlock();
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
        var locker = new Locker(mManager);

        for (int i=0; i<1000; i++) {
            assertEquals(ACQUIRED, locker.doLockShared(0, key("k" + i), -1));
            locker.unlockCombine();
        }
        locker.doUnlock();
        for (int i=0; i<1000; i++) {
            assertEquals(UNOWNED, locker.lockCheck(0, key("k" + i)));
        }

        // Again, but with a lock which should remain.
        assertEquals(ACQUIRED, locker.doLockShared(0, k1, -1));
        for (int i=0; i<1000; i++) {
            assertEquals(ACQUIRED, locker.doLockShared(0, key("k" + i), -1));
            if (i != 0) {
                locker.unlockCombine();
            }
        }
        assertEquals(OWNED_SHARED, locker.lockCheck(0, k1));
        locker.doUnlock();
        assertEquals(OWNED_SHARED, locker.lockCheck(0, k1));
        for (int i=0; i<1000; i++) {
            assertEquals(UNOWNED, locker.lockCheck(0, key("k" + i)));
        }
        locker.doUnlock();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));

        // Again, with a lock which should remain, and a combined downgrade.
        assertEquals(ACQUIRED, locker.doLockShared(0, k1, -1));
        for (int i=0; i<1000; i++) {
            assertEquals(ACQUIRED, locker.doLockExclusive(0, key("k" + i), -1));
            if (i != 0) {
                locker.unlockCombine();
            }
        }
        assertEquals(OWNED_SHARED, locker.lockCheck(0, k1));
        locker.doUnlockToUpgradable();
        assertEquals(OWNED_SHARED, locker.lockCheck(0, k1));
        for (int i=0; i<1000; i++) {
            assertEquals(OWNED_UPGRADABLE, locker.lockCheck(0, key("k" + i)));
        }
        locker.doUnlock();
        for (int i=0; i<1000; i++) {
            assertEquals(UNOWNED, locker.lockCheck(0, key("k" + i)));
        }
        assertEquals(OWNED_SHARED, locker.lockCheck(0, k1));
        locker.doUnlock();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));

        // Again, with a lock which should remain, and a combined downgrade to shared.
        assertEquals(ACQUIRED, locker.doLockShared(0, k1, -1));
        for (int i=0; i<1000; i++) {
            assertEquals(ACQUIRED, locker.doLockExclusive(0, key("k" + i), -1));
            if (i != 0) {
                locker.unlockCombine();
            }
        }
        assertEquals(OWNED_SHARED, locker.lockCheck(0, k1));
        locker.doUnlockToShared();
        assertEquals(OWNED_SHARED, locker.lockCheck(0, k1));
        for (int i=0; i<1000; i++) {
            assertEquals(OWNED_SHARED, locker.lockCheck(0, key("k" + i)));
        }
        locker.doUnlock();
        for (int i=0; i<1000; i++) {
            assertEquals(UNOWNED, locker.lockCheck(0, key("k" + i)));
        }
        assertEquals(OWNED_SHARED, locker.lockCheck(0, k1));
        locker.doUnlock();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));

        // Again, with a lock which should remain, a bunch of upgrades, and a combined downgrade.
        assertEquals(ACQUIRED, locker.doLockShared(0, k1, -1));
        for (int i=0; i<1000; i++) {
            assertEquals(ACQUIRED, locker.doLockUpgradable(0, key("k" + i), -1));
            assertEquals(UPGRADED, locker.doLockExclusive(0, key("k" + i), -1));
            if (i != 0) {
                locker.unlockCombine();
            }
        }
        assertEquals(OWNED_SHARED, locker.lockCheck(0, k1));
        locker.doUnlockToUpgradable();
        assertEquals(OWNED_SHARED, locker.lockCheck(0, k1));
        for (int i=0; i<1000; i++) {
            assertEquals(OWNED_UPGRADABLE, locker.lockCheck(0, key("k" + i)));
        }
        locker.doUnlock();
        for (int i=0; i<1000; i++) {
            assertEquals(UNOWNED, locker.lockCheck(0, key("k" + i)));
        }
        assertEquals(OWNED_SHARED, locker.lockCheck(0, k1));
        locker.doUnlock();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));
    }

    @Test
    public void unlockCombineBlockOfOne() throws Exception {
        // Tests combine logic for a lock block with one entry.

        var locker = new Locker(mManager);

        // This forms a block of two.
        assertEquals(ACQUIRED, locker.doLockShared(0, k1, -1));
        assertEquals(ACQUIRED, locker.doLockShared(0, k2, -1));

        // Unlock the last, leaving a block with one entry.
        locker.doUnlock();

        // Combine shouldn't do anything.
        locker.unlockCombine();
        
        assertEquals(OWNED_SHARED, locker.lockCheck(0, k1));
        assertEquals(UNOWNED, locker.lockCheck(0, k2));

        locker.doUnlock();
    }

    @Test
    public void unlockCombineTooMuch() throws Exception {
        var locker = new Locker(mManager);

        assertEquals(ACQUIRED, locker.doLockShared(0, k1, -1));


        locker.doLockShared(0, k2, -1);
        locker.unlockCombine();
        locker.unlockCombine();
        locker.unlockCombine();

        locker.doUnlock();

        try {
            locker.doUnlock();
            fail();
        } catch (IllegalStateException e) {
            // No locks held.
        }
    }

    @Test
    public void unlockCombineCrossScope() throws Exception {
        var locker = new Locker(mManager);

        assertEquals(ACQUIRED, locker.doLockShared(0, k1, -1));
        locker.scopeEnter();

        locker.unlockCombine();

        locker.doLockShared(0, k2, -1);
        locker.unlockCombine();
        locker.unlockCombine();
        locker.unlockCombine();

        try {
            locker.doUnlock();
            fail();
        } catch (IllegalStateException e) {
        }
    }

    @Test
    public void unlockDowngradeBlockOfOne() throws Exception {
        var locker = new Locker(mManager);

        assertEquals(ACQUIRED, locker.doLockUpgradable(0, k1, -1));

        // After this, a block of 8 (initial size) should exist.
        for (int i=0; i<7; i++) {
            assertEquals(ACQUIRED, locker.doLockShared(0, key("k" + i), -1));
        }

        // After this, a block of 1 should be stacked.
        assertEquals(UPGRADED, locker.doLockExclusive(0, k1, -1));

        // This will unlock from a block of size 1, popping it off the stack.
        locker.doUnlockToUpgradable();

        assertEquals(OWNED_UPGRADABLE, locker.lockCheck(0, k1));

        // Now unlock everything and verify along the way.

        for (int i=7; --i>=0; ) {
            assertEquals(OWNED_SHARED, locker.lockCheck(0, key("k" + i)));
            locker.doUnlock();
            assertEquals(UNOWNED, locker.lockCheck(0, key("k" + i)));
        }

        assertEquals(OWNED_UPGRADABLE, locker.lockCheck(0, k1));
        locker.doUnlock();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));
    }

    @Test
    public void unlockDowngradeGroup() throws Exception {
        var locker = new Locker(mManager);

        assertEquals(ACQUIRED, locker.doLockShared(0, k1, -1));
        assertEquals(ACQUIRED, locker.doLockUpgradable(0, k2, -1));
        assertEquals(ACQUIRED, locker.doLockUpgradable(0, k3, -1));
        assertEquals(ACQUIRED, locker.doLockUpgradable(0, k4, -1));

        assertEquals(UPGRADED, locker.doLockExclusive(0, k2, -1));
        assertEquals(UPGRADED, locker.doLockExclusive(0, k3, -1));
        locker.unlockCombine();
        assertEquals(UPGRADED, locker.doLockExclusive(0, k4, -1));
        locker.unlockCombine();

        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k2));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k3));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k4));

        locker.doUnlockToUpgradable();

        assertEquals(OWNED_SHARED, locker.lockCheck(0, k1));
        assertEquals(OWNED_UPGRADABLE, locker.lockCheck(0, k2));
        assertEquals(OWNED_UPGRADABLE, locker.lockCheck(0, k3));
        assertEquals(OWNED_UPGRADABLE, locker.lockCheck(0, k4));

        locker.scopeExit();

        // Again with various sizes.
        for (int size=1; size<=30; size++) {
            for (int i=0; i<size; i++) {
                assertEquals(ACQUIRED, locker.doLockUpgradable(0, key("k" + i), -1));
            }

            for (int i=0; i<size; i++) {
                assertEquals(UPGRADED, locker.doLockExclusive(0, key("k" + i), -1));
                if (i != 0) {
                    locker.unlockCombine();
                }
            }

            for (int i=0; i<size; i++) {
                assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, key("k" + i)));
            }

            locker.doUnlockToUpgradable();

            for (int i=0; i<size; i++) {
                assertEquals(OWNED_UPGRADABLE, locker.lockCheck(0, key("k" + i)));
            }

            for (int i=1; i<size; i++) {
                locker.unlockCombine();
            }

            locker.doUnlock();

            for (int i=0; i<size; i++) {
                assertEquals(UNOWNED, locker.lockCheck(0, key("k" + i)));
            }
        }
    }

    @Test
    public void tryLockExclusiveWaitingUpgrader() throws Exception {
        var locker1 = new Locker(mManager);
        assertEquals(ACQUIRED, locker1.doLockShared(0, k1, -1));

        var ex2 = new AtomicReference<Throwable>();

        Thread t2 = startAndWaitUntilBlocked(new Thread(() -> {
            try {
                var locker2 = new Locker(mManager);
                locker2.doLockExclusive(0, k1, -1);
                fail();
            } catch (LockInterruptedException e) {
                // Expected.
            } catch (Throwable e) {
                ex2.set(e);
            }
        }));

        var ex3 = new AtomicReference<Throwable>();

        Thread t3 = startAndWaitUntilBlocked(new Thread(() -> {
            try {
                var locker3 = new Locker(mManager);
                assertEquals(ACQUIRED, locker3.doLockUpgradable(0, k1, -1));
            } catch (Throwable e) {
                ex3.set(e);
            }
        }));

        // At this point, t2 owns the upgradable lock but is blocked by the initial shared
        // lock. Thread t3 is waiting for the upgradable lock. By interrupting t2, it will give
        // up and signal that t3 can proceed.

        t2.interrupt();
        t2.join();
        t3.join();

        assertNull(ex2.get());
        assertNull(ex3.get());
    }

    @Test
    public void unlockNonExclusiveSingle() throws Exception {
        var locker = new Locker(mManager);
        locker.transferExclusive(locker);

        assertEquals(ACQUIRED, locker.doLockShared(0, k1, -1));
        locker.transferExclusive(locker);
        assertEquals(UNOWNED, locker.lockCheck(0, k1));

        assertEquals(ACQUIRED, locker.doLockUpgradable(0, k1, -1));
        locker.transferExclusive(locker);
        assertEquals(UNOWNED, locker.lockCheck(0, k1));

        assertEquals(ACQUIRED, locker.doLockExclusive(0, k1, -1));
        locker.transferExclusive(locker);
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
        locker.scopeUnlockAll();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));

        assertEquals(ACQUIRED, locker.doLockUpgradable(0, k1, -1));
        assertEquals(UPGRADED, locker.doLockExclusive(0, k1, -1));
        locker.transferExclusive(locker);
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
        locker.scopeUnlockAll();
        assertEquals(UNOWNED, locker.lockCheck(0, k1));
    }

    @Test
    public void unlockNonExclusiveMulti() throws Exception {
        var locker = new Locker(mManager);

        assertEquals(ACQUIRED, locker.doLockShared(0, k1, -1));
        assertEquals(ACQUIRED, locker.doLockShared(0, k2, -1));
        locker.transferExclusive(locker);
        assertEquals(UNOWNED, locker.lockCheck(0, k1));
        assertEquals(UNOWNED, locker.lockCheck(0, k2));

        assertEquals(ACQUIRED, locker.doLockUpgradable(0, k1, -1));
        assertEquals(ACQUIRED, locker.doLockUpgradable(0, k2, -1));
        assertEquals(UPGRADED, locker.doLockExclusive(0, k1, -1));
        locker.transferExclusive(locker);
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
        assertEquals(UNOWNED, locker.lockCheck(0, k2));
        locker.scopeUnlockAll();

        assertEquals(ACQUIRED, locker.doLockShared(0, k1, -1));
        assertEquals(ACQUIRED, locker.doLockUpgradable(0, k2, -1));
        assertEquals(ACQUIRED, locker.doLockUpgradable(0, k3, -1));
        assertEquals(UPGRADED, locker.doLockExclusive(0, k2, -1));
        locker.transferExclusive(locker);
        assertEquals(UNOWNED, locker.lockCheck(0, k1));
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k2));
        assertEquals(UNOWNED, locker.lockCheck(0, k3));
        locker.scopeUnlockAll();

        for (int i=0; i<1000; i++) {
            assertEquals(ACQUIRED, locker.doLockShared(0, key("key-" + i), -1));
        }
        locker.transferExclusive(locker);
        for (int i=0; i<1000; i++) {
            assertEquals(UNOWNED, locker.lockCheck(0, key("key-" + i)));
        }
        locker.scopeUnlockAll();

        for (int i=0; i<1000; i++) {
            assertEquals(ACQUIRED, locker.doLockShared(0, key("key-" + i), -1));
        }
        locker.doLockExclusive(0, k1, -1);
        locker.transferExclusive(locker);
        assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, k1));
        for (int i=0; i<1000; i++) {
            assertEquals(UNOWNED, locker.lockCheck(0, key("key-" + i)));
        }
        locker.scopeUnlockAll();
    }

    @Test
    public void unlockNonExclusiveFuzz() throws Exception {
        var locker = new Locker(mManager);
        var rnd = new Random(8675309);

        var keep = new HashSet<String>();
        var toss = new HashSet<String>();

        for (int i=0; i<10_000; i++) {
            int k;
            if (rnd.nextBoolean()) {
                k = rnd.nextInt(10);
            } else {
                k = rnd.nextInt(1000);
            }

            String strKey = "key-" + k;
            byte[] key = key(strKey);

            LockResult result;
            switch (rnd.nextInt(40)) {
            default:
                result = locker.doLockShared(0, key, -1);
                break;
            case 0:
                try {
                    result = locker.doLockUpgradable(0, key, -1);
                } catch (IllegalUpgradeException e) {
                    continue;
                }
                break;
            case 1:
                try {
                    result = locker.doLockExclusive(0, key, -1);
                } catch (IllegalUpgradeException e) {
                    continue;
                }
                break;
            }

            if (result == LockResult.UPGRADED || result == OWNED_EXCLUSIVE) {
                keep.add(strKey);
                toss.remove(strKey);
            } else {
                toss.add(strKey);
            }
        }

        locker.transferExclusive(locker);

        for (String strKey : keep) {
            assertEquals(OWNED_EXCLUSIVE, locker.lockCheck(0, key(strKey)));
        }

        for (String strKey : toss) {
            assertEquals(UNOWNED, locker.lockCheck(0, key(strKey)));
        }

        locker.scopeUnlockAll();

        for (String strKey : keep) {
            assertEquals(UNOWNED, locker.lockCheck(0, key(strKey)));
        }

        for (String strKey : toss) {
            assertEquals(UNOWNED, locker.lockCheck(0, key(strKey)));
        }
    }

    @Test
    public void detachedLock() throws Exception {
        var db = (LocalDatabase) Database.open(new DatabaseConfig());

        Transaction owner = db.newTransaction();
        DetachedLock lock = db.newDetachedLock(owner);

        Transaction txn1 = db.newTransaction();
        txn1.lockTimeout(1, TimeUnit.MILLISECONDS);

        lock.acquireShared(txn1);

        LockResult result = lock.tryAcquireExclusive(1000);
        assertEquals(TIMED_OUT_LOCK, result);

        txn1.reset();

        result = lock.tryAcquireExclusive(1000);
        assertEquals(ACQUIRED, result);
        result = lock.tryAcquireExclusive(1000);
        assertEquals(OWNED_EXCLUSIVE, result);

        try {
            lock.acquireShared(txn1);
            fail();
        } catch (LockTimeoutException e) {
        }

        owner.reset();

        lock.acquireShared(txn1);
        txn1.reset();

        result = lock.tryAcquireShared(owner, 1000);
        assertEquals(OWNED_UPGRADABLE, result);

        result = lock.tryAcquireExclusive(1000);
        assertEquals(ACQUIRED, result);

        result = lock.tryAcquireShared(owner, 1000);
        assertEquals(OWNED_EXCLUSIVE, result);

        try {
            lock.acquireShared(txn1);
            fail();
        } catch (LockTimeoutException e) {
        }

        owner.reset();

        lock.acquireShared(txn1);
        result = lock.tryAcquireShared(txn1, 1000);
        assertEquals(OWNED_SHARED, result);

        db.close();
    }

    private long scheduleUnlock(final Locker locker, final long delayMillis)
        throws InterruptedException
    {
        return schedule(locker, delayMillis, 0);
    }

    private long scheduleUnlockToShared(final Locker locker, final long delayMillis)
        throws InterruptedException
    {
        return schedule(locker, delayMillis, 1);
    }

    private long scheduleUnlockToUpgradable(final Locker locker, final long delayMillis)
        throws InterruptedException
    {
        return schedule(locker, delayMillis, 2);
    }

    private long schedule(final Locker locker, final long delayMillis, final int type)
        throws InterruptedException
    {
        if (mScheduledUnlock != null) {
            mScheduledUnlock.join();
        }

        long end = System.nanoTime() + delayMillis * ONE_MILLIS_IN_NANOS;

        var t = new Thread(() -> {
            LockTest.sleep(delayMillis);
            switch (type) {
                default -> locker.doUnlock();
                case 1 -> locker.doUnlockToShared();
                case 2 -> locker.doUnlockToUpgradable();
            }
        });

        mScheduledUnlock = t;
        t.start();

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
        return mExecutor.submit(() -> {
            LockResult result;
            synchronized (locker) {
                result = switch (type) {
                    default -> locker.doTryLockShared(indexId, key, MEDIUM_TIMEOUT);
                    case 1 -> locker.doTryLockUpgradable(indexId, key, MEDIUM_TIMEOUT);
                    case 2 -> locker.doTryLockExclusive(indexId, key, MEDIUM_TIMEOUT);
                };
            }
            return result;
        });
    }
}
