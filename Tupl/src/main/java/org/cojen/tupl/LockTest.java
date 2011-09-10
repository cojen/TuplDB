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

    private static final long SHORT_TIMEOUT = 1000000L; // 1 millis

    static {
        k1 = key("hello");
        k2 = key("world");
    }

    static byte[] key(String str) {
        return str.getBytes();
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
        assertEquals(LockResult.OWNED_SHARED, locker.lockShared(k1, -1));
        assertEquals(LockResult.ACQUIRED, locker.lockShared(k2, -1));
        assertEquals(LockResult.OWNED_SHARED, locker.lockShared(k1, -1));
        assertEquals(LockResult.OWNED_SHARED, locker.lockShared(k2, -1));
        locker.unlockAll();

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
    }

    @Test
    public void basicUpgradable() {
        Locker locker = new Locker(mManager);
        assertEquals(LockResult.ACQUIRED, locker.lockUpgradable(k1, -1));
        assertEquals(LockResult.OWNED_UPGRADABLE, locker.lockUpgradable(k1, -1));
        assertEquals(LockResult.ACQUIRED, locker.lockUpgradable(k2, -1));
        assertEquals(LockResult.OWNED_UPGRADABLE, locker.lockUpgradable(k1, -1));
        assertEquals(LockResult.OWNED_UPGRADABLE, locker.lockUpgradable(k2, -1));
        locker.unlockAll();

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
    }

    @Test
    public void basicExclusive() {
        Locker locker = new Locker(mManager);
        assertEquals(LockResult.ACQUIRED, locker.lockExclusive(k1, -1));
        assertEquals(LockResult.OWNED_EXCLUSIVE, locker.lockExclusive(k1, -1));
        assertEquals(LockResult.ACQUIRED, locker.lockExclusive(k2, -1));
        assertEquals(LockResult.OWNED_EXCLUSIVE, locker.lockExclusive(k1, -1));
        assertEquals(LockResult.OWNED_EXCLUSIVE, locker.lockExclusive(k2, -1));
        locker.unlockAll();

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
    }

    @Test
    public void pileOfLocks() {
        Locker locker = new Locker(mManager);
        for (int i=0; i<1000; i++) {
            assertEquals(LockResult.ACQUIRED, locker.lockExclusive(key("k" + i), -1));
        }
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
    }
}
