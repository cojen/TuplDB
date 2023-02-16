/*
 *  Copyright (C) 2023 Cojen.org
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
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.remote;

import java.net.ServerSocket;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import org.cojen.tupl.core.CursorTest;

import org.cojen.tupl.io.Utils;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RemoteCursorTest extends CursorTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RemoteCursorTest.class.getName());
    }

    @Before
    @Override
    public void createTempDb() throws Exception {
        mServerDb = newTempDatabase(getClass());

        var ss = new ServerSocket(0);
        mServerDb.newServer().acceptAll(ss, 123456);

        mDb = Database.connect(ss.getLocalSocketAddress(), null, 111, 123456);
    }

    @After
    @Override
    public void teardown() throws Exception {
        if (mDb != null) {
            mDb.close();
            mDb = null;
        }
        deleteTempDatabases(getClass());
        mServerDb = null;
    }

    @Override
    protected <T extends Thread> T startAndWaitUntilBlocked(T t) throws InterruptedException {
        return startAndWaitUntilBlockedSocket(t);
    }

    private Database mServerDb;

    @Test
    @Override
    public void stubEviction() throws Exception {
        stubCursor(1000);
    }

    @Override
    public void bigSkip() throws Exception {
        // Override and do nothing. Test is slow, and doing it remotely doesn't test anythng new.
    }

    @Override
    public void bigSkipBounded() throws Exception {
        // Override and do nothing. Test is slow, and doing it remotely doesn't test anythng new.
    }

    @Override
    public void bigSkipBoundedBigKeys() throws Exception {
        // Override and do nothing. Test is slow, and doing it remotely doesn't test anythng new.
    }

    @Test
    public void skip() throws Exception {
        // Need new skip test because the "big" skip tests aren't run.

        View ix = openIndex("skippy");

        for (int i=0; i<1_000; i++) {
            ix.store(Transaction.BOGUS, key(i), value(1));
        }

        Cursor c = ix.newCursor(null);

        c.first();
        c.skip(100);
        fastAssertArrayEquals(key(100), c.key());

        c.skip(200);
        fastAssertArrayEquals(key(300), c.key());

        c.last();
        c.skip(-99);
        fastAssertArrayEquals(key(900), c.key());

        c.skip(-100);
        fastAssertArrayEquals(key(800), c.key());

        c.skip(Long.MIN_VALUE);
        assertNull(c.key());

        try {
            c.skip(Long.MAX_VALUE);
            fail();
        } catch (UnpositionedCursorException e) {
        }

        c.first();
        c.skip(100, key(50), false);
        assertNull(c.key());
    }

    @Test
    @Override
    public void findNearby() throws Exception {
        findNearby(30);
    }

    @Test
    @Override
    public void random() throws Exception {
        View ix = openIndex("test");

        Cursor c = ix.newCursor(null);
        c.random(null, null);
        assertNull(c.key());
        assertNull(c.value());

        for (int i=0; i<1000; i++) {
            ix.store(Transaction.BOGUS, key(i), value(i));
        }

        var buckets = new int[10];

        c = ix.newCursor(null);
        for (int i=0; i<1000; i++) {
            c.random(null, null);
            int key = Utils.decodeIntBE(c.key(), 0);
            buckets[key / 100]++;
        }

        for (int bucket : buckets) {
            // When the b-tree is small, random selection isn't well distributed, so allow a
            // high tolerance.
            assertTrue(0 < bucket && bucket < 500);
        }
    }

    @Test
    @Override
    public void randomRange() throws Exception {
        View ix = openIndex("test");

        for (int i=0; i<1000; i++) {
            ix.store(Transaction.BOGUS, key(i), value(i));
        }

        var buckets = new int[10];

        byte[] low = key(100);
        byte[] high = key(900);

        try (Cursor c = ix.newCursor(null)) {
            for (int i=0; i<1000; i++) {
                c.random(low, high);
                int key = Utils.decodeIntBE(c.key(), 0);
                assertTrue(100 <= key && key < 900);
            }
        }
    }

    @Test
    @Override
    public void randomNonRange() throws Exception {
        randomNonRange(200);
    }

    @Test
    @Override
    public void stubRecycle() throws Exception {
        stubRecycle(100);
    }

    @Test
    @Override
    public void stability() throws Exception {
        stability(1000);
    }

    @Test
    @Override
    public void stability2() throws Exception {
        stability2(1000);
    }

    @Test
    @Override
    public void stability3() throws Exception {
        stability3(1000);
    }
}
