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

package org.cojen.tupl;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * Tests specialized union view operations. See MergeViewTest for more tests.
 *
 * @author Brian S O'Neill
 */
public class UnionViewTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(UnionViewTest.class.getName());
    }

    @Before
    public void createTempDb() throws Exception {
        DatabaseConfig config = new DatabaseConfig();
        config.directPageAccess(false);
        config.maxCacheSize(100000000);
        mDb = Database.open(config);
    }

    @After
    public void teardown() throws Exception {
        mDb.close();
        mDb = null;
    }

    protected Database mDb;

    @Test
    public void unlockCombine() throws Exception {
        unlockCombine(0); // normal step
        unlockCombine(1);
        unlockCombine(2);
    }

    private void unlockCombine(int delta) throws Exception {
        // Steping over entries with an updater should combine locks and then unlock them
        // together.

        final int keyCount = 10;

        Index[] indexes = new Index[4];
        for (int i=0; i<indexes.length; i++) {
            indexes[i] = mDb.openIndex("test-" + i);
            for (int j=0; j<keyCount; j++) {
                byte[] key = ("key-" + j).getBytes();
                byte[] value = ("value-" + i).getBytes();
                indexes[i].store(null, key, value);
            }
        }

        View union;
        {
            View v1 = indexes[0].viewUnion(null, indexes[1]);
            View v2 = indexes[2].viewUnion(null, indexes[3]);
            union = v1.viewUnion(null, v2);
        }

        Transaction txn = mDb.newTransaction();

        // Null transaction means that another one should be defined internally.
        Updater up = union.newUpdater(null);

        byte[] value = "value-0".getBytes();

        int j=0;
        while (true) {
            byte[] key = ("key-" + j).getBytes();
            fastAssertArrayEquals(key, up.key());
            fastAssertArrayEquals(value, up.value());

            // Verify that lock is held by updater.
            for (int i=0; i<indexes.length; i++) {
                Index ix = indexes[i];
                assertEquals(LockResult.TIMED_OUT_LOCK, txn.tryLockExclusive(ix.getId(), key, 0));
            }

            boolean more;
            if (delta == 0) {
                more = up.step();
                j++;
            } else {
                more = up.step(delta);
                j += delta;
            }

            // Verify that lock is released by updater.
            for (int i=0; i<indexes.length; i++) {
                Index ix = indexes[i];
                assertEquals(LockResult.ACQUIRED, txn.tryLockExclusive(ix.getId(), key, 0));
                txn.unlock();
            }

            if (!more) {
                break;
            }
        }

        assertEquals(keyCount, j);

        // Verify all locks are available.
        for (int i=0; i<indexes.length; i++) {
            Index ix = indexes[i];
            for (j=0; j<keyCount; j++) {
                byte[] key = ("key-" + j).getBytes();
                assertEquals(LockResult.ACQUIRED, txn.tryLockExclusive(ix.getId(), key, 0));
            }
        }

        txn.reset();
    }
}
