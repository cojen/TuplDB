/*
 *  Copyright (C) 2019 Cojen.org
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

import java.util.Random;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class CheckpointFailureTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(CheckpointFailureTest.class.getName());
    }

    private Database mDb;

    @After
    public void teardown() throws Exception {
        if (mDb != null) {
            mDb.close();
        }
        deleteTempDatabases(getClass());
    }

    @Test
    public void checkpointResume() throws Exception {
        DatabaseConfig config0 = new DatabaseConfig()
            .directPageAccess(false)
            .checkpointRate(-1, null)
            .durabilityMode(DurabilityMode.NO_FLUSH)
            .pageSize(4096);

        Database db0 = newTempDatabase(getClass(), config0);

        DatabasePageArray pa = new DatabasePageArray(4096, db0);
        DatabaseConfig config = config0.clone();
        config.dataPageArray(pa).baseFile(newTempBaseFile(getClass()));
        mDb = Database.open(config);

        final int count = 100_000;

        // Fill up with stuff.
        final long seed = 29083745;
        {
            Index ix = mDb.openIndex("test");

            Transaction txn = mDb.newTransaction();
            ix.store(txn, "hello".getBytes(), "world".getBytes());

            Random rnd = new Random(seed);
            for (int i=0; i<count; i++) {
                byte[] key = randomStr(rnd, 10, 20);
                byte[] value = randomStr(rnd, 10, 100);
                ix.store(Transaction.BOGUS, key, value);
            }

            // Checkpoint and fail.
            pa.enableWriteFailures(() -> rnd.nextDouble() < 0.1);
            try {
                mDb.checkpoint();
                fail();
            } catch (WriteFailureException e) {
            }
        }

        // Resume checkpoint without failures.
        pa.enableWriteFailures(null);
        mDb.checkpoint();

        // Reopen and verify.
        mDb.close();
        db0 = reopenTempDatabase(getClass(), db0, config0);
        pa = new DatabasePageArray(4096, db0);
        config.dataPageArray(pa);
        mDb = Database.open(config);

        assertTrue(mDb.verify(null));

        Index ix = mDb.openIndex("test");

        Random rnd = new Random(seed);
        for (int i=0; i<count; i++) {
            byte[] key = randomStr(rnd, 10, 20);
            byte[] value = randomStr(rnd, 10, 100);
            fastAssertArrayEquals(value, ix.load(null, key));
        }

        assertEquals(count, ix.count(null, null));

        // Rolled back.
        assertNull(ix.load(null, "hello".getBytes()));
    }
}
