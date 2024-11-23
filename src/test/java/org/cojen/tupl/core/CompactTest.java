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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;

import java.util.Arrays;
import java.util.Random;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import org.cojen.tupl.diag.CompactionObserver;
import org.cojen.tupl.diag.DatabaseStats;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class CompactTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(CompactTest.class.getName());
    }

    protected DatabaseConfig decorate(DatabaseConfig config) throws Exception {
        return config;
    }

    protected Database newTempDb(boolean autoCheckpoints) throws Exception {
        var config = new DatabaseConfig().durabilityMode(DurabilityMode.NO_SYNC);
        if (!autoCheckpoints) {
            config.checkpointRate(-1, null);
        }
        return newTempDatabase(getClass(), config);
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
        mDb = null;
        mIndex = null;
    }

    protected Database mDb;

    private Index mIndex;

    private Index openTestIndex() throws Exception {
        // Stash in a field to prevent GC activity from closing index too soon and messing up
        // the stats.
        return mIndex = mDb.openIndex("test");
    }

    @Test
    public void basic() throws Exception {
        mDb = newTempDb(true);

        final Index ix = openTestIndex();
        final int seed = 98232;
        final int count = 100000;

        var rnd = new Random(seed);
        for (int i=0; i<count; i++) {
            int k = rnd.nextInt();
            byte[] key = ("key" + k).getBytes();
            ix.store(Transaction.BOGUS, key, key);
        }

        rnd = new Random(seed);
        for (int i=0; i<count; i++) {
            int k = rnd.nextInt();
            if (i % 4 != 0) {
                byte[] key = ("key" + k).getBytes();
                ix.delete(Transaction.BOGUS, key);
            }
        }

        DatabaseStats stats1 = mDb.stats();

        mDb.compactFile(null, 0.9);

        DatabaseStats stats2 = mDb.stats();

        try {
            assertTrue(stats2.freePages < stats1.freePages);
            assertTrue(stats2.totalPages < stats1.totalPages);
        } catch (AssertionError e) {
            // Can fail if delayed by concurrent test load. Retry.
            mDb.compactFile(null, 0.9);
            stats2 = mDb.stats();
            assertTrue(stats2.freePages < stats1.freePages);
            assertTrue(stats2.totalPages < stats1.totalPages);
        }

        assertTrue(mDb.verify(null, 1));

        rnd = new Random(seed);
        for (int i=0; i<count; i++) {
            int k = rnd.nextInt();
            if (i % 4 == 0) {
                byte[] key = ("key" + k).getBytes();
                byte[] value = ix.load(Transaction.BOGUS, key);
                fastAssertArrayEquals(key, value);
            }
        }

        // Compact even further.
        for (int i=91; i<=99; i++) {
            mDb.compactFile(null, i / 100.0);
        }

        DatabaseStats stats3 = mDb.stats();

        assertTrue(stats3.freePages < stats2.freePages);
        assertTrue(stats3.totalPages < stats2.totalPages);

        assertTrue(mDb.verify(null, 0));

        rnd = new Random(seed);
        for (int i=0; i<count; i++) {
            int k = rnd.nextInt();
            if (i % 4 == 0) {
                byte[] key = ("key" + k).getBytes();
                byte[] value = ix.load(Transaction.BOGUS, key);
                fastAssertArrayEquals(key, value);
            }
        }
    }

    @Test
    public void largeValues1() throws Exception {
        largeValues(false, 512, 1000, 4000, 4000);
    }

    @Test
    public void largeValues2() throws Exception {
        largeValues(false, 16384, 100, 10000, 50000);
    }

    @Test
    public void largeValues3() throws Exception {
        largeValues(false, 65536, 100, 70000, 90000);
    }

    @Test
    public void largeValues4() throws Exception {
        largeValues(false, 512, 100, 30000, 50000);
    }

    @Test
    public void largeValues5() throws Exception {
        // Include a large sparse value too.
        largeValues(false, 512, 100, 30000, 50000, () -> {
            Index ix = openTestIndex();
            ValueAccessor accessor = ix.newAccessor(null, "sparse".getBytes());
            accessor.valueLength(100_000);
            accessor.close();
        });
    }

    @Test
    public void largeKeys1() throws Exception {
        largeValues(true, 512, 1000, 4000, 4000);
    }

    @Test
    public void largeKeys2() throws Exception {
        largeValues(true, 16384, 100, 10000, 50000);
    }

    @Test
    public void largeKeys3() throws Exception {
        largeValues(true, 65536, 100, 70000, 90000);
    }

    @Test
    public void largeKeys4() throws Exception {
        largeValues(true, 512, 100, 30000, 50000);
    }

    private void largeValues(final boolean forKey, final int pageSize,
                             final int count, final int min, final int max)
        throws Exception
    {
        largeValues(forKey, pageSize, count, min, max, null);
    }

    private void largeValues(final boolean forKey, final int pageSize,
                             final int count, final int min, final int max,
                             final Callback prepare)
        throws Exception
    {
        mDb = newTempDatabase(getClass(),
                              decorate(new DatabaseConfig()
                                       .pageSize(pageSize)
                                       .minCacheSize(10_000_000)
                                       .durabilityMode(DurabilityMode.NO_FLUSH)));

        final Index ix = openTestIndex();
        final int seed = 1234;

        var rnd1 = new Random(seed);
        var rnd2 = new Random(seed);

        for (int i=0; i<count; i++) {
            int k = rnd1.nextInt();
            byte[] key, value;
            if (forKey) {
                key = randomStr(rnd1, min, max);
                value = ("value" + k).getBytes();
            } else {
                key = ("key" + k).getBytes();
                value = randomStr(rnd2, min, max);
            }
            ix.store(Transaction.BOGUS, key, value);
        }

        rnd1 = new Random(seed);
        for (int i=0; i<count; i++) {
            int k = rnd1.nextInt();
            byte[] key;
            if (forKey) {
                key = randomStr(rnd1, min, max);
            } else {
                key = ("key" + k).getBytes();
            }
            if (i % 2 != 0) {
                ix.delete(Transaction.BOGUS, key);
            }
        }

        if (prepare != null) {
            prepare.run();
        }

        DatabaseStats stats1 = mDb.stats();

        mDb.compactFile(null, 0.9);

        DatabaseStats stats2 = mDb.stats();

        try {
            assertTrue(stats2.freePages < stats1.freePages);
            assertTrue(stats2.totalPages < stats1.totalPages);
        } catch (AssertionError e) {
            // Can fail if delayed by concurrent test load. Retry.
            mDb.compactFile(null, 0.9);
            stats2 = mDb.stats();
            assertTrue(stats2.freePages < stats1.freePages);
            assertTrue(stats2.totalPages < stats1.totalPages);
        }

        assertTrue(mDb.verify(null, 1));

        rnd1 = new Random(seed);
        rnd2 = new Random(seed);
        for (int i=0; i<count; i++) {
            int k = rnd1.nextInt();
            if (forKey) {
                byte[] key = randomStr(rnd1, min, max);
                if (i % 2 == 0) {
                    byte[] v = ("value" + k).getBytes();
                    byte[] value = ix.load(Transaction.BOGUS, key);
                    fastAssertArrayEquals(v, value);
                }
            } else {
                byte[] v = randomStr(rnd2, min, max);
                if (i % 2 == 0) {
                    byte[] key = ("key" + k).getBytes();
                    byte[] value = ix.load(Transaction.BOGUS, key);
                    fastAssertArrayEquals(v, value);
                }
            }
        }

        // Compact even further.
        for (int i=91; i<=99; i++) {
            mDb.compactFile(null, i / 100.0);
        }

        DatabaseStats stats3 = mDb.stats();

        assertTrue(stats3.freePages < stats2.freePages);
        assertTrue(stats3.totalPages < stats2.totalPages);

        assertTrue(mDb.verify(null, 0));

        rnd1 = new Random(seed);
        rnd2 = new Random(seed);
        for (int i=0; i<count; i++) {
            int k = rnd1.nextInt();
            if (forKey) {
                byte[] key = randomStr(rnd1, min, max);
                if (i % 2 == 0) {
                    byte[] v = ("value" + k).getBytes();
                    byte[] value = ix.load(Transaction.BOGUS, key);
                    fastAssertArrayEquals(v, value);
                }
            } else {
                byte[] v = randomStr(rnd2, min, max);
                if (i % 2 == 0) {
                    byte[] key = ("key" + k).getBytes();
                    byte[] value = ix.load(Transaction.BOGUS, key);
                    fastAssertArrayEquals(v, value);
                }
            }
        }
    }

    @Test
    public void largeInternalKeys() throws Exception {
        // Test with large keys that start with the same long prefix, defeating suffix
        // compression.

        final int count = 1000;
        final int min = 4000;
        final int max = 4000;

        final byte[] prefix = new byte[1000];
        Arrays.fill(prefix, (byte) 0x55);

        mDb = newTempDatabase(getClass(),
                              decorate(new DatabaseConfig()
                                       .pageSize(512)
                                       .minCacheSize(10_000_000)
                                       .durabilityMode(DurabilityMode.NO_FLUSH)));
        
        final Index ix = openTestIndex();
        final int seed = 1234;

        var rnd1 = new Random(seed);
        var rnd2 = new Random(seed);

        for (int i=0; i<count; i++) {
            int k = rnd1.nextInt();
            byte[] key = randomStr(rnd1, prefix, min, max);
            byte[] value = ("value" + k).getBytes();
            ix.store(Transaction.BOGUS, key, value);
        }

        mDb.verify(null, 1);

        rnd1 = new Random(seed);
        for (int i=0; i<count; i++) {
            int k = rnd1.nextInt();
            byte[] key = randomStr(rnd1, prefix, min, max);
            if (i % 2 != 0) {
                ix.delete(Transaction.BOGUS, key);
            }
        }

        DatabaseStats stats1 = mDb.stats();

        mDb.compactFile(null, 0.9);

        DatabaseStats stats2 = mDb.stats();

        try {
            assertTrue(stats2.freePages < stats1.freePages);
            assertTrue(stats2.totalPages < stats1.totalPages);
        } catch (AssertionError e) {
            // Can fail if delayed by concurrent test load. Retry.
            mDb.compactFile(null, 0.9);
            stats2 = mDb.stats();
            assertTrue(stats2.freePages < stats1.freePages);
            assertTrue(stats2.totalPages < stats1.totalPages);
        }

        assertTrue(mDb.verify(null, 1));
    }

    @Test
    public void manualAbort() throws Exception {
        mDb = newTempDb(false);

        final Index ix = openTestIndex();
        final int seed = 98232;
        final int count = 100000;

        var rnd = new Random(seed);
        for (int i=0; i<count; i++) {
            int k = rnd.nextInt();
            byte[] key = ("key" + k).getBytes();
            ix.store(Transaction.BOGUS, key, key);
        }

        rnd = new Random(seed);
        for (int i=0; i<count; i++) {
            int k = rnd.nextInt();
            if (i % 4 != 0) {
                byte[] key = ("key" + k).getBytes();
                ix.delete(Transaction.BOGUS, key);
            }
        }

        mDb.checkpoint();
        DatabaseStats stats1 = mDb.stats();

        var obs = new CompactionObserver() {
            private int count;

            @Override
            public boolean indexNodeVisited(long id) {
                return ++count < 100;
            }
        };

        mDb.compactFile(obs, 0.5);

        DatabaseStats stats2 = mDb.stats();
        assertEqualStats(stats1, stats2);

        assertTrue(mDb.verify(null, 1));
    }

    @Test
    public void autoAbort() throws Exception {
        mDb = newTempDb(true);

        final Index ix = openTestIndex();
        final int seed = 98232;
        final int count = 100000;

        var rnd = new Random(seed);
        for (int i=0; i<count; i++) {
            int k = rnd.nextInt();
            byte[] key = ("key" + k).getBytes();
            ix.store(Transaction.BOGUS, key, key);
        }

        rnd = new Random(seed);
        for (int i=0; i<count; i++) {
            int k = rnd.nextInt();
            if (i % 4 != 0) {
                byte[] key = ("key" + k).getBytes();
                ix.delete(Transaction.BOGUS, key);
            }
        }

        final var obs = new CompactionObserver() {
            private int count;
            private boolean resume;

            @Override
            public synchronized boolean indexNodeVisited(long id) {
                if (++count > 100) {
                    try {
                        while (!resume) {
                            wait();
                        }
                    } catch (InterruptedException e) {
                        return false;
                    }
                }
                return true;
            }

            synchronized void resume() {
                this.resume = true;
                notify();
            }
        };

        var comp = new Thread() {
            volatile Object result;

            @Override
            public void run() {
                try {
                    DatabaseStats stats1 = mDb.stats();
                    mDb.compactFile(obs, 0.5);
                    DatabaseStats stats2 = mDb.stats();
                    result = stats2.totalPages < stats1.totalPages;
                } catch (Exception e) {
                    result = e;
                }
            }
        };

        comp.start();

        // Compaction will abort because of database growth.
        rnd = new Random(seed);
        for (int i=0; i<count; i++) {
            int k = rnd.nextInt();
            if (i % 4 != 0) {
                byte[] key = ("key" + k).getBytes();
                ix.insert(Transaction.BOGUS, key, key);
            }
        }

        obs.resume();
        comp.join();

        assertEquals(Boolean.FALSE, comp.result);

        assertTrue(mDb.verify(null, 1));

        rnd = new Random(seed);
        for (int i=0; i<count; i++) {
            int k = rnd.nextInt();
            byte[] key = ("key" + k).getBytes();
            byte[] value = ix.load(Transaction.BOGUS, key);
            fastAssertArrayEquals(key, value);
        }
    }

    @Test
    public void stress() throws Exception {
        for (int i=3; --i>=0; ) {
            AssertionError e = doStress();
            if (e == null) {
                break;
            }
            if (i == 0) {
                throw e;
            }
            // Retry.
            teardown();
        }
    }

    private AssertionError doStress() throws Exception {
        mDb = newTempDatabase(getClass(),
                              decorate(new DatabaseConfig()
                                       .pageSize(512)
                                       .minCacheSize(10_000_000).maxCacheSize(100_000_000)
                                       .durabilityMode(DurabilityMode.NO_FLUSH)));

        var comp = new Thread() {
            volatile boolean stop;
            volatile int success;
            volatile int abort;
            volatile Exception failed;

            @Override
            public void run() {
                try {
                    while (!stop) {
                        DatabaseStats stats1 = mDb.stats();
                        mDb.compactFile(null, 0.5);
                        DatabaseStats stats2 = mDb.stats();
                        if (stats2.totalPages < stats1.totalPages) {
                            success++;
                        } else {
                            abort++;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    failed = e;
                }
            }
        };

        comp.setPriority((Thread.NORM_PRIORITY + Thread.MAX_PRIORITY) / 2);
        comp.start();

        final Index ix = openTestIndex();
        final int count = 1000;
        int seed = 1234;

        for (int round=0; round<1000; round++) {
            int roundCount = round;

            var rnd1 = new Random(seed);
            var rnd2 = new Random(seed);

            for (int i=0; i<roundCount; i++) {
                int k = rnd1.nextInt();
                byte[] key = ("key" + k).getBytes();
                byte[] value = randomStr(rnd2, 1000);
                ix.store(Transaction.BOGUS, key, value);
            }

            rnd1 = new Random(seed);
            for (int i=0; i<roundCount; i++) {
                int k = rnd1.nextInt();
                byte[] key = ("key" + k).getBytes();
                ix.delete(Transaction.BOGUS, key);
            }

            Thread.sleep(10);

            seed++;
        }

        comp.stop = true;
        comp.join();

        assertNull(comp.failed);
        assertTrue(comp.abort > 0);
        try {
            // This assertion might fail on a slower machine.
            assertTrue(comp.success > 0);
            return null;
        } catch (AssertionError e) {
            return e;
        }
    }

    @Test
    public void longTransaction() throws Exception {
        // A transaction with an undo log persisted will not be discovered by the compaction
        // scan. This is only a problem for long running transactions -- they need to span the
        // entire duration of the compaction.

        mDb = newTempDb(false);
        final Index ix = openTestIndex();

        for (int i=100000; i<200000; i++) {
            byte[] key = ("key-" + i).getBytes();
            ix.insert(null, key, key);
        }

        mDb.checkpoint();
        DatabaseStats stats1 = mDb.stats();

        assertEquals(0, stats1.freePages);

        Transaction txn = mDb.newTransaction();
        for (int i=110000; i<200000; i++) {
            byte[] key = ("key-" + i).getBytes();
            ix.delete(txn, key);
        }

        mDb.checkpoint();
        DatabaseStats stats2 = mDb.stats();
        assertTrue(stats2.freePages > 100);

        mDb.compactFile(null, 0.9);

        // Nothing happened because most pages were in the undo log and not moved.
        assertEqualStats(stats2, mDb.stats());

        txn.commit();

        for (int i=10; --i>=0; ) {
            // Compact will work this time now that undo log is gone.
            mDb.compactFile(null, 0.9);
            DatabaseStats stats3 = mDb.stats();

            try {
                assertTrue(stats3.freePages < stats2.freePages);
                assertTrue(stats3.totalPages < stats2.totalPages);
                break;
            } catch (AssertionError e) {
                // Can fail if delayed by concurrent test load.
                if (i == 0) {
                    throw e;
                }
                // Retry.
                sleep(1000);
            }
        }
    }

    @Test
    public void trashHiding() throws Exception {
        // A transaction can move a large value into the fragmented trash and compaction won't
        // be able to move it. This is not desirable, but at least confirm the behavior. If the
        // trash could be scanned, it would also need to check if compaction is in progress
        // when values move to and from the trash.

        mDb = newTempDb(true);
        final Index ix = openTestIndex();

        byte[] key = "hello".getBytes();
        byte[] value = randomStr(new Random(), 1000000);

        ix.store(Transaction.BOGUS, key, value);
        
        mDb.checkpoint();
        DatabaseStats stats1 = mDb.stats();

        Transaction txn = mDb.newTransaction();
        ix.delete(txn, key);

        mDb.compactFile(null, 0.9);

        DatabaseStats stats2 = mDb.stats();
        assertTrue(stats2.totalPages - stats2.freePages > 200);

        txn.commit();

        mDb.compactFile(null, 0.9);

        DatabaseStats stats3 = mDb.stats();
        assertTrue(stats3.totalPages - stats3.freePages < 50);
    }

    @Test
    public void randomInserts() throws Exception {
        // Random inserts with a small cache size tends to create a lot of extra unused space
        // in the file. Verify that compaction can reclaim the space.

        mDb = newTempDatabase(getClass(),
                              decorate(new DatabaseConfig()
                                       .minCacheSize(1_000_000)
                                       .checkpointRate(-1, null)
                                       .durabilityMode(DurabilityMode.NO_FLUSH)));
        
        final Index ix = openTestIndex();

        final int seed = 793846;
        final int count = 500000;

        var rnd = new Random(seed);
        var value = new byte[0];

        for (int i=0; i<count; i++) {
            byte[] key = randomStr(rnd, 10, 20);
            ix.store(null, key, value);
            if (i % 100000 == 0) {
                mDb.checkpoint();
            }
        }

        mDb.checkpoint();

        DatabaseStats stats1 = mDb.stats();

        assertTrue(mDb.compactFile(null, 0.95));

        DatabaseStats stats2 = mDb.stats();

        assertTrue(stats1.totalPages > stats2.totalPages * 2);

        // Verify no data loss.

        mDb.verify(null, 1);

        rnd = new Random(seed);

        for (int i=0; i<count; i++) {
            byte[] key = randomStr(rnd, 10, 20);
            value = ix.load(null, key);
            assertNotNull(value);
            assertEquals(0, value.length);
        }
    }

    @Test
    public void snapshotAbort() throws Exception {
        mDb = newTempDatabase(getClass(),
                              decorate(new DatabaseConfig()
                                       .checkpointRate(-1, null)
                                       .durabilityMode(DurabilityMode.NO_FLUSH)));

        final Index ix = openTestIndex();

        for (int i=0; i<100000; i++) {
            byte[] key = ("key-" + i).getBytes();
            ix.store(null, key, key);
        }

        mDb.checkpoint();

        for (int i=0; i<100000; i++) {
            byte[] key = ("key-" + i).getBytes();
            ix.delete(null, key);
        }

        Snapshot snap = mDb.beginSnapshot();

        for (int i=0; i<10; i++) {
            assertFalse(mDb.compactFile(null, 0.9));
        }

        var dbFile = new File(baseFileForTempDatabase(getClass(), mDb).getPath() + ".db");
        assertTrue(dbFile.length() > 1_000_000);

        var bout = new ByteArrayOutputStream();
        snap.writeTo(bout);

        assertTrue(mDb.compactFile(null, 0.9));
        assertTrue(dbFile.length() < 100_000);

        deleteTempDatabase(getClass(), mDb);

        var bin = new ByteArrayInputStream(bout.toByteArray());

        DatabaseConfig config = decorate(new DatabaseConfig()
                                         .baseFile(newTempBaseFile(getClass())));

        mDb = Database.restoreFromSnapshot(config, bin);

        assertTrue(mDb.verify(null, 1));

        mDb.close();

        // Verify that this doesn't fail after database is closed.
        DatabaseStats stats = mDb.stats();
        assertEquals(0, stats.cachePages);
        assertEquals(0, stats.dirtyPages);
    }

    private static void assertEqualStats(DatabaseStats stats1, DatabaseStats stats2) {
        // Ignore these.
        stats1.checkpointDuration = 0;
        stats2.checkpointDuration = 0;

        assertEquals(stats1, stats2);
    }
}
