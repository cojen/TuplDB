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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;

import java.util.Random;

import org.junit.*;
import static org.junit.Assert.*;

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
        config.directPageAccess(false);
        return config;
    }

    protected Database newTempDb() throws Exception {
        return newTempDatabase(getClass());
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
        mDb = newTempDb();

        final Index ix = openTestIndex();
        final int seed = 98232;
        final int count = 100000;

        Random rnd = new Random(seed);
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

        Database.Stats stats1 = mDb.stats();

        mDb.compactFile(null, 0.9);

        Database.Stats stats2 = mDb.stats();

        assertTrue(stats2.freePages() < stats1.freePages());
        assertTrue(stats2.totalPages() < stats1.totalPages());

        assertTrue(mDb.verify(null));

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

        Database.Stats stats3 = mDb.stats();

        assertTrue(stats3.freePages() < stats2.freePages());
        assertTrue(stats3.totalPages() < stats2.totalPages());

        assertTrue(mDb.verify(null));

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
    public void largeValues() throws Exception {
        mDb = newTempDatabase(getClass(),
                              decorate(new DatabaseConfig()
                                       .pageSize(512)
                                       .minCacheSize(10000000)
                                       .durabilityMode(DurabilityMode.NO_FLUSH)));

        final Index ix = openTestIndex();
        final int seed = 1234;
        final int count = 1000;

        Random rnd1 = new Random(seed);
        Random rnd2 = new Random(seed);

        for (int i=0; i<count; i++) {
            int k = rnd1.nextInt();
            byte[] key = ("key" + k).getBytes();
            byte[] value = randomStr(rnd2, 4000);
            ix.store(Transaction.BOGUS, key, value);
        }

        rnd1 = new Random(seed);
        for (int i=0; i<count; i++) {
            int k = rnd1.nextInt();
            if (i % 2 != 0) {
                byte[] key = ("key" + k).getBytes();
                ix.delete(Transaction.BOGUS, key);
            }
        }

        Database.Stats stats1 = mDb.stats();

        mDb.compactFile(null, 0.9);

        Database.Stats stats2 = mDb.stats();

        assertTrue(stats2.freePages() < stats1.freePages());
        assertTrue(stats2.totalPages() < stats1.totalPages());

        assertTrue(mDb.verify(null));

        rnd1 = new Random(seed);
        rnd2 = new Random(seed);
        for (int i=0; i<count; i++) {
            int k = rnd1.nextInt();
            byte[] v = randomStr(rnd2, 4000);
            if (i % 2 == 0) {
                byte[] key = ("key" + k).getBytes();
                byte[] value = ix.load(Transaction.BOGUS, key);
                fastAssertArrayEquals(v, value);
            }
        }

        // Compact even further.
        for (int i=91; i<=99; i++) {
            mDb.compactFile(null, i / 100.0);
        }

        Database.Stats stats3 = mDb.stats();

        assertTrue(stats3.freePages() < stats2.freePages());
        assertTrue(stats3.totalPages() < stats2.totalPages());

        assertTrue(mDb.verify(null));

        rnd1 = new Random(seed);
        rnd2 = new Random(seed);
        for (int i=0; i<count; i++) {
            int k = rnd1.nextInt();
            byte[] v = randomStr(rnd2, 4000);
            if (i % 2 == 0) {
                byte[] key = ("key" + k).getBytes();
                byte[] value = ix.load(Transaction.BOGUS, key);
                fastAssertArrayEquals(v, value);
            }
        }
    }

    @Test
    public void manualAbort() throws Exception {
        mDb = newTempDb();

        final Index ix = openTestIndex();
        final int seed = 98232;
        final int count = 100000;

        Random rnd = new Random(seed);
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
        Database.Stats stats1 = mDb.stats();

        CompactionObserver obs = new CompactionObserver() {
            private int count;

            @Override
            public boolean indexNodeVisited(long id) {
                return ++count < 100;
            }
        };

        mDb.compactFile(obs, 0.5);

        Database.Stats stats2 = mDb.stats();
        assertEquals(stats1, stats2);

        assertTrue(mDb.verify(null));
    }

    @Test
    public void autoAbort() throws Exception {
        mDb = newTempDb();

        final Index ix = openTestIndex();
        final int seed = 98232;
        final int count = 100000;

        Random rnd = new Random(seed);
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

        class Obs extends CompactionObserver {
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

        final Obs obs = new Obs();

        class Compactor extends Thread {
            volatile Object result;

            @Override
            public void run() {
                try {
                    Database.Stats stats1 = mDb.stats();
                    mDb.compactFile(obs, 0.5);
                    Database.Stats stats2 = mDb.stats();
                    result = stats2.totalPages() < stats1.totalPages();
                } catch (Exception e) {
                    result = e;
                }
            }
        };

        Compactor comp = new Compactor();
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

        assertTrue(mDb.verify(null));

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
        for (int i=1; i<=3; i++) {
            try {
                doStress();
                break;
            } catch (AssertionError e) {
                // Retry.
                teardown();
            }
        }
    }

    private void doStress() throws Exception {
        mDb = newTempDatabase(getClass(),
                              decorate(new DatabaseConfig()
                                       .pageSize(512)
                                       .minCacheSize(100000000)
                                       .durabilityMode(DurabilityMode.NO_FLUSH)));

        class Compactor extends Thread {
            volatile boolean stop;
            volatile int success;
            volatile int abort;
            volatile Exception failed;

            @Override
            public void run() {
                try {
                    while (!stop) {
                        Database.Stats stats1 = mDb.stats();
                        mDb.compactFile(null, 0.5);
                        Database.Stats stats2 = mDb.stats();
                        if (stats2.totalPages() < stats1.totalPages()) {
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

        Compactor comp = new Compactor();
        comp.start();

        final Index ix = openTestIndex();
        final int count = 1000;
        int seed = 1234;

        for (int round=0; round<1000; round++) {
            int roundCount = round;

            Random rnd1 = new Random(seed);
            Random rnd2 = new Random(seed);

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

            seed++;
        }

        comp.stop = true;
        comp.join();

        assertNull(comp.failed);
        assertTrue(comp.abort > 0);
        // This assertion might fail on a slower machine.
        assertTrue(comp.success > 0);
    }

    @Test
    public void longTransaction() throws Exception {
        // A transaction with an undo log persisted will not be discovered by the compaction
        // scan. This is only a problem for long running transactions -- they need to span the
        // entire duration of the compaction.

        mDb = newTempDb();
        mDb.suspendCheckpoints();
        final Index ix = openTestIndex();

        for (int i=100000; i<200000; i++) {
            byte[] key = ("key-" + i).getBytes();
            ix.insert(null, key, key);
        }

        mDb.checkpoint();
        Database.Stats stats1 = mDb.stats();

        assertEquals(0, stats1.freePages());

        Transaction txn = mDb.newTransaction();
        for (int i=110000; i<200000; i++) {
            byte[] key = ("key-" + i).getBytes();
            ix.delete(txn, key);
        }

        mDb.checkpoint();
        Database.Stats stats2 = mDb.stats();
        assertTrue(stats2.freePages() > 100);

        mDb.compactFile(null, 0.9);

        // Nothing happened because most pages were in the undo log and not moved.
        assertEquals(stats2, mDb.stats());

        txn.commit();

        // Compact will work this time now that undo log is gone.
        mDb.compactFile(null, 0.9);
        Database.Stats stats3 = mDb.stats();

        assertTrue(stats3.freePages() < stats2.freePages());
        assertTrue(stats3.totalPages() < stats2.totalPages());
    }

    @Test
    public void trashHiding() throws Exception {
        // A transaction can move a large value into the fragmented trash and compaction won't
        // be able to move it. This is not desirable, but at least confirm the behavior. If the
        // trash could be scanned, it would also need to check if compaction is in progress
        // when values move to and from the trash.

        mDb = newTempDb();
        final Index ix = openTestIndex();

        byte[] key = "hello".getBytes();
        byte[] value = randomStr(new Random(), 1000000);

        ix.store(Transaction.BOGUS, key, value);
        
        mDb.checkpoint();
        Database.Stats stats1 = mDb.stats();

        Transaction txn = mDb.newTransaction();
        ix.delete(txn, key);

        mDb.compactFile(null, 0.9);

        Database.Stats stats2 = mDb.stats();
        assertTrue(stats2.totalPages() - stats2.freePages() > 200);

        txn.commit();

        mDb.compactFile(null, 0.9);

        Database.Stats stats3 = mDb.stats();
        assertTrue(stats3.totalPages() - stats3.freePages() < 50);
    }

    @Test
    public void randomInserts() throws Exception {
        // Random inserts with a small cache size tends to create a lot of extra unused space
        // in the file. Verify compaction can reclaim the space.

        mDb = newTempDatabase(getClass(),
                              decorate(new DatabaseConfig()
                                       .minCacheSize(1000000)
                                       .checkpointRate(-1, null)
                                       .durabilityMode(DurabilityMode.NO_FLUSH)));
        
        final Index ix = openTestIndex();

        final int seed = 793846;
        final int count = 500000;

        Random rnd = new Random(seed);
        byte[] value = new byte[0];

        for (int i=0; i<count; i++) {
            byte[] key = randomStr(rnd, 10, 20);
            ix.store(null, key, value);
            if (i % 100000 == 0) {
                mDb.checkpoint();
            }
        }

        mDb.checkpoint();

        Database.Stats stats1 = mDb.stats();

        assertTrue(mDb.compactFile(null, 0.95));

        Database.Stats stats2 = mDb.stats();

        assertTrue(stats1.totalPages() > stats2.totalPages() * 2);

        // Verify no data loss.

        mDb.verify(null);

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

        File dbFile = new File(baseFileForTempDatabase(getClass(), mDb).getPath() + ".db");
        assertTrue(dbFile.length() > 1_000_000);

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        snap.writeTo(bout);

        assertTrue(mDb.compactFile(null, 0.9));
        assertTrue(dbFile.length() < 100_000);

        deleteTempDatabase(getClass(), mDb);

        ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());

        DatabaseConfig config = decorate(new DatabaseConfig()
                                         .baseFile(newTempBaseFile(getClass())));

        mDb = Database.restoreFromSnapshot(config, bin);

        assertTrue(mDb.verify(null));

        mDb.close();
    }
}
