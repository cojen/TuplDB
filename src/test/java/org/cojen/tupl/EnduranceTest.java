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

import static org.cojen.tupl.TestUtils.deleteTempDatabases;
import static org.cojen.tupl.TestUtils.newTempDatabase;
import static org.cojen.tupl.TestUtils.randomStr;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class EnduranceTest {
    public static void main(String[] args) {
        org.junit.runner.JUnitCore.main(EnduranceTest.class.getName());
    }

    protected void decorate(DatabaseConfig config) throws Exception {
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
        mDb = null;
    }

    protected Database mDb;
    protected Index mIx;

    /**
     * Regression test for a racecondition loading fragmented values into the cache
     */
    @Test
    public void loadFragmented() throws Exception {

        DatabaseConfig config = new DatabaseConfig()
                .pageSize(1024)
                .directPageAccess(false)
                .durabilityMode(DurabilityMode.NO_REDO)
                .minCacheSize(200 * 1024)
                .maxCacheSize(200 * 1024)
                .checkpointRate(-1, null);

        decorate(config);

        mDb = newTempDatabase(getClass(), config);
        mIx = mDb.openIndex("test");

        // Only use row ids up to this value, then wrap around
        final long maxRows = 50;

        // Seed the database
        {
            ByteBuffer keyBuffer = ByteBuffer.allocate(8);
            // Use 10k values
            ByteBuffer valueBuffer = ByteBuffer.allocate(10 * 1024);
            for (long i = 0; i < maxRows; i++) {
                // The key is exactly 8 bytes long and it is the row id.
                keyBuffer.clear();
                keyBuffer.putLong(i);
                // The first 8 bytes of the value is the row id. The rest of the 10k is 0.
                valueBuffer.clear();
                long j = 0xFF0000;
                while(valueBuffer.remaining() >= 16) {
                    valueBuffer.putLong(i);
                    valueBuffer.putLong(j);
                    j++;
                }
                mIx.store(null, keyBuffer.array(), valueBuffer.array());
                if (i % 10 == 9) {
                    // Checkpoint after writing every 10 rows so that the very small cache
                    // does not fill up.
                    mDb.checkpoint();
                }
            }
            mDb.checkpoint();
        }

        int numWorkers = 10;
        final AtomicBoolean keepRunning = new AtomicBoolean(true);
        // Next row to read
        final AtomicLong nextRow = new AtomicLong(0);

        ExecutorService executor = Executors.newFixedThreadPool(numWorkers);
        ArrayList<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < numWorkers; i++) {
            Runnable readRows = () -> {
                ByteBuffer keyBuffer = ByteBuffer.allocate(8);
                long prevRowId = -1;
                Cursor cursor = mIx.newCursor(null);
                while (keepRunning.get()) {
                    nextRow.compareAndSet(prevRowId, prevRowId + 1);
                    long rowId = nextRow.get();

                    keyBuffer.clear();
                    keyBuffer.putLong(rowId % maxRows);
                    try {
                        cursor.find(keyBuffer.array());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    long keyRowId = ByteBuffer.wrap(cursor.key()).getLong();
                    assertEquals(rowId % maxRows, keyRowId);
                    long valueRowId = ByteBuffer.wrap(cursor.value()).getLong();
                    assertEquals(rowId % maxRows, valueRowId);
                    prevRowId = rowId;
                }
                cursor.reset();
            };
            futures.add(executor.submit(readRows));
        }
        Thread.sleep(10_000);
        keepRunning.set(false);
        for (Future<?> f : futures) {
            f.get();
        }
        executor.shutdown();
    }

    /**
     * Regression test for a deadlock bug between splitting a page and checkpointing.
     */
    @Test
    public void splitAndCheckpoint() throws Exception {

        DatabaseConfig config = new DatabaseConfig()
                .pageSize(1024)
                .directPageAccess(false)
                .durabilityMode(DurabilityMode.NO_REDO)
                .checkpointRate(1, TimeUnit.MILLISECONDS)
                .checkpointDelayThreshold(1, TimeUnit.MILLISECONDS);

        decorate(config);

        mDb = newTempDatabase(getClass(), config);
        mIx = mDb.openIndex("test");

        int numWorkers = 5;
        final AtomicBoolean keepRunning = new AtomicBoolean(true);
        // Next row to insert
        final AtomicLong nextRow = new AtomicLong(0);
        final byte[] rowValue = new byte[512];
        // Only use row ids up to this value, then wrap around
        final long maxRows = 100_000;

        // Creates a set of threads. Each thread grabs the next row to insert (by incrementing
        // nextRow). The thread first inserts that row, then deletes the row as far from the
        // grabbed row as possible.
        //
        // Since all of the threads are inserting keys right next to each other, the page
        // often splits and all those threads fight to finish the split.
        ExecutorService executor = Executors.newFixedThreadPool(numWorkers);
        for (int i = 0; i < numWorkers; i++) {
            Runnable writeRow = () -> {
                ByteBuffer keyBuffer = ByteBuffer.allocate(8);
                while (keepRunning.get()) {
                    long rowId = nextRow.incrementAndGet();

                    // Insert that row
                    keyBuffer.clear();
                    keyBuffer.putLong(rowId % maxRows);
                    try {
                        mIx.store(null, keyBuffer.array(), rowValue);
                    } catch (IOException e) {
                        if (keepRunning.get()) {
                            assertNull(e);
                        }
                    }

                    // If the test only did inserts and stores, eventually all row ids would
                    // be written. Splits only occur when rows get bigger. Overwriting rows
                    // with new values of the same size does not cause splits.
                    //
                    // In order to keep splitting, old rows need to be deleted.
                    keyBuffer.clear();
                    keyBuffer.putLong((rowId + maxRows / 2) % maxRows);
                    try {
                        mIx.delete(null, keyBuffer.array());
                    } catch (IOException e) {
                        if (keepRunning.get()) {
                            assertNull(e);
                        }
                    }
                }
            };
            executor.execute(writeRow);
        }
        Thread.sleep(1_000);
        keepRunning.set(false);
        executor.shutdown();
    }

    @Test
    public void writeAndEvict() throws Exception {
        // Tests interaction between eviction and writing. This test exposed a bug which caused
        // an ArrayIndexOutOfBounds exception to be thrown from the evict method. It was unable
        // to cope with empty leaf nodes.

        DatabaseConfig config = new DatabaseConfig()
            .checkpointRate(-1, null)
            .directPageAccess(false)
            .durabilityMode(DurabilityMode.NO_FLUSH);

        decorate(config);

        mDb = newTempDatabase(getClass(), config);

        for (int trial = 1; trial <= 5; trial++) {
            final Index ix = mDb.openIndex("write_evict");

            Callable<Void> writeSome = () -> {
                final Random rnd = ThreadLocalRandom.current();
                for (int i = 0; i < 100_000; i++) {
                    byte[] key = TestUtils.randomStr(rnd, 10, 1000);
                    byte[] value = TestUtils.randomStr(rnd, 10, 1000);
                    ix.store(null, key, value);
                }
                return null;
            };

            boolean autocommit = true;
            Callable<Long> evictSome = () -> {
                long tot = 0;
                for (int i = 0; i < 100_000; i++) {
                    Transaction txn = null;
                    if (!autocommit) {
                        txn = mDb.newTransaction(DurabilityMode.NO_REDO);
                    }
                    try {
                        tot += ix.evict(txn, null, null, null, true);
                        if (txn != null) {
                            txn.commit();
                        }
                    } finally {
                        if (txn != null) {
                            txn.reset();
                        }
                    }
                }
                return Long.valueOf(tot);
            };

            // Write some initial data.
            writeSome.call();
            ix.analyze(null, null);

            // Write some more data while evicting.
            Future<?> writeJob = ForkJoinPool.commonPool().submit(writeSome);
            evictSome.call();

            try {
                writeJob.get();
            } catch (ExecutionException ee) {
                Utils.rethrow(Utils.rootCause(ee));
            }

            mDb.deleteIndex(ix).run();
            mDb.checkpoint();
        }
    }

    @Test
    public void churn() throws Exception {
        // Stress test which ensures that cursor position doesn't break when concurrent
        // insert/delete operations are making structural tree changes.

        DatabaseConfig config = new DatabaseConfig()
            .pageSize(512)
            .directPageAccess(false);

        decorate(config);

        Database db = Database.open(config);

        Index ix = db.openIndex("test");

        final AtomicReference<Exception> failure = new AtomicReference<>();

        Thread mutator = new Thread(() -> {
            try {
                while (true) {
                    for (int i=0; i<120; i++) {
                        ix.store(Transaction.BOGUS, ("key-" + i).getBytes(),
                                 ("value-" + i).getBytes());
                    }
                    for (int i=0; i<120; i++) {
                        ix.store(Transaction.BOGUS, ("key-" + i).getBytes(), null);
                    }
                }
            } catch (Exception e) {
                failure.set(e);
            }
        });

        mutator.start();

        for (int i=0; i<100_000; i++) {
            Cursor c = ix.newCursor(Transaction.BOGUS);
            try {
                for (c.first(); c.key() != null; c.next()) {
                }
            } catch (Exception e) {
                e.printStackTrace(System.out);
                System.exit(1);
            } finally {
                c.reset();
            }
        }

        assertNull(failure.get());

        // Again in reverse.

        for (int i=0; i<100_000; i++) {
            Cursor c = ix.newCursor(Transaction.BOGUS);
            try {
                for (c.last(); c.key() != null; c.previous()) {
                }
            } catch (Exception e) {
                e.printStackTrace(System.out);
                System.exit(1);
            } finally {
                c.reset();
            }
        }

        assertNull(failure.get());

        db.close();

        mutator.join();
    }

    @Test
    public void ghosts() throws Exception {
        // Runs concurrent transactional inserts and deletes, making sure that the ghost
        // deletion code handles splits correctly.

        DatabaseConfig config = new DatabaseConfig()
            .directPageAccess(false)
            .durabilityMode(DurabilityMode.NO_FLUSH);

        decorate(config);

        mDb = newTempDatabase(getClass(), config);
        mIx = mDb.openIndex("test");

        AtomicBoolean stop = new AtomicBoolean();

        class Task extends Thread {
            @Override
            public void run() {
                Random rnd = new Random();
                try {
                    byte[][] keys = new byte[1000][];

                    while (!stop.get()) {
                        for (int i=0; i<keys.length; i++) {
                            keys[i] = randomStr(rnd, 10);
                            mIx.store(null, keys[i], randomStr(rnd, 10, 100));
                        }

                        Transaction txn = mDb.newTransaction();
                        try {
                            for (int i=0; i<keys.length; i++) {
                                mIx.store(txn, keys[i], null);
                            }
                            txn.commit();
                        } finally {
                            txn.reset();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    System.exit(1);
                }
            }
        }

        Task[] tasks = new Task[2];
        for (int i=0; i<tasks.length; i++) {
            (tasks[i] = new Task()).start();
        }

        Thread.sleep(10_000);

        stop.set(true);

        for (Task t : tasks) {
            t.join();
        }

        assertEquals(0, mIx.count(null, null));
    }

    @Test
    public void testBasic() throws Exception {
        DatabaseConfig config = new DatabaseConfig()
            .pageSize(2048)
            .minCacheSize(1_000_000)
            .maxCacheSize(1_000_000)    // cacheSize ~ 500 nodes
            .durabilityMode(DurabilityMode.NO_FLUSH)
            .directPageAccess(false);

        decorate(config);

        mDb = newTempDatabase(getClass(), config);
        mIx = mDb.openIndex("test");

        int numWorkers = 5;
        // First populate Tupl so we have data exceeding the cache size
        Random random = ThreadLocalRandom.current();
        for (int i=0; i<150_000; i++) {
            byte[] key = randomStr(random, 10, 100);
            byte[] val = randomStr(random, 100, 500);
            mIx.store(null, key, val);
        }
        
        List<Worker> workers = new ArrayList<>(numWorkers);
        ExecutorService executor = Executors.newFixedThreadPool(numWorkers);
        workers.add(new StoreOpWorker(mDb, mIx));
        workers.add(new DeleteOpWorker(mDb, mIx));
        workers.add(new ReadOpWorker(mDb, mIx));
        workers.add(new ReadOpWorker(mDb, mIx));
        workers.add(new EvictOpWorker(mDb, mIx));

        for (Worker w : workers) {
            executor.execute(w);
        }
        Thread.sleep(10_000);
        for (Worker w : workers) {
            w.stop();
        }
        executor.shutdown();

        int numOperations = 0;
        int numFailures = 0;
        for (Worker w : workers) {
            numFailures += w.numFailures();
            numOperations += w.numOperations();
        }
        double failureRatePercentage = (numFailures/numOperations) * 100;
        assertFalse(failureRatePercentage > 0.1);
        // System.out.printf("NumOperations=%d, FailureRate = %.2f%%\n", numOperations, failureRatePercentage);

        VerificationObserver observer = new VerificationObserver();
        mIx.verify(observer);
        assertFalse(observer.failed);
    }

    interface Worker extends Runnable {
        int numOperations();
        int numFailures();
        long duration();
        void stop();
    }

    abstract class AbstractWorker implements Worker {
        private long mStartTimeMillis;
        private long mEndTimeMillis;
        private volatile boolean mRunning;
        private int mNumOperations;
        private int mNumFailures;
        private int mSleepBetweenOpInMillis;
        private int mStartDelayInMillis;

        private Index mIx;
        private Database mDb;

        abstract void executeOperation() throws IOException;

        AbstractWorker(Database db, Index ix) {
            mDb = db;
            mIx = ix;
            mSleepBetweenOpInMillis = 0;
            mStartDelayInMillis = 0;
        }

        AbstractWorker(Database db, Index ix, int sleepBetweenOpInMillis) {
            mDb = db;
            mIx = ix;
            mSleepBetweenOpInMillis = sleepBetweenOpInMillis;
            mStartDelayInMillis = 0;
        }

        AbstractWorker(Database db, Index ix, int startDelayInMillis, int sleepBetweenOpInMillis) {
            mDb = db;
            mIx = ix;
            mStartDelayInMillis = startDelayInMillis;
            mSleepBetweenOpInMillis = sleepBetweenOpInMillis;
        }

        public void run() {
            mStartTimeMillis = System.currentTimeMillis();
            mRunning = true;
            if (mStartDelayInMillis > 0) {
                try {
                    Thread.sleep(mStartDelayInMillis);
                } catch (InterruptedException e) {
                    mRunning = false;
                }
            }
            while (mRunning) {
                try {
                    mNumOperations++;
                    executeOperation();
                } catch (Exception e) {
                    mNumFailures++;
                }
                if (Thread.currentThread().isInterrupted()) {
                    mRunning = false;
                }
                if (mSleepBetweenOpInMillis > 0) {
                    try {
                        Thread.sleep(mSleepBetweenOpInMillis);
                    } catch (InterruptedException e) {
                        mRunning = false;
                    }
                }
            }
        }

        public void stop() {
            mEndTimeMillis = System.currentTimeMillis();
            mRunning = false;
        }

        public long duration() {
            if (mRunning) {
                throw new IllegalStateException();
            }
            return mEndTimeMillis - mStartTimeMillis;
        }

        public int numOperations() {
            return mNumOperations;
        }

        public int numFailures() {
            return mNumFailures;
        }

        public void failed(int num) {
            mNumFailures+=num;
        }

        public Transaction newTransaction() {
            return mDb.newTransaction();
        }

        public Index getIndex() {
            return mIx;
        }
    }

    class StoreOpWorker extends AbstractWorker {
        Random mRandom;
        StoreOpWorker(Database db, Index ix) {
            super(db, ix, 5);
            mRandom = ThreadLocalRandom.current();
        }

        void executeOperation() throws IOException {
            Transaction txn = newTransaction();
            byte[] key = randomStr(mRandom, 10, 100);
            byte[] val = randomStr(mRandom, 100, 500);
            getIndex().store(txn, key, val);
            txn.commit();
        }
    }

    class DeleteOpWorker extends AbstractWorker {
        Random mRandom;

        DeleteOpWorker(Database db, Index ix) {
            super(db, ix, 10);
            mRandom = ThreadLocalRandom.current();
        }

        void executeOperation() throws IOException {
            Transaction txn = newTransaction();
            byte[] key = randomStr(mRandom, 10, 100);
            getIndex().delete(txn, key);
            txn.commit();
        }
    }

    class ReadOpWorker extends AbstractWorker {
        Random mRandom;

        ReadOpWorker(Database db, Index ix) {
            super(db, ix, 1);
            mRandom = ThreadLocalRandom.current();
        }

        void executeOperation() throws IOException {
            Transaction txn = newTransaction();
            byte[] key = randomStr(mRandom, 10, 100);
            getIndex().load(txn, key);
            txn.commit();
        }
    }

    class EvictOpWorker extends AbstractWorker {
        
        EvictOpWorker(Database db, Index ix) {
            super(db, ix, 25);
        }

        void executeOperation() throws IOException {
            Transaction txn = newTransaction();
            if (getIndex().evict(txn, null, null, null, false) == 0) {
                failed(1);
            }
            txn.commit();
        }
    }
}
