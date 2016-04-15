/*
 *  Copyright 2015-2015 Cojen.org
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

import static org.cojen.tupl.TestUtils.deleteTempDatabases;
import static org.cojen.tupl.TestUtils.newTempDatabase;
import static org.cojen.tupl.TestUtils.randomStr;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

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
        deleteTempDatabases();
        mDb = null;
    }

    protected Database mDb;
    protected Index mIx;

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

        mDb = newTempDatabase(config);

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
    public void testBasic() throws Exception {
        DatabaseConfig config = new DatabaseConfig()
            .pageSize(2048)
            .minCacheSize(1_000_000)
            .maxCacheSize(1_000_000)    // cacheSize ~ 500 nodes
            .durabilityMode(DurabilityMode.NO_FLUSH)
            .directPageAccess(false);

        decorate(config);

        mDb = newTempDatabase(config);
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
