/*
 *  Copyright (C) 2018 Cojen.org
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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.io.IOException;

import java.util.Arrays;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;

import java.util.concurrent.atomic.LongAdder;

/**
 * Parallel tree separating utility. All entries from the source trees are separated into new
 * target trees, with no overlapping key ranges.
 *
 * @author Brian S O'Neill
 */
/*P*/
abstract class TreeSeparator extends LongAdder {
    protected final LocalDatabase mDatabase;
    protected final Tree[] mSources;
    protected final Executor mExecutor;

    private final int mWorkerCount;
    private final Worker[] mWorkerHashtable;

    // Linked list of workers, ordered by the range of keys they act upon.
    private Worker mFirstWorker;

    private volatile Throwable mException;

    static final VarHandle cExceptionHandle, cSpawnCountHandle;

    static {
        try {
            cExceptionHandle =
                MethodHandles.lookup().findVarHandle
                (TreeSeparator.class, "mException", Throwable.class);

            cSpawnCountHandle =
                MethodHandles.lookup().findVarHandle
                (Worker.class, "mSpawnCount", int.class);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    /**
     * @param executor used for parallel separation; pass null to use only the starting thread
     * @param workerCount maximum parallelism; must be at least 1
     */
    TreeSeparator(LocalDatabase db, Tree[] sources, Executor executor, int workerCount) {
        if (db == null || sources.length <= 0 || workerCount <= 0) {
            throw new IllegalArgumentException();
        }
        if (executor == null) {
            workerCount = 1;
        }
        mDatabase = db;
        mSources = sources;
        mExecutor = executor;
        mWorkerCount = workerCount;
        mWorkerHashtable = new Worker[Utils.roundUpPower2(workerCount)];
    }

    /**
     * Separates the sources into new temporary trees. No other threads can be acting on the
     * sources, which shrink during the separation.
     */
    public void start() {
        startWorker(null, mWorkerCount - 1, null, null);
    }

    /**
     * Attempt to stop the separation early.
     */
    public void stop() {
        Worker[] hashtable = mWorkerHashtable;

        synchronized (hashtable) {
            for (int slot = 0; slot < hashtable.length; slot++) {
                for (Worker w = hashtable[slot]; w != null; ) {
                    // Signal to stop by setting the sign bit.
                    while (true) {
                        int spawnCount = w.mSpawnCount;
                        if (cSpawnCountHandle.compareAndSet
                            (w, spawnCount, spawnCount | (1 << 31)))
                        {
                            break;
                        }
                    }

                    w = w.mHashtableNext;
                }
            }
        }
    }

    /**
     * Returns the first exception supressed by any worker, if any.
     */
    public Throwable exceptionCheck() {
        return mException;
    }

    protected void failed(Throwable cause) {
        cExceptionHandle.compareAndSet(this, null, cause);
        stop();
    }

    /**
     * Called when separation has finished. When finished normally (not stopped), then all
     * source trees are empty, but not deleted.
     *
     * @param firstRange first separated range; the ranges are ordered lowest to highest.
     */
    protected abstract void finished(Chain<Tree> firstRange);

    private void startWorker(Worker from, int spawnCount, byte[] lowKey, byte[] highKey) {
        Worker worker = new Worker(spawnCount, lowKey, highKey, mSources.length);
 
        Worker[] hashtable = mWorkerHashtable;
        int slot = worker.mHash & (hashtable.length - 1);
 
        synchronized (hashtable) {
            if (from != null && from.mSpawnCount < 0) {
                // Propagate the stop signal.
                worker.mSpawnCount = spawnCount | (1 << 31);
            }
            worker.mHashtableNext = hashtable[slot];
            hashtable[slot] = worker;

            if (from == null) {
                mFirstWorker = worker;
            } else {
                worker.mPrev = from;
                Worker next = from.mNext;
                from.mNext = worker;
                if (next != null) {
                    worker.mNext = next;
                    next.mPrev = worker;
                }
            }
        }

        if (mExecutor == null) {
            worker.run();
        } else {
            mExecutor.execute(worker);
        }
    }

    /**
     * @param lowKey inclusive lowest key in the worker range; pass null for open range
     */
    private TreeCursor openSourceCursor(int sourceSlot, byte[] lowKey) throws IOException {
        TreeCursor scursor = mSources[sourceSlot].newCursor(Transaction.BOGUS);
        scursor.mKeyOnly = true;
        if (lowKey == null) {
            scursor.first();
        } else {
            scursor.findGe(lowKey);
        }
        return scursor;
    }

    /**
     * @param lowKey inclusive lowest key in the random range; pass null for open range
     * @param highKey exclusive highest key in the random range; pass null for open range
     * @return null if no key was found
     */
    private byte[] selectSplitKey(byte[] lowKey, byte[] highKey) throws IOException {
        // Select a random key from a random source.

        Tree source = mSources[ThreadLocalRandom.current().nextInt(mSources.length)];

        TreeCursor scursor = source.newCursor(Transaction.BOGUS);
        try {
            scursor.mKeyOnly = true;
            scursor.random(lowKey, highKey);
            return scursor.key();
        } finally {
            scursor.reset();
        }
    }

    private void workerFinished(Worker worker) {
        Worker first;
        Worker[] hashtable = mWorkerHashtable;
        int slot = worker.mHash & (hashtable.length - 1);

        synchronized (hashtable) {
            // Remove the worker from the hashtable, which is expected to be in it.
            for (Worker w = hashtable[slot], prev = null;;) {
                Worker next = w.mHashtableNext;
                if (w == worker) {
                    if (prev == null) {
                        hashtable[slot] = next;
                    } else {
                        prev.mHashtableNext = next;
                    }
                    break;
                } else {
                    prev = w;
                    w = next;
                }
            }

            // Amount of workers to spawn. Add the removed worker's spawn count, which might
            // have increased by another worker which just finished. Ignore the sign bit for
            // now, which is the signal to stop working.
            int addCount = 1 + (worker.mSpawnCount & ~(1 << 31));
 
            if (addCount < mWorkerCount) {
                // More work to do, so randomly select a worker and force it to spawn more.
                int randomSlot = ThreadLocalRandom.current().nextInt(hashtable.length);
                while (true) {
                    Worker w = hashtable[randomSlot];
                    if (w != null) {
                        cSpawnCountHandle.getAndAdd(w, addCount);
                        return;
                    }
                    // Slot is empty, so keep looking.
                    randomSlot++;
                    if (randomSlot >= hashtable.length) {
                        randomSlot = 0;
                    }
                }
            }

            first = mFirstWorker;
            mFirstWorker = null;
        }
 
        finished(first);
    }

    private final class Worker implements Runnable, Chain<Tree> {
        final int mHash;
        final byte[] mLowKey;
        byte[] mHighKey;
        final Selector[] mQueue;
        volatile int mSpawnCount;
        Worker mHashtableNext;
        private Tree mTarget;

        // Linked list of workers, ordered by the range of keys they act upon.
        Worker mNext;
        Worker mPrev;

        /**
         * @param lowKey inclusive lowest key in the worker range; pass null for open range
         * @param highKey exclusive highest key in the worker range; pass null for open range
         * @param numSources total number of source trees
         */
        Worker(int spawnCount, byte[] lowKey, byte[] highKey, int numSources) {
            mHash = ThreadLocalRandom.current().nextInt();
            mLowKey = lowKey;
            mHighKey = highKey;
            mQueue = new Selector[numSources];
            mSpawnCount = spawnCount;
        }

        @Override
        public void run() {
            try {
                doRun();
            } catch (Throwable e) {
                for (Selector s : mQueue) {
                    if (s != null) {
                        s.mSource.reset();
                    }
                }
                failed(e);
            }

            workerFinished(this);
        }

        @Override
        public Tree element() {
            return mTarget;
        }

        @Override
        public Worker next() {
            return mNext;
        }

        private void doRun() throws Exception {
            final Selector[] queue = mQueue;

            int queueSize = 0;
            for (int slot = 0; slot < queue.length; slot++) {
                TreeCursor scursor = openSourceCursor(slot, mLowKey);
                if (scursor.key() != null) {
                    queue[queueSize++] = new Selector(slot, scursor);
                }
            }

            if (queueSize == 0) {
                return;
            }

            // Heapify.
            for (int i=queueSize >>> 1; --i>=0; ) {
                siftDown(queue, queueSize, i, queue[i]);
            }

            TreeCursor tcursor = null;
            byte[] highKey = mHighKey;
            byte count = 0;

            while (true) {
                Selector selector = queue[0];
                TreeCursor scursor = selector.mSource;

                transfer: {
                    if (highKey != null && Utils.compareUnsigned(scursor.key(), highKey) >= 0) {
                        scursor.reset();
                    } else {
                        if (selector.mSkip) {
                            scursor.store(null);
                            scursor.next();
                            selector.mSkip = false;
                        } else {
                            if (tcursor == null) {
                                mTarget = mDatabase.newTemporaryIndex();
                                tcursor = mTarget.newCursor(Transaction.BOGUS);
                                tcursor.mKeyOnly = true;
                                tcursor.firstLeaf();
                            }
                            tcursor.appendTransfer(scursor);
                            if (++count == 0) {
                                // Inherited from LongAdder.
                                add(256);
                            }
                        }
                        if (scursor.key() != null) {
                            break transfer;
                        }
                    }

                    if (--queueSize == 0) {
                        break;
                    }

                    // Sift in the last selector.
                    selector = queue[queueSize];
                    queue[queueSize] = null;
                }

                // Fix the heap.
                siftDown(queue, queueSize, 0, selector);

                int spawnCount = mSpawnCount;
                if (spawnCount != 0) {
                    if (spawnCount < 0) {
                        // Signalled to stop.
                        mHighKey = scursor.key();
                        for (int i=0; i<queueSize; i++) {
                            queue[i].mSource.reset();
                        }
                        break;
                    }

                    // Split the work with another worker.

                    byte[] splitKey = selectSplitKey(queue[0].mSource.key(), highKey);
                    trySplit: if (splitKey != null) {
                        // Don't split on keys currently being processed, since it interferes
                        // with duplicate detection.
                        for (int i=0; i<queueSize; i++) {
                            if (Arrays.equals(splitKey, queue[i].mSource.key())) {
                                break trySplit;
                            }
                        }

                        startWorker(this, 0, splitKey, highKey);
                        mHighKey = highKey = splitKey;
                        cSpawnCountHandle.getAndAdd(this, -1);
                    }
                }
            }

            if (tcursor != null) {
                tcursor.reset();
            }

            add(count & 0xffL);
        }
    }

    private static final class Selector {
        final int mSourceSlot;
        final TreeCursor mSource;

        boolean mSkip;

        Selector(int slot, TreeCursor source) {
            mSourceSlot = slot;
            mSource = source;
        }

        int compareTo(Selector other) {
            int compare = Utils.compareUnsigned(this.mSource.key(), other.mSource.key());

            if (compare == 0) {
                // Favor the later source when duplicates are found.
                if (this.mSourceSlot < other.mSourceSlot) {
                    this.mSkip = true;
                    compare = -1;
                } else {
                    other.mSkip = true;
                    compare = 1;
                }
            }

            return compare;
        }
    }

    static void siftDown(Selector[] selectors, int size, int pos, Selector element) {
        int half = size >>> 1;
        while (pos < half) {
            int childPos = (pos << 1) + 1;
            Selector child = selectors[childPos];
            int rightPos = childPos + 1;
            if (rightPos < size && child.compareTo(selectors[rightPos]) > 0) {
                childPos = rightPos;
                child = selectors[childPos];
            }
            if (element.compareTo(child) <= 0) {
                break;
            }
            selectors[pos] = child;
            pos = childPos;
        }
        selectors[pos] = element;
    }
}
