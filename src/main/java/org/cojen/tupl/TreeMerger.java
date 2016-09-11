/*
 *  Copyright 2016 Cojen.org
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

import java.io.InterruptedIOException;
import java.io.IOException;

import java.util.Arrays;

import java.util.concurrent.Executor;

import java.util.function.BiConsumer;

/**
 * Parallel tree merging utility.
 *
 * @author Brian S O'Neill
 */
/*P*/
class TreeMerger {
    private final LocalDatabase mDatabase;
    private final BiConsumer<TreeMerger, Tree> mConsumer;
    private final Tree[] mSources;
    private final Worker[] mWorkers;

    private Tree mTarget;
    private int mActiveCount;
    private int mStoppedCount;

    /**
     * @param workerCount maximum parallelism; must be at least 1
     * @param consumer receives the target tree, plus any source trees if aborted, and then
     * null when merger is finished
     */
    TreeMerger(LocalDatabase db, int workerCount, Tree[] sources,
               BiConsumer<TreeMerger, Tree> consumer)
    {
        if (db == null || workerCount <= 0 || sources.length <= 0 || consumer == null) {
            throw new IllegalArgumentException();
        }
        mDatabase = db;
        mSources = sources;
        mConsumer = consumer;
        mWorkers = new Worker[workerCount];
    }

    /**
     * Merges the sources into a new temporary tree. No other threads can be acting on the
     * sources, which shrink during the merge. Unless the merge is aborted, the sources are
     * fully dropped when the merge finishes.
     *
     * @param executor used for parallel merging; pass null to use only the calling thread
     */
    void start(Executor executor) throws IOException {
        int workerCount = mWorkers.length;
        byte[][] partitions = selectPartitions(workerCount, mSources);

        Tree target = mDatabase.newTemporaryIndex();
        if (partitions == null) {
            workerCount = 1;
        } else {
            target.prepareForMerge(partitions);
            workerCount = Math.min(partitions.length + 1, workerCount);
        }

        try {
            for (int i=0; i<workerCount; i++) {
                TreeCursor tcursor = target.newCursor(Transaction.BOGUS);

                if (partitions != null && i > 0) {
                    tcursor.find(partitions[i - 1]);
                    tcursor.mLeaf.mNodePos = 0;
                } else {
                    tcursor.firstAny();
                }

                Worker w = new Worker(tcursor, mSources.length);
                mWorkers[i] = w;

                for (int j=0; j<mSources.length; j++) {
                    Tree source = mSources[j];

                    TreeCursor cursor = source.newCursor(Transaction.BOGUS);
                    cursor.autoload(false);
                    byte[] upperBound = null;

                    if (partitions == null) {
                        cursor.first();
                    } else {
                        if (i > 0) {
                            cursor.findGe(partitions[i - 1]);
                        } else {
                            cursor.first();
                        }
                        if (i < partitions.length) {
                            upperBound = partitions[i];
                        }
                    }

                    byte[] key = cursor.key();

                    if (key != null) {
                        if (upperBound == null || Utils.compareUnsigned(key, upperBound) < 0) {
                            w.add(new Selector(j, cursor, upperBound));
                        } else {
                            cursor.reset();
                        }
                    }
                }
            }

            synchronized (this) {
                mTarget = target;
                for (int i=0; i<workerCount; i++) {
                    Worker w = mWorkers[i];
                    if (executor == null) {
                        w.run();
                    } else {
                        executor.execute(w);
                    }
                }
                mActiveCount = workerCount;
            }
        } catch (Throwable e) {
            stop();

            try {
                mDatabase.deleteIndex(target).run();
            } catch (IOException e2) {
                // Ignore.
            }

            throw e;
        }
    }

    /**
     * Attempt to stop the merge. The remaining source trees are passed to the consumer,
     * immediately after the target tree.
     */
    void stop() {
        for (Worker w : mWorkers) {
            if (w == null) {
                break;
            }
            w.mStop = true;
        }
    }

    /**
     * Returns the first exception supressed by any worker, if any.
     */
    Throwable exceptionCheck() {
        for (Worker w : mWorkers) {
            if (w == null) {
                break;
            }
            Throwable ex = w.mException;
            if (ex != null) {
                return ex;
            }
        }
        return null;
    }

    /**
     * @param stopped 1 if stopped, 0 if fully finished
     */
    private synchronized void workerFinished(int stopped) throws IOException {
        mStoppedCount += stopped;

        if (--mActiveCount <= 0) {
            mConsumer.accept(this, mTarget);

            if (mStoppedCount == 0) {
                for (Tree source : mSources) {
                    mDatabase.quickDeleteTemporaryTree(source);
                }
            } else {
                for (Tree source : mSources) {
                    mConsumer.accept(this, source);
                }
            }

            mConsumer.accept(this, null);
        }
    }

    /**
     * @return keys to separate thread activity
     */
    private static byte[][] selectPartitions(int threadCount, Tree... sources) throws IOException {
        if (threadCount <= 1) {
            return null;
        }

        // Get a sampling of keys for determining the partitions.

        int minCount = Math.max(1000, threadCount * 10);
        int samplesPerSource = (minCount + sources.length - 1) / sources.length;
        byte[][] samples = new byte[sources.length * samplesPerSource][];

        for (int i=0; i<sources.length; i++) {
            Cursor c = sources[i].newCursor(Transaction.BOGUS);
            try {
                c.autoload(false);
                for (int j=0; j<samplesPerSource; j++) {
                    c.random(null, null);
                    samples[i * samplesPerSource + j] = c.key();
                }
            } finally {
                c.reset();
            }
        }

        Arrays.sort(samples, KeyComparator.THE);

        byte[][] partitions = new byte[threadCount - 1][];

        for (int i=0; i<partitions.length; i++) {
            int pos = (samples.length * (i + 1)) / (partitions.length + 1);
            partitions[i] = samples[pos];
        }

        // Eliminate duplicate partitions.
        if (partitions.length > 1) {
            byte[] last = partitions[0];
            for (int i=1; i<partitions.length; i++) {
                byte[] partition = partitions[i];
                if (!Arrays.equals(partition, last)) {
                    last = partition;
                } else {
                    int pos = i;
                    for (; i<partitions.length; i++) {
                        partition = partitions[i];
                        if (!Arrays.equals(partition, last)) {
                            partitions[pos++] = partition;
                            last = partition;
                        }
                    }
                    partitions = Arrays.copyOf(partitions, pos);
                    break;
                }
            }
        }

        // Trim the partitions for suffix compression. Find the lowest common length where all
        // keys differ from each other. Only works with ordered partitions, which they are.

        if (partitions.length > 1) {
            int len = 0;
            outer: while (true) {
                for (int i=0; i<partitions.length-1; i++) {
                    byte[] a = partitions[i];
                    byte[] b = partitions[i + 1];
                    int compare = Utils.compareUnsigned
                        (a, 0, Math.min(len, a.length), b, 0, Math.min(len, b.length));
                    if (compare == 0) {
                        len++;
                        continue outer;
                    }
                }
                break;
            }

            for (int i=0; i<partitions.length; i++) {
                byte[] p = partitions[i];
                if (p.length > len) {
                    partitions[i] = Arrays.copyOfRange(p, 0, len);
                }
            }
        }

        return partitions;
    }

    private class Worker implements Runnable {
        final TreeCursor mTarget;
        private final Selector[] mQueue;
        private int mQueueSize;
        volatile Throwable mException;
        volatile boolean mStop;

        Worker(TreeCursor target, int capacity) {
            mTarget = target;
            mQueue = new Selector[capacity];
        }

        void add(Selector selector) {
            mQueue[mQueueSize++] = selector;
        }

        @Override
        public void run() {
            try {
                int size = mQueueSize;

                if (size == 0) {
                    finished(0);
                    return;
                }

                final TreeCursor target = mTarget;
                final Selector[] queue = mQueue;

                // Heapify.
                for (int i=size >>> 1; --i>=0; ) {
                    siftDown(queue, size, i, queue[i]);
                }

                while (true) {
                    if (mStop) {
                        finished(1);
                        return;
                    }

                    Selector selector = queue[0];
                    TreeCursor source = selector.mSource;

                    if (selector.mSkip) {
                        source.store(null);
                        source.next();
                        selector.mSkip = false;
                    } else {
                        target.appendTransfer(source);
                    }

                    doneCheck: {
                        byte[] key = source.key();

                        if (key != null) {
                            byte[] upperBound = selector.mUpperBound;
                            if (upperBound == null || Utils.compareUnsigned(key, upperBound) < 0) {
                                // Selector not done yet.
                                break doneCheck;
                            }
                            source.reset();
                        }

                        if (--size == 0) {
                            finished(0);
                            return;
                        }

                        // Sift in the last selector.
                        selector = queue[size];
                    }

                    // Fix the heap.
                    siftDown(queue, size, 0, selector);
                }
            } catch (Throwable e) {
                mException = e;
                finished(1);
            }
        }

        /**
         * @param stopped 1 if stopped, 0 if fully finished
         */
        private void finished(int stopped) {
            mTarget.reset();

            while (mQueueSize > 0) {
                mQueue[--mQueueSize].mSource.reset();
            }

            try {
                workerFinished(stopped);
            } catch (Throwable e) {
                if (mException == null) {
                    mException = e;
                }
            }
        }
    }

    private static class Selector implements Comparable<Selector> {
        final int mOrder;
        final TreeCursor mSource;
        final byte[] mUpperBound;

        boolean mSkip;

        Selector(int order, TreeCursor source, byte[] upperBound) throws IOException {
            mOrder = order;
            mSource = source;
            mUpperBound = upperBound;
        }

        @Override
        public int compareTo(Selector other) {
            int compare = Utils.compareUnsigned(this.mSource.key(), other.mSource.key());
            if (compare == 0) {
                // Favor the later source when duplicates are found.
                if (this.mOrder < other.mOrder) {
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

    private static void siftDown(Selector[] selectors, int size, int pos, Selector element)
        throws IOException
    {
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
