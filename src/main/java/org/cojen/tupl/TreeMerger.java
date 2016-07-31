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

import java.io.Closeable;
import java.io.IOException;

import java.util.Arrays;
import java.util.PriorityQueue;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class TreeMerger {
    private final LocalDatabase mDatabase;
    private final int mThreadCount;
    private final Tree[] mSources;

    TreeMerger(LocalDatabase db, int threadCount, Tree... sources) {
        if (db == null || threadCount <= 0 || sources.length <= 0) {
            throw new IllegalArgumentException();
        }
        mDatabase = db;
        mThreadCount = threadCount;
        mSources = sources;
    }

    Tree merge() throws IOException {
        ExecutorService executor = Executors.newFixedThreadPool(mThreadCount);
        try {
            return merge(executor);
        } finally {
            executor.shutdown();
        }
    }

    Tree merge(ExecutorService executor) throws IOException {
        if (executor == null) {
            throw new IllegalArgumentException();
        }

        byte[][] partitions = selectPartitions();

        Worker[] workers = new Worker[mThreadCount];
        Tree[] targets = new Tree[mThreadCount];

        try {
            for (int i=0; i<mThreadCount; i++) {
                targets[i] = mDatabase.newTemporaryIndex();
                Worker w = new Worker(targets[i]);
                workers[i] = w;

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

                    w.add(new Selector(j, cursor, upperBound));
                }
            }

            Future[] results = new Future[mThreadCount];

            long start = System.currentTimeMillis();

            for (int i=0; i<mThreadCount; i++) {
                Worker w = workers[i];
                results[i] = executor.submit(w);
            }

            for (Future result : results) {
                try {
                    result.get();
                } catch (ExecutionException e) {
                    Utils.rethrow(e.getCause());
                } catch (Exception e) {
                    Utils.rethrow(e);
                }
            }

            long end = System.currentTimeMillis();
            System.out.println("duration: " + (end - start) / 1000.0);

            // FIXME: stitch the targets together

            long total = 0;
            for (Index target : targets) {
                System.out.println(target);
                long count = target.count(null, null);
                System.out.println(count);
                total += count;
            }

            System.out.println("total: " + total);

            // FIXME
            return null;
        } catch (Throwable e) {
            e.printStackTrace(System.out);

            for (Index target : targets) {
                try {
                    mDatabase.deleteIndex(target).run();
                } catch (IOException e2) {
                    // Ignore.
                }
            }
            throw e;
        } finally {
            for (Worker w : workers) {
                if (w != null) {
                    w.close();
                }
            }
        }
    }

    /**
     * @return keys to separate thread activity
     */
    private byte[][] selectPartitions() throws IOException {
        if (mThreadCount <= 1) {
            return null;
        }

        // Get a sampling of keys for determining the partitions.

        int minCount = Math.max(1000, mThreadCount * 10);
        int samplesPerSource = (minCount + mSources.length - 1) / mSources.length;
        byte[][] samples = new byte[mSources.length * samplesPerSource][];

        for (int i=0; i<mSources.length; i++) {
            Cursor c = mSources[i].newCursor(Transaction.BOGUS);
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

        byte[][] partitions = new byte[mThreadCount - 1][];

        for (int i=0; i<partitions.length; i++) {
            int pos = (samples.length * (i + 1)) / (partitions.length + 1);
            partitions[i] = samples[pos];
        }

        return partitions;
    }

    private class Worker implements Callable<Object>, Closeable {
        private final TreeCursor mTarget;
        private final PriorityQueue<Selector> mQueue;

        Worker(Tree target) throws IOException {
            mTarget = target.newAppendCursor();
            mQueue = new PriorityQueue<Selector>();
        }

        void add(Selector s) {
            mQueue.add(s);
        }

        @Override
        public Object call() throws IOException {
            final TreeCursor target = mTarget;
            final PriorityQueue<Selector> queue = mQueue;

            Selector selector;
            while ((selector = queue.poll()) != null) {
                TreeCursor source = selector.mSource;

                // FIXME: needs dup detection; or can selector can skip them?
                target.appendTransfer(source);

                byte[] key = source.key();

                if (key != null) {
                    byte[] upperBound = selector.mUpperBound;
                    if (upperBound == null || Utils.compareUnsigned(key, upperBound) < 0) {
                        queue.add(selector);
                    }
                }
            }

            return null;
        }

        @Override
        public void close() {
            mTarget.reset();

            Selector selector;
            while ((selector = mQueue.poll()) != null) {
                selector.mSource.reset();
            }
        }
    }

    private static class Selector implements Comparable<Selector> {
        final int mOrder;
        final TreeCursor mSource;
        final byte[] mUpperBound;

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
                compare = this.mOrder < other.mOrder ? 1 : -1;
            }
            return compare;
        }
    }
}
