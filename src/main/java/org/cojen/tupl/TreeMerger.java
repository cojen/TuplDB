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
 * Parallel tree merging utility.
 *
 * @author Brian S O'Neill
 */
/*P*/
class TreeMerger {
    /**
     * Merges the given sources into a new temporary tree. No other threads can be acting on
     * the sources, which shrink during the merge. The sources are fully dropped when the merge
     * finishes.
     */
    static Tree merge(ExecutorService executor, LocalDatabase db, int threadCount, Tree... sources)
        throws IOException
    {
        if (executor == null || db == null || threadCount <= 0 || sources.length <= 0) {
            throw new IllegalArgumentException();
        }

        byte[][] partitions = selectPartitions(threadCount, sources);

        Tree target = db.newTemporaryIndex();
        if (partitions != null) {
            target.prepareForMerge(partitions);
        }

        Worker[] workers = new Worker[threadCount];

        try {
            for (int i=0; i<threadCount; i++) {
                TreeCursor tcursor = target.newCursor(Transaction.BOGUS);

                if (partitions != null && i > 0) {
                    tcursor.find(partitions[i - 1]);
                    tcursor.mLeaf.mNodePos = 0;
                } else {
                    tcursor.firstAny();
                }

                Worker w = new Worker(tcursor);
                workers[i] = w;

                for (int j=0; j<sources.length; j++) {
                    Tree source = sources[j];

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

            Future[] results = new Future[threadCount];

            for (int i=0; i<threadCount; i++) {
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

            for (Tree source : sources) {
                db.deleteIndex(source).run();
            }

            return target;
        } catch (Throwable e) {
            try {
                db.deleteIndex(target).run();
            } catch (IOException e2) {
                // Ignore.
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

        // FIXME: Skip duplicates! If too few remaining, reduce partition count. Caller must
        // reduce thread count.
        Arrays.sort(samples, KeyComparator.THE);

        byte[][] partitions = new byte[threadCount - 1][];

        for (int i=0; i<partitions.length; i++) {
            int pos = (samples.length * (i + 1)) / (partitions.length + 1);
            partitions[i] = samples[pos];
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

        for (byte[] p : partitions) {
            System.out.println(Utils.toHex(p));
        }
        System.out.println("---");

        return partitions;
    }

    private static class Worker implements Callable<Object>, Closeable {
        private final TreeCursor mTarget;
        private final PriorityQueue<Selector> mQueue;

        Worker(TreeCursor target) {
            mTarget = target;
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
                    } else {
                        source.reset();
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
