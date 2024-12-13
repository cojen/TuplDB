/*
 *  Copyright (C) 2024 Cojen.org
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
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.core;

import java.io.InterruptedIOException;
import java.io.IOException;

import java.util.Queue;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import java.util.function.Supplier;

import org.cojen.tupl.util.Latch;
import org.cojen.tupl.util.LocalPool;

/**
 * Parallel tree copying utility. All entries from the source tree are copied into a new target
 * temporary tree. No threads should be active in the source tree.
 *
 * @author Brian S. O'Neill
 */
final class BTreeCopier extends BTreeSeparator implements Supplier<byte[]> {
    private final int mPageSize;
    private final int mBufferSize;

    private final LocalPool<byte[]> mBufferPool;

    private final Latch mLatch;
    private final Latch.Condition mCondition;

    private BTree mMerged;
    private IOException mException;

    /**
     * @param dest is only used for calling newTemporaryIndex
     * @param executor used for parallel separation; pass null to use only the starting thread
     * @param workerCount maximum parallelism; must be at least 1
     */
    BTreeCopier(LocalDatabase dest, BTree source, Executor executor, int workerCount) {
        super(dest, new BTree[] {source}, executor, workerCount);
        mPageSize = dest.stats().pageSize;
        mBufferSize = Math.max(source.mDatabase.stats().pageSize, mPageSize);
        mBufferPool = new LocalPool<>(this, workerCount);
        mLatch = new Latch();
        mCondition = new Latch.Condition();
    }

    /**
     * Returns a new temporary index with all the results, or null if empty.
     */
    public BTree result() throws IOException {
        mLatch.acquireExclusive();
        try {
            while (true) {
                if (mException != null) {
                    throw mException;
                }
                if (mMerged != null) {
                    return mMerged;
                }
                if (mCondition.await(mLatch) < 0) {
                    stop();
                    throw new InterruptedIOException();
                }
            }

        } finally {
            mLatch.releaseExclusive();
        }
    }

    @Override // Supplier
    public byte[] get() {
        return new byte[mBufferSize];
    }

    @Override
    protected void finished(Chain<BTree> firstRange) {
        BTree merged = firstRange.element();

        if (merged != null) {
            Chain<BTree> range = firstRange.next();

            while (range != null) {
                BTree tree = range.element();

                if (tree != null) {
                    try {
                        merged = BTree.graftTempTree(merged, tree);
                    } catch (IOException e) {
                        mException = e;
                        merged = null;
                        break;
                    }
                }

                range = range.next();
            }
        }

        mMerged = merged;

        mLatch.acquireExclusive();
        mCondition.signalAll(mLatch);
        mLatch.releaseExclusive();
    }

    @Override
    protected Worker newWorker(int spawnCount, byte[] lowKey, byte[] highKey, int numSources) {
        return new Copier(spawnCount, lowKey, highKey, numSources);
    }

    final class Copier extends Worker {
        private Queue<Task> mTasks;

        Copier(int spawnCount, byte[] lowKey, byte[] highKey, int numSources) {
            super(spawnCount, lowKey, highKey, numSources);
        }

        @Override
        protected void transfer(BTreeCursor source, BTreeCursor target) throws IOException {
            target.findNearby(source.key());

            long length = source.valueLength();

            if (length <= mPageSize) {
                source.load();
                target.store(source.value());
            } else {
                target.valueLength(length);
                largeTransfer(source, target, 0, length);

                Queue<Task> tasks = mTasks;
                if (tasks != null) {
                    int total = 0;
                    Task task;
                    while ((task = tasks.poll()) != null) {
                        task.await();
                        total++;
                    }
                    if (total > 0) {
                        // Restore the spawn count such that BTreeSeparator can continue
                        // splitting up the work as intended.
                        cSpawnCountHandle.getAndAdd(this, total);
                    }
                }
            }

            source.next();
        }

        private void largeTransfer(BTreeCursor source, BTreeCursor target, long start, long end)
            throws IOException
        {
            LocalPool.Entry<byte[]> entry = mBufferPool.access();
            byte[] buf = entry.get();

            try {
                while (true) {
                    int length = (int) Math.min(buf.length, (end - start));
                    int amt = source.valueReadToGap(start, buf, 0, length);
                    target.valueWrite(start, buf, 0, amt);
                    start += amt;
                    if (start >= end) {
                        break;
                    }
                    if (amt < length) {
                        // Skipped amount can be past the target end, but this is harmless.
                        long skipped = source.valueSkipGap(start);
                        if (skipped <= 0) {
                            throw new IOException("Value isn't fully copied");
                        }
                        start += skipped;
                        if (start >= end) {
                            break;
                        }
                    }

                    int spawnCount = (int) cSpawnCountHandle.getOpaque(this);

                    if (spawnCount > 0 && (end - start) > buf.length * 8L &&
                        cSpawnCountHandle.compareAndSet(this, spawnCount, spawnCount - 1))
                    {
                        // Split the work with another thread.

                        long mid = start + ((end - start) / 2);
                        long taskEnd = end;
                        end = mid;

                        Queue<Task> tasks = mTasks;
                        if (tasks == null) {
                            mTasks = tasks = new ConcurrentLinkedQueue<>();
                        }

                        var task = new Task() {
                            @Override
                            void doRun() throws IOException {
                                largeTransfer(source, target, mid, taskEnd);
                            }
                        };

                        mExecutor.execute(task);

                        tasks.add(task);
                    }
                }
            } finally {
                entry.release();
            }
        }

        private static abstract class Task extends Latch implements Runnable {
            private final Latch.Condition mCondition = new Latch.Condition();

            private Object mDone;

            public void run() {
                acquireExclusive();
                try {
                    doRun();
                    mDone = true;
                } catch (Throwable e) {
                    mDone = e;
                } finally {
                    mCondition.signalAll(this);
                    releaseExclusive();
                }
            }

            public void await() throws IOException {
                acquireExclusive();
                try {
                    while (mDone == null) {
                        if (mCondition.await(this) < 0) {
                            throw new InterruptedIOException();
                        }
                    }
                    if (mDone instanceof Throwable e) {
                        throw Utils.rethrow(e);
                    }
                } finally {
                    releaseExclusive();
                }
            }

            abstract void doRun() throws IOException;
        }

        @Override
        protected void skip(BTreeCursor source) throws IOException {
            // Nothing should be skipped.
            throw new AssertionError();
        }
    }
}
