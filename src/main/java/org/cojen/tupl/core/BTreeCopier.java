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

import java.util.concurrent.Executor;

import org.cojen.tupl.Index;

import org.cojen.tupl.util.Latch;

/**
 * Parallel tree copying utility. All entries from the source tree are copied into a new target
 * temporary tree. No threads should be active in the source tree.
 *
 * @author Brian S. O'Neill
 */
/*P*/
final class BTreeCopier extends BTreeSeparator {
    private final int mPageSize;

    private final Latch mLatch;
    private final Latch.Condition mCondition;

    private byte[] mBuf;

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
        mLatch = new Latch();
        mCondition = new Latch.Condition();
    }

    /**
     * Returns a new temporary index with all the results, or null if empty.
     */
    BTree result() throws IOException {
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
                    throw new InterruptedIOException();
                }
            }

        } finally {
            mLatch.releaseExclusive();
        }
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
    protected void transfer(BTreeCursor source, BTreeCursor target) throws IOException {
        target.findNearby(source.key());

        long length = source.valueLength();

        if (length <= mPageSize) {
            source.load();
            target.store(source.value());
        } else {
            byte[] buf = mBuf;

            if (buf == null) {
                mBuf = buf = new byte[Math.max(source.mTree.mDatabase.stats().pageSize, mPageSize)];
            }

            target.valueLength(length);

            long pos = 0;
            while (true) {
                int amt = source.valueRead(pos, buf, 0, buf.length);
                target.valueWrite(pos, buf, 0, amt);
                pos += amt;
                if (amt < buf.length) {
                    break;
                }
            }

            if (pos != length) {
                throw new AssertionError("Value isn't fully copied");
            }
        }

        source.next();
    }

    @Override
    protected void skip(BTreeCursor source) throws IOException {
        source.next();
    }
}
