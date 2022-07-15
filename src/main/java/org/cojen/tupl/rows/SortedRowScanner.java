/*
 *  Copyright (C) 2022 Cojen.org
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

package org.cojen.tupl.rows;

import java.io.IOException;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;

import org.cojen.tupl.RowScanner;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Sorter;
import org.cojen.tupl.Transaction;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class SortedRowScanner<R> extends ScanBatch<R> implements BaseRowScanner<R>, RowConsumer<R> {
    // FIXME: Make configrable and/or "smart".
    private static int BIG_THRESHOLD = 1_000_000;

    private ScanBatch<R> mFirstBatch, mLastBatch;

    private Comparator<R> mComparator;

    private RowScanner<R> mScanner;

    @SuppressWarnings("unchecked")
    SortedRowScanner(BaseTable<R> table, String orderBySpec, Comparator<R> comparator,
                     SingleScanController<R> controller, Transaction txn)
        throws IOException
    {
        init(table, orderBySpec, comparator, null,
             // Pass `this` as if it's a row, but it's actually a RowConsumer.
             table.newRowScanner(txn, (R) this, table.unfiltered()), txn);
    }

    @SuppressWarnings("unchecked")
    SortedRowScanner(BaseTable<R> table, String orderBySpec, Comparator<R> comparator,
                     QueryLauncher<R> launcher, Transaction txn, Object... args)
        throws IOException
    {
        init(table, orderBySpec, comparator, launcher,
             // Pass `this` as if it's a row, but it's actually a RowConsumer.
             launcher.newRowScanner(txn, (R) this, args), txn, args);
    }

    @SuppressWarnings("unchecked")
    private void init(BaseTable<R> table, String orderBySpec, Comparator<R> comparator,
                      QueryLauncher<R> launcher, RowScanner source,
                      Transaction txn, Object... args)
        throws IOException
    {
        mComparator = comparator;

        int numRows = 0;
        for (Object c = source.row(); c != null; c = source.step(c)) {
            if (++numRows >= BIG_THRESHOLD) {
                mScanner = new BigSort(table, orderBySpec, launcher).finish(source);
                return;
            }
        }

        if (numRows == 0) {
            mScanner = new ArrayRowScanner<>();
            return;
        }

        var rows = (R[]) new Object[numRows];

        if (mFirstBatch != null) {
            mFirstBatch.decodeAllRows(rows, 0);
        }

        mFirstBatch = null;
        mLastBatch = null;

        // FIXME: This can be a problem if the projection eliminates any necessary sort
        // columns. Will need to transcode always in this case.
        Arrays.parallelSort(rows, mComparator);

        mScanner = new ArrayRowScanner<>(table, rows);
    }

    @Override
    public R row() {
        return mScanner.row();
    }

    @Override
    public R step() throws IOException {
        return mScanner.step();
    }

    @Override
    public R step(R dst) throws IOException {
        return mScanner.step(dst);
    }

    @Override
    public void close() throws IOException {
        mScanner.close();
    }

    @Override
    public long estimateSize() {
        return mScanner.estimateSize();
    }

    @Override
    public int characteristics() {
        // FIXME: Depending on the projection, DISTINCT shouldn't be included.
        int c = ORDERED | DISTINCT | SORTED | NONNULL | IMMUTABLE;
        if (mScanner instanceof ArrayRowScanner) {
            c |= SIZED;
        }
        return c;
    }

    @Override
    public Comparator<R> getComparator() {
        return mComparator;
    }

    @Override
    public void beginBatch(RowEvaluator<R> evaluator) {
        ScanBatch<R> batch;
        if (mLastBatch == null) {
            mFirstBatch = batch = this;
        } else {
            batch = new ScanBatch<R>();
            mLastBatch.mNextBatch = batch;
        }
        mLastBatch = batch;
        batch.mEvaluator = evaluator;
    }

    @Override
    public void accept(byte[] key, byte[] value) throws IOException {
        mLastBatch.addEntry(key, value);
    }

    private class BigSort implements RowConsumer<R> {
        private final RowStore mRowStore;
        private final Sorter mSorter;
        private final Class<?> mRowType;
        private final SecondaryInfo mSortedInfo;
        private final RowDecoder mDecoder;

        private Transcoder mTranscoder;

        private byte[][] mBatch;
        private int mBatchSize;

        BigSort(BaseTable<R> table, String orderBySpec, QueryLauncher<R> launcher)
            throws IOException
        {
            mRowStore = table.rowStore();
            mSorter = mRowStore.mDatabase.newSorter();
            mRowType = table.rowType();
            Set<String> projection = launcher == null ? null : launcher.projection();
            mSortedInfo = SortDecoderMaker.findSortedInfo(mRowType, orderBySpec, projection, true);
            mDecoder = SortDecoderMaker.findDecoder(mRowType, mSortedInfo, projection);
        }

        RowScanner<R> finish(RowScanner source) throws IOException {
            try {
                return doFinish(source);
            } catch (Throwable e) {
                try {
                    mSorter.reset();
                } catch (Throwable e2) {
                    RowUtils.suppress(e, e2);
                }
                throw e;
            }
        }

        @SuppressWarnings("unchecked")
        RowScanner<R> doFinish(RowScanner source) throws IOException {
            // Transfer all the undecoded rows into the sorter.

            ScanBatch<R> batch = mFirstBatch;

            mFirstBatch = null;
            mLastBatch = null;

            byte[][] kvPairs = null;

            do {
                mTranscoder = transcoder(batch.mEvaluator);
                kvPairs = batch.transcode(mTranscoder, mSorter, kvPairs);
            } while ((batch = batch.mNextBatch) != null);

            // Transfer all the remaining undecoded rows into the sorter, passing `this` as the
            // RowConsumer.

            mBatch = new byte[100][];
            while (source.step(this) != null);
            flush();
            mBatch = null;

            return new ScannerRowScanner<>(mSorter.finishScan(), mDecoder);
        }

        private Transcoder transcoder(RowEvaluator<R> evaluator) {
            return mRowStore.findSortTranscoder(mRowType, evaluator, mSortedInfo);
        }

        @Override
        public void beginBatch(RowEvaluator<R> evaluator) throws IOException {
            flush();
            mTranscoder = transcoder(evaluator);
        }

        @Override
        public void accept(byte[] key, byte[] value) throws IOException {
            byte[][] batch = mBatch;
            int size = mBatchSize;
            mTranscoder.transcode(key, value, batch, size);
            size += 2;
            if (size < batch.length) {
                mBatchSize = size;
            } else {
                mSorter.addBatch(batch, 0, size >> 1);
                mBatchSize = 0;
            }
        }

        private void flush() throws IOException {
            if (mBatchSize > 0) {
                mSorter.addBatch(mBatch, 0, mBatchSize >> 1);
                mBatchSize = 0;
            }
        }
    }
}
