/*
 *  Copyright (C) 2021 Cojen.org
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

package org.cojen.tupl.table;

import java.io.IOException;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UnpositionedCursorException;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class BasicScanner<R> implements Scanner<R> {
    final StoredTable<R> mTable;
    final ScanController<R> mController;

    Cursor mCursor;
    RowEvaluator<R> mEvaluator;

    R mRow;

    BasicScanner(StoredTable<R> table, ScanController<R> controller) {
        mTable = table;
        mController = controller;
    }

    /**
     * Must be called after construction.
     *
     * @param txn can be null
     * @param row initial row; can be null
     */
    void init(Transaction txn, R row) throws IOException {
        a: while (true) {
            beginBatch(row, mController.evaluator());

            Cursor c = mController.newCursor(mTable.mSource, txn);
            mCursor = c;

            LockResult result = toFirst(c);
            while (true) {
                byte[] key = c.key();
                if (key == null) {
                    if (!mController.next()) {
                        break a;
                    }
                    continue a;
                }
                try {
                    R decoded = evalRow(c, result, row);
                    if (decoded != null) {
                        mRow = decoded;
                        return;
                    }
                } catch (StoppedCursorException e) {
                    if (result == LockResult.ACQUIRED) {
                        c.link().unlock();
                        unlocked();
                    }
                    continue;
                } catch (Throwable e) {
                    throw RowUtils.fail(this, e);
                }
                if (result == LockResult.ACQUIRED) {
                    c.link().unlock();
                    unlocked();
                }
                result = toNext(c);
            }
        }

        finished();
    }

    @Override
    public String toString() {
        return RowUtils.scannerToString(this, mController);
    }

    @Override
    public final long estimateSize() {
        return mController.estimateSize();
    }

    @Override
    public final int characteristics() {
        return mController.characteristics();
    }

    @Override
    public final R row() {
        return mRow;
    }

    @Override
    public final R step(R row) throws IOException {
        return doStep(row);
    }

    protected final R doStep(R row) throws IOException {
        Cursor c = mCursor;
        try {
            a: while (true) {
                LockResult result = toNext(c);
                b: while (true) {
                    while (c.key() == null) {
                        if (!mController.next()) {
                            break a;
                        }
                        beginBatch(row, mController.evaluator());
                        Transaction txn = c.link();
                        mCursor = c = mController.newCursor(mTable.mSource, txn);
                        toFirst(c);
                    }
                    try {
                        R decoded = evalRow(c, result, row);
                        if (decoded != null) {
                            mRow = decoded;
                            return decoded;
                        }
                    } catch (StoppedCursorException e) {
                        if (result == LockResult.ACQUIRED) {
                            c.link().unlock();
                            unlocked();
                        }
                        continue b;
                    }
                    if (result == LockResult.ACQUIRED) {
                        c.link().unlock();
                        unlocked();
                    }
                    continue a;
                }
            }
        } catch (UnpositionedCursorException e) {
        } catch (Throwable e) {
            throw RowUtils.fail(this, e);
        }

        finished();
        return null;
    }

    @SuppressWarnings("unchecked")
    protected void beginBatch(R row, RowEvaluator<R> evaluator) throws IOException {
        mEvaluator = evaluator;
        if (row instanceof RowConsumer consumer) {
            consumer.beginBatch(this, evaluator);
        }
    }

    protected R evalRow(Cursor c, LockResult result, R row) throws IOException {
        return mEvaluator.evalRow(c, result, row);
    }

    @Override
    public final void close() throws IOException {
        finished();
        mCursor.reset();
    }

    protected LockResult toFirst(Cursor c) throws IOException {
        return c.first();
    }

    protected LockResult toNext(Cursor c) throws IOException {
        return c.next();
    }

    /**
     * Called to inform subclasses that they shouldn't attempt to unlock the current row.
     */
    protected void unlocked() {
    }

    protected void finished() throws IOException {
        mRow = null;
    }
}
