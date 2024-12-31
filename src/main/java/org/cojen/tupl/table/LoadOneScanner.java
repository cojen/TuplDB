/*
 *  Copyright (C) 2023 Cojen.org
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

import org.cojen.tupl.LockResult;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.View;

/**
 * 
 *
 * @author Brian S O'Neill
 * @see LoadOneQueryLauncher
 */
final class LoadOneScanner<R> implements Scanner<R>, UnsupportedCursor {
    private final ScanController mController;
    private byte[] mKey, mValue;
    private R mRow;

    /**
     * Constructor used by the LoadOneQueryLauncher.newScanner method.
     *
     * @param row can pass null to construct a new instance
     */
    LoadOneScanner(View source, Transaction txn, ScanController<R> controller, R row)
        throws IOException
    {
        byte[] key = controller.oneKey();
        byte[] value = source.load(txn, key);
        if (value != null) {
            mKey = key;
            mValue = value;
            mRow = controller.evaluator().evalRow(this, LockResult.UNOWNED, row);
        }
        mController = controller;
    }

    /**
     * Constructor used by the LoadOneQueryLauncher.scanWrite method.
     */
    @SuppressWarnings("unchecked")
    LoadOneScanner(RowConsumer consumer, View source, Transaction txn, ScanController<R> controller)
        throws IOException
    {
        byte[] key = controller.oneKey();
        byte[] value = source.load(txn, key);
        if (value != null) {
            mKey = key;
            mValue = value;
            RowEvaluator<R> evaluator = controller.evaluator();
            consumer.beginBatch(this, evaluator);
            mRow = evaluator.evalRow(this, LockResult.UNOWNED, (R) consumer);
        }
        mController = controller;
    }

    @Override
    public String toString() {
        return RowUtils.scannerToString(this, mController);
    }

    @Override
    public long estimateSize() {
        return 1;
    }

    @Override
    public int characteristics() {
        return NONNULL | ORDERED | CONCURRENT | DISTINCT | SIZED;
    }

    @Override
    public R row() {
        return mRow;
    }

    @Override
    public R step(R row) {
        mRow = null;
        return null;
    }

    @Override
    public void close() {
        mRow = null;
    }

    // The remaining methods are defined in the Cursor interface. RowEvaluator only needs the
    // key, value and reset methods. The rest aren't implemented.

    @Override
    public byte[] key() {
        return mKey;
    }

    @Override
    public byte[] value() {
        return mValue;
    }

    @Override
    public void reset() {
        close();
    }
}
