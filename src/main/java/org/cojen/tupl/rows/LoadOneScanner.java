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

package org.cojen.tupl.rows;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Ordering;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.View;

/**
 * 
 *
 * @author Brian S O'Neill
 * @see LoadOneQueryLauncher
 */
final class LoadOneScanner<R> implements Scanner<R>, Cursor {
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
        if (value == null) {
            mRow = null;
        } else {
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
        mValue = source.load(txn, key);
        mKey = key;
        RowEvaluator<R> evaluator = controller.evaluator();
        consumer.beginBatch(this, evaluator);
        mRow = (R) evaluator.evalRow(this, LockResult.UNOWNED, (R) consumer);
        mController = controller;
    }

    @Override
    public String toString() {
        return RowUtils.scannerToString(this, mController);
    }

    @Override
    public final long estimateSize() {
        return 1;
    }

    @Override
    public final int characteristics() {
        return NONNULL | ORDERED | CONCURRENT | DISTINCT | SIZED;
    }

    @Override
    public final R row() {
        return mRow;
    }

    @Override
    public final R step(R row) {
        mRow = null;
        return null;
    }

    @Override
    public final void close() {
        mRow = null;
    }

    // The remainding methods are defined in the Cursor interface. RowEvaluator only needs the
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

    @Override
    public long valueLength() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void valueLength(long length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int valueRead(long pos, byte[] buf, int off, int len) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void valueWrite(long pos, byte[] buf, int off, int len) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void valueClear(long pos, long length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream newValueInputStream(long pos) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream newValueInputStream(long pos, int bufferSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public OutputStream newValueOutputStream(long pos) {
        throw new UnsupportedOperationException();
    }

    @Override
    public OutputStream newValueOutputStream(long pos, int bufferSize) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Ordering ordering() {
        return Ordering.UNSPECIFIED;
    }

    @Override
    public Transaction link(Transaction txn) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Transaction link() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean autoload(boolean mode) {
        return true;
    }

    @Override
    public boolean autoload() {
        return true;
    }

    @Override
    public LockResult first() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LockResult last() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LockResult skip(long amount) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LockResult next() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LockResult previous() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LockResult find(byte[] key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LockResult random(byte[] lowKey, boolean lowInclusive,
                             byte[] highKey, boolean highInclusive)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean exists() {
        throw new UnsupportedOperationException();
    }

    @Override
    public LockResult load() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void store(byte[] value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Cursor copy() {
        throw new UnsupportedOperationException();
    }
}
