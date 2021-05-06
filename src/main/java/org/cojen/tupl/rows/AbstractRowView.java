/*
 *  Copyright 2021 Cojen.org
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

package org.cojen.tupl.rows;

import java.io.IOException;

import java.util.Objects;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.Index;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.RowIndex;
import org.cojen.tupl.RowScanner;
import org.cojen.tupl.RowUpdater;
import org.cojen.tupl.RowView;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.View;

/**
 * 
 *
 * @author Brian S O'Neill
 */
abstract class AbstractRowView<R> implements RowIndex<R> {
    protected final View mSource;

    protected AbstractRowView(View source) {
        mSource = Objects.requireNonNull(source);
    }

    @Override
    public long id() {
        return (mSource instanceof Index) ? ((Index) mSource).id() : 0;
    }

    @Override
    public byte[] name() {
        return (mSource instanceof Index) ? ((Index) mSource).name() : null;
    }

    @Override
    public String nameString() {
        return (mSource instanceof Index) ? ((Index) mSource).nameString() : null;
    }

    @Override
    public RowScanner<R> newScanner(Transaction txn) throws IOException {
        return newScanner(mSource.newCursor(txn));
    }

    @Override
    public RowUpdater<R> newUpdater(Transaction txn) throws IOException {
        // FIXME: more types of updaters are required; see View.newUpdater
        Cursor c = mSource.newCursor(txn);
        // FIXME: register the cursor
        return newUpdater(c);
    }

    @Override
    public void close() throws IOException {
        if (mSource instanceof Index) {
            ((Index) mSource).close();
        }
    }

    @Override
    public boolean isClosed() {
        return (mSource instanceof Index) ? ((Index) mSource).isClosed() : false;
    }

    @Override
    public String toString() {
        if (!(mSource instanceof Index)) {
            return super.toString();
        }
        var b = new StringBuilder();
        b.append(getClass().getName()).append('@').append(Integer.toHexString(hashCode()));
        b.append('{');
        String nameStr = nameString();
        if (nameStr != null) {
            b.append("name").append(": ").append(nameStr);
            b.append(", ");
        }
        b.append("id").append(": ").append(id());
        return b.append('}').toString();
    }

    @Override
    public Transaction newTransaction(DurabilityMode durabilityMode) {
        return mSource.newTransaction(durabilityMode);
    }

    @Override
    public boolean isEmpty() throws IOException {
        return mSource.isEmpty();
    }

    /**
     * Returns a new transaction or enters a scope with the UPGRADABLE_READ lock mode.
     */
    protected final Transaction enterTransaction(Transaction txn) throws IOException {
        if (txn == null) {
            txn = newTransaction(null);
        } else {
            txn.enter();
            txn.lockMode(LockMode.UPGRADABLE_READ);
        }
        return txn;
    }

    /**
     * @param c unpositioned
     */
    protected abstract RowScanner<R> newScanner(Cursor c);

    /**
     * @param c unpositioned
     */
    protected abstract RowUpdater<R> newUpdater(Cursor c);

    protected abstract RowView<R> newView(View source);
}
