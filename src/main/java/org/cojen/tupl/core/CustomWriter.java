/*
 *  Copyright 2019 Cojen.org
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

package org.cojen.tupl.core;

import java.io.IOException;

import org.cojen.tupl.Transaction;

import org.cojen.tupl.ext.CustomHandler;

/**
 * 
 *
 * @author Brian S O'Neill
 */
/*P*/
final class CustomWriter implements CustomHandler {
    private final LocalDatabase mDatabase;
    private final int mHandlerId;

    CustomWriter(LocalDatabase db, int handlerId) {
        mDatabase = db;
        mHandlerId = handlerId;
    }

    @Override
    public void redo(Transaction txn, byte[] message) throws IOException {
        redo(txn, message, 0, null);
    }

    @Override
    public void redo(Transaction txn, byte[] message, long indexId, byte[] key) throws IOException {
        if (txn instanceof LocalTransaction) {
            var local = (LocalTransaction) txn;
            if (local.mDatabase == mDatabase) {
                local.customRedo(mHandlerId, message, indexId, key);
                return;
            }
        }

        fail(txn);
    }

    @Override
    public void undo(Transaction txn, byte[] message) throws IOException {
        if (txn instanceof LocalTransaction) {
            var local = (LocalTransaction) txn;
            if (local.mDatabase == mDatabase) {
                local.customUndo(mHandlerId, message);
                return;
            }
        }

        fail(txn);
    }

    private static void fail(Transaction txn) {
        if (txn == null || txn == Transaction.BOGUS) {
            throw new IllegalArgumentException("Invalid transaction: " + txn);
        }

        throw new IllegalStateException("Transaction belongs to a different database");
    }
}
