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

package org.cojen.tupl.remote;

import java.io.IOException;

import org.cojen.tupl.Query;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.Updater;

import org.cojen.tupl.diag.QueryPlan;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
final class ClientQuery<R> implements Query<R> {
    final ClientTable<R> mTable;
    final RemoteQuery mRemote;

    ClientQuery(ClientTable<R> table, RemoteQuery remote) {
        mTable = table;
        mRemote = remote;
    }

    @Override
    public Scanner<R> newScanner(R row, Transaction txn, Object... args) throws IOException {
        return mTable.newScanner(mRemote.newScanner(mTable.mDb.remoteTransaction(txn), null), row);
    }

    @Override
    public Updater<R> newUpdater(R row, Transaction txn, Object... args) throws IOException {
        return mTable.newUpdater(mRemote.newUpdater(mTable.mDb.remoteTransaction(txn), null), row);
    }

    @Override
    public long deleteAll(Transaction txn, Object... args) throws IOException {
        return mRemote.deleteAll(mTable.mDb.remoteTransaction(txn), args);
    }

    @Override
    public boolean anyRows(Transaction txn) throws IOException {
        return mRemote.anyRows(mTable.mDb.remoteTransaction(txn));
    }

    @Override
    public boolean anyRows(R row, Transaction txn) throws IOException {
        return anyRows(txn);
    }

    @Override
    public boolean anyRows(Transaction txn, Object... args) throws IOException {
        return mRemote.anyRows(mTable.mDb.remoteTransaction(txn), args);
    }

    @Override
    public boolean anyRows(R row, Transaction txn, Object... args) throws IOException {
        return anyRows(txn, args);
    }

    @Override
    public QueryPlan scannerPlan(Transaction txn, Object... args) throws IOException {
        return mRemote.scannerPlan(mTable.mDb.remoteTransaction(txn), args);
    }

    @Override
    public QueryPlan updaterPlan(Transaction txn, Object... args) throws IOException {
        return mRemote.updaterPlan(mTable.mDb.remoteTransaction(txn), args);
    }

    @Override
    public QueryPlan streamPlan(Transaction txn, Object... args) throws IOException {
        return mRemote.scannerPlan(mTable.mDb.remoteTransaction(txn), args);
    }
}
