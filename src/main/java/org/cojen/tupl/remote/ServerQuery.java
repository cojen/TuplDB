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

import org.cojen.dirmi.Pipe;

import org.cojen.tupl.Query;

import org.cojen.tupl.diag.QueryPlan;

import org.cojen.tupl.io.Utils;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
final class ServerQuery<R> implements RemoteQuery {
    final ServerTable<R> mTable;
    final Query<R> mQuery;

    ServerQuery(ServerTable<R> table, Query<R> query) {
        mTable = table;
        mQuery = query;
    }

    @Override
    public Pipe newScanner(RemoteTransaction txn, Pipe pipe, Object... args) throws IOException {
        try {
            mTable.mTable.scanWrite(ServerTransaction.txn(txn), pipe, mQuery, args);
            pipe.flush();
            pipe.recycle();
            return null;
        } catch (Throwable e) {
            Utils.closeQuietly(pipe);
            if (!(e instanceof IOException)) {
                throw e;
            }
            return null;
        }
    }

    @Override
    public Pipe newUpdater(RemoteTransaction txn, Pipe pipe, Object... args) throws IOException {
        mTable.newUpdater(mQuery.newUpdater(ServerTransaction.txn(txn), args), pipe);
        return null;
    }

    @Override
    public long deleteAll(RemoteTransaction txn, Object... args) throws IOException {
        return mQuery.deleteAll(ServerTransaction.txn(txn), args);
    }

    @Override
    public boolean anyRows(RemoteTransaction txn, Object... args) throws IOException {
        return mQuery.anyRows(ServerTransaction.txn(txn), args);
    }

    @Override
    public QueryPlan scannerPlan(RemoteTransaction txn, Object... args) throws IOException {
        return mQuery.scannerPlan(ServerTransaction.txn(txn), args);
    }

    @Override
    public QueryPlan updaterPlan(RemoteTransaction txn, Object... args) throws IOException {
        return mQuery.updaterPlan(ServerTransaction.txn(txn), args);
    }

    @Override
    public QueryPlan streamPlan(RemoteTransaction txn, Object... args) throws IOException {
        return mQuery.streamPlan(ServerTransaction.txn(txn), args);
    }
}
