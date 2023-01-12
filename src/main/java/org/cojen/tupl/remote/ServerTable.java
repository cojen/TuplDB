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

package org.cojen.tupl.remote;

import java.io.IOException;

import org.cojen.dirmi.Pipe;

import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.Table;

import org.cojen.tupl.diag.QueryPlan;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.rows.BaseTable;
import org.cojen.tupl.rows.WeakCache;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class ServerTable<R> implements RemoteTable {
    final BaseTable<R> mTable;

    private final WeakCache<byte[], RemoteTableProxy, Object> mProxyCache;

    ServerTable(BaseTable<R> table) throws IOException {
        mTable = table;

        mProxyCache = new WeakCache<>() {
            @Override
            protected RemoteTableProxy newValue(byte[] descriptor, Object unused) {
                try {
                    return mTable.newRemoteProxy(descriptor);
                } catch (IOException e) {
                    throw Utils.rethrow(e);
                }
            }
        };
    }

    @Override
    public Pipe newScanner(RemoteTransaction txn, Pipe pipe) throws IOException {
        try {
            mTable.scanWrite(ServerTransaction.txn(txn), pipe);
            pipe.flush();
            pipe.recycle();
            return null;
        } catch (IOException e) {
            Utils.closeQuietly(pipe);
            throw e;
        }
    }

    @Override
    public Pipe newScanner(RemoteTransaction txn, Pipe pipe, String query, Object... args)
        throws IOException
    {
        try {
            mTable.scanWrite(ServerTransaction.txn(txn), pipe, query, args);
            pipe.flush();
            pipe.recycle();
            return null;
        } catch (IOException e) {
            Utils.closeQuietly(pipe);
            throw e;
        }
    }

    @Override
    public Pipe newUpdater(RemoteTransaction txn, Pipe pipe) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public Pipe newUpdater(RemoteTransaction txn, Pipe pipe, String query, Object... args)
        throws IOException
    {
        // FIXME
        throw null;
    }

    @Override
    public RemoteTransaction newTransaction(DurabilityMode dm) {
        return ServerTransaction.from(mTable.newTransaction(dm));
    }

    @Override
    public boolean isEmpty() throws IOException {
        return mTable.isEmpty();
    }

    @Override
    public RemoteTableProxy proxy(byte[] descriptor) throws IOException {
        return mProxyCache.obtain(descriptor, null);
    }

    @Override
    public QueryPlan scannerPlan(RemoteTransaction txn, String query, Object... args)
        throws IOException
    {
        return mTable.scannerPlan(ServerTransaction.txn(txn), query, args);
    }

    @Override
    public QueryPlan updaterPlan(RemoteTransaction txn, String query, Object... args)
        throws IOException
    {
        return mTable.updaterPlan(ServerTransaction.txn(txn), query, args);
    }

    @Override
    public QueryPlan streamPlan(RemoteTransaction txn, String query, Object... args)
        throws IOException
    {
        return mTable.streamPlan(ServerTransaction.txn(txn), query, args);
    }

    @Override
    public void close() throws IOException {
        mTable.close();
    }

    @Override
    public boolean isClosed() {
        return mTable.isClosed();
    }

    @Override
    public void dispose() {
    }
}
