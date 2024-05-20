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
import org.cojen.tupl.Query;
import org.cojen.tupl.Updater;

import org.cojen.tupl.diag.QueryPlan;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.table.BaseTable;
import org.cojen.tupl.table.WeakCache;

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
        } catch (Throwable e) {
            Utils.closeQuietly(pipe);
            if (!(e instanceof IOException)) {
                throw e;
            }
            return null;
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
        } catch (Throwable e) {
            Utils.closeQuietly(pipe);
            if (!(e instanceof IOException)) {
                throw e;
            }
            return null;
        }
    }

    @Override
    public Pipe newUpdater(RemoteTransaction txn, Pipe pipe) throws IOException {
        newUpdater(mTable.newUpdater(ServerTransaction.txn(txn)), pipe);
        return null;
    }

    @Override
    public Pipe newUpdater(RemoteTransaction txn, Pipe pipe, String query, Object... args)
        throws IOException
    {
        newUpdater(mTable.newUpdater(ServerTransaction.txn(txn), query, args), pipe);
        return null;
    }

    void newUpdater(Updater updater, Pipe pipe) throws IOException {
        try {
            var proxy = (RemoteTableProxy) pipe.readObject();
            int characteristics = updater.characteristics();
            pipe.writeInt(characteristics);
            if ((characteristics & Updater.SIZED) != 0) {
                pipe.writeLong(updater.estimateSize());
            }
            if (updater.row() == null) {
                pipe.writeNull();
                pipe.flush();
                pipe.recycle();
            } else {
                var server = new ServerUpdater(updater);
                pipe.writeObject(server);
                proxy.row(server, pipe); // pipe is flushed and recycled as a side-effect
            }
        } catch (Throwable e) {
            Utils.closeQuietly(pipe);
            Utils.closeQuietly(updater);
            if (!(e instanceof IOException)) {
                throw e;
            }
        }
    }

    @Override
    public int query(String query) throws IOException {
        // This just validates the query.
        return mTable.query(query).argumentCount();
    }

    @Override
    public long deleteAll(RemoteTransaction txn, String queryStr, Object... args)
        throws IOException
    {
        Query query = mTable.query(queryStr);
        return query.deleteAll(ServerTransaction.txn(txn), args);
    }

    @Override
    public boolean anyRows(RemoteTransaction txn) throws IOException {
        return mTable.anyRows(ServerTransaction.txn(txn));
    }

    @Override
    public boolean anyRows(RemoteTransaction txn, String query, Object... args) throws IOException {
        return mTable.anyRows(ServerTransaction.txn(txn), query, args);
    }

    @Override
    public RemoteTransaction newTransaction(DurabilityMode dm) {
        return new ServerTransaction(mTable.newTransaction(dm));
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
    public QueryPlan scannerPlan(RemoteTransaction txn, String queryStr, Object... args)
        throws IOException
    {
        Query query = mTable.query(queryStr);
        return query.scannerPlan(ServerTransaction.txn(txn), args);
    }

    @Override
    public QueryPlan updaterPlan(RemoteTransaction txn, String queryStr, Object... args)
        throws IOException
    {
        Query query = mTable.query(queryStr);
        return query.updaterPlan(ServerTransaction.txn(txn), args);
    }

    @Override
    public QueryPlan streamPlan(RemoteTransaction txn, String queryStr, Object... args)
        throws IOException
    {
        Query query = mTable.query(queryStr);
        return query.streamPlan(ServerTransaction.txn(txn), args);
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
