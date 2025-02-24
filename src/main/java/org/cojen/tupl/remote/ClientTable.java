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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.util.Spliterator;

import org.cojen.dirmi.ClosedException;
import org.cojen.dirmi.Pipe;
import org.cojen.dirmi.RemoteException;

import org.cojen.tupl.ColumnProcessor;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.Query;
import org.cojen.tupl.Row;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.Updater;

import org.cojen.tupl.core.TupleKey;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.table.ClientTableHelper;
import org.cojen.tupl.table.RowInfo;
import org.cojen.tupl.table.RowReader;
import org.cojen.tupl.table.RowStore;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class ClientTable<R> implements Table<R> {
    final ClientDatabase mDb;
    final RemoteTable mRemote;
    final Class<R> mType;

    private final ClientTableHelper<R> mHelper;

    private RemoteTableProxy mProxy;

    static final VarHandle cProxyHandle;

    static {
        try {
            var lookup = MethodHandles.lookup();
            cProxyHandle = lookup.findVarHandle
                (ClientTable.class, "mProxy", RemoteTableProxy.class);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    ClientTable(ClientDatabase db, RemoteTable remote, Class<R> type) {
        mDb = db;
        mRemote = remote;
        mType = type;

        mHelper = ClientTableHelper.find(type);
    }

    @Override
    public boolean hasPrimaryKey() {
        return mRemote.hasPrimaryKey();
    }

    @Override
    public Class<R> rowType() {
        return mType;
    }

    @Override
    public R newRow() {
        return mHelper.newRow();
    }

    @Override
    public R cloneRow(R row) {
        return mHelper.cloneRow(row);
    }

    @Override
    public void unsetRow(R row) {
        mHelper.unsetRow(row);
    }

    @Override
    public void cleanRow(R row) {
        mHelper.cleanRow(row);
    }

    @Override
    public void copyRow(R from, R to) {
        mHelper.copyRow(from, to);
    }

    @Override
    public boolean isSet(R row, String name) {
        return mHelper.isSet(row, name);
    }

    @Override
    public void forEach(R row, ColumnProcessor<? super R> action) {
        mHelper.forEach(row, action);
    }

    @Override
    public Scanner<R> newScanner(R row, Transaction txn) throws IOException {
        return newScanner(mRemote.newScanner(mDb.remoteTransaction(txn), null), row);
    }

    @Override
    public Scanner<R> newScanner(R row, Transaction txn, String query, Object... args)
        throws IOException
    {
        return newScanner(mRemote.newScanner(mDb.remoteTransaction(txn), null, query, args), row);
    }

    private Scanner<R> newScanner(Pipe pipe, R row) throws IOException {
        try {
            pipe.flush();
            return new RowReader<R>(mType, pipe, row);
        } catch (IOException e) {
            Utils.closeQuietly(pipe);
            throw e;
        }
    }

    @Override
    public Updater<R> newUpdater(R row, Transaction txn) throws IOException {
        return newUpdater(mRemote.newUpdater(mDb.remoteTransaction(txn), null), row);
    }

    @Override
    public Updater<R> newUpdater(R row, Transaction txn, String query, Object... args)
        throws IOException
    {
        return newUpdater(mRemote.newUpdater(mDb.remoteTransaction(txn), null, query, args), row);
    }

    private ClientUpdater<R> newUpdater(Pipe pipe, R row) throws IOException {
        RemoteTableProxy proxy = proxy();

        pipe.writeObject(proxy);
        pipe.flush();

        int characteristics = pipe.readInt();
        long size = (characteristics & Spliterator.SIZED) == 0 ? Long.MAX_VALUE : pipe.readLong();
        var updater = (RemoteUpdater) pipe.readObject();

        try {
            if (updater == null) {
                row = null;
                pipe.recycle();
            } else {
                if (row == null) {
                    row = mHelper.newRow();
                }
                mHelper.updaterRow(row, pipe); // pipe is recycled or closed as a side-effect
            }
            return new ClientUpdater<R>(mHelper, proxy, characteristics, size, updater, row);
        } catch (Throwable e) {
            if (updater != null) {
                try {
                    updater.dispose();
                } catch (RemoteException e2) {
                    e.addSuppressed(e2);
                }
            }
            throw e;
        }
    }

    @Override
    public Query<R> query(String query) throws IOException {
        // TODO: This design just validates the query and sends the query string to the server
        // each time. An improved design should cache the client and server query objects, and
        // the server needs to be able to release the objects when memory is low. It will need
        // to use a SoftReference and some way of disposing the remote object in a way that
        // preserves restorability.
        int argCount = mRemote.query(query);
        return new ClientQuery<>(this, query, argCount);
    }

    @Override
    public Query<R> queryAll() throws IOException {
        return query("{*}");
    }

    @Override
    public boolean anyRows(Transaction txn) throws IOException {
        return mRemote.anyRows(mDb.remoteTransaction(txn));
    }

    @Override
    public boolean anyRows(R row, Transaction txn) throws IOException {
        return anyRows(txn);
    }

    @Override
    public boolean anyRows(Transaction txn, String query, Object... args) throws IOException {
        return mRemote.anyRows(mDb.remoteTransaction(txn), query, args);
    }

    @Override
    public boolean anyRows(R row, Transaction txn, String query, Object... args)
        throws IOException
    {
        return anyRows(txn, query, args);
    }

    @Override
    public Transaction newTransaction(DurabilityMode dm) {
        return new ClientTransaction(mDb, mRemote.newTransaction(dm), dm);
    }

    @Override
    public boolean isEmpty() throws IOException {
        return mRemote.isEmpty();
    }

    @Override
    public boolean tryLoad(Transaction txn, R row) throws IOException {
        return mHelper.tryLoad(row, proxy().tryLoad(mDb.remoteTransaction(txn), null));
    }

    @Override
    public boolean exists(Transaction txn, R row) throws IOException {
        return mHelper.exists(row, proxy().exists(mDb.remoteTransaction(txn), null));
    }

    @Override
    public void store(Transaction txn, R row) throws IOException {
        mHelper.store(row, proxy().store(mDb.remoteTransaction(txn), null));
    }

    @Override
    public R exchange(Transaction txn, R row) throws IOException {
        return mHelper.exchange(row, proxy().exchange(mDb.remoteTransaction(txn), null));
    }

    @Override
    public boolean tryInsert(Transaction txn, R row) throws IOException {
        return mHelper.tryInsert(row, proxy().tryInsert(mDb.remoteTransaction(txn), null));
    }

    @Override
    public boolean tryReplace(Transaction txn, R row) throws IOException {
        return mHelper.tryReplace(row, proxy().tryReplace(mDb.remoteTransaction(txn), null));
    }

    @Override
    public boolean tryUpdate(Transaction txn, R row) throws IOException {
        return mHelper.tryUpdate(row, proxy().tryUpdate(mDb.remoteTransaction(txn), null));
    }

    @Override
    public boolean tryMerge(Transaction txn, R row) throws IOException {
        return mHelper.tryMerge(row, proxy().tryMerge(mDb.remoteTransaction(txn), null));
    }

    @Override
    public boolean tryDelete(Transaction txn, R row) throws IOException {
        return mHelper.tryDelete(row, proxy().tryDelete(mDb.remoteTransaction(txn), null));
    }

    @Override
    public <D> Table<D> derive(Class<D> derivedType, String query, Object... args)
        throws IOException
    {
        return ClientCache.get(TupleKey.make.with(this, derivedType, query, args), key -> {
            try {
                RowInfo info = RowInfo.find(derivedType);
                byte[] descriptor = RowStore.primaryDescriptor(info);
                RemoteTable rtable = mRemote.derive(info.name, descriptor, query, args);
                return new ClientTable<D>(mDb, rtable, derivedType);
            } catch (IOException e) {
                throw Utils.rethrow(e);
            }
        });
    }

    @Override
    public Table<Row> derive(String query, Object... args) throws IOException {
        return ClientCache.get(TupleKey.make.with(this, query, args), key -> {
            try {
                RemoteTable rtable = mRemote.derive(query, args);
                Class<Row> rowType = RowTypeCache.findRow(rtable.descriptor());
                return new ClientTable<Row>(mDb, rtable, rowType);
            } catch (IOException e) {
                throw Utils.rethrow(e);
            }
        });
    }

    @Override
    public Table<R> distinct() throws IOException {
        // FIXME: distinct
        throw null;
    }

    private RemoteTableProxy proxy() throws IOException {
        var proxy = (RemoteTableProxy) cProxyHandle.getAcquire(this);

        if (proxy == null) {
            synchronized (mHelper) {
                proxy = mProxy;
                if (proxy == null) {
                    proxy = mRemote.proxy(mHelper.rowDescriptor());
                    cProxyHandle.setRelease(this, proxy);
                }
            }
        }

        return proxy;
    }

    @Override
    public void close() throws IOException {
        ClientCache.remove(this);
        try {
            mRemote.dispose();
        } catch (ClosedException e) {
            // Ignore.
        }
    }

    @Override
    public boolean isClosed() {
        try {
            return mRemote.isClosed();
        } catch (Exception e) {
            if (e instanceof ClosedException) {
                return true;
            }
            throw e;
        }
    }
}
