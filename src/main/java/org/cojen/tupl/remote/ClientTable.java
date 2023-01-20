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

import java.util.Comparator;

import java.util.function.Predicate;

import java.util.stream.Stream;

import org.cojen.dirmi.ClosedException;
import org.cojen.dirmi.Pipe;

import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.Updater;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.diag.QueryPlan;

import org.cojen.tupl.rows.ClientTableHelper;
import org.cojen.tupl.rows.RowReader;

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
    public void copyRow(R from, R to) {
        mHelper.copyRow(from, to);
    }

    @Override
    public Scanner<R> newScanner(Transaction txn) throws IOException {
        return newScanner(mRemote.newScanner(mDb.remoteTransaction(txn), null));
    }

    @Override
    public Scanner<R> newScanner(Transaction txn, String query, Object... args) throws IOException {
        return newScanner(mRemote.newScanner(mDb.remoteTransaction(txn), null, query, args));
    }

    private Scanner<R> newScanner(Pipe pipe) throws IOException {
        try {
            pipe.flush();

            return new RowReader<R, Pipe>(mType, pipe) {
                @Override
                protected void close(Pipe pipe, boolean finished) throws IOException {
                    if (finished) {
                        pipe.recycle();
                    } else {
                        pipe.close();
                    }
                }
            };
        } catch (IOException e) {
            Utils.closeQuietly(pipe);
            throw e;
        }
    }

    @Override
    public Updater<R> newUpdater(Transaction txn) throws IOException {
        // FIXME: newUpdater
        throw null;
    }

    @Override
    public Updater<R> newUpdater(Transaction txn, String query, Object... args) throws IOException {
        // FIXME: newUpdater
        throw null;
    }

    @Override
    public Transaction newTransaction(DurabilityMode dm) {
        return ClientTransaction.from(mDb, mRemote.newTransaction(dm), dm);
    }

    @Override
    public boolean isEmpty() throws IOException {
        return mRemote.isEmpty();
    }

    @Override
    public boolean load(Transaction txn, R row) throws IOException {
        return mHelper.load(row, proxy().load(mDb.remoteTransaction(txn), null));
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
    public boolean insert(Transaction txn, R row) throws IOException {
        return mHelper.insert(row, proxy().insert(mDb.remoteTransaction(txn), null));
    }

    @Override
    public boolean replace(Transaction txn, R row) throws IOException {
        return mHelper.replace(row, proxy().replace(mDb.remoteTransaction(txn), null));
    }

    @Override
    public boolean update(Transaction txn, R row) throws IOException {
        return mHelper.update(row, proxy().update(mDb.remoteTransaction(txn), null));
    }

    @Override
    public boolean merge(Transaction txn, R row) throws IOException {
        return mHelper.merge(row, proxy().merge(mDb.remoteTransaction(txn), null));
    }

    @Override
    public boolean delete(Transaction txn, R row) throws IOException {
        return mHelper.delete(row, proxy().delete(mDb.remoteTransaction(txn), null));
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
    public Comparator<R> comparator(String spec) {
        return mHelper.comparator(spec);
    }

    @Override
    public Predicate<R> predicate(String query, Object... args) {
        return mHelper.predicate(query, args);
    }

    @Override
    public QueryPlan scannerPlan(Transaction txn, String query, Object... args) throws IOException {
        return mRemote.scannerPlan(mDb.remoteTransaction(txn), query, args);
    }

    @Override
    public QueryPlan updaterPlan(Transaction txn, String query, Object... args) throws IOException {
        return mRemote.updaterPlan(mDb.remoteTransaction(txn), query, args);
    }

    @Override
    public QueryPlan streamPlan(Transaction txn, String query, Object... args) throws IOException {
        return mRemote.scannerPlan(mDb.remoteTransaction(txn), query, args);
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
