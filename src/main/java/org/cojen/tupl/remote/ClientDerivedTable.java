/*
 *  Copyright (C) 2024 Cojen.org
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

import java.lang.ref.WeakReference;

import java.lang.reflect.Proxy;

import java.util.function.BiPredicate;

import org.cojen.dirmi.RemoteException;
import org.cojen.dirmi.Session;

import org.cojen.tupl.ColumnProcessor;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.Query;
import org.cojen.tupl.Row;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.Updater;

import org.cojen.tupl.io.Utils;

/**
 * Defines a special client-side wrapper for derived tables which supports restorability. The
 * reason why a restorable method won't work is because a row type descriptor must also be
 * returned by the remote call.
 *
 * @author Brian S. O'Neill
 * @see ClientTable#derive
 */
final class ClientDerivedTable implements Table<Row> {
    private final ClientTable<?> mTable;
    private final String mQueryStr;
    private final Object[] mQueryArgs;

    private ClientTable<Row> mDerived;
    private static final VarHandle cDerivedHandle;

    static {
        try {
            var lookup = MethodHandles.lookup();
            cDerivedHandle = lookup.findVarHandle
                (ClientDerivedTable.class, "mDerived", ClientTable.class);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    ClientDerivedTable(ClientTable<?> table, String queryStr, Object... queryArgs)
        throws IOException
    {
        mTable = table;
        mQueryStr = queryStr;
        mQueryArgs = queryArgs.length == 0 ? queryArgs : queryArgs.clone();

        Session.access(table.mRemote).addStateListener(new Listener(this));

        cDerivedHandle.setRelease(this, newDerived());
    }

    private static final class Listener extends WeakReference<ClientDerivedTable>
        implements BiPredicate<Session<?>, Throwable>
    {
        private boolean mDoRestore;

        Listener(ClientDerivedTable table) {
            super(table);
        }

        @Override
        public boolean test(Session<?> session, Throwable ex) {
            if (refersTo(null)) {
                return false;
            }

            Session.State state = session.state();

            if (state == Session.State.RECONNECTED) {
                mDoRestore = true;
            } else if (state == Session.State.CONNECTED && mDoRestore) {
                mDoRestore = false;
                ClientDerivedTable cdt = get();
                if (cdt == null) {
                    return false;
                }
                ClientTable<Row> toReplace = cdt.derived();
                if (toReplace != null) {
                    session.execute(() -> cdt.restore(toReplace));
                }
            }

            return true;
        }
    }

    private ClientTable<Row> derived() {
        return (ClientTable<Row>) cDerivedHandle.getAcquire(this);
    }

    private ClientTable<Row> newDerived() throws IOException {
        // Assume that ClientTable.mRemote is restorable.
        DeriveResult dtable = mTable.mRemote.derive(mQueryStr, mQueryArgs);
        return new ClientTable<Row>(mTable.mDb, dtable.table(), dtable.rowType());
    }

    /**
     * @param toReplace must not be null
     */
    private void restore(ClientTable<Row> toReplace) {
        if (derived() != toReplace) {
            return;
        }

        ClientTable<Row> derived;

        try {
            derived = newDerived();
        } catch (RemoteException e) {
            // Assume the listener will handle the reconnect.
            return;
        } catch (Exception e) {
            derived = newBroken(toReplace, e);
        }

        cDerivedHandle.compareAndExchange(this, toReplace, derived);
    }

    private ClientTable<Row> newBroken(ClientTable<Row> toReplace, Throwable cause) {
        var broken = (RemoteTable) Proxy.newProxyInstance
            (RemoteTable.class.getClassLoader(),
             new Class<?>[] {RemoteTable.class},
             (proxy, method, args) -> {
                throw new IllegalStateException("Unable to restore derived table", cause);
            });

        return new ClientTable<Row>(toReplace.mDb, broken, toReplace.mType);
    }

    @Override
    public boolean hasPrimaryKey() {
        return derived().hasPrimaryKey();
    }

    @Override
    public Class<Row> rowType() {
        return derived().rowType();
    }

    @Override
    public Row newRow() {
        return derived().newRow();
    }

    @Override
    public Row cloneRow(Row row) {
        return derived().cloneRow(row);
    }

    @Override
    public void unsetRow(Row row) {
        derived().unsetRow(row);
    }

    @Override
    public void cleanRow(Row row) {
        derived().cleanRow(row);
    }

    @Override
    public void copyRow(Row from, Row to) {
        derived().copyRow(from, to);
    }

    @Override
    public boolean isSet(Row row, String name) {
        return derived().isSet(row, name);
    }

    @Override
    public void forEach(Row row, ColumnProcessor<? super Row> action) {
        derived().forEach(row, action);
    }

    @Override
    public Scanner<Row> newScanner(Row row, Transaction txn) throws IOException {
        return derived().newScanner(row, txn);
    }

    @Override
    public Scanner<Row> newScanner(Row row, Transaction txn, String query, Object... args)
        throws IOException
    {
        return derived().newScanner(row, txn, query, args);
    }

    @Override
    public Updater<Row> newUpdater(Row row, Transaction txn) throws IOException {
        return derived().newUpdater(row, txn);
    }

    @Override
    public Updater<Row> newUpdater(Row row, Transaction txn, String query, Object... args)
        throws IOException
    {
        return derived().newUpdater(row, txn, query, args);
    }

    @Override
    public Query<Row> query(String query) throws IOException {
        return derived().query(query);
    }

    @Override
    public Query<Row> queryAll() throws IOException {
        return derived().queryAll();
    }

    @Override
    public boolean anyRows(Transaction txn) throws IOException {
        return derived().anyRows(txn);
    }

    @Override
    public boolean anyRows(Row row, Transaction txn) throws IOException {
        return derived().anyRows(row, txn);
    }

    @Override
    public boolean anyRows(Transaction txn, String query, Object... args) throws IOException {
        return derived().anyRows(txn, query, args);
    }

    @Override
    public boolean anyRows(Row row, Transaction txn, String query, Object... args)
        throws IOException
    {
        return derived().anyRows(row, txn, query, args);
    }

    @Override
    public Transaction newTransaction(DurabilityMode dm) {
        return derived().newTransaction(dm);
    }

    @Override
    public boolean isEmpty() throws IOException {
        return derived().isEmpty();
    }

    @Override
    public boolean tryLoad(Transaction txn, Row row) throws IOException {
        return derived().tryLoad(txn, row);
    }

    @Override
    public boolean exists(Transaction txn, Row row) throws IOException {
        return derived().exists(txn, row);
    }

    @Override
    public void store(Transaction txn, Row row) throws IOException {
        derived().store(txn, row);
    }

    @Override
    public Row exchange(Transaction txn, Row row) throws IOException {
        return derived().exchange(txn, row);
    }

    @Override
    public boolean tryInsert(Transaction txn, Row row) throws IOException {
        return derived().tryInsert(txn, row);
    }

    @Override
    public boolean tryReplace(Transaction txn, Row row) throws IOException {
        return derived().tryReplace(txn, row);
    }

    @Override
    public boolean tryUpdate(Transaction txn, Row row) throws IOException {
        return derived().tryUpdate(txn, row);
    }

    @Override
    public boolean tryMerge(Transaction txn, Row row) throws IOException {
        return derived().tryMerge(txn, row);
    }

    @Override
    public boolean tryDelete(Transaction txn, Row row) throws IOException {
        return derived().tryDelete(txn, row);
    }

    @Override
    public <D> Table<D> derive(Class<D> derivedType, String query, Object... args)
        throws IOException
    {
        return derived().derive(derivedType, query, args);
    }

    @Override
    public Table<Row> derive(String query, Object... args) throws IOException {
        return derived().derive(query, args);
    }

    @Override
    public void close() throws IOException {
        derived().close();
    }

    @Override
    public boolean isClosed() {
        return derived().isClosed();
    }
}
