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

import java.util.Set;

import org.cojen.dirmi.Pipe;
import org.cojen.dirmi.RemoteException;

import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.Row;

import org.cojen.tupl.diag.QueryPlan;

/**
 * DerivedTable is a client-side object which implements RemoteTable to support restorability.
 * The reason why DerivedTable exists instead of using RemoteTable directly is for the
 * generated descriptor to be returned by the server without requiring a second remote call.
 *
 * @author Brian S. O'Neill
 * @see RemoteTable#derive
 */
public final class DerivedTable implements RemoteTable {
    private final RemoteTable mTable;
    private final byte[] mDescriptor;

    DerivedTable(RemoteTable table, byte[] descriptor) {
        mTable = table;
        mDescriptor = descriptor;
    }

    // FIXME: Must make this class be manually restorable. The client must pass the origin
    // RemoteTable, the query, and the args into this object. The fields of the this class must
    // not be final. The methods in the class need to catch exceptions, restore, and retry.

    @Override
    public Pipe newScanner(RemoteTransaction txn, Pipe pipe) throws IOException {
        return mTable.newScanner(txn, pipe);
    }

    @Override
    public Pipe newScanner(RemoteTransaction txn, Pipe pipe, String query, Object... args)
        throws IOException
    {
        return mTable.newScanner(txn, pipe, query, args);
    }

    @Override
    public Pipe newUpdater(RemoteTransaction txn, Pipe pipe) throws IOException {
        return mTable.newUpdater(txn, pipe);
    }

    @Override
    public Pipe newUpdater(RemoteTransaction txn, Pipe pipe, String query, Object... args)
        throws IOException
    {
        return mTable.newUpdater(txn, pipe, query, args);
    }

    @Override
    public int query(String query) throws IOException {
        return mTable.query(query);
    }

    @Override
    public long deleteAll(RemoteTransaction txn, String query, Object... args) throws IOException {
        return mTable.deleteAll(txn, query, args);
    }

    @Override
    public boolean anyRows(RemoteTransaction txn) throws IOException {
        return mTable.anyRows(txn);
    }

    @Override
    public boolean anyRows(RemoteTransaction txn, String query, Object... args) throws IOException {
        return mTable.anyRows(txn, query, args);
    }

    @Override
    public RemoteTransaction newTransaction(DurabilityMode dm) {
        return mTable.newTransaction(dm);
    }

    @Override
    public boolean isEmpty() throws IOException {
        return mTable.isEmpty();
    }

    @Override
    public RemoteTable derive(String typeName, byte[] descriptor, String query, Object... args)
        throws IOException
    {
        return mTable.derive(typeName, descriptor, query, args);
    }

    @Override
    public DerivedTable derive(String query, Object... args) throws IOException {
        return mTable.derive(query, args);
    }

    @Override
    public RemoteTableProxy proxy(byte[] descriptor) throws IOException {
        return mTable.proxy(descriptor);
    }

    @Override
    public QueryPlan scannerPlan(RemoteTransaction txn, String query, Object... args)
        throws IOException
    {
        return mTable.scannerPlan(txn, query, args);
    }

    @Override
    public QueryPlan updaterPlan(RemoteTransaction txn, String query, Object... args)
        throws IOException
    {
        return mTable.updaterPlan(txn, query, args);
    }

    @Override
    public QueryPlan streamPlan(RemoteTransaction txn, String query, Object... args)
        throws IOException
    {
        return mTable.streamPlan(txn, query, args);
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
    public void dispose() throws RemoteException {
        // Note: No need to restore if this operation fails.
        mTable.dispose();
    }

    Class<Row> rowType() {
        return RowTypeCache.find(mDescriptor);
    }

    public static final class Serializer implements org.cojen.dirmi.Serializer {
        static final Serializer THE = new Serializer();

        Serializer() {
        }

        @Override
        public Set<Class<?>> supportedTypes() {
            return Set.of(DerivedTable.class);
        }

        @Override
        public void write(Pipe pipe, Object obj) throws IOException {
            var dt = (DerivedTable) obj;
            pipe.writeObject(dt.mTable);
            byte[] descriptor = dt.mDescriptor;
            pipe.writeInt(descriptor.length);
            pipe.write(descriptor);
        }

        @Override
        public Object read(Pipe pipe) throws IOException {
            var table = (RemoteTable) pipe.readObject();
            byte[] descriptor = new byte[pipe.readInt()];
            pipe.readFully(descriptor);
            return new DerivedTable(table, descriptor);
        }
    }
}
