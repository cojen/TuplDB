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

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class ServerTable<R> implements RemoteTable {
    final Table<R> mTable;

    ServerTable(Table<R> table) {
        mTable = table;
    }

    @Override
    public Pipe newScanner(RemoteTransaction txn, Pipe pipe) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public Pipe newScanner(RemoteTransaction txn, Pipe pipe, String query, Object... args)
        throws IOException
    {
        // FIXME
        throw null;
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
    public Pipe load(RemoteTransaction txn, Pipe pipe) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public Pipe exists(RemoteTransaction txn, Pipe pipe) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public Pipe store(RemoteTransaction txn, Pipe pipe) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public Pipe exchange(RemoteTransaction txn, Pipe pipe) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public Pipe insert(RemoteTransaction txn, Pipe pipe) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public Pipe replace(RemoteTransaction txn, Pipe pipe) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public Pipe update(RemoteTransaction txn, Pipe pipe) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public Pipe merge(RemoteTransaction txn, Pipe pipe) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public Pipe delete(RemoteTransaction txn, Pipe pipe) throws IOException {
        // FIXME
        throw null;
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
