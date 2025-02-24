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

import org.cojen.dirmi.AutoDispose;
import org.cojen.dirmi.Batched;
import org.cojen.dirmi.Data;
import org.cojen.dirmi.Disposer;
import org.cojen.dirmi.Pipe;
import org.cojen.dirmi.Remote;
import org.cojen.dirmi.RemoteFailure;
import org.cojen.dirmi.Restorable;
import org.cojen.dirmi.Serialized;

import org.cojen.tupl.DurabilityMode;

import org.cojen.tupl.diag.QueryPlan;

/**
 * 
 *
 * @author Brian S O'Neill
 */
@AutoDispose
public interface RemoteTable extends Remote, Disposable {
    /**
     * Is non-null only for derived tables.
     */
    @Data
    public byte[] descriptor();

    @Data
    public boolean hasPrimaryKey();

    public Pipe newScanner(RemoteTransaction txn, Pipe pipe) throws IOException;

    public Pipe newScanner(RemoteTransaction txn, Pipe pipe, String query, Object... args)
        throws IOException;

    public Pipe newUpdater(RemoteTransaction txn, Pipe pipe) throws IOException;

    public Pipe newUpdater(RemoteTransaction txn, Pipe pipe, String query, Object... args)
        throws IOException;

    /**
     * @return argCount
     */
    public int query(String query) throws IOException;

    public long deleteAll(RemoteTransaction txn, String query, Object... args) throws IOException;

    public boolean anyRows(RemoteTransaction txn) throws IOException;

    public boolean anyRows(RemoteTransaction txn, String query, Object... args) throws IOException;

    @Batched
    @RemoteFailure(declared=false)
    public RemoteTransaction newTransaction(DurabilityMode dm);

    public boolean isEmpty() throws IOException;

    @Restorable
    public RemoteTable derive(String typeName, byte[] descriptor, String query, Object... args)
        throws IOException;

    @Restorable
    public RemoteTable derive(String query, Object... args) throws IOException;

    @Restorable
    public RemoteTableProxy proxy(byte[] descriptor) throws IOException;

    @Serialized(filter="java.base/*;org.cojen.tupl.**")
    public QueryPlan scannerPlan(RemoteTransaction txn, String query, Object... args)
        throws IOException;

    @Serialized(filter="java.base/*;org.cojen.tupl.**")
    public QueryPlan updaterPlan(RemoteTransaction txn, String query, Object... args)
        throws IOException;

    @Serialized(filter="java.base/*;org.cojen.tupl.**")
    public QueryPlan streamPlan(RemoteTransaction txn, String query, Object... args)
        throws IOException;

    @Disposer
    public void close() throws IOException;

    @RemoteFailure(declared=false)
    public boolean isClosed();
}
