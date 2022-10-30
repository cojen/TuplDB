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

import org.cojen.dirmi.Batched;
import org.cojen.dirmi.Disposer;
import org.cojen.dirmi.NoReply;
import org.cojen.dirmi.Pipe;
import org.cojen.dirmi.Remote;
import org.cojen.dirmi.RemoteException;
import org.cojen.dirmi.RemoteFailure;
import org.cojen.dirmi.Serialized;

import org.cojen.tupl.DurabilityMode;

import org.cojen.tupl.diag.QueryPlan;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public interface RemoteTable extends Remote {
    public Pipe newScanner(RemoteTransaction txn, Pipe pipe) throws IOException;

    public Pipe newScanner(RemoteTransaction txn, Pipe pipe, String query, Object... args)
        throws IOException;

    public Pipe newUpdater(RemoteTransaction txn, Pipe pipe) throws IOException;

    public Pipe newUpdater(RemoteTransaction txn, Pipe pipe, String query, Object... args)
        throws IOException;

    @Batched
    @RemoteFailure(declared=false)
    public RemoteTransaction newTransaction(DurabilityMode dm);

    public boolean isEmpty() throws IOException;

    public Pipe load(RemoteTransaction txn, Pipe pipe) throws IOException;

    public Pipe exists(RemoteTransaction txn, Pipe pipe) throws IOException;

    public Pipe store(RemoteTransaction txn, Pipe pipe) throws IOException;

    public Pipe exchange(RemoteTransaction txn, Pipe pipe) throws IOException;

    public Pipe insert(RemoteTransaction txn, Pipe pipe) throws IOException;

    public Pipe replace(RemoteTransaction txn, Pipe pipe) throws IOException;

    public Pipe update(RemoteTransaction txn, Pipe pipe) throws IOException;

    public Pipe merge(RemoteTransaction txn, Pipe pipe) throws IOException;

    public Pipe delete(RemoteTransaction txn, Pipe pipe) throws IOException;

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

    @NoReply
    @Disposer
    public void dispose() throws RemoteException;
}
