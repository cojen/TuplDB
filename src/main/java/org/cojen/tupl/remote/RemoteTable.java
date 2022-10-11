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
import org.cojen.dirmi.Pipe;
import org.cojen.dirmi.Remote;
import org.cojen.dirmi.RemoteException;
import org.cojen.dirmi.RemoteFailure;

import org.cojen.tupl.diag.QueryPlan;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public interface RemoteTable extends Remote {
    @RemoteFailure(declared=false)
    public Object newRow();

    @RemoteFailure(declared=false)
    public Object cloneRow(Object row);

    @RemoteFailure(declared=false)
    public void unsetRow(Object row);

    @RemoteFailure(declared=false)
    public void copyRow(Object from, Object to);

    public Pipe newRowScanner(RemoteTransaction txn, Pipe pipe) throws IOException;

    public Pipe newRowScanner(RemoteTransaction txn, Pipe pipe, String query, Object... args)
        throws IOException;

    public Pipe newRowUpdater(RemoteTransaction txn, Pipe pipe) throws IOException;

    public Pipe newRowUpdater(RemoteTransaction txn, Pipe pipe, String query, Object... args)
        throws IOException;

    @Batched
    @RemoteFailure(declared=false)
    public RemoteTransaction newTransaction(int durabilityMode);

    public boolean isEmpty() throws IOException;

    public boolean load(RemoteTransaction txn, Object row) throws IOException;

    public boolean exists(RemoteTransaction txn, Object row) throws IOException;

    public void store(RemoteTransaction txn, Object row) throws IOException;

    public Object exchange(RemoteTransaction txn, Object row) throws IOException;

    public boolean insert(RemoteTransaction txn, Object row) throws IOException;

    public boolean replace(RemoteTransaction txn, Object row) throws IOException;

    public boolean update(RemoteTransaction txn, Object row) throws IOException;

    public boolean merge(RemoteTransaction txn, Object row) throws IOException;

    public boolean delete(RemoteTransaction txn, Object row) throws IOException;

    public QueryPlan scannerPlan(RemoteTransaction txn, String query, Object... args)
        throws IOException;

    public QueryPlan updaterPlan(RemoteTransaction txn, String query, Object... args)
        throws IOException;

    public QueryPlan streamPlan(RemoteTransaction txn, String query, Object... args)
        throws IOException;

    @Disposer
    public void dispose() throws RemoteException;
}
