/*
 *  Copyright (C) 2023 Cojen.org
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
import org.cojen.dirmi.Remote;
import org.cojen.dirmi.Serialized;

import org.cojen.tupl.diag.QueryPlan;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public interface RemoteQuery extends Remote {
    public Pipe newScanner(RemoteTransaction txn, Pipe pipe, Object... args) throws IOException;

    public Pipe newUpdater(RemoteTransaction txn, Pipe pipe, Object... args) throws IOException;

    public long deleteAll(RemoteTransaction txn, Object... args) throws IOException;

    public boolean anyRows(RemoteTransaction txn, Object... args) throws IOException;

    @Serialized(filter="java.base/*;org.cojen.tupl.**")
    public QueryPlan scannerPlan(RemoteTransaction txn, Object... args) throws IOException;

    @Serialized(filter="java.base/*;org.cojen.tupl.**")
    public QueryPlan updaterPlan(RemoteTransaction txn, Object... args) throws IOException;

    @Serialized(filter="java.base/*;org.cojen.tupl.**")
    public QueryPlan streamPlan(RemoteTransaction txn, Object... args) throws IOException;
}
