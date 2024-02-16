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

/**
 * 
 *
 * @author Brian S O'Neill
 */
public interface RemoteTableProxy extends Remote {
    public Pipe tryLoad(RemoteTransaction txn, Pipe pipe) throws IOException;

    public Pipe exists(RemoteTransaction txn, Pipe pipe) throws IOException;

    public Pipe store(RemoteTransaction txn, Pipe pipe) throws IOException;

    public Pipe exchange(RemoteTransaction txn, Pipe pipe) throws IOException;

    public Pipe insert(RemoteTransaction txn, Pipe pipe) throws IOException;

    public Pipe replace(RemoteTransaction txn, Pipe pipe) throws IOException;

    public Pipe update(RemoteTransaction txn, Pipe pipe) throws IOException;

    public Pipe merge(RemoteTransaction txn, Pipe pipe) throws IOException;

    public Pipe tryDelete(RemoteTransaction txn, Pipe pipe) throws IOException;

    public Pipe row(RemoteUpdater updater, Pipe pipe) throws IOException;

    public Pipe step(RemoteUpdater updater, Pipe pipe) throws IOException;

    public Pipe update(RemoteUpdater updater, Pipe pipe) throws IOException;

    public Pipe delete(RemoteUpdater updater, Pipe pipe) throws IOException;
}
