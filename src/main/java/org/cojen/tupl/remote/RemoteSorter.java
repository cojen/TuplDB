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

import org.cojen.dirmi.Disposer;
import org.cojen.dirmi.Pipe;
import org.cojen.dirmi.Remote;
import org.cojen.dirmi.RemoteException;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public interface RemoteSorter extends Remote {
    public void add(byte[] key, byte[] value) throws IOException;

    public Pipe addBatch(Pipe pipe) throws IOException;

    public Pipe addAll(Pipe pipe) throws IOException;

    @Disposer
    public RemoteIndex finish() throws IOException;

    @Disposer
    public Pipe finishScan(boolean reverse, Pipe pipe) throws IOException;

    @Disposer
    public Pipe addAllFinishScan(boolean reverse, Pipe pipe) throws IOException;

    public long progress() throws RemoteException;

    @Disposer
    public void reset() throws IOException;
}
