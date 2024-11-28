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

import java.util.Map;

import org.cojen.dirmi.Batched;
import org.cojen.dirmi.Disposer;
import org.cojen.dirmi.Pipe;
import org.cojen.dirmi.Remote;
import org.cojen.dirmi.RemoteException;
import org.cojen.dirmi.RemoteFailure;
import org.cojen.dirmi.Restorable;

import org.cojen.tupl.DurabilityMode;

import org.cojen.tupl.diag.DatabaseStats;

/**
 * 
 *
 * @author Brian S O'Neill
 * @see ClientDatabase
 * @see ServerDatabase
 */
public interface RemoteDatabase extends Remote, Disposable {
    @Restorable
    public RemoteIndex openIndex(byte[] name) throws IOException;

    @Restorable
    public RemoteIndex findIndex(byte[] name) throws IOException;

    @Restorable
    public RemoteIndex indexById(long id) throws IOException;

    public void renameIndex(RemoteIndex index, byte[] newName) throws IOException;

    public RemoteDeleteIndex deleteIndex(RemoteIndex index) throws IOException;

    public RemoteIndex newTemporaryIndex() throws IOException;

    @Restorable
    public RemoteView indexRegistryByName() throws IOException;

    @Restorable
    public RemoteView indexRegistryById() throws IOException;

    @Batched
    @RemoteFailure(declared=false)
    public RemoteTransaction newTransaction();

    @Batched
    @RemoteFailure(declared=false)
    public RemoteTransaction newTransaction(DurabilityMode dm);

    @Restorable
    public RemoteTransaction bogus() throws RemoteException;

    @Restorable
    public RemoteCustomHandler customWriter(String name) throws IOException;

    @Restorable
    public RemotePrepareHandler prepareWriter(String name) throws IOException;

    public RemoteSorter newSorter() throws RemoteException;

    public long preallocate(long bytes) throws IOException;

    @RemoteFailure(declared=false)
    public long capacityLimit();

    public Map beginSnapshot() throws IOException;

    public Pipe createCachePrimer(Pipe pipe) throws IOException;

    public Pipe applyCachePrimer(Pipe pipe) throws IOException;

    @RemoteFailure(declared=false)
    public DatabaseStats stats();

    public void flush() throws IOException;

    public void sync() throws IOException;

    public void checkpoint() throws IOException;

    /**
     * @param flags bit 1: provide indexNodePassed messages
     */
    public boolean compactFile(int flags, RemoteCompactionObserver observer, double target)
        throws IOException;

    /**
     * @param flags bit 1: provide indexNodePassed messages
     */
    public boolean verify(int flags, RemoteVerificationObserver observer, int numThreads)
        throws IOException;

    @RemoteFailure(declared=false)
    public boolean isLeader();

    @Restorable
    public RemoteLeaderNotification uponLeader(RemoteRunnable acquired, RemoteRunnable lost)
        throws RemoteException;

    public boolean failover() throws IOException;

    @Disposer
    public void close() throws IOException;

    @Disposer
    public void close(Throwable cause) throws IOException;

    @RemoteFailure(declared=false)
    public boolean isClosed();

    @Disposer
    public void shutdown() throws IOException;
}
