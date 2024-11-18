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

import org.cojen.dirmi.Pipe;

import org.cojen.tupl.Database;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.Index;
import org.cojen.tupl.Snapshot;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.diag.DatabaseStats;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public final class ServerDatabase implements RemoteDatabase {
    public static ServerDatabase from(Database db) {
        return new ServerDatabase(db);
    }

    private final Database mDb;

    private ServerDatabase(Database db) {
        mDb = db;
    }

    @Override
    public RemoteIndex openIndex(byte[] name) throws IOException {
        return new ServerIndex(mDb.openIndex(name));
    }

    @Override
    public RemoteIndex findIndex(byte[] name) throws IOException {
        Index ix = mDb.findIndex(name);
        return ix == null ? null : new ServerIndex(ix);
    }

    @Override
    public RemoteIndex indexById(long id) throws IOException {
        Index ix = mDb.indexById(id);
        return ix == null ? null : new ServerIndex(ix);
    }

    @Override
    public void renameIndex(RemoteIndex index, byte[] newName) throws IOException {
        mDb.renameIndex(((ServerIndex) index).mView, newName);
    }

    @Override
    public RemoteDeleteIndex deleteIndex(RemoteIndex remote) throws IOException {
        var server = (ServerIndex) remote;
        var del = new ServerDeleteIndex(mDb.deleteIndex(server.mView));
        if (server instanceof ServerTemporaryIndex temp) {
            temp.deleted();
        }
        return del;
    }

    @Override
    public RemoteIndex newTemporaryIndex() throws IOException {
        return new ServerTemporaryIndex(mDb, mDb.newTemporaryIndex());
    }

    @Override
    public RemoteView indexRegistryByName() throws IOException {
        return new ServerView<>(mDb.indexRegistryByName());
    }

    @Override
    public RemoteView indexRegistryById() throws IOException {
        return new ServerView<>(mDb.indexRegistryById());
    }

    @Override
    public RemoteTransaction newTransaction() {
        return new ServerTransaction(mDb.newTransaction());
    }

    @Override
    public RemoteTransaction newTransaction(DurabilityMode dm) {
        return new ServerTransaction(mDb.newTransaction(dm));
    }

    @Override
    public RemoteTransaction bogus() {
        return new ServerTransaction(Transaction.BOGUS);
    }

    @Override
    public RemoteCustomHandler customWriter(String name) throws IOException {
        return new ServerCustomHandler(mDb.customWriter(name));
    }

    @Override
    public RemotePrepareHandler prepareWriter(String name) throws IOException {
        return new ServerPrepareHandler(mDb.prepareWriter(name));
    }

    @Override
    public RemoteSorter newSorter() {
        return new ServerSorter(mDb, mDb.newSorter());
    }

    @Override
    public long preallocate(long bytes) throws IOException {
        return mDb.preallocate(bytes);
    }

    @Override
    public long capacityLimit() {
        return mDb.capacityLimit();
    }

    @Override
    public Map beginSnapshot() throws IOException {
        Snapshot snapshot = mDb.beginSnapshot();
        return Map.of("snapshot", new ServerSnapshot(snapshot),
                      "length", snapshot.length(),
                      "position", snapshot.position(),
                      "isCompressible", snapshot.isCompressible());
    }

    @Override
    public Pipe createCachePrimer(Pipe pipe) throws IOException {
        try (pipe) {
            mDb.createCachePrimer(pipe.outputStream());
            pipe.flush();
        }
        return null;
    }

    @Override
    public Pipe applyCachePrimer(Pipe pipe) throws IOException {
        try (pipe) {
            mDb.applyCachePrimer(pipe.inputStream());
        }
        return null;
    }

    @Override
    public DatabaseStats stats() {
        return mDb.stats();
    }

    @Override
    public void flush() throws IOException {
        mDb.flush();
    }

    @Override
    public void sync() throws IOException {
        mDb.sync();
    }

    @Override
    public void checkpoint() throws IOException {
        mDb.checkpoint();
    }

    @Override
    public boolean compactFile(int flags, RemoteCompactionObserver remote, double target)
        throws IOException
    { 
        return CompactionObserverRelay.compactFile(flags, mDb, remote, target);
    }

    @Override
    public boolean verify(int flags, RemoteVerificationObserver remote, int numThreads)
        throws IOException
    {
        return VerificationObserverRelay.verify(flags, remote, obs -> mDb.verify(obs, numThreads));
    }

    @Override
    public boolean isLeader() {
        return mDb.isLeader();
    }

    @Override
    public RemoteLeaderNotification uponLeader(RemoteRunnable acquired, RemoteRunnable lost) {
        mDb.uponLeader(new ClientRunnable(acquired), new ClientRunnable(lost));

        return new RemoteLeaderNotification() {
            @Override
            public void dispose() {
                // Nothing to do.
            }
        };
    }

    @Override
    public boolean failover() throws IOException {
        return mDb.failover();

    }

    @Override
    public void close() throws IOException {
        mDb.close();
    }

    @Override
    public void close(Throwable cause) throws IOException {
        mDb.close(cause);
    }

    @Override
    public boolean isClosed() {
        return mDb.isClosed();
    }

    @Override
    public void shutdown() throws IOException {
        mDb.shutdown();
    }

    @Override
    public void dispose() {
    }
}
