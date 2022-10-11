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

import org.cojen.tupl.Database;
import org.cojen.tupl.Index;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.diag.DatabaseStats;

import static org.cojen.tupl.remote.RemoteUtils.*;

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
        return ServerIndex.from(mDb.openIndex(name));
    }

    @Override
    public RemoteIndex findIndex(byte[] name) throws IOException {
        Index ix = mDb.findIndex(name);
        return ix == null ? null : ServerIndex.from(ix);
    }

    @Override
    public RemoteIndex indexById(long id) throws IOException {
        Index ix = mDb.indexById(id);
        return ix == null ? null : ServerIndex.from(ix);
    }

    @Override
    public RemoteTable openTable(String typeName) throws IOException {
        // FIXME: openTable
        throw null;
    }

    @Override
    public RemoteTable findTable(String typeName) throws IOException {
        // FIXME: findTable
        throw null;
    }

    @Override
    public void renameIndex(RemoteIndex index, byte[] newName) throws IOException {
        mDb.renameIndex(((ServerIndex) index).mView, newName);
    }

    @Override
    public RemoteRunnable deleteIndex(RemoteIndex index) throws IOException {
        // FIXME: deleteIndex
        throw null;
    }

    @Override
    public RemoteIndex newTemporaryIndex() throws IOException {
        return ServerIndex.from(mDb.newTemporaryIndex());
    }

    @Override
    public RemoteView indexRegistryByName() throws IOException {
        return ServerIndex.from(mDb.indexRegistryByName());
    }

    @Override
    public RemoteView indexRegistryById() throws IOException {
        return ServerIndex.from(mDb.indexRegistryById());
    }

    @Override
    public RemoteTransaction newTransaction() {
        return ServerTransaction.from(mDb.newTransaction());
    }

    @Override
    public RemoteTransaction newTransaction(byte durabilityMode) {
        return ServerTransaction.from(mDb.newTransaction(toDurabilityMode(durabilityMode)));
    }

    @Override
    public RemoteTransaction bogus() {
        return ServerTransaction.from(Transaction.BOGUS);
    }

    @Override
    public RemoteCustomHandler customWriter(String name) throws IOException {
        // FIXME: customWriter
        throw null;
    }

    @Override
    public RemotePrepareHandler prepareWriter(String name) throws IOException {
        // FIXME: prepareWriter
        throw null;
    }

    @Override
    public RemoteSorter newSorter() {
        // FIXME: newSorter
        throw null;
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
    public RemoteSnapshot beginSnapshot() throws IOException {
        // FIXME: beginSnapshot
        throw null;
    }

    @Override
    public Pipe createCachePrimer(Pipe pipe) throws IOException {
        try {
            mDb.createCachePrimer(pipe.outputStream());
            pipe.flush();
        } finally {
            pipe.close();
        }
        return null;
    }

    @Override
    public Pipe applyCachePrimer(Pipe pipe) throws IOException {
        try {
            mDb.applyCachePrimer(pipe.inputStream());
        } finally {
            pipe.close();
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
    public RemoteSuspend suspendControl() {
        // FIXME: suspendControl
        throw null;
    }

    @Override
    public RemoteLock commitLock() {
        // FIXME: commitLock
        throw null;
    }

    @Override
    public boolean compactFile(RemoteCompactionObserver observer, double target)
        throws IOException
    {
        // FIXME: compactFile
        throw null;
    }

    @Override
    public boolean verify(RemoteVerificationObserver observer) throws IOException {
        // FIXME: verify
        throw null;
    }

    @Override
    public boolean isLeader() {
        return mDb.isLeader();
    }

    @Override
    public void uponLeader(RemoteRunnable acquired, RemoteRunnable lost) {
        // FIXME: uponLeader
        throw null;
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
