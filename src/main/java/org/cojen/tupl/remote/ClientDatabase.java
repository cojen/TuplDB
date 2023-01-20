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

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;

import java.util.concurrent.locks.Lock;

import java.util.function.Function;

import org.cojen.dirmi.ClosedException;
import org.cojen.dirmi.Environment;
import org.cojen.dirmi.Pipe;
import org.cojen.dirmi.RemoteException;

import org.cojen.tupl.Database;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.Index;
import org.cojen.tupl.Server;
import org.cojen.tupl.Snapshot;
import org.cojen.tupl.Sorter;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.View;

import org.cojen.tupl.diag.CompactionObserver;
import org.cojen.tupl.diag.DatabaseStats;
import org.cojen.tupl.diag.VerificationObserver;

import org.cojen.tupl.ext.CustomHandler;
import org.cojen.tupl.ext.PrepareHandler;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.rows.ArrayKey;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public final class ClientDatabase implements Database {
    public static ClientDatabase from(RemoteDatabase remote) throws RemoteException {
        return new ClientDatabase(remote, null);
    }

    public static ClientDatabase connect(SocketAddress addr, long... tokens) throws IOException {
        Environment env = RemoteUtils.createEnvironment();

        env.connector(session -> {
            SocketAddress address = session.remoteAddress();
            if (address instanceof InetSocketAddress) {
                var s = new Socket();
                s.connect(address);
                initConnection(s.getInputStream(), s.getOutputStream(), tokens);
                session.connected(s);
            } else {
                SocketChannel s = SocketChannel.open(address);
                initConnection(Channels.newInputStream(s), Channels.newOutputStream(s), tokens);
                session.connected(s);
            }
        });

        var remote = env.connect(RemoteDatabase.class, Database.class.getName(), addr).root();

        return new ClientDatabase(remote, env);
    }

    private static void initConnection(InputStream in, OutputStream out, long... tokens)
        throws IOException
    {
        out.write(RemoteUtils.encodeConnectHeader(tokens));
        out.flush();

        if (!RemoteUtils.testConnection(in, null, tokens)) {
            throw new IOException("Connection rejected");
        }
    }

    private final RemoteDatabase mRemote;
    private final Environment mEnv;

    final ClientTransaction mBogus;

    private ClientDatabase(RemoteDatabase remote, Environment env) throws RemoteException {
        mRemote = remote;
        mEnv = env;
        mBogus = ClientTransaction.from(this, remote.bogus(), null);
    }

    @Override
    public Index openIndex(byte[] name) throws IOException {
        return findIndex(name.clone(), true);
    }

    @Override
    public Index findIndex(byte[] name) throws IOException {
        return findIndex(name.clone(), false);
    }

    private ClientIndex findIndex(byte[] name, boolean open) throws IOException {
        return ClientCache.get(ArrayKey.make(this, name), key -> {
            RemoteIndex rindex;
            try {
                rindex = open ? mRemote.openIndex(name) : mRemote.findIndex(name);
            } catch (IOException e) {
                throw Utils.rethrow(e);
            }

            if (rindex == null) {
                return null;
            }

            // Prefer a canonical client instance.

            var factory = new Function<Pair<ClientDatabase, Long>, ClientIndex>() {
                boolean invoked;

                @Override
                public ClientIndex apply(Pair<ClientDatabase, Long> key2) {
                    invoked = true;
                    return new ClientIndex(ClientDatabase.this, rindex);
                }
            };

            ClientIndex cindex = ClientCache.get(new Pair<>(this, rindex.id()), factory);

            if (!factory.invoked) {
                // An existing ClientIndex was selected, so dispose the unused RemoteIndex.
                try {
                    rindex.dispose();
                } catch (IOException e) {
                    // Ignore.
                }
            }

            return cindex;
        });
    }

    @Override
    public ClientIndex indexById(long id) throws IOException {
        return ClientCache.get(new Pair<>(this, id), key -> {
            RemoteIndex rindex;
            try {
                rindex = mRemote.indexById(id);
            } catch (IOException e) {
                throw Utils.rethrow(e);
            }

            return rindex == null ? null : new ClientIndex(this, rindex);
        });
    }

    @Override
    public void renameIndex(Index index, byte[] newName) throws IOException {
        mRemote.renameIndex(remoteIndex(index), newName);
    }

    @Override
    public Runnable deleteIndex(Index index) throws IOException {
        // FIXME: deleteIndex
        throw null;
    }

    @Override
    public Index newTemporaryIndex() throws IOException {
        RemoteIndex ix = mRemote.newTemporaryIndex();

        return new ClientIndex(this, ix) {
            @Override
            public void close() throws IOException {
                mRemote.close();
            }
        };
    }

    @Override
    public View indexRegistryByName() throws IOException {
        return indexRegistry(true);
    }

    @Override
    public View indexRegistryById() throws IOException {
        return indexRegistry(false);
    }

    private View indexRegistry(boolean byName) throws IOException {
        return ClientCache.get(new Pair<>(this, byName), key -> {
            RemoteView rview;
            try {
                rview = byName ? mRemote.indexRegistryByName() : mRemote.indexRegistryById();
            } catch (IOException e) {
                throw Utils.rethrow(e);
            }

            return new ClientView<>(this, rview);
        });
    }

    @Override
    public Transaction newTransaction() {
        return ClientTransaction.from(this, newRemoteTransaction(), null);
    }

    RemoteTransaction newRemoteTransaction() {
        return mRemote.newTransaction();
    }

    @Override
    public Transaction newTransaction(DurabilityMode dm) {
        return ClientTransaction.from(this, newRemoteTransaction(dm), dm);
    }

    RemoteTransaction newRemoteTransaction(DurabilityMode dm) {
        return mRemote.newTransaction(dm);
    }

    @Override
    public CustomHandler customWriter(String name) throws IOException {
        // FIXME: customWriter
        throw null;
    }

    @Override
    public PrepareHandler prepareWriter(String name) throws IOException {
        // FIXME: prepareWriter
        throw null;
    }

    @Override
    public Sorter newSorter() {
        // FIXME: newSorter
        throw null;
    }

    @Override
    public long preallocate(long bytes) throws IOException {
        return mRemote.preallocate(bytes);
    }

    @Override
    public long capacityLimit() {
        return mRemote.capacityLimit();
    }

    @Override
    public Snapshot beginSnapshot() throws IOException {
        return new ClientSnapshot(mRemote.beginSnapshot());
    }

    @Override
    public void createCachePrimer(OutputStream out) throws IOException {
        Pipe pipe = mRemote.createCachePrimer(null);
        try {
            pipe.flush();
            pipe.inputStream().transferTo(out);
        } finally {
            pipe.close();
        }
    }

    @Override
    public void applyCachePrimer(InputStream in) throws IOException {
        Pipe pipe = mRemote.applyCachePrimer(null);
        try {
            pipe.flush();
            in.transferTo(pipe.outputStream());
            pipe.flush();
        } finally {
            pipe.close();
        }
    }

    @Override
    public Server newServer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public DatabaseStats stats() {
        return mRemote.stats();
    }

    @Override
    public void flush() throws IOException {
        mRemote.flush();
    }

    @Override
    public void sync() throws IOException {
        mRemote.sync();
    }

    @Override
    public void checkpoint() throws IOException {
        mRemote.checkpoint();
    }

    @Override
    public void suspendCheckpoints() {
        // FIXME: suspendCheckpoints
        throw null;
    }

    @Override
    public void resumeCheckpoints() {
        // FIXME: resumeCheckpoints
        throw null;
    }

    @Override
    public Lock commitLock() {
        // FIXME: commitLock
        throw null;
    }

    @Override
    public boolean compactFile(CompactionObserver observer, double target) throws IOException {
        // FIXME: compactFile
        throw null;
    }

    @Override
    public boolean verify(VerificationObserver observer) throws IOException {
        // FIXME: verify
        throw null;
    }

    @Override
    public boolean isLeader() {
        return mRemote.isLeader();
    }

    @Override
    public void uponLeader(Runnable acquired, Runnable lost) {
        // FIXME: uponLeader
        throw null;
    }

    @Override
    public boolean failover() throws IOException {
        return mRemote.failover();
    }

    @Override
    public void close(Throwable cause) throws IOException {
        dispose();
    }

    @Override
    public boolean isClosed() {
        try {
            return mRemote.isClosed();
        } catch (Exception e) {
            if (e instanceof ClosedException) {
                return true;
            }
            throw e;
        }
    }

    @Override
    public void shutdown() throws IOException {
        dispose();
    }

    private void dispose() throws RemoteException {
        if (mEnv != null) {
            mEnv.close();
        } else {
            mRemote.dispose();
            mBogus.mRemote.dispose();
        }
    }

    RemoteIndex remoteIndex(Index ix) {
        if (ix == null) {
            return null;
        } else if (ix instanceof ClientIndex) {
            var ci = (ClientIndex) ix;
            if (ci.mDb == this) {
                return ci.mRemote;
            }
        }
        throw new IllegalStateException("Index belongs to a different database");
    }

    RemoteTransaction remoteTransaction(Transaction txn) {
        if (txn == null) {
            return null;
        } else if (txn instanceof ClientTransaction) {
            var ct = (ClientTransaction) txn;
            if (ct.mDb == this) {
                return ct.mRemote;
            }
        }
        throw new IllegalStateException("Transaction belongs to a different database");
    }
}
