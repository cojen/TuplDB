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

import java.util.Objects;

import java.util.concurrent.locks.Lock;

import javax.net.ssl.SSLContext;

import org.cojen.dirmi.ClosedException;
import org.cojen.dirmi.Environment;
import org.cojen.dirmi.Pipe;
import org.cojen.dirmi.RemoteException;
import org.cojen.dirmi.Session;

import org.cojen.tupl.ClosedIndexException;
import org.cojen.tupl.Database;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.Index;
import org.cojen.tupl.Server;
import org.cojen.tupl.Snapshot;
import org.cojen.tupl.Sorter;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.View;

import org.cojen.tupl.core.TupleKey;

import org.cojen.tupl.diag.CompactionObserver;
import org.cojen.tupl.diag.DatabaseStats;
import org.cojen.tupl.diag.VerificationObserver;

import org.cojen.tupl.ext.CustomHandler;
import org.cojen.tupl.ext.PrepareHandler;

import org.cojen.tupl.io.Utils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public final class ClientDatabase implements Database {
    public static ClientDatabase from(RemoteDatabase remote) throws RemoteException {
        return new ClientDatabase(remote, null);
    }

    public static ClientDatabase connect(SocketAddress addr, SSLContext context, long... tokens)
        throws IOException
    {
        Environment env = RemoteUtils.createEnvironment();
        env.connector(session -> connect(session, context, tokens));
        var remote = env.connect(RemoteDatabase.class, Database.class.getName(), addr).root();
        return new ClientDatabase(remote, env);
    }

    private static void connect(Session session, SSLContext context, long... tokens)
        throws IOException
    {
        SocketAddress address = session.remoteAddress();

        Socket s;
        if (context != null) {
            s = context.getSocketFactory().createSocket();
        } else if (address instanceof InetSocketAddress) {
            s = new Socket();
        } else {
            SocketChannel sc = SocketChannel.open(address);
            initConnection(Channels.newInputStream(sc), Channels.newOutputStream(sc), tokens);
            session.connected(sc);
            return;
        }

        s.connect(address);
        initConnection(s.getInputStream(), s.getOutputStream(), tokens);
        session.connected(s);
    }

    static void initConnection(InputStream in, OutputStream out, long... tokens)
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
        mBogus = new ClientTransaction(this, remote.bogus(), null);
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
        return ClientCache.get(TupleKey.make.with(this, name), key -> {
            RemoteIndex rindex;
            try {
                rindex = open ? mRemote.openIndex(name) : mRemote.findIndex(name);
            } catch (IOException e) {
                throw Utils.rethrow(e);
            }

            return rindex == null ? null : new ClientIndex(this, rindex);
        });
    }

    @Override
    public ClientIndex indexById(long id) throws IOException {
        return ClientCache.get(TupleKey.make.with(this, id), key -> {
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
    public ClientDeleteIndex deleteIndex(Index index) throws IOException {
        Objects.requireNonNull(index);
        RemoteDeleteIndex remote = mRemote.deleteIndex(remoteIndex(index));
        ClientCache.remove(index);
        return new ClientDeleteIndex((ClientIndex) index, remote);
    }

    @Override
    public Index newTemporaryIndex() throws IOException {
        return new ClientIndex.Temp(this, mRemote.newTemporaryIndex());
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
        return ClientCache.get(TupleKey.make.with(this, byName), key -> {
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
        return new ClientTransaction(this, newRemoteTransaction(), null);
    }

    RemoteTransaction newRemoteTransaction() {
        return mRemote.newTransaction();
    }

    @Override
    public Transaction newTransaction(DurabilityMode dm) {
        return new ClientTransaction(this, newRemoteTransaction(dm), dm);
    }

    RemoteTransaction newRemoteTransaction(DurabilityMode dm) {
        return mRemote.newTransaction(dm);
    }

    @Override
    public CustomHandler customWriter(String name) throws IOException {
        return ClientCache.get(TupleKey.make.with(CustomHandler.class, this, name), key -> {
            RemoteCustomHandler handler;
            try {
                handler = mRemote.customWriter(name);
            } catch (IOException e) {
                throw Utils.rethrow(e);
            }

            return new ClientCustomHandler(this, handler);
        });
    }

    @Override
    public PrepareHandler prepareWriter(String name) throws IOException {
        return ClientCache.get(TupleKey.make.with(PrepareHandler.class, this, name), key -> {
            RemotePrepareHandler handler;
            try {
                handler = mRemote.prepareWriter(name);
            } catch (IOException e) {
                throw Utils.rethrow(e);
            }

            return new ClientPrepareHandler(this, handler);
        });
    }

    @Override
    public Sorter newSorter() {
        return new ClientSorter(this);
    }

    RemoteSorter newRemoteSorter() throws RemoteException {
        return mRemote.newSorter();
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
        try (Pipe pipe = mRemote.createCachePrimer(null)) {
            pipe.flush();
            pipe.inputStream().transferTo(out);
        }
    }

    @Override
    public void applyCachePrimer(InputStream in) throws IOException {
        try (Pipe pipe = mRemote.applyCachePrimer(null)) {
            pipe.flush();
            in.transferTo(pipe.outputStream());
            pipe.flush();
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
        /*
          Suspend/resume cannot be safely controlled remotely. Suspend is intended to be
          called during a critical operation, but if the remote connection is dropped, the
          resume operation must be performed automatically to be safe. The client then has no
          way of immediately knowing that checkpoints are no longer suspended, and this can
          break the critical operation.

          If suspend/resume was transactional, and the critical operation was also
          transactional, suspend/resume could be implemented safely. The feature is intended
          for low-level coordination between two different systems, and so making it fully
          transactional isn't always feasible.
        */
        throw new UnsupportedOperationException();
    }

    @Override
    public void resumeCheckpoints() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Lock commitLock() {
        /*
          A commitLock cannot be safely controlled remotely. The commitLock is intended to be
          used during a critical operation, but if the remote connection is dropped, the
          commitLock must be released automatically to be safe. The client then has no way of
          immediately knowing that the commitLock is no longer held, and this can break the
          critical operation.

          If the commitLock was transactional, and the critical operation was also
          transactional, the commitLock could be implemented safely. The feature is intended
          for low-level coordination between two different systems, and so making it fully
          transactional isn't always feasible.
         */
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean compactFile(CompactionObserver observer, double target) throws IOException {
        var server = ServerCompactionObserver.make(this, observer);
        return server.check(mRemote.compactFile(server.flags(), server, target));
    }

    @Override
    public boolean verify(VerificationObserver observer, int numThreads) throws IOException {
        var server = ServerVerificationObserver.make(this, observer);
        return server.check(mRemote.verify(server.flags(), server, numThreads));
    }

    @Override
    public boolean isLeader() {
        return mRemote.isLeader();
    }

    @Override
    public void uponLeader(Runnable acquired, Runnable lost) {
        var serverAcquired = new ServerRunnable(acquired);
        var serverLost = new ServerRunnable(lost);
        if (!uponLeader(serverAcquired, serverLost)) {
            Session.access(mRemote).addStateListener((session, exception) -> {
                return session.state() != Session.State.CONNECTED
                    || !uponLeader(serverAcquired, serverLost);
            });
        }
    }

    /**
     * @return true if the remote call succeeded
     */
    private boolean uponLeader(ServerRunnable acquired, ServerRunnable lost) {
        RemoteLeaderNotification notification;
        try {
            notification = mRemote.uponLeader(acquired, lost);
        } catch (RemoteException e) {
            return false;
        }

        // The notification object can be disposed when leadership is lost because it's an edge
        // triggered event. The acquired notification is level triggered and it's invoked
        // immediately if the database is already the leader. It's not valid to receive a lost
        // notification before an acquired notification, except in cases when leadership is
        // immediately lost. The notification object can be disposed of regardless.

        lost.finishTask(() -> {
            try {
                notification.dispose();
            } catch (RemoteException e) {
                // Ignore
            }
        });

        return true;
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

    Session session() {
        return Session.access(mRemote);
    }

    RemoteIndex remoteIndex(Index ix) throws ClosedIndexException {
        if (ix == null) {
            return null;
        } else if (ix instanceof ClientIndex ci) {
            ci.checkClosed();
            if (ci.mDb == this) {
                return ci.mRemote;
            }
        }
        throw new IllegalStateException("Index belongs to a different database");
    }

    RemoteTransaction remoteTransaction(Transaction txn) {
        if (txn == null) {
            return null;
        } else if (txn instanceof ClientTransaction ct) {
            if (ct.mDb == this) {
                return ct.remote();
            }
        } else if (txn.isBogus()) {
            return mBogus.mRemote;
        }
        throw new IllegalStateException("Transaction belongs to a different database");
    }
}
