/*
 *  Copyright (C) 2017 Cojen.org
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
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.repl;

import java.io.Closeable;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.IOException;
import java.io.OutputStream;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import java.util.concurrent.ConcurrentHashMap;

import java.util.function.Consumer;
import java.util.function.LongPredicate;

import java.util.zip.CRC32C;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;

import org.cojen.tupl.core.Scheduler;

import org.cojen.tupl.util.Latch;

import static org.cojen.tupl.io.Utils.closeQuietly;
import static org.cojen.tupl.io.Utils.decodeIntLE;
import static org.cojen.tupl.io.Utils.decodeLongLE;
import static org.cojen.tupl.io.Utils.encodeIntLE;
import static org.cojen.tupl.io.Utils.encodeLongLE;
import static org.cojen.tupl.io.Utils.readFully;
import static org.cojen.tupl.io.Utils.rethrow;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class ChannelManager {
    /*
      New connection header structure: (little endian fields)

      0:  Magic number (long)
      8:  Group id (long)
      16: Member id (long)
      24: Connection type (int)
      28: Group token 1 (long)
      36: Group token 2 (long)

      Connection type is TYPE_*. Bit 0 enables CRC checks for TYPE_CONTROL.

      Command header structure: (little endian fields)

      0:  Command length (3 bytes)  -- excludes the 8-byte command header itself
      3:  Opcode (byte)
      4:  CRC (int)

    */

    static final long MAGIC_NUMBER = 2825672906279293275L;

    private static final int GROUP_ID = 8, MEMBER_ID = 16, CONNECTION_TYPE = 24,
        GROUP_TOKEN_1 = 28, GROUP_TOKEN_2 = 36;
    private static final int INIT_HEADER_SIZE = GROUP_TOKEN_2 + 8;

    static final int TYPE_PLAIN = 0, TYPE_JOIN = 2, TYPE_CONTROL = 4, TYPE_SNAPSHOT = 6;

    private static final int CONNECT_TIMEOUT_MILLIS = 5000;
    private static final int MIN_RECONNECT_DELAY_MILLIS = 10;
    private static final int MAX_RECONNECT_DELAY_MILLIS = 1000;
    private static final int INITIAL_READ_TIMEOUT_MILLIS = 5000;
    private static final int WRITE_CHECK_DELAY_MILLIS = 125;

    // By convention, requests are even and replies are odd.
    private static final int
        OP_NOP             = 0,
        OP_REQUEST_VOTE    = 2,  OP_REQUEST_VOTE_REPLY   = 3,
        OP_QUERY_TERMS     = 4,  OP_QUERY_TERMS_REPLY    = 5,
        OP_QUERY_DATA      = 6,  OP_QUERY_DATA_REPLY     = 7,
        OP_WRITE_DATA      = 8,  OP_WRITE_DATA_REPLY     = 9,
        OP_SYNC_COMMIT     = 10, OP_SYNC_COMMIT_REPLY    = 11,
        OP_COMPACT         = 12,
        OP_SNAPSHOT_SCORE  = 14, OP_SNAPSHOT_SCORE_REPLY = 15,
        OP_UPDATE_ROLE     = 16, OP_UPDATE_ROLE_REPLY    = 17,
        OP_GROUP_VERSION   = 18, OP_GROUP_VERSION_REPLY  = 19,
        OP_GROUP_FILE      = 20, OP_GROUP_FILE_REPLY     = 21,
        OP_LEADER_CHECK    = 22, OP_LEADER_CHECK_REPLY   = 23,
        OP_WRITE_AND_PROXY = 24,
        OP_WRITE_VIA_PROXY = 26,
        OP_QUERY_DATA_REPLY_MISSING = 29, // alternate reply from OP_QUERY_DATA
        OP_FORCE_ELECTION  = 34;

    private static final VarHandle cControlVersionHandle;

    static {
        try {
            cControlVersionHandle =
                MethodHandles.lookup().findVarHandle
                (ChannelManager.class, "mControlVersion", int.class);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    private final SocketFactory mSocketFactory;
    private final Scheduler mScheduler;
    private final long mGroupToken1, mGroupToken2;
    private final boolean mWriteCRCs;
    private final Consumer<Throwable> mUncaughtHandler;
    private final Map<SocketAddress, Peer> mPeerMap;
    private final TreeSet<Peer> mPeerSet;
    private final Set<SocketChannel> mChannels;
    private final TreeMap<Peer, ServerChannel> mPeerServerChannels;
    private final Set<Socket> mFreshSockets;

    private long mGroupId;
    private long mLocalMemberId;
    private ServerSocket mServerSocket;
    private boolean mKeepServerSocket;

    private Channel mLocalServer;

    private volatile Consumer<Socket> mSocketAcceptor;
    private volatile Consumer<Socket> mJoinAcceptor;
    private volatile Consumer<Socket> mSnapshotRequestAcceptor;

    volatile boolean mPartitioned;

    private volatile int mControlVersion;

    /**
     * @param factory optional
     * @param writeCRCs true to write CRCs for all control commands written by client connections
     */
    ChannelManager(SocketFactory factory, Scheduler scheduler,
                   long groupToken1, long groupToken2, long groupId,
                   boolean writeCRCs, Consumer<Throwable> uncaughtHandler)
    {
        if (scheduler == null || uncaughtHandler == null) {
            throw new IllegalArgumentException();
        }
        mSocketFactory = factory;
        mScheduler = scheduler;
        mGroupToken1 = groupToken1;
        mGroupToken2 = groupToken2;
        mWriteCRCs = writeCRCs;
        mUncaughtHandler = uncaughtHandler;
        mPeerMap = new HashMap<>();
        Comparator<Peer> cmp = (a, b) -> Long.compare(a.mMemberId, b.mMemberId);
        mPeerSet = new TreeSet<>(cmp);
        mChannels = ConcurrentHashMap.newKeySet();
        mPeerServerChannels = new TreeMap<>(cmp);
        mFreshSockets = ConcurrentHashMap.newKeySet();
        setGroupId(groupId);
    }

    /**
     * Creates and binds a server socket, which can be passed to the setLocalMemberId method.
     *
     * @param factory optional
     */
    static ServerSocket newServerSocket(ServerSocketFactory factory, SocketAddress listenAddress)
        throws IOException
    {
        ServerSocket ss = factory == null ? new ServerSocket() : factory.createServerSocket();
        try {
            ss.setReuseAddress(true);
            ss.bind(listenAddress);
            return ss;
        } catch (Throwable e) {
            closeQuietly(ss);
            throw e;
        }
    }

    /**
     * Set the local member id to a non-zero value, which cannot be changed later.
     */
    synchronized void setLocalMemberId(long localMemberId, ServerSocket ss) {
        if (localMemberId == 0 || ss == null) {
            throw new IllegalArgumentException();
        }
        if (mLocalMemberId != 0) {
            throw new IllegalStateException();
        }
        mLocalMemberId = localMemberId;
        mServerSocket = ss;
    }

    /**
     * Don't close the ServerSocket when closing this ChannelManager.
     */
    synchronized void keepServerSocket() {
        mKeepServerSocket = true;
    }

    long groupToken1() {
        return mGroupToken1;
    }

    long groupToken2() {
        return mGroupToken2;
    }

    synchronized long groupId() {
        return mGroupId;
    }

    synchronized void setGroupId(long groupId) {
        mGroupId = groupId;
    }

    synchronized long localMemberId() {
        return mLocalMemberId;
    }

    synchronized boolean isStarted() {
        return mLocalServer != null;
    }

    /**
     * Starts accepting incoming channels, but does nothing if already started.
     */
    synchronized boolean start(Channel localServer) throws IOException {
        if (localServer == null) {
            throw new IllegalArgumentException();
        }

        if (mLocalMemberId == 0) {
            throw new IllegalStateException();
        }

        if (mLocalServer != null) {
            return false;
        }

        execute(this::acceptLoop);

        mLocalServer = localServer;

        // Start task.
        checkWrites();

        return true;
    }

    Set<? extends Channel> allChannels() {
        return mChannels;
    }

    void socketAcceptor(Consumer<Socket> acceptor) {
        mSocketAcceptor = acceptor;
    }

    void joinAcceptor(Consumer<Socket> acceptor) {
        mJoinAcceptor = acceptor;
    }

    void snapshotRequestAcceptor(Consumer<Socket> acceptor) {
        mSnapshotRequestAcceptor = acceptor;
    }

    boolean hasSnapshotRequestAcceptor() {
        return mSnapshotRequestAcceptor != null;
    }

    /**
     * Returns true if the set of control connections changed since the last time this check
     * was made.
     *
     * @param limit maximum number of times false can be returned consecutively
     */
    boolean checkControlVersion(int limit) {
        while (true) {
            int version = mControlVersion;
            if (version > 0 || -version >= limit) {
                if (cControlVersionHandle.compareAndSet(this, version, 0)) {
                    return true;
                }
            } else {
                if (cControlVersionHandle.compareAndSet(this, version, version - 1)) {
                    return false;
                }
            }
        }
    }

    private void incrementControlVersion() {
        while (true) {
            int version = mControlVersion;
            int newVersion = Math.max(0, version) + 1;
            if (cControlVersionHandle.compareAndSet(this, version, newVersion)) {
                break;
            }
        }
    }

    /**
     * Stop accepting incoming channels, close all existing channels, and disconnect all remote
     * members.
     */
    void stop() {
        synchronized (this) {
            if (!mKeepServerSocket) {
                closeQuietly(mServerSocket);
            }
            mServerSocket = null;
            mLocalServer = null;
            mPeerServerChannels.clear();
        }

        for (SocketChannel channel : mChannels) {
            channel.disconnect();
        }

        mChannels.clear();

        for (Socket s : mFreshSockets) {
            closeQuietly(s);
        }

        mFreshSockets.clear();
    }

    synchronized boolean isStopped() {
        return mServerSocket == null;
    }

    /**
     * Enable or disable partitioned mode, which simulates a network partition. New connections
     * are rejected and existing connections are closed.
     */
    synchronized void partitioned(boolean enable) {
        mPartitioned = enable;

        if (enable) {
            for (SocketChannel channel : mChannels) {
                channel.closeSocket();
            }
        }
    }

    private void execute(Runnable task) {
        if (!mScheduler.execute(task)) {
            stop();
        }
    }

    private void scheduleMillis(Runnable task, long delayMillis) {
        if (!mScheduler.scheduleMillis(task, delayMillis)) {
            stop();
        }
    }

    /**
     * Immediately return a shared channel to the given peer, which is connected in the
     * background.
     *
     * @throws IllegalStateException if peer is already connected to a different address
     */
    Channel connect(Peer peer, Channel localServer) {
        if (peer.mMemberId == 0 || peer.mAddress == null || localServer == null) {
            throw new IllegalArgumentException();
        }

        ClientChannel client;

        synchronized (this) {
            if (mLocalMemberId == peer.mMemberId) {
                throw new IllegalArgumentException("Cannot connect to self");
            }

            Peer existing = mPeerSet.ceiling(peer); // findGe

            if (existing != null && existing.mMemberId == peer.mMemberId
                && !existing.mAddress.equals(peer.mAddress))
            {
                throw new IllegalStateException("Already connected with a different address");
            }

            client = new ClientChannel(peer, localServer);

            if (mPeerMap.putIfAbsent(peer.mAddress, peer) != null) {
                throw new IllegalStateException("Duplicate address: " + peer);
            }

            mPeerSet.add(peer);
            mChannels.add(client);
        }

        execute(client::connect);

        return client;
    }

    Socket connectPlain(SocketAddress addr) throws IOException {
        return connectSocket(addr, new int[] {TYPE_PLAIN});
    }

    Socket connectSnapshot(SocketAddress addr) throws IOException {
        return connectSocket(addr, new int[] {TYPE_SNAPSHOT});
    }

    private Socket connectSocket(SocketAddress addr, int[] connectionType) throws IOException {
        if (addr == null) {
            throw new IllegalArgumentException();
        }

        Peer peer;
        synchronized (this) {
            peer = mPeerMap.get(addr);
            if (peer == null) {
                throw new ConnectException("Not a group member: " + addr);
            }
        }

        Socket s = doConnect(peer, connectionType);

        if (s == null) {
            throw new ConnectException("Rejected");
        }

        return s;
    }

    /**
     * @param conType initially set to the requested type, and then set to the actual
     * type from the peer; only expected to differ in the CRC bit
     * @return null if peer response was malformed
     */
    Socket doConnect(Peer peer, int[] conType) throws IOException {
        if (mPartitioned) {
            return null;
        }

        Socket s = mSocketFactory == null ? new Socket() : mSocketFactory.createSocket();

        doConnect: try {
            s.connect(peer.mAddress, CONNECT_TIMEOUT_MILLIS);

            long groupId;
            long localMemberId;

            synchronized (this) {
                groupId = mGroupId;
                localMemberId = mLocalMemberId;
            }

            byte[] header = newConnectHeader(groupId, localMemberId, conType[0],
                                             mGroupToken1, mGroupToken2);

            s.getOutputStream().write(header);

            header = readHeader(s, false);
            if (header == null) {
                break doConnect;
            }

            // Verify expected member id.
            if (decodeLongLE(header, MEMBER_ID) != peer.mMemberId) {
                break doConnect;
            }

            int actualType = decodeIntLE(header, CONNECTION_TYPE);

            if ((actualType | 1) != (conType[0] | 1)) {
                break doConnect;
            }

            conType[0] = actualType;

            return s;
        } catch (IOException e) {
            closeQuietly(s);
            throw e;
        }

        closeQuietly(s);
        return null;
    }

    static byte[] newConnectHeader(long groupId, long memberId, int conType,
                                   long groupToken1, long groupToken2)
    {
        var header = new byte[INIT_HEADER_SIZE];
        encodeLongLE(header, 0, MAGIC_NUMBER);
        encodeLongLE(header, GROUP_ID, groupId);
        encodeLongLE(header, MEMBER_ID, memberId);
        encodeIntLE(header, CONNECTION_TYPE, conType);
        encodeLongLE(header, GROUP_TOKEN_1, groupToken1);
        encodeLongLE(header, GROUP_TOKEN_2, groupToken2);

        return header;
    }

    /**
     * Disconnect all channels for the given remote member, and disallow any new connections
     * from it.
     */
    void disconnect(long remoteMemberId) {
        if (remoteMemberId == 0) {
            throw new IllegalArgumentException();
        }

        disconnect(id -> id == remoteMemberId);
    }

    /**
     * Iterates over all the channels and passes the remote member id to the given tester to
     * decide if the member should be disconnected.
     */
    void disconnect(LongPredicate tester) {
        Iterator<SocketChannel> it = mChannels.iterator();
        while (it.hasNext()) {
            SocketChannel channel = it.next();
            long memberId = channel.mPeer.mMemberId;
            if (tester.test(memberId)) {
                it.remove();
                synchronized (this) {
                    removeIfServerChannel(channel);
                    Peer peer = mPeerSet.ceiling(new Peer(memberId)); // findGe
                    if (peer != null && peer.mMemberId == memberId) {
                        mPeerSet.remove(peer);
                        mPeerMap.remove(peer.mAddress);
                    }
                }
                channel.disconnect();
            }
        }
    }

    private synchronized void unregister(SocketChannel channel) {
        mChannels.remove(channel);
        removeIfServerChannel(channel);
    }

    // Caller must be synchronized.
    private void removeIfServerChannel(SocketChannel channel) {
        if (channel instanceof ServerChannel) {
            mPeerServerChannels.remove(channel.mPeer, channel);
        }
    }

    private void acceptLoop() {
        ServerSocket ss;
        Channel localServer;
        synchronized (this) {
            ss = mServerSocket;
            localServer = mLocalServer;
        }

        if (ss == null) {
            return;
        }

        while (true) {
            try {
                doAccept(ss, localServer);
            } catch (Throwable e) {
                synchronized (this) {
                    if (ss != mServerSocket) {
                        return;
                    }
                }

                mUncaughtHandler.accept(e);

                if (ss.isClosed()) {
                    stop();
                    return;
                }

                Thread.yield();
            }
        }
    }

    private void doAccept(final ServerSocket ss, final Channel localServer) throws IOException {
        Socket s = ss.accept();

        if (mPartitioned) {
            closeQuietly(s);
            return;
        }

        ServerChannel server = null;

        try {
            byte[] header = readHeader(s, true);
            if (header == null) {
                return;
            }

            long remoteMemberId = decodeLongLE(header, MEMBER_ID);
            int connectionType = decodeIntLE(header, CONNECTION_TYPE);
            boolean checkCRCs = (connectionType & 1) != 0;
            connectionType &= ~1;

            Consumer<Socket> acceptor = null;

            checkType: {
                switch (connectionType) {
                case TYPE_CONTROL:
                    break checkType;
                case TYPE_PLAIN:
                    acceptor = mSocketAcceptor;
                    break;
                case TYPE_JOIN:
                    acceptor = mJoinAcceptor;
                    break;
                case TYPE_SNAPSHOT:
                    acceptor = mSnapshotRequestAcceptor;
                    break;
                }

                if (acceptor != null) {
                    break checkType;
                }

                closeQuietly(s);
                return;
            }

            ServerChannel existing = null;
            boolean doClose = false;

            prepare: synchronized (this) {
                if (mServerSocket == null) {
                    // Stopped.
                    doClose = true;
                    break prepare;
                }

                Peer peer;
                if (remoteMemberId == 0) {
                    // Anonymous connection.
                    peer = null;
                } else {
                    peer = mPeerSet.ceiling(new Peer(remoteMemberId)); // findGe
                    if (peer == null || peer.mMemberId != remoteMemberId) {
                        // Unknown member.
                        doClose = true;
                        break prepare;
                    }
                }

                if (connectionType == TYPE_CONTROL) {
                    if (peer == null) {
                        // Reject anonymous member control connection.
                        doClose = true;
                        break prepare;
                    }
                    server = new ServerChannel(peer, localServer);
                    mChannels.add(server);
                    existing = mPeerServerChannels.put(peer, server);

                    if (mWriteCRCs) {
                        // Indicate that CRCs will be written and should be checked.
                        connectionType |= 1;
                    }

                    encodeIntLE(header, CONNECTION_TYPE, connectionType);
                }

                encodeLongLE(header, MEMBER_ID, mLocalMemberId);

                // Don't lose the socket if stopped before the task can run. After it runs, the
                // socket is tracked by the acceptor, which might be a ServerChannel.
                mFreshSockets.add(s);
            }

            if (doClose) {
                closeQuietly(s);
                return;
            }

            final var fserver = server;

            if (acceptor == null) {
                acceptor = sock -> fserver.accepted(sock, checkCRCs);
            }

            final var facceptor = acceptor;

            Runnable replyTask = () -> {
                try {
                    mFreshSockets.remove(s);
                    s.getOutputStream().write(header);
                    facceptor.accept(s);
                    return;
                } catch (IOException e) {
                    // Ignore.
                } catch (Throwable e) {
                    mUncaughtHandler.accept(e);
                }
                closeQuietly(s);
                // Closing the ServerChannel also unregisters it.
                closeQuietly(fserver);
            };

            if (existing == null) {
                execute(replyTask);
            } else {
                // Wait for existing connection to close first and then reply, to prevent a
                // flood of active connections. In case no close is received, run a task to
                // explicitly close the existing channel before the remote side times out.
                if (!existing.replaced(replyTask)) {
                    scheduleMillis(existing::close, INITIAL_READ_TIMEOUT_MILLIS / 2);
                }
            }

        } catch (Throwable e) {
            closeQuietly(s);
            closeQuietly(server);
            throw e;
        }
    }

    /**
     * @return null if invalid
     */
    byte[] readHeader(Socket s, boolean accepted) throws JoinException {
        byte[] header;
        try {
            s.setSoTimeout(INITIAL_READ_TIMEOUT_MILLIS);
            header = readHeader(s, accepted, groupId(), mGroupToken1, mGroupToken2);
            s.setSoTimeout(0);
            s.setTcpNoDelay(true);
        } catch (IOException e) {
            closeQuietly(s);
            if (e instanceof JoinException je) {
                throw je;
            }
            return null;
        }

        if (header == null) {
            closeQuietly(s);
        }

        return header;
    }

    /**
     * @return null if invalid
     */
    static byte[] readHeader(Socket s, boolean accepted, long groupId,
                             long groupToken1, long groupToken2)
        throws IOException, JoinException
    {
        InputStream in = s.getInputStream();

        var header = new byte[INIT_HEADER_SIZE];
        readFully(in, header, 0, header.length);

        if (decodeLongLE(header, 0) != MAGIC_NUMBER) {
            return null;
        }

        long gt1 = decodeLongLE(header, GROUP_TOKEN_1);
        long gt2 = decodeLongLE(header, GROUP_TOKEN_2);

        boolean tokenMatch = gt1 == groupToken1 || gt1 == groupToken2
            || gt2 == groupToken1 || gt2 == groupToken2;

        boolean idMatch = decodeLongLE(header, GROUP_ID) == groupId
            || decodeIntLE(header, CONNECTION_TYPE) < TYPE_CONTROL;

        if (!tokenMatch || !idMatch) {
            if (!accepted) {
                throw new JoinException("Group token or id doesn't match",
                                        s.getRemoteSocketAddress());
            }

            // Use illegal identifiers (0) to indicate that group token or id doesn't match.
            encodeLongLE(header, GROUP_ID, 0);
            encodeLongLE(header, MEMBER_ID, 0);
            s.getOutputStream().write(header);

            return null;
        }

        return header;
    }

    private void checkWrites() {
        if (isStopped()) {
            return;
        }

        for (SocketChannel channel : mChannels) {
            int state = channel.mWriteState;
            if (state >= channel.maxWriteTagCount()) {
                if (cWriteStateHandle.compareAndSet(channel, state, 0)) {
                    channel.closeSocket();
                }
            } else if (state >= 1) {
                cWriteStateHandle.compareAndSet(channel, state, state + 1);
            }
        }

        scheduleMillis(this::checkWrites, WRITE_CHECK_DELAY_MILLIS);
    }

    static final VarHandle cWriteStateHandle;

    static {
        try {
            cWriteStateHandle =
                MethodHandles.lookup().findVarHandle
                (SocketChannel.class, "mWriteState", int.class);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    abstract class SocketChannel extends Latch implements Channel, Closeable {
        final Peer mPeer;
        private Channel mLocalServer;
        private volatile Socket mSocket;
        private OutputStream mOut;
        private final CRC32C mOutCRC;
        private ChannelInputStream mIn;
        private int mReconnectDelay;
        private volatile long mConnectAttemptStartedAt;
        private boolean mJoinFailure;

        // 0: not writing;  1: writing;  2+: tagged to be closed due to timeout
        volatile int mWriteState;

        // Probably too small, but start with something.
        private byte[] mWriteBuffer = new byte[128];

        private Runnable mReplacementTask;

        SocketChannel(Peer peer, Channel localServer) {
            mPeer = peer;
            mLocalServer = localServer;
            mConnectAttemptStartedAt = Long.MAX_VALUE;
            mOutCRC = mWriteCRCs ? new CRC32C() : null;
        }

        void connect() {
            InputStream in;
            synchronized (this) {
                in = mIn;
                if (mConnectAttemptStartedAt == Long.MAX_VALUE) {
                    mConnectAttemptStartedAt = System.currentTimeMillis();
                }
            }

            int[] conType = {TYPE_CONTROL};

            if (mOutCRC != null) {
                // Indicate that CRCs will be written and should be checked.
                conType[0] |= 1;
            }

            try {
                connected(doConnect(mPeer, conType), conType[0] < 0);
            } catch (JoinException e) {
                boolean report;
                synchronized (this) {
                    // Only report first failure.
                    report = !mJoinFailure;
                    if (report) {
                        mJoinFailure = true;
                    }
                }
                if (report) {
                    mUncaughtHandler.accept(e);
                }
            } catch (IOException e) {
                // Ignore.
            }

            reconnect(in);
        }

        void reconnect(InputStream existing) {
            Channel localServer;
            Socket s;
            int delay;
            synchronized (this) {
                if (existing != mIn) {
                    // Already reconnected or in progress.
                    return;
                }
                localServer = mLocalServer;
                s = mSocket;
                mSocket = null;
                mOut = null;
                mIn = null;
                delay = Math.max(mReconnectDelay, MIN_RECONNECT_DELAY_MILLIS);
                mReconnectDelay = Math.min(delay << 1, MAX_RECONNECT_DELAY_MILLIS);
            }
            
            incrementControlVersion();
            closeQuietly(s);

            if (localServer != null) {
                scheduleMillis(this::connect, delay);
            }
        }

        /**
         * Called when this channel has been replaced. Run the given task when the channel
         * closes naturally. A separate task should forcibly call close after a timeout has
         * elapsed.
         *
         * @return true of task was immediately executed
         * @throws IllegalStateException if already replaced
         */
        synchronized boolean replaced(Runnable task) {
            if (mReplacementTask != null) {
                throw new IllegalStateException();
            }
            if (mSocket == null) {
                execute(task);
                return true;
            } else {
                mReplacementTask = task;
                return false;
            }
        }

        /**
         * Unregister the channel, disconnect, and don't attempt to reconnect.
         */
        @Override
        public void close() {
            unregister(this);
            disconnect();
        }

        /**
         * Disconnect and don't attempt to reconnect.
         */
        void disconnect() {
            Socket s;
            Runnable task;
            synchronized (this) {
                mLocalServer = null;
                s = mSocket;
                mSocket = null;
                mOut = null;
                mIn = null;
                task = mReplacementTask;
                mReplacementTask = null;
            }

            if (task != null) {
                execute(task);
            }

            incrementControlVersion();
            closeQuietly(s);
        }

        /**
         * Close the socket and reconnect if possible. Reconnect will be attempted by inputLoop.
         */
        void closeSocket() {
            closeQuietly(mSocket);
        }

        void accepted(Socket s, boolean checkCRCs) {
            connected(s, checkCRCs);
        }

        private void connected(Socket s, boolean checkCRCs) {
            Closeable toClose;

            apply: synchronized (this) {
                if (mPartitioned) {
                    toClose = s;
                    break apply;
                }

                OutputStream out;
                ChannelInputStream in;
                try {
                    out = s.getOutputStream();
                    // Initial buffer is probably too small, but start with something.
                    in = new ChannelInputStream(s.getInputStream(), 128, checkCRCs);
                } catch (Throwable e) {
                    toClose = s;
                    break apply;
                }

                incrementControlVersion();
                toClose = mSocket;

                acquireExclusive();
                mSocket = s;
                mOut = out;
                mIn = in;
                mReconnectDelay = 0;
                mConnectAttemptStartedAt = Long.MAX_VALUE;
                mJoinFailure = false;
                releaseExclusive();
                
                execute(this::inputLoop);

                notifyAll();
            }

            closeQuietly(toClose);
        }

        private void inputLoop() {
            Channel localServer;
            ChannelInputStream in;
            synchronized (this) {
                localServer = mLocalServer;
                in = mIn;
            }

            if (localServer == null || in == null) {
                return;
            }

            try {
                while (true) {
                    long header = in.readLongLE();

                    int opAndLength = (int) header;
                    int commandLength = (opAndLength >> 8) & 0xffffff;
                    int op = opAndLength & 0xff;
                    int crc = ((int) (header >> 32)) ^ opAndLength;

                    in.prepareChecksum(commandLength, crc);

                    switch (op) {
                    case OP_NOP:
                        break;
                    case OP_REQUEST_VOTE:
                        localServer.requestVote(this, in.readLongLE(), in.readLongLE(),
                                                in.readLongLE(), in.readLongLE());
                        commandLength -= (8 * 4);
                        break;
                    case OP_REQUEST_VOTE_REPLY:
                        localServer.requestVoteReply(this, in.readLongLE());
                        commandLength -= (8 * 1);
                        break;
                    case OP_FORCE_ELECTION:
                        localServer.forceElection(this);
                        break;
                    case OP_QUERY_TERMS:
                        localServer.queryTerms(this, in.readLongLE(), in.readLongLE());
                        commandLength -= (8 * 2);
                        break;
                    case OP_QUERY_TERMS_REPLY:
                        localServer.queryTermsReply(this, in.readLongLE(),
                                                    in.readLongLE(), in.readLongLE());
                        commandLength -= (8 * 3);
                        break;
                    case OP_QUERY_DATA:
                        localServer.queryData(this, in.readLongLE(), in.readLongLE());
                        commandLength -= (8 * 2);
                        break;
                    case OP_QUERY_DATA_REPLY:
                        long currentTerm = in.readLongLE();
                        long prevTerm = in.readLongLE();
                        long term = in.readLongLE();
                        long position = in.readLongLE();
                        commandLength -= (8 * 4);
                        in.readFully(commandLength);
                        localServer.queryDataReply(this, currentTerm, prevTerm, term, position,
                                                   in.mBuffer, in.mPos, commandLength);
                        in.mPos += commandLength;
                        commandLength = 0;
                        break;
                    case OP_QUERY_DATA_REPLY_MISSING:
                        currentTerm = in.readLongLE();
                        prevTerm = in.readLongLE();
                        term = in.readLongLE();
                        position = in.readLongLE();
                        long endPosition = in.readLongLE();
                        localServer.queryDataReplyMissing(this, currentTerm, prevTerm, term,
                                                          position, endPosition);
                        commandLength -= (8 * 5);
                        break;
                    case OP_WRITE_DATA:
                        prevTerm = in.readLongLE();
                        term = in.readLongLE();
                        position = in.readLongLE();
                        long highestPosition = in.readLongLE();
                        long commitPosition = in.readLongLE();
                        commandLength -= (8 * 5);
                        in.readFully(commandLength);
                        localServer.writeData(this, prevTerm, term, position,
                                              highestPosition, commitPosition,
                                              null, in.mBuffer, in.mPos, commandLength);
                        in.mPos += commandLength;
                        commandLength = 0;
                        break;
                    case OP_WRITE_DATA_REPLY:
                        localServer.writeDataReply(this, in.readLongLE(), in.readLongLE());
                        commandLength -= (8 * 2);
                        break;
                    case OP_WRITE_AND_PROXY:
                        prevTerm = in.readLongLE();
                        term = in.readLongLE();
                        position = in.readLongLE();
                        highestPosition = in.readLongLE();
                        commitPosition = in.readLongLE();
                        commandLength -= (8 * 5);
                        in.readFully(commandLength);
                        localServer.writeDataAndProxy(this, prevTerm, term, position,
                                                      highestPosition, commitPosition,
                                                      null, in.mBuffer, in.mPos, commandLength);
                        in.mPos += commandLength;
                        commandLength = 0;
                        break;
                    case OP_WRITE_VIA_PROXY:
                        prevTerm = in.readLongLE();
                        term = in.readLongLE();
                        position = in.readLongLE();
                        highestPosition = in.readLongLE();
                        commitPosition = in.readLongLE();
                        commandLength -= (8 * 5);
                        in.readFully(commandLength);
                        localServer.writeDataViaProxy(this, prevTerm, term, position,
                                                      highestPosition, commitPosition,
                                                      null, in.mBuffer, in.mPos, commandLength);
                        in.mPos += commandLength;
                        commandLength = 0;
                        break;
                    case OP_SYNC_COMMIT:
                        localServer.syncCommit(this, in.readLongLE(),
                                               in.readLongLE(), in.readLongLE());
                        commandLength -= (8 * 3);
                        break;
                    case OP_SYNC_COMMIT_REPLY:
                        localServer.syncCommitReply(this, in.readLongLE(),
                                                    in.readLongLE(), in.readLongLE());
                        commandLength -= (8 * 3);
                        break;
                    case OP_COMPACT:
                        localServer.compact(this, in.readLongLE());
                        commandLength -= (8 * 1);
                        break;
                    case OP_SNAPSHOT_SCORE:
                        localServer.snapshotScore(this);
                        break;
                    case OP_SNAPSHOT_SCORE_REPLY:
                        localServer.snapshotScoreReply(this, in.readIntLE(),
                                                       Float.intBitsToFloat(in.readIntLE()));
                        commandLength -= (4 * 2);
                        break;
                    case OP_UPDATE_ROLE:
                        localServer.updateRole(this, in.readLongLE(), in.readLongLE(),
                                               Role.decode(in.readByte()));
                        commandLength -= (8 * 2 + 1);
                        break;
                    case OP_UPDATE_ROLE_REPLY:
                        localServer.updateRoleReply(this, in.readLongLE(), in.readLongLE(),
                                                    in.readByte());
                        commandLength -= (8 * 2 + 1);
                        break;
                    case OP_GROUP_VERSION:
                        localServer.groupVersion(this, in.readLongLE());
                        commandLength -= (8 * 1);
                        break;
                    case OP_GROUP_VERSION_REPLY:
                        localServer.groupVersionReply(this, in.readLongLE());
                        commandLength -= (8 * 1);
                        break;
                    case OP_GROUP_FILE:
                        localServer.groupFile(this, in.readLongLE());
                        commandLength -= (8 * 1);
                        break;
                    case OP_GROUP_FILE_REPLY:
                        localServer.groupFileReply(this, in, null);
                        break;
                    case OP_LEADER_CHECK:
                        localServer.leaderCheck(this);
                        break;
                    case OP_LEADER_CHECK_REPLY:
                        localServer.leaderCheckReply(this, in.readLongLE());
                        commandLength -= (8 * 1);
                        break;
                    default:
                        localServer.unknown(this, op);
                        break;
                    }

                    in.skipFully(commandLength);
                }
            } catch (IOException e) {
                // Ignore.
            } catch (Throwable e) {
                mUncaughtHandler.accept(e);
            }

            reconnect(in);
        }

        @Override
        public synchronized boolean isConnected() {
            return mOut != null;
        }

        @Override
        public Peer peer() {
            return mPeer;
        }

        @Override
        public int waitForConnection(int timeoutMillis) throws InterruptedIOException {
            if (timeoutMillis == 0) {
                return 0;
            }

            long end = Long.MIN_VALUE;

            synchronized (this) {
                while (true) {
                    if (mSocket != null || mLocalServer == null) {
                        return timeoutMillis;
                    }

                    try {
                        if (timeoutMillis < 0) {
                            wait();
                        } else {
                            long now = System.currentTimeMillis();
                            if (end == Long.MIN_VALUE) {
                                end = now + timeoutMillis;
                            } else {
                                timeoutMillis = (int) (end - now);
                            }
                            if (timeoutMillis <= 0) {
                                return 0;
                            }
                            wait(timeoutMillis);
                        }
                    } catch (InterruptedException e) {
                        throw new InterruptedIOException();
                    }
                }
            }
        }

        @Override
        public long connectAttemptStartedAt() {
            return mConnectAttemptStartedAt;
        }

        @Override
        public void unknown(Channel from, int op) {
            // Not a normal remote call.
        }

        @Override
        public boolean requestVote(Channel from, long term, long candidateId,
                                   long highestTerm, long highestPosition)
        {
            return writeCommand(OP_REQUEST_VOTE, term, candidateId, highestTerm, highestPosition);
        }

        @Override
        public boolean requestVoteReply(Channel from, long term) {
            return writeCommand(OP_REQUEST_VOTE_REPLY, term);
        }

        @Override
        public boolean forceElection(Channel from) {
            return writeCommand(OP_FORCE_ELECTION);
        }

        @Override
        public boolean queryTerms(Channel from, long startPosition, long endPosition) {
            return writeCommand(OP_QUERY_TERMS, startPosition, endPosition);
        }

        @Override
        public boolean queryTermsReply(Channel from, long prevTerm, long term, long startPosition) {
            return writeCommand(OP_QUERY_TERMS_REPLY, prevTerm, term, startPosition);
        }

        @Override
        public boolean queryData(Channel from, long startPosition, long endPosition) {
            return writeCommand(OP_QUERY_DATA, startPosition, endPosition);
        }

        @Override
        public boolean queryDataReply(Channel from, long currentTerm,
                                      long prevTerm, long term, long position,
                                      byte[] data, int off, int len)
        {
            while (len >= ((1 << 24) - (8 * 4))) {
                // Break it up into several commands.
                int max = (1 << 24) - ((8 * 4) + 1);
                if (!queryDataReply(from, currentTerm, prevTerm, term, position, data, off, max)) {
                    return false;
                }
                position += max;
                off += max;
                len -= max;
            }

            acquireExclusive();
            try {
                OutputStream out = mOut;
                if (out == null) {
                    return false;
                }
                final int commandLength = (8 + 8 * 4) + len;
                byte[] command = allocWriteBuffer(commandLength);
                prepareCommand(command, OP_QUERY_DATA_REPLY, 0, commandLength - 8);
                encodeLongLE(command, 8, currentTerm);
                encodeLongLE(command, 16, prevTerm);
                encodeLongLE(command, 24, term);
                encodeLongLE(command, 32, position);
                System.arraycopy(data, off, command, 40, len);
                return writeCommand(out, command, 0, commandLength);
            } finally {
                releaseExclusive();
            }
        }

        @Override
        public boolean queryDataReplyMissing(Channel from, long currentTerm,
                                             long prevTerm, long term,
                                             long startPosition, long endPosition)
        {
            return writeCommand(OP_QUERY_DATA_REPLY_MISSING,
                                currentTerm, prevTerm, term, startPosition, endPosition);
        }

        @Override
        public boolean writeData(Channel from, long prevTerm, long term, long position,
                                 long highestPos, long commitPos,
                                 byte[] prefix, byte[] data, int off, int len)
        {
            return writeData(OP_WRITE_DATA,
                             prevTerm, term, position, highestPos, commitPos,
                             prefix, data, off, len);
        }

        private boolean writeData(int op, long prevTerm, long term, long position,
                                  long highestPosition, long commitPosition,
                                  byte[] prefix, byte[] data, int off, int len)
        {
            int fullLen = len;

            if (prefix != null) {
                fullLen += prefix.length;
            }

            while (fullLen >= ((1 << 24) - (8 * 5))) {
                // Break it up into several commands.
                if (prefix != null) {
                    if (!writeData(op, prevTerm, term, position, highestPosition, commitPosition,
                                   null, prefix, 0, prefix.length))
                    {
                        return false;
                    }
                    position += prefix.length;
                    prefix = null;
                } else {
                    int max = (1 << 24) - ((8 * 5) + 1);
                    if (!writeData(op, prevTerm, term, position, highestPosition, commitPosition,
                                   null, data, off, max))
                    {
                        return false;
                    }
                    position += max;
                    off += max;
                    len -= max;
                }
                fullLen = len;
            }

            acquireExclusive();
            try {
                OutputStream out = mOut;
                if (out == null) {
                    return false;
                }
                final int commandLength = (8 + 8 * 5) + fullLen;
                byte[] command = allocWriteBuffer(commandLength);
                prepareCommand(command, op, 0, commandLength - 8);
                encodeLongLE(command, 8, prevTerm);
                encodeLongLE(command, 16, term);
                encodeLongLE(command, 24, position);
                encodeLongLE(command, 32, highestPosition);
                encodeLongLE(command, 40, commitPosition);
                int commandOffset = 48;
                if (prefix != null) {
                    System.arraycopy(prefix, 0, command, commandOffset, prefix.length);
                    commandOffset += prefix.length;
                }
                System.arraycopy(data, off, command, commandOffset, len);
                return writeCommand(out, command, 0, commandLength);
            } finally {
                releaseExclusive();
            }
        }

        @Override
        public boolean writeDataReply(Channel from, long term, long highestPosition) {
            return writeCommand(OP_WRITE_DATA_REPLY, term, highestPosition);
        }

        @Override
        public boolean writeDataAndProxy(Channel from, long prevTerm, long term, long position,
                                         long highestPos, long commitPos,
                                         byte[] prefix, byte[] data, int off, int len)
        {
            return writeData(OP_WRITE_AND_PROXY,
                             prevTerm, term, position, highestPos, commitPos,
                             prefix, data, off, len);
        }

        @Override
        public boolean writeDataViaProxy(Channel from, long prevTerm, long term, long position,
                                         long highestPos, long commitPos,
                                         byte[] prefix, byte[] data, int off, int len)
        {
            return writeData(OP_WRITE_VIA_PROXY,
                             prevTerm, term, position, highestPos, commitPos,
                             prefix, data, off, len);
        }

        @Override
        public boolean syncCommit(Channel from, long prevTerm, long term, long position) {
            return writeCommand(OP_SYNC_COMMIT, prevTerm, term, position);
        }

        @Override
        public boolean syncCommitReply(Channel from, long groupVersion, long term, long position) {
            return writeCommand(OP_SYNC_COMMIT_REPLY, groupVersion, term, position);
        }

        @Override
        public boolean compact(Channel from, long position) {
            return writeCommand(OP_COMPACT, position);
        }

        @Override
        public boolean snapshotScore(Channel from) {
            return writeCommand(OP_SNAPSHOT_SCORE);
        }

        @Override
        public boolean snapshotScoreReply(Channel from, int activeSessions, float weight) {
            acquireExclusive();
            try {
                OutputStream out = mOut;
                if (out == null) {
                    return false;
                }
                final int commandLength = 8 + 4 * 2;
                byte[] command = allocWriteBuffer(commandLength);
                prepareCommand(command, OP_SNAPSHOT_SCORE_REPLY, 0, 4 * 2);
                encodeIntLE(command, 8, activeSessions);
                encodeIntLE(command, 12, Float.floatToIntBits(weight));
                return writeCommand(out, command, 0, commandLength);
            } finally {
                releaseExclusive();
            }
        }

        @Override
        public boolean updateRole(Channel from, long groupVersion, long memberId, Role role) {
            return writeCommand(OP_UPDATE_ROLE, groupVersion, memberId, role.mCode);
        }

        @Override
        public boolean updateRoleReply(Channel from,
                                       long groupVersion, long memberId, byte result)
        {
            return writeCommand(OP_UPDATE_ROLE_REPLY, groupVersion, memberId, result);
        }

        @Override
        public boolean groupVersion(Channel from, long groupVersion) {
            return writeCommand(OP_GROUP_VERSION, groupVersion);
        }

        @Override
        public boolean groupVersionReply(Channel from, long groupVersion) {
            return writeCommand(OP_GROUP_VERSION_REPLY, groupVersion);
        }

        @Override
        public boolean groupFile(Channel from, long groupVersion) {
            return writeCommand(OP_GROUP_FILE, groupVersion);
        }

        @Override
        public boolean groupFileReply(Channel from, InputStream unused,
                                      Consumer<OutputStream> consumer) throws IOException
        {
            if (unused != null || consumer == null) {
                throw new IllegalArgumentException();
            }

            acquireExclusive();
            try {
                OutputStream out = mOut;
                if (out != null) {
                    final int commandLength = 8;
                    byte[] command = allocWriteBuffer(commandLength);
                    prepareCommand(command, OP_GROUP_FILE_REPLY, 0, 0);
                    if (writeCommand(out, command, 0, commandLength)) {
                        try {
                            // Called with exclusive latch still held.
                            consumer.accept(out);
                            out.flush();
                            return true;
                        } catch (IOException e) {
                            mOut = null;
                            // Close and let inputLoop attempt to reconnect.
                            closeSocket();
                        }
                    }
                }
            } finally {
                releaseExclusive();
            }

            return false;
        }

        @Override
        public boolean leaderCheck(Channel from) {
            return writeCommand(OP_LEADER_CHECK);
        }

        @Override
        public boolean leaderCheckReply(Channel from, long term) {
            return writeCommand(OP_LEADER_CHECK_REPLY, term);
        }

        private boolean writeCommand(int op) {
            acquireExclusive();
            try {
                OutputStream out = mOut;
                if (out == null) {
                    return false;
                }
                final int commandLength = 8;
                byte[] command = allocWriteBuffer(commandLength);
                prepareCommand(command, op, 0, 0);
                return writeCommand(out, command, 0, commandLength);
            } finally {
                releaseExclusive();
            }
        }

        private boolean writeCommand(int op, long a) {
            acquireExclusive();
            try {
                OutputStream out = mOut;
                if (out == null) {
                    return false;
                }
                final int commandLength = 8 + 8 * 1;
                byte[] command = allocWriteBuffer(commandLength);
                prepareCommand(command, op, 0, 8 * 1);
                encodeLongLE(command, 8, a);
                return writeCommand(out, command, 0, commandLength);
            } finally {
                releaseExclusive();
            }
        }

        private boolean writeCommand(int op, long a, long b) {
            acquireExclusive();
            try {
                OutputStream out = mOut;
                if (out == null) {
                    return false;
                }
                final int commandLength = 8 + 8 * 2;
                byte[] command = allocWriteBuffer(commandLength);
                prepareCommand(command, op, 0, 8 * 2);
                encodeLongLE(command, 8, a);
                encodeLongLE(command, 16, b);
                return writeCommand(out, command, 0, commandLength);
            } finally {
                releaseExclusive();
            }
        }

        private boolean writeCommand(int op, long a, long b, byte c) {
            acquireExclusive();
            try {
                OutputStream out = mOut;
                if (out == null) {
                    return false;
                }
                final int commandLength = 8 + 8 * 2 + 1;
                byte[] command = allocWriteBuffer(commandLength);
                prepareCommand(command, op, 0, 8 * 2 + 1);
                encodeLongLE(command, 8, a);
                encodeLongLE(command, 16, b);
                command[24] = c;
                return writeCommand(out, command, 0, commandLength);
            } finally {
                releaseExclusive();
            }
        }

        private boolean writeCommand(int op, long a, long b, long c) {
            acquireExclusive();
            try {
                OutputStream out = mOut;
                if (out == null) {
                    return false;
                }
                final int commandLength = 8 + 8 * 3;
                byte[] command = allocWriteBuffer(commandLength);
                prepareCommand(command, op, 0, 8 * 3);
                encodeLongLE(command, 8, a);
                encodeLongLE(command, 16, b);
                encodeLongLE(command, 24, c);
                return writeCommand(out, command, 0, commandLength);
            } finally {
                releaseExclusive();
            }
        }

        private boolean writeCommand(int op, long a, long b, long c, long d) {
            acquireExclusive();
            try {
                OutputStream out = mOut;
                if (out == null) {
                    return false;
                }
                final int commandLength = 8 + 8 * 4;
                byte[] command = allocWriteBuffer(commandLength);
                prepareCommand(command, op, 0, 8 * 4);
                encodeLongLE(command, 8, a);
                encodeLongLE(command, 16, b);
                encodeLongLE(command, 24, c);
                encodeLongLE(command, 32, d);
                return writeCommand(out, command, 0, commandLength);
            } finally {
                releaseExclusive();
            }
        }

        private boolean writeCommand(int op, long a, long b, long c, long d, long e) {
            acquireExclusive();
            try {
                OutputStream out = mOut;
                if (out == null) {
                    return false;
                }
                final int commandLength = 8 + 8 * 5;
                byte[] command = allocWriteBuffer(commandLength);
                prepareCommand(command, op, 0, 8 * 5);
                encodeLongLE(command, 8, a);
                encodeLongLE(command, 16, b);
                encodeLongLE(command, 24, c);
                encodeLongLE(command, 32, d);
                encodeLongLE(command, 40, e);
                return writeCommand(out, command, 0, commandLength);
            } finally {
                releaseExclusive();
            }
        }

        /**
         * Caller must hold exclusive latch.
         */
        private byte[] allocWriteBuffer(int size) {
            byte[] buf = mWriteBuffer;
            if (size > buf.length) {
                buf = growWriteBuffer(size);
            }
            return buf;
        }

        /**
         * Caller must hold exclusive latch.
         */
        private byte[] growWriteBuffer(int size) {
            var buf = new byte[Math.max(size, (int) (mWriteBuffer.length * 1.5))];
            mWriteBuffer = buf;
            return buf;
        }

        /**
         * Caller must hold exclusive latch.
         *
         * @param command must have at least 8 bytes, used for the header
         * @param length max allowed is 16,777,215 bytes
         */
        private void prepareCommand(byte[] command, int op, int offset, int length) {
            encodeIntLE(command, offset, (length << 8) | (byte) op);
        }

        /**
         * Caller must hold exclusive latch and have verified that mOut isn't null.
         */
        private boolean writeCommand(OutputStream out, byte[] command, int offset, int length) {
            try {
                CRC32C crc = mOutCRC;
                if (crc != null) {
                    crc.reset();
                    crc.update(command, offset + 8, length - 8);
                    int crcValue = (int) crc.getValue();
                    // Fold in the length and opcode too.
                    crcValue ^= decodeIntLE(command, offset);
                    encodeIntLE(command, offset + 4, crcValue);
                }
                mWriteState = 1;
                out.write(command, offset, length);
                mWriteState = 0;
                return true;
            } catch (IOException e) {
                mOut = null;
                // Close and let inputLoop attempt to reconnect.
                closeSocket();
                return false;
            }
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{peer=" + mPeer + ", socket=" + mSocket + '}';
        }

        /**
         * Effective write timeout varies from (WRITE_CHECK_DELAY_MILLIS * count) to
         * (WRITE_CHECK_DELAY_MILLIS * (count + 1)).
         */
        abstract int maxWriteTagCount();
    }

    final class ClientChannel extends SocketChannel {
        ClientChannel(Peer peer, Channel localServer) {
            super(peer, localServer);
        }

        @Override
        int maxWriteTagCount() {
            // Effective timeout is 250..375ms.
            return 2;
        }
    }

    final class ServerChannel extends SocketChannel {
        ServerChannel(Peer peer, Channel localServer) {
            super(peer, localServer);
        }

        @Override
        void reconnect(InputStream existing) {
            // Don't reconnect.
            close();
        }

        @Override
        int maxWriteTagCount() {
            // Be more lenient with connections requested by remote endpoint, because writes
            // through it don't block critical operations. Effective timeout is 6500..6625ms.
            return 50;
        }
    }
}
