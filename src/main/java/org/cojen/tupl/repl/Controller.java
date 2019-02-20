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

import java.io.File;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.IOException;
import java.io.OutputStream;

import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

import java.util.logging.Level;

import javax.net.SocketFactory;

import org.cojen.tupl.util.Latch;
import org.cojen.tupl.util.LatchCondition;

import org.cojen.tupl.io.Utils;
import static org.cojen.tupl.io.Utils.closeQuietly;
import static org.cojen.tupl.io.Utils.encodeLongLE;
import static org.cojen.tupl.io.Utils.rethrow;

/**
 * Core replicator implementation.
 *
 * @author Brian S O'Neill
 */
final class Controller extends Latch implements StreamReplicator, Channel {
    private static final int MODE_FOLLOWER = 0, MODE_CANDIDATE = 1, MODE_LEADER = 2;
    private static final int ELECTION_DELAY_LOW_MILLIS = 200, ELECTION_DELAY_HIGH_MILLIS = 300;
    private static final int QUERY_TERMS_RATE_MILLIS = 1;
    private static final int MISSING_DELAY_LOW_MILLIS = 400, MISSING_DELAY_HIGH_MILLIS = 600;
    private static final int SYNC_COMMIT_RETRY_MILLIS = 100;
    private static final int CONNECT_TIMEOUT_MILLIS = 500;
    private static final int SNAPSHOT_REPLY_TIMEOUT_MILLIS = 5000;
    private static final int JOIN_TIMEOUT_MILLIS = 5000;
    private static final int MISSING_DATA_REQUEST_SIZE = 100_000;
    private static final int SYNC_RATE_LOW_MILLIS = 2000;
    private static final int SYNC_RATE_HIGH_MILLIS = 3000;

    private static final byte CONTROL_OP_JOIN = 1, CONTROL_OP_UPDATE_ROLE = 2,
        CONTROL_OP_UNJOIN = 3;

    private static final byte[] EMPTY_DATA = new byte[0];

    private final BiConsumer<Level, String> mEventListener;
    private final Scheduler mScheduler;
    private final ChannelManager mChanMan;
    private final StateLog mStateLog;

    private final LatchCondition mSyncCommitCondition;

    private final boolean mProxyWrites;

    private GroupFile mGroupFile;

    // Local role as desired, which might not initially match what the GroupFile says.
    private Role mLocalRole;

    // Normal and standby members are required to participate in consensus.
    private Peer[] mConsensusPeers;
    private Channel[] mConsensusChannels;
    // All channels includes observers, which cannot participate in consensus.
    private Channel[] mAllChannels;

    // Reference to leader's reply channel, only used for accessing the peer object.
    private Channel mLeaderReplyChannel;
    private Channel mLeaderRequestChannel;

    private int mLocalMode;

    private long mCurrentTerm;
    private int mGrantsRemaining;
    private int mElectionValidated;
    private long mValidatedTerm;
    private long mLeaderCommitPosition;

    private LogWriter mLeaderLogWriter;
    private ReplWriter mLeaderReplWriter;

    // Position used to check for missing data.
    private long mMissingContigPosition = Long.MAX_VALUE; // unknown initially
    private boolean mSkipMissingDataTask;
    private volatile boolean mReceivingMissingData;

    // Limit the rate at which missing terms are queried.
    private volatile long mNextQueryTermTime = Long.MIN_VALUE;

    private volatile Consumer<byte[]> mControlMessageAcceptor;

    // Incremented when snapshot senders are created, and decremented when they are closed.
    private int mSnapshotSessionCount;

    /**
     * @param factory optional
     * @param localSocket optional; used for testing
     */
    static Controller open(BiConsumer<Level, String> eventListener,
                           StateLog log, long groupToken, File groupFile,
                           SocketFactory factory,
                           SocketAddress localAddress, SocketAddress listenAddress,
                           Role localRole, Set<SocketAddress> seeds, ServerSocket localSocket,
                           boolean proxyWrites)
        throws IOException
    {
        GroupFile gf = GroupFile.open(eventListener, groupFile, localAddress, seeds.isEmpty());
        Controller con = new Controller(eventListener, log, groupToken, gf, factory, proxyWrites);
        con.init(groupFile, localAddress, listenAddress, localRole, seeds, localSocket);
        return con;
    }

    private Controller(BiConsumer<Level, String> eventListener,
                       StateLog log, long groupToken, GroupFile gf, SocketFactory factory,
                       boolean proxyWrites)
        throws IOException
    {
        mEventListener = eventListener;
        mStateLog = log;
        mScheduler = new Scheduler();
        mChanMan = new ChannelManager
            (factory, mScheduler, groupToken, gf == null ? 0 : gf.groupId());
        mGroupFile = gf;
        mSyncCommitCondition = new LatchCondition();
        mProxyWrites = proxyWrites;
    }

    private void init(File groupFile,
                      SocketAddress localAddress, SocketAddress listenAddress,
                      Role localRole, Set<SocketAddress> seeds, ServerSocket localSocket)
        throws IOException
    {
        acquireExclusive();
        try {
            mLocalRole = localRole;

            if (mGroupFile == null) {
                // Need to join the group.

                for (int trials = 2; --trials >= 0; ) {
                    try {
                        GroupJoiner joiner = new GroupJoiner
                            (mEventListener, groupFile, mChanMan.getGroupToken(),
                             localAddress, listenAddress);

                        joiner.join(seeds, JOIN_TIMEOUT_MILLIS);

                        mGroupFile = joiner.mGroupFile;
                        mChanMan.setGroupId(mGroupFile.groupId());
                        break;
                    } catch (JoinException e) {
                        if (trials <= 0) {
                            throw e;
                        }
                        // Try again. Might be a version mismatch race condition caused by
                        // concurrent joins.
                    }
                }
            }

            long localMemberId = mGroupFile.localMemberId();

            if (localMemberId == 0) {
                throw new JoinException("Not in the replication group. Local address \""
                                        + mGroupFile.localMemberAddress() + "\" wasn't found in "
                                        + groupFile.getPath());
            }

            mChanMan.setLocalMemberId(localMemberId, localSocket);

            refreshPeerSet();
        } catch (Throwable e) {
            closeQuietly(localSocket);
            closeQuietly(this);
            throw e;
        } finally {
            releaseExclusive();
        }
    }

    // Caller must hold exclusive latch.
    private void refreshPeerSet() {
        Map<Long, Channel> oldPeerChannels = new HashMap<>();

        if (mAllChannels != null) {
            for (Channel channel : mAllChannels) {
                oldPeerChannels.put(channel.peer().mMemberId, channel);
            }
        }

        Map<Long, Channel> newPeerChannels = new HashMap<>();

        for (Peer peer : mGroupFile.allPeers()) {
            Long memberId = peer.mMemberId;
            newPeerChannels.put(memberId, oldPeerChannels.remove(memberId));
        }

        for (long toRemove : oldPeerChannels.keySet()) {
            mChanMan.disconnect(toRemove);
        }

        List<Peer> consensusPeers = new ArrayList<>();
        List<Channel> consensusChannels = new ArrayList<>();
        List<Channel> allChannels = new ArrayList<>();

        for (Peer peer : mGroupFile.allPeers()) {
            Channel channel = newPeerChannels.get(peer.mMemberId);

            if (channel == null) {
                channel = mChanMan.connect(peer, this);
            }

            allChannels.add(channel);

            if (peer.mRole == Role.NORMAL || peer.mRole == Role.STANDBY) {
                consensusPeers.add(peer);
                consensusChannels.add(channel);
            }
        }

        mConsensusPeers = consensusPeers.toArray(new Peer[0]);
        mConsensusChannels = consensusChannels.toArray(new Channel[0]);
        mAllChannels = allChannels.toArray(new Channel[0]);

        if (mLeaderReplWriter != null) {
            mLeaderReplWriter.update(mLeaderLogWriter, mAllChannels, consensusChannels.isEmpty());
        }

        if (mLocalMode != MODE_FOLLOWER && mGroupFile.localMemberRole() != Role.NORMAL) {
            // Step down as leader or candidate.
            toFollower("local role changed");
        }

        // Wake up syncCommit waiters which are stuck because removed peers aren't responding.
        mSyncCommitCondition.signalAll();
    }

    @Override
    public void start() throws IOException {
        start(null);
    }

    @Override
    public SnapshotReceiver restore(Map<String, String> options) throws IOException {
        if (mChanMan.isStarted()) {
            throw new IllegalStateException("Already started");
        }

        SocketSnapshotReceiver receiver = requestSnapshot(options);

        if (receiver != null) {
            try {
                start(receiver);
            } catch (Throwable e) {
                closeQuietly(receiver);
                throw e;
            }
        }

        return receiver;
    }

    private void start(SocketSnapshotReceiver receiver) throws IOException {
        if (!mChanMan.isStarted()) {
            if (receiver != null) {
                mStateLog.truncateAll(receiver.prevTerm(), receiver.term(), receiver.position());
            }

            mChanMan.start(this);

            scheduleElectionTask();
            scheduleMissingDataTask();
            scheduleSyncTask();

            mChanMan.joinAcceptor(this::requestJoin);

            acquireExclusive();
            boolean quickElection = mConsensusChannels.length == 0;
            mElectionValidated = quickElection ? -1 : 1;
            releaseExclusive();

            if (quickElection) {
                // Lone member can become leader immediately.
                doElectionTask();
            }
        }

        if (receiver == null) {
            acquireShared();
            boolean roleChange = mGroupFile.localMemberRole() != mLocalRole;
            releaseShared();

            if (roleChange) {
                mScheduler.execute(this::roleChangeTask);
            }
        }
    }

    @Override
    public Reader newReader(long position, boolean follow) {
        if (follow) {
            return mStateLog.openReader(position);
        }

        acquireShared();
        try {
            Reader reader;
            if (mLeaderLogWriter != null
                && position >= mLeaderLogWriter.termStartPosition()
                && position < mLeaderLogWriter.termEndPosition())
            {
                reader = null;
            } else {
                reader = mStateLog.openReader(position);
            }
            return reader;
        } finally {
            releaseShared();
        }
    }

    @Override
    public Writer newWriter() {
        return createWriter(-1);
    }

    @Override
    public Writer newWriter(long position) {
        if (position < 0) {
            throw new IllegalArgumentException();
        }
        return createWriter(position);
    }

    private Writer createWriter(long position) {
        acquireExclusive();
        try {
            if (mLeaderReplWriter != null) {
                throw new IllegalStateException("Writer already exists");
            }
            if (mLeaderLogWriter == null
                || (position >= 0 && position != mLeaderLogWriter.position()))
            {
                return null;
            }
            ReplWriter writer = new ReplWriter
                (mLeaderLogWriter, mAllChannels, mConsensusPeers.length == 0, mProxyWrites);
            mLeaderReplWriter = writer;
            return writer;
        } finally {
            releaseExclusive();
        }
    }

    void writerClosed(ReplWriter writer) {
        acquireExclusive();
        if (mLeaderReplWriter == writer) {
            mLeaderReplWriter = null;
        }
        releaseExclusive();
    }

    @Override
    public void sync() throws IOException {
        mStateLog.sync();
    }

    @Override
    public boolean syncCommit(long position, long nanosTimeout) throws IOException {
        long nanosEnd = nanosTimeout <= 0 ? 0 : System.nanoTime() + nanosTimeout;

        while (true) {
            if (mStateLog.isDurable(position)) {
                break;
            }

            if (nanosTimeout == 0) {
                return false;
            }

            TermLog termLog = mStateLog.termLogAt(position);
            long prevTerm = termLog.prevTermAt(position);
            long term = termLog.term();

            // Sync locally before remotely, avoiding race conditions when the peers reply back
            // quickly. The syncCommitReply method would end up calling commitDurable too soon.
            long commitPosition = mStateLog.syncCommit(prevTerm, term, position);

            if (position > commitPosition) {
                // If syncCommit returns -1, assume that the leadership changed and try again.
                if (commitPosition >= 0) {
                    throw new IllegalStateException
                        ("Invalid commit position: " + position + " > " + commitPosition);
                }
            } else {
                acquireShared();
                Channel[] channels = mConsensusChannels;
                releaseShared();

                if (channels.length == 0) {
                    // Already have consensus.
                    mStateLog.commitDurable(position);
                    break;
                }

                for (Channel channel : channels) {
                    channel.syncCommit(this, prevTerm, term, position);
                }

                // Don't wait on the condition for too long. The remote calls to the peers
                // might have failed, and so retries are necessary.
                long actualTimeout = SYNC_COMMIT_RETRY_MILLIS * 1_000_000L;
                if (nanosTimeout >= 0) {
                    actualTimeout = Math.min(nanosTimeout, actualTimeout);
                }

                acquireExclusive();
                if (mStateLog.isDurable(position)) {
                    releaseExclusive();
                    break;
                }
                int result = mSyncCommitCondition.await(this, actualTimeout, nanosEnd);
                releaseExclusive();

                if (result < 0) {
                    throw new InterruptedIOException();
                }
            }

            if (nanosTimeout > 0 && (nanosTimeout = (nanosEnd - System.nanoTime())) < 0) {
                nanosTimeout = 0;
            }
        }

        // Notify all peers that they don't need to retain data on our behalf.

        acquireShared();
        Channel[] channels = mAllChannels;
        releaseShared();

        for (Channel channel : channels) {
            channel.compact(this, position);
        }

        return true;
    }

    @Override
    public void compact(long position) throws IOException {
        long lowestPosition = position;

        acquireShared();
        try {
            for (Channel channel : mAllChannels) {
                lowestPosition = Math.min(lowestPosition, channel.peer().mCompactPosition);
            }
        } finally {
            releaseShared();
        }

        mStateLog.compact(lowestPosition);
    }

    @Override
    public long getLocalMemberId() {
        return mChanMan.getLocalMemberId();
    }

    @Override
    public SocketAddress getLocalAddress() {
        acquireShared();
        SocketAddress addr = mGroupFile.localMemberAddress();
        releaseShared();
        return addr;
    }

    @Override
    public Role getLocalRole() {
        acquireShared();
        Role role = mGroupFile.localMemberRole();
        releaseShared();
        return role;
    }

    @Override
    public Socket connect(SocketAddress addr) throws IOException {
        return mChanMan.connectPlain(addr);
    }

    @Override
    public void socketAcceptor(Consumer<Socket> acceptor) {
        mChanMan.socketAcceptor(acceptor);
    }

    @Override
    public void controlMessageReceived(long position, byte[] message) throws IOException {
        boolean quickCommit = false;

        acquireExclusive();
        try {
            boolean refresh;

            switch (message[0]) {
            default:
                // Unknown message type.
                return;
            case CONTROL_OP_JOIN:
                refresh = mGroupFile.applyJoin(position, message) != null;
                break;
            case CONTROL_OP_UPDATE_ROLE:
                refresh = mGroupFile.applyUpdateRole(message);
                break;
            case CONTROL_OP_UNJOIN:
                refresh = mGroupFile.applyRemovePeer(message);
                break;
            }

            // TODO: Followers should inform the leader very early of their current group
            // version, and if they inform the leader again whenever it changes. This speeds up
            // updateRole without forcing the caller to retry.

            if (refresh) {
                refreshPeerSet();

                if (mLocalMode == MODE_LEADER) {
                    // Ensure that all replicas see the commit position, allowing them to receive
                    // and process the control message as soon as possible.
                    // TODO: Not required when quick commit reply is implemented for everything.
                    quickCommit = true;
                }
            }
        } finally {
            releaseExclusive();
        }

        if (quickCommit) {
            mScheduler.execute(this::affirmLeadership);
        }
    }

    @Override
    public void controlMessageAcceptor(Consumer<byte[]> acceptor) {
        mControlMessageAcceptor = acceptor;
    }

    @Override
    public SocketSnapshotReceiver requestSnapshot(Map<String, String> options) throws IOException {
        acquireShared();
        Channel[] channels = mAllChannels;
        releaseShared();

        if (channels.length == 0) {
            // No peers.
            return null;
        }

        // Snapshots are requested early, so wait for connections to be established.
        waitForConnections(channels);

        final Object requestedBy = Thread.currentThread();
        for (Channel channel : channels) {
            channel.peer().resetSnapshotScore(new SnapshotScore(requestedBy, channel));
            channel.snapshotScore(this);
        }

        long timeoutMillis = SNAPSHOT_REPLY_TIMEOUT_MILLIS;
        long end = System.currentTimeMillis() + timeoutMillis;

        List<SnapshotScore> results = new ArrayList<>(channels.length);

        for (int i=0; i<channels.length; ) {
            Channel channel = channels[i];
            SnapshotScore score = channel.peer().awaitSnapshotScore(requestedBy, timeoutMillis);
            if (score != null) {
                results.add(score);
            }
            if (++i >= channels.length) {
                break;
            }
            timeoutMillis = end - System.currentTimeMillis();
            if (timeoutMillis <= 0) {
                break;
            }
        }

        if (results.isEmpty()) {
            throw new ConnectException("Unable to obtain a snapshot from a peer (timed out)");
        }

        Collections.shuffle(results); // random selection in case of ties
        Collections.sort(results); // stable sort

        Socket sock = mChanMan.connectSnapshot(results.get(0).mChannel.peer().mAddress);

        try {
            return new SocketSnapshotReceiver(mGroupFile, sock, options);
        } catch (IOException e) {
            closeQuietly(sock);
            throw e;
        }
    }

    @Override
    public void snapshotRequestAcceptor(Consumer<SnapshotSender> acceptor) {
        mChanMan.snapshotRequestAcceptor(sock -> {
            SnapshotSender sender;
            try {
                sender = new Sender(sock);
            } catch (IOException e) {
                closeQuietly(sock);
                return;
            } catch (Throwable e) {
                closeQuietly(sock);
                throw e;
            }

            mScheduler.execute(() -> acceptor.accept(sender));
        });
    }

    final class Sender extends SocketSnapshotSender {
        boolean mClosed;

        Sender(Socket socket) throws IOException {
            super(mGroupFile, socket);
            adjustSnapshotSessionCount(this, +1);
        }

        @Override
        public void close() throws IOException {
            adjustSnapshotSessionCount(this, -1);
            super.close();
        }

        @Override
        TermLog termLogAt(long position) {
            return mStateLog.termLogAt(position);
        }
    }

    private void adjustSnapshotSessionCount(Sender sender, int amt) {
        acquireExclusive();

        if (!sender.mClosed) {
            mSnapshotSessionCount += amt;
            if (amt < 0) {
                sender.mClosed = true;
            }
        }

        releaseExclusive();
    }

    final class ReplWriter implements Writer {
        // Limit the amount of bytes written by a proxy before selecting another one. A small
        // amount causes too many holes to appear in the replication log, which is inefficient
        // and can cause spurious hole filling requests. On saturated 10GBit network, a limit
        // of 10MBytes causes a round-robin proxy selection every 10ms. At lower transmission
        // rates, the selection rate is lower, but it's less necessary because the network
        // isn't saturated. The primary goal of using a proxy is to limit load on the leader.
        private static final long PROXY_LIMIT = 10_000_000;

        private final LogWriter mWriter;
        private Channel[] mPeerChannels;
        private boolean mSelfCommit;
        private int mProxy;
        private long mWriteAmount;

        ReplWriter(LogWriter writer, Channel[] peerChannels, boolean selfCommit, boolean proxy) {
            mWriter = writer;
            mPeerChannels = peerChannels;
            mSelfCommit = selfCommit;
            mProxy = proxy ? 0 : -1;
        }

        @Override
        public long term() {
            return mWriter.term();
        }

        @Override
        public long termStartPosition() {
            return mWriter.termStartPosition();
        }

        @Override
        public long termEndPosition() {
            return mWriter.termEndPosition();
        }

        @Override
        public long position() {
            return mWriter.position();
        }

        @Override
        public long commitPosition() {
            return mWriter.commitPosition();
        }

        @Override
        public int write(byte[] data, int offset, int length, long highestPosition)
            throws IOException
        {
            Channel[] peerChannels;
            long prevTerm, term, position, commitPosition;
            int proxy;

            w0: {
                synchronized (this) {
                    peerChannels = mPeerChannels;

                    if (peerChannels == null) {
                        // Deactivated.
                        return -1;
                    }

                    LogWriter writer = mWriter;

                    // Must capture the previous term before the write potentially changes it.
                    prevTerm = writer.prevTerm();
                    term = writer.term();
                    position = writer.position();

                    int amt = writer.write(data, offset, length, highestPosition);

                    if (amt <= 0) {
                        if (length > 0) {
                            mPeerChannels = null;
                        }
                        return amt;
                    }

                    length = amt;

                    mStateLog.captureHighest(writer);
                    highestPosition = writer.mHighestPosition;

                    if (mSelfCommit) {
                        // Only a consensus group of one, so commit changes immediately.
                        mStateLog.commit(highestPosition);
                    }

                    if (peerChannels.length == 0) {
                        return length;
                    }

                    commitPosition = writer.mCommitPosition;

                    proxy = mProxy;

                    if (proxy >= 0) {
                        long writeAmount = mWriteAmount + length;
                        if (writeAmount < PROXY_LIMIT) {
                            mWriteAmount = writeAmount;
                        } else {
                            // Select a different proxy to balance the load.
                            proxy = (proxy + 1) % peerChannels.length;
                            mProxy = proxy;
                            mWriteAmount = 0;
                        }
                        break w0;
                    }
                }

                // Write directly to all the peers.

                for (Channel peerChan : peerChannels) {
                    peerChan.writeData(null, prevTerm, term,
                                       position, highestPosition, commitPosition,
                                       data, offset, length);
                }

                return length;
            }

            // Proxy the write through a selected peer.

            for (int i = peerChannels.length; --i >= 0; ) {
                Channel peerChan = peerChannels[proxy];

                if (peerChan.writeDataAndProxy
                    (null, prevTerm, term, position, highestPosition, commitPosition,
                     data, offset, length))
                {
                    // Success.
                    return length;
                }

                synchronized (this) {
                    // Select the next peer and try again.
                    proxy = (proxy + 1) % peerChannels.length;
                    mProxy = proxy;
                }
            }

            return length;
        }

        @Override
        public long waitForCommit(long position, long nanosTimeout) throws InterruptedIOException {
            return mWriter.waitForCommit(position, nanosTimeout);
        }

        @Override
        public void uponCommit(long position, LongConsumer task) {
            mWriter.uponCommit(position, task);
        }

        @Override
        public void close() {
            if (deactivate()) {
                mWriter.release();
                writerClosed(this);
            }
        }

        synchronized void update(LogWriter writer, Channel[] peerChannels, boolean selfCommit) {
            if (mWriter == writer && mPeerChannels != null) {
                mPeerChannels = peerChannels;
                mSelfCommit = selfCommit;
            }
        }

        synchronized boolean deactivate() {
            if (mPeerChannels == null) {
                return false;
            }
            mPeerChannels = null;
            mSelfCommit = false;
            return true;
        }
    }

    @Override
    public void close() throws IOException {
        mChanMan.stop();
        mScheduler.shutdown();
        mStateLog.close();
        acquireExclusive();
        try {
            mSyncCommitCondition.signalAll();
        } finally {
            releaseExclusive();
        }
    }

    void uncaught(Throwable e) {
        if (!mChanMan.isStopped()) {
            Utils.uncaught(e);
        }
    }

    Scheduler scheduler() {
        return mScheduler;
    }

    /**
     * Enable or disable partitioned mode, which simulates a network partition. New connections
     * are rejected and existing connections are closed.
     */
    void partitioned(boolean enable) {
        mChanMan.partitioned(enable);
    }

    private static void waitForConnections(Channel[] channels) throws InterruptedIOException {
        int timeoutMillis = CONNECT_TIMEOUT_MILLIS;
        for (Channel channel : channels) {
            timeoutMillis = channel.waitForConnection(timeoutMillis);
        }
    }

    private void scheduleSyncTask() {
        // Runs a task which checks if the durable position is advancing, and if not, forces it
        // to advance by calling syncCommit. This allows peers to compact their logs even if
        // the application isn't calling syncCommit very often.

        new Delayed(0) {
            {
                schedule();
            }

            private long mTargetDurablePosition;

            @Override
            protected void doRun(long counter) {
                StateLog log = mStateLog;
                long commitPosition = log.captureHighest().mCommitPosition;

                if (!log.isDurable(mTargetDurablePosition)) {
                    try {
                        syncCommit(mTargetDurablePosition, -1);
                    } catch (IOException e) {
                        // Ignore.
                    }
                }

                mTargetDurablePosition = commitPosition;

                schedule();
            }

            private void schedule() {
                int delayMillis = ThreadLocalRandom.current()
                    .nextInt(SYNC_RATE_LOW_MILLIS, SYNC_RATE_HIGH_MILLIS);
                mCounter = System.currentTimeMillis() + delayMillis;
                mScheduler.schedule(this);
            }
        };
    }

    private void scheduleMissingDataTask() {
        int delayMillis = ThreadLocalRandom.current()
            .nextInt(MISSING_DELAY_LOW_MILLIS, MISSING_DELAY_HIGH_MILLIS);
        mScheduler.schedule(this::missingDataTask, delayMillis);
    }

    private void missingDataTask() {
        if (tryAcquireShared()) {
            if (mLocalMode == MODE_LEADER) {
                // Leader doesn't need to check for missing data.
                mMissingContigPosition = Long.MAX_VALUE; // reset to unknown
                mSkipMissingDataTask = true;
                releaseShared();
                return;
            }
            releaseShared();
        }

        class Collector implements PositionRange {
            long[] mRanges;
            int mSize;

            @Override
            public void range(long startPosition, long endPosition) {
                if (mRanges == null) {
                    mRanges = new long[16];
                } else if (mSize >= mRanges.length) {
                    mRanges = Arrays.copyOf(mRanges, mRanges.length << 1);
                }
                mRanges[mSize++] = startPosition;
                mRanges[mSize++] = endPosition;
            }
        };

        if (mReceivingMissingData) {
            // Avoid overlapping requests for missing data if results are flowing in.
            mReceivingMissingData = false;
        } else {
            Collector collector = new Collector();
            mMissingContigPosition = mStateLog.checkForMissingData
                (mMissingContigPosition, collector);

            for (int i=0; i<collector.mSize; ) {
                long startPosition = collector.mRanges[i++];
                long endPosition = collector.mRanges[i++];
                requestMissingData(startPosition, endPosition);
            }
        }

        scheduleMissingDataTask();
    }

    private void requestMissingData(final long startPosition, final long endPosition) {
        event(Level.FINE, () ->
              String.format("Requesting missing data: %1$,d bytes @[%2$d, %3$d)",
                            endPosition - startPosition, startPosition, endPosition));

        // TODO: Need a way to abort outstanding requests.

        long remaining = endPosition - startPosition;
        long position = startPosition;

        // Distribute the request among the channels at random.

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        acquireShared();
        // Only query normal and standby members. Observers aren't first-class and might not
        // have the data.
        Channel[] channels = mConsensusChannels;
        releaseShared();

        doRequestData: while (remaining > 0) {
            long amt = Math.min(remaining, MISSING_DATA_REQUEST_SIZE);

            int selected = rnd.nextInt(channels.length);
            int attempts = 0;

            while (true) {
                Channel channel = channels[selected];

                if (channel.queryData(this, position, position + amt)) {
                    break;
                }

                // Peer not available, so try another.

                if (++attempts >= channels.length) {
                    // Attempted all peers, and none are available.
                    break doRequestData;
                }

                selected++;
                if (selected >= channels.length) {
                    selected = 0;
                }
            }

            position += amt;
            remaining -= amt;
        }
    }

    private void scheduleElectionTask() {
        int delayMillis = ThreadLocalRandom.current()
            .nextInt(ELECTION_DELAY_LOW_MILLIS, ELECTION_DELAY_HIGH_MILLIS);
        mScheduler.schedule(this::electionTask, delayMillis);
    }

    private void electionTask() {
        try {
            doElectionTask();
        } finally {
            scheduleElectionTask();
        }
    }

    private void doElectionTask() {
        acquireExclusive();

        if (mLocalMode == MODE_LEADER) {
            doAffirmLeadership();
            return;
        }

        Channel[] peerChannels;

        if (mElectionValidated >= 0) {
            // Current leader or candidate is still active, so don't start an election yet.
            mElectionValidated--;

            if (mElectionValidated >= 0
                || mGroupFile.localMemberRole() != Role.NORMAL
                || (peerChannels = mConsensusChannels).length <= 0)
            {
                releaseExclusive();
                return;
            }

            // Next time around, the local member might try to become a candidate. Ask the
            // peers if they still have an active leader and check later. This process isn't
            // actually necessary, but it helps with election stability.

            long term = mStateLog.captureHighest().mTerm;

            for (Channel peerChan : peerChannels) {
                peerChan.peer().mLeaderCheck = term;
            }

            releaseExclusive();

            for (Channel peerChan : peerChannels) {
                peerChan.leaderCheck(null);
            }

            return;
        }

        LogInfo info;
        long term, candidateId;

        try {
            if (mLocalMode == MODE_CANDIDATE) {
                // Abort current election and start a new one.
                toFollower("election timed out");
            }

            if (mGroupFile.localMemberRole() != Role.NORMAL) {
                // Only NORMAL members can become candidates.
                releaseExclusive();
                return;
            }

            peerChannels = mConsensusChannels;
            info = mStateLog.captureHighest();

            int noLeaderCount = 1; // include self
            for (Channel peerChan : peerChannels) {
                if (peerChan.peer().mLeaderCheck == -1) {
                    noLeaderCount++;
                }
            }

            if (noLeaderCount <= (peerChannels.length + 1) / 2) {
                // A majority must indicate that there's no leader, so don't anything this time.
                mElectionValidated = 0;
                releaseExclusive();
                return;
            }

            // Convert to candidate.
            mLocalMode = MODE_CANDIDATE;

            candidateId = mChanMan.getLocalMemberId();

            try {
                mCurrentTerm = term = mStateLog.incrementCurrentTerm(1, candidateId);
            } catch (IOException e) {
                releaseExclusive();
                uncaught(e);
                return;
            }

            // Only need a majority of vote grants (already voted for self).
            mGrantsRemaining = (peerChannels.length + 1) / 2;

            // Don't give up candidacy too soon.
            mElectionValidated = 1;
        } catch (Throwable e) {
            releaseExclusive();
            throw e;
        }

        if (mGrantsRemaining == 0) {
            // Only a group of one, so become leader immediately.
            toLeader(term, info.mHighestPosition);
        } else {
            releaseExclusive();

            event(Level.INFO, "Local member is a candidate: term=" + term + ", highestTerm=" +
                  info.mTerm + ", highestPosition=" + info.mHighestPosition);

            for (Channel peerChan : peerChannels) {
                peerChan.requestVote(null, term, candidateId, info.mTerm, info.mHighestPosition);
            }
        }
    }

    private void affirmLeadership() {
        acquireExclusive();
        doAffirmLeadership();
    }

    // Caller must acquire exclusive latch, which is released by this method.
    private void doAffirmLeadership() {
        LogWriter writer;
        long highestPosition, commitPosition;

        try {
            writer = mLeaderLogWriter;

            if (writer == null) {
                // Not the leader anymore.
                return;
            }

            mStateLog.captureHighest(writer);
            highestPosition = writer.mHighestPosition;
            commitPosition = writer.mCommitPosition;

            if (commitPosition >= highestPosition || commitPosition > mLeaderCommitPosition) {
                // Smallest validation value is 1, but boost it to avoiding stalling too soon.
                mElectionValidated = 5;
                mLeaderCommitPosition = commitPosition;
            } else if (mElectionValidated >= 0) {
                mElectionValidated--;
            } else {
                toFollower("commit position is stalled");
                return;
            }
        } finally {
            releaseExclusive();
        }

        Channel[] peerChannels = mAllChannels;

        long prevTerm = writer.prevTerm();
        long term = writer.term();
        long position = writer.position();

        for (Channel peerChan : peerChannels) {
            peerChan.writeData(null, prevTerm, term, position, highestPosition, commitPosition,
                               EMPTY_DATA, 0, 0);
        }
    }

    /**
     * Caller must hold exclusive latch.
     *
     * @param reason pass null if term incremented
     */
    private void toFollower(String reason) {
        final int originalMode = mLocalMode;

        if (originalMode != MODE_FOLLOWER) {
            mLocalMode = MODE_FOLLOWER;

            if (mLeaderLogWriter != null) {
                mLeaderLogWriter.release();
                mLeaderLogWriter = null;
            }

            if (mLeaderReplWriter != null) {
                mLeaderReplWriter.deactivate();
            }

            StringBuilder b = new StringBuilder("Local member ");

            if (originalMode == MODE_LEADER) {
                if (mSkipMissingDataTask) {
                    mSkipMissingDataTask = false;
                    scheduleMissingDataTask();
                }
                b.append("leadership");
            } else {
                b.append("candidacy");
            }

            b.append(" lost: ");

            if (reason == null) {
                b.append("term=").append(mCurrentTerm);
            } else {
                b.append(reason);
            }

            event(Level.INFO, b.toString());
        }
    }

    private void requestJoin(Socket s) {
        try {
            try {
                if (doRequestJoin(s)) {
                    return;
                }
            } catch (IllegalStateException e) {
                // Cannot remove local member.
                OutputStream out = s.getOutputStream();
                out.write(new byte[] {GroupJoiner.OP_ERROR, ErrorCodes.INVALID_ADDRESS});
            }
        } catch (Throwable e) {
            closeQuietly(s);
            rethrow(e);
        }

        closeQuietly(s);
    }

    /**
     * @return false if socket should be closed
     */
    @SuppressWarnings("fallthrough")
    private boolean doRequestJoin(Socket s) throws IOException {
        ChannelInputStream in = new ChannelInputStream(s.getInputStream(), 100);

        SocketAddress addr;
        long memberId;

        int op = in.read();

        switch (op) {
        default:
            return joinFailure(s, ErrorCodes.UNKNOWN_OPERATION);
        case GroupJoiner.OP_ADDRESS:
        case GroupJoiner.OP_UNJOIN_ADDRESS:
            addr = GroupFile.parseSocketAddress(in.readStr(in.readIntLE()));
            memberId = 0;
            break;
        case GroupJoiner.OP_UNJOIN_MEMBER:
            addr = null;
            memberId = in.readLongLE();
            break;
        }

        OutputStream out = s.getOutputStream();

        acquireShared();
        boolean isLeader = mLocalMode == MODE_LEADER;
        Channel leaderReplyChannel = mLeaderReplyChannel;
        releaseShared();

        if (!isLeader) {
            Peer leaderPeer;
            if (leaderReplyChannel == null || (leaderPeer = leaderReplyChannel.peer()) == null) {
                return joinFailure(s, ErrorCodes.NO_LEADER);
            }
            EncodingOutputStream eout = new EncodingOutputStream();
            eout.write(GroupJoiner.OP_ADDRESS);
            eout.encodeStr(leaderPeer.mAddress.toString());
            out.write(eout.toByteArray());
            return false;
        }

        Consumer<byte[]> acceptor = mControlMessageAcceptor;

        if (acceptor == null) {
            return joinFailure(s, ErrorCodes.NO_ACCEPTOR);
        }

        byte[] message;

        switch (op) {
        case GroupJoiner.OP_ADDRESS:
            message = mGroupFile.proposeJoin(CONTROL_OP_JOIN, addr, (gfIn, position) -> {
                // Join is accepted, so reply with the log position info and the group
                // file. The position is immediately after the control message.

                try {
                    if (gfIn == null) {
                        out.write(new byte[] {
                            // Assume failure is due to version mismatch, but could be
                            // something else.
                            // TODO: Use more specific error code.
                            GroupJoiner.OP_ERROR, ErrorCodes.VERSION_MISMATCH
                        });
                        return;
                    }

                    TermLog termLog = mStateLog.termLogAt(position);

                    byte[] buf = new byte[1000];
                    int off = 0;
                    buf[off++] = GroupJoiner.OP_JOINED;
                    encodeLongLE(buf, off, termLog.prevTermAt(position)); off += 8;
                    encodeLongLE(buf, off, termLog.term()); off += 8;
                    encodeLongLE(buf, off, position); off += 8;

                    while (true) {
                        int amt;
                        try {
                            amt = gfIn.read(buf, off, buf.length - off);
                        } catch (IOException e) {
                            // Report any GroupFile corruption.
                            uncaught(e);
                            return;
                        }
                        if (amt < 0) {
                            break;
                        }
                        out.write(buf, 0, off + amt);
                        off = 0;
                    }
                } catch (IOException e) {
                    // Ignore.
                } finally {
                    closeQuietly(out);
                }
            });

            break;

        case GroupJoiner.OP_UNJOIN_ADDRESS:
            // Find the member and fall through to the next case.
            if (mGroupFile.localMemberAddress().equals(addr)) {
                memberId = mGroupFile.localMemberId();
            } else {
                for (Peer peer : mGroupFile.allPeers()) {
                    if (peer.mAddress.equals(addr)) {
                        memberId = peer.mMemberId;
                        break;
                    }
                }
            }

        case GroupJoiner.OP_UNJOIN_MEMBER:
            message = mGroupFile.proposeRemovePeer(CONTROL_OP_UNJOIN, memberId, success -> {
                try {
                    byte[] reply;

                    if (success) {
                        reply = new byte[] {GroupJoiner.OP_UNJOINED};
                    } else {
                        reply = new byte[] {
                            // Assume failure is due to version mismatch, but could be
                            // something else.
                            // TODO: Use more specific error code.
                            GroupJoiner.OP_ERROR, ErrorCodes.VERSION_MISMATCH
                        };
                    }

                    out.write(reply);
                } catch (IOException e) {
                    // Ignore.
                } finally {
                    closeQuietly(out);
                }
            });
            break;

        default:
            throw new AssertionError();
        }

        mScheduler.schedule(() -> {
            if (mGroupFile.discardProposeConsumer(message)) {
                closeQuietly(s);
            }
        }, JOIN_TIMEOUT_MILLIS);

        acceptor.accept(message);
        return true;
    }

    private static boolean joinFailure(Socket s, byte errorCode) throws IOException {
        s.getOutputStream().write(new byte[] {GroupJoiner.OP_ERROR, errorCode});
        return false;
    }

    private void roleChangeTask() {
        acquireShared();
        Role desiredRole = mLocalRole;
        if (desiredRole == mGroupFile.localMemberRole()) {
            releaseShared();
            return;
        }
        long groupVersion = mGroupFile.version();
        long localMemberId = mGroupFile.localMemberId();
        releaseShared();

        Channel requestChannel = leaderRequestChannel();

        if (requestChannel != null) {
            requestChannel.updateRole(this, groupVersion, localMemberId, desiredRole);
        }

        // Check again later.
        mScheduler.schedule(this::roleChangeTask, ELECTION_DELAY_LOW_MILLIS);
    }

    /**
     * @return this Controller instance if local member is the leader, or non-null if the
     * leader is remote, or null if the group has no leader
     */
    private Channel leaderRequestChannel() {
        Channel requestChannel;
        Channel replyChannel;

        acquireShared();
        boolean exclusive = false;

        while (true) {
            if (mLocalMode == MODE_LEADER) {
                release(exclusive);
                return this;
            }
            requestChannel = mLeaderRequestChannel;
            if (requestChannel != null || (replyChannel = mLeaderReplyChannel) == null) {
                release(exclusive);
                return requestChannel;
            }
            if (exclusive || tryUpgrade()) {
                break;
            }
            releaseShared();
            acquireExclusive();
            exclusive = true;
        }

        // Find the request channel to the leader.

        Peer leader = replyChannel.peer();

        for (Channel channel : mAllChannels) {
            if (leader.equals(channel.peer())) {
                mLeaderRequestChannel = requestChannel = channel;
                break;
            }
        }

        releaseExclusive();

        return requestChannel;
    }

    private void event(Level level, String message) {
        if (mEventListener != null) {
            try {
                mEventListener.accept(level, message);
            } catch (Throwable e) {
                // Ignore.
            }
        }
    }

    private void event(Level level, Supplier<String> message) {
        if (mEventListener != null) {
            try {
                mEventListener.accept(level, message.get());
            } catch (Throwable e) {
                // Ignore.
            }
        }
    }

    /**
     * Called from a remote group member.
     */
    @Override // Channel
    public void unknown(Channel from, int op) {
        event(Level.WARNING,
              "Unknown operation received from: " + from.peer().mAddress + ", op=" + op);
    }

    /**
     * Called from a remote group member.
     */
    @Override // Channel
    public boolean nop(Channel from) {
        return true;
    }

    /**
     * Called from a remote group member.
     */
    @Override // Channel
    public boolean requestVote(Channel from, long term, long candidateId,
                               long highestTerm, long highestPosition)
    {
        long currentTerm;
        acquireExclusive();
        try {
            final long originalTerm = mCurrentTerm;

            mCurrentTerm = currentTerm = mStateLog.checkCurrentTerm(term);

            if (currentTerm > originalTerm) {
                toFollower(null);
            }

            if (currentTerm >= originalTerm && !isBehind(highestTerm, highestPosition)) {
                if (mStateLog.checkCandidate(candidateId)) {
                    // Set voteGranted result bit to true.
                    currentTerm |= 1L << 63;
                    // Treat new candidate as active, so don't start a new election too soon.
                    mElectionValidated = 1;
                }
            }
        } catch (IOException e) {
            uncaught(e);
            return false;
        } finally {
            releaseExclusive();
        }

        from.requestVoteReply(null, currentTerm);
        return true;
    }

    private boolean isBehind(long term, long position) {
        LogInfo info = mStateLog.captureHighest();
        return term < info.mTerm || (term == info.mTerm && position < info.mHighestPosition);
    }

    /**
     * Called from a remote group member.
     */
    @Override // Channel
    public boolean requestVoteReply(Channel from, long term) {
        acquireExclusive();

        final long originalTerm = mCurrentTerm;

        if (term < 0 && (term &= ~(1L << 63)) == originalTerm) {
            // Vote granted.

            if (--mGrantsRemaining > 0 || mLocalMode != MODE_CANDIDATE) {
                // Majority not received yet, or voting is over.
                releaseExclusive();
                return true;
            }

            LogInfo info = mStateLog.captureHighest();
            toLeader(term, info.mHighestPosition);
            return true;
        }

        // Vote denied.

        try {
            mCurrentTerm = mStateLog.checkCurrentTerm(term);
        } catch (IOException e) {
            releaseExclusive();
            uncaught(e);
            return false;
        }

        if (mCurrentTerm <= originalTerm) {
            // Remain candidate for now, waiting for more votes.
            releaseExclusive();
            return true;
        }

        toFollower("vote denied");
        releaseExclusive();
        return true;
    }

    // Caller must acquire exclusive latch, which is released by this method.
    private void toLeader(long term, long position) {
        try {
            event(Level.INFO, "Local member is the leader: term=" + term +
                  ", position=" + position);

            mLeaderReplyChannel = null;
            mLeaderRequestChannel = null;

            long prevTerm = mStateLog.termLogAt(position).prevTermAt(position);

            mLeaderLogWriter = mStateLog.openWriter(prevTerm, term, position);
            mLocalMode = MODE_LEADER;
            for (Channel channel : mAllChannels) {
                channel.peer().mMatchPosition = 0;
            }

            if (mConsensusPeers.length == 0) {
                // Only a consensus group of one, so commit changes immediately.
                mStateLog.captureHighest(mLeaderLogWriter);
                mStateLog.commit(mLeaderLogWriter.mHighestPosition);
            }
        } catch (Throwable e) {
            releaseExclusive();
            uncaught(e);
            return;
        }

        doAffirmLeadership();
    }

    /**
     * Called from a remote group member.
     */
    @Override // Channel
    public boolean queryTerms(Channel from, long startPosition, long endPosition) {
        mStateLog.queryTerms(startPosition, endPosition, (prevTerm, term, position) -> {
            from.queryTermsReply(null, prevTerm, term, position);
        });

        return true;
    }

    /**
     * Called from a remote group member.
     */
    @Override // Channel
    public boolean queryTermsReply(Channel from, long prevTerm, long term, long startPosition) {
        try {
            queryReplyTermCheck(term);

            mStateLog.defineTerm(prevTerm, term, startPosition);
        } catch (IOException e) {
            uncaught(e);
        }

        return true;
    }

    /**
     * Term check to apply when receiving results from a query. Forces a conversion to follower
     * if necessary, although leaders don't issue queries.
     */
    private void queryReplyTermCheck(long term) throws IOException {
        acquireShared();
        long originalTerm = mCurrentTerm;

        if (term < originalTerm) {
            releaseShared();
            return;
        }

        if (!tryUpgrade()) {
            releaseShared();
            acquireExclusive();
            originalTerm = mCurrentTerm;
        }

        try {
            if (term > originalTerm) {
                mCurrentTerm = mStateLog.checkCurrentTerm(term);
                if (mCurrentTerm > originalTerm) {
                    toFollower(null);
                }
            }
        } finally {
            releaseExclusive();
        }
    }

    /**
     * Called from a remote group member.
     */
    @Override // Channel
    public boolean queryData(Channel from, long startPosition, long endPosition) {
        if (endPosition > startPosition) {
            mScheduler.execute(() -> doQueryData(from, startPosition, endPosition));
        }
        return true;
    }

    private void doQueryData(Channel from, long startPosition, long endPosition) {
        // TODO: Gracefully handle stale data requests, received after log compaction.

        try {
            LogReader reader = mStateLog.openReader(startPosition);

            try {
                long remaining = endPosition - startPosition;
                byte[] buf = new byte[(int) Math.min(9000, remaining)];

                while (true) {
                    long currentTerm = 0;

                    long prevTerm = reader.prevTerm();
                    long position = reader.position();
                    long term = reader.term();

                    int require = (int) Math.min(buf.length, remaining);
                    int amt = reader.tryRead(buf, 0, require);

                    if (amt == 0) {
                        // If the leader, read past the commit position, up to the highest
                        // position.
                        acquireShared();
                        try {
                            if (mLocalMode == MODE_LEADER) {
                                int any = reader.tryReadAny(buf, amt, require);
                                if (any > 0) {
                                    currentTerm = mCurrentTerm;
                                    amt += any;
                                }
                            }
                        } finally {
                            releaseShared();
                        }
                    }

                    if (amt <= 0) {
                        if (amt < 0) {
                            // Move to next term.
                            reader.release();
                            reader = mStateLog.openReader(startPosition);
                        }
                        break;
                    }

                    from.queryDataReply(null, currentTerm, prevTerm, term, position, buf, 0, amt);

                    startPosition += amt;
                    remaining -= amt;

                    if (remaining <= 0) {
                        break;
                    }
                }
            } finally {
                reader.release();
            }
        } catch (IOException e) {
            uncaught(e);
        }
    }

    /**
     * Called from a remote group member.
     */
    @Override // Channel
    public boolean queryDataReply(Channel from, long currentTerm,
                                  long prevTerm, long term, long position,
                                  byte[] data, int off, int len)
    {
        if (currentTerm != 0 && validateLeaderTerm(from, currentTerm) == this) {
            return false;
        }

        mReceivingMissingData = true;

        try {
            queryReplyTermCheck(term);

            LogWriter writer = mStateLog.openWriter(prevTerm, term, position);
            if (writer != null) {
                try {
                    writer.write(data, off, len, 0);
                } finally {
                    writer.release();
                }
            }
        } catch (IOException e) {
            uncaught(e);
        }

        return true;
    }

    /**
     * Called from a remote group member.
     *
     * @param from pass null if called from writeDataViaProxy
     */
    @Override // Channel
    public boolean writeData(Channel from, long prevTerm, long term, long position,
                             long highestPosition, long commitPosition,
                             byte[] data, int off, int len)
    {
        from = validateLeaderTerm(from, term);
        if (from == this) {
            return false;
        }

        try {
            LogWriter writer = mStateLog.openWriter(prevTerm, term, position);

            if (writer == null) {
                // TODO: stash the write for later (unless it's a stale write)
                // Cannot write because terms are missing, so request them.
                long now = System.currentTimeMillis();
                if (now >= mNextQueryTermTime) {
                    LogInfo info = mStateLog.captureHighest();
                    if (highestPosition > info.mCommitPosition && position > info.mCommitPosition) {
                        Channel requestChannel = leaderRequestChannel();
                        if (requestChannel != null && requestChannel != this) {
                            requestChannel.queryTerms(this, info.mCommitPosition, position);
                        }
                    }
                    mNextQueryTermTime = now + QUERY_TERMS_RATE_MILLIS;
                }

                return true;
            }

            try {
                writer.write(data, off, len, highestPosition);
                mStateLog.commit(commitPosition);
                mStateLog.captureHighest(writer);
                long highestTerm = writer.mTerm;
                if (highestTerm < term) {
                    // If the highest term is lower than the leader's, don't bother reporting
                    // it. The leader will just reject the reply.
                    return true;
                }
                term = highestTerm;
                highestPosition = writer.mHighestPosition;
            } finally {
                writer.release();
            }
        } catch (IOException e) {
            uncaught(e);
        }

        if (from != null) {
            // TODO: Can skip reply if successful and highest didn't change.
            // TODO: Can skip reply if local role is OBSERVER.
            from.writeDataReply(null, term, highestPosition);
        }

        return true;
    }

    /**
     * Called when receiving data from the leader.
     *
     * @param from pass null if called from writeDataViaProxy
     * @return this if validation failed, or else leader reply channel, possibly null
     */
    private Channel validateLeaderTerm(Channel from, long term) {
        acquireShared();

        if (from == null) {
            from = mLeaderReplyChannel;
        }

        long originalTerm = mCurrentTerm;

        vadidate: {
            if (term == originalTerm) {
                if (mElectionValidated > 0 || from == null) {
                    // Already validated, or cannot fully complete validation without a reply
                    // channel. A message directly from the leader is required.
                    releaseShared();
                    return from; // validation success
                }
                if (tryUpgrade()) {
                    break vadidate;
                }
            } else if (term < originalTerm) {
                releaseShared();
                return this; // validation failed
            }

            if (from == null) {
                // Cannot fully complete validation without a reply channel. A message directly
                // from the leader is required.
                releaseShared();
                return from;
            }

            if (!tryUpgrade()) {
                releaseShared();
                acquireExclusive();
                originalTerm = mCurrentTerm;
                if (term < originalTerm) {
                    releaseExclusive();
                    return this; // validation failed
                }
            }

            if (term != originalTerm) {
                try {
                    assert term > originalTerm;
                    try {
                        mCurrentTerm = mStateLog.checkCurrentTerm(term);
                    } catch (IOException e) {
                        uncaught(e);
                        releaseExclusive();
                        return this; // validation failed
                    }
                    if (mCurrentTerm <= originalTerm) {
                        releaseExclusive();
                        return from; // validation success
                    }
                    toFollower(null);
                } catch (Throwable e) {
                    releaseExclusive();
                    throw e;
                }
            }
        }

        if (from == null) {
            // Cannot fully complete validation without a reply channel. A message directly
            // from the leader is required.
            releaseExclusive();
            return from; // validation success (mostly)
        }

        mElectionValidated = 1;
        mLeaderReplyChannel = from;

        boolean first = false;
        if (term != mValidatedTerm) {
            mValidatedTerm = term;
            // Find it later when leaderRequestChannel is called.
            mLeaderRequestChannel = null;
            first = true;
        }

        releaseExclusive();

        if (first) {
            event(Level.INFO, "Remote member is the leader: " + from.peer().mAddress +
                  ", term=" + term);
        }

        return from; // validation success
    }

    /**
     * Called from a remote group member.
     */
    @Override // Channel
    public boolean writeDataReply(Channel from, long term, long highestPosition) {
        long commitPosition;

        acquireExclusive();
        try {
            if (mLocalMode != MODE_LEADER) {
                return true;
            }

            final long originalTerm = mCurrentTerm;

            if (term != originalTerm) {
                try {
                    mCurrentTerm = mStateLog.checkCurrentTerm(term);
                } catch (IOException e) {
                    uncaught(e);
                    return false;
                }

                if (mCurrentTerm > originalTerm) {
                    toFollower(null);
                } else {
                    // Cannot commit on behalf of older terms.
                }
                return true;
            }

            Peer peer = from.peer();
            long matchPosition = peer.mMatchPosition;

            if (highestPosition <= matchPosition || mConsensusPeers.length == 0) {
                return true;
            }

            // Updating and sorting the peers by match position is simple, but for large groups
            // (>10?), a tree data structure should be used instead.
            peer.mMatchPosition = highestPosition;
            Arrays.sort(mConsensusPeers);

            commitPosition = mConsensusPeers[mConsensusPeers.length >> 1].mMatchPosition;
        } finally {
            releaseExclusive();
        }

        mStateLog.commit(commitPosition);
        return true;
    }

    /**
     * Called from a remote group member.
     */
    @Override // Channel
    public boolean writeDataAndProxy(Channel from, long prevTerm, long term, long position,
                                     long highestPosition, long commitPosition,
                                     byte[] data, int off, int len)
    {
        if (!writeData(from, prevTerm, term, position, highestPosition, commitPosition,
                       data, off, len))
        {
            return false;
        }

        Peer fromPeer = from.peer();

        Channel[] peerChannels;
        acquireShared();
        peerChannels = mAllChannels;
        releaseShared();

        if (peerChannels != null) {
            for (Channel peerChan : peerChannels) {
                if (!fromPeer.equals(peerChan.peer())) {
                    peerChan.writeDataViaProxy(null, prevTerm, term,
                                               position, highestPosition, commitPosition,
                                               data, off, len);
                }
            }
        }

        return true;
    }

    /**
     * Called from a remote group member.
     */
    @Override // Channel
    public boolean writeDataViaProxy(Channel from, long prevTerm, long term, long position,
                                     long highestPosition, long commitPosition,
                                     byte[] data, int off, int len)
    {
        // From is null to prevent proxy channel from being selected as the leader channel.
        return writeData(null, prevTerm, term, position, highestPosition, commitPosition,
                         data, off, len);
    }

    /**
     * Called from a remote group member.
     */
    @Override // Channel
    public boolean syncCommit(Channel from, long prevTerm, long term, long position) {
        try {
            if (mStateLog.syncCommit(prevTerm, term, position) >= position) {
                from.syncCommitReply(null, mGroupFile.version(), term, position);
            }
            return true;
        } catch (IOException e) {
            uncaught(e);
            return false;
        }
    }

    /**
     * Called from a remote group member.
     */
    @Override // Channel
    public boolean syncCommitReply(Channel from, long groupVersion, long term, long position) {
        checkGroupVersion(groupVersion);

        long durablePosition;

        acquireExclusive();
        try {
            TermLog termLog;
            if (mConsensusPeers.length == 0
                || (termLog = mStateLog.termLogAt(position)) == null || term != termLog.term())
            {
                // Received a stale reply.
                return true;
            }

            Peer peer = from.peer();
            long syncMatchPosition = peer.mSyncMatchPosition;

            if (position > syncMatchPosition) {
                peer.mSyncMatchPosition = position;
            }

            // Updating and sorting the peers by match position is simple, but for large groups
            // (>10?), a tree data structure should be used instead.
            Arrays.sort(mConsensusPeers, (a, b) ->
                        Long.compare(a.mSyncMatchPosition, b.mSyncMatchPosition));
            
            durablePosition = mConsensusPeers[mConsensusPeers.length >> 1].mSyncMatchPosition;
        } finally {
            releaseExclusive();
        }

        try {
            if (mStateLog.commitDurable(durablePosition)) {
                acquireExclusive();
                try {
                    mSyncCommitCondition.signalAll();
                } finally {
                    releaseExclusive();
                }
            }
        } catch (IOException e) {
            uncaught(e);
        }

        return true;
    }

    /**
     * Called from a remote group member.
     */
    @Override // Channel
    public boolean compact(Channel from, long position) {
        from.peer().mCompactPosition = position;
        return true;
    }

    /**
     * Called from a remote group member.
     */
    @Override // Channel
    public boolean snapshotScore(Channel from) {
        if (!mChanMan.hasSnapshotRequestAcceptor()) {
            // No acceptor, so reply with infinite score (least preferred).
            from.snapshotScoreReply(null, Integer.MAX_VALUE, Float.POSITIVE_INFINITY);
            return true;
        }

        acquireShared();
        int sessionCount = mSnapshotSessionCount;
        int mode = mLocalMode;
        releaseShared();

        from.snapshotScoreReply(null, sessionCount, mode == MODE_LEADER ? 1 : -1);

        return true;
    }

    /**
     * Called from a remote group member.
     */
    @Override // Channel
    public boolean snapshotScoreReply(Channel from, int activeSessions, float weight) {
        from.peer().snapshotScoreReply(activeSessions, weight);
        return true;
    }

    /**
     * Called from a remote group member.
     */
    @Override // Channel
    public boolean updateRole(Channel from, long groupVersion, long memberId, Role role) {
        Consumer<byte[]> acceptor;
        byte[] message = null;
        byte result;

        acquireShared();
        tryUpdateRole: try {
            final long givenVersion = groupVersion;
            groupVersion = mGroupFile.version();

            acceptor = mControlMessageAcceptor;

            if (acceptor == null) {
                result = ErrorCodes.NO_ACCEPTOR;
                break tryUpdateRole;
            }

            if (mLocalMode != MODE_LEADER) {
                // Only the leader can update the role.
                result = ErrorCodes.NOT_LEADER;
                break tryUpdateRole;
            }

            if (givenVersion != groupVersion) {
                result = ErrorCodes.VERSION_MISMATCH;
                break tryUpdateRole;
            }

            Peer key = new Peer(memberId);
            Peer peer = mGroupFile.allPeers().ceiling(key); // findGe

            if (peer == null || peer.mMemberId != memberId) {
                // Member doesn't exist.
                // TODO: Permit leader to change itself.
                result = ErrorCodes.UNKNOWN_MEMBER;
                break tryUpdateRole;
            }

            if (peer.mRole == role) {
                // Role already matches.
                result = ErrorCodes.SUCCESS;
                break tryUpdateRole;
            }

            message = mGroupFile.proposeUpdateRole(CONTROL_OP_UPDATE_ROLE, memberId, role);

            if (peer.mRole == Role.OBSERVER || role == Role.OBSERVER) {
                // When changing the consensus, restrict to one change at a time (by the
                // majority). Ensure that all consensus peers are caught up.

                int count = 0;
                for (Peer cp : mConsensusPeers) {
                    if (cp.mGroupVersion == groupVersion) {
                        count++;
                    }
                }

                if (count < ((mConsensusPeers.length + 1) >> 1)) {
                    // Majority of versions don't match.
                    message = null;
                    result = ErrorCodes.NO_CONSENSUS;

                    // Request updated versions. Caller must retry.
                    for (Channel channel : mConsensusChannels) {
                        channel.groupVersion(this, groupVersion);
                    }

                    break tryUpdateRole;
                }
            }

            result = 0;
        } finally {
            releaseShared();
        }

        if (message != null) {
            acceptor.accept(message);
        }

        from.updateRoleReply(null, groupVersion, memberId, result);

        return true;
    }

    /**
     * Called from a remote group member.
     */
    @Override // Channel
    public boolean updateRoleReply(Channel from, long groupVersion, long memberId, byte result) {
        if (result != ErrorCodes.SUCCESS) {
            acquireShared();
            boolean ok = mLocalRole == mGroupFile.localMemberRole();
            releaseShared();
            if (!ok) {
                event(ErrorCodes.levelFor(result),
                      "Unable to update role: " + ErrorCodes.toString(result));
            }
        }

        checkGroupVersion(groupVersion);

        return true;
    }

    /**
     * Called from a remote group member.
     */
    @Override // Channel
    public boolean groupVersion(Channel from, long groupVersion) {
        // Note: If peer group version is higher than local version, can probably resync local
        // group file right away.
        from.peer().updateGroupVersion(groupVersion);
        from.groupVersionReply(null, mGroupFile.version());
        return true;
    }

    /**
     * Called from a remote group member.
     */
    @Override // Channel
    public boolean groupVersionReply(Channel from, long groupVersion) {
        from.peer().updateGroupVersion(groupVersion);
        return true;
    }

    /**
     * Called from a remote group member.
     */
    @Override
    public boolean groupFile(Channel from, long groupVersion) throws IOException {
        if (groupVersion < mGroupFile.version()) {
            OutputStream out = from.groupFileReply(null, null);
            if (out != null) {
                mGroupFile.writeTo(out);
                out.flush();
            }
        }
        return true;
    }

    /**
     * Called from a remote group member.
     */
    @Override // Channel
    public OutputStream groupFileReply(Channel from, InputStream in) throws IOException {
        boolean refresh;

        acquireExclusive();
        try {
            refresh = mGroupFile.readFrom(in);
            if (refresh) {
                refreshPeerSet();
            }
        } finally {
            releaseExclusive();
        }

        if (refresh) {
            // Inform the leader right away so that group updates can be applied quickly.
            Channel requestChannel = leaderRequestChannel();
            if (requestChannel != null && requestChannel != this) {
                requestChannel.groupVersion(this, mGroupFile.version());
            }
        }

        return null;
    }

    /**
     * Called from a remote group member.
     */
    @Override // Channel
    public boolean leaderCheck(Channel from) {
        long term;

        acquireShared();
        try {
            if (mElectionValidated <= 0) {
                // Permit peer to become a candidate.
                term = -1;
            } else {
                // Reply with the current term, in case local member or peer is behind.
                term = mStateLog.captureHighest().mTerm;
            }
        } finally {
            releaseShared();
        }

        from.leaderCheckReply(null, term);

        return true;
    }

    /**
     * Called from a remote group member.
     */
    @Override // Channel
    public boolean leaderCheckReply(Channel from, long term) {
        from.peer().mLeaderCheck = term;
        return true;
    }

    /**
     * Request that the group file be updated if it's behind. By doing this early instead of
     * waiting for a control message from the replication stream, this member can participate
     * in consensus decisions earlier.
     */
    private void checkGroupVersion(long groupVersion) {
        if (groupVersion > mGroupFile.version()) {
            Channel requestChannel = leaderRequestChannel();
            if (requestChannel != null && requestChannel != this) {
                try {
                    requestChannel.groupFile(this, mGroupFile.version());
                } catch (IOException e) {
                    // Ignore.
                }
            }
        }
    }
}
