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

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadLocalRandom;

import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

import org.cojen.tupl.core.Delayed;
import org.cojen.tupl.core.Scheduler;

import org.cojen.tupl.diag.EventListener;
import org.cojen.tupl.diag.EventType;

import org.cojen.tupl.util.Latch;

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
    private static final int SYNC_RATE_LOW_MILLIS = 2000;
    private static final int SYNC_RATE_HIGH_MILLIS = 3000;
    private static final int COMMIT_CONFLICT_REPORT_MILLIS = 60000;
    private static final int CONNECTING_THRESHOLD_MILLIS = 10000;

    // Smallest validation value is 1, but boost it to avoiding stalling too soon. If the local
    // member is the group leader and hasn't observed the commit position advancing over this
    // many election periods, it revokes its own leadership. At an average election period of
    // 250ms, the stall can last as long as 10 seconds before revocation.
    private static final int LOCAL_LEADER_VALIDATED = 40;

    // At an average election period of 250ms, this stall is 2.5 seconds on average.
    private static final int FAILOVER_STALL = 10;

    private static final byte CONTROL_OP_JOIN = 1, CONTROL_OP_UPDATE_ROLE = 2,
        CONTROL_OP_UNJOIN = 3;

    private static final byte[] EMPTY_DATA = new byte[0];

    private final EventListener mEventListener;
    private final Scheduler mScheduler;
    private final ChannelManager mChanMan;
    private final StateLog mStateLog;

    private final Latch.Condition mSyncCommitCondition;

    private final boolean mProxyWrites;

    private final long mFailoverLagTimeoutMillis;

    private GroupFile mGroupFile;

    // Local role as desired, which might not initially match what the GroupFile says.
    private Role mDesiredRole;

    // Normal and standby members are required to participate in consensus.
    private Peer[] mConsensusPeers;
    // Candidate channels can become the leader, possibly an interim leader.
    private Channel[] mCandidateChannels;
    // Proxy channels includes proxies, which cannot participate in consensus.
    private Channel[] mProxyChannels;
    // All channels includes observers, which cannot act as proxies.
    private Channel[] mAllChannels;

    // Reference to leader's reply channel, only used for accessing the peer object.
    private Channel mLeaderReplyChannel;
    private Channel mLeaderRequestChannel;

    private volatile int mLocalMode;

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

    private volatile Set<byte[]> mRegisteredControlMessages;

    // Incremented when snapshot senders are created, and decremented when they are closed.
    private int mSnapshotSessionCount;

    // Last time that a commit conflict was reported.
    private volatile long mLastConflictReport = Long.MIN_VALUE;

    // True when a task is scheduled to remove stale members.
    private boolean mRemovingStaleMembers;

    // Prevent electing self as a candidate until this value is zero.
    private int mCandidateStall;

    /**
     * @param localSocket optional; used for testing
     */
    static Controller open(ReplicatorConfig config,
                           StateLog log, long groupToken1, long groupToken2, File groupFile,
                           SocketAddress localAddress, SocketAddress listenAddress,
                           Set<SocketAddress> seeds, ServerSocket localSocket)
        throws IOException
    {
        Role localRole = config.mLocalRole;
        boolean canCreate = seeds.isEmpty() && localRole == Role.NORMAL;
        GroupFile gf = GroupFile.open(config.mEventListener, groupFile, localAddress, canCreate);

        if (gf == null && seeds.isEmpty()) {
            throw new JoinException
                ("Not a member of the group and no seeds are provided. Local role must be " +
                 Role.NORMAL + " to create the group, but configured role is: " + localRole);
        }

        var con = new Controller(config, log, groupToken1, groupToken2, gf);

        try {
            con.init(groupFile, localAddress, listenAddress, localRole, seeds, localSocket);
        } catch (Throwable e) {
            // Cleanup the mess after the init method has released the exclusive latch, to
            // prevent deadlock when calling close.
            closeQuietly(localSocket);
            closeQuietly(con);
            throw e;
        }

        return con;
    }

    private Controller(ReplicatorConfig config,
                       StateLog log, long groupToken1, long groupToken2,
                       GroupFile gf)
    {
        mEventListener = config.mEventListener;
        mStateLog = log;
        mScheduler = new Scheduler("Replicator", false);
        mChanMan = new ChannelManager(config.mSocketFactory, mScheduler, groupToken1, groupToken2,
                                      gf == null ? 0 : gf.groupId(), config.mChecksumSockets,
                                      this::uncaught);
        mGroupFile = gf;
        mSyncCommitCondition = new Latch.Condition();
        mProxyWrites = config.mProxyWrites;
        mFailoverLagTimeoutMillis = config.mFailoverLagTimeoutMillis;
    }

    private void init(File groupFile,
                      SocketAddress localAddress, SocketAddress listenAddress,
                      Role localRole, Set<SocketAddress> seeds, final ServerSocket localSocket)
        throws IOException
    {
        acquireExclusive();
        try {
            mDesiredRole = localRole;

            if (mGroupFile == null) {
                // Need to join the group.

                for (int trials = 2; --trials >= 0; ) {
                    try {
                        var joiner = new GroupJoiner
                            (mEventListener, groupFile,
                             mChanMan.groupToken1(), mChanMan.groupToken2(),
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
        } finally {
            releaseExclusive();
        }
    }

    // Caller must hold exclusive latch.
    @SuppressWarnings("unchecked")
    private void refreshPeerSet() {
        var oldPeerChannels = new HashMap<Long, Channel>();

        if (mAllChannels != null) {
            for (Channel channel : mAllChannels) {
                oldPeerChannels.put(channel.peer().mMemberId, channel);
            }
        }

        var newPeerChannels = new HashMap<Long, Channel>();

        for (Peer peer : mGroupFile.allPeers()) {
            Long memberId = peer.mMemberId;
            newPeerChannels.put(memberId, oldPeerChannels.remove(memberId));
        }

        for (long toRemove : oldPeerChannels.keySet()) {
            mChanMan.disconnect(toRemove);
        }

        var consensusPeers = new ArrayList<Peer>();
        var candidateChannels = new ArrayList<Channel>();
        var proxyChannels = new ArrayList<Channel>();
        var allChannels = new ArrayList<Channel>();

        for (Peer peer : mGroupFile.allPeers()) {
            Channel channel = newPeerChannels.get(peer.mMemberId);

            if (channel == null) {
                channel = mChanMan.connect(peer, this);
            }

            Role role = peer.role();

            if (role.providesConsensus()) {
                consensusPeers.add(peer);
            }

            if (role.isCandidate()) {
                candidateChannels.add(channel);
            }

            if (role.canProxy()) {
                proxyChannels.add(channel);
            }

            allChannels.add(channel);
        }

        mConsensusPeers = consensusPeers.toArray(new Peer[consensusPeers.size()]);

        Channel[][] arrays = toArrays(candidateChannels, proxyChannels, allChannels);

        mCandidateChannels = arrays[0];
        mProxyChannels = arrays[1];
        mAllChannels = arrays[2];

        if (mLeaderReplWriter != null) {
            mLeaderReplWriter.update
                (mLeaderLogWriter, mAllChannels, candidateChannels.isEmpty(),
                 mProxyWrites ? mProxyChannels : null);
        }

        if (mLocalMode != MODE_FOLLOWER && localMemberRole() != Role.NORMAL) {
            // Step down as leader or candidate.
            toFollower("local role changed");
        }

        // Wake up syncCommit waiters which are stuck because removed peers aren't responding.
        mSyncCommitCondition.signalAll(this);
    }

    @SuppressWarnings("unchecked")
    private static Channel[][] toArrays(List<Channel>... lists) {
        var arrays = new Channel[lists.length][];

        fill: for (int i=0; i<lists.length; i++) {
            List<Channel> list = lists[i];

            for (int j=0; j<i; j++) {
                if (list.equals(lists[j])) {
                    arrays[i] = arrays[j];
                    continue fill;
                }
            }

            arrays[i] = list.toArray(new Channel[list.size()]);
        }

        return arrays;
    }

    /**
     * Don't close the ServerSocket when closing this Replicator.
     */
    void keepServerSocket() {
        mChanMan.keepServerSocket();
    }

    @Override
    public long encoding() {
        return 7944834171105125288L;
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
            if (mCandidateChannels.length == 0) {
                // Lone member can become leader immediately.
                forceElection(); // releases exclusive latch as a side-effect
            } else {
                mElectionValidated = 1;
                releaseExclusive();
            }
        }

        if (receiver == null) {
            acquireShared();
            Role desiredRole = desiredRole();
            releaseShared();

            if (desiredRole != null) {
                mScheduler.execute(this::roleChangeTask);
            }
        }
    }

    /**
     * Caller must hold exclusive or shared latch. Returns null if no role change is requested.
     */
    private Role desiredRole() {
        if (mDesiredRole != null) {
            if (localMemberRole() != mDesiredRole) {
                return mDesiredRole;
            }
            // Clearing it with only a shared latch is fine because it's never set again.
            mDesiredRole = null;
        }
        return null;
    }

    private void roleChangeTask() {
        acquireShared();
        Role desiredRole = desiredRole();
        if (desiredRole == null) {
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
        mScheduler.scheduleMillis(this::roleChangeTask, ELECTION_DELAY_LOW_MILLIS);
    }

    @Override
    public boolean isReadable(long position) {
        return mStateLog.isReadable(position);
    }

    @Override
    public Reader newReader(long position, boolean follow) {
        if (follow) {
            return doNewReader(position);
        }

        acquireShared();
        try {
            Reader reader;
            if (mLeaderLogWriter != null
                && position >= mLeaderLogWriter.termStartPosition()
                && position < mLeaderLogWriter.termEndPosition()
                && localMemberRole() != Role.STANDBY)
            {
                reader = null;
            } else {
                reader = doNewReader(position);
            }
            return reader;
        } finally {
            releaseShared();
        }
    }

    private Reader doNewReader(long position) {
        Reader reader = mStateLog.openReader(position);
        long commitPosition = reader.commitPosition();
        if (position <= commitPosition) {
            return reader;
        }
        reader.close();
        throw new InvalidReadException
            ("Position is higher than commit position: " + position + " > " + commitPosition);
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
                throw new IllegalStateException
                    ("Writer already exists: term=" + mLeaderReplWriter.term());
            }
            if (mLeaderLogWriter == null
                || (position >= 0 && position < mLeaderLogWriter.position())
                || localMemberRole() == Role.STANDBY)
            {
                return null;
            }
            var writer = new ReplWriter
                (mLeaderLogWriter, mAllChannels, mConsensusPeers.length == 0,
                 mProxyWrites ? mProxyChannels : null);
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
            checkException(writer);
        }
        releaseExclusive();
    }

    /**
     * Caller must hold exclusive latch.
     *
     * @return true if local member is now a follower due to a write exception
     */
    private boolean checkException(ReplWriter writer) {
        if (writer != null) {
            Throwable e = writer.mException;
            if (e != null) {
                toFollower(e.toString());
                writer.mException = null;
                return true;
            }
        }
        return false;
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

            if (termLog == null) {
                long commitPosition = mStateLog.captureHighest().mCommitPosition;
                if (position > commitPosition) {
                    throw invalidCommit(position, commitPosition);
                }
                if (mStateLog.isDurable(position)) {
                    // Assume leadership change caused term to briefly vanish.
                    return true;
                }
                throw new IllegalStateException("No term at position: " + position);
            }

            long prevTerm = termLog.prevTermAt(position);
            long term = termLog.term();

            // Sync locally before remotely, avoiding race conditions when the peers reply back
            // quickly. The syncCommitReply method would end up calling commitDurable too soon.
            long commitPosition = mStateLog.syncCommit(prevTerm, term, position);

            if (position > commitPosition) {
                // If syncCommit returns -1, assume that the leadership changed and try again.
                if (commitPosition >= 0) {
                    throw invalidCommit(position, commitPosition);
                }
            } else {
                acquireShared();
                Channel[] channels = mCandidateChannels;
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

    private static IllegalStateException invalidCommit(long position, long commitPosition) {
        throw new IllegalStateException
            ("Invalid commit position: " + position + " > " + commitPosition);
    }

    @Override
    public boolean failover() {
        return doFailover(null, false);
    }

    private boolean doFailover(ReplWriter from, boolean lagCheck) {
        Channel peerChan;

        acquireExclusive();
        try {
            if (from != mLeaderReplWriter) {
                if (lagCheck) {
                    // Writer was created or changed, so assume not lagging behind.
                    return false;
                } else if (from != null) {
                    // Requested from a stale writer.
                    return false;
                }
            }

            // If already a replica return true. If an interim leader, also return true because
            // new writes cannot be accepted. Externally, it's acting like a replica.
            if (mLocalMode == MODE_FOLLOWER || localMemberRole() == Role.STANDBY) {
                return true;
            }

            // Randomly select a peer to become the new leader.

            select: {
                final int len = mCandidateChannels.length;
                Channel foundOne = null;

                if (len > 0) {
                    int offset = ThreadLocalRandom.current().nextInt(len);
                    for (int i=0; i<len; i++) {
                        peerChan = mCandidateChannels[(i + offset) % len];
                        if (peerChan.peer().role() == Role.NORMAL) {
                            if (foundOne == null) {
                                foundOne = peerChan;
                            }
                            if (peerChan.isConnected()) {
                                break select;
                            }
                        }
                    }
                }

                if (foundOne == null) {
                    // No peers can become a normal leader.
                    return false;
                }

                // Try to send a command, even though it might not be connected.
                peerChan = foundOne;
            }

            if (!lagCheck && from == null && mLeaderReplWriter != null) {
                // Let the leader write some more and deactivate when the writer is closed.
                if (mLeaderReplWriter.deactivateExplicit()) {
                    return true;
                }
            }

            if (lagCheck) {
                toFollower("lagging behind");
            } else {
                toFollower("explicit failover");
            }

            mCandidateStall = FAILOVER_STALL;
        } finally {
            releaseExclusive();
        }

        peerChan.forceElection(null);

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
    public long commitPosition() {
        return mStateLog.potentialCommitPosition();
    }

    @Override
    public long localMemberId() {
        return mChanMan.localMemberId();
    }

    @Override
    public SocketAddress localAddress() {
        acquireShared();
        SocketAddress addr = mGroupFile.localMemberAddress();
        releaseShared();
        return addr;
    }

    @Override
    public Role localRole() {
        acquireShared();
        Role role = localMemberRole();
        releaseShared();
        return role;
    }

    // Caller must hold exclusive or shared latch.
    private Role localMemberRole() {
        return mGroupFile.localMemberRole();
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
                // Counterpart to GroupFile.proposeJoin.
                refresh = mGroupFile.applyJoin(position, message) != null;
                break;
            case CONTROL_OP_UPDATE_ROLE:
                // Counterpart to GroupFile.proposeUpdateRole.
                refresh = mGroupFile.applyUpdateRole(message);
                break;
            case CONTROL_OP_UNJOIN:
                // Counterpart to GroupFile.proposeRemovePeer.
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
        try {
            return requestSnapshot(options, SNAPSHOT_REPLY_TIMEOUT_MILLIS);
        } catch (IOException e) {
            // Try again.
            return requestSnapshot(options, SNAPSHOT_REPLY_TIMEOUT_MILLIS << 1);
        }
    }

    private SocketSnapshotReceiver requestSnapshot(Map<String, String> options,
                                                   long timeoutMillis)
        throws IOException
    {
        // Request snapshots from normal, standby, and proxy members. Observers aren't
        // first-class and might not have the data.
        acquireShared();
        Channel[] channels = mProxyChannels;
        releaseShared();

        if (channels.length == 0) {
            // No peers.
            return null;
        }

        // Snapshots are requested early, so wait for connections to be established.
        waitForConnections(channels);

        final Object requestedBy = Thread.currentThread();
        for (Channel channel : channels) {
            channel.peer().prepareSnapshotScore(requestedBy);
            channel.snapshotScore(this);
        }

        long end = System.currentTimeMillis() + timeoutMillis;

        var results = new ArrayList<SnapshotScore>(channels.length);

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

        Socket sock = mChanMan.connectSnapshot(results.get(0).mPeer.mAddress);

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
        private Channel[] mProxyChannels;
        private int mProxy;
        private long mWriteAmount;

        private static final int DEACTIVATED = 1, DEACTIVATE_EXPLICIT = 2, DEACTIVATE_STALLED = 3;

        private int mDeactivated;
        private String mDeactivateReason;

        // Set when an exception is thrown when trying to write to the local log. When the
        // writer is closed, the local member should convert to a follower.
        volatile Throwable mException;

        ReplWriter(LogWriter writer, Channel[] peerChannels, boolean selfCommit,
                   Channel[] proxyChannels)
        {
            mWriter = writer;
            init(peerChannels, selfCommit, proxyChannels);
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
        public void addCommitListener(LongConsumer listener) {
            mWriter.addCommitListener(listener);
        }

        @Override
        public int write(byte[] prefix,
                         byte[] data, int offset, int length,
                         long highestPosition)
            throws IOException
        {
            Channel[] proxyChannels;
            long prevTerm, term, position, commitPosition;
            int result;
            int proxy;

            w0: {
                Channel[] peerChannels;
                synchronized (this) {
                    result = 0;
                    if (mDeactivated != 0) {
                        if (mDeactivated == DEACTIVATED) {
                            // Fully deactivated.
                            return -1;
                        }
                        // Actual returned result will be 0 after writer is called.
                        result = -1;
                    }

                    LogWriter writer = mWriter;

                    // Must capture the previous term before the write potentially changes it.
                    prevTerm = writer.prevTerm();
                    term = writer.term();
                    position = writer.position();

                    try {
                        result += writer.write(prefix, data, offset, length, highestPosition);
                    } catch (Throwable e) {
                        mException = e;
                        fullyDeactivate();
                        throw e;
                    }

                    if (result < 0) {
                        fullyDeactivate();
                        return -1;
                    }

                    mStateLog.captureHighest(writer);
                    highestPosition = writer.mHighestPosition;

                    if (mSelfCommit) {
                        // Only a consensus group of one, so commit changes immediately.
                        mStateLog.commit(highestPosition);
                    }

                    peerChannels = mPeerChannels;

                    if (peerChannels.length == 0) {
                        return result;
                    }

                    commitPosition = writer.mCommitPosition;

                    proxy = mProxy;

                    if (proxy >= 0) {
                        proxyChannels = mProxyChannels;
                        long writeAmount = mWriteAmount + length;
                        if (writeAmount < PROXY_LIMIT) {
                            mWriteAmount = writeAmount;
                        } else {
                            // Select a different proxy to balance the load.
                            proxy = (proxy + 1) % proxyChannels.length;
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
                                       prefix, data, offset, length);
                }

                return result;
            }

            // Proxy the write through a selected peer.

            for (int i = proxyChannels.length; --i >= 0; ) {
                Channel peerChan = proxyChannels[proxy];

                if (peerChan.writeDataAndProxy
                    (null, prevTerm, term, position, highestPosition, commitPosition,
                     prefix, data, offset, length))
                {
                    // Success.
                    return result;
                }

                synchronized (this) {
                    // Select the next peer and try again.
                    proxyChannels = mProxyChannels;
                    if (proxyChannels == null) {
                        break;
                    }
                    proxy = (proxy + 1) % proxyChannels.length;
                    mProxy = proxy;
                }
            }

            return result;
        }

        @Override
        public void uponCommit(long position, LongConsumer task) {
            mWriter.uponCommit(position, task);
        }

        @Override
        public long waitForCommit(long position, long nanosTimeout) throws InterruptedIOException {
            return mWriter.waitForCommit(position, nanosTimeout);
        }

        @Override
        public long waitForEndCommit(long nanosTimeout) throws InterruptedIOException {
            return mWriter.waitForEndCommit(nanosTimeout);
        }

        @Override
        public void close() {
            fullyDeactivate();
            writerClosed(this);
        }

        synchronized void update(LogWriter writer, Channel[] peerChannels, boolean selfCommit,
                                 Channel[] proxyChannels)
        {
            if (mWriter == writer && mDeactivated != DEACTIVATED) {
                init(peerChannels, selfCommit, proxyChannels);
            }
        }

        private void init(Channel[] peerChannels, boolean selfCommit, Channel[] proxyChannels) {
            mPeerChannels = peerChannels;
            mSelfCommit = selfCommit;
            if (proxyChannels == null || proxyChannels.length == 0) {
                mProxyChannels = null;
                mProxy = -1;
            } else {
                mProxyChannels = proxyChannels;
                mProxy = 0;
            }
        }

        /**
         * Called when explicit failover is requested.
         */
        boolean deactivateExplicit() {
            return deactivate(DEACTIVATE_EXPLICIT, null);
        }

        /**
         * Called when the commit position is stalled.
         */
        boolean deactivateStalled(String message) {
            return deactivate(DEACTIVATE_STALLED, message);
        }

        private synchronized boolean deactivate(int mode, String reason) {
            if (mDeactivated == DEACTIVATED) {
                return false;
            }
            mDeactivated = mode;
            mDeactivateReason = reason;
            return true;
        }

        /**
         * Called to fully deactivate.
         */
        void fullyDeactivate() {
            int deactivated;
            synchronized (this) {
                deactivated = mDeactivated;
                mDeactivated = DEACTIVATED;
                mSelfCommit = false;
            }

            if (deactivated == DEACTIVATE_EXPLICIT) {
                doFailover(this, false);
            } else if (deactivated == DEACTIVATE_STALLED) {
                toFollower(this, mDeactivateReason);
            }
        }
    }

    @Override
    public void close() throws IOException {
        mChanMan.stop();
        mScheduler.shutdown();
        mStateLog.close();
        acquireExclusive();
        try {
            mSyncCommitCondition.signalAll(this);
        } finally {
            releaseExclusive();
        }
    }

    void uncaught(Throwable e) {
        if (!mChanMan.isStopped()) {
            if (e instanceof JoinException && mEventListener != null) {
                try {
                    mEventListener.notify(EventType.REPLICATION_WARNING, e.getMessage());
                } catch (Throwable e2) {
                    // Ignore.
                }
            } else {
                Utils.uncaught(e);
            }
        }
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
            protected void doRun() {
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
                mCounter = System.nanoTime() + delayMillis * 1_000_000L;
                mScheduler.scheduleNanos(this);
            }
        };
    }

    private void scheduleMissingDataTask() {
        int delayMillis = ThreadLocalRandom.current()
            .nextInt(MISSING_DELAY_LOW_MILLIS, MISSING_DELAY_HIGH_MILLIS);
        mScheduler.scheduleMillis(this::missingDataTask, delayMillis);
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

        if (mReceivingMissingData) {
            // Avoid overlapping requests for missing data if results are flowing in.
            mReceivingMissingData = false;
        } else {
            var collector = new PositionRange() {
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

            mMissingContigPosition = mStateLog.checkForMissingData
                (mMissingContigPosition, collector);

            if (collector.mSize <= 0) {
                acquireShared();
                Role local = localMemberRole();
                Channel leader = mLeaderReplyChannel;
                releaseShared();
                if (local == Role.NORMAL && leader != null
                    && leader.peer().role() == Role.STANDBY)
                {
                    acquireExclusive();
                    forceElection(); // releases exclusive latch as a side-effect
                }
            } else {
                // Only request missing data when the set of control connections changes. If
                // data is simply delayed, then don't request it again. Limit the number of
                // times the request is denied, in case data is missing for another reason.
                // The most common other reason is due to proxying, where the proxy needed to
                // request missing data, but no connections changed here.
                if (mChanMan.checkControlVersion(1)) for (int i=0; i<collector.mSize; ) {
                    long startPosition = collector.mRanges[i++];
                    long endPosition = collector.mRanges[i++];
                    requestMissingData(startPosition, endPosition);
                }
            }
        }

        scheduleMissingDataTask();
    }

    private void requestMissingData(final long startPosition, final long endPosition) {
        // Note: Missing data can be caused by the ChannelManager closing a client channel due
        // to a write timeout, which then creates a gap until a new channel is established.
        // See ChannelManager.SocketChannel::maxWriteTagCount

        event(EventType.REPLICATION_DEBUG, () ->
              String.format("Requesting missing data: %1$,d bytes @[%2$d, %3$d)",
                            endPosition - startPosition, startPosition, endPosition));

        // TODO: Need a way to abort outstanding requests.

        long remaining = endPosition - startPosition;
        long position = startPosition;

        // Distribute the request among the channels at random.

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        acquireShared();
        // Query normal, standby, and proxy members. Observers aren't first-class and might not
        // have the data.
        Channel[] channels = mProxyChannels;
        releaseShared();

        // Break up into smaller requests, to help distribute the load. A larger request count
        // provides more even distribution due to random selection, but each request is
        // smaller. Tiny requests cause more gaps to be tracked in the term log.
        long requestSize;
        if (channels.length <= 1) {
            if (channels.length == 0) {
                event(EventType.REPLICATION_PANIC, "No peers to request data from");
                return;
            }
            requestSize = remaining;
        } else {
            long requestCount = channels.length * 10L;
            requestSize = Math.max(100_000, (remaining + requestCount - 1) / requestCount);
        }

        doRequestData: while (remaining > 0) {
            long amt = Math.min(remaining, requestSize);

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
        mScheduler.scheduleMillis(this::electionTask, delayMillis);
    }

    private void electionTask() {
        try {
            acquireExclusive();
            doElectionTask();
        } finally {
            scheduleElectionTask();
        }
    }

    // Caller must acquire exclusive latch, which is released by this method.
    private void forceElection() {
        mElectionValidated = Integer.MIN_VALUE;
        mCandidateStall = 0;
        doElectionTask();
    }

    // Caller must acquire exclusive latch, which is released by this method.
    private void doElectionTask() {
        if (mLocalMode == MODE_LEADER) {
            doAffirmLeadership();
            return;
        }

        Channel[] peerChannels;

        if (mElectionValidated >= 0) {
            // Current leader or candidate is still active, so don't start an election yet.
            mElectionValidated--;

            if (mElectionValidated >= 0
                || !isCandidate(localMemberRole())
                || (peerChannels = mCandidateChannels).length <= 0)
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

            if (!isCandidate(localMemberRole())) {
                // Only NORMAL/STANDBY members can become candidates.
                releaseExclusive();
                return;
            }

            if (mCandidateStall > 0) {
                // Don't try to become a candidate too soon following a failover.
                mCandidateStall--;
                releaseExclusive();
                return;
            }

            peerChannels = mCandidateChannels;
            info = mStateLog.captureHighest();

            if (mElectionValidated > Integer.MIN_VALUE) {
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
            }

            // Convert to candidate.
            mLocalMode = MODE_CANDIDATE;

            candidateId = mChanMan.localMemberId();

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

            var b = new StringBuilder().append("Local member is ");
            if (localMemberRole() == Role.STANDBY) {
                b.append("an interim ");
            } else {
                b.append("a ");
            }
            b.append("candidate: term=").append(term).append(", highestTerm=")
                .append(info.mTerm).append(", highestPosition=").append(info.mHighestPosition);

            event(EventType.REPLICATION_INFO, b.toString());

            for (Channel peerChan : peerChannels) {
                peerChan.requestVote(null, term, candidateId, info.mTerm, info.mHighestPosition);
            }
        }
    }

    private static boolean isCandidate(Role role) {
        return role != null && role.isCandidate();
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

            if (checkException(mLeaderReplWriter)) {
                return;
            }

            mStateLog.captureHighest(writer);
            highestPosition = writer.mHighestPosition;
            commitPosition = writer.mCommitPosition;

            if (commitPosition >= highestPosition || commitPosition > mLeaderCommitPosition) {
                mElectionValidated = LOCAL_LEADER_VALIDATED;
                mLeaderCommitPosition = commitPosition;
            } else if (mElectionValidated >= 0) {
                mElectionValidated--;
            } else {
                String reason = "commit position is stalled: " +
                    commitPosition + " < " + highestPosition;
                if (mLeaderReplWriter == null) {
                    toFollower(reason);
                    return;
                } else {
                    mLeaderReplWriter.deactivateStalled(reason);
                }
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
                               null, EMPTY_DATA, 0, 0);
        }
    }

    /**
     * Only works if called from the current ReplWriter.
     */
    private void toFollower(ReplWriter from, String reason) {
        acquireExclusive();
        try {
            if (mLeaderReplWriter == from) {
                toFollower(reason);
            }            
        } finally {
            releaseExclusive();
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
            mElectionValidated = 0;

            if (mLeaderReplWriter != null) {
                // Deactivate before releasing the log underlying writer, ensuring that it's
                // not used again afterwards.
                mLeaderReplWriter.fullyDeactivate();
            }

            if (mLeaderLogWriter != null) {
                // Note: Once released, instance cannot be directly used again. Double release
                // of the writer is harmful if instance has been recycled.
                mLeaderLogWriter.release();
                mLeaderLogWriter = null;
            }

            var b = new StringBuilder("Local member ");

            if (localMemberRole() == Role.STANDBY) {
                b.append("interim ");
            }

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

            event(EventType.REPLICATION_INFO, b.toString());
        }
    }

    private void scheduleRemoveStaleMembersTask() {
        mScheduler.scheduleMillis(this::removeStaleMembers, CONNECTING_THRESHOLD_MILLIS / 2);
    }

    private void removeStaleMembers() {
        try {
            doRemoveStaleMembers();
        } catch (Throwable e) {
            uncaught(e);
        }

        if (tryAcquireShared()) {
            if (mLocalMode != MODE_LEADER) {
                // Only the leader can remove members.
                if (tryUpgrade()) {
                    mRemovingStaleMembers = false;
                    releaseExclusive();
                    return;
                }
            }
            releaseShared();
        }

        scheduleRemoveStaleMembersTask();
    }

    private void doRemoveStaleMembers() {
        Set<? extends Channel> channels = mChanMan.allChannels();

        if (channels.isEmpty()) {
            return;
        }

        long now = Long.MIN_VALUE;

        for (Channel channel : channels) {
            long startedAt = channel.connectAttemptStartedAt();
            if (startedAt == Long.MAX_VALUE) {
                continue;
            }
            if (now == Long.MIN_VALUE) {
                now = System.currentTimeMillis();
            }
            long duration = now - startedAt;
            if (duration <= CONNECTING_THRESHOLD_MILLIS) {
                continue;
            }

            Peer peer = channel.peer();
            Role role = peer.role();
            if (role != Role.RESTORING) {
                // Only remove stale restoring members for now.
                continue;
            }

            Consumer<byte[]> acceptor = mControlMessageAcceptor;
            if (acceptor == null) {
                // Cannot do anything without this.
                return;
            }

            byte[] message = mGroupFile.proposeRemovePeer(CONTROL_OP_UNJOIN, peer.mMemberId, null);

            if (registerControlMessage(message)) {
                try {
                    acceptor.accept(message);
                } finally {
                    unregisterControlMessage(message);
                }
            }
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
        var in = new ChannelInputStream(s.getInputStream(), 100, false);

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
            var eout = new EncodingOutputStream();
            eout.write(GroupJoiner.OP_ADDRESS);
            eout.encodeStr(GroupFile.addressToString(leaderPeer.mAddress));
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

                // Note: This callback is invoked with the GroupFile shared latch held.

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

                    var buf = new byte[1000];
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
                // Note: This callback is invoked without any GroupFile latch.

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

        mScheduler.scheduleMillis(() -> {
            if (mGroupFile.discardProposeConsumer(message)) {
                closeQuietly(s);
            }
        }, JOIN_TIMEOUT_MILLIS);

        if (registerControlMessage(message)) {
            try {
                acceptor.accept(message);
            } finally {
                unregisterControlMessage(message);
            }
        }

        return true;
    }

    private static boolean joinFailure(Socket s, byte errorCode) throws IOException {
        s.getOutputStream().write(new byte[] {GroupJoiner.OP_ERROR, errorCode});
        return false;
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

    private void event(EventType type, String message) {
        if (mEventListener != null) {
            try {
                mEventListener.notify(type, message);
            } catch (Throwable e) {
                // Ignore.
            }
        }
    }

    private void event(EventType type, Supplier<String> message) {
        if (mEventListener != null) {
            try {
                mEventListener.notify(type, message.get());
            } catch (Throwable e) {
                // Ignore.
            }
        }
    }

    private void commitConflict(Channel from, CommitConflictException e) {
        if (from != null) {
            Peer peer = from.peer();
            if (peer != null && !peer.role().providesConsensus()) {
                // Only report the conflict if it came from an authority, in which case the
                // conflict won't likely go away,
                return;
            }
        }

        // TODO: Potential leader could pull data from proxies (not observers) to prevent the
        // conflict in the first place. Note that this doesn't work if proxy is behind. It
        // might know the term, but it might not have the data.

        long now = System.currentTimeMillis();

        if (now >= (mLastConflictReport + COMMIT_CONFLICT_REPORT_MILLIS)) {
            if (mEventListener == null) {
                uncaught(e);
            } else {
                var b = new StringBuilder()
                    .append(e.isFatal() ? "Fatal commit" : "Commit")
                    .append(" conflict detected: position=").append(e.mPosition)
                    .append(", conflicting term=").append(e.mTermInfo.mTerm)
                    .append(", commit position=").append(e.mTermInfo.mCommitPosition);
                if (!e.isFatal()) {
                    b.append(". Restarting might rollback the conflict and resolve the issue.");
                }
                mEventListener.notify(EventType.REPLICATION_PANIC, b.toString());
            }

            mLastConflictReport = now;
        }
    }

    /**
     * Call this to prevent a flood of pending control messages which request the same thing.
     * Latch must not be held by caller, but it might be acquired and released here.
     *
     * @return false if message is already registered, or is null
     */
    private boolean registerControlMessage(byte[] message) {
        if (message == null) {
            return false;
        }

        Set<byte[]> registered = mRegisteredControlMessages;

        if (registered == null) {
            acquireExclusive();
            try {
                registered = mRegisteredControlMessages;
                if (registered == null) {
                    mRegisteredControlMessages = registered =
                        new ConcurrentSkipListSet<>(Arrays::compareUnsigned);
                    registered.add(message);
                    return true;
                }
            } finally {
                releaseExclusive();
            }
        }

        return registered.add(message);
    }

    /**
     * Remove a message registered earlier. Latch must not be held by caller, but it might be
     * acquired and released here.
     */
    private void unregisterControlMessage(byte[] message) {
        Set<byte[]> registered = mRegisteredControlMessages;

        if (registered != null) {
            registered.remove(message);
            if (registered.isEmpty()) {
                acquireShared();
                if (registered == mRegisteredControlMessages && registered.isEmpty()) {
                    // No harm if multiple threads clear this field concurrently.
                    mRegisteredControlMessages = null;
                }
                releaseShared();
            }
        }
    }

    /**
     * Called from a remote group member.
     */
    @Override // Channel
    public void unknown(Channel from, int op) {
        event(EventType.REPLICATION_WARNING,
              "Unknown operation received from: " + from.peer().mAddress + ", op=" + op);
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

            if (currentTerm >= originalTerm && !isBehind(from, highestTerm, highestPosition)) {
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

    private boolean isBehind(Channel from, long term, long position) {
        LogInfo info = mStateLog.captureHighest();
        return term < info.mTerm || (term == info.mTerm && position < info.mHighestPosition)
            // Don't elect an interim leader if it's at the same position.
            || (localMemberRole() == Role.NORMAL && from.peer().role() == Role.STANDBY
                && position <= info.mHighestPosition);
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
            var b = new StringBuilder().append("Local member is ");
            if (localMemberRole() == Role.STANDBY) {
                b.append("an interim ");
            } else {
                b.append("the ");
            }
            b.append("leader: term=").append(term).append(", position=").append(position);

            event(EventType.REPLICATION_INFO, b.toString());

            mElectionValidated = LOCAL_LEADER_VALIDATED;

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

        if (position > 0 && mFailoverLagTimeoutMillis >= 0) {
            // Schedule a task which forces a failover if a writer isn't created in time. This
            // implies that the member is lagging behind and shouldn't be the leader.
            ReplWriter old = mLeaderReplWriter; // expected to be null
            mScheduler.scheduleMillis(() -> doFailover(old, true), mFailoverLagTimeoutMillis);
        }

        if (!mRemovingStaleMembers) {
            mRemovingStaleMembers = true;
            scheduleRemoveStaleMembersTask();
        }

        doAffirmLeadership();
    }

    /**
     * Called from a remote group member.
     */
    @Override // Channel
    public boolean forceElection(Channel from) {
        event(EventType.REPLICATION_INFO,
              "Forcing an election, as requested by: " + from.peer().mAddress);
        acquireExclusive();
        forceElection(); // releases exclusive latch as a side-effect
        return true;
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
        } catch (CommitConflictException e) {
            commitConflict(from, e);
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
        if (endPosition <= startPosition) {
            return true;
        }

        Peer peer = from.peer();
        if (peer == null) {
            return true;
        }

        RangeSet rangeSet = peer.queryData(startPosition, endPosition);
        if (rangeSet == null) {
            // Assume a thread is already running.
            return true;
        }

        return mScheduler.execute(() -> {
            RangeSet set = rangeSet;
            while (true) {
                RangeSet.Range range = set.removeLowest();
                if (range == null) {
                    set = peer.finishedQueries(set);
                    if (set == null) {
                        return;
                    }
                } else {
                    try {
                        doQueryData(from, range.start, range.end);
                    } catch (Throwable e) {
                        if (!(e instanceof InvalidReadException)) {
                            uncaught(e);
                        }
                        peer.discardQueries(set);
                        return;
                    }
                }
            }
        });
    }

    private void doQueryData(Channel from, long startPosition, long endPosition)
        throws IOException
    {
        LogReader reader = mStateLog.openReader(startPosition);

        try {
            long remaining = endPosition - startPosition;
            var buf = new byte[(int) Math.min(9000, remaining)];

            while (true) {
                long currentTerm = 0;

                long prevTerm = reader.prevTerm();
                long position = reader.position();
                long term = reader.term();

                int require = (int) Math.min(buf.length, remaining);
                int amt = reader.tryRead(buf, 0, require);

                if (amt == 0) {
                    // If the leader, read past the commit position, up to the highest position.
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
                        continue;
                    }
                    from.queryDataReplyMissing
                        (null, currentTerm, prevTerm, term, position, position + remaining);
                    break;
                }

                if (!from.queryDataReply
                    (null, currentTerm, prevTerm, term, position, buf, 0, amt))
                {
                    break;
                }

                startPosition += amt;
                remaining -= amt;

                if (remaining <= 0) {
                    break;
                }
            }
        } finally {
            reader.release();
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
        } catch (CommitConflictException e) {
            commitConflict(from, e);
        } catch (IOException e) {
            uncaught(e);
        }

        return true;
    }

    /**
     * Called from a remote group member.
     */
    @Override // Channel
    public boolean queryDataReplyMissing(Channel from, long currentTerm,
                                         long prevTerm, long term,
                                         long startPosition, long endPosition)
    {
        if (currentTerm != 0 && validateLeaderTerm(from, currentTerm) == this) {
            return false;
        }

        // No missing data was received by this reply, but indicate progress.
        mReceivingMissingData = true;

        try {
            queryReplyTermCheck(term);
        } catch (IOException e) {
            uncaught(e);
        }

        // Replica peer doesn't have the data, but the leader should have it.

        Channel requestChannel = leaderRequestChannel();
        if (requestChannel == null || requestChannel == this) {
            return false;
        }

        // Prune the range if the contiguous position has advanced.
        TermLog termLog = mStateLog.termLogAt(startPosition);
        if (termLog != null) {
            startPosition = Math.max(startPosition, termLog.contigPosition());
            if (startPosition >= endPosition) {
                return true;
            }
        }

        return requestChannel.queryData(this, startPosition, endPosition);
    }

    /**
     * Called from a remote group member.
     *
     * @param from pass null if called from writeDataViaProxy
     */
    @Override // Channel
    public boolean writeData(Channel from, long prevTerm, long term, long position,
                             long highestPosition, long commitPosition,
                             byte[] prefix, byte[] data, int off, int len)
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
                writer.write(prefix, data, off, len, highestPosition);
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
        } catch (CommitConflictException e) {
            commitConflict(from, e);
        } catch (IOException e) {
            uncaught(e);
        }

        if (from != null && mGroupFile.localMemberRoleOpaque().providesConsensus()) {
            // TODO: Can skip reply if successful and highest didn't change.
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
        boolean exclusive = false;

        validate: while (true) {
            final long originalTerm = mCurrentTerm;

            if (term == originalTerm) {
                if (mElectionValidated > 0) {
                    if (from == null && term == mValidatedTerm) {
                        from = mLeaderReplyChannel;
                    }
                    release(exclusive);
                    return from; // validation success
                }
                if (exclusive || tryUpgrade()) {
                    break validate;
                }
                releaseShared();
                acquireExclusive();
                exclusive = true;
                continue;
            }

            if (term < originalTerm) {
                release(exclusive);
                return this; // validation failed
            }

            if (!exclusive) {
                if (tryUpgrade()) {
                    exclusive = true;
                } else {
                    releaseShared();
                    acquireExclusive();
                    exclusive = true;
                    continue;
                }
            }

            try {
                try {
                    mCurrentTerm = mStateLog.checkCurrentTerm(term);
                } catch (IOException e) {
                    uncaught(e);
                    releaseExclusive();
                    return this; // validation failed
                }
                if (mCurrentTerm <= originalTerm) {
                    continue;
                }
                toFollower(null);
            } catch (Throwable e) {
                releaseExclusive();
                throw e;
            }
        }

        if (from == null) {
            if (term == mValidatedTerm) {
                from = mLeaderReplyChannel;
            }
            if (from == null) {
                // Cannot fully complete validation without a reply channel. A message directly
                // from the leader is required.
                releaseExclusive();
                return from; // validation success (mostly)
            }
        }

        mElectionValidated = 1;
        mLeaderReplyChannel = from;

        boolean first = false;
        if (term != mValidatedTerm) {
            mValidatedTerm = term;
            // Find it later when leaderRequestChannel is called.
            mLeaderRequestChannel = null;
            first = true;
            mCandidateStall = 0;
        }

        releaseExclusive();

        if (first) {
            var b = new StringBuilder().append("Remote member is ");
            if (from.peer().role() == Role.STANDBY) {
                b.append("an interim ");
            } else {
                b.append("the ");
            }
            b.append("leader: ").append(from.peer().mAddress).append(", term=").append(term);
            event(EventType.REPLICATION_INFO, b.toString());
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
                                     byte[] prefix, byte[] data, int off, int len)
    {
        writeData(from, prevTerm, term, position, highestPosition, commitPosition,
                  prefix, data, off, len);

        // Always proxy the data, even if it was locally rejected. The local member might be a
        // bit behind with respect to the current leadership status.

        Peer fromPeer = from.peer();

        Channel[] peerChannels;
        acquireShared();
        peerChannels = mAllChannels;
        releaseShared();

        if (peerChannels != null) {
            for (Channel peerChan : peerChannels) {
                Peer peer = peerChan.peer();
                if (!fromPeer.equals(peer)) {
                    peerChan.writeDataViaProxy(null, prevTerm, term,
                                               position, highestPosition, commitPosition,
                                               prefix, data, off, len);
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
                                     byte[] prefix, byte[] data, int off, int len)
    {
        // From is null to prevent proxy channel from being selected as the leader channel.
        return writeData(null, prevTerm, term, position, highestPosition, commitPosition,
                         prefix, data, off, len);
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
                    mSyncCommitCondition.signalAll(this);
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
        from.peer().updateCompactPosition(position);
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

            var key = new Peer(memberId);
            Peer peer = mGroupFile.allPeers().ceiling(key); // findGe

            if (peer == null || peer.mMemberId != memberId) {
                // Member doesn't exist.
                // TODO: Permit leader to change itself.
                result = ErrorCodes.UNKNOWN_MEMBER;
                break tryUpdateRole;
            }

            Role currentRole = peer.role();

            if (currentRole == role) {
                // Role already matches.
                result = ErrorCodes.SUCCESS;
                break tryUpdateRole;
            }

            message = mGroupFile.proposeUpdateRole(CONTROL_OP_UPDATE_ROLE, memberId, role);

            if (currentRole.providesConsensus() != role.providesConsensus()) {
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
                    for (Channel channel : mCandidateChannels) {
                        channel.groupVersion(this, groupVersion);
                    }

                    break tryUpdateRole;
                }
            }

            result = 0;
        } finally {
            releaseShared();
        }

        if (registerControlMessage(message)) {
            // The acceptor can block, so call it in a separate thread.
            final byte[] fmessage = message;
            mScheduler.execute(() -> {
                try {
                    acceptor.accept(fmessage);
                } finally {
                    unregisterControlMessage(fmessage);
                }
            });
        }

        from.updateRoleReply(null, groupVersion, memberId, result);

        return true;
    }

    /**
     * Called from a remote group member.
     */
    @Override // Channel
    public boolean updateRoleReply(Channel from, long groupVersion, long memberId, byte result) {
        boolean versionOk = checkGroupVersion(groupVersion);

        if (result != ErrorCodes.SUCCESS) {
            acquireShared();
            Role desiredRole = desiredRole();
            releaseShared();
            if (desiredRole != null && (result != ErrorCodes.VERSION_MISMATCH || !versionOk)) {
                event(ErrorCodes.typeFor(result),
                      "Unable to update role: " + ErrorCodes.toString(result));
            }
        }

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
            from.groupFileReply(null, null, out -> {
                try {
                    mGroupFile.writeTo(out);
                } catch (IOException e) {
                    rethrow(e);
                }
            });
        }

        return true;
    }

    /**
     * Called from a remote group member.
     */
    @Override // Channel
    public boolean groupFileReply(Channel from, InputStream in, Consumer<OutputStream> unused)
        throws IOException
    {
        if (in == null || unused != null) {
            throw new IllegalArgumentException();
        }

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

        return true;
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
     *
     * @return false if group version is stale and the request for a new group file failed
     */
    private boolean checkGroupVersion(long groupVersion) {
        if (groupVersion > mGroupFile.version()) {
            Channel requestChannel = leaderRequestChannel();
            if (requestChannel != null && requestChannel != this) {
                try {
                    requestChannel.groupFile(this, mGroupFile.version());
                    return true;
                } catch (IOException e) {
                }
            }
            return false;
        }

        return true;
    }
}
