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

import java.io.InterruptedIOException;
import java.io.IOException;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import java.util.function.Consumer;
import java.util.concurrent.ThreadLocalRandom;

import java.util.function.Consumer;
import java.util.function.LongConsumer;

import org.cojen.tupl.util.Latch;

import static org.cojen.tupl.io.Utils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class Controller extends Latch implements StreamReplicator, Channel {
    private static final int MODE_FOLLOWER = 0, MODE_CANDIDATE = 1, MODE_LEADER = 2;
    private static final int ELECTION_DELAY_LOW_MILLIS = 200, ELECTION_DELAY_HIGH_MILLIS = 300;
    private static final int QUERY_TERMS_RATE_MILLIS = 1;
    private static final int MISSING_DELAY_LOW_MILLIS = 400, MISSING_DELAY_HIGH_MILLIS = 600;
    private static final int CONNECT_TIMEOUT_MILLIS = 500;
    private static final int SNAPSHOT_REPLY_TIMEOUT_MILLIS = 2000;
    private static final int MISSING_DATA_REQUEST_SIZE = 100_000;

    private final Scheduler mScheduler;
    private final ChannelManager mChanMan;
    private final StateLog mStateLog;

    private int mLocalMode;

    private Peer[] mPeers;
    private Channel[] mPeerChannels;

    private long mCurrentTerm;
    private long mVotedFor;
    private int mGrantsRemaining;
    private int mElectionValidated;

    private LogWriter mLeaderLogWriter;
    private ReplWriter mLeaderReplWriter;

    // Index used to check for missing data.
    private long mMissingContigIndex = Long.MAX_VALUE; // unknown initially
    private boolean mSkipMissingDataTask;
    private volatile boolean mReceivingMissingData;

    // Limit the rate at which missing terms are queried.
    private volatile long mNextQueryTermTime = Long.MIN_VALUE;

    Controller(StateLog log, long groupToken) {
        mStateLog = log;
        mScheduler = new Scheduler();
        mChanMan = new ChannelManager(mScheduler, groupToken);
    }

    /**
     * @param localSocket optional; used for testing
     */
    void init(Map<Long, SocketAddress> members, long localMemberId, ServerSocket localSocket)
        throws IOException
    {
        acquireExclusive();
        try {
            Peer[] peers = new Peer[members.size() - 1];
            Channel[] peerChannels = new Channel[peers.length];

            if (localSocket == null) {
                mChanMan.setLocalMemberId(localMemberId, members.get(localMemberId));
            } else {
                mChanMan.setLocalMemberId(localMemberId, localSocket);
            }

            int i = 0;
            for (Map.Entry<Long, SocketAddress> e : members.entrySet()) {
                long memberId = e.getKey();
                if (memberId != localMemberId) {
                    Peer peer = new Peer(memberId, e.getValue());
                    peers[i] = peer;
                    peerChannels[i] = mChanMan.connect(peer, this);
                    i++;
                }
            }

            mPeers = peers;
            mPeerChannels = peerChannels;
        } finally {
            releaseExclusive();
        }
    }

    @Override
    public boolean start() throws IOException {
        return start(0, 0, 0, false);
    }

    @Override
    public boolean start(long prevTerm, long term, long index) throws IOException {
        return start(prevTerm, term, index, true);
    }

    private boolean start(long prevTerm, long term, long index, boolean truncateStart)
        throws IOException
    {
        if (index < 0) {
            throw new IllegalArgumentException("Start index: " + index);
        }

        if (mChanMan.isStarted()) {
            return false;
        }

        if (truncateStart) {
            mStateLog.truncateStart(index);
            mStateLog.defineTerm(prevTerm, term, index);
        }

        mChanMan.start(this);

        scheduleElectionTask();
        scheduleMissingDataTask();

        return true;
    }

    @Override
    public Reader newReader(long index, boolean follow) {
        if (follow) {
            return mStateLog.openReader(index);
        }

        acquireShared();
        try {
            Reader reader;
            if (mLeaderLogWriter != null
                && index >= mLeaderLogWriter.termStartIndex()
                && index < mLeaderLogWriter.termEndIndex())
            {
                reader = null;
            } else {
                reader = mStateLog.openReader(index);
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
    public Writer newWriter(long index) {
        if (index < 0) {
            throw new IllegalArgumentException();
        }
        return createWriter(index);
    }

    private Writer createWriter(long index) {
        acquireExclusive();
        try {
            if (mLeaderReplWriter != null) {
                throw new IllegalStateException("Writer already exists");
            }
            if (mLeaderLogWriter == null || (index >= 0 && index != mLeaderLogWriter.index())) {
                return null;
            }
            ReplWriter writer = new ReplWriter(mLeaderLogWriter, mPeerChannels);
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
    public long getLocalMemberId() {
        return mChanMan.getLocalMemberId();
    }

    @Override
    public SocketAddress getLocalAddress() {
        return mChanMan.getLocalAddress();
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
    public void controlMessageReceived(byte[] message) {
        // FIXME
    }

    @Override
    public void controlMessageAcceptor(Consumer<byte[]> acceptor) {
        // FIXME
    }

    @Override
    public SnapshotReceiver requestSnapshot(Map<String, String> options) throws IOException {
        acquireShared();
        Channel[] channels = mPeerChannels;
        releaseShared();

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
            return null;
        }

        Collections.shuffle(results); // random selection in case of ties
        Collections.sort(results); // stable sort

        Socket sock = mChanMan.connectSnapshot(results.get(0).mChannel.peer().mAddress);

        try {
            return new SocketSnapshotReceiver(sock, options);
        } catch (IOException e) {
            closeQuietly(e, sock);
            throw e;
        }
    }

    @Override
    public void snapshotRequestAcceptor(Consumer<SnapshotSender> acceptor) {
        final class Sender extends SocketSnapshotSender {
            Sender(Socket socket) throws IOException {
                super(socket);
            }

            @Override
            TermLog termLogAt(long index) {
                return mStateLog.termLogAt(index);
            }
        }

        mChanMan.snapshotRequestAcceptor(sock -> {
            SnapshotSender sender;
            try {
                sender = new Sender(sock);
            } catch (IOException e) {
                closeQuietly(e, sock);
                return;
            }

            // FIXME: register the sender

            mScheduler.execute(() -> acceptor.accept(sender));
        });
    }

    final class ReplWriter implements Writer {
        private final LogWriter mWriter;
        private Channel[] mPeerChannels;

        ReplWriter(LogWriter writer, Channel[] peerChannels) {
            mWriter = writer;
            mPeerChannels = peerChannels;
        }

        @Override
        public long term() {
            return mWriter.term();
        }

        @Override
        public long termStartIndex() {
            return mWriter.termStartIndex();
        }

        @Override
        public long termEndIndex() {
            return mWriter.termEndIndex();
        }

        @Override
        public long index() {
            return mWriter.index();
        }

        @Override
        public int write(byte[] data, int offset, int length, long highestIndex)
            throws IOException
        {
            Channel[] peerChannels;
            long prevTerm, term, index, commitIndex;
            int amt;

            synchronized (this) {
                peerChannels = mPeerChannels;

                if (peerChannels == null) {
                    return -1;
                }

                LogWriter writer = mWriter;
                index = writer.index();

                amt = writer.write(data, offset, length, highestIndex);

                if (amt <= 0) {
                    if (length > 0) {
                        mPeerChannels = null;
                    }
                    return amt;
                }

                mStateLog.captureHighest(writer);
                highestIndex = writer.mHighestIndex;

                if (peerChannels.length == 0) {
                    // Only a group of one, so commit changes immediately.
                    mStateLog.commit(highestIndex);
                    return amt;
                }

                prevTerm = writer.prevTerm();
                term = writer.term();
                commitIndex = writer.mCommitIndex;
            }

            // FIXME: stream it
            data = Arrays.copyOfRange(data, offset, offset + length);

            for (Channel peerChan : peerChannels) {
                peerChan.writeData(null, prevTerm, term, index, highestIndex, commitIndex, data);
            }

            return amt;
        }

        @Override
        public long waitForCommit(long index, long nanosTimeout) throws InterruptedIOException {
            return mWriter.waitForCommit(index, nanosTimeout);
        }

        @Override
        public void uponCommit(long index, LongConsumer task) {
            mWriter.uponCommit(index, task);
        }

        @Override
        public void close() {
            mWriter.close();
            writerClosed(this);
        }

        synchronized void deactivate() {
            mPeerChannels = null;
        }
    }

    @Override
    public void close() throws IOException {
        mChanMan.stop();
        mScheduler.shutdown();
        mStateLog.close();
    }

    private static void waitForConnections(Channel[] channels) throws InterruptedIOException {
        int timeoutMillis = CONNECT_TIMEOUT_MILLIS;
        for (Channel channel : channels) {
            timeoutMillis = channel.waitForConnection(timeoutMillis);
        }
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
                mMissingContigIndex = Long.MAX_VALUE; // reset to unknown
                mSkipMissingDataTask = true;
                releaseShared();
                return;
            }
            releaseShared();
        }

        if (mReceivingMissingData) {
            // Avoid overlapping requests for missing data if results are flowing in.
            mReceivingMissingData = false;
        }

        class Collector implements IndexRange {
            long[] mRanges;
            int mSize;

            @Override
            public void range(long startIndex, long endIndex) {
                if (mRanges == null) {
                    mRanges = new long[16];
                } else if (mSize >= mRanges.length) {
                    mRanges = Arrays.copyOf(mRanges, mRanges.length << 1);
                }
                mRanges[mSize++] = startIndex;
                mRanges[mSize++] = endIndex;
            }
        };

        Collector collector = new Collector();
        mMissingContigIndex = mStateLog.checkForMissingData(mMissingContigIndex, collector);

        for (int i=0; i<collector.mSize; ) {
            long startIndex = collector.mRanges[i++];
            long endIndex = collector.mRanges[i++];
            requestMissingData(startIndex, endIndex);
        }

        scheduleMissingDataTask();
    }

    private void requestMissingData(long startIndex, long endIndex) {
        // FIXME: Need a way to abort outstanding requests.
        System.out.println("must call queryData! " + startIndex + ".." + endIndex);

        long remaining = endIndex - startIndex;

        // Distribute the request among the channels at random.

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        acquireShared();
        Channel[] channels = mPeerChannels;
        releaseShared();

        doRequestData: while (remaining > 0) {
            long amt = Math.min(remaining, MISSING_DATA_REQUEST_SIZE);

            int selected = rnd.nextInt(channels.length);
            int attempts = 0;

            while (true) {
                Channel channel = channels[selected];

                if (channel.queryData(this, startIndex, startIndex + amt)) {
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

            startIndex += amt;
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
        Channel[] peerChannels;
        long term, candidateId;
        LogInfo info = new LogInfo();

        acquireExclusive();
        try {
            if (mLocalMode == MODE_LEADER) {
                // FIXME: If commit index is lagging behind and not moving, switch to follower.
                // This isn't in the Raft algorithm, but it really should be.
                affirmLeadership();
                return;
            }

            if (mLocalMode == MODE_CANDIDATE) {
                // Abort current election and start a new one.
                toFollower();
            }

            if (mElectionValidated >= 0) {
                // Current leader is still active, so don't start an election yet.
                mElectionValidated--;
                releaseExclusive();
                return;
            }

            // Convert to candidate.
            // FIXME: Don't permit rogue members to steal leadership if process is
            // suspended. Need to double check against local clock to detect this. Or perhaps
            // check with peers to see if leader is up.
            mLocalMode = MODE_CANDIDATE;

            peerChannels = mPeerChannels;

            mStateLog.captureHighest(info);

            try {
                mCurrentTerm = term = mStateLog.incrementCurrentTerm(1);
            } catch (IOException e) {
                releaseExclusive();
                uncaught(e);
                return;
            }

            candidateId = mChanMan.getLocalMemberId();
            mVotedFor = candidateId;

            // Only need a majority of vote grants (already voted for self).
            mGrantsRemaining = (peerChannels.length + 1) / 2;
        } catch (Throwable e) {
            releaseExclusive();
            throw e;
        }

        if (mGrantsRemaining == 0) {
            // Only a group of one, so become leader immediately.
            toLeader(term, info.mHighestIndex);
        } else {
            releaseExclusive();

            for (Channel peerChan : peerChannels) {
                peerChan.requestVote(null, term, candidateId, info.mTerm, info.mHighestIndex);
            }
        }
    }

    // Caller must acquire exclusive latch, which is released by this method.
    private void affirmLeadership() {
        Channel[] peerChannels = mPeerChannels;
        LogWriter writer = mLeaderLogWriter;
        releaseExclusive();

        if (writer == null) {
            // Not leader anymore.
            return;
        }

        long prevTerm = writer.prevTerm();
        long term = writer.term();
        long index = writer.index();

        mStateLog.captureHighest(writer);
        long highestIndex = writer.mHighestIndex;
        long commitIndex = writer.mCommitIndex;

        // FIXME: use a custom command
        // FIXME: ... or use standard client write method
        byte[] EMPTY_DATA = new byte[0];

        for (Channel peerChan : peerChannels) {
            peerChan.writeData(null, prevTerm, term, index, highestIndex, commitIndex, EMPTY_DATA);
        }
    }

    // Caller must hold exclusive latch.
    private void toFollower() {
        final int originalMode = mLocalMode;
        if (originalMode != MODE_FOLLOWER) {
            System.out.println("follower: " + mCurrentTerm);

            mLocalMode = MODE_FOLLOWER;
            mVotedFor = 0;
            mGrantsRemaining = 0;

            if (mLeaderLogWriter != null) {
                mLeaderLogWriter.release();
                mLeaderLogWriter = null;
            }

            if (mLeaderReplWriter != null) {
                mLeaderReplWriter.deactivate();
            }

            if (originalMode == MODE_LEADER && mSkipMissingDataTask) {
                mSkipMissingDataTask = false;
                scheduleMissingDataTask();
            }
        }
    }

    @Override
    public boolean nop(Channel from) {
        return true;
    }

    @Override
    public boolean requestVote(Channel from, long term, long candidateId,
                               long highestTerm, long highestIndex)
    {
        long currentTerm;
        acquireExclusive();
        try {
            final long originalTerm = mCurrentTerm;

            try {
                mCurrentTerm = currentTerm = mStateLog.checkCurrentTerm(term);
            } catch (IOException e) {
                uncaught(e);
                return false;
            }

            if (currentTerm > originalTerm) {
                toFollower();
            }

            if (currentTerm >= originalTerm
                && ((mVotedFor == 0 || mVotedFor == candidateId)
                    && !isBehind(highestTerm, highestIndex)))
            {
                mVotedFor = candidateId;
                // Set voteGranted result bit to true.
                currentTerm |= 1L << 63;
                mElectionValidated = 1;
            }
        } finally {
            releaseExclusive();
        }

        from.requestVoteReply(null, currentTerm);
        return true;
    }

    private boolean isBehind(long term, long index) {
        LogInfo info = mStateLog.captureHighest();
        return term < info.mTerm || (term == info.mTerm && index < info.mHighestIndex);
    }

    @Override
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

            if (mLocalMode == MODE_CANDIDATE) {
                LogInfo info = mStateLog.captureHighest();
                toLeader(term, info.mHighestIndex);
                return true;
            }
        } else {
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
        }

        toFollower();
        releaseExclusive();
        return true;
    }

    // Caller must acquire exclusive latch, which is released by this method.
    private void toLeader(long term, long index) {
        try {
            long prevTerm = mStateLog.termLogAt(index).prevTermAt(index);
            System.out.println("leader: " + prevTerm + " -> " + term + " @" + index);
            mLeaderLogWriter = mStateLog.openWriter(prevTerm, term, index);
            mLocalMode = MODE_LEADER;
            for (Channel channel : mPeerChannels) {
                channel.peer().mMatchIndex = 0;
            }
        } catch (Throwable e) {
            releaseExclusive();
            uncaught(e);
        }

        affirmLeadership();
    }

    @Override
    public boolean queryTerms(Channel from, long startIndex, long endIndex) {
        mStateLog.queryTerms(startIndex, endIndex, (prevTerm, term, index) -> {
            from.queryTermsReply(null, prevTerm, term, index);
        });

        return true;
    }

    @Override
    public boolean queryTermsReply(Channel from, long prevTerm, long term, long startIndex) {
        try {
            queryReplyTermCheck(term);

            mStateLog.defineTerm(prevTerm, term, startIndex);
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
                    toFollower();
                }
            }
        } finally {
            releaseExclusive();
        }
    }

    @Override
    public boolean queryData(Channel from, long startIndex, long endIndex) {
        if (endIndex <= startIndex) {
            return true;
        }

        try {
            LogReader reader = mStateLog.openReader(startIndex);

            try {
                long remaining = endIndex - startIndex;
                byte[] buf = new byte[(int) Math.min(9000, remaining)];

                while (true) {
                    long prevTerm = reader.prevTerm();
                    long index = reader.index();
                    long term = reader.term();

                    int amt = reader.readAny(buf, 0, (int) Math.min(buf.length, remaining));

                    if (amt <= 0) {
                        if (amt < 0) {
                            // Move to next term.
                            reader.release();
                            reader = mStateLog.openReader(startIndex);
                        }
                        break;
                    }

                    // FIXME: stream it
                    byte[] data = new byte[amt];
                    System.arraycopy(buf, 0, data, 0, amt);

                    from.queryDataReply(null, prevTerm, term, index, data);

                    startIndex += amt;
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

        return true;
    }

    @Override
    public boolean queryDataReply(Channel from, long prevTerm, long term,
                                  long index, byte[] data)
    {
        mReceivingMissingData = true;

        try {
            queryReplyTermCheck(term);

            LogWriter writer = mStateLog.openWriter(prevTerm, term, index);
            if (writer != null) {
                try {
                    writer.write(data, 0, data.length, 0);
                } finally {
                    writer.release();
                }
            }
        } catch (IOException e) {
            uncaught(e);
        }

        return true;
    }

    @Override
    public boolean writeData(Channel from, long prevTerm, long term, long index,
                             long highestIndex, long commitIndex, byte[] data)
    {
        checkTerm: {
            acquireShared();
            long originalTerm = mCurrentTerm;

            if (term == originalTerm) {
                if (mElectionValidated > 0) {
                    releaseShared();
                    break checkTerm;
                } else if (tryUpgrade()) {
                    mElectionValidated = 1;
                    releaseExclusive();
                    break checkTerm;
                }
            }

            if (!tryUpgrade()) {
                releaseShared();
                acquireExclusive();
                originalTerm = mCurrentTerm;
            }

            try {
                if (term == originalTerm) {
                    mElectionValidated = 1;
                } else {
                    try {
                        mCurrentTerm = mStateLog.checkCurrentTerm(term);
                    } catch (IOException e) {
                        uncaught(e);
                        return false;
                    }
                    if (mCurrentTerm > originalTerm) {
                        toFollower();
                    }
                }
            } finally {
                releaseExclusive();
            }
        }

        try {
            LogWriter writer = mStateLog.openWriter(prevTerm, term, index);

            if (writer == null) {
                // FIXME: stash the write for later
                // Cannot write because terms are missing, so request them.
                long now = System.currentTimeMillis();
                if (now >= mNextQueryTermTime) {
                    LogInfo info = mStateLog.captureHighest();
                    if (highestIndex > info.mCommitIndex && index > info.mCommitIndex) {
                        from.queryTerms(null, info.mCommitIndex, index);
                    }
                    mNextQueryTermTime = now + QUERY_TERMS_RATE_MILLIS;
                }

                return true;
            }

            try {
                writer.write(data, 0, data.length, highestIndex);
                mStateLog.commit(commitIndex);
                mStateLog.captureHighest(writer);
                long highestTerm = writer.mTerm;
                if (highestTerm < term) {
                    // If the highest term is lower than the leader's, don't bother reporting
                    // it. The leader will just reject the reply.
                    return true;
                }
                term = highestTerm;
                highestIndex = writer.mHighestIndex;
            } finally {
                writer.release();
            }
        } catch (IOException e) {
            uncaught(e);
        }

        // TODO: can skip reply if successful and highest didn't change
        from.writeDataReply(null, term, highestIndex);

        return true;
    }

    @Override
    public boolean writeDataReply(Channel from, long term, long highestIndex) {
        LogWriter writer;
        long commitIndex;

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
                    toFollower();
                } else {
                    // Cannot commit on behalf of older terms.
                }
                return true;
            }

            Peer peer = from.peer();
            long matchIndex = peer.mMatchIndex;

            if (highestIndex <= matchIndex) {
                return true;
            }

            // Updating and sorting the peers by match index is simple, but for large groups
            // (>10?), a tree data structure should be used instead.
            peer.mMatchIndex = highestIndex;
            Arrays.sort(mPeers);

            commitIndex = mPeers[mPeers.length >> 1].mMatchIndex;
        } finally {
            releaseExclusive();
        }

        mStateLog.commit(commitIndex);
        return true;
    }

    @Override
    public boolean snapshotScore(Channel from) {
        System.out.println("snapshotScore: " + from);

        // FIXME: reply with high session count and weight if no acceptor is installed

        acquireShared();
        int mode = mLocalMode;
        releaseShared();

        // FIXME: provide active session count
        from.snapshotScoreReply(null, 0, mode == MODE_LEADER ? 1 : -1);

        return true;
    }

    @Override
    public boolean snapshotScoreReply(Channel from, int activeSessions, float weight) {
        synchronized (from) {
            from.peer().snapshotScoreReply(activeSessions, weight);
        }
        return true;
    }
}
