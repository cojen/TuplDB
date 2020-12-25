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

import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.IOException;
import java.io.OutputStream;

import java.util.function.Consumer;

/**
 * Defines a collection of remote asynchronous methods. Method invocations are permitted to
 * block if the send buffer is full.
 *
 * @author Brian S O'Neill
 */
interface Channel {
    default boolean isConnected() {
        return true;
    }

    default Peer peer() {
        return null;
    }

    /**
     * Returns immediately if connected, else waits a bit until connected. Connection
     * establishment happens in the background, and channels attempt to stay connected unless
     * explicitly closed.
     *
     * @param timeoutMillis is infinite if negative
     * @return timeout remaining
     */
    default int waitForConnection(int timeoutMillis) throws InterruptedIOException {
        return timeoutMillis;
    }

    /**
     * Returns the time at which a connection attempt was started, as milliseconds from 1970.
     * Returns MAX_VALUE if currently connected.
     */
    default long connectAttemptStartedAt() {
        return Long.MAX_VALUE;
    }

    /**
     * Called when an unknown operation was received.
     */
    void unknown(Channel from, int op);

    /**
     * @param term must be greater than the highest term
     * @param candidateId must not be zero
     * @return false if not sent or processed
     */
    boolean requestVote(Channel from, long term, long candidateId,
                        long highestTerm, long highestPosition);

    /**
     * @param term highest bit is set if vote was granted
     * @return false if not sent or processed
     */
    boolean requestVoteReply(Channel from, long term);

    /**
     * @return false if not sent or processed
     */
    boolean forceElection(Channel from);

    /**
     * Query for all the terms which are defined over the given range.
     *
     * @param startPosition inclusive log start position
     * @param endPosition exclusive log end position
     * @return false if not sent or processed
     */
    boolean queryTerms(Channel from, long startPosition, long endPosition);

    /**
     * One reply is sent for each defined term over the queried range.
     *
     * @param prevTerm previous term
     * @param term term number
     * @param startPosition position at start of term
     * @return false if not sent or processed
     */
    boolean queryTermsReply(Channel from, long prevTerm, long term, long startPosition);

    /**
     * Query for missing data over the given range.
     *
     * @param startPosition inclusive log start position
     * @param endPosition exclusive log end position
     * @return false if not sent or processed
     */
    boolean queryData(Channel from, long startPosition, long endPosition);

    /**
     * @param currentTerm current term of leader which replied; is 0 if data is committed
     * @param prevTerm expected term at previous position
     * @param term term at given position
     * @param position any position in the term to write to
     * @param off data offset
     * @param len data length
     * @return false if not sent or processed
     */
    boolean queryDataReply(Channel from, long currentTerm,
                           long prevTerm, long term, long position,
                           byte[] data, int off, int len);

    /**
     * @param currentTerm current term of leader which replied; is 0 if data is committed
     * @param prevTerm expected term at previous position
     * @param term term at given position
     * @param startPosition inclusive log start position
     * @param endPosition exclusive log end position
     * @return false if not sent or processed
     */
    boolean queryDataReplyMissing(Channel from, long currentTerm,
                                  long prevTerm, long term, long startPosition, long endPosition);

    /**
     * @param from the leader channel
     * @param prevTerm expected term at previous position
     * @param term term at given position
     * @param position any position in the term to write to
     * @param highestPosition highest position (exclusive) which can become the commit position
     * @param commitPosition current commit position (exclusive)
     * @param prefix optional data prefix
     * @param off data offset
     * @param len data length
     * @return false if not sent or processed
     */
    boolean writeData(Channel from, long prevTerm, long term, long position,
                      long highestPosition, long commitPosition,
                      byte[] prefix, byte[] data, int off, int len);

    /**
     * @return false if not sent or processed
     */
    boolean writeDataReply(Channel from, long term, long highestPosition);

    /**
     * Same as writeData except peer is expected to proxy the write to the remaining peers.
     *
     * @param from the leader channel
     * @param prefix optional data prefix
     */
    boolean writeDataAndProxy(Channel from, long prevTerm, long term, long position,
                              long highestPosition, long commitPosition,
                              byte[] prefix, byte[] data, int off, int len);

    /**
     * Same as writeData except this is called from a peer which acted as a proxy.
     *
     * @param from the proxy channel
     * @param prefix optional data prefix
     */
    boolean writeDataViaProxy(Channel from, long prevTerm, long term, long position,
                              long highestPosition, long commitPosition,
                              byte[] prefix, byte[] data, int off, int len);

    /**
     * @param prevTerm expected term at previous position
     * @param term term at given position
     * @param position minimum highest commit position to persist
     * @return false if not sent or processed
     */
    boolean syncCommit(Channel from, long prevTerm, long term, long position);

    /**
     * @param position highest sync'd position
     * @return false if not sent or processed
     */
    boolean syncCommitReply(Channel from, long groupVersion, long term, long position);

    /**
     * @param position lowest position which must be retained
     * @return false if not sent or processed
     */
    boolean compact(Channel from, long position);

    /**
     * @return false if not sent or processed
     */
    boolean snapshotScore(Channel from);

    /**
     * @param activeSessions count of active snapshot sessions; negative if rejected
     * @param weight -1 if follower, or 1 if leader (lower is preferred)
     * @return false if not sent or processed
     */
    boolean snapshotScoreReply(Channel from, int activeSessions, float weight);

    /**
     * Only the leader can update the member role.
     *
     * @return false if not sent or processed
     */
    boolean updateRole(Channel from, long groupVersion, long memberId, Role role);

    /**
     * @param result see ErrorCodes
     * @return false if not sent or processed
     */
    boolean updateRoleReply(Channel from, long groupVersion, long memberId, byte result);

    /**
     * Request the current group file version.
     *
     * @param groupVersion requester group version, which can be ignored
     * @return false if not sent or processed
     */
    boolean groupVersion(Channel from, long groupVersion);

    /**
     * @return false if not sent or processed
     */
    boolean groupVersionReply(Channel from, long groupVersion);

    /**
     * Request the current group file, if the version is higher.
     *
     * @param groupVersion requester group version, which can be ignored
     * @return false if not sent or processed
     */
    boolean groupFile(Channel from, long groupVersion) throws IOException;

    /**
     * Sender-side passes null for the InputStream and consumer is called to write into an
     * OutputStream. Receiver-side implementation receives an InputStream to read from and the
     * consumer isn't called.
     *
     * @param in pass to GroupFile.readFrom
     * @param consumer receives the stream to pass to GroupFile.writeTo
     * @return false if not sent or processed
     */
    boolean groupFileReply(Channel from, InputStream in, Consumer<OutputStream> consumer)
        throws IOException;

    /**
     * @return false if not sent or processed
     */
    boolean leaderCheck(Channel from);

    /**
     * @param term -1 if leader isn't validated
     * @return false if not sent or processed
     */
    boolean leaderCheckReply(Channel from, long term);
}
