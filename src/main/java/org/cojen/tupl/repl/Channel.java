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

/**
 * Defines a collection of remote asynchronous methods. Method invocations are permitted to
 * block if the send buffer is full.
 *
 * @author Brian S O'Neill
 */
interface Channel {
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
     * @return false if not sent or processed
     */
    boolean nop(Channel from);

    /**
     * @param term must be greater than the highest term
     * @param candidateId must not be zero
     * @return false if not sent or processed
     */
    boolean requestVote(Channel from, long term, long candidateId,
                        long highestTerm, long highestIndex);

    /**
     * @param term highest bit is set if vote was granted
     * @return false if not sent or processed
     */
    boolean requestVoteReply(Channel from, long term);

    /**
     * Query for all the terms which are defined over the given range.
     *
     * @param startIndex inclusive log start index
     * @param endIndex exclusive log end index
     * @return false if not sent or processed
     */
    boolean queryTerms(Channel from, long startIndex, long endIndex);

    /**
     * One reply is sent for each defined term over the queried range.
     *
     * @param prevTerm previous term
     * @param term term number
     * @param index index at start of term
     * @return false if not sent or processed
     */
    boolean queryTermsReply(Channel from, long prevTerm, long term, long startIndex);

    /**
     * Query for missing data over the given range.
     *
     * @param startIndex inclusive log start index
     * @param endIndex exclusive log end index
     * @return false if not sent or processed
     */
    boolean queryData(Channel from, long startIndex, long endIndex);

    /**
     * @param prevTerm expected term at previous index
     * @param term term at given index
     * @param index any index in the term to write to
     * @return false if not sent or processed
     */
    boolean queryDataReply(Channel from, long prevTerm, long term, long index, byte[] data);

    /**
     * @param prevTerm expected term at previous index
     * @param term term at given index
     * @param index any index in the term to write to
     * @param highestIndex highest index (exclusive) which can become the commit index
     * @param commitIndex current commit index (exclusive)
     * @return false if not sent or processed
     */
    boolean writeData(Channel from, long prevTerm, long term, long index,
                      long highestIndex, long commitIndex, byte[] data);

    /**
     * @return false if not sent or processed
     */
    boolean writeDataReply(Channel from, long term, long highestIndex);

    /**
     * @param prevTerm expected term at previous index
     * @param term term at given index
     * @param index minimum highest commit index to persist
     * @return false if not sent or processed
     */
    boolean syncCommit(Channel from, long prevTerm, long term, long index);

    /**
     * @param index highest sync'd index
     * @return false if not sent or processed
     */
    boolean syncCommitReply(Channel from, long groupVersion, long term, long index);

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
     * @param groupVersion requestor group version, which can be ignored
     * @return false if not sent or processed
     */
    boolean groupVersion(Channel from, long groupVersion);

    /**
     * @return false if not sent or processed
     */
    boolean groupVersionReply(Channel from, long groupVersion);
}
