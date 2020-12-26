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
import java.io.InterruptedIOException;
import java.io.IOException;

/**
 * Log of data for a single term.
 *
 * @author Brian S O'Neill
 */
interface TermLog extends LKey<TermLog>, Closeable {
    @Override
    default long key() {
        return startPosition();
    }

    /**
     * Returns the previous term of this log, relative to the start position.
     */
    long prevTerm();

    /**
     * Returns the fixed term this log applies to.
     */
    long term();

    /**
     * Returns the position at the start of the term.
     */
    long startPosition();

    long prevTermAt(long position);

    /**
     * Attempt to increase the term start position, assumed to be a valid commit position, and
     * truncate as much data as possible lower than it. The effective start position applied might
     * be lower than what was requested, dependent on how much data could be truncated. As a
     * side-effect of calling this method, the previous term might be updated.
     *
     * @return true if compaction attempt was full and all segments were deleted
     */
    boolean compact(long startPosition) throws IOException;

    /**
     * Returns the potential position, which isn't appliable until the highest position reaches it.
     */
    long potentialCommitPosition();

    /**
     * @return true if the potential commit position is higher than the start position
     */
    default boolean hasPotentialCommit() {
        return hasPotentialCommit(startPosition());
    }

    /**
     * @return true if the potential commit position is higher than the given position
     */
    default boolean hasPotentialCommit(long position) {
        return potentialCommitPosition() > position;
    }

    /**
     * Returns the position at the end of the term (exclusive), which is Long.MAX_VALUE if
     * undefined.
     */
    long endPosition();

    /**
     * Copies into all relevant fields of the given info object, for the highest position. The
     * commit position provided is appliable.
     */
    void captureHighest(LogInfo info);

    /**
     * Returns true if the highest commit position is greater than or equal to the end position.
     */
    boolean isFinished();

    /**
     * Permit the commit position to advance. If the highest position (over a contiguous range)
     * is less than the given commit position, the actual commit position doesn't advance until
     * the highest position catches up.
     */
    void commit(long commitPosition);

    /**
     * Blocks until the commit position reaches the given position.
     *
     * @param nanosTimeout relative nanosecond time to wait; infinite if {@literal <0}
     * @return current commit position, or -1 if term finished before the position could be
     * reached, or -2 if timed out
     */
    long waitForCommit(long position, long nanosTimeout) throws InterruptedIOException;

    /**
     * Invokes the given task when the commit position reaches the requested position. The current
     * commit position is passed to the task, or -1 if the term ended before the position could be
     * reached. If the task can be run when this method is called, then the current thread
     * invokes it.
     */
    void uponCommit(CommitCallback task);

    /**
     * Attempt to rollback the commit position to be no lower than the appliable commit
     * position or the start position.
     */
    boolean tryRollbackCommit(long commitPosition);

    /**
     * Set the end position for this term instance, truncating all higher data. The highest
     * position will also be set, if the given position is within the contiguous region of
     * data.
     *
     * @throws IllegalStateException if the given position is lower than the commit position
     */
    void finishTerm(long endPosition);

    /**
     * Returns the highest position of contiguous data.
     */
    long contigPosition();

    /**
     * Check for missing data by examination of the contiguous range. Pass in the highest
     * contiguous position (exclusive), as returned by the previous invocation of this method, or
     * pass Long.MAX_VALUE if unknown. The given callback receives all the missing ranges, and
     * an updated contiguous position is returned. As long as the contiguous range is changing, no
     * missing ranges are reported.
     */
    long checkForMissingData(long contigPosition, PositionRange results);

    /**
     * Returns a new or existing writer which can write data to the log, starting from the
     * given position.
     *
     * @param position any position in the term
     */
    LogWriter openWriter(long position);

    /**
     * Returns a new or existing reader which accesses data starting from the given position. The
     * reader returns EOF whenever the end of this term is reached.
     */
    LogReader openReader(long position);

    /**
     * Returns true if committed data exists at the given position.
     */
    boolean isReadable(long position);

    /**
     * Durably persist all data up to the highest position.
     */
    void sync() throws IOException;
}
