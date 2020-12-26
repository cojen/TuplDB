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
import java.io.IOException;

/**
 * Full state log for data over multiple terms.
 *
 * @author Brian S O'Neill
 */
interface StateLog extends Closeable {
    /**
     * Copies into all relevant fields of the returned info object, for the highest term over a
     * contiguous range (by highest position).
     */
    default LogInfo captureHighest() {
        var info = new LogInfo();
        captureHighest(info);
        return info;
    }

    /**
     * Copies into all relevant fields of the given info object, for the highest term over a
     * contiguous range (by highest position). The commit position provided is appliable.
     *
     * @return the highest term, or null if none
     */
    TermLog captureHighest(LogInfo info);

    /**
     * Permit the commit position to advance. If the highest position (over a contiguous range)
     * is less than the given commit position, the actual commit position doesn't advance until
     * the highest position catches up.
     */
    void commit(long commitPosition);

    /**
     * Returns the highest observed commit position overall. This commit position might be
     * higher than what can be currently applied, if gaps exist in the log.
     */
    long potentialCommitPosition();

    /**
     * Increment the current term by the amount given.
     *
     * @param candidateId local member id, voting for itself
     * @throws IllegalArgumentException if amount isn't greater than zero
     */
    long incrementCurrentTerm(int amount, long candidateId) throws IOException;

    /**
     * Compares the given term against the one given. If given a higher term, then the current
     * term is updated to match it and the voted-for field is cleared.
     *
     * @return effective current term
     */
    long checkCurrentTerm(long term) throws IOException;

    /**
     * Returns true if the given candidate matches the current voted-for candidate, or if the
     * current voted-for candidate is zero. Returning true also implies that the given
     * candidate is now the current voted-for candidate.
     */
    boolean checkCandidate(long candidateId) throws IOException;

    /**
     * Set the log start position higher, assumed to be a valid commit position, and potentially
     * truncate any lower data. Method does nothing if given start position is lower than the
     * current start.
     */
    void compact(long position) throws IOException;

    /**
     * Truncate the entire log, and create a primordial term.
     */
    void truncateAll(long prevTerm, long term, long position) throws IOException;

    /**
     * Ensures that a term is defined at the given position.
     *
     * @param prevTerm expected term at previous position
     * @param term term to define
     * @param position any position in the term
     * @return false if not defined due to term mismatch
     */
    boolean defineTerm(long prevTerm, long term, long position) throws IOException;

    /**
     * Return the term for the given position, or null if unknown.
     */
    TermLog termLogAt(long position);

    /**
     * Query for all the terms which are defined over the given range.
     *
     * @param startPosition inclusive log start position
     * @param endPosition exclusive log end position
     */
    void queryTerms(long startPosition, long endPosition, TermQuery results);

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
     * @param prevTerm expected term at previous position; pass 0 to not check
     * @param term existing or higher term to apply
     * @param position any position in the term
     * @return null due to term mismatch
     */
    LogWriter openWriter(long prevTerm, long term, long position) throws IOException;

    /**
     * Returns a new or existing reader which accesses data starting from the given position. The
     * reader returns EOF whenever the end of a term is reached.
     *
     * @return reader or null if timed out
     * @throws InvalidReadException if position is lower than the start position
     * @throws IllegalStateException if replicator is closed
     */
    LogReader openReader(long position);

    /**
     * Returns true if committed data exists at the given position.
     *
     * @throws IllegalStateException if replicator is closed
     */
    boolean isReadable(long position);

    /**
     * Durably persist all data up to the highest position. The highest term, the highest position,
     * and the durable commit position are all recovered when reopening the state log. Incomplete
     * data beyond this is discarded.
     */
    void sync() throws IOException;

    /**
     * Durably persist all data up to the given committed position. The highest term, the highest
     * position, and the durable commit position are all recovered when reopening the state log.
     * Incomplete data beyond this is discarded.
     *
     * <p>If the given position is greater than the highest position, then no sync is actually
     * performed and -1 is returned. This case occurs when this method is called by a remote
     * member which is at a higher position in the log.
     *
     * @return current commit position, or -1 due to term mismatch or if position is too high
     */
    long syncCommit(long prevTerm, long term, long position) throws IOException;

    /**
     * @return true if given position is less than or equal to current durable commit position
     */
    boolean isDurable(long position);

    /**
     * Durably persist the commit position, as agreed by consensus. Only metadata is persisted by
     * this method -- sync or syncCommit must have been called earlier, ensuring that all data
     * up to the given position is durable.
     *
     * @return false if given position is already durable
     * @throws IllegalStateException if position is too high
     */
    boolean commitDurable(long position) throws IOException;
}
