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
     * contiguous range (by highest index).
     */
    default LogInfo captureHighest() {
        LogInfo info = new LogInfo();
        captureHighest(info);
        return info;
    }

    /**
     * Copies into all relevant fields of the given info object, for the highest term over a
     * contiguous range (by highest index).
     */
    void captureHighest(LogInfo info);

    /**
     * Permit the commit index to advance. If the highest index (over a contiguous range)
     * is less than the given commit index, the actual commit index doesn't advance until
     * the highest index catches up.
     */
    void commit(long commitIndex);

    /**
     * Increment the current term by the amount given.
     *
     * @throws IllegalArgumentException if amount isn't greater than zero
     */
    long incrementCurrentTerm(int amount) throws IOException;

    /**
     * Compares the given term against the one given. If given a higher term, then the current
     * term is updated to match it.
     *
     * @return effective current term
     */
    long checkCurrentTerm(long term) throws IOException;

    /**
     * Set the log start index higher, assumed to be a valid commit index, and potentially
     * truncate any lower data. Method does nothing if given start index is lower than the
     * current start.
     *
     * @throws IllegalStateException if term is unknown at given index
     */
    void truncateStart(long index) throws IOException;

    /**
     * Ensures that a term is defined at the given index.
     *
     * @param prevTerm expected term at previous index
     * @param term term to define
     * @param index any index in the term
     * @return false if not defined due to term mismatch
     */
    boolean defineTerm(long prevTerm, long term, long index) throws IOException;

    /**
     * Return the term for the given index, or null if unknown.
     */
    TermLog termLogAt(long index);

    /**
     * Query for all the terms which are defined over the given range.
     *
     * @param startIndex inclusive log start index
     * @param endIndex exclusive log end index
     */
    void queryTerms(long startIndex, long endIndex, TermQuery results);

    /**
     * Check for missing data by examination of the contiguous range. Pass in the highest
     * contiguous index (exclusive), as returned by the previous invocation of this method, or
     * pass Long.MAX_VALUE if unknown. The given callback receives all the missing ranges, and
     * an updated contiguous index is returned. As long as the contiguous range is changing, no
     * missing ranges are reported.
     */
    long checkForMissingData(long contigIndex, IndexRange results);

    /**
     * Returns a new or existing writer which can write data to the log, starting from the
     * given index.
     *
     * @param prevTerm expected term at previous index; pass 0 to not check
     * @param term existing or higher term to apply
     * @param index any index in the term
     * @return null due to term mismatch
     */
    LogWriter openWriter(long prevTerm, long term, long index) throws IOException;

    /**
     * Returns a new or existing reader which accesses data starting from the given index. The
     * reader returns EOF whenever the end of a term is reached.
     *
     * @return reader or null if timed out
     * @throws IllegalStateException if index is lower than the start index
     */
    LogReader openReader(long index);

    /**
     * Durably persist all data up to the highest index. The highest term, the highest index,
     * and the durable commit index are all recovered when reopening the state log. Incomplete
     * data beyond this is discarded.
     */
    void sync() throws IOException;

    /**
     * Durably persist all data up to the given index, unless the term doesn't match. If the
     * given index is lower than the highest index already known to be persisted, then the log
     * data doesn't need to be sync'd at all. If the given durable index is higher than what's
     * known, the metadata file must be updated regardless.
     *
     * @param index minimum highest committed index to become durable
     * @param durableIndex highest index (exclusive) which is durable; pass zero if unknown
     * @return current durable index, or -1 due to term mismatch
     */
    long syncCommit(long prevTerm, long term, long index, long durableIndex) throws IOException;
}
