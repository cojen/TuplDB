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
     * Copies into all relevant fields of the given info object, for the highest term over a
     * contiguous range (by highest index).
     */
    void captureHighest(LogInfo info);

    /**
     * Permit the commit index to advance. If the highest index (over a contiguous range)
     * is less than the given commit index, the actual commit index doesn't advance until
     * the highest index catches up.
     */
    // FIXME: Provide some sort of "token" object which is replaced when membership changes. When
    // this happens, caller must get the current token and re-evaluate the commit decision.
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
     * Ensures that a term is defined at the given index.
     *
     * @param prevTerm expected term at previous index; pass 0 to not check
     * @param term term to define
     * @param index any index in the term
     * @return false if not defined due to term mismatch
     */
    boolean defineTerm(long prevTerm, long term, long index) throws IOException;

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
     * Durably persist all data up to the highest index. When recovering the state log, the
     * highest term, the highest index, and the commit index are all recovered. Incomplete data
     * beyond this is discarded.
     */
    void sync() throws IOException;
}
