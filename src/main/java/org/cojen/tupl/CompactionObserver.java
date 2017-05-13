/*
 *  Copyright (C) 2011-2017 Cojen.org
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

package org.cojen.tupl;

/**
 * Index compaction observer. Implementation does not need to be thread-safe, but instances
 * should not be shared by concurrent compactions.
 *
 * @author Brian S O'Neill
 * @see Database#compactFile Database.compactFile
 */
public class CompactionObserver {
    /** Index currently being compacted.  */
    protected Index index;

    /**
     * Called before full index compaction begins. Default implementation records the index,
     * and then simply returns true.
     *
     * @param index index to compact
     * @return false if compaction should stop
     */
    public boolean indexBegin(Index index) {
        this.index = index;
        return true;
    }

    /**
     * Called after index compaction has finished. Default implementation clears the index
     * field, and then returns true.
     *
     * @param index index which finished compaction
     * @return false if compaction should stop
     */
    public boolean indexComplete(Index index) {
        this.index = null;
        return true;
    }

    /**
     * Called after an index node has been visted by the compactor. Implementation is free to
     * report incremental progress or throttle compaction. Default implementation does nothing
     * but return true.
     *
     * @param id ephemeral node identifier
     * @return false if compaction should stop
     */
    public boolean indexNodeVisited(long id) {
        return true;
    }
}
