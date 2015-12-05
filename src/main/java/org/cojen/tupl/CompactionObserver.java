/*
 *  Copyright 2013-2015 Cojen.org
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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
