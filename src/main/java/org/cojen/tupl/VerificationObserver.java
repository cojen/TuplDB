/*
 *  Copyright 2012-2015 Cojen.org
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
 * Index verification observer. Implementation does not need to be thread-safe, but instances
 * should not be shared by concurrent verifications.
 *
 * @author Brian S O'Neill
 * @see Database#verify Database.verify
 * @see Index#verify Index.verify
 */
public class VerificationObserver {
    /** Index currently being verified.  */
    protected Index index;

    /** Index height; is zero for empty indexes. */
    protected int height;

    boolean failed;

    /**
     * Called before full index verification begins. Default implementation
     * records the index and height, and then returns true.
     *
     * @param index index being verified
     * @param height index height; is zero for empty indexes
     * @return false if verification should stop
     */
    public boolean indexBegin(Index index, int height) {
        this.index = index;
        this.height = height;
        return true;
    }

    /**
     * Called after index verification has finished. Default implementation
     * clears the index and height fields, and then returns true.
     *
     * @param index index which finished verification
     * @param passed true if index passed all verification
     * @param message optional message
     * @return false if verification should stop
     */
    public boolean indexComplete(Index index, boolean passed, String message) {
        this.index = null;
        this.height = 0;
        return true;
    }

    /**
     * Called after an index node passes verification. Implementation is free
     * to report incremental progress or throttle verification. Default
     * implementation does nothing but return true.
     *
     * @param id ephemeral node identifier
     * @param level index node level; root node is level one
     * @param entryCount total number of entries in the node
     * @param freeBytes amount of unused bytes in the node
     * @param largeValueCount number of values which don't fit entirely in the node
     * @return false if verification should stop
     */
    public boolean indexNodePassed(long id, int level,
                                   int entryCount, int freeBytes, int largeValueCount)
    {
        return true;
    }

    /**
     * Called after an index node fails verification. Implementation is free to
     * report incremental progress or throttle verification. Default
     * implementation prints a message to standard out and returns true.
     *
     * @param id ephemeral node identifier
     * @param level index node level; root node is level one
     * @param message failure message
     * @return false if verification should stop
     */
    public boolean indexNodeFailed(long id, int level, String message) {
        StringBuilder b = new StringBuilder("Verification failure: index=");

        Index index = this.index;
        if (index == null) {
            b.append("null");
        } else {
            b.append(index.getId());
        }

        b.append(", node=").append(id).append(", level=").append(level)
            .append(": ").append(message);

        reportFailure(b.toString());

        return true;
    }

    /**
     * Invoked by default implementation of {@link #indexNodeFailed indexNodeFailed}.
     */
    protected void reportFailure(String message) {
        System.out.println(message);
    }
}
