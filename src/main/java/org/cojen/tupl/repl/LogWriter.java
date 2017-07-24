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

import java.io.IOException;

/**
 * For writing data into the log. The inherited info fields only exist for convenience, and are
 * not used directly by the writer.
 *
 * @author Brian S O'Neill
 */
abstract class LogWriter extends LogInfo {
    /**
     * Returns the term at the previous writer index.
     */
    abstract long prevTerm();

    /**
     * Returns the fixed term being written to.
     */
    abstract long term();

    /**
     * Returns the next log index which will be written to.
     */
    abstract long index();

    /**
     * Write data to any index in the log, in no particular order. The write can be
     * silently rejected due data duplication. Implementation is also permitted to truncate
     * conflicting data, unless doing so would force the commit index to retreat.
     *
     * @param highestIndex highest index (exclusive) which can become the commit index
     * @return amount of bytes written, which is less than the given length only if the
     * term end has been reached
     */
    abstract int write(byte[] data, int offset, int length, long highestIndex)
        throws IOException;

    /**
     * Blocks until the commit index reaches the given index.
     *
     * @param nanosTimeout relative nanosecond time to wait; infinite if &lt;0
     * @return current commit index, or -1 if timed out or if term finished before the index
     * could be reached
     */
    abstract long waitForCommit(long index, long nanosTimeout) throws IOException;

    /**
     * Invokes the given task when the commit index reaches the requested index. The current
     * commit index is passed to the task, or -1 if the term ended before the index could be
     * reached. If the commit index is high enough when this method is called, then the current
     * thread invokes the task.
     */
    abstract void uponCommit(Delayed task);

    /**
     * Indicate that the writer isn't intended to be used again, allowing file handles to be
     * closed. Writing again will reopen them.
     */
    abstract void release();
}
