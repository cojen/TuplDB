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
 * 
 *
 * @author Brian S O'Neill
 */
public interface Writer extends Closeable {
    /*
     * Returns the fixed term being written to.
     */
    long term();

    /**
     * Returns the next log index which will be written to.
     */
    long index();

    /**
     * Write complete messages to the log. Implementation is permitted reject or truncate
     * conflicting messages, unless doing so would force the commit index to retreat.
     *
     * @return amount of bytes written, which is less than the message length only if the term
     * end has been reached
     */
    default int write(byte[] messages) throws IOException {
        return write(messages, 0, messages.length);
    }

    /**
     * Write complete messages to the log. Implementation is permitted reject or truncate
     * conflicting messages, unless doing so would force the commit index to retreat.
     *
     * @return amount of bytes written, which is less than the given length only if the
     * term end has been reached
     */
    default int write(byte[] messages, int offset, int length) throws IOException {
        return write(messages, offset, length, index() + length);
    }

    /**
     * Write complete or partial messages to the log. Implementation is permitted reject or
     * truncate conflicting messages, unless doing so would force the commit index to retreat.
     *
     * @param highestIndex highest index (exclusive) which can become the commit index
     * @return amount of bytes written, which is less than the given length only if the
     * term end has been reached
     */
    int write(byte[] messages, int offset, int length, long highestIndex) throws IOException;

    /**
     * Blocks until the commit index reaches the given index.
     *
     * @param nanosTimeout relative nanosecond time to wait; infinite if &lt;0
     * @return current commit index, or -1 if timed out or if term finished before the index
     * could be reached, or MIN_VALUE if closed
     */
    long waitForCommit(long index, long nanosTimeout) throws IOException;

    @Override
    void close();
}
