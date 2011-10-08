/*
 *  Copyright 2011 Brian S O'Neill
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

import java.io.IOException;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public interface UndoLog {
    /**
     * Add an entry to the end of the log. Entries are seen again when this log
     * instance is explicitly rolled back, and they can be seen again when the
     * database is re-opened. For this reason, entries must encode enough
     * information to be applicable outside the context of their original log
     * instance.
     *
     * @param entry non-null log entry data
     * @throws IllegalArgumentException if entry is null
     * @throws IllegalStateException if log is closed
     */
    public void add(byte[] entry) throws IOException;

    /**
     * @param entry non-null log entry data
     * @param offset offset to start of entry data
     * @param length length of entry, from offset
     * @throws IllegalArgumentException if entry is null, or if offset/length is invalid
     * @throws IllegalStateException if log is closed
     */
    public void add(byte[] entry, int offset, int length) throws IOException;

    /**
     * Closes this log instance, discarding all entries.
     */
    public void commit() throws IOException;

    /**
     * Closes this log instance and passes all log entries to the database
     * rollback handler, in reverse order.
     *
     * @throws IllegalStateException if log is closed
     */
    public void rollback();

    public static interface RollbackHandler {
        /**
         * Interpret the log entry and logically undo the encoded operation.
         * Undo operations must be idempotent, because they might be seen again
         * when the database is re-opened.
         *
         * @param entry non-null log entry data
         * @param offset offset to start of entry data
         * @param length length of entry, from offset
         */
        public void undo(Database db, byte[] entry, int offset, int length);
    }
}
