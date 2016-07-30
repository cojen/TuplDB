/*
 *  Copyright 2016 Cojen.org
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

import org.cojen.tupl.io.CauseCloseable;

/**
 * Defines hidden internal methods for cursor implementations.
 *
 * @author Brian S O'Neill
 */
// FIXME: remove this interface
abstract class AbstractCursor implements CauseCloseable, Cursor {
    /**
     * Position the cursor for append operations. Tree must be empty.
     */
    abstract void appendInit() throws IOException;

    /**
     * Non-transactionally insert an entry as the highest overall, for filling up a new tree
     * with ordered entries. No other cursors can be active in the tree. No check is performed
     * to verify that the entry is the highest and unique. Cursor key and value reference are
     * untouched.
     */
    // FIXME: remove this
    abstract void appendEntry(byte[] key, byte[] value) throws IOException;

    /**
     * Non-transactionally moves the entry from the given cursor, as the highest overall. No
     * other cursors can be active in the target tree. No check is performed to verify that the
     * entry is the highest and unique. The source cursor is positioned at the next entry as a
     * side effect. Nodes from the source are deleted only when empty.
     */
    abstract void appendTransfer(AbstractCursor source) throws IOException;
}
