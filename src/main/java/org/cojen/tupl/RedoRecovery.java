/*
 *  Copyright 2012 Brian S O'Neill
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
 * Single-use recovery workflow and RedoWriter factory.
 *
 * @author Brian S O'Neill
 */
interface RedoRecovery {
    /**
     * @param position position to start recovery from
     * @param undoLogs all active transactions during last checkpoint; all
     * committed and (explicitly) rolled back entries must be removed
     * @return highest transaction applied; zero if none
     */
    long recover(Database db, DatabaseConfig config,
                 long position, LHashTable.Obj<UndoLog> undoLogs)
        throws IOException;

    /**
     * Called after recovery, to obtain a writer for new transactions.
     */
    RedoWriter newWriter() throws IOException;

    /**
     * Called after recovery checkpoint, to perform additional cleanup.
     */
    void cleanup() throws IOException;
}
