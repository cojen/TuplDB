/*
 *  Copyright 2012-2013 Brian S O'Neill
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

import java.io.File;
import java.io.IOException;

import java.util.Set;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class RedoLogRecovery implements RedoRecovery {
    private DatabaseConfig mConfig;
    private Set<File> mRedoFiles;
    private long mHighestTxnId;
    private long mNewLogId;

    @Override
    public boolean recover(Database db, DatabaseConfig config,
                           long logId, long txnId,
                           LHashTable.Obj<Transaction> txns)
        throws IOException
    {
        mConfig = config;

        // Make sure old redo logs are deleted. Process might have exited
        // before last checkpoint could delete them.
        for (int i=1; i<=2; i++) {
            RedoLog.deleteOldFile(config.mBaseFile, logId - i);
        }

        RedoLogApplier applier = new RedoLogApplier(db, txns);
        RedoLog redoLog = new RedoLog(config, logId, true);

        // As a side-effect, log id is set one higher than last file observed.
        mRedoFiles = redoLog.replay
            (applier, config.mEventListener, EventType.RECOVERY_APPLY_REDO_LOG,
             "Applying redo log: %1$d");

        mHighestTxnId = applier.mHighestTxnId;

        // Always set to one higher than the last file observed.
        mNewLogId = redoLog.currentLogId();

        return !mRedoFiles.isEmpty();
    }

    @Override
    public long highestTxnId() {
        return mHighestTxnId;
    }

    @Override
    public RedoWriter newWriter() throws IOException {
        // New redo logs begin with identifiers one higher than last scanned.
        return new RedoLog(mConfig, mNewLogId, false);
    }

    @Override
    public void cleanup() throws IOException {
        for (File file : mRedoFiles) {
            file.delete();
        }
    }
}
