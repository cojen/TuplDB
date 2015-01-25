/*
 *  Copyright 2012-2015 Brian S O'Neill
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
class ReplRedoWriter extends RedoWriter {
    final ReplRedoEngine mEngine;

    // Is non-null if writes are allowed.
    final ReplicationManager.Writer mReplWriter;

    // These fields capture the state of the last written commit.
    long mLastCommitPos;
    long mLastCommitTxnId;

    ReplRedoWriter(ReplRedoEngine engine, ReplicationManager.Writer writer) {
        super(4096, 0);
        mEngine = engine;
        mReplWriter = writer;
    }

    @Override
    public final long store(long indexId, byte[] key, byte[] value, DurabilityMode mode)
        throws IOException
    {
        // Pass SYNC mode to flush the buffer and obtain the commit position. Return value from
        // this method indicates if an actual sync should be performed.
        long pos = super.store(indexId, key, value, DurabilityMode.SYNC);
        // Replication makes no distinction between SYNC and NO_SYNC durability. The NO_REDO
        // mode is not expected to be passed into this method.
        return mode == DurabilityMode.NO_FLUSH ? 0 : pos;
    }

    @Override
    public final long storeNoLock(long indexId, byte[] key, byte[] value, DurabilityMode mode)
        throws IOException
    {
        // Ditto comments from above.
        long pos = super.storeNoLock(indexId, key, value, DurabilityMode.SYNC);
        return mode == DurabilityMode.NO_FLUSH ? 0 : pos;
    }

    @Override
    public final long dropIndex(long txnId, long indexId, DurabilityMode mode)
        throws IOException
    {
        // Ditto comments from above.
        long pos = super.dropIndex(txnId, indexId, DurabilityMode.SYNC);
        return mode == DurabilityMode.NO_FLUSH ? 0 : pos;
    }

    @Override
    public final long renameIndex(long txnId, long indexId, byte[] newName, DurabilityMode mode)
        throws IOException
    {
        // Ditto comments from above.
        long pos = super.renameIndex(txnId, indexId, newName, DurabilityMode.SYNC);
        return mode == DurabilityMode.NO_FLUSH ? 0 : pos;
    }

    @Override
    public final long deleteIndex(long txnId, long indexId, DurabilityMode mode)
        throws IOException
    {
        // Ditto comments from above.
        long pos = super.deleteIndex(txnId, indexId, DurabilityMode.SYNC);
        return mode == DurabilityMode.NO_FLUSH ? 0 : pos;
    }

    @Override
    public final long txnCommitFinal(long txnId, DurabilityMode mode) throws IOException {
        // Ditto comments from above.
        long pos = super.txnCommitFinal(txnId, DurabilityMode.SYNC);
        return mode == DurabilityMode.NO_FLUSH ? 0 : pos;
    }

    @Override
    public final void txnCommitSync(long commitPos) throws IOException {
        ReplicationManager.Writer writer = mReplWriter;
        if (writer == null) {
            throw new UnmodifiableReplicaException();
        }
        if (!writer.confirm(commitPos)) {
            throw unmodifiable();
        }
    }

    @Override
    public final synchronized void close(Throwable cause) throws IOException {
        super.close(cause);
        forceAndClose();
    }

    @Override
    public final long encoding() {
        return mEngine.mManager.encoding();
    }

    @Override
    final boolean isOpen() {
        // Returning false all the time prevents close and shutdown messages from being
        // written. They aren't very useful anyhow, considering that they don't prevent new log
        // messages from appearing afterwards.
        return false;
    }

    @Override
    public RedoWriter txnRedoWriter() {
        return this;
    }

    @Override
    boolean shouldCheckpoint(long sizeThreshold) {
        return false;
    }

    @Override
    void checkpointPrepare() throws IOException {
        throw fail();
    }

    @Override
    void checkpointSwitch() throws IOException {
        throw fail();
    }

    @Override
    long checkpointNumber() {
        throw fail();
    }

    @Override
    long checkpointPosition() {
        throw fail();
    }

    @Override
    long checkpointTransactionId() {
        throw fail();
    }

    @Override
    void checkpointAborted() {
    }

    @Override
    void checkpointStarted() throws IOException {
        throw fail();
    }

    @Override
    void checkpointFinished() throws IOException {
        throw fail();
    }

    @Override
    final void write(byte[] buffer, int len) throws IOException {
        // Length check is included because super class can invoke this method to flush the
        // buffer even when empty. Operation should never fail.
        if (len > 0) {
            ReplicationManager.Writer writer = mReplWriter;
            if (writer == null) {
                throw new UnmodifiableReplicaException();
            }
            if (writer.write(buffer, 0, len) < 0) {
                throw unmodifiable();
            }
        }
    }

    @Override
    final long writeCommit(byte[] buffer, int len) throws IOException {
        // Length check is included because super class can invoke this method to flush the
        // buffer even when empty. Operation should never fail.
        if (len > 0) {
            ReplicationManager.Writer writer = mReplWriter;
            if (writer == null) {
                throw new UnmodifiableReplicaException();
            }
            long pos = writer.write(buffer, 0, len);
            if (pos >= 0) {
                mLastCommitPos = pos;
                mLastCommitTxnId = lastTransactionId();
                return pos;
            } else {
                throw unmodifiable();
            }
        }
        return 0;
    }

    @Override
    final void force(boolean metadata) throws IOException {
        mEngine.mManager.sync();
    }

    @Override
    final void forceAndClose() throws IOException {
        IOException ex = null;
        try {
            force(false);
        } catch (IOException e) {
            ex = e;
        }
        try {
            mEngine.mManager.close();
        } catch (IOException e) {
            if (ex == null) {
                ex = e;
            }
        }
        if (ex != null) {
            throw ex;
        }
    }

    @Override
    final void writeTerminator() throws IOException {
        // No terminators.
    }

    private UnsupportedOperationException fail() {
        // ReplRedoEngineWriter subclass supports checkpoint operations.
        return new UnsupportedOperationException();
    }

    private UnmodifiableReplicaException unmodifiable() throws IOException {
        return mEngine.mController.unmodifiable(mReplWriter);
    }
}
