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

import java.io.IOException;

import org.cojen.tupl.ext.ReplicationManager;

/**
 * 
 *
 * @author Brian S O'Neill
 */
/*P*/
class ReplRedoWriter extends RedoWriter {
    final ReplRedoEngine mEngine;

    // Is non-null if writes are allowed.
    final ReplicationManager.Writer mReplWriter;

    // These fields capture the state of the last written commit, but not yet confirmed.
    long mLastCommitPos;
    long mLastCommitTxnId;

    private volatile PendingTxnWaiter mPendingWaiter;

    ReplRedoWriter(ReplRedoEngine engine, ReplicationManager.Writer writer) {
        mEngine = engine;
        mReplWriter = writer;
    }

    @Override
    public final void txnCommitSync(LocalTransaction txn, long commitPos) throws IOException {
        ReplicationManager.Writer writer = mReplWriter;
        if (writer == null) {
            throw mEngine.unmodifiable();
        }
        if (writer.confirm(commitPos)) {
            txn.mContext.confirmed(commitPos, txn.txnId());
        } else {
            throw nowUnmodifiable();
        }
    }

    @Override
    public final void txnCommitPending(PendingTxn pending) throws IOException {
        PendingTxnWaiter waiter = mPendingWaiter;
        int action;
        if (waiter == null || (action = waiter.add(pending)) == PendingTxnWaiter.EXITED) {
            acquireExclusive();
            try {
                waiter = mPendingWaiter;
                if (waiter == null || (action = waiter.add(pending)) == PendingTxnWaiter.EXITED) {
                    waiter = new PendingTxnWaiter(this);
                    mPendingWaiter = waiter;
                    action = waiter.add(pending);
                    if (action == PendingTxnWaiter.PENDING) {
                        waiter.setName("PendingTxnWaiter-" + waiter.getId());
                        waiter.setDaemon(true);
                        waiter.start();
                    }
                }
            } finally {
                releaseExclusive();
            }
        }

        if (action != PendingTxnWaiter.PENDING) {
            LocalDatabase db = mEngine.mDatabase;
            if (action == PendingTxnWaiter.DO_COMMIT) {
                pending.commit(db);
            } else if (action == PendingTxnWaiter.DO_ROLLBACK) {
                pending.rollback(db);
            }
        }
    }

    protected final void flipped(long commitPos) {
        PendingTxnWaiter waiter;
        acquireExclusive();
        try {
            waiter = mPendingWaiter;
            if (waiter == null) {
                waiter = new PendingTxnWaiter(this);
                mPendingWaiter = waiter;
                // Don't start it.
            }
            waiter.flipped(commitPos);
        } finally {
            releaseExclusive();
        }

        waiter.finishAll();
    }

    /**
     * Block waiting for the given committed position to be confirmed. Returns false if not the
     * leader.
     */
    final boolean confirm(PendingTxn pending) {
        // Note: Similar to txnCommitSync.

        ReplicationManager.Writer writer = mReplWriter;
        if (writer == null) {
            return false;
        }

        long commitPos = pending.mCommitPos;

        try {
            if (writer.confirm(commitPos)) {
                pending.mContext.confirmed(commitPos, pending.mTxnId);
                return true;
            }
        } catch (IOException e) {
            // Treat as leader switch.
        }

        mEngine.mController.switchToReplica(mReplWriter, false);

        return false;
    }

    @Override
    public final long encoding() {
        return mEngine.mManager.encoding();
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
    void checkpointSwitch(TransactionContext[] contexts) throws IOException {
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
    void checkpointFlushed() throws IOException {
        throw fail();
    }

    @Override
    void checkpointFinished() throws IOException {
        throw fail();
    }

    @Override
    DurabilityMode opWriteCheck(DurabilityMode mode) throws IOException {
        // All redo methods which accept a DurabilityMode must always use SYNC mode. This
        // ensures that write commit option is true, for capturing the log position. If
        // Transaction.commit sees that DurabilityMode wasn't actually SYNC, it prepares a
        // PendingTxn instead of immediately calling txnCommitSync. Replication makes no
        // distinction between NO_FLUSH and NO_SYNC mode.
        return DurabilityMode.SYNC;
    }

    @Override
    boolean shouldWriteTerminators() {
        return false;
    }

    @Override
    final long write(byte[] bytes, int offset, int length, int commit) throws IOException {
        ReplicationManager.Writer writer = mReplWriter;
        if (writer == null) {
            throw mEngine.unmodifiable();
        }

        long pos = writer.write(bytes, offset, length, commit);

        if (pos < 0) {
            throw nowUnmodifiable();
        }

        if (commit >= 0) {
            mLastCommitPos = pos - offset + commit;
            mLastCommitTxnId = mLastTxnId;
        }

        return pos;
    }

    @Override
    void alwaysFlush(boolean enable) throws IOException {
        // Always flushes already.
    }

    @Override
    void force(boolean metadata) throws IOException {
        mEngine.mManager.sync();
    }

    @Override
    public void close() throws IOException {
        mEngine.mManager.close();
    }

    private UnsupportedOperationException fail() {
        // ReplRedoController subclass supports checkpoint operations.
        return new UnsupportedOperationException();
    }

    private UnmodifiableReplicaException nowUnmodifiable() throws DatabaseException {
        return mEngine.mController.nowUnmodifiable(mReplWriter);
    }
}
