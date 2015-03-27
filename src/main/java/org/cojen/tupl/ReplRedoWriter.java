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

import org.cojen.tupl.ext.ReplicationManager;

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

    // These fields capture the state of the highest confirmed commit.
    long mConfirmedPos;
    long mConfirmedTxnId;

    private volatile PendingTxnWaiter mPendingWaiter;

    ReplRedoWriter(ReplRedoEngine engine, ReplicationManager.Writer writer) {
        super(4096, 0);
        mEngine = engine;
        mReplWriter = writer;
    }

    // All inherited methods which accept a DurabilityMode must be overridden and always use
    // SYNC mode. This ensures that writeCommit is called, to capture the log position. If
    // Transaction.commit sees that DurabilityMode wasn't actually SYNC, it prepares a
    // PendingTxn instead of immediately calling txnCommitSync. Replication makes no
    // distinction between NO_FLUSH and NO_SYNC mode.

    @Override
    public final long store(long indexId, byte[] key, byte[] value, DurabilityMode mode)
        throws IOException
    {
        // Note: This method can only have been called when using an auto-commit transaction,
        // but this is prohibited by TxnTree. It creates explict transactions if necessary,
        // ensuring that all store operations can roll back.
        return super.store(indexId, key, value, DurabilityMode.SYNC);
    }

    @Override
    public final long storeNoLock(long indexId, byte[] key, byte[] value, DurabilityMode mode)
        throws IOException
    {
        // Note: This method can only be have been called when using a transaction which uses
        // the unsafe locking mode and also supports redo durability. The store cannot roll
        // back if leadership is lost, resulting in an inconsistency. Unsafe is what it is.
        return super.storeNoLock(indexId, key, value, DurabilityMode.SYNC);
    }

    @Override
    public final long dropIndex(long txnId, long indexId, DurabilityMode mode)
        throws IOException
    {
        return super.dropIndex(txnId, indexId, DurabilityMode.SYNC);
    }

    @Override
    public final long renameIndex(long txnId, long indexId, byte[] newName, DurabilityMode mode)
        throws IOException
    {
        return super.renameIndex(txnId, indexId, newName, DurabilityMode.SYNC);
    }

    @Override
    public final long deleteIndex(long txnId, long indexId, DurabilityMode mode)
        throws IOException
    {
        return super.deleteIndex(txnId, indexId, DurabilityMode.SYNC);
    }

    @Override
    public final long txnCommitFinal(long txnId, DurabilityMode mode) throws IOException {
        return super.txnCommitFinal(txnId, DurabilityMode.SYNC);
    }

    @Override
    public final void txnCommitSync(Transaction txn, long commitPos) throws IOException {
        ReplicationManager.Writer writer = mReplWriter;
        if (writer == null) {
            throw new UnmodifiableReplicaException();
        }

        if (writer.confirm(commitPos)) {
            synchronized (this) {
                if (commitPos > mConfirmedPos) {
                    mConfirmedPos = commitPos;
                    mConfirmedTxnId = txn.txnId();
                }
            }
            return;
        }

        synchronized (this) {
            if (mConfirmedPos >= commitPos) {
                // Was already was confirmed.
                return;
            }
        }

        throw unmodifiable();
    }

    @Override
    public final void txnCommitPending(PendingTxn pending) throws IOException {
        PendingTxnWaiter waiter = mPendingWaiter;
        int action;
        if (waiter == null || (action = waiter.add(pending)) == PendingTxnWaiter.EXITED) {
            synchronized (this) {
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
            }
        }

        if (action != PendingTxnWaiter.PENDING) {
            Database db = mEngine.mDatabase;
            if (action == PendingTxnWaiter.DO_COMMIT) {
                pending.commit(db);
            } else if (action == PendingTxnWaiter.DO_ROLLBACK) {
                pending.rollback(db);
            }
        }
    }

    protected final void flipped(long commitPos) {
        PendingTxnWaiter waiter;
        synchronized (this) {
            waiter = mPendingWaiter;
            if (waiter == null) {
                waiter = new PendingTxnWaiter(this);
                mPendingWaiter = waiter;
                // Don't start it.
            }
            waiter.flipped(commitPos);
        }

        waiter.finishAll();
    }

    /**
     * Block waiting for the given committed position to be confirmed. Returns false if not the
     * leader.
     */
    final boolean confirm(long txnId, long commitPos) {
        // Note: Similar to txnCommitSync.

        ReplicationManager.Writer writer = mReplWriter;
        if (writer == null) {
            return false;
        }

        try {
            if (writer.confirm(commitPos)) {
                synchronized (this) {
                    if (commitPos > mConfirmedPos) {
                        mConfirmedPos = commitPos;
                        mConfirmedTxnId = txnId;
                    }
                }
                return true;
            }
        } catch (IOException e) {
            // Treat as leader switch.
        }

        synchronized (this) {
            if (mConfirmedPos >= commitPos) {
                // Was already was confirmed.
                return true;
            }
        }

        return false;
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

    private UnmodifiableReplicaException unmodifiable() {
        return mEngine.mController.unmodifiable(mReplWriter);
    }
}
