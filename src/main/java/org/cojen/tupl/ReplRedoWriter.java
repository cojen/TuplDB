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

import java.io.IOException;

/**
 * 
 *
 * @author Brian S O'Neill
 * @see ReplRedoEngine
 */
final class ReplRedoWriter extends RedoWriter {
    private final ReplRedoEngine mEngine;

    private ReplicationManager.Output mOut;

    private long mCheckpointPos;
    private long mCheckpointTxnId;

    private boolean mIsLeader;

    ReplRedoWriter(ReplRedoEngine engine, ReplicationManager.Output out) {
        super(4096, 0);
        mEngine = engine;
        synchronized (this) {
            // Initially unmodifiable. Requires notification from engine.
            mOut = NonOut.THE;
        }
    }

    @Override
    public void store(long indexId, byte[] key, byte[] value, DurabilityMode mode)
        throws IOException
    {
        long pos;
        synchronized (this) {
            super.store(indexId, key, value, DurabilityMode.NO_SYNC);
            pos = mOut.commit();
        }
        if (pos < 0) {
            throw unmodifiable();
        }
        // FIXME: mode stuff; wait for confirmation unless NO_FLUSH
    }

    @Override
    public void storeNoLock(long indexId, byte[] key, byte[] value, DurabilityMode mode)
        throws IOException
    {
        long pos;
        synchronized (this) {
            super.storeNoLock(indexId, key, value, DurabilityMode.NO_SYNC);
            pos = mOut.commit();
        }
        if (pos < 0) {
            throw unmodifiable();
        }
        // FIXME: mode stuff; wait for confirmation unless NO_FLUSH
    }

    @Override
    public boolean txnCommitFinal(long txnId, DurabilityMode mode) throws IOException {
        long pos;
        synchronized (this) {
            super.txnCommitFinal(txnId, DurabilityMode.NO_SYNC);
            pos = mOut.commit();
        }
        if (pos < 0) {
            throw unmodifiable();
        }
        // FIXME: mode stuff; wait for confirmation unless NO_FLUSH
        return false;
    }

    @Override
    public void txnCommitSync() throws IOException {
        // FIXME
        super.txnCommitSync();
    }

    @Override
    public synchronized void close(Throwable cause) throws IOException {
        super.close(cause);
        forceAndClose();
    }

    @Override
    boolean isOpen() {
        // Returning false all the time prevents close and shutdown messages from being
        // written. They aren't very useful anyhow, considering that they don't prevent new log
        // messages from appearing afterwards.
        return false;
    }

    @Override
    synchronized boolean shouldCheckpoint(long sizeThreshold) {
        long pos;
        if (mIsLeader) {
            pos = mEngine.mManager.position();
        } else {
            pos = mEngine.mDecodePosition;
        }
        return (pos - mCheckpointPos) >= sizeThreshold;
    }

    @Override
    void checkpointPrepare() throws IOException {
        // Suspend before commit lock is acquired, preventing deadlock.
        mEngine.suspend();
    }

    @Override
    synchronized void checkpointSwitch() {
        if (mIsLeader) {
            mCheckpointPos = mEngine.mManager.position();
            mCheckpointTxnId = lastTransactionId();
        } else {
            mCheckpointPos = mEngine.mDecodePosition;
            mCheckpointTxnId = mEngine.mDecodeTransactionId;
        }
    }

    @Override
    long checkpointPosition() {
        System.out.println("checkpointPosition: " + mCheckpointPos);
        return mCheckpointPos;
    }

    @Override
    long checkpointTransactionId() {
        System.out.println("checkpointTransactionId: " + mCheckpointTxnId);
        return mCheckpointTxnId;
    }

    @Override
    void checkpointStarted() throws IOException {
        mEngine.resume();
    }

    @Override
    void checkpointFinished() throws IOException {
        // Nothing to do.
    }

    @Override
    void write(byte[] buffer, int len) throws IOException {
        if (!mOut.write(buffer, 0, len)) {
            throw unmodifiable();
        }
    }

    @Override
    void force(boolean metadata) throws IOException {
        // FIXME: If not leader, don't throw any exception from this method.
        /* FIXME
        long position;
        synchronized (this) {
            position = mPosition;
        }
        if (!mOut.sync(position)) {
            throw unmodifiable();
        }
        */
    }

    @Override
    void forceAndClose() throws IOException {
        // FIXME: If not leader, don't throw any exception from this method.
        force(false);
        // FIXME: Close stuff..
    }

    @Override
    void writeTerminator() throws IOException {
        // No terminators.
    }

    /**
     * Called by ReplRedoEngine when local instance has become the leader.
     */
    // FIXME: need an up-to-date txn id passed in
    synchronized void leaderNotify(ReplicationManager.Output out)
        throws UnmodifiableReplicaException, IOException
    {
        if (out != mOut) {
            mIsLeader = true;
            mOut = out;
            // FIXME: provide txn id to clearAndReset
            clearAndReset();
            flush();
        }
    }

    private synchronized UnmodifiableReplicaException unmodifiable() {
        if (mIsLeader) {
            mIsLeader = false;

            // Invoke from a separate thread, avoiding deadlock during the transition.
            new Thread() {
                public void run() {
                    // Start receiving if not, but does nothing if already receiving. A reset
                    // op is expected, and so the initial transaction id can be zero.
                    mEngine.startReceiving(0);
                }
            }.start();
        }

        return new UnmodifiableReplicaException();
    }

    static final class NonOut implements ReplicationManager.Output {
        static final NonOut THE = new NonOut();

        private NonOut() {}

        @Override
        public boolean write(byte[] b, int off, int len) {
            return false;
        }

        @Override
        public long commit() throws IOException {
            return -1;
        }

        @Override
        public boolean confirm(long position, long timeoutNanos) {
            return false;
        }

        @Override
        public boolean sync(long position) {
            return false;
        }
    }
}
