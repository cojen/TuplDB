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
 */
class ReplRedoLog extends RedoWriter {
    private final ReplicationManager mReplManager;
    private final ReplRedoReceiver mReceiver;

    private ReplicationManager.Output mOut;
    private long mPosition;

    private long mCheckpointPos;
    private long mCheckpointTxnId;

    ReplRedoLog(ReplicationManager rm, long position, ReplRedoReceiver receiver)
        throws IOException
    {
        super(4096, 0);

        mReplManager = rm;
        mReceiver = receiver;

        leaderNotify(position);
    }

    @Override
    public void store(long indexId, byte[] key, byte[] value, DurabilityMode mode)
        throws IOException
    {
        super.store(indexId, key, value, DurabilityMode.NO_SYNC);
        // FIXME: mode stuff; wait for confirmation unless NO_FLUSH
    }

    @Override
    public void storeNoLock(long indexId, byte[] key, byte[] value, DurabilityMode mode)
        throws IOException
    {
        super.storeNoLock(indexId, key, value, DurabilityMode.NO_SYNC);
        // FIXME: mode stuff; wait for confirmation unless NO_FLUSH
    }

    @Override
    public boolean txnCommitFinal(long txnId, DurabilityMode mode) throws IOException {
        super.txnCommitFinal(txnId, DurabilityMode.NO_SYNC);
        // FIXME: mode stuff; wait for confirmation unless NO_FLUSH
        return false;
    }

    @Override
    public void txnCommitSync() throws IOException {
        // FIXME
        super.txnCommitSync();
    }

    @Override
    boolean isOpen() {
        // FIXME
        return true;
    }

    @Override
    boolean shouldCheckpoint(long sizeThreshold) {
        // FIXME
        return true;
    }

    @Override
    void prepareCheckpoint() throws IOException {
        // Nothing to prepare.
    }

    @Override
    synchronized void captureCheckpointState() {
        mCheckpointPos = mPosition;
        mCheckpointTxnId = lastTransactionId();
    }

    @Override
    long checkpointPosition() {
        return mCheckpointPos;
    }

    @Override
    long checkpointTransactionId() {
        return mCheckpointTxnId;
    }

    @Override
    void checkpointed(long position) throws IOException {
        // FIXME
        System.out.println("checkpointed: " + position);
    }

    @Override
    void write(byte[] buffer, int len) throws IOException {
        if (!mOut.write(buffer, 0, len)) {
            throw new UnmodifiableReplicaException();
        }
    }

    @Override
    void force(boolean metadata) throws IOException {
        long position;
        synchronized (this) {
            position = mPosition;
        }
        if (!mOut.sync(position)) {
            throw new UnmodifiableReplicaException();
        }
    }

    @Override
    void forceAndClose() throws IOException {
        force(false);
        // FIXME: Close stuff..
    }

    @Override
    void writeTerminator() throws IOException {
        // No terminators.
    }

    /**
     * Called by ReplRedoReceiver.
     */
    synchronized void leaderNotify(long position) {
        mPosition = position;

        ReplicationManager.Output out;
        try {
            out = mReplManager.out(position);
        } catch (IOException e) {
            // FIXME: log it?
            e.printStackTrace(System.out);
            out = null;
        }
        
        if (out != null) {
            mOut = out;
            try {
                clearAndReset();
            } catch (IOException e) {
                // FIXME: log it?
                e.printStackTrace(System.out);
                mOut = null;
            }
        } else {
            mOut = NonOutput.THE;

            try {
                final ReplicationManager.Input in = mReplManager.in(mPosition);

                if (in != null) {
                    // FIXME: initialTxnId
                    RedoDecoder decoder = new RedoDecoder(new DataIn(in), false, 0);
                    mReceiver.setDecoder(decoder, this);
                } else {
                    // FIXME: What if no in and no out?
                }
            } catch (IOException e) {
                // FIXME: now what?
                e.printStackTrace(System.out);
            }
        }
    }

    static final class NonOutput implements ReplicationManager.Output {
        static final NonOutput THE = new NonOutput();

        private NonOutput() {
        }

        @Override
        public boolean write(byte[] b, int off, int len) {
            return false;
        }

        @Override
        public boolean sync(long position) {
            return false;
        }

        @Override
        public boolean confirm(long position, long timeoutNanos) {
            return false;
        }
    }
}
