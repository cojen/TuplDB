/*
 *  Copyright 2013 Brian S O'Neill
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
final class ReplRedoDecoder extends RedoDecoder {
    private final ReplRedoEngine mEngine;
    private final In mIn;

    ReplRedoDecoder(ReplRedoEngine engine, long initialTxnId) {
        super(false, initialTxnId);

        mEngine = engine;
        mIn = new In(engine.mManager);

        Latch opLatch = engine.mOpLatch;
        opLatch.acquireExclusive();
        mEngine.mDecodeTransactionId = initialTxnId;
        mEngine.mDecodePosition = mIn.mPos;
        opLatch.releaseExclusive();
    }

    @Override
    long readTxnId(DataIn in) throws IOException {
        // Capture the last transaction id, before a delta is applied.
        // See "in()" comments below regarding updates to this field.
        long txnId = mTxnId;
        mEngine.mDecodeTransactionId = txnId;

        txnId += in.readSignedVarLong();
        mTxnId = txnId;
        return txnId;
    }

    @Override
    DataIn in() {
        In in = mIn;

        // Capture the position for the next operation. No conflict exists when assigning the
        // value because the decode latch should be held exclusively. ReplRedoEngine must
        // acquire shared op latch when applying operations, and suspending the engine acquires
        // an exclusive op latch. This creates a happens-before relationship allowing the
        // suspending thread to safely read the highest read postion.
        mEngine.mDecodePosition = in.mPos;

        return in;
    }

    @Override
    boolean verifyTerminator(DataIn in) {
        // No terminators to verify.
        return true;
    }

    static final class In extends DataIn {
        private final ReplicationManager mManager;

        In(ReplicationManager manager) {
            this(manager, 4096);
        }

        In(ReplicationManager manager, int bufferSize) {
            super(manager.readPosition(), bufferSize);
            mManager = manager;
        }

        @Override
        int doRead(byte[] buf, int off, int len) throws IOException {
            return mManager.read(buf, off, len);
        }

        @Override
        public void close() throws IOException {
            // Nothing to close.
        }
    }
}
