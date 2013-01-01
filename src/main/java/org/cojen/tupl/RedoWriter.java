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

import java.io.Flushable;
import java.io.IOException;

import static org.cojen.tupl.RedoOps.*;

/**
 * Abstract class for active transactions to write into.
 *
 * @author Brian S O'Neill
 * @see RedoDecoder
 */
abstract class RedoWriter extends CauseCloseable implements Checkpointer.Shutdown, Flushable {
    private final byte[] mBuffer;
    private int mBufferPos;

    private long mLastTxnId;

    private boolean mAlwaysFlush;

    volatile Throwable mCause;

    RedoWriter(int bufferSize, long initialTxnId) {
        mBuffer = new byte[bufferSize];
        mLastTxnId = initialTxnId;
    }

    /**
     * Auto-commit transactional store.
     *
     * @param indexId non-zero index id
     * @param key non-null key
     * @param value value to store; null to delete
     */
    public void store(long indexId, byte[] key, byte[] value, DurabilityMode mode)
        throws IOException
    {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }

        boolean sync;
        synchronized (this) {
            if (value == null) {
                writeOp(OP_DELETE, indexId);
                writeUnsignedVarInt(key.length);
                writeBytes(key);
            } else {
                writeOp(OP_STORE, indexId);
                writeUnsignedVarInt(key.length);
                writeBytes(key);
                writeUnsignedVarInt(value.length);
                writeBytes(value);
            }
            writeTerminator();

            sync = conditionalFlush(mode);
        }

        if (sync) {
            sync(false);
        }
    }

    public synchronized void reset() throws IOException {
        writeOp(OP_RESET, mLastTxnId);
        writeTerminator();
    }

    public synchronized void txnEnter(long txnId) throws IOException {
        writeTxnOp(OP_TXN_ENTER, txnId);
        writeTerminator();
    }

    public synchronized void txnRollback(long txnId) throws IOException {
        writeTxnOp(OP_TXN_ROLLBACK, txnId);
        writeTerminator();
    }

    public synchronized void txnRollbackFinal(long txnId) throws IOException {
        writeTxnOp(OP_TXN_ROLLBACK_FINAL, txnId);
        writeTerminator();
    }

    public synchronized void txnCommit(long txnId) throws IOException {
        writeTxnOp(OP_TXN_COMMIT, txnId);
        writeTerminator();
    }

    /**
     * @return true if caller should call txnCommitSync
     */
    public synchronized boolean txnCommitFinal(long txnId, DurabilityMode mode)
        throws IOException
    {
        writeTxnOp(OP_TXN_COMMIT_FINAL, txnId);
        writeTerminator();
        return conditionalFlush(mode);
    }

    /**
     * Called after txnCommitFinal.
     */
    public void txnCommitSync() throws IOException {
        sync(false);
    }

    public synchronized void txnStore(byte op, long txnId, long indexId, byte[] key, byte[] value)
        throws IOException
    {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }
        writeTxnOp(op, txnId);
        writeLongLE(indexId);
        writeUnsignedVarInt(key.length);
        writeBytes(key);
        writeUnsignedVarInt(value.length);
        writeBytes(value);
        writeTerminator();
    }

    public synchronized void txnDelete(byte op, long txnId, long indexId, byte[] key)
        throws IOException
    {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }
        writeTxnOp(op, txnId);
        writeLongLE(indexId);
        writeUnsignedVarInt(key.length);
        writeBytes(key);
        writeTerminator();
    }

    public synchronized void timestamp() throws IOException {
        writeOp(OP_TIMESTAMP, System.currentTimeMillis());
        writeTerminator();
    }

    public synchronized void endFile() throws IOException {
        writeOp(OP_END_FILE, System.currentTimeMillis());
        writeTerminator();
        flush();
    }

    @Override
    public synchronized void flush() throws IOException {
        doFlush();
    }

    public void sync() throws IOException {
        flush();
        sync(false);
    }

    @Override
    public synchronized void close() throws IOException {
        close(null);
    }

    @Override
    public synchronized void close(Throwable cause) throws IOException {
        if (cause != null) {
            mCause = cause;
        }
        shutdown(OP_CLOSE);
    }

    @Override
    public void shutdown() {
        try {
            shutdown(OP_SHUTDOWN);
        } catch (IOException e) {
            // Ignore.
        }
    }

    private void shutdown(byte op) throws IOException {
        synchronized (this) {
            mAlwaysFlush = true;

            if (!isOpen()) {
                return;
            }

            writeOp(op, System.currentTimeMillis());
            writeTerminator();
            doFlush();

            if (op == OP_CLOSE) {
                try {
                    forceAndClose();
                } catch (IOException e) {
                    throw Utils.rethrow(e, mCause);
                }
                return;
            }
        }

        force(true);
    }

    // Caller must be synchronized.
    abstract boolean isOpen();

    /**
     * Returns true if uncheckpointed redo size is at least the given threshold
     */
    abstract boolean shouldCheckpoint(long sizeThreshold);

    /**
     * Returns the redo position for the next change, as would be recorded by a
     * checkpoint.
     */
    abstract long checkpointPosition();

    /**
     * Prepares a new redo log file, if applicable.
     */
    abstract void prepareCheckpoint() throws IOException;

    /**
     * Writer can discard all redo data lower than the given checkpointed
     * position.
     */
    abstract void checkpointed(long position) throws IOException;

    // Caller must be synchronized.
    abstract void write(byte[] buffer, int len) throws IOException;

    abstract void force(boolean metadata) throws IOException;

    abstract void forceAndClose() throws IOException;

    // Caller must be synchronized.
    abstract void writeTerminator() throws IOException;

    // Caller must be synchronized.
    void writeIntLE(int v) throws IOException {
        byte[] buffer = mBuffer;
        int pos = mBufferPos;
        if (pos > buffer.length - 4) {
            doFlush(buffer, pos);
            pos = 0;
        }
        Utils.writeIntLE(buffer, pos, v);
        mBufferPos = pos + 4;
    }

    // Caller must be synchronized.
    void writeLongLE(long v) throws IOException {
        byte[] buffer = mBuffer;
        int pos = mBufferPos;
        if (pos > buffer.length - 8) {
            doFlush(buffer, pos);
            pos = 0;
        }
        Utils.writeLongLE(buffer, pos, v);
        mBufferPos = pos + 8;
    }

    // Caller must be synchronized.
    void writeUnsignedVarInt(int v) throws IOException {
        byte[] buffer = mBuffer;
        int pos = mBufferPos;
        if (pos > buffer.length - 5) {
            doFlush(buffer, pos);
            pos = 0;
        }
        mBufferPos = Utils.writeUnsignedVarInt(buffer, pos, v);
    }

    // Caller must be synchronized.
    void writeBytes(byte[] bytes) throws IOException {
        writeBytes(bytes, 0, bytes.length);
    }

    // Caller must be synchronized.
    void writeBytes(byte[] bytes, int offset, int length) throws IOException {
        if (length == 0) {
            return;
        }
        byte[] buffer = mBuffer;
        int pos = mBufferPos;
        while (true) {
            if (pos <= buffer.length - length) {
                System.arraycopy(bytes, offset, buffer, pos, length);
                mBufferPos = pos + length;
                return;
            }
            int remaining = buffer.length - pos;
            System.arraycopy(bytes, offset, buffer, pos, remaining);
            doFlush(buffer, buffer.length);
            pos = 0;
            offset += remaining;
            length -= remaining;
        }
    }

    // Caller must be synchronized.
    private void writeOp(byte op, long operand) throws IOException {
        byte[] buffer = mBuffer;
        int pos = mBufferPos;
        if (pos >= buffer.length - (1 + 8)) { // 1 for op, 8 for operand
            doFlush(buffer, pos);
            pos = 0;
        }
        buffer[pos] = op;
        Utils.writeLongLE(buffer, pos + 1, operand);
        mBufferPos = pos + 9;
    }

    // Caller must be synchronized.
    private void writeTxnOp(byte op, long txnId) throws IOException {
        byte[] buffer = mBuffer;
        int pos = mBufferPos;
        if (pos >= buffer.length - (1 + 9)) { // 1 for op, up to 9 for txn delta
            doFlush(buffer, pos);
            pos = 0;
        }
        buffer[pos] = op;
        mBufferPos = Utils.writeSignedVarLong(buffer, pos + 1, txnId - mLastTxnId);
        mLastTxnId = txnId;
    }

    // Caller must be synchronized.
    private void doFlush() throws IOException {
        doFlush(mBuffer, mBufferPos);
    }

    // Caller must be synchronized.
    private void doFlush(byte[] buffer, int len) throws IOException {
        try {
            write(buffer, len);
            mBufferPos = 0;
        } catch (IOException e) {
            throw Utils.rethrow(e, mCause);
        }
    }

    private void sync(boolean metadata) throws IOException {
        try {
            force(metadata);
        } catch (IOException e) {
            throw Utils.rethrow(e, mCause);
        }
    }

    // Caller must be synchronized. Returns true if caller should sync.
    private boolean conditionalFlush(DurabilityMode mode) throws IOException {
        switch (mode) {
        default:
            return false;
        case NO_FLUSH:
            if (mAlwaysFlush) {
                doFlush();
            }
            return false;
        case SYNC:
            doFlush();
            return true;
        case NO_SYNC:
            doFlush();
            return false;
        }
    }
}
