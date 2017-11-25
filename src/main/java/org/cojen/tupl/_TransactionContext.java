/*
 *  Copyright (C) 2011-2017 Cojen.org
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl;

import java.io.Flushable;
import java.io.IOException;

import java.util.concurrent.ThreadLocalRandom;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.cojen.tupl.util.Latch;

import static org.cojen.tupl.RedoOps.*;
import static org.cojen.tupl.Utils.*;

/**
 * State shared by multiple transactions. Contention is reduced by creating many context
 * instances, and distributing them among the transactions. The context vends out transaction
 * ids, supports undo log registration, and contains redo log buffers. All redo actions
 * performed by transactions flow through the context, to reduce contention on the redo writer.
 *
 * @author Generated by PageAccessTransformer from TransactionContext.java
 */
/*P*/
final class _TransactionContext extends Latch implements Flushable {
    private final static AtomicLongFieldUpdater<_TransactionContext> cHighTxnIdUpdater =
        AtomicLongFieldUpdater.newUpdater(_TransactionContext.class, "mHighTxnId");

    private final static AtomicLongFieldUpdater<_TransactionContext> cConfirmedPosUpdater =
        AtomicLongFieldUpdater.newUpdater(_TransactionContext.class, "mConfirmedPos");

    private static final int SPIN_LIMIT = Runtime.getRuntime().availableProcessors();

    private final int mTxnStride;

    // Access to these fields is protected by synchronizing on this context object.
    private long mInitialTxnId;
    private volatile long mHighTxnId;
    private _UndoLog mTopUndoLog;
    private int mUndoLogCount;

    // Access to these fields is protected by the inherited latch.
    private final byte[] mRedoBuffer;
    private int mRedoPos;
    private long mRedoFirstTxnId;
    private long mRedoLastTxnId;
    private _RedoWriter mRedoWriter;
    private boolean mRedoWriterLatched;
    private long mRedoWriterPos;

    // These fields capture the state of the highest confirmed commit, used by replication.
    // Access to these fields is protected by spinning on the mConfirmedPos field.
    private volatile long mConfirmedPos;
    private long mConfirmedTxnId;

    /**
     * @param txnStride transaction id increment
     */
    _TransactionContext(int txnStride, int redoBufferSize) {
        if (txnStride <= 0) {
            throw new IllegalArgumentException();
        }
        mTxnStride = txnStride;
        mRedoBuffer = new byte[redoBufferSize];
    }

    synchronized void addStats(Database.Stats stats) {
        stats.txnCount += mUndoLogCount;
        stats.txnsCreated += mHighTxnId / mTxnStride;
    }

    /**
     * Set the previously vended transaction id. A call to nextTransactionId returns a higher one.
     */
    void resetTransactionId(long txnId) {
        if (txnId < 0) {
            throw new IllegalArgumentException();
        }
        synchronized (this) {
            mInitialTxnId = txnId;
            mHighTxnId = txnId;
        }
    }

    /**
     * To be called only by transaction instances, and caller must hold commit lock. The commit
     * lock ensures that highest transaction id is persisted correctly by checkpoint.
     *
     * @return positive non-zero transaction id
     */
    long nextTransactionId() {
        long txnId = cHighTxnIdUpdater.addAndGet(this, mTxnStride);

        if (txnId <= 0) {
            // Improbably, the transaction identifier has wrapped around. Only vend positive
            // identifiers. Non-replicated transactions always have negative identifiers.
            synchronized (this) {
                if (mHighTxnId <= 0 && (txnId = mHighTxnId + mTxnStride) <= 0) {
                    txnId = mInitialTxnId % mTxnStride;
                }
                mHighTxnId = txnId;
            }
        }

        return txnId;
    }

    void acquireRedoLatch() {
        acquireExclusive();
    }

    void releaseRedoLatch() throws IOException {
        try {
            if (mRedoWriterLatched) try {
                if (mRedoFirstTxnId == 0) {
                    int length = mRedoPos;
                    if (length != 0) {
                        // Flush out the remaining messages.
                        try {
                            mRedoWriterPos = mRedoWriter.write(false, mRedoBuffer, 0, length);
                        } catch (IOException e) {
                            throw rethrow(e, mRedoWriter.mCloseCause);
                        }
                        mRedoPos = 0;
                    }
                }
            } finally {
                mRedoWriter.releaseExclusive();
                mRedoWriterLatched = false;
            }
        } finally {
            releaseExclusive();
        }
    }

    /**
     * Acquire the redo latch for this context, switch to the given redo writer, and then latch
     * the redo writer. Call releaseRedoLatch to release both latches.
     */
    void fullAcquireRedoLatch(_RedoWriter redo) throws IOException {
        acquireExclusive();
        try {
            if (redo != mRedoWriter) {
                switchRedo(redo);
            }
            redo.acquireExclusive();
        } catch (Throwable e) {
            releaseRedoLatch();
            throw e;
        }

        mRedoWriterLatched = true;
    }

    /**
     * Auto-commit transactional store.
     *
     * @param indexId non-zero index id
     * @param key non-null key
     * @param value value to store; null to delete
     * @return non-zero position if caller should call txnCommitSync
     */
    long redoStoreAutoCommit(_RedoWriter redo, long indexId, byte[] key, byte[] value,
                             DurabilityMode mode)
        throws IOException
    {
        keyCheck(key);
        mode = redo.opWriteCheck(mode);

        acquireRedoLatch();
        try {
            if (value == null) {
                redoWriteOp(redo, OP_DELETE, indexId);
                redoWriteUnsignedVarInt(key.length);
                redoWriteBytes(key, true);
            } else {
                redoWriteOp(redo, OP_STORE, indexId);
                redoWriteUnsignedVarInt(key.length);
                redoWriteBytes(key, false);
                redoWriteUnsignedVarInt(value.length);
                redoWriteBytes(value, true);
            }

            return redoNonTxnTerminateCommit(redo, mode);
        } finally {
            releaseRedoLatch();
        }
    }

    /**
     * Auto-commit non-transactional store.
     *
     * @param indexId non-zero index id
     * @param key non-null key
     * @param value value to store; null to delete
     * @return non-zero position if caller should call txnCommitSync
     */
    long redoStoreNoLockAutoCommit(_RedoWriter redo, long indexId, byte[] key, byte[] value,
                                   DurabilityMode mode)
        throws IOException
    {
        keyCheck(key);
        mode = redo.opWriteCheck(mode);

        acquireRedoLatch();
        try {
            if (value == null) {
                redoWriteOp(redo, OP_DELETE_NO_LOCK, indexId);
                redoWriteUnsignedVarInt(key.length);
                redoWriteBytes(key, true);
            } else {
                redoWriteOp(redo, OP_STORE_NO_LOCK, indexId);
                redoWriteUnsignedVarInt(key.length);
                redoWriteBytes(key, false);
                redoWriteUnsignedVarInt(value.length);
                redoWriteBytes(value, true);
            }

            return redoNonTxnTerminateCommit(redo, mode);
        } finally {
            releaseRedoLatch();
        }
    }

    /**
     * Auto-commit index rename.
     *
     * @param indexId non-zero index id
     * @param newName non-null new index name
     * @return non-zero position if caller should call txnCommitSync
     */
    long redoRenameIndexCommitFinal(_RedoWriter redo, long txnId, long indexId,
                                    byte[] newName, DurabilityMode mode)
        throws IOException
    {
        mode = redo.opWriteCheck(mode);

        acquireRedoLatch();
        try {
            redoWriteTxnOp(redo, OP_RENAME_INDEX, txnId);
            redoWriteLongLE(indexId);
            redoWriteUnsignedVarInt(newName.length);
            redoWriteBytes(newName, true);
            redoWriteTerminator(redo);
            return redoFlushCommit(mode);
        } finally {
            releaseRedoLatch();
        }
    }

    /**
     * Auto-commit index delete.
     *
     * @param indexId non-zero index id
     * @return non-zero position if caller should call txnCommitSync
     */
    long redoDeleteIndexCommitFinal(_RedoWriter redo, long txnId, long indexId,
                                    DurabilityMode mode)
        throws IOException
    {
        mode = redo.opWriteCheck(mode);

        acquireRedoLatch();
        try {
            redoWriteTxnOp(redo, OP_DELETE_INDEX, txnId);
            redoWriteLongLE(indexId);
            redoWriteTerminator(redo);
            return redoFlushCommit(mode);
        } finally {
            releaseRedoLatch();
        }
    }

    void redoEnter(_RedoWriter redo, long txnId) throws IOException {
        redo.opWriteCheck(null);

        acquireRedoLatch();
        try {
            redoWriteTxnOp(redo, OP_TXN_ENTER, txnId);
            redoWriteTerminator(redo);
        } finally {
            releaseRedoLatch();
        }
    }

    void redoRollback(_RedoWriter redo, long txnId) throws IOException {
        // Because rollback can release locks, it must always be flushed like a commit.
        // Otherwise, recovery can deadlock or timeout when attempting to acquire the released
        // locks. _Lock releases must always be logged before acquires.
        DurabilityMode mode = redo.opWriteCheck(DurabilityMode.NO_FLUSH);

        acquireRedoLatch();
        try {
            redoWriteTxnOp(redo, OP_TXN_ROLLBACK, txnId);
            redoWriteTerminator(redo);
            redoFlushCommit(mode);
        } finally {
            releaseRedoLatch();
        }
    }

    void redoRollbackFinal(_RedoWriter redo, long txnId) throws IOException {
        // See comments in redoRollback method.
        DurabilityMode mode = redo.opWriteCheck(DurabilityMode.NO_FLUSH);

        acquireRedoLatch();
        try {
            redoWriteTxnOp(redo, OP_TXN_ROLLBACK_FINAL, txnId);
            redoWriteTerminator(redo);
            redoFlushCommit(mode);
        } finally {
            releaseRedoLatch();
        }
    }

    void redoCommit(_RedoWriter redo, long txnId) throws IOException {
        redo.opWriteCheck(null);

        acquireRedoLatch();
        try {
            redoWriteTxnOp(redo, OP_TXN_COMMIT, txnId);
            redoWriteTerminator(redo);
        } finally {
            releaseRedoLatch();
        }
    }

    /**
     * @return non-zero position if caller should call txnCommitSync
     */
    long redoCommitFinal(_RedoWriter redo, long txnId, DurabilityMode mode)
        throws IOException
    {
        mode = redo.opWriteCheck(mode);

        acquireRedoLatch();
        try {
            redoWriteTxnOp(redo, OP_TXN_COMMIT_FINAL, txnId);
            redoWriteTerminator(redo);
            return redoFlushCommit(mode);
        } finally {
            releaseRedoLatch();
        }
    }

    void redoStore(_RedoWriter redo, byte op, long txnId, long indexId,
                   byte[] key, byte[] value)
        throws IOException
    {
        keyCheck(key);
        redo.opWriteCheck(null);

        acquireRedoLatch();
        try {
            doRedoStore(redo, op, txnId, indexId, key, value, false);
        } finally {
            releaseRedoLatch();
        }
    }

    /**
     * @return non-zero position if caller should call txnCommitSync
     */
    long redoStoreCommitFinal(_RedoWriter redo, long txnId, long indexId,
                              byte[] key, byte[] value, DurabilityMode mode)
        throws IOException
    {
        keyCheck(key);
        mode = redo.opWriteCheck(mode);

        acquireRedoLatch();
        try {
            doRedoStore(redo, OP_TXN_STORE_COMMIT_FINAL, txnId, indexId, key, value, true);
            return redoFlushCommit(mode);
        } finally {
            releaseRedoLatch();
        }
    }

    /**
     * @param commit true if last encoded operation should be treated as a transaction commit
     */
    private void doRedoStore(_RedoWriter redo, byte op, long txnId, long indexId,
                             byte[] key, byte[] value, boolean commit)
        throws IOException
    {
        redoWriteTxnOp(redo, op, txnId);
        redoWriteLongLE(indexId);
        redoWriteUnsignedVarInt(key.length);
        redoWriteBytes(key, false);
        redoWriteUnsignedVarInt(value.length);
        redoWriteBytes(value, commit);
        redoWriteTerminator(redo);
    }

    void redoDelete(_RedoWriter redo, byte op, long txnId, long indexId, byte[] key)
        throws IOException
    {
        keyCheck(key);
        redo.opWriteCheck(null);

        acquireRedoLatch();
        try {
            doRedoDelete(redo, op, txnId, indexId, key, false);
        } finally {
            releaseRedoLatch();
        }
    }

    /**
     * @return non-zero position if caller should call txnCommitSync
     */
    long redoDeleteCommitFinal(_RedoWriter redo, long txnId, long indexId,
                               byte[] key, DurabilityMode mode)
        throws IOException
    {
        keyCheck(key);
        mode = redo.opWriteCheck(mode);

        acquireRedoLatch();
        try {
            doRedoDelete(redo, OP_TXN_DELETE_COMMIT_FINAL, txnId, indexId, key, true);
            return redoFlushCommit(mode);
        } finally {
            releaseRedoLatch();
        }
    }

    /**
     * @param commit true if last encoded operation should be treated as a transaction commit
     */
    private void doRedoDelete(_RedoWriter redo, byte op, long txnId, long indexId,
                              byte[] key, boolean commit)
        throws IOException
    {
        redoWriteTxnOp(redo, op, txnId);
        redoWriteLongLE(indexId);
        redoWriteUnsignedVarInt(key.length);
        redoWriteBytes(key, commit);
        redoWriteTerminator(redo);
    }

    void redoCursorRegister(_RedoWriter redo, long cursorId, long indexId) throws IOException {
        redo.opWriteCheck(null);

        acquireRedoLatch();
        try {
            redoWriteTxnOp(redo, OP_CURSOR_REGISTER, cursorId);
            redoWriteLongLE(indexId);
            redoWriteTerminator(redo);
            // Must always flush this out, in case cursor transaction linkage changes, or when
            // transaction is linked to null. Otherwise, the cursor registration operation
            // might appear out of order in the log due to context striping.
            redoFlush(false);
        } finally {
            releaseRedoLatch();
        }
    }

    void redoCursorUnregister(_RedoWriter redo, long cursorId) throws IOException {
        redo.opWriteCheck(null);

        acquireRedoLatch();
        try {
            redoWriteTxnOp(redo, OP_CURSOR_UNREGISTER, cursorId);
            redoWriteTerminator(redo);
        } finally {
            releaseRedoLatch();
        }
    }

    void redoCursorStore(_RedoWriter redo, long cursorId, long txnId, byte[] key, byte[] value)
        throws IOException
    {
        keyCheck(key);
        redo.opWriteCheck(null);

        acquireRedoLatch();
        try {
            redoWriteTxnOp(redo, OP_CURSOR_STORE, cursorId);
            redoWriteTxnId(txnId);
            redoWriteUnsignedVarInt(key.length);
            redoWriteBytes(key, false);
            redoWriteUnsignedVarInt(value.length);
            redoWriteBytes(value, false);
            redoWriteTerminator(redo);
        } finally {
            releaseRedoLatch();
        }
    }

    void redoCursorDelete(_RedoWriter redo, long cursorId, long txnId, byte[] key)
        throws IOException
    {
        keyCheck(key);
        redo.opWriteCheck(null);

        acquireRedoLatch();
        try {
            redoWriteTxnOp(redo, OP_CURSOR_DELETE, cursorId);
            redoWriteTxnId(txnId);
            redoWriteUnsignedVarInt(key.length);
            redoWriteBytes(key, false);
            redoWriteTerminator(redo);
        } finally {
            releaseRedoLatch();
        }
    }

    void redoCursorFind(_RedoWriter redo, long cursorId, long txnId, byte[] key)
        throws IOException
    {
        redo.opWriteCheck(null);

        acquireRedoLatch();
        try {
            redoWriteTxnOp(redo, OP_CURSOR_FIND, cursorId);
            redoWriteTxnId(txnId);
            redoWriteUnsignedVarInt(key.length);
            redoWriteBytes(key, false);
            redoWriteTerminator(redo);
        } finally {
            releaseRedoLatch();
        }
    }

    void redoCursorValueSetLength(_RedoWriter redo, long cursorId, long txnId, long length)
        throws IOException
    {
        redo.opWriteCheck(null);

        acquireRedoLatch();
        try {
            redoWriteTxnOp(redo, OP_CURSOR_VALUE_SET_LENGTH, cursorId);
            redoWriteTxnId(txnId);
            redoWriteUnsignedVarLong(length);
            redoWriteTerminator(redo);
        } finally {
            releaseRedoLatch();
        }
    }

    void redoCursorValueWrite(_RedoWriter redo, long cursorId, long txnId,
                              long pos, byte[] buf, int off, int len)
        throws IOException
    {
        redo.opWriteCheck(null);

        acquireRedoLatch();
        try {
            redoWriteTxnOp(redo, OP_CURSOR_VALUE_WRITE, cursorId);
            redoWriteTxnId(txnId);
            redoWriteUnsignedVarLong(pos);
            redoWriteUnsignedVarInt(len);
            redoWriteBytes(buf, off, len, false);
            redoWriteTerminator(redo);
        } finally {
            releaseRedoLatch();
        }
    }

    void redoCursorValueClear(_RedoWriter redo, long cursorId, long txnId, long pos, long length)
        throws IOException
    {
        redo.opWriteCheck(null);

        acquireRedoLatch();
        try {
            redoWriteTxnOp(redo, OP_CURSOR_VALUE_CLEAR, cursorId);
            redoWriteTxnId(txnId);
            redoWriteUnsignedVarLong(pos);
            redoWriteUnsignedVarLong(length);
            redoWriteTerminator(redo);
        } finally {
            releaseRedoLatch();
        }
    }

    void redoCustom(_RedoWriter redo, long txnId, byte[] message) throws IOException {
        if (message == null) {
            throw new NullPointerException("Message is null");
        }
        redo.opWriteCheck(null);

        acquireRedoLatch();
        try {
            redoWriteTxnOp(redo, OP_TXN_CUSTOM, txnId);
            redoWriteUnsignedVarInt(message.length);
            redoWriteBytes(message, false);
            redoWriteTerminator(redo);
        } finally {
            releaseRedoLatch();
        }
    }

    void redoCustomLock(_RedoWriter redo, long txnId, byte[] message, long indexId, byte[] key)
        throws IOException
    {
        keyCheck(key);
        if (message == null) {
            throw new NullPointerException("Message is null");
        }
        redo.opWriteCheck(null);

        acquireRedoLatch();
        try {
            redoWriteTxnOp(redo, OP_TXN_CUSTOM_LOCK, txnId);
            redoWriteLongLE(indexId);
            redoWriteUnsignedVarInt(key.length);
            redoWriteBytes(key, false);
            redoWriteUnsignedVarInt(message.length);
            redoWriteBytes(message, false);
            redoWriteTerminator(redo);
        } finally {
            releaseRedoLatch();
        }
    }

    // Caller must hold redo latch.
    void doRedoReset(_RedoWriter redo) throws IOException {
        redo.opWriteCheck(null);
        redoWriteOp(redo, OP_RESET);
        redoNonTxnTerminateCommit(redo, DurabilityMode.NO_FLUSH);
        assert mRedoWriterLatched;
        redo.mLastTxnId = 0;
    }

    /**
     * @param op OP_TIMESTAMP, OP_SHUTDOWN, OP_CLOSE, or OP_END_FILE
     */
    void redoTimestamp(_RedoWriter redo, byte op) throws IOException {
        acquireRedoLatch();
        try {
            doRedoTimestamp(redo, op, DurabilityMode.NO_FLUSH);
        } finally {
            releaseRedoLatch();
        }
    }

    /**
     * @param op OP_TIMESTAMP, OP_SHUTDOWN, OP_CLOSE, or OP_END_FILE
     */
    // Caller must hold redo latch.
    void doRedoTimestamp(_RedoWriter redo, byte op, DurabilityMode mode) throws IOException {
        doRedoOp(redo, op, System.currentTimeMillis(), mode);
    }

    // Caller must hold redo latch.
    void doRedoNopRandom(_RedoWriter redo, DurabilityMode mode) throws IOException {
        doRedoOp(redo, OP_NOP_RANDOM, ThreadLocalRandom.current().nextLong(), mode);
    }

    // Caller must hold redo latch.
    private void doRedoOp(_RedoWriter redo, byte op, long operand, DurabilityMode mode)
        throws IOException
    {
        redo.opWriteCheck(null);
        redoWriteOp(redo, op, operand);
        redoNonTxnTerminateCommit(redo, mode);
    }

    long redoControl(_RedoWriter redo, byte[] message) throws IOException {
        if (message == null) {
            throw new NullPointerException("Message is null");
        }
        redo.opWriteCheck(null);

        acquireRedoLatch();
        try {
            redoWriteOp(redo, OP_CONTROL);
            redoWriteUnsignedVarInt(message.length);
            redoWriteBytes(message, true);
            // Must use SYNC to obtain the log position.
            return redoNonTxnTerminateCommit(redo, DurabilityMode.SYNC);
        } finally {
            releaseRedoLatch();
        }
    }

    /**
     * Terminate and commit a non-transactional operation. Caller must hold redo latch.
     *
     * @return non-zero position if sync is required.
     */
    private long redoNonTxnTerminateCommit(_RedoWriter redo, DurabilityMode mode)
        throws IOException
    {
        if (!redo.shouldWriteTerminators()) {
            // Commit the normal way.
            return redoFlushCommit(mode);
        }

        if (mRedoFirstTxnId != 0) {
            // Terminate and commit the normal way.
            redoWriteIntLE(nzHash(mRedoLastTxnId));
            return redoFlushCommit(mode);
        }

        boolean commit = mode == DurabilityMode.SYNC || mode == DurabilityMode.NO_SYNC;

        int length = mRedoPos;
        byte[] buffer = mRedoBuffer;
        redo = latchWriter();

        if (length > buffer.length - 4) {
            // Flush and make room for the terminator.
            try {
                mRedoWriterPos = redo.write(false, buffer, 0, length);
            } catch (IOException e) {
                throw rethrow(e, redo.mCloseCause);
            }
            length = 0;
        }

        // Encode the terminator using the "true" last transaction id.
        Utils.encodeIntLE(buffer, length, nzHash(redo.mLastTxnId));
        length += 4;

        try {
            mRedoWriterPos = redo.write(commit, buffer, 0, length);
        } catch (IOException e) {
            throw rethrow(e, redo.mCloseCause);
        }

        mRedoPos = 0;

        return mode == DurabilityMode.SYNC ? mRedoWriterPos : 0;
    }

    // Caller must hold redo latch.
    private void redoWriteTerminator(_RedoWriter redo) throws IOException {
        // Note: A terminator following a commit operation doesn't need any special handling
        // here. The call to redoWriteIntLE always leaves something in the buffer, and so the
        // call to redoFlushCommit will have something to do.
        if (redo.shouldWriteTerminators()) {
            redoWriteIntLE(nzHash(mRedoLastTxnId));
        }
    }

    // Caller must hold redo latch.
    private void redoWriteIntLE(int v) throws IOException {
        byte[] buffer = mRedoBuffer;
        int pos = mRedoPos;
        if (pos > buffer.length - 4) {
            redoFlush(false);
            pos = 0;
        }
        Utils.encodeIntLE(buffer, pos, v);
        mRedoPos = pos + 4;
    }

    // Caller must hold redo latch.
    private void redoWriteLongLE(long v) throws IOException {
        byte[] buffer = mRedoBuffer;
        int pos = mRedoPos;
        if (pos > buffer.length - 8) {
            redoFlush(false);
            pos = 0;
        }
        Utils.encodeLongLE(buffer, pos, v);
        mRedoPos = pos + 8;
    }

    // Caller must hold redo latch.
    private void redoWriteUnsignedVarInt(int v) throws IOException {
        byte[] buffer = mRedoBuffer;
        int pos = mRedoPos;
        if (pos > buffer.length - 5) {
            redoFlush(false);
            pos = 0;
        }
        mRedoPos = Utils.encodeUnsignedVarInt(buffer, pos, v);
    }

    // Caller must hold redo latch.
    private void redoWriteUnsignedVarLong(long v) throws IOException {
        byte[] buffer = mRedoBuffer;
        int pos = mRedoPos;
        if (pos > buffer.length - 9) {
            redoFlush(false);
            pos = 0;
        }
        mRedoPos = Utils.encodeUnsignedVarLong(buffer, pos, v);
    }

    /**
     * @param commit true if last encoded operation should be treated as a transaction commit
     */
    // Caller must hold redo latch.
    private void redoWriteBytes(byte[] bytes, boolean commit) throws IOException {
        redoWriteBytes(bytes, 0, bytes.length, commit);
    }

    /**
     * @param commit true if last encoded operation should be treated as a transaction commit
     */
    // Caller must hold redo latch.
    private void redoWriteBytes(byte[] bytes, int offset, int length, boolean commit)
        throws IOException
    {
        if (length == 0) {
            return;
        }

        byte[] buffer = mRedoBuffer;
        int avail = buffer.length - mRedoPos;

        if (avail >= length) {
            if (mRedoPos == 0 && avail == length) {
                _RedoWriter redo = latchWriter();
                try {
                    mRedoWriterPos = redo.write(isCommit(commit), bytes, offset, length);
                } catch (IOException e) {
                    throw rethrow(e, redo.mCloseCause);
                }
            } else {
                System.arraycopy(bytes, offset, buffer, mRedoPos, length);
                mRedoPos += length;
            }
        } else {
            // Fill remainder of buffer and flush it.
            System.arraycopy(bytes, offset, buffer, mRedoPos, avail);
            mRedoPos = buffer.length;

            // Latches writer as a side-effect.
            redoFlush(false);

            offset += avail;
            length -= avail;

            if (length >= buffer.length) {
                try {
                    mRedoWriterPos = mRedoWriter.write(isCommit(commit), bytes, offset, length);
                } catch (IOException e) {
                    throw rethrow(e, mRedoWriter.mCloseCause);
                }
            } else {
                System.arraycopy(bytes, offset, buffer, 0, length);
                mRedoPos = length;
            }
        }
    }

    /**
     * Returns true only when commit is requested and no terminators are written.
     */
    private boolean isCommit(boolean commit) {
        return commit && !mRedoWriter.shouldWriteTerminators();
    }

    /**
     * Write a non-transactional operation. Caller must hold redo latch and always flush the
     * operation. Flushing ensures that transactional operations that follow can encode
     * transaction id deltas correctly.
     */
    private void redoWriteOp(_RedoWriter redo, byte op) throws IOException {
        mRedoPos = doRedoWriteOp(redo, op, 1); // 1 for op
    }

    /**
     * Write a non-transactional operation. Caller must hold redo latch and always flush the
     * operation. Flushing ensures that transactional operations that follow can encode
     * transaction id deltas correctly.
     */
    private void redoWriteOp(_RedoWriter redo, byte op, long operand) throws IOException {
        int pos = doRedoWriteOp(redo, op, 1 + 8); // 1 for op, 8 for operand
        Utils.encodeLongLE(mRedoBuffer, pos, operand);
        mRedoPos = pos + 8;
    }

    // Caller must hold redo latch.
    private int doRedoWriteOp(_RedoWriter redo, byte op, int len) throws IOException {
        if (redo != mRedoWriter) {
            switchRedo(redo);
        }

        byte[] buffer = mRedoBuffer;
        int pos = mRedoPos;

        if (pos > buffer.length - len) {
            redoFlush(false);
            pos = 0;
        }

        buffer[pos] = op;
        return pos + 1;
    }

    // Caller must hold redo latch.
    private void redoWriteTxnOp(_RedoWriter redo, byte op, long txnId) throws IOException {
        if (redo != mRedoWriter) {
            switchRedo(redo);
        }

        byte[] buffer = mRedoBuffer;
        int pos = mRedoPos;

        prepare: {
            if (pos > buffer.length - (1 + 9)) { // 1 for op, up to 9 for txn delta
                redoFlush(false);
                pos = 0;
            } else if (pos != 0) {
                mRedoPos = Utils.encodeSignedVarLong(buffer, pos + 1, txnId - mRedoLastTxnId);
                break prepare;
            }
            mRedoFirstTxnId = txnId;
            mRedoPos = 1 + 9; // 1 for op, and reserve 9 for txn delta
        }

        buffer[pos] = op;
        mRedoLastTxnId = txnId;
    }

    // Caller must hold redo latch.
    private void redoWriteTxnId(long txnId) throws IOException {
        byte[] buffer = mRedoBuffer;
        int pos = mRedoPos;

        prepare: {
            if (pos > buffer.length - 9) { // up to 9 for txn delta
                redoFlush(false);
                pos = 0;
            } else if (pos != 0) {
                mRedoPos = Utils.encodeSignedVarLong(buffer, pos, txnId - mRedoLastTxnId);
                break prepare;
            }
            mRedoFirstTxnId = txnId;
            mRedoPos = 9; // reserve 9 for txn delta
        }

        mRedoLastTxnId = txnId;
    }

    /**
     * Flush redo buffer to the current _RedoWriter.
     */
    @Override
    public void flush() throws IOException {
        acquireRedoLatch();
        try {
            doFlush();
        } finally {
            releaseRedoLatch();
        }
    }

    // Caller must hold redo latch.
    void doFlush() throws IOException {
        redoFlush(false);
    }

    // Caller must hold redo latch.
    private void switchRedo(_RedoWriter redo) throws IOException {
        try {
            redoFlush(false);
        } catch (UnmodifiableReplicaException e) {
            // Terminal state, so safe to discard everything.
            mRedoPos = 0;
            mRedoFirstTxnId = 0;
        } finally {
            if (mRedoWriterLatched) {
                mRedoWriter.releaseExclusive();
                mRedoWriterLatched = false;
            }
        }

        mRedoWriter = redo;
    }

    /**
     * Caller must hold redo latch and ensure that mRedoWriter is set.
     *
     * @return non-zero position if sync is required.
     */
    private long redoFlushCommit(DurabilityMode mode) throws IOException {
        if (mode == DurabilityMode.SYNC) {
            redoFlush(true);
            return mRedoWriterPos;
        } else {
            redoFlush(mode == DurabilityMode.NO_SYNC); // ignore commit for NO_FLUSH, etc.
            return 0;
        }
    }

    /**
     * Caller must hold redo latch and ensure that mRedoWriter is set.
     *
     * @param commit true if last encoded operation should be treated as a transaction commit
     */
    private void redoFlush(boolean commit) throws IOException {
        int length = mRedoPos;
        if (length == 0) {
            return;
        }

        byte[] buffer = mRedoBuffer;
        int offset = 0;
        _RedoWriter redo = latchWriter();

        final long redoWriterLastTxnId = redo.mLastTxnId;

        if (mRedoFirstTxnId != 0) {
            // Encode the first transaction delta and shift the opcode.
            long delta = convertSignedVarLong(mRedoFirstTxnId - redoWriterLastTxnId);
            int varLen = calcUnsignedVarLongLength(delta);
            offset = (1 + 9) - varLen;
            encodeUnsignedVarLong(buffer, offset, delta);
            buffer[--offset] = buffer[0];
            length -= offset;
            // Must always set before write is called, so that it can see the update.
            redo.mLastTxnId = mRedoLastTxnId;
        }

        try {
            try {
                mRedoWriterPos = redo.write(commit, buffer, offset, length);
            } catch (IOException e) {
                throw rethrow(e, redo.mCloseCause);
            }
        } catch (Throwable e) {
            // Rollback.
            redo.mLastTxnId = redoWriterLastTxnId;
            throw e;
        }

        mRedoPos = 0;
        mRedoFirstTxnId = 0;
    }

    // Caller must hold redo latch.
    private _RedoWriter latchWriter() {
        _RedoWriter redo = mRedoWriter;
        if (!mRedoWriterLatched) {
            redo.acquireExclusive();
            mRedoWriterLatched = true;
        }
        return redo;
    }

    /**
     * Only to be called when commit position which was confirmed doesn't have an associated
     * transaction. Expected to only be used for replication control operations, so it doesn't
     * need an optimized implementation.
     */
    void confirmed(long commitPos) {
        if (commitPos == -1) {
            throw new IllegalArgumentException();
        }
        mConfirmedPos = Math.max(latchConfirmed(), commitPos);
    }

    void confirmed(long commitPos, long txnId) {
        if (commitPos == -1) {
            throw new IllegalArgumentException();
        }

        long confirmedPos = mConfirmedPos;

        check: {
            if (confirmedPos != -1) {
                if (commitPos <= confirmedPos) {
                    return;
                }
                if (cConfirmedPosUpdater.compareAndSet(this, confirmedPos, -1)) {
                    break check;
                }
            }

            confirmedPos = latchConfirmed();

            if (commitPos <= confirmedPos) {
                // Release the latch.
                mConfirmedPos = confirmedPos;
                return;
            }
        }

        mConfirmedTxnId = txnId;
        // Set this last, because it releases the latch.
        mConfirmedPos = commitPos;
    }

    /**
     * Returns the context with the higher confirmed position.
     */
    _TransactionContext higherConfirmed(_TransactionContext other) {
        return mConfirmedPos >= other.mConfirmedPos ? this : other;
    }

    /**
     * Copy the confirmed position and transaction id to the returned array.
     */
    long[] copyConfirmed() {
        long[] result = new long[2];
        long confirmedPos = latchConfirmed();
        result[0] = confirmedPos;
        result[1] = mConfirmedTxnId;
        // Release the latch.
        mConfirmedPos = confirmedPos;
        return result;
    }

    /**
     * @return value of mConfirmedPos to set to release the latch
     */
    private long latchConfirmed() {
        int trials = 0;
        while (true) {
            long confirmedPos = mConfirmedPos;
            if (confirmedPos != -1 && cConfirmedPosUpdater.compareAndSet(this, confirmedPos, -1)) {
                return confirmedPos;
            }
            trials++;
            if (trials >= SPIN_LIMIT) {
                Thread.yield();
                trials = 0;
            }
        }
    }

    /**
     * Caller must hold db commit lock.
     */
    synchronized void register(_UndoLog undo) {
        _UndoLog top = mTopUndoLog;
        if (top != null) {
            undo.mPrev = top;
            top.mNext = undo;
        }
        mTopUndoLog = undo;
        mUndoLogCount++;
    }

    /**
     * Should only be called after all log entries have been truncated or rolled back. Caller
     * does not need to hold db commit lock.
     */
    synchronized void unregister(_UndoLog log) {
        _UndoLog prev = log.mPrev;
        _UndoLog next = log.mNext;
        if (prev != null) {
            prev.mNext = next;
            log.mPrev = null;
        }
        if (next != null) {
            next.mPrev = prev;
            log.mNext = null;
        } else if (log == mTopUndoLog) {
            mTopUndoLog = prev;
        }
        mUndoLogCount--;
    }

    /**
     * Returns the current transaction id or the given one, depending on which is higher.
     */
    long higherTransactionId(long txnId) {
        return Math.max(mHighTxnId, txnId);
    }

    /**
     * Caller must synchronize on this context object.
     */
    boolean hasUndoLogs() {
        return mTopUndoLog != null;
    }

    /**
     * Write any undo log references to the master undo log. Caller must hold db commit lock
     * and synchronize on this context object.
     *
     * @param workspace temporary buffer, allocated on demand
     * @return new or original workspace instance
     */
    byte[] writeToMaster(_UndoLog master, byte[] workspace) throws IOException {
        for (_UndoLog log = mTopUndoLog; log != null; log = log.mPrev) {
            workspace = log.writeToMaster(master, workspace);
        }
        return workspace;
    }

    /**
     * Deletes any _UndoLog instances, as part of database close sequence. Caller must hold
     * exclusive db commit lock.
     */
    synchronized void deleteUndoLogs() {
        for (_UndoLog log = mTopUndoLog; log != null; log = log.mPrev) {
            log.delete();
        }
        mTopUndoLog = null;
    }
}
