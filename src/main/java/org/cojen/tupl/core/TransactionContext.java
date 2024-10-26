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

package org.cojen.tupl.core;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.io.Flushable;
import java.io.IOException;

import java.util.concurrent.ThreadLocalRandom;

import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.UnmodifiableReplicaException;

import org.cojen.tupl.diag.DatabaseStats;

import org.cojen.tupl.util.Latch;

import static org.cojen.tupl.core.RedoOps.*;
import static org.cojen.tupl.core.Utils.*;

/**
 * State shared by multiple transactions. Contention is reduced by creating many context
 * instances, and distributing them among the transactions. The context vends out transaction
 * ids, supports undo log registration, and contains redo log buffers. All redo actions
 * performed by transactions flow through the context, to reduce contention on the redo writer.
 *
 * @author Brian S O'Neill
 */
/*P*/
final class TransactionContext extends Latch implements Flushable {
    private final static VarHandle cHighTxnIdHandle;

    static {
        try {
            cHighTxnIdHandle =
                MethodHandles.lookup().findVarHandle
                (TransactionContext.class, "mHighTxnId", long.class);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    private final long mTxnStride;

    // Access to these fields is protected by synchronizing on this context object.
    private long mInitialTxnId;
    private volatile long mHighTxnId;
    private UndoLog mTopUndoLog;
    private int mUndoLogCount;
    private LHashSet mUncommitted;

    // Access to these fields is protected by the inherited latch.
    private final byte[] mRedoBuffer;
    private int mRedoPos;
    private int mRedoTerminatePos;
    private long mRedoFirstTxnId;
    private long mRedoLastTxnId;
    private RedoWriter mRedoWriter;
    private boolean mRedoWriterLatched;
    private long mRedoWriterPos;

    /**
     * @param txnStride transaction id increment
     */
    TransactionContext(int txnStride, int redoBufferSize) {
        if (txnStride <= 0) {
            throw new IllegalArgumentException();
        }
        mTxnStride = txnStride;
        mRedoBuffer = new byte[redoBufferSize];
    }

    synchronized void addStats(DatabaseStats stats) {
        stats.transactionCount += mUndoLogCount;
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
        long txnId = (long) cHighTxnIdHandle.getAndAdd(this, mTxnStride) + mTxnStride;

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
                        // Write out the remaining non-transactional messages.
                        try {
                            mRedoWriterPos = mRedoWriter.write
                                (false, mRedoBuffer, 0, length, mRedoTerminatePos, null);
                        } catch (IOException e) {
                            throw rethrow(e, mRedoWriter.mCloseCause);
                        }
                        mRedoPos = 0;
                        mRedoTerminatePos = 0;
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
    void fullAcquireRedoLatch(RedoWriter redo) throws IOException {
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
     * Called when switching to replica mode, instead of referencing the useless old RedoWriter
     * instance indefinitely. This permits the garbage collector to delete it.
     */
    void discardRedoWriter(RedoWriter expect) {
        acquireExclusive();
        if (mRedoWriter == expect) {
            mRedoPos = 0;
            mRedoTerminatePos = 0;
            mRedoFirstTxnId = 0;
            mRedoWriter = null;
        }
        releaseExclusive();
    }

    /**
     * Auto-commit transactional store.
     *
     * @param indexId non-zero index id
     * @param key non-null key
     * @param value value to store; null to delete
     * @return non-zero position if caller should call txnCommitSync
     */
    long redoStoreAutoCommit(RedoWriter redo, long indexId, byte[] key, byte[] value,
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
    long redoStoreNoLockAutoCommit(RedoWriter redo, long indexId, byte[] key, byte[] value,
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
    long redoRenameIndexCommitFinal(RedoWriter redo, long txnId, long indexId,
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
    long redoDeleteIndexCommitFinal(RedoWriter redo, long txnId, long indexId,
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

    void redoEnter(RedoWriter redo, long txnId) throws IOException {
        redo.opWriteCheck(null);

        acquireRedoLatch();
        try {
            redoWriteTxnOp(redo, OP_TXN_ENTER, txnId);
            redoWriteTerminator(redo);
        } finally {
            releaseRedoLatch();
        }
    }

    void redoRollback(RedoWriter redo, long txnId) throws IOException {
        // Because rollback can release locks, it must always be flushed like a commit.
        // Otherwise, recovery can deadlock or timeout when attempting to acquire the released
        // locks. Lock releases must always be logged before acquires.
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

    void redoRollbackFinal(RedoWriter redo, long txnId) throws IOException {
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

    void redoCommit(RedoWriter redo, long txnId) throws IOException {
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
     * @return non-zero position if caller should call txnCommitSync; -1 if transaction is pending
     */
    long redoCommitFinal(LocalTransaction txn) throws IOException {
        RedoWriter redo = txn.mRedo;
        DurabilityMode mode = redo.opWriteCheck(txn.mDurabilityMode);

        if (mode == DurabilityMode.SYNC && txn.mDurabilityMode != DurabilityMode.SYNC) {
            PendingTxn pending = txn.preparePending();
            try {
                acquireRedoLatch();
                try {
                    redoWriteTxnOp(redo, OP_TXN_COMMIT_FINAL, pending.mTxnId);
                    redoWriteTerminator(redo);
                    redoFlush(true, pending);
                } finally {
                    releaseRedoLatch();
                }
                return -1;
            } catch (Throwable e) {
                throw pending.rollback(e);
            }
        }

        acquireRedoLatch();
        try {
            redoWriteTxnOp(redo, OP_TXN_COMMIT_FINAL, txn.mTxnId);
            redoWriteTerminator(redo);
            return redoFlushCommit(mode);
        } finally {
            releaseRedoLatch();
        }
    }

    /**
     * @param op OP_TXN_LOCK_SHARED, OP_TXN_LOCK_UPGRADABLE, or OP_TXN_LOCK_EXCLUSIVE
     */
    void redoLock(RedoWriter redo, byte op, long txnId, long indexId, byte[] key)
        throws IOException
    {
        keyCheck(key);
        redo.opWriteCheck(null);

        acquireRedoLatch();
        try {
            redoWriteTxnOp(redo, op, txnId);
            redoWriteLongLE(indexId);
            redoWriteUnsignedVarInt(key.length);
            redoWriteBytes(key, true);
            redoWriteTerminator(redo);
        } finally {
            releaseRedoLatch();
        }
    }

    void redoStore(RedoWriter redo, byte op, long txnId, long indexId,
                   byte[] key, byte[] value)
        throws IOException
    {
        keyCheck(key);
        redo.opWriteCheck(null);

        acquireRedoLatch();
        try {
            doRedoStore(redo, op, txnId, indexId, key, value);
        } finally {
            releaseRedoLatch();
        }
    }

    /**
     * @return non-zero position if caller should call txnCommitSync; -1 if transaction is pending
     */
    long redoStoreCommitFinal(LocalTransaction txn, long indexId, byte[] key, byte[] value)
        throws IOException
    {
        keyCheck(key);

        RedoWriter redo = txn.mRedo;
        DurabilityMode mode = redo.opWriteCheck(txn.mDurabilityMode);

        if (mode == DurabilityMode.SYNC && txn.mDurabilityMode != DurabilityMode.SYNC) {
            PendingTxn pending = txn.preparePending();
            try {
                acquireRedoLatch();
                try {
                    doRedoStore(redo, OP_TXN_STORE_COMMIT_FINAL,
                                pending.mTxnId, indexId, key, value);
                    redoFlush(true, pending);
                } finally {
                    releaseRedoLatch();
                }
                return -1;
            } catch (Throwable e) {
                throw pending.rollback(e);
            }
        }

        acquireRedoLatch();
        try {
            doRedoStore(redo, OP_TXN_STORE_COMMIT_FINAL, txn.mTxnId, indexId, key, value);
            return redoFlushCommit(mode);
        } finally {
            releaseRedoLatch();
        }
    }

    private void doRedoStore(RedoWriter redo, byte op, long txnId, long indexId,
                             byte[] key, byte[] value)
        throws IOException
    {
        redoWriteTxnOp(redo, op, txnId);
        redoWriteLongLE(indexId);
        redoWriteUnsignedVarInt(key.length);
        redoWriteBytes(key, false);
        redoWriteUnsignedVarInt(value.length);
        redoWriteBytes(value, true);
        redoWriteTerminator(redo);
    }

    void redoDelete(RedoWriter redo, byte op, long txnId, long indexId, byte[] key)
        throws IOException
    {
        keyCheck(key);
        redo.opWriteCheck(null);

        acquireRedoLatch();
        try {
            doRedoDelete(redo, op, txnId, indexId, key);
        } finally {
            releaseRedoLatch();
        }
    }

    /**
     * @return non-zero position if caller should call txnCommitSync; -1 if transaction is pending
     */
    long redoDeleteCommitFinal(LocalTransaction txn, long indexId, byte[] key)
        throws IOException
    {
        keyCheck(key);

        RedoWriter redo = txn.mRedo;
        DurabilityMode mode = redo.opWriteCheck(txn.mDurabilityMode);

        if (mode == DurabilityMode.SYNC && txn.mDurabilityMode != DurabilityMode.SYNC) {
            PendingTxn pending = txn.preparePending();
            try {
                acquireRedoLatch();
                try {
                    doRedoDelete(redo, OP_TXN_DELETE_COMMIT_FINAL, pending.mTxnId, indexId, key);
                    redoFlush(true, pending);
                } finally {
                    releaseRedoLatch();
                }
                return -1;
            } catch (Throwable e) {
                throw pending.rollback(e);
            }
        }

        acquireRedoLatch();
        try {
            doRedoDelete(redo, OP_TXN_DELETE_COMMIT_FINAL, txn.mTxnId, indexId, key);
            return redoFlushCommit(mode);
        } finally {
            releaseRedoLatch();
        }
    }

    private void doRedoDelete(RedoWriter redo, byte op, long txnId, long indexId, byte[] key)
        throws IOException
    {
        redoWriteTxnOp(redo, op, txnId);
        redoWriteLongLE(indexId);
        redoWriteUnsignedVarInt(key.length);
        redoWriteBytes(key, true);
        redoWriteTerminator(redo);
    }

    void redoCursorRegister(RedoWriter redo, long cursorId, long indexId) throws IOException {
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

    void redoCursorUnregister(RedoWriter redo, long cursorId) throws IOException {
        redo.opWriteCheck(null);

        acquireRedoLatch();
        try {
            redoWriteTxnOp(redo, OP_CURSOR_UNREGISTER, cursorId);
            redoWriteTerminator(redo);
        } finally {
            releaseRedoLatch();
        }
    }

    void redoCursorStore(RedoWriter redo, long cursorId, long txnId, byte[] key, byte[] value)
        throws IOException
    {
        keyCheck(key);
        redo.opWriteCheck(null);

        acquireRedoLatch();
        try {
            redoWriteCursorOp(redo, OP_CURSOR_STORE, cursorId, txnId);
            redoWriteUnsignedVarInt(key.length);
            redoWriteBytes(key, false);
            redoWriteUnsignedVarInt(value.length);
            redoWriteBytes(value, true);
            redoWriteTerminator(redo);
        } finally {
            releaseRedoLatch();
        }
    }

    void redoCursorDelete(RedoWriter redo, long cursorId, long txnId, byte[] key)
        throws IOException
    {
        keyCheck(key);
        redo.opWriteCheck(null);

        acquireRedoLatch();
        try {
            redoWriteCursorOp(redo, OP_CURSOR_DELETE, cursorId, txnId);
            redoWriteUnsignedVarInt(key.length);
            redoWriteBytes(key, true);
            redoWriteTerminator(redo);
        } finally {
            releaseRedoLatch();
        }
    }

    void redoCursorFind(RedoWriter redo, long cursorId, long txnId, byte[] key)
        throws IOException
    {
        redo.opWriteCheck(null);

        acquireRedoLatch();
        try {
            redoWriteCursorOp(redo, OP_CURSOR_FIND, cursorId, txnId);
            redoWriteUnsignedVarInt(key.length);
            redoWriteBytes(key, true);
            redoWriteTerminator(redo);
        } finally {
            releaseRedoLatch();
        }
    }

    void redoCursorValueSetLength(RedoWriter redo, long cursorId, long txnId, long length)
        throws IOException
    {
        redo.opWriteCheck(null);

        acquireRedoLatch();
        try {
            redoWriteCursorOp(redo, OP_CURSOR_VALUE_SET_LENGTH, cursorId, txnId);
            redoWriteUnsignedVarLong(length);
            redoWriteTerminator(redo);
        } finally {
            releaseRedoLatch();
        }
    }

    void redoCursorValueWrite(RedoWriter redo, long cursorId, long txnId,
                              long pos, byte[] buf, int off, int len)
        throws IOException
    {
        redo.opWriteCheck(null);

        acquireRedoLatch();
        try {
            redoWriteCursorOp(redo, OP_CURSOR_VALUE_WRITE, cursorId, txnId);
            redoWriteUnsignedVarLong(pos);
            redoWriteUnsignedVarInt(len);
            redoWriteBytes(buf, off, len, true);
            redoWriteTerminator(redo);
        } finally {
            releaseRedoLatch();
        }
    }

    void redoCursorValueClear(RedoWriter redo, long cursorId, long txnId, long pos, long length)
        throws IOException
    {
        redo.opWriteCheck(null);

        acquireRedoLatch();
        try {
            redoWriteCursorOp(redo, OP_CURSOR_VALUE_CLEAR, cursorId, txnId);
            redoWriteUnsignedVarLong(pos);
            redoWriteUnsignedVarLong(length);
            redoWriteTerminator(redo);
        } finally {
            releaseRedoLatch();
        }
    }

    /**
     * @param message optional
     */
    void redoPrepare(RedoWriter redo, long txnId, long prepareTxnId,
                     int handlerId, byte[] message, boolean commit)
        throws IOException
    {
        redo.opWriteCheck(null);

        acquireRedoLatch();
        try {
            if (message == null) {
                byte op = commit ? OP_TXN_PREPARE_COMMIT : OP_TXN_PREPARE;
                redoWriteTxnOp(redo, op, txnId);
                redoWriteLongLE(prepareTxnId);
                redoWriteUnsignedVarInt(handlerId);
            } else {
                byte op = commit ? OP_TXN_PREPARE_COMMIT_MESSAGE : OP_TXN_PREPARE_MESSAGE;
                redoWriteTxnOp(redo, op, txnId);
                redoWriteLongLE(prepareTxnId);
                redoWriteUnsignedVarInt(handlerId);
                redoWriteUnsignedVarInt(message.length);
                redoWriteBytes(message, true);
            }
            redoWriteTerminator(redo);
        } finally {
            releaseRedoLatch();
        }
    }

    void redoPrepareRollback(RedoWriter redo, long txnId, long prepareTxnId) throws IOException {
        redo.opWriteCheck(null);

        acquireRedoLatch();
        try {
            redoWriteTxnOp(redo, OP_TXN_PREPARE_ROLLBACK, txnId);
            redoWriteLongLE(prepareTxnId);
            redoWriteTerminator(redo);
        } finally {
            releaseRedoLatch();
        }
    }

    /**
     * Note: This method doesn't flush the log, and so the caller must still explicitly commit
     * the transaction to ensure the log is flushed.
     */
    void redoCommitFinalNotifySchema(RedoWriter redo, long txnId, long indexId) throws IOException {
        acquireRedoLatch();
        try {
            redoWriteTxnOp(redo, OP_TXN_COMMIT_FINAL_NOTIFY_SCHEMA, txnId);
            redoWriteLongLE(indexId);
            redoWriteTerminator(redo);
        } finally {
            releaseRedoLatch();
        }
    }

    void redoPredicateMode(RedoWriter redo, long txnId) throws IOException {
        redo.opWriteCheck(null);

        acquireRedoLatch();
        try {
            redoWriteTxnOp(redo, OP_TXN_PREDICATE_MODE, txnId);
            redoWriteTerminator(redo);
        } finally {
            releaseRedoLatch();
        }
    }

    void redoCustom(RedoWriter redo, long txnId, int handlerId, byte[] message) throws IOException {
        if (message == null) {
            throw new NullPointerException("Message is null");
        }
        redo.opWriteCheck(null);

        acquireRedoLatch();
        try {
            redoWriteTxnOp(redo, OP_TXN_CUSTOM, txnId);
            redoWriteUnsignedVarInt(handlerId);
            redoWriteUnsignedVarInt(message.length);
            redoWriteBytes(message, true);
            redoWriteTerminator(redo);
        } finally {
            releaseRedoLatch();
        }
    }

    void redoCustomLock(RedoWriter redo, long txnId, int handlerId, byte[] message,
                        long indexId, byte[] key)
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
            redoWriteUnsignedVarInt(handlerId);
            redoWriteLongLE(indexId);
            redoWriteUnsignedVarInt(key.length);
            redoWriteBytes(key, false);
            redoWriteUnsignedVarInt(message.length);
            redoWriteBytes(message, true);
            redoWriteTerminator(redo);
        } finally {
            releaseRedoLatch();
        }
    }

    // Caller must hold redo latch.
    void doRedoReset(RedoWriter redo) throws IOException {
        redo.opWriteCheck(null);
        redoWriteOp(redo, OP_RESET);
        redoNonTxnTerminateCommit(redo, DurabilityMode.NO_FLUSH);
        assert mRedoWriterLatched;
        redo.mLastTxnId = 0;
    }

    /**
     * @param op OP_TIMESTAMP, OP_SHUTDOWN, OP_CLOSE, or OP_END_FILE
     */
    void redoTimestamp(RedoWriter redo, byte op) throws IOException {
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
    void doRedoTimestamp(RedoWriter redo, byte op, DurabilityMode mode) throws IOException {
        doRedoOp(redo, op, System.currentTimeMillis(), mode);
    }

    // Caller must hold redo latch.
    void doRedoNopRandom(RedoWriter redo, DurabilityMode mode) throws IOException {
        doRedoOp(redo, OP_NOP_RANDOM, ThreadLocalRandom.current().nextLong(), mode);
    }

    // Caller must hold redo latch.
    private void doRedoOp(RedoWriter redo, byte op, long operand, DurabilityMode mode)
        throws IOException
    {
        redo.opWriteCheck(null);
        redoWriteOp(redo, op, operand);
        redoNonTxnTerminateCommit(redo, mode);
    }

    long redoControl(RedoWriter redo, byte[] message) throws IOException {
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
    private long redoNonTxnTerminateCommit(RedoWriter redo, DurabilityMode mode)
        throws IOException
    {
        mRedoTerminatePos = mRedoPos;

        if (!redo.shouldWriteTerminators()) {
            // Commit the normal way.
            return redoFlushCommit(mode);
        }

        if (mRedoFirstTxnId != 0) {
            // Terminate and commit the normal way.
            redoWriteIntLE(nzHash(mRedoLastTxnId));
            return redoFlushCommit(mode);
        }

        int length = mRedoPos;
        int commitLen = mRedoTerminatePos;
        byte[] buffer = mRedoBuffer;
        redo = latchWriter();

        if (length > buffer.length - 4) {
            // Flush and make room for the terminator.
            try {
                mRedoWriterPos = redo.write(false, buffer, 0, length, commitLen, null);
            } catch (IOException e) {
                throw rethrow(e, redo.mCloseCause);
            }
            mRedoPos = 0;
            mRedoTerminatePos = 0;
            length = 0;
            commitLen = 0;
        }

        // Encode the terminator using the "true" last transaction id.
        Utils.encodeIntLE(buffer, length, nzHash(redo.mLastTxnId));
        length += 4;

        boolean flush = mode == DurabilityMode.SYNC || mode == DurabilityMode.NO_SYNC;

        try {
            mRedoWriterPos = redo.write(flush, buffer, 0, length, commitLen, null);
        } catch (IOException e) {
            throw rethrow(e, redo.mCloseCause);
        }

        mRedoPos = 0;
        mRedoTerminatePos = 0;

        return mode == DurabilityMode.SYNC ? mRedoWriterPos : 0;
    }

    // Caller must hold redo latch.
    private void redoWriteTerminator(RedoWriter redo) throws IOException {
        mRedoTerminatePos = mRedoPos;
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
     * @param term true if writing the last bytes of an operation
     */
    // Caller must hold redo latch.
    private void redoWriteBytes(byte[] bytes, boolean term) throws IOException {
        redoWriteBytes(bytes, 0, bytes.length, term);
    }

    /**
     * @param term true if writing the last bytes of an operation
     */
    // Caller must hold redo latch.
    private void redoWriteBytes(byte[] bytes, int offset, int length, boolean term)
        throws IOException
    {
        if (length == 0) {
            return;
        }

        byte[] buffer = mRedoBuffer;
        int avail = buffer.length - mRedoPos;

        if (avail >= length) {
            if (mRedoPos == 0 && avail == length) {
                RedoWriter redo = latchWriter();
                mRedoWriterPos = write(redo, bytes, offset, length, term);
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
                mRedoWriterPos = write(mRedoWriter, bytes, offset, length, term);
            } else {
                System.arraycopy(bytes, offset, buffer, 0, length);
                mRedoPos = length;
            }
        }
    }

    /**
     * @param redo must be latched
     */
    private static long write(RedoWriter redo, byte[] bytes, int offset, int length, boolean term)
        throws IOException
    {
        try {
            return redo.write(false, bytes, offset, length, term ? length : 0, null);
        } catch (IOException e) {
            throw rethrow(e, redo.mCloseCause);
        }
    }

    /**
     * Write a non-transactional operation. Caller must hold redo latch and always flush the
     * operation. Flushing ensures that transactional operations that follow can encode
     * transaction id deltas correctly.
     */
    private void redoWriteOp(RedoWriter redo, byte op) throws IOException {
        mRedoPos = doRedoWriteOp(redo, op, 1); // 1 for op
    }

    /**
     * Write a non-transactional operation. Caller must hold redo latch and always flush the
     * operation. Flushing ensures that transactional operations that follow can encode
     * transaction id deltas correctly.
     */
    private void redoWriteOp(RedoWriter redo, byte op, long operand) throws IOException {
        int pos = doRedoWriteOp(redo, op, 1 + 8); // 1 for op, 8 for operand
        Utils.encodeLongLE(mRedoBuffer, pos, operand);
        mRedoPos = pos + 8;
    }

    // Caller must hold redo latch.
    private int doRedoWriteOp(RedoWriter redo, byte op, int len) throws IOException {
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
    private void redoWriteTxnOp(RedoWriter redo, byte op, long txnId) throws IOException {
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
    private void redoWriteCursorOp(RedoWriter redo, byte op, long cursorId, long txnId)
        throws IOException
    {
        if (redo != mRedoWriter) {
            switchRedo(redo);
        }

        byte[] buffer = mRedoBuffer;
        int pos = mRedoPos;

        prepare: {
            if (pos > buffer.length - ((1 + 9) << 1)) { // 2 ops and 2 deltas (max length)
                redoFlush(false);
            } else if (pos != 0) {
                buffer[pos] = op;
                pos = Utils.encodeSignedVarLong(buffer, pos + 1, cursorId - mRedoLastTxnId);
                break prepare;
            }
            buffer[0] = op;
            pos = 1 + 9;  // 1 for op, and reserve 9 for txn delta (cursorId actually)
            mRedoFirstTxnId = cursorId;
        }

        mRedoPos = Utils.encodeSignedVarLong(buffer, pos, txnId - cursorId);
        mRedoLastTxnId = txnId;
    }

    /**
     * Flush redo buffer to the current RedoWriter.
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
    private void switchRedo(RedoWriter redo) throws IOException {
        try {
            redoFlush(false);
        } catch (UnmodifiableReplicaException e) {
            // Terminal state, so safe to discard everything.
            mRedoPos = 0;
            mRedoTerminatePos = 0;
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
            redoFlush(mode == DurabilityMode.NO_SYNC); // ignore flush for NO_FLUSH, etc.
            return 0;
        }
    }

    /**
     * Caller must hold redo latch and ensure that mRedoWriter is set.
     *
     * @param full true to fully flush through all buffers (used for SYNC/NO_SYNC commit)
     */
    private void redoFlush(boolean full) throws IOException {
        redoFlush(full, null);
    }

    private void redoFlush(boolean full, PendingTxn pending) throws IOException {
        int length = mRedoPos;
        if (length == 0) {
            return;
        }

        int commitLen = mRedoTerminatePos;
        byte[] buffer = mRedoBuffer;
        int offset = 0;
        RedoWriter redo = latchWriter();

        final long redoWriterLastTxnId = redo.mLastTxnId;

        if (mRedoFirstTxnId != 0) {
            // Encode the first transaction delta and shift the opcode.
            long delta = convertSignedVarLong(mRedoFirstTxnId - redoWriterLastTxnId);
            int varLen = calcUnsignedVarLongLength(delta);
            offset = (1 + 9) - varLen;
            encodeUnsignedVarLong(buffer, offset, delta);
            buffer[--offset] = buffer[0];
            length -= offset;
            commitLen -= offset;
            // Must always set before write is called, so that it can see the update.
            redo.mLastTxnId = mRedoLastTxnId;
        }

        try {
            try {
                mRedoWriterPos = redo.write(full, buffer, offset, length, commitLen, pending);
            } catch (IOException e) {
                throw rethrow(e, redo.mCloseCause);
            }
        } catch (Throwable e) {
            // Rollback.
            redo.mLastTxnId = redoWriterLastTxnId;
            throw e;
        }

        mRedoPos = 0;
        mRedoTerminatePos = 0;
        mRedoFirstTxnId = 0;
    }

    // Caller must hold redo latch.
    private RedoWriter latchWriter() {
        RedoWriter redo = mRedoWriter;
        if (!mRedoWriterLatched) {
            redo.acquireExclusive();
            mRedoWriterLatched = true;
        }
        return redo;
    }

    /**
     * Caller must hold db commit lock.
     */
    synchronized void register(UndoLog log) {
        UndoLog top = mTopUndoLog;
        if (top != null) {
            log.mPrev = top;
            top.mNext = log;
        }
        mTopUndoLog = log;
        mUndoLogCount++;
    }

    /**
     * Should only be called after all log entries have been truncated or rolled back. Caller
     * does not need to hold db commit lock.
     */
    synchronized void unregister(UndoLog log) {
        UndoLog prev = log.mPrev;
        UndoLog next = log.mNext;
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
     * Called for transactions which committed optimistically by writing to the undo log, but
     * the redo confirmation failed and the transaction needs to rollback.
     */
    synchronized void uncommitted(long txnId) {
        if (mUncommitted == null) {
            mUncommitted = new LHashSet(4);
        }
        mUncommitted.add(txnId);
    }

    /**
     * Checks if any of the given transaction ids match transactions which are still registered
     * and have an optimistic commit state.
     *
     * @param committed set with transaction id keys
     * @return true if at least one of the given transactions is active
     */
    synchronized boolean anyActive(LHashSet committed) {
        for (UndoLog log = mTopUndoLog; log != null; log = log.mPrev) {
            if (committed.contains(log.mTxnId) && log.isCommitted()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Moves any uncommitted transactions into the given set.
     *
     * @param dest set with transaction id keys; can be null initially
     * @return exiting or new set
     */
    synchronized LHashSet moveUncommitted(LHashSet dest) {
        final LHashSet uncommitted = mUncommitted;

        if (uncommitted != null) {
            mUncommitted = null;
            if (dest == null) {
                return uncommitted;
            }
            dest.addAll(uncommitted);
        }

        return dest;
    }

    /**
     * Clears the set of uncommitted transactions, freeing memory. Caller must synchronize on
     * this context object.
     */
    void clearUncommitted() {
        mUncommitted = null;
    }

    /**
     * Gather all the ids for transactions which have an optimistic committed state. Returns
     * null if none.
     *
     * @param dest set with transaction id keys; can be null initially
     * @return exiting or new set
     */
    synchronized LHashSet gatherCommitted(LHashSet dest) {
        for (UndoLog log = mTopUndoLog; log != null; log = log.mPrev) {
            if (log.isCommitted()) {
                if (dest == null) {
                    dest = new LHashSet(4);
                }
                dest.add(log.mTxnId);
            }
        }
        return dest;
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
    byte[] writeToMaster(UndoLog master, byte[] workspace) throws IOException {
        for (UndoLog log = mTopUndoLog; log != null; log = log.mPrev) {
            workspace = log.writeToMaster(master, workspace);
        }
        return workspace;
    }

    /**
     * Deletes any UndoLog instances, as part of database close sequence. Caller must hold
     * exclusive db commit lock.
     */
    synchronized void deleteUndoLogs() {
        for (UndoLog log = mTopUndoLog; log != null; log = log.mPrev) {
            log.delete();
        }
        mTopUndoLog = null;
    }
}
