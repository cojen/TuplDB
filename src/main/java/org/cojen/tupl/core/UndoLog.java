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

import java.io.IOException;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;

import static java.lang.System.arraycopy;

import org.cojen.tupl.ClosedIndexException;
import org.cojen.tupl.CorruptDatabaseException;
import org.cojen.tupl.Cursor;
import org.cojen.tupl.DatabaseException;
import org.cojen.tupl.Index;
import org.cojen.tupl.LockFailureException;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.diag.EventListener;
import org.cojen.tupl.diag.EventType;

import static org.cojen.tupl.core.PageOps.*;
import static org.cojen.tupl.core.Utils.*;

/**
 * Specialized stack used to record compensating actions for rolling back transactions. UndoLog
 * instances are created on a per-transaction basis -- they're not shared.
 *
 * @author Brian S O'Neill
 */
final class UndoLog implements DatabaseAccess {
    // Linked list of UndoLogs registered with a TransactionContext.
    UndoLog mPrev;
    UndoLog mNext;

    /*
      UndoLog is persisted in Nodes. All multibyte types are little endian encoded.

      +----------------------------------------+
      | byte:   node type                      |  header
      | byte:   reserved (must be 0)           |
      | ushort: pointer to top entry           |
      | ulong:  lower node id                  |
      +----------------------------------------+
      | free space                             |
      -                                        -
      |                                        |
      +----------------------------------------+
      | log stack entries                      |
      -                                        -
      |                                        |  tail
      +----------------------------------------+

      Stack entries are encoded from the tail end of the node towards the
      header. Entries without payloads are encoded with an opcode less than 16.
      All other types of entries are composed of three sections:

      +----------------------------------------+
      | byte:   opcode                         |
      | varint: payload length                 |
      | n:      payload                        |
      +----------------------------------------+

      Popping entries off the stack involves reading the opcode and moving
      forwards. Payloads which don't fit into the node spill over into the
      lower node(s).
    */

    static final int I_LOWER_NODE_ID = 4;
    private static final int HEADER_SIZE = 12;

    // Must be power of two.
    private static final int INITIAL_BUFFER_SIZE = 128;

    private static final byte OP_SCOPE_ENTER = 1;
    private static final byte OP_SCOPE_COMMIT = 2;

    // Same as OP_UNINSERT, except uses OP_ACTIVE_KEY. (ValueAccessor op)
    static final byte OP_UNCREATE = 12;

    // All ops less than 16 have no payload.
    private static final byte PAYLOAD_OP = 16;

    // Copy to another log from master log. Payload is transaction id, active
    // index id, buffer size (short type), and serialized buffer.
    private static final byte OP_LOG_COPY = 16;

    // Reference to another log from master log. Payload is transaction id,
    // active index id, length, node id, and top entry offset.
    private static final byte OP_LOG_REF = 17;

    // Payload is active index id.
    private static final byte OP_INDEX = 18;

    // Payload is key to delete to undo an insert.
    static final byte OP_UNINSERT = 19;

    // Payload is Node-encoded key/value entry to store, to undo an update.
    static final byte OP_UNUPDATE = 20;

    // Payload is Node-encoded key/value entry to store, to undo a delete.
    static final byte OP_UNDELETE = 21;

    // Payload is Node-encoded key and trash id, to undo a fragmented value delete.
    static final byte OP_UNDELETE_FRAGMENTED = 22;

    // Payload is a key for ValueAccessor operations.
    static final byte OP_ACTIVE_KEY = 23;

    // Payload is custom handler id and message.
    static final byte OP_CUSTOM = 24;

    private static final int LK_ADJUST = 5;

    // Payload is a (large) key and value to store, to undo an update.
    static final byte OP_UNUPDATE_LK = OP_UNUPDATE + LK_ADJUST; //25

    // Payload is a (large) key and value to store, to undo a delete.
    static final byte OP_UNDELETE_LK = OP_UNDELETE + LK_ADJUST; //26

    // Payload is a (large) key and trash id, to undo a fragmented value delete.
    static final byte OP_UNDELETE_LK_FRAGMENTED = OP_UNDELETE_FRAGMENTED + LK_ADJUST; //27

    // Payload is the value length to undo a value extension. (ValueAccessor op)
    static final byte OP_UNEXTEND = 29;

    // Payload is the value length and position to undo value hole fill. (ValueAccessor op)
    static final byte OP_UNALLOC = 30;

    // Payload is the value position and bytes to undo a value write. (ValueAccessor op)
    static final byte OP_UNWRITE = 31;

    // Copy to a committed log from master log. Payload is transaction id, active index id,
    // buffer size (short type), and serialized buffer.
    private static final byte OP_LOG_COPY_C = 32;

    // Reference to a committed log from master log. Payload is transaction id, active index
    // id, length, node id, and top entry offset.
    private static final byte OP_LOG_REF_C = 33;

    // Payload is key to delete to recover an exclusive lock.
    static final byte OP_LOCK_EXCLUSIVE = 34;

    // Payload is key to delete to recover an upgradable lock.
    static final byte OP_LOCK_UPGRADABLE = 35;

    // Payload is key to delete to undo an insert. No lock is acquired.
    static final byte OP_UNPREPARE = 36;

    // Payload is handler id and optional message, to tag undelete a prepared transaction
    // entry. No lock is acquired.
    static final byte OP_PREPARED = 37;

    // Same encoding as OP_PREPARED.
    static final byte OP_PREPARED_COMMIT = 38;

    // Payload is key to insert a ghost to undo a prepared transaction rollback. No lock is
    // acquired.
    static final byte OP_PREPARED_UNROLLBACK = 39;

    private final LocalDatabase mDatabase;
    final long mTxnId;

    // Number of bytes currently pushed into log.
    private long mLength;

    // Except for mLength, all field modifications during normal usage must be
    // performed while holding shared db commit lock. See writeToMaster method.

    private byte[] mBuffer;
    private int mBufferPos;

    // Top node, if required. Nodes are not used for logs which fit into local buffer.
    private Node mNode;
    private int mNodeTopPos;

    private long mActiveIndexId;

    // Active key is used for ValueAccessor operations.
    private byte[] mActiveKey;

    private int mCommitted;

    private static final VarHandle cCommittedHandle;

    static {
        try {
            cCommittedHandle =
                MethodHandles.lookup().findVarHandle(UndoLog.class, "mCommitted", int.class);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    UndoLog(LocalDatabase db, long txnId) {
        mDatabase = db;
        mTxnId = txnId;
    }

    @Override
    public LocalDatabase getDatabase() {
        return mDatabase;
    }

    /**
     * Ensures all entries are stored in persistable nodes, unless the log is empty. Caller
     * must hold db commit lock.
     *
     * @return top node id or 0 if log is empty
     */
    long persistReady() throws IOException {
        Node node = mNode;

        if (node != null) {
            node.acquireExclusive();
            try {
                mDatabase.markUnmappedDirty(node);
            } catch (Throwable e) {
                node.releaseExclusive();
                throw e;
            }
        } else {
            if (mLength == 0) {
                return 0;
            }
            // Note: Buffer cannot be null if length is non-zero.
            byte[] buffer = mBuffer;
            int pos = mBufferPos;
            int size = buffer.length - pos;
            mNode = node = allocUnevictableNode(0);
            long pageAddr = node.mPageAddr;
            int newPos = pageSize() - size;
            p_copy(buffer, pos, pageAddr, newPos, size);
            // Set pointer to top entry.
            mNodeTopPos = newPos;
            mBuffer = null;
            mBufferPos = 0;
        }

        node.undoTop(mNodeTopPos);
        node.releaseExclusive();

        return mNode.id();
    }

    private int pageSize() {
        return mDatabase.pageSize();
    }

    /**
     * Deletes just the top node, as part of database close sequence. Caller must hold
     * exclusive db commit lock.
     */
    void delete() {
        mLength = 0;
        mBufferPos = 0;
        mBuffer = null;
        Node node = mNode;
        if (node != null) {
            mNode = null;
            node.delete(mDatabase);
        }
    }

    /**
     * Called by LocalTransaction with db commit lock held.
     */
    void commit() {
        mCommitted = OP_LOG_COPY_C - OP_LOG_COPY;
    }

    /**
     * Called by LocalTransaction, from the thread which currently owns the transaction.
     */
    void uncommit() {
        cCommittedHandle.setRelease(this, 0);
    }

    /**
     * Called by a thread other than the one which currently owns the transaction. This state
     * should be examined after acquiring the exclusive db commit lock.
     */
    boolean isCommitted() {
        // Note the use of acquire/release mode. This allows a call to uncommit to be observed
        // without acquiring the db commit lock again.
        return ((int) cCommittedHandle.getAcquire(this)) != 0;
    }

    /**
     * If the transaction was committed and truncates the log. Later when locks are released,
     * any ghosts are deleted.
     *
     * @return true if transaction was committed
     */
    boolean recoveryCleanup() throws IOException {
        if (mCommitted == 0) {
            return false;
        } else {
            // Transaction was actually committed, but redo log is gone. This can happen when a
            // checkpoint completes in the middle of the transaction commit operation.
            truncate();
            return true;
        }
    }

    /**
     * Caller must hold db commit lock.
     */
    final void pushUninsert(final long indexId, byte[] key) throws IOException {
        setActiveIndexId(indexId);
        doPush(OP_UNINSERT, key);
    }

    /**
     * Push an operation with a Node-encoded key and value, which might be fragmented. Caller
     * must hold db commit lock.
     *
     * @param op OP_UNUPDATE, OP_UNDELETE or OP_UNDELETE_FRAGMENTED
     */
    final void pushNodeEncoded(final long indexId, byte op, byte[] payload, int off, int len)
        throws IOException
    {
        setActiveIndexId(indexId);

        if ((payload[off] & 0xc0) == 0xc0) {
            // Key is fragmented and cannot be stored as-is, so expand it fully and switch to
            // using the "LK" op variant.
            long copyAddr = p_transfer(payload);
            try {
                payload = Node.expandKeyAtLoc
                    (this, copyAddr, off, len, op != OP_UNDELETE_FRAGMENTED);
            } finally {
                p_delete(copyAddr);
            }
            off = 0;
            len = payload.length;
            op += LK_ADJUST;
        }

        doPush(op, payload, off, len);
    }

    /**
     * Push an operation with a Node-encoded key and value, which might be fragmented. Caller
     * must hold db commit lock.
     *
     * @param op OP_UNUPDATE, OP_UNDELETE or OP_UNDELETE_FRAGMENTED
     */
    final void pushNodeEncoded(long indexId, byte op, long payloadAddr, int off, int len)
        throws IOException
    {
        setActiveIndexId(indexId);
    
        byte[] payload;
        if ((p_byteGet(payloadAddr, off) & 0xc0) == 0xc0) {
            // Key is fragmented and cannot be stored as-is, so expand it fully and
            // switch to using the "LK" op variant.
            payload = Node.expandKeyAtLoc
                (this, payloadAddr, off, len, op != OP_UNDELETE_FRAGMENTED);
            op += LK_ADJUST;
        } else {
            payload = new byte[len];
            p_copy(payloadAddr, off, payload, 0, len);
        }
    
        doPush(op, payload);
    }

    private void setActiveIndexId(long indexId) throws IOException {
        long activeIndexId = mActiveIndexId;
        if (indexId != activeIndexId) {
            if (activeIndexId != 0) {
                var payload = new byte[8];
                encodeLongLE(payload, 0, activeIndexId);
                doPush(OP_INDEX, payload, 0, 8, 1);
            }
            mActiveIndexId = indexId;
        }
    }

    /**
     * Caller must hold db commit lock.
     */
    void pushCustom(int handlerId, byte[] message) throws IOException {
        var payload = new byte[calcUnsignedVarIntLength(handlerId) + message.length];
        int pos = encodeUnsignedVarInt(payload, 0, handlerId);
        arraycopy(message, 0, payload, pos, message.length);
        doPush(OP_CUSTOM, payload);
    }

    /**
     * Caller must hold db commit lock.
     */
    void pushUncreate(long indexId, byte[] key) throws IOException {
        setActiveIndexIdAndKey(indexId, key);
        doPush(OP_UNCREATE);
    }

    /**
     * Caller must hold db commit lock.
     *
     * @param savepoint used to check if op isn't necessary
     */
    void pushUnextend(long savepoint, long indexId, byte[] key, long length) throws IOException {
        if (setActiveIndexIdAndKey(indexId, key) && savepoint < mLength) discardCheck: {
            // Check if op isn't necessary because it's action will be superseded by another.

            long unlen;

            Node node = mNode;
            if (node == null) {
                byte op = mBuffer[mBufferPos];
                if (op == OP_UNCREATE) {
                    return;
                }
                if (op != OP_UNEXTEND) {
                    break discardCheck;
                }
                int pos = mBufferPos + 1;
                long decoded = decodeUnsignedVarInt(mBuffer, pos);
                int payloadLen = (int) decoded;
                pos = (int) (decoded >> 32);
                var offsetRef = new IntegerRef.Value();
                offsetRef.value = pos;
                unlen = decodeUnsignedVarLong(mBuffer, offsetRef);
            } else {
                byte op = p_byteGet(mNode.mPageAddr, mNodeTopPos);
                if (op == OP_UNCREATE) {
                    return;
                }
                if (op != OP_UNEXTEND) {
                    break discardCheck;
                }
                int pos = mNodeTopPos + 1;
                long decoded = p_uintGetVar(mNode.mPageAddr, pos);
                int payloadLen = (int) decoded;
                pos = (int) (decoded >> 32);
                if (pos + payloadLen > pageSize()) {
                    // Don't bother decoding payload which spills into the next node.
                    break discardCheck;
                }
                var offsetRef = new IntegerRef.Value();
                offsetRef.value = pos;
                unlen = p_ulongGetVar(mNode.mPageAddr, offsetRef);
            }

            if (unlen <= length) {
                // Existing unextend length will truncate at least as much.
                return;
            }
        }

        var payload = new byte[9];
        int off = encodeUnsignedVarLong(payload, 0, length);
        doPush(OP_UNEXTEND, payload, 0, off);
    }

    /**
     * Caller must hold db commit lock.
     */
    void pushUnalloc(long indexId, byte[] key, long pos, long length) throws IOException {
        setActiveIndexIdAndKey(indexId, key);
        var payload = new byte[9 + 9];
        int off = encodeUnsignedVarLong(payload, 0, length);
        off = encodeUnsignedVarLong(payload, off, pos);
        doPush(OP_UNALLOC, payload, 0, off);
    }

    /**
     * Caller must hold db commit lock.
     */
    void pushUnwrite(long indexId, byte[] key, long pos, byte[] b, int off, int len)
        throws IOException
    {
        setActiveIndexIdAndKey(indexId, key);
        int pLen = calcUnsignedVarLongLength(pos);
        int varIntLen = calcUnsignedVarIntLength(pLen + len);
        doPush(OP_UNWRITE, b, off, len, varIntLen, pLen);

        // Now encode the pos in the reserved region before the payload.
        Node node = mNode;
        int posOff = 1 + varIntLen;
        if (node != null) {
            p_ulongPutVar(node.mPageAddr, mNodeTopPos + posOff, pos);
        } else {
            encodeUnsignedVarLong(mBuffer, mBufferPos + posOff, pos);
        }
    }

    /**
     * Caller must hold db commit lock.
     */
    void pushUnwrite(long indexId, byte[] key, long pos, long addr, int off, int len)
        throws IOException
    {
        var b = new byte[len];
        p_copy(addr, off, b, 0, len);
        pushUnwrite(indexId, key, pos, b, 0, len);
    }

    /**
     * Caller must hold db commit lock.
     */
    final void pushLock(byte op, long indexId, byte[] key) throws IOException {
        setActiveIndexId(indexId);
        doPush(op, key);
    }

    /**
     * Caller must hold db commit lock.
     */
    final void pushUnprepare(byte[] key) throws IOException {
        doPush(OP_UNPREPARE, key);
    }

    /**
     * Caller must hold db commit lock.
     */
    final void pushPrepared(int handlerId, byte[] message, boolean commit) throws IOException {
        int handlerLen = calcUnsignedVarIntLength(handlerId);

        byte[] payload;
        if (message == null) {
            payload = new byte[handlerLen];
        } else {
            // A zero byte in between the handler id and message indicates it's non-null.
            int offset = handlerLen + 1;
            payload = new byte[offset + message.length];
            arraycopy(message, 0, payload, offset, message.length);
        }

        encodeUnsignedVarInt(payload, 0, handlerId);

        doPush(commit ? OP_PREPARED_COMMIT : OP_PREPARED, payload);
    }

    /**
     * Caller must hold db commit lock.
     */
    final void pushPreparedUnrollback(byte[] key) throws IOException {
        doPush(OP_PREPARED_UNROLLBACK, key);
    }

    /**
     * @return true if active index and key already match
     */
    private boolean setActiveIndexIdAndKey(long indexId, byte[] key) throws IOException {
        boolean result = true;

        long activeIndexId = mActiveIndexId;
        if (indexId != activeIndexId) {
            if (activeIndexId != 0) {
                var payload = new byte[8];
                encodeLongLE(payload, 0, activeIndexId);
                doPush(OP_INDEX, payload, 0, 8, 1);
            }
            mActiveIndexId = indexId;
            result = false;
        }

        byte[] activeKey = mActiveKey;
        if (!Arrays.equals(key, activeKey)) {
            if (activeKey != null) {
                doPush(OP_ACTIVE_KEY, mActiveKey);
            }
            mActiveKey = key;
            result = false;
        }

        return result;
    }

    /**
     * Caller must hold db commit lock.
     */
    private void doPush(final byte op) throws IOException {
        doPush(op, EMPTY_BYTES, 0, 0, 0);
    }

    /**
     * Caller must hold db commit lock.
     */
    private void doPush(final byte op, final byte[] payload) throws IOException {
        doPush(op, payload, 0, payload.length);
    }

    /**
     * Caller must hold db commit lock.
     */
    private void doPush(final byte op, final byte[] payload, final int off, final int len)
        throws IOException
    {
        doPush(op, payload, off, len, calcUnsignedVarIntLength(len), 0);
    }

    /**
     * Caller must hold db commit lock.
     */
    private void doPush(final byte op, final byte[] payload, final int off, final int len,
                        final int varIntLen)
        throws IOException
    {
        doPush(op, payload, off, len, varIntLen, 0);
    }

    /**
     * Caller must hold db commit lock.
     *
     * @param pLen space to reserve before the payload; must be accounted for in varIntLen
     */
    private void doPush(final byte op, final byte[] payload, final int off, final int len,
                        final int varIntLen, final int pLen)
        throws IOException
    {
        final int encodedLen = 1 + varIntLen + pLen + len;

        Node node = mNode;
        if (node != null) {
            // Push into allocated node, which must be marked dirty.
            node.acquireExclusive();
            try {
                mDatabase.markUnmappedDirty(node);
            } catch (Throwable e) {
                node.releaseExclusive();
                throw e;
            }
        } else quick: {
            // Try to push into a local buffer before allocating a node.
            byte[] buffer = mBuffer;
            int pos;
            if (buffer == null) {
                int newCap = Math.max(INITIAL_BUFFER_SIZE, roundUpPower2(encodedLen));
                int pageSize = pageSize();
                if (newCap <= (pageSize >> 1)) {
                    mBuffer = buffer = new byte[newCap];
                    mBufferPos = pos = newCap;
                } else {
                    // Required capacity is large, so just use a node.
                    mNode = node = allocUnevictableNode(0);
                    // Set pointer to top entry (none at the moment).
                    mNodeTopPos = pageSize;
                    break quick;
                }
            } else {
                pos = mBufferPos;
                if (pos < encodedLen) {
                    final int size = buffer.length - pos;
                    int newCap = roundUpPower2(encodedLen + size);
                    if (newCap < 0) {
                        newCap = Integer.MAX_VALUE;
                    } else {
                        newCap = Math.max(buffer.length << 1, newCap);
                    }
                    if (newCap <= (pageSize() >> 1)) {
                        var newBuf = new byte[newCap];
                        int newPos = newCap - size;
                        arraycopy(buffer, pos, newBuf, newPos, size);
                        mBuffer = buffer = newBuf;
                        mBufferPos = pos = newPos;
                    } else {
                        // Required capacity is large, so just use a node.
                        mNode = node = allocUnevictableNode(0);
                        long pageAddr = node.mPageAddr;
                        int newPos = pageSize() - size;
                        p_copy(buffer, pos, pageAddr, newPos, size);
                        // Set pointer to top entry.
                        mNodeTopPos = newPos;
                        mBuffer = null;
                        mBufferPos = 0;
                        break quick;
                    }
                }
            }

            pos -= encodedLen;
            buffer[pos] = op;
            if (op >= PAYLOAD_OP) {
                int payloadPos = encodeUnsignedVarInt(buffer, pos + 1, pLen + len) + pLen;
                arraycopy(payload, off, buffer, payloadPos, len);
            }
            mBufferPos = pos;
            mLength += encodedLen;
            return;
        }

        int pos = mNodeTopPos;
        int available = pos - HEADER_SIZE;
        if (available >= encodedLen) {
            pos -= encodedLen;
            long pageAddr = node.mPageAddr;
            p_bytePut(pageAddr, pos, op);
            if (op >= PAYLOAD_OP) {
                int payloadPos = p_uintPutVar(pageAddr, pos + 1, pLen + len) + pLen;
                p_copy(payload, off, pageAddr, payloadPos, len);
            }
            node.releaseExclusive();
            mNodeTopPos = pos;
            mLength += encodedLen;
            return;
        }

        // Payload doesn't fit into node, so break it up.
        int remaining = len;

        while (true) {
            int amt = Math.min(available, remaining);
            pos -= amt;
            available -= amt;
            remaining -= amt;
            long pageAddr = node.mPageAddr;
            p_copy(payload, off + remaining, pageAddr, pos, amt);

            if (remaining <= 0 && available >= (encodedLen - len)) {
                if (varIntLen > 0) {
                    p_uintPutVar(pageAddr, pos -= varIntLen + pLen, pLen + len);
                }
                p_bytePut(pageAddr, --pos, op);
                node.releaseExclusive();
                break;
            }

            Node newNode;
            try {
                newNode = allocUnevictableNode(node.id());
            } catch (Throwable e) {
                // Undo the damage.
                while (node != mNode) {
                    node = popNode(node, true);
                }
                node.releaseExclusive();
                throw e;
            }

            node.undoTop(pos);
            mDatabase.nodeMapPut(node);
            node.releaseExclusive();
            node.makeEvictable();

            node = newNode;
            pos = pageSize();
            available = pos - HEADER_SIZE;
        }

        mNode = node;
        mNodeTopPos = pos;
        mLength += encodedLen;
    }

    /**
     * Return a savepoint that can be passed to scopeRollback.
     */
    final long savepoint() {
        return mLength;
    }

    /**
     * Caller does not need to hold db commit lock.
     *
     * @return savepoint
     */
    final long scopeEnter() throws IOException {
        final CommitLock.Shared shared = mDatabase.commitLock().acquireShared();
        try {
            long savepoint = mLength;
            doScopeEnter();
            return savepoint;
        } finally {
            shared.release();
        }
    }

    /**
     * Caller must hold db commit lock.
     */
    final void doScopeEnter() throws IOException {
        doPush(OP_SCOPE_ENTER);
    }

    /**
     * Caller does not need to hold db commit lock.
     *
     * @return savepoint
     */
    final long scopeCommit() throws IOException {
        final CommitLock.Shared shared = mDatabase.commitLock().acquireShared();
        try {
            doPush(OP_SCOPE_COMMIT);
            return mLength;
        } finally {
            shared.release();
        }
    }

    /**
     * Rollback all log entries to the given savepoint. Pass zero to rollback
     * everything. Caller does not need to hold db commit lock.
     */
    final void scopeRollback(long savepoint) throws IOException {
        final CommitLock.Shared shared = mDatabase.commitLock().acquireShared();
        try {
            if (savepoint < mLength) {
                // Rollback the entire scope, including the enter op.
                doRollback(savepoint);
            }
        } finally {
            shared.release();
        }
    }

    /**
     * Truncate all log entries. Caller does not need to hold db commit lock.
     */
    final void truncate() throws IOException {
        if (mLength <= 0) {
            return;
        }

        final CommitLock commitLock = mDatabase.commitLock();
        CommitLock.Shared shared = commitLock.acquireShared();
        try {
            Node node = mNode;
            if (node == null) {
                mBufferPos = mBuffer.length;
            } else {
                node.acquireExclusive();
                while (true) {
                    try {
                        if ((node = popNode(node, true)) == null) {
                            break;
                        }
                    } catch (Throwable e) {
                        // Caller will release the commit lock, and so these fields must be
                        // cleared. See comments below.
                        mNodeTopPos = 0;
                        mActiveKey = null;
                        throw e;
                    }

                    if (commitLock.hasQueuedThreads()) {
                        // Release and re-acquire, to unblock any threads waiting for
                        // checkpoint to begin. In case the checkpoint writes out the node(s)
                        // before truncation finishes, use a top position of zero to indicate
                        // that recovery should simply complete the truncation. A position of
                        // zero within the node is otherwise illegal, since it refers to the
                        // header, which doesn't contain undo operations.
                        mNodeTopPos = 0;
                        // Clear this early, to prevent writeToMaster from attempting to push
                        // anything to the log. The key isn't needed to complete truncation.
                        mActiveKey = null;
                        shared.release();
                        commitLock.acquireShared(shared);
                    }
                }
            }
            mLength = 0;
            mActiveIndexId = 0;
            mActiveKey = null;
        } finally {
            shared.release();
        }
    }

    /**
     * Truncate all master undo log entries. Caller does not need to hold db commit lock. This
     * method is different from the regular truncate method by checking if the database has
     * been closed, and by never checking for commit lock waiters.
     */
    final void truncateMaster() throws IOException {
        final CommitLock commitLock = mDatabase.commitLock();
        CommitLock.Shared shared = commitLock.acquireShared();
        try {
            if (mDatabase.isClosed()) {
                delete();
            } else {
                Node node = mNode;
                if (node == null) {
                    mBufferPos = mBuffer.length;
                } else {
                    node.acquireExclusive();
                    while (true) {
                        try {
                            if ((node = popNode(node, true)) == null) {
                                break;
                            }
                        } catch (Throwable e) {
                            mNodeTopPos = 0;
                            mActiveKey = null;
                            throw e;
                        }
                    }
                }
                mLength = 0;
                mActiveIndexId = 0;
                mActiveKey = null;
            }
        } finally {
            shared.release();
        }
    }

    /**
     * Rollback all log entries, and then discard this UndoLog object. Caller does not need to
     * hold db commit lock.
     */
    final void rollback() throws IOException {
        if (mLength == 0) {
            // Nothing to rollback, so return quickly.
            return;
        }

        final CommitLock.Shared shared = mDatabase.commitLock().acquireShared();
        try {
            mCommitted = 0;
            doRollback(0);
        } finally {
            shared.release();
        }
    }

    /**
     * @param savepoint must be less than mLength
     */
    private void doRollback(long savepoint) throws IOException {
        new PopAll() {
            Index activeIndex;

            @Override
            public boolean accept(byte op, byte[] entry) throws IOException {
                activeIndex = undo(activeIndex, op, entry);
                return true;
            }
        }.go(true, savepoint);
    }

    /**
     * RTP: "Rollback To Prepare" helper class.
     */
    class RTP extends IOException implements Popper {
        private Index activeIndex;

        // Decoded recovery handler and message.
        int handlerId;
        byte[] message;

        // True if prepareCommit was called on the transaction.
        boolean commit;

        @Override
        public boolean accept(byte op, byte[] entry) throws IOException {
            if (op == OP_PREPARED || op == OP_PREPARED_COMMIT) {
                long decoded = decodeUnsignedVarInt(entry, 0);
                handlerId = (int) decoded;
                int messageLoc = ((int) (decoded >> 32)) + 1;
                if (messageLoc <= entry.length) {
                    message = Arrays.copyOfRange(entry, messageLoc, entry.length);
                }
                if (op == OP_PREPARED_COMMIT) {
                    commit = true;
                }
                // Found the prepare operation, but don't pop it.
                throw this;
            }

            activeIndex = undo(activeIndex, op, entry);
            return true;
        }

        // Disable stack trace capture, since it's not required.
        @Override
        public Throwable fillInStackTrace() {
            return this;
        }
    }

    final RTP rollbackToPrepare() throws IOException {
        RTP rtp = new RTP();

        try {
            while (pop(true, rtp));
            throw new IllegalStateException("Prepare operation not found");
        } catch (RTP r) {
            // Expected.
        }

        return rtp;
    }

    /**
     * @param activeIndex active index, possibly null
     * @param op undo op
     * @return new active index, possibly null
     */
    private Index undo(Index activeIndex, byte op, byte[] entry) throws IOException {
        switch (op) {
        default:
            throw new DatabaseException("Unknown undo log entry type: " + op);

        case OP_SCOPE_ENTER:
        case OP_SCOPE_COMMIT:
        case OP_LOCK_EXCLUSIVE:
        case OP_LOCK_UPGRADABLE:
            // Only needed by recovery.
            break;

        case OP_INDEX:
            mActiveIndexId = decodeLongLE(entry, 0);
            activeIndex = null;
            break;

        case OP_UNCREATE:
            activeIndex = doUndo(activeIndex, ix -> ix.delete(Transaction.BOGUS, mActiveKey));
            break;

        case OP_UNINSERT:
            activeIndex = doUndo(activeIndex, ix -> ix.delete(Transaction.BOGUS, entry));
            break;

        case OP_UNUPDATE:
        case OP_UNDELETE: {
            byte[][] pair = decodeNodeKeyValuePair(entry);
            activeIndex = doUndo(activeIndex, ix -> ix.store(Transaction.BOGUS, pair[0], pair[1]));
            break;
        }

        case OP_UNUPDATE_LK:
        case OP_UNDELETE_LK: {
            long decoded = decodeUnsignedVarInt(entry, 0);
            var key = new byte[(int) decoded];
            int keyLoc = (int) (decoded >> 32);
            arraycopy(entry, keyLoc, key, 0, key.length);

            int valueLoc = keyLoc + key.length;
            byte[] value = new byte[entry.length - valueLoc];
            arraycopy(entry, valueLoc, value, 0, value.length);

            activeIndex = doUndo(activeIndex, ix -> ix.store(Transaction.BOGUS, key, value));
            break;
        }

        case OP_UNDELETE_FRAGMENTED:
            activeIndex = doUndo(activeIndex, ix -> {
                FragmentedTrash.remove(mDatabase.fragmentedTrash(), mTxnId, (BTree) ix, entry);
            });
            break;

        case OP_UNDELETE_LK_FRAGMENTED: {
            long decoded = decodeUnsignedVarInt(entry, 0);
            var key = new byte[(int) decoded];
            int keyLoc = (int) (decoded >> 32);
            arraycopy(entry, keyLoc, key, 0, key.length);

            int tidLoc = keyLoc + key.length;
            int tidLen = entry.length - tidLoc;
            var trashKey = new byte[8 + tidLen];
            encodeLongBE(trashKey, 0, mTxnId);
            arraycopy(entry, tidLoc, trashKey, 8, tidLen);

            activeIndex = doUndo(activeIndex, ix -> {
                FragmentedTrash.remove(mDatabase.fragmentedTrash(), (BTree) ix, key, trashKey);
            });
            break;
        }

        case OP_CUSTOM:
            long decoded = decodeUnsignedVarInt(entry, 0);
            int handlerId = (int) decoded;
            int messageLoc = (int) (decoded >> 32);
            var message = new byte[entry.length - messageLoc];
            arraycopy(entry, messageLoc, message, 0, message.length);
            mDatabase.findCustomRecoveryHandler(handlerId).undo(null, message);
            break;

        case OP_ACTIVE_KEY:
            mActiveKey = entry;
            break;

        case OP_UNEXTEND:
            long length = decodeUnsignedVarLong(entry, new IntegerRef.Value());
            activeIndex = doUndo(activeIndex, ix -> {
                try (Cursor c = ix.newAccessor(Transaction.BOGUS, mActiveKey)) {
                    c.valueLength(length);
                }
            });
            break;

        case OP_UNALLOC:
            var offsetRef = new IntegerRef.Value();
            length = decodeUnsignedVarLong(entry, offsetRef);
            long pos = decodeUnsignedVarLong(entry, offsetRef);
            activeIndex = doUndo(activeIndex, ix -> {
                try (Cursor c = ix.newAccessor(Transaction.BOGUS, mActiveKey)) {
                    c.valueClear(pos, length);
                }
            });
            break;

        case OP_UNWRITE:
            offsetRef = new IntegerRef.Value();
            pos = decodeUnsignedVarLong(entry, offsetRef);
            int off = offsetRef.get();
            activeIndex = doUndo(activeIndex, ix -> {
                try (Cursor c = ix.newAccessor(Transaction.BOGUS, mActiveKey)) {
                    c.valueWrite(pos, entry, off, entry.length - off);
                }
            });
            break;

        case OP_UNPREPARE:
            BTree preparedTxns = mDatabase.tryPreparedTxns();
            if (preparedTxns != null) {
                // The entry is the prepare key. Delete it.
                preparedTxns.store(Transaction.BOGUS, entry, null);
            }
            break;

        case OP_PREPARED: case OP_PREPARED_COMMIT:
            // Just a tag.
            break;

        case OP_PREPARED_UNROLLBACK:
            // The entry is the prepare key. Replace the ghost entry.
            preparedTxns = mDatabase.preparedTxns();
            try (var c = new BTreeCursor(preparedTxns)) {
                c.mTxn = LocalTransaction.BOGUS;
                c.autoload(false);
                c.find(entry);
                c.storeGhost(null);
            }
            break;
        }

        return activeIndex;
    }

    private byte[] decodeNodeKey(byte[] entry) throws IOException {
        byte[] key;
        var pentryAddr = p_transfer(entry);
        try {
            key = Node.retrieveKeyAtLoc(this, pentryAddr, 0);
        } finally {
            p_delete(pentryAddr);
        }
        return key;
    }

    private byte[][] decodeNodeKeyValuePair(byte[] entry) throws IOException {
        byte[][] pair;
        var pentryAddr = p_transfer(entry);
        try {
            pair = Node.retrieveKeyValueAtLoc(this, pentryAddr, 0);
        } finally {
            p_delete(pentryAddr);
        }
        return pair;
    }

    @FunctionalInterface
    private static interface UndoTask {
        void run(Index activeIndex) throws IOException;
    }

    /**
     * @return null if index was deleted
     */
    private Index doUndo(Index activeIndex, UndoTask task) throws IOException {
        while ((activeIndex = findIndex(activeIndex)) != null) {
            try {
                task.run(activeIndex);
                break;
            } catch (ClosedIndexException e) {
                // User closed the shared index reference, so re-open it.
                activeIndex = null;
            }
        }
        return activeIndex;
    }

    /**
     * @return null if index was deleted
     */
    private Index findIndex(Index activeIndex) throws IOException {
        if (activeIndex == null || activeIndex.isClosed()) {
            activeIndex = mDatabase.anyIndexById(mActiveIndexId);
        }
        return activeIndex;
    }

    private static interface Popper {
        /**
         * @return false if popping should stop
         */
        boolean accept(byte op, byte[] entry) throws IOException;
    }

    /**
     * Implementation of Popper which processes all undo operations up to a savepoint. Any
     * exception thrown by the accept method preserves the state of the log. That is, the
     * failed operation isn't discarded, and it remains as the top item.
     */
    private abstract class PopAll implements Popper {
        /**
         * @param delete true to delete nodes
         * @param savepoint must be less than mLength; pass 0 to pop the entire stack
         */
        void go(boolean delete, long savepoint) throws IOException {
            while (pop(delete, this) && savepoint < mLength);
        }
    }

    /**
     * Implementation of Popper which copies the undo entry before processing it. Any exception
     * thrown during processing (by the user of the class) won't preserve the state of the log.
     * As a result, this variant should only be used when checkpoints will never run, like
     * during recovery.
     */
    private static class PopOne implements Popper {
        byte mOp;
        byte[] mEntry;

        @Override
        public boolean accept(byte op, byte[] entry) throws IOException {
            mOp = op;
            mEntry = entry;
            return true;
        }
    }

    /**
     * Caller must hold db commit lock. The given popper is called such that if it throws any
     * exception, the operation is effectively un-popped (no state changes).
     *
     * @param delete true to delete nodes
     * @param popper called at most once per call to this method
     * @return false if nothing left
     */
    private boolean pop(boolean delete, Popper popper) throws IOException {
        final byte op;

        Node node = mNode;
        if (node == null) {
            byte[] buffer = mBuffer;
            int pos;
            if (buffer == null || (pos = mBufferPos) >= buffer.length) {
                mLength = 0;
                return false;
            }
            boolean result;
            op = buffer[pos++];
            if (op < PAYLOAD_OP) {
                result = popper.accept(op, EMPTY_BYTES);
                mBufferPos = pos;
                mLength -= 1;
            } else {
                long decoded = decodeUnsignedVarInt(buffer, pos);
                int payloadLen = (int) decoded;
                int varIntLen = ((int) (decoded >> 32)) - pos;
                pos += varIntLen;
                var entry = new byte[payloadLen];
                arraycopy(buffer, pos, entry, 0, payloadLen);
                result = popper.accept(op, entry);
                mBufferPos = pos + payloadLen;
                mLength -= 1 + varIntLen + payloadLen;
            }
            return result;
        }

        node.acquireExclusive();
        long pageAddr;
        while (true) {
            pageAddr = node.mPageAddr;
            if (mNodeTopPos < pageSize()) {
                break;
            }
            if ((node = popNode(node, delete)) == null) {
                mLength = 0;
                return false;
            }
        }

        int nodeTopPos = mNodeTopPos;
        op = p_byteGet(pageAddr, nodeTopPos++);

        if (op < PAYLOAD_OP) {
            boolean result;
            try {
                result = popper.accept(op, EMPTY_BYTES);
            } catch (Throwable e) {
                node.releaseExclusive();
                throw e;
            }
            mNodeTopPos = nodeTopPos;
            mLength -= 1;
            if (nodeTopPos >= pageSize()) {
                node = popNode(node, delete);
            }
            if (node != null) {
                node.releaseExclusive();
            }
            return result;
        }

        long length = mLength;

        long decoded = p_uintGetVar(pageAddr, nodeTopPos);
        int payloadLen = (int) decoded;
        int varIntLen = ((int) (decoded >> 32)) - nodeTopPos;
        nodeTopPos += varIntLen;
        length -= 1 + varIntLen + payloadLen;

        final var entry = new byte[payloadLen];
        int entryPos = 0;

        while (true) {
            int avail = Math.min(payloadLen, pageSize() - nodeTopPos);
            p_copy(pageAddr, nodeTopPos, entry, entryPos, avail);
            payloadLen -= avail;
            nodeTopPos += avail;
            entryPos += avail;

            if (nodeTopPos >= pageSize()) {
                long lowerNodeId = p_longGetLE(node.mPageAddr, I_LOWER_NODE_ID);
                node.releaseExclusive();
                if (lowerNodeId == 0) {
                    node = null;
                    nodeTopPos = 0;
                } else {
                    LocalDatabase db = mDatabase;
                    node = db.nodeMapGetExclusive(lowerNodeId);
                    if (node == null) {
                        // Node was evicted, so reload it.
                        node = readUndoLogNode(db, lowerNodeId, 0);
                        db.nodeMapPut(node);
                    }
                    nodeTopPos = node.undoTop();
                }
            }

            if (payloadLen <= 0) {
                break;
            }

            if (node == null) {
                throw new CorruptDatabaseException("Remainder of undo log is missing");
            }

            pageAddr = node.mPageAddr;
        }

        // At this point, the node variable refers to the top node which must remain after the
        // consumed nodes are popped. It can be null if all nodes must be popped. Capture the
        // id to stop at before releasing the latch, after which it can be evicted and change.

        long nodeId = 0;
        if (node != null) {
            nodeId = node.id();
            node.releaseExclusive();
        }

        boolean result = popper.accept(op, entry);

        Node n = mNode;
        if (node != n) {
            // Now pop as many nodes as necessary.
            try {
                n.acquireExclusive();
                while (true) {
                    n = popNode(n, delete);
                    if (n == null) {
                        if (nodeId != 0) {
                            throw new AssertionError();
                        }
                        break;
                    }
                    if (n.id() == nodeId) {
                        n.releaseExclusive();
                        break;
                    }
                }
            } catch (Throwable e) {
                // Panic.
                mDatabase.close(e);
                throw e;
            }
        }

        mNodeTopPos = nodeTopPos;
        mLength = length;

        return result;
    }

    /**
     * @param parent latched parent node
     * @param delete true to delete nodes
     * @return current (latched) mNode; null if none left
     */
    private Node popNode(Node parent, boolean delete) throws IOException {
        Node lowerNode = null;
        long lowerNodeId = p_longGetLE(parent.mPageAddr, I_LOWER_NODE_ID);
        if (lowerNodeId != 0) {
            lowerNode = mDatabase.nodeMapGetAndRemove(lowerNodeId);
            if (lowerNode != null) {
                lowerNode.makeUnevictable();
            } else {
                // Node was evicted, so reload it.
                try {
                    lowerNode = readUndoLogNode(mDatabase, lowerNodeId);
                } catch (Throwable e) {
                    parent.releaseExclusive();
                    throw e;
                }
            }
        }

        parent.makeEvictable();

        if (delete) {
            // Safer to never recycle undo log nodes. Keep them until the next checkpoint, when
            // there's a guarantee that the master undo log will not reference them anymore.
            // Of course, it's fine to recycle pages from master undo log itself, which is the
            // only one with a transaction id of zero.
            try {
                mDatabase.deleteNode(parent, mTxnId == 0);
            } catch (Throwable e) {
                if (lowerNode != null) {
                    mDatabase.nodeMapPut(lowerNode);
                    lowerNode.releaseExclusive();
                    lowerNode.makeEvictable();
                }
                throw e;
            }
        } else {
            parent.releaseExclusive();
        }

        mNode = lowerNode;
        mNodeTopPos = lowerNode == null ? 0 : lowerNode.undoTop();

        return lowerNode;
    }

    private static interface Visitor {
        /**
         * @param node first node of operation, latched exclusively; caller always releases it
         * @param op undo operation
         * @param pos position of undo operation
         * @return size of payload to decode (zero to skip it)
         */
        int accept(Node node, byte op, int pos);

        /**
         * @param node last node of operation, latched exclusively; caller always releases it
         * @param payload full or partial payload, as prescribed by the accept method
         */
        void payload(Node node, byte[] payload) throws IOException;
    }

    /**
     * Scans through all the nodes and calls the visitor for each operation. Assumes that this
     * undo log is persist-ready and has a top node.
     */
    private void scanNodes(Visitor v) throws IOException {
        Node node = mNode;
        int nodeTopPos = mNodeTopPos;
        node.acquireExclusive();
        long pageAddr = node.mPageAddr;

        while (true) {
            int opPos = nodeTopPos;
            byte op = p_byteGet(pageAddr, nodeTopPos++);

            int payloadLen = 0;
            if (op >= PAYLOAD_OP) {
                long decoded = p_uintGetVar(pageAddr, nodeTopPos);
                payloadLen = (int) decoded;
                nodeTopPos = (int) (decoded >> 32);
            }

            int payloadRequired = v.accept(node, op, opPos);

            byte[] entry = null;
            int entryPos = 0;
            if (payloadRequired > 0) {
                entry = new byte[payloadRequired];
            }

            do {
                int avail = Math.min(payloadLen, pageSize() - nodeTopPos);

                if (entry != null) {
                    int amt = Math.min(avail, entry.length - entryPos);
                    p_copy(pageAddr, nodeTopPos, entry, entryPos, amt);
                    entryPos += avail;
                    if (entryPos >= entry.length) {
                        try {
                            v.payload(node, entry);
                        } catch (Throwable e) {
                            node.releaseExclusive();
                            throw e;
                        }
                        entry = null;
                    }
                }

                payloadLen -= avail;
                nodeTopPos += avail;

                if (nodeTopPos >= pageSize()) {
                    long lowerNodeId = p_longGetLE(pageAddr, I_LOWER_NODE_ID);
                    node.releaseExclusive();
                    if (lowerNodeId == 0) {
                        return;
                    }
                    LocalDatabase db = mDatabase;
                    node = db.nodeMapGetExclusive(lowerNodeId);
                    if (node == null) {
                        // Node was evicted, so reload it.
                        node = readUndoLogNode(db, lowerNodeId, 0);
                        db.nodeMapPut(node);
                    }
                    pageAddr = node.mPageAddr;
                    nodeTopPos = node.undoTop();
                }
            } while (payloadLen > 0);
        }
    }

    /**
     * Caller must hold db commit lock.
     */
    private Node allocUnevictableNode(long lowerNodeId) throws IOException {
        Node node = mDatabase.allocDirtyNode(NodeGroup.MODE_UNEVICTABLE);
        node.type(Node.TYPE_UNDO_LOG);
        p_longPutLE(node.mPageAddr, I_LOWER_NODE_ID, lowerNodeId);
        return node;
    }

    /**
     * Caller must hold exclusive db commit lock.
     *
     * @param workspace temporary buffer, allocated on demand
     * @return new or original workspace instance
     */
    final byte[] writeToMaster(UndoLog master, byte[] workspace) throws IOException {
        if (mActiveKey != null) {
            doPush(OP_ACTIVE_KEY, mActiveKey);
            // Set to null to reduce redundant pushes if transaction is long-lived and is
            // written to the master multiple times.
            mActiveKey = null;
        }

        Node node = mNode;
        if (node == null) {
            byte[] buffer = mBuffer;
            if (buffer == null) {
                return workspace;
            }
            int pos = mBufferPos;
            int bsize = buffer.length - pos;
            if (bsize == 0) {
                return workspace;
            }
            // TODO: Consider calling persistReady if UndoLog is still in a buffer next time.
            final int psize = (8 + 8 + 2) + bsize;
            if (workspace == null || workspace.length < psize) {
                workspace = new byte[Math.max(INITIAL_BUFFER_SIZE, roundUpPower2(psize))];
            }
            writeHeaderToMaster(workspace);
            encodeShortLE(workspace, (8 + 8), bsize);
            arraycopy(buffer, pos, workspace, (8 + 8 + 2), bsize);
            master.doPush((byte) (OP_LOG_COPY + mCommitted), workspace, 0, psize);
        } else {
            if (workspace == null) {
                workspace = new byte[INITIAL_BUFFER_SIZE];
            }
            writeHeaderToMaster(workspace);
            encodeLongLE(workspace, (8 + 8), mLength);
            encodeLongLE(workspace, (8 + 8 + 8), node.id());
            encodeShortLE(workspace, (8 + 8 + 8 + 8), mNodeTopPos);
            master.doPush((byte) (OP_LOG_REF + mCommitted), workspace, 0, (8 + 8 + 8 + 8 + 2), 1);
        }

        return workspace;
    }

    private void writeHeaderToMaster(byte[] workspace) {
        encodeLongLE(workspace, 0, mTxnId);
        encodeLongLE(workspace, 8, mActiveIndexId);
    }

    static UndoLog recoverMasterUndoLog(LocalDatabase db, long nodeId) throws IOException {
        var log = new UndoLog(db, 0);
        // Length is not recoverable.
        log.recoverMaster(nodeId, Long.MAX_VALUE);
        return log;
    }

    private void recoverMaster(long nodeId, long length) throws IOException {
        mLength = length;
        mNode = readUndoLogNode(mDatabase, nodeId);
        mNodeTopPos = mNode.undoTop();
        mNode.releaseExclusive();
    }

    /**
     * Finds all the transactions which were written to this master undo log in a committed
     * state, returning null if none. Assumes that master undo log is persist-ready and has a
     * top node. Caller must hold db commit lock.
     *
     * @return optional set with transaction id keys and null values
     */
    LHashSet findCommitted() throws IOException {
        var finder = new Visitor() {
            LHashSet mTxns;

            @Override
            public int accept(Node node, byte op, int pos) {
                // Need to decode the transaction id of the payload.
                return (op == OP_LOG_COPY_C || op == OP_LOG_REF_C) ? 8 : 0;
            }

            @Override
            public void payload(Node node, byte[] payload) {
                long txnId = decodeLongLE(payload, 0);
                if (mTxns == null) {
                    mTxns = new LHashSet(4);
                }
                mTxns.add(txnId);
            }
        };

        scanNodes(finder);

        return finder.mTxns;
    }

    /**
     * Updates all references to the given transaction ids in this master undo log to be
     * uncommitted. Caller must hold db commit lock.
     */
    void markUncommitted(LHashSet uncommitted) throws IOException {
        // Note that it's safe to blindly re-write the nodes. The nodes were intended for a
        // checkpoint which failed, and so nothing refers to them.

        var uncommitter = new Visitor() {
            final Deque<Node> mMatched = new ArrayDeque<>();
            private int mOp;
            private int mPos;

            @Override
            public int accept(Node node, byte op, int pos) {
                if (op == OP_LOG_COPY_C || op == OP_LOG_REF_C) {
                    mMatched.add(node);
                    mOp = op - OP_LOG_COPY;
                    mPos = pos;
                    // Need to decode the transaction id of the payload.
                    return 8;
                }
                return 0;
            }

            @Override
            public void payload(Node node, byte[] payload) throws IOException {
                long txnId = decodeLongLE(payload, 0);
                if (uncommitted.contains(txnId)) {
                    Node opNode = mMatched.getLast();
                    if (node == opNode) {
                        // Payload didn't span nodes.
                        p_bytePut(node.mPageAddr, mPos, mOp);
                    } else {
                        // Must latch and modify the node that contained the operation.
                        opNode.acquireExclusive();
                        try {
                            p_bytePut(opNode.mPageAddr, mPos, mOp);
                        } finally {
                            opNode.releaseExclusive();
                        }
                    }
                }
            }
        };

        scanNodes(uncommitter);

        // All matched nodes were changed and must be re-written.
        for (Node node : uncommitter.mMatched) {
            node.acquireExclusive();
            try {
                node.write(mDatabase.mPageDb);
            } finally {
                node.releaseExclusive();
            }
        }
    }

    /**
     * Recover transactions which were recorded by this master log, keyed by
     * transaction id. Recovered transactions have a NO_REDO durability mode.
     * All transactions are registered, and so they must be reset after
     * recovery is complete. Master log is truncated as a side effect of
     * calling this method.
     *
     * @param debugListener optional
     * @param trace when true, log all recovered undo operations to debugListener
     */
    void recoverTransactions(EventListener debugListener, boolean trace,
                             LHashTable.Obj<LocalTransaction> txns)
        throws IOException
    {
        new PopAll() {
            @Override
            public boolean accept(byte op, byte[] entry) throws IOException {
                UndoLog log = recoverUndoLog(op, entry);

                if (debugListener != null) {
                    debugListener.notify
                        (EventType.DEBUG,
                         "Recovered transaction undo log: " +
                         "txnId=%1$d, length=%2$d, bufferPos=%3$d, " +
                         "nodeId=%4$d, nodeTopPos=%5$d, activeIndexId=%6$d, committed=%7$s",
                         log.mTxnId, log.mLength, log.mBufferPos,
                         log.mNode == null ? 0 : log.mNode.id(), log.mNodeTopPos,
                         log.mActiveIndexId, log.mCommitted != 0);
                }

                LocalTransaction txn = log.recoverTransaction(debugListener, trace);

                // Reload the UndoLog, since recoverTransaction consumes it all.
                txn.recoveredUndoLog(recoverUndoLog(op, entry));
                txn.attach("recovery");

                txns.put(log.mTxnId).value = txn;

                return true;
            }
        }.go(true, 0);
    }

    /**
     * Method consumes entire log as a side-effect.
     */
    private LocalTransaction recoverTransaction(EventListener debugListener, boolean trace)
        throws IOException
    {
        if (mNode != null && mNodeTopPos == 0) {
            // The checkpoint captured a committed log in the middle of truncation. The
            // recoveryCleanup method will finish the truncation.
            return new LocalTransaction(mDatabase, mTxnId, 0);
        }

        var popper = new PopOne();
        var scope = new Scope();

        // Scopes are recovered in the opposite order in which they were
        // created. Gather them in a stack to reverse the order.
        var scopes = new ArrayDeque<Scope>();
        scopes.addFirst(scope);

        int depth = 1;

        int hasState = 0;

        while (mLength > 0) {
            if (!pop(false, popper)) {
                // Undo log would have to be corrupt for this case to occur.
                break;
            }

            byte op = popper.mOp;
            byte[] entry = popper.mEntry;

            if (trace) {
                traceOp(debugListener, op, entry);
            }

            switch (op) {
            default:
                throw new DatabaseException("Unknown undo log entry type: " + op);

            case OP_SCOPE_ENTER:
                depth++;
                if (depth > scopes.size()) {
                    scope.mSavepoint = mLength;
                    scope = new Scope();
                    scopes.addFirst(scope);
                }
                break;

            case OP_SCOPE_COMMIT:
                depth--;
                break;

            case OP_INDEX:
                mActiveIndexId = decodeLongLE(entry, 0);
                break;

            case OP_UNINSERT:
            case OP_LOCK_EXCLUSIVE:
                scope.addExclusiveLock(mActiveIndexId, entry);
                break;

            case OP_LOCK_UPGRADABLE:
                scope.addUpgradableLock(mActiveIndexId, entry);
                break;

            case OP_UNUPDATE:
            case OP_UNDELETE:
            case OP_UNDELETE_FRAGMENTED: {
                byte[] key = decodeNodeKey(entry);
                Lock lock = scope.addExclusiveLock(mActiveIndexId, key);
                if (op != OP_UNUPDATE) {
                    // Indicate that a ghost must be deleted when the transaction is
                    // committed. When the frame is uninitialized, the Node.deleteGhost
                    // method uses the slow path and searches for the entry.
                    lock.setGhostFrame(new GhostFrame());
                }
                break;
            }

            case OP_UNUPDATE_LK:
            case OP_UNDELETE_LK:
            case OP_UNDELETE_LK_FRAGMENTED: {
                long decoded = decodeUnsignedVarInt(entry, 0);
                var key = new byte[(int) decoded];
                int keyLoc = (int) (decoded >> 32);
                arraycopy(entry, keyLoc, key, 0, key.length);
                Lock lock = scope.addExclusiveLock(mActiveIndexId, key);
                if (op != OP_UNUPDATE_LK) {
                    // Indicate that a ghost must be deleted when the transaction is
                    // committed. When the frame is uninitialized, the Node.deleteGhost
                    // method uses the slow path and searches for the entry.
                    lock.setGhostFrame(new GhostFrame());
                }
                break;
            }

            case OP_CUSTOM:
            case OP_UNPREPARE:
            case OP_PREPARED_UNROLLBACK:
                break;

            case OP_PREPARED:
                hasState |= LocalTransaction.HAS_PREPARE;
                break;

            case OP_PREPARED_COMMIT:
                hasState |= LocalTransaction.HAS_PREPARE | LocalTransaction.HAS_PREPARE_COMMIT;
                break;

            case OP_ACTIVE_KEY:
                mActiveKey = entry;
                break;

            case OP_UNCREATE:
            case OP_UNEXTEND:
            case OP_UNALLOC:
            case OP_UNWRITE:
                if (mActiveKey != null) {
                    scope.addExclusiveLock(mActiveIndexId, mActiveKey);
                    // Avoid creating a huge list of redundant Lock objects.
                    mActiveKey = null;
                }
                break;
            }
        }

        var txn = new LocalTransaction(mDatabase, mTxnId, hasState);

        scope = scopes.pollFirst();
        scope.acquireLocks(txn);

        while ((scope = scopes.pollFirst()) != null) {
            txn.recoveredScope(scope.mSavepoint, LocalTransaction.HAS_TRASH);
            scope.acquireLocks(txn);
        }

        return txn;
    }

    private void traceOp(EventListener debugListener, byte op, byte[] entry) throws IOException {
        String opStr;
        String payloadStr = null;

        switch (op) {
        default:
            opStr = "UNKNOWN";
            payloadStr = "op=" + (op & 0xff) + ", entry=0x" + toHex(entry);
            break;

        case OP_SCOPE_ENTER:
            opStr = "SCOPE_ENTER";
            break;

        case OP_SCOPE_COMMIT:
            opStr = "SCOPE_COMMIT";
            break;

        case OP_UNCREATE:
            opStr = "UNCREATE";
            break;

        case OP_LOG_COPY:
            opStr = "LOG_COPY";
            break;

        case OP_LOG_REF:
            opStr = "LOG_REF";
            break;

        case OP_LOG_COPY_C:
            opStr = "LOG_COPY_C";
            break;

        case OP_LOG_REF_C:
            opStr = "LOG_REF_C";
            break;

        case OP_INDEX:
            opStr = "INDEX";
            payloadStr = "indexId=" + decodeLongLE(entry, 0);
            break;

        case OP_UNINSERT:
            opStr = "UNINSERT";
            payloadStr = "key=0x" + toHex(entry) + " (" + utf8(entry) + ')';
            break;

        case OP_UNUPDATE: case OP_UNDELETE:
            opStr = op == OP_UNUPDATE ? "UNUPDATE" : "UNDELETE";
            byte[][] pair = decodeNodeKeyValuePair(entry);
            payloadStr = "key=0x" + toHex(pair[0]) + " (" +
                utf8(pair[0]) + ") value=0x" + toHex(pair[1]);
            break;

        case OP_UNDELETE_FRAGMENTED:
            opStr = "UNDELETE_FRAGMENTED";
            byte[] key = decodeNodeKey(entry);
            payloadStr = "key=0x" + toHex(key) + " (" + utf8(key) + ')';
            break;

        case OP_ACTIVE_KEY:
            opStr = "ACTIVE_KEY";
            payloadStr = "key=0x" + toHex(entry) + " (" + utf8(entry) + ')';
            break;

        case OP_CUSTOM:
            opStr = "CUSTOM";
            long decoded = decodeUnsignedVarInt(entry, 0);
            int handlerId = (int) decoded;
            int messageLoc = (int) (decoded >> 32);
            String handlerName = mDatabase.findHandlerName(handlerId, LocalDatabase.RK_CUSTOM_ID);
            payloadStr = "handlerId=" + handlerId + ", handlerName=" + handlerName +
                ", message=0x" + toHex(entry, messageLoc, entry.length - messageLoc);
            break;

        case OP_UNUPDATE_LK: case OP_UNDELETE_LK:
            opStr = op == OP_UNUPDATE_LK ? "UNUPDATE_LK" : "UNDELETE_LK";

            decoded = decodeUnsignedVarInt(entry, 0);
            int keyLen = (int) decoded;
            int keyLoc = (int) (decoded >> 32);
            int valueLoc = keyLoc + keyLen;
            int valueLen = entry.length - valueLoc;

            payloadStr = "key=0x" + toHex(entry, keyLoc, keyLen) + " (" +
                utf8(entry, keyLoc, keyLen) + ") value=0x" +
                toHex(entry, valueLoc, valueLen);

            break;

        case OP_UNDELETE_LK_FRAGMENTED:
            opStr = "UNDELETE_LK_FRAGMENTED";

            decoded = decodeUnsignedVarInt(entry, 0);
            keyLen = (int) decoded;
            keyLoc = (int) (decoded >> 32);

            payloadStr = "key=0x" + toHex(entry, keyLoc, keyLen) + " (" +
                utf8(entry, keyLoc, keyLen) + ')';

            break;

        case OP_UNEXTEND:
            opStr = "UNEXTEND";
            payloadStr = "length=" + decodeUnsignedVarLong(entry, new IntegerRef.Value());
            break;

        case OP_UNALLOC:
            opStr = "UNALLOC";
            var offsetRef = new IntegerRef.Value();
            long length = decodeUnsignedVarLong(entry, offsetRef);
            long pos = decodeUnsignedVarLong(entry, offsetRef);
            payloadStr = "pos=" + pos + ", length=" + length;
            break;

        case OP_UNWRITE:
            opStr = "UNWRITE";
            offsetRef = new IntegerRef.Value();
            pos = decodeUnsignedVarLong(entry, offsetRef);
            int off = offsetRef.get();
            payloadStr = "pos=" + pos + ", value=0x" + toHex(entry, off, entry.length - off);
            break;

        case OP_LOCK_UPGRADABLE:
            opStr = "LOCK_UPGRADABLE";
            payloadStr = "key=0x" + toHex(entry) + " (" + utf8(entry) + ')';
            break;

        case OP_LOCK_EXCLUSIVE:
            opStr = "LOCK_EXCLUSIVE";
            payloadStr = "key=0x" + toHex(entry) + " (" + utf8(entry) + ')';
            break;

        case OP_UNPREPARE:
            opStr = "UNPREPARE";
            payloadStr = "txnId=" + decodeLongBE(entry, 0);
            break;

        case OP_PREPARED: case OP_PREPARED_COMMIT:
            opStr = op == OP_PREPARED_COMMIT ? "PREPARED_COMMIT" : "PREPARED";
            decoded = decodeUnsignedVarInt(entry, 0);
            handlerId = (int) decoded;
            handlerName = mDatabase.findHandlerName(handlerId, LocalDatabase.RK_PREPARE_ID);
            payloadStr = "handlerId=" + handlerId + ", handlerName=" + handlerName;
            messageLoc = ((int) (decoded >> 32)) + 1;
            if (messageLoc <= entry.length) {
                payloadStr += ", message=0x" + toHex(entry, messageLoc, entry.length - messageLoc);
            }
            break;

        case OP_PREPARED_UNROLLBACK:
            opStr = "PREPARED_UNROLLBACK";
            payloadStr = "txnId=" + decodeLongBE(entry, 0);
            break;
        }

        if (payloadStr == null) {
            debugListener.notify(EventType.DEBUG, "Undo recover %1$s", opStr);
        } else {
            debugListener.notify(EventType.DEBUG, "Undo recover %1$s %2$s", opStr, payloadStr);
        }
    }

    /**
     * Recovered undo scope.
     */
    static class Scope {
        long mSavepoint;

        // Locks are recovered in the opposite order in which they were acquired. Gather them
        // in a stack to reverse the order. Re-use the LockManager collision chain field and
        // form a linked list.
        Lock mTopLock;

        Scope() {
        }

        Lock addExclusiveLock(long indexId, byte[] key) {
            return addLock(indexId, key, ~0);
        }

        Lock addUpgradableLock(long indexId, byte[] key) {
            return addLock(indexId, key, 1 << 31);
        }

        Lock addLock(long indexId, byte[] key, int lockCount) {
            var lock = new Lock();
            lock.mIndexId = indexId;
            lock.mKey = key;
            lock.mHashCode = LockManager.hash(indexId, key);
            lock.mLockNext = mTopLock;
            lock.mLockCount = lockCount;
            mTopLock = lock;
            return lock;
        }

        void acquireLocks(LocalTransaction txn) throws LockFailureException {
            Lock lock = mTopLock;
            if (lock != null) while (true) {
                // Copy next before the field is overwritten.
                Lock next = lock.mLockNext;
                txn.recoverLock(lock);
                if (next == null) {
                    break;
                }
                mTopLock = lock = next;
            }
        }
    }

    /**
     * @param masterLogOp OP_LOG_*
     */
    private UndoLog recoverUndoLog(byte masterLogOp, byte[] masterLogEntry)
        throws IOException
    {
        if (masterLogOp != OP_LOG_COPY && masterLogOp != OP_LOG_REF &&
            masterLogOp != OP_LOG_COPY_C && masterLogOp != OP_LOG_REF_C)
        {
            throw new DatabaseException("Unknown undo log entry type: " + masterLogOp);
        }

        long txnId = decodeLongLE(masterLogEntry, 0);
        var log = new UndoLog(mDatabase, txnId);
        log.mActiveIndexId = decodeLongLE(masterLogEntry, 8);

        if ((masterLogOp & 1) == 0) { // OP_LOG_COPY or OP_LOG_COPY_C
            int bsize = decodeUnsignedShortLE(masterLogEntry, (8 + 8));
            log.mLength = bsize;
            var buffer = new byte[bsize];
            arraycopy(masterLogEntry, (8 + 8 + 2), buffer, 0, bsize);
            log.mBuffer = buffer;
            log.mBufferPos = 0;
        } else { // OP_LOG_REF or OP_LOG_REF_C
            log.mLength = decodeLongLE(masterLogEntry, (8 + 8));
            long nodeId = decodeLongLE(masterLogEntry, (8 + 8 + 8));
            int topEntry = decodeUnsignedShortLE(masterLogEntry, (8 + 8 + 8 + 8));
            log.mNode = readUndoLogNode(mDatabase, nodeId);
            log.mNodeTopPos = topEntry;
            log.mNode.releaseExclusive();
        }

        log.mCommitted = (masterLogOp >> 1) & OP_LOG_COPY;

        return log;
    }

    /**
     * @return latched, unevictable node
     */
    private static Node readUndoLogNode(LocalDatabase db, long nodeId) throws IOException {
        return readUndoLogNode(db, nodeId, NodeGroup.MODE_UNEVICTABLE);
    }

    /**
     * @return latched node with given eviction mode (pass 0 for normal mode)
     */
    private static Node readUndoLogNode(LocalDatabase db, long nodeId, int mode)
        throws IOException
    {
        Node node = db.allocLatchedNode(mode);
        try {
            node.read(db, nodeId);
            if (node.type() != Node.TYPE_UNDO_LOG) {
                throw new CorruptDatabaseException
                    ("Not an undo log node type: " + node.type() + ", id: " + nodeId);
            }
            return node;
        } catch (Throwable e) {
            node.releaseExclusive();
            node.makeEvictableNow();
            throw e;
        }
    }
}
