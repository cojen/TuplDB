/*
 *  Copyright 2011-2013 Brian S O'Neill
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

import java.util.ArrayDeque;
import java.util.Deque;

import java.util.concurrent.locks.Lock;

import static java.lang.System.arraycopy;

import static org.cojen.tupl.Utils.*;

/**
 * Specialized stack used by UndoLog.
 *
 * @author Brian S O'Neill
 */
final class UndoLog {
    // Linked list of UndoLogs registered with Database.
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
      |                                        |
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

    private static final byte OP_SCOPE_ENTER = (byte) 1;
    private static final byte OP_SCOPE_COMMIT = (byte) 2;

    // Indicates that transaction has been committed.
    static final byte OP_COMMIT = (byte) 4;

    // Indicates that transaction has been committed and log is partially truncated.
    static final byte OP_COMMIT_TRUNCATE = (byte) 5;

    // All ops less than 16 have no payload.
    private static final byte PAYLOAD_OP = (byte) 16;

    // Copy to another log from master log. Payload is transaction id, active
    // index id, buffer size (short type), and serialized buffer.
    private static final byte OP_LOG_COPY = (byte) 16;

    // Reference to another log from master log. Payload is transaction id,
    // active index id, length, node id, and top entry offset.
    private static final byte OP_LOG_REF = (byte) 17;

    // Payload is active index id.
    private static final byte OP_INDEX = (byte) 18;

    // Payload is key to delete to undo an insert.
    static final byte OP_DELETE = (byte) 19;

    // Payload is key and value to store to undo an update.
    static final byte OP_UPDATE = (byte) 20;

    // Payload is key and value to store to undo a delete.
    static final byte OP_INSERT = (byte) 21;

    // Payload is key and trash id to undo a fragmented value delete.
    static final byte OP_RECLAIM_FRAGMENTED = (byte) 22;

    private final Database mDatabase;
    private final long mTxnId;

    // Number of bytes currently pushed into log.
    private long mLength;

    // Except for mLength, all field modifications during normal usage must be
    // performed while holding shared db commit lock. See writeToMaster method.

    private byte[] mBuffer;
    private int mBufferPos;

    // Top node, if required. Nodes are not used for logs which fit into local buffer.
    private Node mNode;

    private long mActiveIndexId;

    UndoLog(Database db, long txnId) {
        mDatabase = db;
        mTxnId = txnId;
    }

    /**
     * Ensures all entries are stored in persistable nodes. Caller must hold db
     * commit lock.
     */
    private void persistReady() throws IOException {
        if (mNode != null) {
            return;
        }

        Node node;
        byte[] buffer = mBuffer;
        if (buffer == null) {
            mNode = node = allocUnevictableNode(0);
            // Set pointer to top entry (none at the moment).
            node.mGarbage = node.mPage.length;
            node.releaseExclusive();
        } else {
            mNode = node = allocUnevictableNode(0);
            int pos = mBufferPos;
            int size = buffer.length - pos;
            byte[] page = node.mPage;
            int newPos = page.length - size;
            arraycopy(buffer, pos, page, newPos, size);
            // Set pointer to top entry.
            node.mGarbage = newPos;
            mBuffer = null;
            mBufferPos = 0;
            node.releaseExclusive();
        }
    }

    long txnId() {
        return mTxnId;
    }

    /**
     * Caller must hold db commit lock.
     *
     * @return 0 if log is empty
     */
    long topNodeId() throws IOException {
        if (mNode == null) {
            if (mLength == 0) {
                return 0;
            }
            persistReady();
        }
        return mNode.mId;
    }

    /**
     * Caller must hold db commit lock.
     */
    final void push(long indexId, byte op, byte[] payload) throws IOException {
        push(indexId, op, payload, 0, payload.length);
    }

    /**
     * Caller must hold db commit lock.
     */
    final void push(final long indexId,
                    final byte op, final byte[] payload, final int off, final int len)
        throws IOException
    {
        long activeIndexId = mActiveIndexId;
        if (indexId != activeIndexId) {
            if (activeIndexId != 0) {
                pushIndexId(activeIndexId);
            }
            mActiveIndexId = indexId;
        }

        doPush(op, payload, off, len, calcUnsignedVarIntLength(len));
    }

    private void pushIndexId(long indexId) throws IOException {
        byte[] payload = new byte[8];
        encodeLongLE(payload, 0, indexId);
        doPush(OP_INDEX, payload, 0, 8, 1);
    }

    /**
     * Caller must hold db commit lock.
     */
    void pushCommit() throws IOException {
        doPush(OP_COMMIT);
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
    private void doPush(final byte op, final byte[] payload, final int off, final int len,
                        int varIntLen)
        throws IOException
    {
        final int encodedLen = 1 + varIntLen + len;
        mLength += encodedLen;

        Node node = mNode;
        if (node != null) {
            // Push into allocated node, which must be marked dirty.
            node.acquireExclusive();
            mDatabase.markUndoLogDirty(node);
        } else quick: {
            // Try to push into a local buffer before allocating a node.
            byte[] buffer = mBuffer;
            int pos;
            if (buffer == null) {
                int newCap = Math.max(INITIAL_BUFFER_SIZE, roundUpPower2(encodedLen));
                int pageSize = mDatabase.pageSize();
                if (newCap <= (pageSize >> 1)) {
                    mBuffer = buffer = new byte[newCap];
                    mBufferPos = pos = newCap;
                } else {
                    // Required capacity is large, so just use a node.
                    mNode = node = allocUnevictableNode(0);
                    // Set pointer to top entry (none at the moment).
                    node.mGarbage = pageSize;
                    break quick;
                }
            } else {
                pos = mBufferPos;
                if (pos < encodedLen) {
                    final int size = buffer.length - pos;
                    int newCap = Math.max
                        (buffer.length << 1, roundUpPower2(encodedLen + size));
                    if (newCap <= (mDatabase.pageSize() >> 1)) {
                        byte[] newBuf = new byte[newCap];
                        int newPos = newCap - size;
                        arraycopy(buffer, pos, newBuf, newPos, size);
                        mBuffer = buffer = newBuf;
                        mBufferPos = pos = newPos;
                    } else {
                        // Required capacity is large, so just use a node.
                        mNode = node = allocUnevictableNode(0);
                        byte[] page = node.mPage;
                        int newPos = page.length - size;
                        arraycopy(buffer, pos, page, newPos, size);
                        // Set pointer to top entry.
                        node.mGarbage = newPos;
                        mBuffer = null;
                        mBufferPos = 0;
                        break quick;
                    }
                }
            }

            writeEntry(buffer, pos -= encodedLen, op, payload, off, len);
            mBufferPos = pos;
            return;
        }

        // Re-use mGarbage as pointer to top entry.
        int pos = node.mGarbage;
        int available = pos - HEADER_SIZE;
        if (available >= encodedLen) {
            writeEntry(node.mPage, pos -= encodedLen, op, payload, off, len);
            node.mGarbage = pos;
            node.releaseExclusive();
            return;
        }

        // Payload doesn't fit into node, so break it up.
        int remaining = len;

        while (true) {
            int amt = Math.min(available, remaining);
            pos -= amt;
            available -= amt;
            remaining -= amt;
            byte[] page = node.mPage;
            arraycopy(payload, off + remaining, page, pos, amt);
            node.mGarbage = pos;

            if (remaining <= 0 && available >= (1 + varIntLen)) {
                if (varIntLen > 0) {
                    encodeUnsignedVarInt(page, pos -= varIntLen, len);
                }
                page[--pos] = op;
                node.mGarbage = pos;
                node.releaseExclusive();
                break;
            }

            Node newNode;
            {
                Node[] childNodes = new Node[] {node};
                newNode = allocUnevictableNode(node.mId);
                newNode.mChildNodes = childNodes;
                newNode.mGarbage = pos = page.length;
                available = pos - HEADER_SIZE;
            }

            node.releaseExclusive();
            mDatabase.makeEvictable(node);
            mNode = node = newNode;
        }
    }

    /**
     * Caller does not need to hold db commit lock.
     *
     * @return savepoint
     */
    final long scopeEnter() throws IOException {
        final Lock sharedCommitLock = mDatabase.sharedCommitLock();
        sharedCommitLock.lock();
        try {
            long savepoint = mLength;
            doPush(OP_SCOPE_ENTER);
            return savepoint;
        } finally {
            sharedCommitLock.unlock();
        }
    }

    /**
     * Caller does not need to hold db commit lock.
     *
     * @return savepoint
     */
    final long scopeCommit() throws IOException {
        final Lock sharedCommitLock = mDatabase.sharedCommitLock();
        sharedCommitLock.lock();
        try {
            doPush(OP_SCOPE_COMMIT);
            return mLength;
        } finally {
            sharedCommitLock.unlock();
        }
    }

    /**
     * Rollback all log entries to the given savepoint. Pass zero to rollback
     * everything. Caller does not need to hold db commit lock.
     */
    final void scopeRollback(long savepoint) throws IOException {
        final Lock sharedCommitLock = mDatabase.sharedCommitLock();
        sharedCommitLock.lock();
        try {
            if (savepoint < mLength) {
                // Rollback the entire scope, including the enter op.
                doRollback(savepoint);
            }
        } finally {
            sharedCommitLock.unlock();
        }
    }

    /**
     * Should only be called after all log entries have been truncated or
     * rolled back. Caller does not need to hold db commit lock.
     */
    final void unregister() {
        final Lock sharedCommitLock = mDatabase.sharedCommitLock();
        sharedCommitLock.lock();
        try {
            mDatabase.unregister(this);
        } finally {
            sharedCommitLock.unlock();
        }
    }

    /**
     * Truncate all log entries. Caller does not need to hold db commit lock.
     *
     * @param commit pass true to indicate that top of stack is a commit op
     */
    final void truncate(boolean commit) throws IOException {
        final Lock sharedCommitLock = mDatabase.sharedCommitLock();
        sharedCommitLock.lock();
        try {
            if (mLength > 0) {
                Node node = mNode;
                if (node == null) {
                    mBufferPos = mBuffer.length;
                } else {
                    node.acquireExclusive();
                    while ((node = popNode(node, true)) != null) {
                        if (commit) {
                            // When shared lock is released, log can be checkpointed in an
                            // incomplete state. Although caller must have already pushed the
                            // commit op, any of the remaining nodes might be referenced by an
                            // older master undo log entry. Must call prepareToDelete before
                            // calling redirty, in case node contains data which has been
                            // marked to be written out with the active checkpoint. The state
                            // assigned by redirty is such that the node might be written
                            // by the next checkpoint.
                            mDatabase.prepareToDelete(node);
                            mDatabase.redirty(node);
                            byte[] page = node.mPage;
                            int end = page.length - 1;
                            node.mGarbage = end;
                            page[end] = OP_COMMIT_TRUNCATE;
                        }
                        // Release and re-acquire, to unblock any threads waiting for
                        // checkpoint to begin.
                        sharedCommitLock.unlock();
                        sharedCommitLock.lock();
                    }
                }
                mLength = 0;
                mActiveIndexId = 0;
            }
        } finally {
            sharedCommitLock.unlock();
        }
    }

    /**
     * Rollback all log entries. Caller does not need to hold db commit lock.
     */
    final void rollback() throws IOException {
        if (mLength == 0) {
            // Nothing to rollback, so return quickly.
            return;
        }

        final Lock sharedCommitLock = mDatabase.sharedCommitLock();
        sharedCommitLock.lock();
        try {
            doRollback(0);
        } finally {
            sharedCommitLock.unlock();
        }
    }

    /**
     * @param savepoint must be less than mLength
     */
    private void doRollback(long savepoint) throws IOException {
        // Implementation could be optimized somewhat, resulting in less
        // temporary arrays and copies. Rollback optimization is generally not
        // necessary, since most transactions are expected to commit.

        byte[] opRef = new byte[1];
        Index activeIndex = null;
        do {
            byte[] entry = pop(opRef, true);
            if (entry == null) {
                break;
            }
            byte op = opRef[0];
            activeIndex = undo(activeIndex, op, entry);
        } while (savepoint < mLength);
    }

    /**
     * Truncate all log entries, and delete any ghosts that were created. Only
     * to be called during recovery.
     */
    final void deleteGhosts() throws IOException {
        if (mLength <= 0) {
            return;
        }

        byte[] opRef = new byte[1];
        Index activeIndex = null;
        do {
            byte[] entry = pop(opRef, true);
            if (entry == null) {
                break;
            }

            byte op = opRef[0];
            switch (op) {
            default:
                throw new DatabaseException("Unknown undo log entry type: " + op);

            case OP_SCOPE_ENTER:
            case OP_SCOPE_COMMIT:
            case OP_COMMIT:
            case OP_COMMIT_TRUNCATE:
            case OP_DELETE:
            case OP_UPDATE:
            case OP_RECLAIM_FRAGMENTED:
                // Ignore.
                break;

            case OP_INDEX:
                mActiveIndexId = decodeLongLE(entry, 0);
                activeIndex = null;
                break;

            case OP_INSERT:
                // Since transaction was committed, don't insert an entry
                // to undo a delete, but instead delete the ghost.
                if ((activeIndex = findIndex(activeIndex)) != null) {
                    byte[] key = Node.retrieveKeyAtLoc(entry, 0);
                    TreeCursor cursor = new TreeCursor((Tree) activeIndex, null);
                    try {
                        // No need to handle case where Tree is closed, because
                        // this method can only be called during recovery.
                        cursor.deleteGhost(key);
                    } catch (Throwable e) {
                        throw closeOnFailure(cursor, e);
                    }
                }
                break;
            }
        } while (mLength > 0);
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
        case OP_COMMIT:
        case OP_COMMIT_TRUNCATE:
            // Only needed by recovery.
            break;

        case OP_INDEX:
            mActiveIndexId = decodeLongLE(entry, 0);
            activeIndex = null;
            break;

        case OP_DELETE:
            while ((activeIndex = findIndex(activeIndex)) != null) {
                try {
                    activeIndex.delete(Transaction.BOGUS, entry);
                    break;
                } catch (ClosedIndexException e) {
                    // User closed the shared index reference, so re-open it.
                    activeIndex = null;
                }
            }
            break;

        case OP_UPDATE:
        case OP_INSERT: {
            byte[][] pair = Node.retrieveKeyValueAtLoc(entry, 0);
            while ((activeIndex = findIndex(activeIndex)) != null) {
                try {
                    activeIndex.store(Transaction.BOGUS, pair[0], pair[1]);
                    break;
                } catch (ClosedIndexException e) {
                    // User closed the shared index reference, so re-open it.
                    activeIndex = null;
                }
            }
            break;
        }

        case OP_RECLAIM_FRAGMENTED:
            while ((activeIndex = findIndex(activeIndex)) != null) {
                try {
                    mDatabase.fragmentedTrash().remove(mTxnId, (Tree) activeIndex, entry);
                    break;
                } catch (ClosedIndexException e) {
                    // User closed the shared index reference, so re-open it.
                    activeIndex = null;
                }
            }
            break;
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

    /**
     * @param delete true to delete nodes
     * @return last pushed op, or 0 if empty
     */
    final byte peek(boolean delete) throws IOException {
        Node node = mNode;
        if (node == null) {
            return (mBuffer == null || mBufferPos >= mBuffer.length) ? 0 : mBuffer[mBufferPos];
        }

        node.acquireExclusive();
        while (true) {
            byte[] page = node.mPage;
            int pos = node.mGarbage;
            if (pos < page.length) {
                byte op = page[pos];
                node.releaseExclusive();
                return op;
            }
            if ((node = popNode(node, delete)) == null) {
                return 0;
            }
        }
    }

    /**
     * Caller must hold db commit lock.
     *
     * @param opRef element zero is filled in with the opcode
     * @param delete true to delete nodes
     * @return null if nothing left
     */
    private final byte[] pop(byte[] opRef, boolean delete) throws IOException {
        Node node = mNode;
        if (node == null) {
            byte[] buffer = mBuffer;
            if (buffer == null) {
                opRef[0] = 0;
                mLength = 0;
                return null;
            }
            int pos = mBufferPos;
            if (pos >= buffer.length) {
                opRef[0] = 0;
                mLength = 0;
                return null;
            }
            if ((opRef[0] = buffer[pos++]) < PAYLOAD_OP) {
                mBufferPos = pos;
                mLength -= 1;
                return EMPTY_BYTES;
            }
            int payloadLen = decodeUnsignedVarInt(buffer, pos);
            int varIntLen = calcUnsignedVarIntLength(payloadLen);
            pos += varIntLen;
            byte[] entry = new byte[payloadLen];
            arraycopy(buffer, pos, entry, 0, payloadLen);
            mBufferPos = pos += payloadLen;
            mLength -= 1 + varIntLen + payloadLen;
            return entry;
        }

        node.acquireExclusive();
        byte[] page;
        int pos;
        while (true) {
            page = node.mPage;
            pos = node.mGarbage;
            if (pos < page.length) {
                break;
            }
            if ((node = popNode(node, delete)) == null) {
                mLength = 0;
                return null;
            }
        }

        if ((opRef[0] = page[pos++]) < PAYLOAD_OP) {
            mLength -= 1;
            node.mGarbage = pos;
            if (pos >= page.length) {
                node = popNode(node, delete);
            }
            if (node != null) {
                node.releaseExclusive();
            }
            return EMPTY_BYTES;
        }

        int payloadLen;
        {
            payloadLen = decodeUnsignedVarInt(page, pos);
            int varIntLen = calcUnsignedVarIntLength(payloadLen);
            pos += varIntLen;
            mLength -= 1 + varIntLen + payloadLen;
        }

        byte[] entry = new byte[payloadLen];
        int entryPos = 0;

        while (true) {
            int avail = Math.min(payloadLen, page.length - pos);
            arraycopy(page, pos, entry, entryPos, avail);
            payloadLen -= avail;
            pos += avail;
            node.mGarbage = pos;

            if (pos >= page.length) {
                node = popNode(node, delete);
            }

            if (payloadLen <= 0) {
                if (node != null) {
                    node.releaseExclusive();
                }
                return entry;
            }

            if (node == null) {
                throw new CorruptDatabaseException("Remainder of undo log is missing");
            }

            page = node.mPage;
            pos = node.mGarbage;
            entryPos += avail;
        }
    }

    /**
     * @param parent latched parent node
     * @param delete true to delete the node too
     * @return current (latched) mNode; null if none left
     */
    private Node popNode(Node parent, boolean delete) throws IOException {
        Node lowerNode = latchLowerNode(parent);
        Database db = mDatabase;
        db.makeEvictable(parent);
        if (delete) {
            db.prepareToDelete(parent);
            // Safer to never recycle undo log nodes. Keep them until the next checkpoint, when
            // there's a guarantee that the master undo log will not reference them anymore.
            db.deleteNode(parent, false);
        } else {
            parent.releaseExclusive();
        }
        return mNode = lowerNode;
    }

    /**
     * @param parent latched parent node
     * @return null if none
     */
    private Node latchLowerNode(Node parent) throws IOException {
        long lowerNodeId = decodeLongLE(parent.mPage, I_LOWER_NODE_ID);
        if (lowerNodeId == 0) {
            return null;
        }

        Node lowerNode;
        Node[] childNodes = parent.mChildNodes;
        if (childNodes != null) {
            lowerNode = childNodes[0];
            lowerNode.acquireExclusive();
            if (lowerNodeId == lowerNode.mId) {
                mDatabase.makeUnevictable(lowerNode);
                return lowerNode;
            }
            lowerNode.releaseExclusive();
        }

        // Node was evicted, so reload it.
        return readUndoLogNode(mDatabase, lowerNodeId);
    }

    private static void writeEntry(byte[] dest, int destPos,
                                   byte op, byte[] payload, int off, int len)
    {
        dest[destPos] = op;
        if (op >= PAYLOAD_OP) {
            int payloadPos = encodeUnsignedVarInt(dest, destPos + 1, len);
            arraycopy(payload, off, dest, payloadPos, len);
        }
    }

    /**
     * Caller must hold db commit lock.
     */
    private Node allocUnevictableNode(long lowerNodeId) throws IOException {
        Node node = mDatabase.allocUnevictableNode();
        node.mType = Node.TYPE_UNDO_LOG;
        encodeLongLE(node.mPage, I_LOWER_NODE_ID, lowerNodeId);
        return node;
    }

    /**
     * Caller must hold exclusive db commit lock.
     *
     * @param workspace temporary buffer, allocated on demand
     * @return new workspace instance
     */
    final byte[] writeToMaster(UndoLog master, byte[] workspace) throws IOException {
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
            master.doPush(OP_LOG_COPY, workspace, 0, psize,
                          calcUnsignedVarIntLength(psize));
        } else {
            if (workspace == null) {
                workspace = new byte[INITIAL_BUFFER_SIZE];
            }
            writeHeaderToMaster(workspace);
            encodeLongLE(workspace, (8 + 8), mLength);
            encodeLongLE(workspace, (8 + 8 + 8), node.mId);
            encodeShortLE(workspace, (8 + 8 + 8 + 8), node.mGarbage);
            master.doPush(OP_LOG_REF, workspace, 0, (8 + 8 + 8 + 8 + 2), 1);
        }
        return workspace;
    }

    private void writeHeaderToMaster(byte[] workspace) {
        encodeLongLE(workspace, 0, mTxnId);
        encodeLongLE(workspace, 8, mActiveIndexId);
    }

    static UndoLog recoverMasterUndoLog(Database db, long nodeId) throws IOException {
        UndoLog log = new UndoLog(db, 0);
        // Length is not recoverable.
        log.mLength = Long.MAX_VALUE;
        (log.mNode = readUndoLogNode(db, nodeId)).releaseExclusive();
        return log;
    }

    /**
     * Recover transactions which were recorded by this master log, keyed by
     * transaction id. Recovered transactions have a NO_REDO durability mode.
     * All transactions are registered, and so they must be reset after
     * recovery is complete. Master log is truncated as a side effect of
     * calling this method.
     */
    void recoverTransactions(LHashTable.Obj<Transaction> txns,
                             LockMode lockMode, long timeoutNanos)
        throws IOException
    {
        byte[] opRef = new byte[1];
        byte[] entry;
        while ((entry = pop(opRef, true)) != null) {
            UndoLog log = recoverUndoLog(opRef[0], entry);
            Transaction txn = log.recoverTransaction(lockMode, timeoutNanos);

            // Reload the UndoLog, since recoverTransaction consumes it all.
            txn.recoveredUndoLog(recoverUndoLog(opRef[0], entry));

            txns.insert(log.mTxnId).value = txn;
        }
    }

    /**
     * Method consumes entire log as a side-effect.
     */
    private final Transaction recoverTransaction(LockMode lockMode, long timeoutNanos)
        throws IOException
    {
        byte[] opRef = new byte[1];
        Scope scope = new Scope();

        // Scopes are recovered in the opposite order in which they were
        // created. Gather them in a stack to reverse the order.
        Deque<Scope> scopes = new ArrayDeque<Scope>();
        scopes.addFirst(scope);

        boolean acquireLocks = true;
        int depth = 1;

        while (mLength > 0) {
            byte[] entry = pop(opRef, false);
            if (entry == null) {
                break;
            }

            byte op = opRef[0];
            switch (op) {
            default:
                throw new DatabaseException("Unknown undo log entry type: " + op);

            case OP_COMMIT:
            case OP_COMMIT_TRUNCATE:
                // Handled by Transaction.recoveryCleanup, but don't acquire
                // locks. This avoids deadlocks with later transactions.
                acquireLocks = false;
                break;

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

            case OP_DELETE:
                if (lockMode != LockMode.UNSAFE) {
                    scope.addLock(mActiveIndexId, entry);
                }
                break;

            case OP_UPDATE:
            case OP_INSERT:
            case OP_RECLAIM_FRAGMENTED:
                if (lockMode != LockMode.UNSAFE) {
                    scope.addLock(mActiveIndexId, Node.retrieveKeyAtLoc(entry, 0));
                }
                break;
            }
        }

        Transaction txn = new Transaction
            (mDatabase, mTxnId, lockMode, timeoutNanos,
             // Blindly assume trash must be deleted. No harm if none exists.
             Transaction.HAS_TRASH);

        scope = scopes.pollFirst();
        if (acquireLocks) {
            scope.acquireLocks(txn);
        }

        while ((scope = scopes.pollFirst()) != null) {
            txn.recoveredScope(scope.mSavepoint, Transaction.HAS_TRASH);
            if (acquireLocks) {
                scope.acquireLocks(txn);
            }
        }

        return txn;
    }

    /**
     * Recovered undo scope.
     */
    static class Scope {
        long mSavepoint;

        // Locks are recovered in the opposite order in which they were acquired. Gather them
        // in a stack to reverse the order. Re-use the LockManager collision chain field and
        // form a linked list.
        org.cojen.tupl.Lock mTopLock;

        Scope() {
        }

        void addLock(long indexId, byte[] key) {
            org.cojen.tupl.Lock lock = new org.cojen.tupl.Lock();
            lock.mIndexId = indexId;
            lock.mKey = key;
            lock.mHashCode = LockManager.hash(indexId, key);
            lock.mLockManagerNext = mTopLock;
            mTopLock = lock;
        }

        void acquireLocks(Transaction txn) throws LockFailureException {
            org.cojen.tupl.Lock lock = mTopLock;
            if (lock != null) while (true) {
                // Copy next before the field is overwritten.
                org.cojen.tupl.Lock next = lock.mLockManagerNext;
                txn.lockExclusive(lock);
                if (next == null) {
                    break;
                }
                mTopLock = lock = next;
            }
        }
    }

    /**
     * @param masterLogOp OP_LOG_COPY or OP_LOG_REF
     */
    private UndoLog recoverUndoLog(byte masterLogOp, byte[] masterLogEntry)
        throws IOException
    {
        if (masterLogOp != OP_LOG_COPY && masterLogOp != OP_LOG_REF) {
            throw new DatabaseException("Unknown undo log entry type: " + masterLogOp);
        }

        long txnId = decodeLongLE(masterLogEntry, 0);
        UndoLog log = new UndoLog(mDatabase, txnId);
        log.mActiveIndexId = decodeLongLE(masterLogEntry, 8);

        if (masterLogOp == OP_LOG_COPY) {
            int bsize = decodeUnsignedShortLE(masterLogEntry, (8 + 8));
            log.mLength = bsize;
            byte[] buffer = new byte[bsize];
            arraycopy(masterLogEntry, (8 + 8 + 2), buffer, 0, bsize);
            log.mBuffer = buffer;
            log.mBufferPos = 0;
        } else {
            log.mLength = decodeLongLE(masterLogEntry, (8 + 8));
            long nodeId = decodeLongLE(masterLogEntry, (8 + 8 + 8));
            int topEntry = decodeUnsignedShortLE(masterLogEntry, (8 + 8 + 8 + 8));
            log.mNode = readUndoLogNode(mDatabase, nodeId);
            log.mNode.mGarbage = topEntry;
            log.mNode.releaseExclusive();
        }

        return log;
    }

    /**
     * @return latched, unevictable node
     */
    private static Node readUndoLogNode(Database db, long nodeId) throws IOException {
        Node node = db.allocLatchedNode(false);
        node.read(db, nodeId);
        if (node.mType != Node.TYPE_UNDO_LOG) {
            throw new CorruptDatabaseException
                ("Not an undo log node type: " + node.mType + ", id: " + nodeId);
        }
        return node;
    }
}
