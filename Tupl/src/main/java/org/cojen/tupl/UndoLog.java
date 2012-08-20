/*
 *  Copyright 2011-2012 Brian S O'Neill
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

import java.util.concurrent.locks.Lock;

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
      header. Each entry is composed of three sections:

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

    // Indicates that transaction has been committed.
    private static final byte OP_COMMIT = (byte) 1;

    // Indicates that transaction has been committed and log is partially truncated.
    private static final byte OP_COMMIT_TRUNCATE = (byte) 2;

    // Copy to another log from master log. Payload is active transaction id,
    // index id, buffer size (short type), and serialized buffer.
    private static final byte OP_LOG_COPY = (byte) 3;

    // Reference to another log from master log. Payload is active transaction
    // id, index id, node id, and top entry offset.
    private static final byte OP_LOG_REF = (byte) 4;

    // Payload is active transaction id.
    private static final byte OP_TRANSACTION = (byte) 5;

    // Payload is active index id.
    private static final byte OP_INDEX = (byte) 6;

    // Payload is key to delete to undo an insert.
    static final byte OP_DELETE = (byte) 7;

    // Payload is key and value to store to undo an update.
    static final byte OP_UPDATE = (byte) 8;

    // Payload is key and value to store to undo a delete.
    static final byte OP_INSERT = (byte) 9;

    // Payload is key and trash id to undo a fragmented value delete.
    static final byte OP_RECLAIM_FRAGMENTED = (byte) 10;

    private final Database mDatabase;

    // Number of bytes currently pushed into log.
    private long mLength;

    // Except for mLength, all field modifications during normal usage must be
    // performed while holding shared db commit lock. See writeToMaster method.

    private byte[] mBuffer;
    private int mBufferPos;

    // Top node, if required. Nodes are not used for logs which fit into local buffer.
    private Node mNode;

    private long mActiveTxnId;
    private long mActiveIndexId;

    /**
     * Returns a new registered UndoLog.
     */
    static UndoLog newUndoLog(Database db, long txnId) {
        UndoLog log = new UndoLog(db);
        log.mActiveTxnId = db.register(log, txnId);
        return log;
    }

    /**
     * Caller must hold db commit lock.
     */
    static UndoLog newMasterUndoLog(Database db) throws IOException {
        UndoLog log = new UndoLog(db);
        log.persistReady();
        return log;
    }

    static UndoLog recoverMasterUndoLog(Database db, long nodeId) throws IOException {
        UndoLog log = new UndoLog(db);
        (log.mNode = readUndoLogNode(db, nodeId)).releaseExclusive();
        return log;
    }

    private UndoLog(Database db) {
        mDatabase = db;
    }

    /**
     * Ensures all entries are stored in persistable nodes. Caller must hold db
     * commit lock.
     */
    private void persistReady() throws IOException {
        Node node;
        byte[] buffer = mBuffer;
        if (buffer == null) {
            mNode = node = allocUnevictableNode(0);
            // Set pointer to top entry (none at the moment).
            node.mGarbage = node.mPage.length;
            node.releaseExclusive();
        } else if (mNode == null) {
            mNode = node = allocUnevictableNode(0);
            int pos = mBufferPos;
            int size = buffer.length - pos;
            byte[] page = node.mPage;
            int newPos = page.length - size;
            System.arraycopy(buffer, pos, page, newPos, size);
            // Set pointer to top entry.
            node.mGarbage = newPos;
            mBuffer = null;
            mBufferPos = 0;
            node.releaseExclusive();
        }
    }

    long topNodeId() {
        return mNode.mId;
    }

    /**
     * Caller must hold db commit lock.
     */
    final long activeTransactionId() {
        return mActiveTxnId;
    }

    /**
     * Caller must hold db commit lock.
     */
    final void activeTransactionId(long txnId) throws IOException {
        long active = mActiveTxnId;
        if (txnId != active) {
            pushTransactionId(active);
            mActiveTxnId = txnId;
        }
    }

    private void pushTransactionId(long txnId) throws IOException {
        byte[] payload = new byte[8];
        writeLongLE(payload, 0, txnId);
        doPush(OP_TRANSACTION, payload, 0, 8, 1);
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
        writeLongLE(payload, 0, indexId);
        doPush(OP_INDEX, payload, 0, 8, 1);
    }

    /**
     * Caller must hold db commit lock.
     */
    void pushCommit() throws IOException {
        doPush(OP_COMMIT, Utils.EMPTY_BYTES, 0, 0, 1); 
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
                int newCap = Math.max(INITIAL_BUFFER_SIZE, Utils.roundUpPower2(encodedLen));
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
                        (buffer.length << 1, Utils.roundUpPower2(encodedLen + size));
                    if (newCap <= (mDatabase.pageSize() >> 1)) {
                        byte[] newBuf = new byte[newCap];
                        int newPos = newCap - size;
                        System.arraycopy(buffer, pos, newBuf, newPos, size);
                        mBuffer = buffer = newBuf;
                        mBufferPos = pos = newPos;
                    } else {
                        // Required capacity is large, so just use a node.
                        mNode = node = allocUnevictableNode(0);
                        byte[] page = node.mPage;
                        int newPos = page.length - size;
                        System.arraycopy(buffer, pos, page, newPos, size);
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
            System.arraycopy(payload, off + remaining, page, pos, amt);
            node.mGarbage = pos;

            if (remaining <= 0 && available >= (1 + varIntLen)) {
                writeUnsignedVarInt(page, pos -= varIntLen, len);
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
     */
    final long savepoint() {
        final Lock sharedCommitLock = mDatabase.sharedCommitLock();
        sharedCommitLock.lock();
        try {
            mActiveTxnId = 0;
            return mLength;
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
                    while ((node = popNode(node)) != null) {
                        if (commit) {
                            // When shared lock is released, log can be
                            // checkpointed in an incomplete state. Update the
                            // top node to indicate that undo log is committed.
                            mDatabase.redirty(node);
                            byte[] page = node.mPage;
                            int end = page.length - 1;
                            node.mGarbage = end;
                            page[end] = OP_COMMIT_TRUNCATE;
                        }
                        // Release and re-acquire, to unblock any threads
                        // waiting for checkpoint to begin.
                        sharedCommitLock.unlock();
                        sharedCommitLock.lock();
                    }
                }
                mLength = 0;
                mActiveIndexId = 0;
            }

            mActiveTxnId = 0;
        } finally {
            sharedCommitLock.unlock();
        }
    }

    /**
     * Rollback all log entries to the given savepoint. Pass zero to rollback
     * everything. Caller does not need to hold db commit lock.
     */
    final void rollback(long savepoint) throws IOException {
        if (savepoint == mLength) {
            // Nothing to rollback, so return quickly.
            return;
        }

        // Implementation could be optimized somewhat, resulting in less
        // temporary arrays and copies. Rollback optimization is generally not
        // necessary, since most transactions are expected to commit.

        final Lock sharedCommitLock = mDatabase.sharedCommitLock();
        sharedCommitLock.lock();
        try {
            if (savepoint < mLength) {
                byte[] opRef = new byte[1];
                Index activeIndex = null;
                do {
                    byte[] entry = pop(opRef);
                    if (entry == null) {
                        break;
                    }
                    byte op = opRef[0];
                    if (op == OP_TRANSACTION) {
                        // Ignore.
                    } else {
                        activeIndex = undo(activeIndex, op, entry);
                    }
                } while (savepoint < mLength);
            }

            mActiveTxnId = 0;
        } finally {
            sharedCommitLock.unlock();
        }
    }

    /**
     * Truncate all log entries, and delete any ghosts that were created. Only
     * to be called during recovery.
     */
    final void deleteGhosts() throws IOException {
        if (mLength > 0) {
            byte[] opRef = new byte[1];
            Index activeIndex = null;
            do {
                byte[] entry = pop(opRef);
                if (entry == null) {
                    break;
                }

                byte op = opRef[0];
                switch (op) {
                default:
                    throw new DatabaseException("Unknown undo log entry type: " + op);

                case OP_COMMIT:
                case OP_COMMIT_TRUNCATE:
                case OP_TRANSACTION:
                case OP_DELETE:
                case OP_UPDATE:
                case OP_RECLAIM_FRAGMENTED:
                    // Ignore.
                    break;

                case OP_INDEX:
                    mActiveIndexId = readLongLE(entry, 0);
                    activeIndex = null;
                    break;

                case OP_INSERT:
                    // Since transaction was committed, don't insert an entry
                    // to undo a delete, but instead delete the ghost.
                    activeIndex = findIndex(activeIndex);
                    byte[] key = Node.retrieveKeyAtLoc(entry, 0);
                    TreeCursor cursor = new TreeCursor((Tree) activeIndex, null);
                    try {
                        cursor.deleteGhost(key);
                        cursor.reset();
                    } catch (Throwable e) {
                        throw Utils.closeOnFailure(cursor, e);
                    }
                    break;
                }
            } while (mLength > 0);
        }

        mActiveTxnId = 0;
    }

    /**
     * Truncate to an enclosing parent scope, as required by master log during
     * recovery. Caller must hold db commit lock or be performing recovery.
     *
     * @param txnId expected active transaction id
     * @param parentTxnId new active transaction to be
     * @return false if not in expected active transaction scope
     */
    final boolean truncateScope(long txnId, long parentTxnId) throws IOException {
        if (txnId != mActiveTxnId) {
            return false;
        }

        if (parentTxnId == mActiveTxnId) {
            return true;
        }

        byte[] opRef = new byte[1];
        loop: while (true) {
            byte[] entry = pop(opRef);

            if (entry == null) {
                mActiveTxnId = 0;
                break loop;
            }

            switch (opRef[0]) {
            default:
                throw new DatabaseException("Unknown undo log entry type: " + opRef[0]);

            case OP_TRANSACTION:
                if ((mActiveTxnId = readLongLE(entry, 0)) == parentTxnId) {
                    break loop;
                }
                break;

            case OP_INDEX:
                mActiveIndexId = readLongLE(entry, 0);
                break;

            case OP_DELETE:
            case OP_UPDATE:
            case OP_INSERT:
            case OP_RECLAIM_FRAGMENTED:
            case OP_COMMIT:
            case OP_COMMIT_TRUNCATE:
                // Ignore.
                break;
            }
        }

        return true;
    }

    /**
     * Rollback to an enclosing parent scope, as required by master log during
     * recovery. Caller must hold db commit lock or be performing recovery.
     *
     * @param txnId expected active transaction id
     * @param parentTxnId new active transaction to be
     * @return false if not in expected active transaction scope
     */
    final boolean rollbackScope(long txnId, long parentTxnId) throws IOException {
        if (txnId != mActiveTxnId) {
            return false;
        }

        if (parentTxnId == mActiveTxnId) {
            return true;
        }

        byte[] opRef = new byte[1];
        Index activeIndex = null;
        while (true) {
            byte[] entry = pop(opRef);
            if (entry == null) {
                mActiveTxnId = 0;
                break;
            }
            byte op = opRef[0];
            if (op == OP_TRANSACTION) {
                if ((mActiveTxnId = readLongLE(entry, 0)) == parentTxnId) {
                    break;
                }
            } else {
                activeIndex = undo(activeIndex, op, entry);
            }
        }

        return true;
    }

    /**
     * @param activeIndex active index, possibly null
     * @param op undo op, not OP_TRANSACTION
     * @return new active index, possibly null
     */
    private Index undo(Index activeIndex, byte op, byte[] entry) throws IOException {
        switch (op) {
        default:
            throw new DatabaseException("Unknown undo log entry type: " + op);

        case OP_TRANSACTION:
            // Caller should have already handled this
            throw new AssertionError();

        case OP_COMMIT:
        case OP_COMMIT_TRUNCATE:
            // Only needs to be processed by processRemaining.
            break;

        case OP_INDEX:
            mActiveIndexId = readLongLE(entry, 0);
            activeIndex = null;
            break;

        case OP_DELETE:
            activeIndex = findIndex(activeIndex);
            activeIndex.delete(Transaction.BOGUS, entry);
            break;

        case OP_UPDATE:
        case OP_INSERT:
            activeIndex = findIndex(activeIndex);
            {
                byte[][] pair = Node.retrieveKeyValueAtLoc(entry, 0);
                activeIndex.store(Transaction.BOGUS, pair[0], pair[1]);
            }
            break;

        case OP_RECLAIM_FRAGMENTED:
            activeIndex = findIndex(activeIndex);
            mDatabase.fragmentedTrash().remove(mActiveTxnId, (Tree) activeIndex, entry);
            break;
        }

        return activeIndex;
    }

    private Index findIndex(Index activeIndex) throws IOException {
        if (activeIndex == null) {
            if ((activeIndex = mDatabase.indexById(mActiveIndexId)) == null) {
                throw new DatabaseException("Index not found: " + mActiveIndexId);
            }
        }
        return activeIndex;
    }

    /**
     * @return last pushed op, or 0 if empty
     */
    final byte peek() throws IOException {
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
            if ((node = popNode(node)) == null) {
                return 0;
            }
        }
    }

    /**
     * Caller must hold db commit lock.
     *
     * @param opRef element zero is filled in with the opcode
     * @return null if nothing left
     */
    private final byte[] pop(byte[] opRef) throws IOException {
        Node node = mNode;
        if (node == null) {
            byte[] buffer = mBuffer;
            if (buffer == null) {
                opRef[0] = 0;
                return null;
            }
            int pos = mBufferPos;
            if (pos >= buffer.length) {
                opRef[0] = 0;
                return null;
            }
            opRef[0] = buffer[pos++];
            int payloadLen = readUnsignedVarInt(buffer, pos);
            int varIntLen = calcUnsignedVarIntLength(payloadLen);
            pos += varIntLen;
            byte[] entry = new byte[payloadLen];
            System.arraycopy(buffer, pos, entry, 0, payloadLen);
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
            if ((node = popNode(node)) == null) {
                return null;
            }
        }

        int payloadLen;
        {
            opRef[0] = page[pos++];
            payloadLen = readUnsignedVarInt(page, pos);
            int varIntLen = calcUnsignedVarIntLength(payloadLen);
            pos += varIntLen;
            mLength -= 1 + varIntLen + payloadLen;
        }

        byte[] entry = new byte[payloadLen];
        int entryPos = 0;

        while (true) {
            int avail = Math.min(payloadLen, page.length - pos);
            System.arraycopy(page, pos, entry, entryPos, avail);
            payloadLen -= avail;
            pos += avail;
            node.mGarbage = pos;

            if (pos >= page.length) {
                node = popNode(node);
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
     * @return current (latched) mNode; null if none left
     */
    private Node popNode(Node parent) throws IOException {
        Node lowerNode = latchLowerNode(parent);
        Database db = mDatabase;
        db.makeEvictable(parent);
        db.prepareToDelete(parent);
        db.deleteNode(null, parent);
        return mNode = lowerNode;
    }

    /**
     * @param parent latched parent node
     * @return null if none
     */
    private Node latchLowerNode(Node parent) throws IOException {
        long lowerNodeId = readLongLE(parent.mPage, I_LOWER_NODE_ID);
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
        int payloadPos = writeUnsignedVarInt(dest, destPos + 1, len);
        System.arraycopy(payload, off, dest, payloadPos, len);
    }

    /**
     * Caller must hold db commit lock.
     */
    private Node allocUnevictableNode(long lowerNodeId) throws IOException {
        Node node = mDatabase.allocUnevictableNode(null);
        node.mType = Node.TYPE_UNDO_LOG;
        writeLongLE(node.mPage, I_LOWER_NODE_ID, lowerNodeId);
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
                workspace = new byte[Math.max(INITIAL_BUFFER_SIZE, Utils.roundUpPower2(psize))];
            }
            writeActiveIds(workspace);
            writeShortLE(workspace, (8 + 8), bsize);
            System.arraycopy(buffer, pos, workspace, (8 + 8 + 2), bsize);
            master.doPush(OP_LOG_COPY, workspace, 0, psize,
                          calcUnsignedVarIntLength(psize));
        } else {
            if (workspace == null) {
                workspace = new byte[INITIAL_BUFFER_SIZE];
            }
            writeActiveIds(workspace);
            writeLongLE(workspace, (8 + 8), node.mId);
            writeShortLE(workspace, (8 + 8 + 8), node.mGarbage);
            master.doPush(OP_LOG_REF, workspace, 0, (8 + 8 + 8 + 2), 1);
        }
        return workspace;
    }

    private void writeActiveIds(byte[] workspace) {
        writeLongLE(workspace, 0, mActiveTxnId);
        writeLongLE(workspace, 8, mActiveIndexId);
    }

    /**
     * Recover UndoLog instances which were written to this master log, keyed
     * by active transaction id. Length of UndoLog is not restored, and so logs
     * can only be used for recovery.
     */
    LHashTable.Obj<UndoLog> recoverLogs() throws IOException {
        LHashTable.Obj<UndoLog> logs = new LHashTable.Obj<UndoLog>(16);

        byte[] opRef = new byte[1];
        byte[] entry;
        while ((entry = pop(opRef)) != null) {
            UndoLog log = new UndoLog(mDatabase);
            log.mLength = Long.MAX_VALUE;

            switch (opRef[0]) {
            default:
                throw new DatabaseException("Unknown undo log entry type: " + opRef[0]);

            case OP_LOG_COPY: {
                setActiveIds(log, entry);
                int bsize = readUnsignedShortLE(entry, (8 + 8));
                byte[] buffer = new byte[bsize];
                System.arraycopy(entry, (8 + 8 + 2), buffer, 0, bsize);
                log.mBuffer = buffer;
                log.mBufferPos = 0;
                break;
            }

            case OP_LOG_REF:
                setActiveIds(log, entry);
                long nodeId = readLongLE(entry, (8 + 8));
                int topEntry = readUnsignedShortLE(entry, (8 + 8 + 8));
                log.mNode = readUndoLogNode(mDatabase, nodeId);
                log.mNode.mGarbage = topEntry;
                log.mNode.releaseExclusive();
                break;
            }

            logs.insert(log.mActiveTxnId).value = log;
        }

        return logs;
    }

    /**
     * Rollback or truncate all remaining undo log entries and delete this
     * master log.
     */
    boolean processRemaining(LHashTable.Obj<UndoLog> logs) throws IOException {
        boolean any = logs.size() > 0;

        if (any) {
            logs.traverse(new LHashTable.Vistor<LHashTable.ObjEntry<UndoLog>, IOException>() {
                public void visit(LHashTable.ObjEntry<UndoLog> entry) throws IOException {
                    UndoLog undo = entry.value;
                    switch (undo.peek()) {
                    default:
                        undo.rollback(0);
                        break;

                    case OP_COMMIT:
                        // Transaction was actually committed, but redo log is
                        // gone. This can happen when a checkpoint completes in
                        // the middle of the transaction commit operation.
                        undo.deleteGhosts();
                        break;

                    case OP_COMMIT_TRUNCATE:
                        // Like OP_COMMIT, but ghosts have already been deleted.
                        undo.truncate(false);
                        break;
                    }
                }
            });
        }

        // Delete this master log.
        truncate(false);

        return any;
    }

    private static void setActiveIds(UndoLog log, byte[] masterLogEntry) {
        log.mActiveTxnId = readLongLE(masterLogEntry, 0);
        log.mActiveIndexId = readLongLE(masterLogEntry, 8);
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
