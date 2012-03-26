/*
 *  Copyright 2011 Brian S O'Neill
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

import java.util.List;

import java.util.concurrent.locks.Lock;

import static org.cojen.tupl.DataUtils.*;

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
      UndoLog is persisted in Nodes. All multibyte types are big endian encoded.

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

    // Copy to another log from master log. Payload is active transaction id,
    // index id, buffer size (short type), and serialized buffer.
    private static final byte OP_LOG_COPY = (byte) 1;

    // Reference to another log from master log. Payload is active transaction
    // id, index id, and node id.
    private static final byte OP_LOG_REF = (byte) 2;

    // Payload is active transaction id.
    private static final byte OP_TRANSACTION = (byte) 3;

    // Payload is active index id.
    private static final byte OP_INDEX = (byte) 4;

    // Payload is key to delete to undo an insert.
    static final byte OP_DELETE = (byte) 5;

    // Payload is key and value to store to undo an update.
    static final byte OP_UPDATE = (byte) 6;

    // Payload is key and value to store to undo a delete.
    static final byte OP_INSERT = (byte) 7;

    private final Database mDatabase;

    private byte[] mBuffer;
    private int mBufferPos;

    // Top node, always latched. This prevents it from being evicted. Nodes are
    // not used for logs which fit into local buffer.
    Node mNode;

    // Number of bytes currently pushed into log.
    private long mLength;

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
     * Caller must hold commit lock.
     */
    static UndoLog newMasterUndoLog(Database db) throws IOException {
        UndoLog log = new UndoLog(db);
        log.persistReady();
        return log;
    }

    static UndoLog recoverMasterUndoLog(Database db, long nodeId) throws IOException {
        UndoLog log = new UndoLog(db);
        log.mNode = readUndoLogNode(db, nodeId);
        return log;
    }

    private UndoLog(Database db) {
        mDatabase = db;
    }

    /**
     * Ensures all entries are stored in persistable nodes. Caller must hold
     * commit lock.
     */
    private void persistReady() throws IOException {
        Node node;
        byte[] buffer = mBuffer;
        if (buffer == null) {
            mNode = node = allocDirtyNode(0);
            // Set pointer to top entry (none at the moment).
            node.mGarbage = node.mPage.length;
        } else if (mNode == null) {
            mNode = node = allocDirtyNode(0);
            int pos = mBufferPos;
            int size = buffer.length - pos;
            byte[] page = node.mPage;
            int newPos = page.length - size;
            System.arraycopy(buffer, pos, page, newPos, size);
            // Set pointer to top entry.
            node.mGarbage = newPos;
            mBuffer = null;
            mBufferPos = 0;
        }
    }

    /**
     * Caller must hold commit lock.
     */
    final long activeTransactionId() {
        return mActiveTxnId;
    }

    /**
     * Caller must hold commit lock.
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
        writeLong(payload, 0, txnId);
        doPush(OP_TRANSACTION, payload, 0, 8, 1);
    }

    /**
     * Caller must hold commit lock.
     */
    final void push(long indexId, byte op, byte[] payload) throws IOException {
        push(indexId, op, payload, 0, payload.length);
    }

    /**
     * Caller must hold commit lock.
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
        writeLong(payload, 0, indexId);
        doPush(OP_INDEX, payload, 0, 8, 1);
    }

    /**
     * Caller must hold commit lock.
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
                    mNode = node = allocDirtyNode(0);
                    // Set pointer to top entry (none at the moment).
                    node.mGarbage = pageSize;
                    break quick;
                }
            } else {
                pos = mBufferPos;
                if (pos < encodedLen) {
                    int size = buffer.length - pos;
                    int newCap = Math.max(buffer.length << 1, Utils.roundUpPower2(encodedLen));
                    if (newCap <= (mDatabase.pageSize() >> 1)) {
                        byte[] newBuf = new byte[newCap];
                        int newPos = newCap - size;
                        System.arraycopy(buffer, pos, newBuf, newPos, size);
                        mBuffer = buffer = newBuf;
                        mBufferPos = pos = newPos;
                    } else {
                        // Required capacity is large, so just use a node.
                        mNode = node = allocDirtyNode(0);
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
                break;
            }

            Node newNode;
            {
                Node[] childNodes = new Node[] {node};
                newNode = allocDirtyNode(node.mId);
                newNode.mChildNodes = childNodes;
                newNode.mGarbage = pos = page.length;
                available = pos - HEADER_SIZE;
            }

            node.releaseExclusive();
            mNode = node = newNode;
        }
    }

    /**
     * Caller does not need to hold commit lock.
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
     * rolled back. Caller does not need to hold commit lock.
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
     * Truncate all log entries. Caller does not need to hold commit lock.
     */
    final void truncate() throws IOException {
        if (mLength > 0) {
            final Lock sharedCommitLock = mDatabase.sharedCommitLock();
            sharedCommitLock.lock();
            try {
                Node node = mNode;
                if (node == null) {
                    mBufferPos = mBuffer.length;
                } else {
                    while ((node = popNode(node)) != null);
                }
                mLength = 0;
                mActiveIndexId = 0;
            } finally {
                sharedCommitLock.unlock();
            }
        }

        mActiveTxnId = 0;
    }

    /**
     * Rollback all log entries to the given savepoint. Pass zero to rollback
     * everything. Caller does not need to hold commit lock.
     */
    final void rollback(long savepoint) throws IOException {
        // Implementation could be optimized somewhat, resulting in less
        // temporary arrays and copies. Rollback optimization is generally not
        // necessary, since most transactions are expected to commit.

        if (savepoint < mLength) {
            final Lock sharedCommitLock = mDatabase.sharedCommitLock();
            sharedCommitLock.lock();
            try {
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
            } finally {
                sharedCommitLock.unlock();
            }
        }

        mActiveTxnId = 0;
    }

    /**
     * Truncate to an enclosing parent scope, as required by master log during
     * recovery. Caller must hold commit lock or be performing recovery.
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
                if ((mActiveTxnId = readLong(entry, 0)) == parentTxnId) {
                    break loop;
                }
                break;

            case OP_INDEX:
                mActiveIndexId = readLong(entry, 0);
                break;

            case OP_DELETE:
            case OP_UPDATE:
                // Ignore.
                break;

            case OP_INSERT:
                // FIXME: OP_INSERT must delete tombstone
                break;
            }
        }

        return true;
    }

    /**
     * Rollback to an enclosing parent scope, as required by master log during
     * recovery. Caller must hold commit lock or be performing recovery.
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
                if ((mActiveTxnId = readLong(entry, 0)) == parentTxnId) {
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

        case OP_INDEX:
            mActiveIndexId = readLong(entry, 0);
            activeIndex = null;
            break;

        case OP_DELETE:
            activeIndex = findIndex(activeIndex);
            activeIndex.delete(Transaction.BOGUS, entry);
            break;

        case OP_UPDATE: case OP_INSERT:
            activeIndex = findIndex(activeIndex);
            {
                byte[][] pair = Node.decodeUndoEntry(entry);
                activeIndex.store(Transaction.BOGUS, pair[0], pair[1]);
            }
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
     * Caller must hold commit lock.
     *
     * @param opRef element zero is filled in with the opcode
     * @return null if nothing left
     */
    // TODO: private
    final byte[] pop(byte[] opRef) throws IOException {
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

        byte[] page = node.mPage;
        int pos = node.mGarbage;

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
     * @return current mNode; null if none left
     */
    private Node popNode(Node parent) throws IOException {
        Node lowerNode = latchLowerNode(parent);
        mDatabase.deleteNode(null, parent);
        return mNode = lowerNode;
    }

    /**
     * @return null if none
     */
    private Node latchLowerNode(Node parent) throws IOException {
        long lowerNodeId = readLong(parent.mPage, I_LOWER_NODE_ID);
        if (lowerNodeId == 0) {
            return null;
        }

        Node lowerNode;
        Node[] childNodes = parent.mChildNodes;
        if (childNodes != null) {
            lowerNode = childNodes[0];
            lowerNode.acquireExclusive();
            if (lowerNodeId == lowerNode.mId) {
                return lowerNode;
            }
            lowerNode.releaseExclusive();
        }

        // Node was evicted, so reload it.
        lowerNode = mDatabase.allocLatchedNode();
        lowerNode.read(mDatabase, lowerNodeId);
        if (lowerNode.mType != Node.TYPE_UNDO_LOG) {
            throw new CorruptDatabaseException("Not an undo log node type: " + lowerNode.mType);
        }

        return lowerNode;
    }

    private static void writeEntry(byte[] dest, int destPos,
                                   byte op, byte[] payload, int off, int len)
    {
        dest[destPos] = op;
        int payloadPos = writeUnsignedVarInt(dest, destPos + 1, len);
        System.arraycopy(payload, off, dest, payloadPos, len);
    }

    /**
     * Caller must hold commit lock.
     */
    private Node allocDirtyNode(long lowerNodeId) throws IOException {
        Node node = mDatabase.allocDirtyNode(null);
        node.mType = Node.TYPE_UNDO_LOG;
        writeLong(node.mPage, I_LOWER_NODE_ID, lowerNodeId);
        return node;
    }

    /**
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
                workspace = new byte[Math.min(INITIAL_BUFFER_SIZE, Utils.roundUpPower2(psize))];
            }
            writeActiveIds(workspace);
            writeShort(workspace, (8 + 8), bsize);
            System.arraycopy(buffer, pos, workspace, (8 + 8 + 2), bsize);
            master.doPush(OP_LOG_COPY, workspace, 0, psize,
                          calcUnsignedVarIntLength(psize));
        } else {
            if (workspace == null) {
                workspace = new byte[INITIAL_BUFFER_SIZE];
            }
            writeActiveIds(workspace);
            writeLong(workspace, (8 + 8), node.mId);
            master.doPush(OP_LOG_REF, workspace, 0, (8 + 8 + 8), 1);
        }
        return workspace;
    }

    private void writeActiveIds(byte[] workspace) {
        writeLong(workspace, 0, mActiveTxnId);
        writeLong(workspace, 8, mActiveIndexId);
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
                int bsize = readUnsignedShort(entry, (8 + 8));
                byte[] buffer = new byte[bsize];
                System.arraycopy(entry, (8 + 8 + 2), buffer, 0, bsize);
                log.mBuffer = buffer;
                log.mBufferPos = 0;
                break;
            }

            case OP_LOG_REF:
                setActiveIds(log, entry);
                long nodeId = readLong(entry, (8 + 8));
                log.mNode = readUndoLogNode(mDatabase, nodeId);
                break;
            }

            logs.insert(log.mActiveTxnId).value = log;
        }

        return logs;
    }

    /**
     * Rollback all remaining undo log entries and delete this master log.
     */
    boolean rollbackRemaining(LHashTable.Obj<UndoLog> logs) throws IOException {
        boolean any = logs.size() > 0;

        if (any) {
            logs.traverse(new LHashTable.Vistor<LHashTable.ObjEntry<UndoLog>, IOException>() {
                public void visit(LHashTable.ObjEntry<UndoLog> entry) throws IOException {
                    entry.value.rollback(0);
                }
            });
        }

        // Delete this master log.
        truncate();

        return any;
    }

    private static void setActiveIds(UndoLog log, byte[] masterLogEntry) {
        log.mActiveTxnId = readLong(masterLogEntry, 0);
        log.mActiveIndexId = readLong(masterLogEntry, 8);
    }

    private static Node readUndoLogNode(Database db, long nodeId) throws IOException {
        Node node = db.allocLatchedNode();
        node.read(db, nodeId);
        if (node.mType != Node.TYPE_UNDO_LOG) {
            throw new CorruptDatabaseException("Not an undo log node type: " + node.mType);
        }
        return node;
    }
}
