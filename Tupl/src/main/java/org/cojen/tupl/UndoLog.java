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

    private static final int I_LOWER_NODE_ID = 4;
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

    // Payload is key and value to store to undo an update or delete.
    static final byte OP_STORE = (byte) 6;

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
        DataIO.writeLong(payload, 0, txnId);
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

        doPush(op, payload, off, len, DataIO.calcUnsignedVarIntLength(len));
    }

    private void pushIndexId(long indexId) throws IOException {
        byte[] payload = new byte[8];
        DataIO.writeLong(payload, 0, indexId);
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
                DataIO.writeUnsignedVarInt(page, pos -= varIntLen, len);
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

                    switch (opRef[0]) {
                    default:
                        throw new DatabaseException("Unknown undo log entry type: " + opRef[0]);

                    case OP_TRANSACTION:
                        // Ignore.
                        break;

                    case OP_INDEX:
                        mActiveIndexId = DataIO.readLong(entry, 0);
                        activeIndex = null;
                        break;

                    case OP_DELETE:
                        activeIndex = findIndex(activeIndex);
                        activeIndex.delete(Transaction.BOGUS, entry);
                        break;

                    case OP_STORE:
                        activeIndex = findIndex(activeIndex);
                        {
                            byte[][] pair = Node.decodeUndoEntry(entry);
                            activeIndex.store(Transaction.BOGUS, pair[0], pair[1]);
                        }
                        break;
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
                if ((mActiveTxnId = DataIO.readLong(entry, 0)) == parentTxnId) {
                    break loop;
                }
                break;

            case OP_INDEX:
                mActiveIndexId = DataIO.readLong(entry, 0);
                break;

            case OP_DELETE:
            case OP_STORE:
                // Ignore.
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
                if ((mActiveTxnId = DataIO.readLong(entry, 0)) == parentTxnId) {
                    break loop;
                }
                break;

            case OP_INDEX:
                mActiveIndexId = DataIO.readLong(entry, 0);
                activeIndex = null;
                break;

            case OP_DELETE:
                activeIndex = findIndex(activeIndex);
                activeIndex.delete(Transaction.BOGUS, entry);
                break;

            case OP_STORE:
                activeIndex = findIndex(activeIndex);
                {
                    byte[][] pair = Node.decodeUndoEntry(entry);
                    activeIndex.store(Transaction.BOGUS, pair[0], pair[1]);
                }
                break;
            }
        }

        return true;
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
            int payloadLen = DataIO.readUnsignedVarInt(buffer, pos);
            int varIntLen = DataIO.calcUnsignedVarIntLength(payloadLen);
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
            payloadLen = DataIO.readUnsignedVarInt(page, pos);
            int varIntLen = DataIO.calcUnsignedVarIntLength(payloadLen);
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
                throw new CorruptNodeException("Remainder of undo log is missing");
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
        mDatabase.deleteNode(parent);
        return mNode = lowerNode;
    }

    /**
     * @return null if none
     */
    private Node latchLowerNode(Node parent) throws IOException {
        long lowerNodeId = DataIO.readLong(parent.mPage, I_LOWER_NODE_ID);
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
        }

        // Node was evicted, so reload it.
        lowerNode = mDatabase.allocLatchedNode();
        lowerNode.read(mDatabase, lowerNodeId);

        return lowerNode;
    }

    private static void writeEntry(byte[] dest, int destPos,
                                   byte op, byte[] payload, int off, int len)
    {
        dest[destPos] = op;
        int payloadPos = DataIO.writeUnsignedVarInt(dest, destPos + 1, len);
        System.arraycopy(payload, off, dest, payloadPos, len);
    }

    /**
     * Caller must hold commit lock.
     */
    private Node allocDirtyNode(long lowerNodeId) throws IOException {
        Node node = mDatabase.allocDirtyNode();
        node.mType = Node.TYPE_UNDO_LOG;
        DataIO.writeLong(node.mPage, I_LOWER_NODE_ID, lowerNodeId);
        return node;
    }

    final void gatherDirtyNodes(DirtyList dirtyList, int dirtyState) {
        Node node = mNode;
        if (node == null) {
            return;
        }

        // All node latches must be released except the first.
        boolean release = false;

        try {
            while (true) {
                if (node.mCachedState == dirtyState) {
                    dirtyList.append(node);
                }

                long lowerNodeId = DataIO.readLong(node.mPage, I_LOWER_NODE_ID);
                if (lowerNodeId == 0) {
                    break;
                }

                Node[] childNodes = node.mChildNodes;
                if (childNodes == null) {
                    break;
                }

                Node lowerNode = childNodes[0];
                lowerNode.acquireExclusive();
                if (lowerNodeId != lowerNode.mId) {
                    // Node and all remaining lower nodes were already evicted.
                    lowerNode.releaseExclusive();
                    break;
                }

                if (release) {
                    node.releaseExclusive();
                }
                node = lowerNode;
                release = true;
            }
        } finally {
            if (release) {
                node.releaseExclusive();
            }
        }
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
            DataIO.writeShort(workspace, (8 + 8), bsize);
            System.arraycopy(buffer, pos, workspace, (8 + 8 + 2), bsize);
            master.doPush(OP_LOG_COPY, workspace, 0, psize,
                          DataIO.calcUnsignedVarIntLength(psize));
        } else {
            if (workspace == null) {
                workspace = new byte[INITIAL_BUFFER_SIZE];
            }
            writeActiveIds(workspace);
            DataIO.writeLong(workspace, (8 + 8), node.mId);
            master.doPush(OP_LOG_REF, workspace, 0, (8 + 8 + 8), 1);
        }
        return workspace;
    }

    private void writeActiveIds(byte[] workspace) {
        DataIO.writeLong(workspace, 0, mActiveTxnId);
        DataIO.writeLong(workspace, 8, mActiveIndexId);
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
                int bsize = DataIO.readUnsignedShort(entry, (8 + 8));
                byte[] buffer = new byte[bsize];
                System.arraycopy(entry, (8 + 8 + 2), buffer, 0, bsize);
                log.mBuffer = buffer;
                log.mBufferPos = 0;
                break;
            }

            case OP_LOG_REF:
                setActiveIds(log, entry);
                long nodeId = DataIO.readLong(entry, (8 + 8));
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
        log.mActiveTxnId = DataIO.readLong(masterLogEntry, 0);
        log.mActiveIndexId = DataIO.readLong(masterLogEntry, 8);
    }

    private static Node readUndoLogNode(Database db, long nodeId) throws IOException {
        Node node = db.allocLatchedNode();
        node.read(db, nodeId);
        if (node.mType != Node.TYPE_UNDO_LOG) {
            throw new CorruptNodeException("Not an undo log node type: " + node.mType);
        }
        return node;
    }
}
