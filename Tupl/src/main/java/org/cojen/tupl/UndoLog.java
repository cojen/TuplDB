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

import java.util.concurrent.locks.Lock;

/**
 * Specialized stack used by UndoLog.
 *
 * @author Brian S O'Neill
 */
final class UndoLog {
    /*
      FIXME: notes

      - Link UndoLog instances together for registration
      - Consider using registration lock also for assigning txnId (no atomic long)
      - Mark node as dirty whenever making changes to it
      - Checkpoint first iterates all UndoLogs and sets commit state (mark phase)
      - All marked UndoLogs are converted to Node form and flushed
      - Consider using Node latches for concurrency control during flush
      - Index id is encoded as an op on top, at checkpoint
      - Transaction id is encoded as an op on top, at checkpoint
    */

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
      | stack entries                          |
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
      
      During payload break up, node allocation might fail, leaving the undo
      stack broken. To handle this case, opcode 0 is reserved for partially
      written entries which must be discarded.
    */

    private static final int HEADER_SIZE = 12;

    // Must be power of two.
    private static final int INITIAL_BUFFER_SIZE = 128;

    // Payload is active transaction id.
    private static final byte OP_TRANSACTION = (byte) 1;

    // Payload is active index id.
    private static final byte OP_INDEX = (byte) 2;

    // Payload is key to delete to undo an insert.
    static final byte OP_DELETE = (byte) 3;

    // Payload is key and value to store to undo an update or delete.
    static final byte OP_STORE = (byte) 4;

    private final Database mDatabase;

    private byte[] mBuffer;
    private int mBufferPos;

    // Top node, always latched. This prevents it from being evicted. Nodes are
    // not used for stacks which fit into local buffer.
    private Node mNode;

    // Number of bytes currently pushed into log.
    private long mLength;

    private long mActiveIndexId;

    UndoLog(Database db) {
        mDatabase = db;
    }

    /**
     * Caller must hold commit lock.
     *
     * @param op non-zero opcode
     */
    void push(long indexId, byte op, byte[] payload) throws IOException {
        push(indexId, op, payload, 0, payload.length);
    }

    /**
     * Caller must hold commit lock.
     *
     * @param op non-zero opcode
     */
    void push(final long indexId,
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

        push(op, payload, off, len, DataIO.calcUnsignedVarIntLength(len));
    }

    /**
     * Caller must hold commit lock.
     *
     * @param op non-zero opcode
     */
    private void push(final byte op, final byte[] payload, final int off, final int len,
                      int varIntLen)
        throws IOException
    {
        final int encodedLen = 1 + varIntLen + len;
        mLength += encodedLen;

        // Try to push into a local buffer first.
        Node node = mNode;
        quick: if (node == null) {
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
                    mNode = node = newDirtyNode(0);
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
                        mNode = node = newDirtyNode(0);
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
                newNode = newDirtyNode(node.mId);
                newNode.mChildNodes = childNodes;
                newNode.mGarbage = pos = page.length;
                available = pos - HEADER_SIZE;
            }

            node.releaseExclusive();
            mNode = node = newNode;
        }
    }

    private void pushIndexId(long indexId) throws IOException {
        byte[] payload = new byte[8];
        DataIO.writeLong(payload, 0, indexId);
        push(OP_INDEX, payload, 0, 8, 1);
    }

    final void pushTransactionId(long txnId) throws IOException {
        byte[] payload = new byte[8];
        DataIO.writeLong(payload, 0, txnId);
        push(OP_TRANSACTION, payload, 0, 8, 1);
    }

    final long savepoint() {
        return mLength;
    }

    /**
     * Truncate all log entries to the given savepoint. Pass zero to truncate
     * everything. Caller must hold commit lock.
     */
    final void truncate(long savepoint) throws IOException {
        Node node = mNode;
        if (node == null) {
            mBufferPos = mBuffer.length - (int) savepoint;
        } else if (savepoint == 0) {
            while ((node = popNode(node)) != null);
        } else {
            int pageSize = mDatabase.pageSize();
            long amount = mLength - savepoint;
            while (true) {
                int size = pageSize - node.mGarbage;
                if (size >= amount) {
                    node.mGarbage += (int) amount;
                    break;
                }
                node = popNode(node);
                if (node == null) {
                    throw new CorruptNodeException("Remainder of undo log is missing");
                }
                amount -= size;
            }
        }
        mLength = savepoint;
        mActiveIndexId = 0;
    }

    /**
     * Rollback all log entries to the given savepoint. Pass zero to rollback
     * everything. Caller must hold commit lock.
     */
    final void rollback(long savepoint) throws IOException {
        // Implementation could be optimized somewhat, resulting in less
        // temporary arrays and copies. Rollback optimization is generally not
        // necessary, since most transactions are expected to commit.

        if (savepoint < mLength) {
            byte[] opRef = new byte[1];
            Index activeIndex = null;
            do {
                byte[] entry = pop(opRef);

                switch (opRef[0]) {
                default:
                    throw new DatabaseException("Unknown undo log entry type: " + opRef[0]);

                case OP_TRANSACTION:
                    // Ignore.
                    break;

                case OP_INDEX:
                    mActiveIndexId = DataIO.readLong(entry, 1);
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
        }
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
     * @return null if none left
     */
    private Node popNode(Node parent) throws IOException {
        Node lowerNode = latchLowerNode(parent);
        mDatabase.deleteNode(parent);
        parent.releaseExclusive();
        return mNode = lowerNode;
    }

    /**
     * @return null if none
     */
    private Node latchLowerNode(Node parent) throws IOException {
        long lowerNodeId = DataIO.readLong(parent.mPage, 4);
        Node lowerNode;
        if (lowerNodeId == 0) {
            return null;
        }

        Node[] childNodes = parent.mChildNodes;
        if (childNodes != null) {
            lowerNode = childNodes[0];
            lowerNode.acquireExclusiveUnfair();
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
    private Node newDirtyNode(long lowerNodeId) throws IOException {
        Node node = mDatabase.newDirtyNode();
        node.mType = Node.TYPE_UNDO_STACK;
        DataIO.writeLong(node.mPage, 4, lowerNodeId);
        return node;
    }
}
