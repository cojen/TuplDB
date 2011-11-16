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
final class UndoStack {
    /*
      FIXME: notes

      - Link UndoStack instances together for registration
      - Consider using registration lock also for assigning txnId (no atomic long)
      - Mark node as dirty whenever making changes to it
      - Checkpoint first iterates all UndoStacks and sets commit state (mark phase)
      - All marked UndoStacks are converted to Node form and flushed
      - Consider using Node latches for concurrency control during flush
      - Node supplies payload, temporarily modifying page such that txn and
        index id are encoded immediately before key/value entry.
    */

    /*
      UndoStack is persisted in Nodes. All multibyte types are big endian encoded.

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

    private static final byte INVALID = (byte) 0;

    private final Database mDatabase;

    private byte[] mBuffer;
    private int mBufferPos;

    // Top node, always latched. This prevents it from being evicted. Nodes are
    // not used for stacks which fit into local buffer.
    private Node mNode;

    UndoStack(Database db) {
        mDatabase = db;
    }

    /**
     * @param op non-zero opcode
     */
    void push(byte op, byte[] payload) throws IOException {
        push(op, payload, 0, payload.length);
    }

    /**
     * @param op non-zero opcode
     */
    void push(final byte op, final byte[] payload, final int off, final int len)
        throws IOException
    {
        if (op == INVALID) {
            throw new IllegalArgumentException();
        }

        Lock sharedCommitLock = mDatabase.sharedCommitLock();
        sharedCommitLock.lock();
        try {
            int varIntLen =  DataIO.calcUnsignedVarIntLength(len);
            int encodedLen = 1 + varIntLen + len;

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
                try {
                    Node[] childNodes = new Node[] {node};
                    newNode = newDirtyNode(node.mId);
                    newNode.mChildNodes = childNodes;
                    newNode.mGarbage = pos = page.length;
                    available = pos - HEADER_SIZE;
                } catch (Throwable e) {
                    // Invalidate partial entry, allowing existing entries to be popped.
                    // Maximum size of (opcode + varint) is 6, so ensure room for it.
                    pos += 6;
                    int completed = len - remaining - 6;
                    varIntLen = DataIO.calcUnsignedVarIntLength(completed);
                    DataIO.writeUnsignedVarInt(page, pos -= varIntLen, completed);
                    page[--pos] = INVALID;
                    node.mGarbage = pos;
                    throw Utils.rethrow(e);
                }

                node.releaseExclusive();
                mNode = node = newNode;
            }
        } finally {
            sharedCommitLock.unlock();
        }
    }

    /**
     * @return null if nothing left; first byte is the opcode
     */
    byte[] pop() throws IOException {
        while (true) {
            byte[] b = popAny();
            if (b == null || b[0] != INVALID) {
                return b;
            }
            // Skip invalid entries. No need to optimize this case, since it is
            // expected to be rare.
        }
    }

    private byte[] popAny() throws IOException {
        Lock sharedCommitLock = mDatabase.sharedCommitLock();
        sharedCommitLock.lock();
        try {
            Node node = mNode;
            if (node == null) {
                byte[] buffer = mBuffer;
                if (buffer == null) {
                    return null;
                }
                int pos = mBufferPos;
                if (pos >= buffer.length) {
                    return null;
                }
                byte op = buffer[pos++];
                int len = DataIO.readUnsignedVarInt(buffer, pos);
                pos += DataIO.calcUnsignedVarIntLength(len);
                byte[] entry = new byte[1 + len];
                entry[0] = op;
                System.arraycopy(buffer, pos, entry, 1, len);
                mBufferPos = pos + len;
                return entry;
            }

            byte[] entry = null;
            int entryPos = 0;
            int payloadLen = 0;

            while (true) {
                byte[] page = node.mPage;
                int pos = node.mGarbage;

                if (entry == null) {
                    byte op = page[pos++];
                    payloadLen = DataIO.readUnsignedVarInt(page, pos);
                    pos += DataIO.calcUnsignedVarIntLength(payloadLen);
                    entry = new byte[1 + payloadLen];
                    entry[0] = op;
                    entryPos = 1;
                }

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
                    throw new CorruptNodeException("Remainder of undo entry is missing");
                }

                entryPos += avail;
            }
        } finally {
            sharedCommitLock.unlock();
        }
    }

    /**
     * Delete the entire stack instance.
     */
    void delete() throws IOException {
        Lock sharedCommitLock = mDatabase.sharedCommitLock();
        sharedCommitLock.lock();
        try {
            Node node = mNode;
            if (node == null) {
                mBuffer = null;
                mBufferPos = 0;
            } else {
                while ((node = popNode(node)) != null);
            }
        } finally {
            sharedCommitLock.unlock();
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
