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

import static java.lang.System.arraycopy;

import static org.cojen.tupl.Utils.*;

/**
 * Node within a B-tree, undo log, or a large value fragment.
 *
 * @author Brian S O'Neill
 */
final class Node extends Latch {
    // Note: Changing these values affects how the Database class handles the
    // commit flag. It only needs to flip bit 0 to switch dirty states.
    static final byte
        CACHED_CLEAN     = 0, // 0b0000
        CACHED_DIRTY_0   = 2, // 0b0010
        CACHED_DIRTY_1   = 3; // 0b0011

    /*
      Node type encoding strategy:

      bits 7..4: major type   0010 (fragment), 0100 (undo log),
                              0110 (internal), 0111 (bottom internal), 1000 (leaf)
      bits 3..1: sub type     for leaf: 000 (normal)
                              for internal: 001 (6 byte child pointers), 010 (8 byte pointers)
      bit  0:    endianness   0 (little), 1 (big)

      TN == Tree Node

      Note that leaf type is always negative. If type encoding changes, the
      isLeaf method might need to be updated.

     */

    static final byte
        TYPE_NONE     = 0,
        TYPE_FRAGMENT = (byte) 0x20, // 0b0010_000_0
        TYPE_UNDO_LOG = (byte) 0x40, // 0b0100_000_0
        TYPE_TN_IN    = (byte) 0x64, // 0b0110_010_0
        TYPE_TN_BIN   = (byte) 0x74, // 0b0111_010_0
        TYPE_TN_LEAF  = (byte) 0x80; // 0b1000_000_0

    // Tree node header size.
    static final int TN_HEADER_SIZE = 12;

    static final int STUB_ID = 1;

    static final int VALUE_FRAGMENTED = 0x40;

    // Links within usage list, guarded by Database.mUsageLatch.
    Node mMoreUsed; // points to more recently used node
    Node mLessUsed; // points to less recently used node

    // Links within dirty list, guarded by OrderedPageAllocator.
    Node mNextDirty;
    Node mPrevDirty;

    /*
      Nodes define the contents of Trees and UndoLogs. All node types start
      with a two byte header.

      +----------------------------------------+
      | byte:   node type                      |  header
      | byte:   reserved (must be 0)           |
      -                                        -

      There are two types of tree nodes, having a similar structure and
      supporting a maximum page size of 65536 bytes. The ushort type is an
      unsigned byte pair, and the ulong type is eight bytes. All multibyte
      types are little endian encoded.

      +----------------------------------------+
      | byte:   node type                      |  header
      | byte:   reserved (must be 0)           |
      | ushort: garbage in segments            |
      | ushort: pointer to left segment tail   |
      | ushort: pointer to right segment tail  |
      | ushort: pointer to search vector start |
      | ushort: pointer to search vector end   |
      +----------------------------------------+
      | left segment                           |
      -                                        -
      |                                        |
      +----------------------------------------+
      | free space                             | <-- left segment tail (exclusive)
      -                                        -
      |                                        |
      +----------------------------------------+
      | search vector                          | <-- search vector start (inclusive)
      -                                        -
      |                                        | <-- search vector end (inclusive)
      +----------------------------------------+
      | free space                             |
      -                                        -
      |                                        | <-- right segment tail (exclusive)
      +----------------------------------------+
      | right segment                          |
      -                                        -
      |                                        |
      +----------------------------------------+

      The left and right segments are used for allocating variable sized entries, and the
      tail points to the next allocation. Allocations are made toward the search vector
      such that the free space before and after the search vector remain the roughly the
      same. The search vector may move if a contiguous allocation is not possible on
      either side.

      The search vector is used for performing a binary search against keys. The keys are
      variable length and are stored anywhere in the left and right segments. The search
      vector itself must point to keys in the correct order, supporting binary search. The
      search vector is also required to be aligned to an even address, contain fixed size
      entries, and it never has holes. Adding or removing entries from the search vector
      requires entries to be shifted. The shift operation can be performed from either
      side, but the smaller shift is always chosen as a performance optimization.
      
      Garbage refers to the amount of unused bytes within the left and right allocation
      segments. Garbage accumulates when entries are deleted and updated from the
      segments. Segments are not immediately shifted because the search vector would also
      need to be repaired. A compaction operation reclaims garbage by rebuilding the
      segments and search vector. A copying garbage collection algorithm is used for this.

      The compaction implementation allocates all surviving entries in the left segment,
      leaving an empty right segment. There is no requirement that the segments be
      balanced -- this only applies to the free space surrounding the search vector.

      Leaf nodes support variable length keys and values, encoded as a pair, within the
      segments. Entries in the search vector are ushort pointers into the segments. No
      distinction is made between the segments because the pointers are absolute.

      Entries start with a one byte key header:

      0b0pxx_xxxx: key is 1..64 bytes
      0b1pxx_xxxx: key is 0..16383 bytes

      When the 'p' bit is zero, the entry is a normal key. Otherwise, it
      indicates that the key starts with the node key prefix.

      For keys 1..64 bytes in length, the length is defined as ((header & 0x3f) + 1). For
      keys 0..16383 bytes in length, a second header byte is used. The second byte is
      unsigned, and the length is defined as (((header & 0x3f) << 8) | header2). The key
      contents immediately follow the header byte(s).

      The value follows the key, and its header encodes the entry length:

      0b0xxx_xxxx: value is 0..127 bytes
      0b1f0x_xxxx: value/entry is 1..8192 bytes
      0b1f10_xxxx: value/entry is 1..1048576 bytes
      0b1111_1111: ghost value (null)

      When the 'f' bit is zero, the entry is a normal value. Otherwise, it is a
      fragmented value, defined by Database.fragment.

      For entries 1..8192 bytes in length, a second header byte is used. The
      length is then defined as ((((h0 & 0x1f) << 8) | h1) + 1). For larger
      entries, the length is ((((h0 & 0x0f) << 16) | (h1 << 8) | h2) + 1).
      Node limit is currently 65536 bytes, which limits maximum entry length.

      The "values" for internal nodes are actually identifiers for child nodes. The number
      of child nodes is always one more than the number of keys. For this reason, the
      key-value format used by leaf nodes cannot be applied to internal nodes. Also, the
      identifiers are always a fixed length, ulong type.

      Child node identifiers are encoded immediately following the search vector. Free space
      management must account for this, treating it as an extension to the search vector.

     */

    // Raw contents of node.
    byte[] mPage;

    // Id is often read without acquiring latch, although in most cases, it
    // doesn't need to be volatile. This is because a double check with the
    // latch held is always performed. So-called double-checked locking doesn't
    // work with object initialization, but it's fine with primitive types.
    // When nodes are evicted, the write operation must complete before the id
    // is re-assigned. For this reason, the id is volatile. A memory barrier
    // between the write and re-assignment should work too.
    volatile long mId;

    byte mCachedState;

    // Entries from header, available as fields for quick access.
    byte mType;
    int mGarbage;
    int mLeftSegTail;
    int mRightSegTail;
    int mSearchVecStart;
    int mSearchVecEnd;

    // References to child nodes currently available. Is null for leaf nodes.
    Node[] mChildNodes;

    // Linked stack of TreeCursorFrames bound to this Node.
    TreeCursorFrame mLastCursorFrame;

    // Set by a partially completed split.
    Split mSplit;

    Node(int pageSize) {
        mPage = new byte[pageSize];
    }

    private Node(byte[] page) {
        mPage = page;
    }

    void asEmptyRoot() {
        mId = 0;
        mCachedState = CACHED_CLEAN;
        mType = TYPE_TN_LEAF;
        clearEntries();
    }

    /**
     * Close the root node when closing a tree. Returns a new node which allows
     * the page to be recycled.
     *
     * @param clone true to clone state into new node
     */
    Node closeRoot(boolean clone) {
        // Allocate node first, in case an OutOfMemoryError is thrown.
        Node newNode = new Node(mPage);

        if (clone) {
            newNode.mId = mId;
            newNode.mCachedState = mCachedState;
            newNode.mType = mType;
            newNode.mGarbage = mGarbage;
            newNode.mLeftSegTail = mLeftSegTail;
            newNode.mRightSegTail = mRightSegTail;
            newNode.mSearchVecStart = mSearchVecStart;
            newNode.mSearchVecEnd = mSearchVecEnd;
            newNode.mChildNodes = mChildNodes;
        }

        // Prevent node from being marked dirty.
        mId = STUB_ID;
        mCachedState = CACHED_CLEAN;
        mType = TYPE_TN_LEAF;
        mPage = EMPTY_BYTES;
        mGarbage = 0;

        // Clear entries with the lowest positive values for an empty node.
        // Binary search must return ~0 and availableBytes must return 0.
        mLeftSegTail = 2;
        mRightSegTail = 1;
        mSearchVecStart = 2;
        mSearchVecEnd = 0;

        // TODO: child node array should be recycled
        mChildNodes = null;

        return newNode;
    }

    private void clearEntries() {
        mGarbage = 0;
        mLeftSegTail = TN_HEADER_SIZE;
        int pageSize = mPage.length;
        mRightSegTail = pageSize - 1;
        // Search vector location must be even.
        mSearchVecStart = (TN_HEADER_SIZE + ((pageSize - TN_HEADER_SIZE) >> 1)) & ~1;
        mSearchVecEnd = mSearchVecStart - 2; // inclusive
    }

    /**
     * Root search.
     *
     * @param key search key
     * @return copy of value or null if not found
     */
    byte[] search(Tree tree, byte[] key) throws IOException {
        acquireShared();
        // Note: No need to check if root has split, since root splits are always
        // completed before releasing the root latch.
        return isLeaf() ? subSearchLeaf(tree, key) : subSearch(tree, this, null, key, false);
    }

    /**
     * Sub search into internal node with shared or exclusive latch held. Latch is
     * released by the time this method returns.
     *
     * @param parentLatch shared latch held on parent; is null for root or if
     * exclusive latch is held on this node
     * @param key search key
     * @param exclusiveHeld is true if exclusive latch is held on this node
     * @return copy of value or null if not found
     */
    private static byte[] subSearch(final Tree tree, Node node, Latch parentLatch,
                                    final byte[] key, boolean exclusiveHeld)
        throws IOException
    {
        // Caller invokes Database.used for this Node. Root node is not
        // managed in usage list, because it cannot be evicted.

        int childPos;
        long childId;

        loop: while (true) {
            childPos = internalPos(node.binarySearch(key));

            Node childNode = node.mChildNodes[childPos >> 1];
            childId = node.retrieveChildRefId(childPos);

            childCheck: if (childNode != null && childId == childNode.mId) {
                childNode.acquireShared();

                // Need to check again in case evict snuck in.
                if (childId != childNode.mId) {
                    childNode.releaseShared();
                    break childCheck;
                }

                if (!exclusiveHeld && parentLatch != null) {
                    parentLatch.releaseShared();
                }

                if (childNode.mSplit != null) {
                    childNode = childNode.mSplit.selectNodeShared(childNode, key);
                }

                if (childNode.isLeaf()) {
                    node.release(exclusiveHeld);
                    tree.mDatabase.used(childNode);
                    return childNode.subSearchLeaf(tree, key);
                } else {
                    // Keep shared latch on this parent node, in case sub search
                    // needs to upgrade its shared latch.
                    if (exclusiveHeld) {
                        node.downgrade();
                        exclusiveHeld = false;
                    }
                    tree.mDatabase.used(childNode);
                    // Tail call: return subSearch(tree, childNode, node, key, false);
                    parentLatch = node;
                    node = childNode;
                    continue loop;
                }
            } // end childCheck

            // Child needs to be loaded.

            if (/*exclusiveHeld =*/ node.tryUpgrade(parentLatch, exclusiveHeld)) {
                // Succeeded in upgrading latch, so break out to load child.
                parentLatch = null;
                break loop;
            }

            // Release shared latch, re-acquire exclusive latch, and start over.

            long id = node.mId;
            node.releaseShared();
            node.acquireExclusive();

            if (node.mId != id && node != tree.mRoot) {
                // Node got evicted or dirtied when latch was released. To be
                // safe, the search must be retried from the root.
                node.releaseExclusive();
                if (parentLatch != null) {
                    parentLatch.releaseShared();
                }
                // Retry with a cursor, which is reliable, but slower.
                TreeCursor cursor = new TreeCursor(tree, Transaction.BOGUS);
                try {
                    cursor.find(key);
                    byte[] value = cursor.value();
                    cursor.reset();
                    return value;
                } catch (Throwable e) {
                    throw closeOnFailure(cursor, e);
                }
            }

            exclusiveHeld = true;

            if (parentLatch != null) {
                parentLatch.releaseShared();
                parentLatch = null;
            }

            if (node.mSplit != null) {
                // Node might have split while shared latch was not held.
                node = node.mSplit.selectNodeExclusive(node, key);
            }

            if (node == tree.mRoot) {
                // This is the root node, and so no parent latch exists. It's
                // possible that a delete slipped in when the latch was
                // released, and that the root is now a leaf.
                if (node.isLeaf()) {
                    node.downgrade();
                    return node.subSearchLeaf(tree, key);
                }
            }
        } // end loop

        // If this point is reached, exclusive latch for this node is held and
        // child needs to be loaded. Parent latch has been released.

        Node childNode = node.loadChild(tree.mDatabase, childPos, childId, true);

        if (childNode.isLeaf()) {
            childNode.downgrade();
            return childNode.subSearchLeaf(tree, key);
        } else {
            // Keep exclusive latch on internal child, because it will most
            // likely need to load its own child nodes to continue the
            // search. This eliminates the latch upgrade step.
            return subSearch(tree, childNode, null, key, true);
        }
    }

    /**
     * Sub search into leaf with shared latch held. Latch is released by the time
     * this method returns.
     *
     * @param key search key
     * @return copy of value or null if not found
     */
    private byte[] subSearchLeaf(final Tree tree, final byte[] key) throws IOException {
        // Same code as binarySearch, but instead of returning the position, it
        // directly copies the value if found. This avoids having to decode the
        // found value location twice.

        final byte[] page = mPage;
        final int keyLen = key.length;
        int lowPos = mSearchVecStart;
        int highPos = mSearchVecEnd;

        int lowMatch = 0;
        int highMatch = 0;

        outer: while (lowPos <= highPos) {
            int midPos = ((lowPos + highPos) >> 1) & ~1;

            int compareLoc = decodeUnsignedShortLE(page, midPos);
            int compareLen = page[compareLoc++];
            compareLen = compareLen >= 0 ? ((compareLen & 0x3f) + 1)
                : (((compareLen & 0x3f) << 8) | ((page[compareLoc++]) & 0xff));

            int minLen = Math.min(compareLen, keyLen);
            int i = Math.min(lowMatch, highMatch);
            for (; i<minLen; i++) {
                byte cb = page[compareLoc + i];
                byte kb = key[i];
                if (cb != kb) {
                    if ((cb & 0xff) < (kb & 0xff)) {
                        lowPos = midPos + 2;
                        lowMatch = i;
                    } else {
                        highPos = midPos - 2;
                        highMatch = i;
                    }
                    continue outer;
                }
            }

            if (compareLen < keyLen) {
                lowPos = midPos + 2;
                lowMatch = i;
            } else if (compareLen > keyLen) {
                highPos = midPos - 2;
                highMatch = i;
            } else {
                try {
                    return retrieveLeafValueAtLoc(this, tree, page, compareLoc + compareLen);
                } finally {
                    releaseShared();
                }
            }
        }

        releaseShared();
        return null;
    }

    /**
     * With this parent node held exclusively, loads child with exclusive latch
     * held. Caller must ensure that child is not already loaded. If an
     * exception is thrown, parent and child latches are always released.
     *
     * @param releaseParent when true, release this node latch always; when false, release only
     * if an exception is thrown
     */
    Node loadChild(Database db, int childPos, long childId, boolean releaseParent)
        throws IOException
    {
        Node childNode;
        try {
            childNode = db.allocLatchedNode();
            childNode.mId = childId;
            mChildNodes[childPos >> 1] = childNode;
        } catch (Throwable e) {
            releaseExclusive();
            throw rethrow(e);
        }

        // Release parent latch before child has been loaded. Any threads
        // which wish to access the same child will block until this thread
        // has finished loading the child and released its exclusive latch.
        if (releaseParent) {
            releaseExclusive();
        }

        // FIXME: Don't hold latch during load. Instead, use an object for
        // holding state, and include a "loading" state. As other threads see
        // this state, they replace the state object with a linked stack of
        // parked threads. When the load is finished, all waiting threads are
        // unparked. Without this change, latch blockage can reach the root.

        try {
            childNode.read(db, childId);
        } catch (Throwable e) {
            // Another thread might access child and see that it is invalid because
            // id is zero. It will assume it got evicted and will load child again.
            childNode.mId = 0;
            childNode.mType = TYPE_NONE;
            childNode.releaseExclusive();

            if (!releaseParent) {
                // Obey the method contract and release latch due to exception.
                releaseExclusive();
            }

            throw rethrow(e);
        }

        return childNode;
    }

    /**
     * Caller must hold exclusive root latch and it must verify that root has split.
     *
     * @param stub Old root node stub, latched exclusively, whose cursors must
     * transfer into the new root. Stub latch is released by this method.
     */
    void finishSplitRoot(Tree tree, Node stub) throws IOException {
        // Create a child node and copy this root node state into it. Then update this
        // root node to point to new and split child nodes. New root is always an internal node.

        Database db = tree.mDatabase;
        Node child = db.allocDirtyNode();

        byte[] newPage = child.mPage;
        child.mPage = mPage;
        child.mType = mType;
        child.mGarbage = mGarbage;
        child.mLeftSegTail = mLeftSegTail;
        child.mRightSegTail = mRightSegTail;
        child.mSearchVecStart = mSearchVecStart;
        child.mSearchVecEnd = mSearchVecEnd;
        child.mChildNodes = mChildNodes;
        child.mLastCursorFrame = mLastCursorFrame;

        // Fix child node cursor frame bindings.
        for (TreeCursorFrame frame = mLastCursorFrame; frame != null; ) {
            frame.mNode = child;
            frame = frame.mPrevCousin;
        }

        final Split split = mSplit;
        final Node sibling = rebindSplitFrames(split);
        mSplit = null;

        Node left, right;
        if (split.mSplitRight) {
            left = child;
            right = sibling;
        } else {
            left = sibling;
            right = child;
        }

        int keyLen = split.copySplitKeyToParent(newPage, TN_HEADER_SIZE);

        // Create new single-element search vector.
        final int searchVecStart =
            ((newPage.length - TN_HEADER_SIZE - keyLen - (2 + 8 + 8)) >> 1) & ~1;
        encodeShortLE(newPage, searchVecStart, TN_HEADER_SIZE);
        encodeLongLE(newPage, searchVecStart + 2, left.mId);
        encodeLongLE(newPage, searchVecStart + 2 + 8, right.mId);

        // TODO: recycle these arrays
        mChildNodes = new Node[] {left, right};

        mPage = newPage;
        mType = mType == TYPE_TN_LEAF ? TYPE_TN_BIN : TYPE_TN_IN;
        mGarbage = 0;
        mLeftSegTail = TN_HEADER_SIZE + keyLen;
        mRightSegTail = newPage.length - 1;
        mSearchVecStart = searchVecStart;
        mSearchVecEnd = searchVecStart;
        mLastCursorFrame = null;

        // Add a parent cursor frame for all left and right node cursors.
        addParentFrames(stub, left, 0);
        addParentFrames(stub, right, 2);

        child.releaseExclusive();
        sibling.releaseExclusive();

        // Split complete, so allow new node to be evictable.
        db.makeEvictable(sibling);

        if (stub != null) {
            stub.releaseExclusive();
        }
    }

    private void addParentFrames(Node stub, Node child, int pos) {
        for (TreeCursorFrame frame = child.mLastCursorFrame; frame != null; ) {
            TreeCursorFrame parentFrame = frame.mParentFrame;
            if (parentFrame == null) {
                parentFrame = new TreeCursorFrame();
            } else {
                if (parentFrame.mNode != stub) {
                    throw new AssertionError
                        ("Stub mismatch: " + parentFrame.mNode + " != " + stub);
                }
                parentFrame.unbind();
            }
            parentFrame.bind(this, pos);
            frame.mParentFrame = parentFrame;
            frame = frame.mPrevCousin;
        }
    }

    /**
     * Caller must hold exclusive latch. Latch is never released by this method, even if
     * an exception is thrown.
     */
    void read(Database db, long id) throws IOException {
        byte[] page = mPage;

        mCachedState = db.readNodePage(id, page);
        mId = id;

        byte type = page[0];
        mType = type;

        // For undo log node, this is top entry pointer.
        mGarbage = decodeUnsignedShortLE(page, 2);

        if (type != TYPE_UNDO_LOG) {
            mLeftSegTail = decodeUnsignedShortLE(page, 4);
            mRightSegTail = decodeUnsignedShortLE(page, 6);
            mSearchVecStart = decodeUnsignedShortLE(page, 8);
            mSearchVecEnd = decodeUnsignedShortLE(page, 10);
            if (type == TYPE_TN_IN || type == TYPE_TN_BIN) {
                // TODO: recycle child node arrays
                mChildNodes = new Node[numKeys() + 1];
            } else if (type != TYPE_TN_LEAF) {
                throw new CorruptDatabaseException("Unknown node type: " + type + ", id: " + id);
            }
        }

        if (page[1] != 0) {
            throw new CorruptDatabaseException("Illegal reserved byte in node: " + page[1]);
        }
    }

    /**
     * Caller must hold any latch, which is not released, even if an exception is thrown.
     */
    void write(PageDb db) throws IOException {
        if (mSplit != null) {
            throw new AssertionError("Cannot write partially split node");
        }

        byte[] page = mPage;

        if (mType != TYPE_FRAGMENT) {
            page[0] = mType;
            page[1] = 0; // reserved

            // For undo log node, this is top entry pointer.
            encodeShortLE(page, 2, mGarbage);

            if (mType != TYPE_UNDO_LOG) {
                encodeShortLE(page, 4, mLeftSegTail);
                encodeShortLE(page, 6, mRightSegTail);
                encodeShortLE(page, 8, mSearchVecStart);
                encodeShortLE(page, 10, mSearchVecEnd);
            }
        }

        db.writePage(mId, page);
    }

    /**
     * Caller must hold exclusive latch on node. Latch is released by this
     * method when null is returned or if an exception is thrown. If another
     * node is returned, it is latched exclusively and original is released.
     *
     * @return original or another node to be evicted; null if cannot evict
     */
    static Node evict(Node node, PageDb db) throws IOException {
        if (node.mType != TYPE_UNDO_LOG) {
            return node.evictTreeNode(db);
        }

        while (true) {
            Node[] childNodes = node.mChildNodes;
            if (childNodes != null && childNodes.length > 0) {
                Node child = childNodes[0];
                if (child != null) {
                    long childId = decodeLongLE(node.mPage, UndoLog.I_LOWER_NODE_ID);
                    // Check id match before lock attempt, as a quick short
                    // circuit if child has already been evicted.
                    if (childId == child.mId) {
                        if (child.tryAcquireExclusive()) {
                            // Check again in case another evict snuck in.
                            if (childId == child.mId && child.mCachedState != CACHED_CLEAN) {
                                // Try evicting the child instead.
                                node.releaseExclusive();
                                node = child;
                                continue;
                            }
                            child.releaseExclusive();
                        } else {
                            // If latch cannot be acquired, assume child is still
                            // in use, and so the parent node should be kept.
                            node.releaseExclusive();
                            return null;
                        }
                    }
                }
            }
            node.doEvict(db);
            return node;
        }
    }

    private Node evictTreeNode(PageDb db) throws IOException {
        if (mLastCursorFrame != null || mSplit != null) {
            // Cannot evict if in use by a cursor or if splitting. The split
            // check is redundant, since a node cannot be in a split state
            // without a cursor registered against it.
            releaseExclusive();
            return null;
        }

        if (mId == STUB_ID) {
            // Stub has one child which is the root or another stub. The child
            // should never be evicted, because this would cause the entire
            // tree to be erroneously evicted.
            mId = 0;
            // TODO: child node array should be recycled
            mChildNodes = null;
            return this;
        }

        Node[] childNodes = mChildNodes;
        if (childNodes != null) for (int i=0; i<childNodes.length; i++) {
            Node child = childNodes[i];
            if (child != null) {
                long childId = retrieveChildRefIdFromIndex(i);
                if (childId != child.mId) {
                    // Not our child -- it was evicted already.
                    childNodes[i] = null;
                } else if (child.tryAcquireExclusive()) {
                    if (childId == child.mId && child.evictTreeNode(db) == null) {
                        // Cannot evict child, and so cannot evict parent.
                        releaseExclusive();
                        return null;
                    }
                    childNodes[i] = null;
                    child.releaseExclusive();
                } else {
                    // If latch cannot be acquired, assume child is still in
                    // use, and so the parent node should be kept.
                    releaseExclusive();
                    return null;
                }
            }
        }

        doEvict(db);
        return this;
    }

    /**
     * Evict all tree nodes, starting from the root. Intended to be used only
     * when tree is no longer referenced, ensuring all dirty nodes are written.
     * Caller must hold exclusive latch on node. Latch is released by this
     * method when an exception is thrown.
     */
    void forceEvictTree(PageDb db) throws IOException {
        // Cursor frames might still exist, if cursors are not being reset properly. Since tree
        // is not referenced, the original cursors are gone. The frames are just garbage.
        mLastCursorFrame = null;

        Node[] childNodes = mChildNodes;
        if (childNodes != null) for (int i=0; i<childNodes.length; i++) {
            Node child = childNodes[i];
            if (child != null) {
                long childId = retrieveChildRefIdFromIndex(i);
                if (childId == child.mId) {
                    child.acquireExclusive();
                    if (childId == child.mId) {
                        child.forceEvictTree(db);
                    }
                    child.releaseExclusive();
                }
                childNodes[i] = null;
            }
        }

        doEvict(db);
    }

    /**
     * Caller must hold exclusive latch on node. Latch is released by this
     * method when an exception is thrown.
     */
    void doEvict(PageDb db) throws IOException {
        if (mCachedState != CACHED_CLEAN) {
            try {
                write(db);
                mCachedState = CACHED_CLEAN;
            } catch (Throwable e) {
                releaseExclusive();
                throw rethrow(e);
            }
        }

        mId = 0;
        mType = TYPE_NONE;
        // TODO: child node array should be recycled
        mChildNodes = null;
    }

    /**
     * Caller must hold any latch.
     */
    boolean isLeaf() {
        return mType < 0;
    }

    /**
     * Caller must hold any latch.
     */
    int numKeys() {
        return (mSearchVecEnd - mSearchVecStart + 2) >> 1;
    }

    /**
     * Caller must hold any latch.
     */
    boolean hasKeys() {
        return mSearchVecEnd >= mSearchVecStart;
    }

    /**
     * Returns the highest possible key position, which is an even number. If
     * node has no keys, return value is negative. Caller must hold any latch.
     */
    int highestKeyPos() {
        return mSearchVecEnd - mSearchVecStart;
    }

    /**
     * Returns highest leaf or internal position. Caller must hold any latch.
     */
    int highestPos() {
        int pos = mSearchVecEnd - mSearchVecStart;
        if (!isLeaf()) {
            pos += 2;
        }
        return pos;
    }

    /**
     * Returns the highest possible leaf key position, which is an even
     * number. If leaf node is empty, return value is negative. Caller must
     * hold any latch.
     */
    int highestLeafPos() {
        return mSearchVecEnd - mSearchVecStart;
    }

    /**
     * Returns the highest possible internal node position, which is an even
     * number. Highest position doesn't correspond to a valid key, but instead
     * a child node position. If internal node has no keys, node has one child
     * at position zero. Caller must hold any latch.
     */
    int highestInternalPos() {
        return mSearchVecEnd - mSearchVecStart + 2;
    }

    /**
     * Caller must hold any latch.
     */
    int availableBytes() {
        return isLeaf() ? availableLeafBytes() : availableInternalBytes();
    }

    /**
     * Caller must hold any latch.
     */
    int availableLeafBytes() {
        return mGarbage + mSearchVecStart - mSearchVecEnd
            - mLeftSegTail + mRightSegTail + (1 - 2);
    }

    /**
     * Caller must hold any latch.
     */
    int availableInternalBytes() {
        return mGarbage + 5 * (mSearchVecStart - mSearchVecEnd)
            - mLeftSegTail + mRightSegTail + (1 - (5 * 2 + 8));
    }

    /**
     * Returns true if leaf is not split and underutilized. If so, it should be
     * merged with its neighbors, and possibly deleted. Caller must hold any latch.
     */
    boolean shouldLeafMerge() {
        return shouldMerge(availableLeafBytes());
    }

    /**
     * Returns true if non-leaf is not split and underutilized. If so, it should be
     * merged with its neighbors, and possibly deleted. Caller must hold any latch.
     */
    boolean shouldInternalMerge() {
        return shouldMerge(availableInternalBytes());
    }

    boolean shouldMerge(int availBytes) {
        return mSplit == null && availBytes >= ((mPage.length - TN_HEADER_SIZE) >> 1);
    }

    /**
     * Returns true if exclusive latch is held and parent latch is released. When
     * false is returned, no state of any latches has changed.
     *
     * @param parentLatch optional shared latch
     */
    private boolean tryUpgrade(Latch parentLatch, boolean exclusiveHeld) {
        if (exclusiveHeld) {
            return true;
        }
        if (tryUpgrade()) {
            if (parentLatch != null) {
                parentLatch.releaseShared();
            }
            return true;
        }
        return false;
    }

    /**
     * @return 2-based insertion pos, which is negative if key not found
     */
    int binarySearch(byte[] key) {
        final byte[] page = mPage;
        final int keyLen = key.length;
        int lowPos = mSearchVecStart;
        int highPos = mSearchVecEnd;

        int lowMatch = 0;
        int highMatch = 0;

        outer: while (lowPos <= highPos) {
            int midPos = ((lowPos + highPos) >> 1) & ~1;

            int compareLoc = decodeUnsignedShortLE(page, midPos);
            int compareLen = page[compareLoc++];
            compareLen = compareLen >= 0 ? ((compareLen & 0x3f) + 1)
                : (((compareLen & 0x3f) << 8) | ((page[compareLoc++]) & 0xff));

            int minLen = Math.min(compareLen, keyLen);
            int i = Math.min(lowMatch, highMatch);
            for (; i<minLen; i++) {
                byte cb = page[compareLoc + i];
                byte kb = key[i];
                if (cb != kb) {
                    if ((cb & 0xff) < (kb & 0xff)) {
                        lowPos = midPos + 2;
                        lowMatch = i;
                    } else {
                        highPos = midPos - 2;
                        highMatch = i;
                    }
                    continue outer;
                }
            }

            if (compareLen < keyLen) {
                lowPos = midPos + 2;
                lowMatch = i;
            } else if (compareLen > keyLen) {
                highPos = midPos - 2;
                highMatch = i;
            } else {
                return midPos - mSearchVecStart;
            }
        }

        return ~(lowPos - mSearchVecStart);
    }

    /**
     * @param midPos 2-based starting position
     * @return 2-based insertion pos, which is negative if key not found
     */
    int binarySearch(byte[] key, int midPos) {
        int lowPos = mSearchVecStart;
        int highPos = mSearchVecEnd;
        if (lowPos > highPos) {
            return -1;
        }
        midPos += lowPos;
        if (midPos > highPos) {
            return ~2 - highPos + lowPos;
        }

        final byte[] page = mPage;
        final int keyLen = key.length;

        int lowMatch = 0;
        int highMatch = 0;

        while (true) {
            compare: {
                int compareLoc = decodeUnsignedShortLE(page, midPos);
                int compareLen = page[compareLoc++];
                compareLen = compareLen >= 0 ? ((compareLen & 0x3f) + 1)
                    : (((compareLen & 0x3f) << 8) | ((page[compareLoc++]) & 0xff));

                int minLen = Math.min(compareLen, keyLen);
                int i = Math.min(lowMatch, highMatch);
                for (; i<minLen; i++) {
                    byte cb = page[compareLoc + i];
                    byte kb = key[i];
                    if (cb != kb) {
                        if ((cb & 0xff) < (kb & 0xff)) {
                            lowPos = midPos + 2;
                            lowMatch = i;
                        } else {
                            highPos = midPos - 2;
                            highMatch = i;
                        }
                        break compare;
                    }
                }

                if (compareLen < keyLen) {
                    lowPos = midPos + 2;
                    lowMatch = i;
                } else if (compareLen > keyLen) {
                    highPos = midPos - 2;
                    highMatch = i;
                } else {
                    return midPos - mSearchVecStart;
                }
            }

            if (lowPos > highPos) {
                break;
            }

            midPos = ((lowPos + highPos) >> 1) & ~1;
        }

        return ~(lowPos - mSearchVecStart);
    }

    /**
     * Ensure binary search position is positive, for internal node.
     */
    static int internalPos(int pos) {
        return pos < 0 ? ~pos : (pos + 2);
    }

    /**
     * @param pos position as provided by binarySearch; must be positive
     */
    byte[] retrieveKey(int pos) {
        final byte[] page = mPage;
        return retrieveKeyAtLoc(page, decodeUnsignedShortLE(page, mSearchVecStart + pos));
    }

    /**
     * @param loc absolute location of entry
     */
    static byte[] retrieveKeyAtLoc(final byte[] page, int loc) {
        int keyLen = page[loc++];
        keyLen = keyLen >= 0 ? ((keyLen & 0x3f) + 1)
            : (((keyLen & 0x3f) << 8) | ((page[loc++]) & 0xff));
        byte[] key = new byte[keyLen];
        arraycopy(page, loc, key, 0, keyLen);
        return key;
    }

    /**
     * Copies the key at the given position based on a limit. If equal, the
     * limitKey instance is returned. If beyond the limit, null is returned.
     *
     * @param pos position as provided by binarySearch; must be positive
     * @param limitKey comparison key
     * @param limitMode positive for LE behavior, negative for GE behavior
     */
    byte[] retrieveKeyCmp(int pos, byte[] limitKey, int limitMode) {
        final byte[] page = mPage;
        int loc = decodeUnsignedShortLE(page, mSearchVecStart + pos);
        int keyLen = page[loc++];
        keyLen = keyLen >= 0 ? ((keyLen & 0x3f) + 1)
            : (((keyLen & 0x3f) << 8) | ((page[loc++]) & 0xff));
        int cmp = compareKeys(page, loc, keyLen, limitKey, 0, limitKey.length);
        if (cmp == 0) {
            return limitKey;
        } else if ((cmp ^ limitMode) < 0) {
            byte[] key = new byte[keyLen];
            arraycopy(page, loc, key, 0, keyLen);
            return key;
        } else {
            return null;
        }
    }

    /**
     * Returns the last key of this node, appended with 0.
     */
    private byte[] higherKey() {
        final byte[] page = mPage;
        int loc = decodeUnsignedShortLE(page, mSearchVecEnd);
        int keyLen = page[loc++];
        keyLen = keyLen >= 0 ? ((keyLen & 0x3f) + 1)
            : (((keyLen & 0x3f) << 8) | ((page[loc++]) & 0xff));
        byte[] key = new byte[keyLen + 1];
        arraycopy(page, loc, key, 0, keyLen);
        return key;
    }

    /**
     * Used by UndoLog for decoding entries. Only works for non-fragmented values.
     *
     * @param loc absolute location of entry
     */
    static byte[][] retrieveKeyValueAtLoc(final byte[] page, int loc) throws IOException {
        int header = page[loc++];
        int keyLen = header >= 0 ? ((header & 0x3f) + 1)
            : (((header & 0x3f) << 8) | ((page[loc++]) & 0xff));
        byte[] key = new byte[keyLen];
        arraycopy(page, loc, key, 0, keyLen);
        return new byte[][] {key, retrieveLeafValueAtLoc(null, null, page, loc + keyLen)};
    }

    /**
     * @param pos position as provided by binarySearch; must be positive
     * @return Cursor.NOT_LOADED if value exists, null if ghost
     */
    byte[] hasLeafValue(int pos) {
        final byte[] page = mPage;
        int loc = decodeUnsignedShortLE(page, mSearchVecStart + pos);
        int header = page[loc++];
        loc += (header >= 0 ? header : (((header & 0x3f) << 8) | (page[loc] & 0xff))) + 1;
        return page[loc] == -1 ? null : Cursor.NOT_LOADED;
    }

    /**
     * @param pos position as provided by binarySearch; must be positive
     * @return null if ghost
     */
    byte[] retrieveLeafValue(Tree tree, int pos) throws IOException {
        final byte[] page = mPage;
        int loc = decodeUnsignedShortLE(page, mSearchVecStart + pos);
        int header = page[loc++];
        loc += (header >= 0 ? header : (((header & 0x3f) << 8) | (page[loc] & 0xff))) + 1;
        return retrieveLeafValueAtLoc(this, tree, page, loc);
    }

    private static byte[] retrieveLeafValueAtLoc(Node caller, Tree tree, byte[] page, int loc)
        throws IOException
    {
        final int header = page[loc++];
        if (header == 0) {
            return EMPTY_BYTES;
        }

        int len;
        if (header >= 0) {
            len = header;
        } else {
            if ((header & 0x20) == 0) {
                len = 1 + (((header & 0x1f) << 8) | (page[loc++] & 0xff));
            } else if (header != -1) {
                len = 1 + (((header & 0x0f) << 16)
                           | ((page[loc++] & 0xff) << 8) | (page[loc++] & 0xff));
            } else {
                // ghost
                return null;
            }
            if ((header & VALUE_FRAGMENTED) != 0) {
                return tree.mDatabase.reconstruct(caller, page, loc, len);
            }
        }

        byte[] value = new byte[len];
        arraycopy(page, loc, value, 0, len);
        return value;
    }

    /**
     * @param pos position as provided by binarySearch; must be positive
     */
    void retrieveLeafEntry(int pos, TreeCursor cursor) throws IOException {
        final byte[] page = mPage;
        int loc = decodeUnsignedShortLE(page, mSearchVecStart + pos);
        int header = page[loc++];
        int keyLen = header >= 0 ? ((header & 0x3f) + 1)
            : (((header & 0x3f) << 8) | ((page[loc++]) & 0xff));
        byte[] key = new byte[keyLen];
        arraycopy(page, loc, key, 0, keyLen);
        cursor.mKey = key;
        cursor.mValue = retrieveLeafValueAtLoc(this, cursor.mTree, page, loc + keyLen);
    }

    /**
     * Transactionally delete a leaf entry, replacing the value with a
     * ghost. When read back, it is interpreted as null. Ghosts are used by
     * transactional deletes, to ensure that they are not visible by cursors in
     * other transactions. They need to acquire a lock first. When the original
     * transaction commits, it deletes all the ghosted entries it created.
     *
     * <p>Caller must hold commit lock and exclusive latch on node.
     *
     * @param pos position as provided by binarySearch; must be positive
     */
    void txnDeleteLeafEntry(Transaction txn, Tree tree, byte[] key, int keyHash, int pos)
        throws IOException
    {
        final byte[] page = mPage;
        final int entryLoc = decodeUnsignedShortLE(page, mSearchVecStart + pos);
        int loc = entryLoc;

        // Read key header and skip key.
        int header = page[loc++];
        loc += (header >= 0 ? header : (((header & 0x3f) << 8) | (page[loc] & 0xff))) + 1;

        // Read value header.
        final int valueHeaderLoc = loc;
        header = page[loc++];

        doUndo: {
            // Note: Similar to leafEntryLengthAtLoc.
            if (header >= 0) {
                // Short value. Move loc to just past end of value.
                loc += header;
            } else {
                // Medium value. Move loc to just past end of value.
                if ((header & 0x20) == 0) {
                    loc += 2 + (((header & 0x1f) << 8) | (page[loc] & 0xff));
                } else if (header != -1) {
                    loc += 3 + (((header & 0x0f) << 16)
                                | ((page[loc] & 0xff) << 8) | (page[loc + 1] & 0xff));
                } else {
                    // Already a ghost, so nothing to undo.
                    break doUndo;
                }

                if ((header & VALUE_FRAGMENTED) != 0) {
                    int valueStartLoc = valueHeaderLoc + 2 + ((header & 0x20) >> 5);
                    tree.mDatabase.fragmentedTrash().add
                        (txn, tree.mId, page,
                         entryLoc, valueHeaderLoc - entryLoc,  // keyStart, keyLen
                         valueStartLoc, loc - valueStartLoc);  // valueStart, valueLen
                    break doUndo;
                }
            }

            // Copy whole entry into undo log.
            txn.undoStore(tree.mId, UndoLog.OP_INSERT, page, entryLoc, loc - entryLoc);
        }

        // Ghost will be deleted later when locks are released.
        tree.mLockManager.ghosted(txn, tree, key, keyHash);

        // Replace value with ghost.
        page[valueHeaderLoc] = (byte) -1;
        mGarbage += loc - valueHeaderLoc - 1;

        if (txn.mDurabilityMode != DurabilityMode.NO_REDO) {
            txn.redoStore(tree.mId, key, null);
        }
    }

    /**
     * Copies existing entry to undo log prior to it being updated. Fragmented
     * values are added to the trash and the fragmented bit is cleared. Caller
     * must hold commit lock and exlusive latch on node.
     *
     * @param pos position as provided by binarySearch; must be positive
     */
    void txnPreUpdateLeafEntry(Transaction txn, Tree tree, byte[] key, int pos)
        throws IOException
    {
        final byte[] page = mPage;
        final int entryLoc = decodeUnsignedShortLE(page, mSearchVecStart + pos);
        int loc = entryLoc;

        // Read key header and skip key.
        int header = page[loc++];
        loc += (header >= 0 ? header : (((header & 0x3f) << 8) | (page[loc] & 0xff))) + 1;

        // Read value header.
        final int valueHeaderLoc = loc;
        header = page[loc++];

        examineEntry: {
            // Note: Similar to leafEntryLengthAtLoc.
            if (header >= 0) {
                // Short value. Move loc to just past end of value.
                loc += header;
                break examineEntry;
            } else {
                // Medium value. Move loc to just past end of value.
                if ((header & 0x20) == 0) {
                    loc += 2 + (((header & 0x1f) << 8) | (page[loc] & 0xff));
                } else if (header != -1) {
                    loc += 3 + (((header & 0x0f) << 16)
                                | ((page[loc] & 0xff) << 8) | (page[loc + 1] & 0xff));
                } else {
                    // Already a ghost, so nothing to undo.
                    break examineEntry;
                }

                if ((header & VALUE_FRAGMENTED) != 0) {
                    int valueStartLoc = valueHeaderLoc + 2 + ((header & 0x20) >> 5);
                    tree.mDatabase.fragmentedTrash().add
                        (txn, tree.mId, page,
                         entryLoc, valueHeaderLoc - entryLoc,  // keyStart, keyLen
                         valueStartLoc, loc - valueStartLoc);  // valueStart, valueLen
                    // Clearing the fragmented bit prevents the update from
                    // double-deleting the fragments, and it also allows the
                    // old entry slot to be re-used.
                    page[valueHeaderLoc] = (byte) (header & ~VALUE_FRAGMENTED);
                    return;
                }
            }
        }

        // Copy whole entry into undo log.
        txn.undoStore(tree.mId, UndoLog.OP_UPDATE, page, entryLoc, loc - entryLoc);
    }

    /**
     * @param pos position as provided by binarySearch; must be positive
     */
    long retrieveChildRefId(int pos) {
        return decodeLongLE(mPage, mSearchVecEnd + 2 + (pos << 2));
    }

    /**
     * @param index index in child node array
     */
    long retrieveChildRefIdFromIndex(int index) {
        return decodeLongLE(mPage, mSearchVecEnd + 2 + (index << 3));
    }

    /**
     * @return length of encoded entry at given location
     */
    static int leafEntryLengthAtLoc(byte[] page, final int entryLoc) {
        int loc = entryLoc;
        int header = page[loc++];
        loc += (header >= 0 ? (header & 0x3f) : (((header & 0x3f) << 8) | (page[loc] & 0xff))) + 1;
        header = page[loc++];
        if (header >= 0) {
            loc += header;
        } else {
            if ((header & 0x20) == 0) {
                loc += 2 + (((header & 0x1f) << 8) | (page[loc] & 0xff));
            } else if (header != -1) {
                loc += 3 + (((header & 0x0f) << 16)
                            | ((page[loc] & 0xff) << 8) | (page[loc + 1] & 0xff));
            }
        }
        return loc - entryLoc;
    }

    /**
     * @return length of encoded entry at given location
     */
    static int internalEntryLengthAtLoc(byte[] page, final int entryLoc) {
        int header = page[entryLoc];
        return (header >= 0 ? (header & 0x3f)
                : (((header & 0x3f) << 8) | (page[entryLoc + 1] & 0xff))) + 2;
    }

    /**
     * @param pos complement of position as provided by binarySearch; must be positive
     */
    void insertLeafEntry(Tree tree, int pos, byte[] key, byte[] value)
        throws IOException
    {
        int encodedKeyLen = calculateKeyLength(key);
        int encodedLen = encodedKeyLen + calculateLeafValueLength(value);

        int fragmented;
        if (encodedLen <= tree.mMaxEntrySize) {
            fragmented = 0;
        } else {
            Database db = tree.mDatabase;
            value = db.fragment(this, value, db.mMaxFragmentedEntrySize - encodedKeyLen);
            if (value == null) {
                throw new LargeKeyException(key.length);
            }
            encodedLen = encodedKeyLen + calculateFragmentedValueLength(value);
            fragmented = VALUE_FRAGMENTED;
        }

        int entryLoc = createLeafEntry(tree, pos, encodedLen);

        if (entryLoc < 0) {
            splitLeafAndCreateEntry(tree, key, fragmented, value, encodedLen, pos, true);
        } else {
            copyToLeafEntry(key, fragmented, value, entryLoc);
        }
    }

    /**
     * @param pos complement of position as provided by binarySearch; must be positive
     */
    void insertFragmentedLeafEntry(Tree tree, int pos, byte[] key, byte[] value)
        throws IOException
    {
        int encodedKeyLen = calculateKeyLength(key);
        int encodedLen = encodedKeyLen + calculateFragmentedValueLength(value);

        int entryLoc = createLeafEntry(tree, pos, encodedLen);

        if (entryLoc < 0) {
            splitLeafAndCreateEntry(tree, key, VALUE_FRAGMENTED, value, encodedLen, pos, true);
        } else {
            copyToLeafEntry(key, VALUE_FRAGMENTED, value, entryLoc);
        }
    }

    /**
     * @param pos complement of position as provided by binarySearch; must be positive
     * @return Location for newly allocated entry, already pointed to by search
     * vector, or negative if leaf must be split. Complement of negative value
     * is maximum space available.
     */
    private int createLeafEntry(Tree tree, int pos, final int encodedLen) {
        int searchVecStart = mSearchVecStart;
        int searchVecEnd = mSearchVecEnd;

        int leftSpace = searchVecStart - mLeftSegTail;
        int rightSpace = mRightSegTail - searchVecEnd - 1;

        final byte[] page = mPage;

        int entryLoc;
        alloc: {
            if (pos < ((searchVecEnd - searchVecStart + 2) >> 1)) {
                // Shift subset of search vector left or prepend.
                if ((leftSpace -= 2) >= 0 &&
                    (entryLoc = allocPageEntry(encodedLen, leftSpace, rightSpace)) >= 0)
                {
                    arraycopy(page, searchVecStart, page, searchVecStart -= 2, pos);
                    pos += searchVecStart;
                    mSearchVecStart = searchVecStart;
                    break alloc;
                }
                // Need to make space, but restore leftSpace value first.
                leftSpace += 2;
            } else {
                // Shift subset of search vector right or append.
                if ((rightSpace -= 2) >= 0 &&
                    (entryLoc = allocPageEntry(encodedLen, leftSpace, rightSpace)) >= 0)
                {
                    pos += searchVecStart;
                    arraycopy(page, pos, page, pos + 2, (searchVecEnd += 2) - pos);
                    mSearchVecEnd = searchVecEnd;
                    break alloc;
                }
                // Need to make space, but restore rightSpace value first.
                rightSpace += 2;
            }

            // Compute remaining space surrounding search vector after insert completes.
            int remaining = leftSpace + rightSpace - encodedLen - 2;

            if (mGarbage > remaining) {
                // Do full compaction and free up the garbage, or else node must be split.
                if (mGarbage + remaining >= 0) {
                    return compactLeaf(tree, encodedLen, pos, true);
                }
                // Determine max possible entry size allowed, accounting too
                // for entry pointer, key length, and value length. Key and
                // value length might only require only require 1 byte fields,
                // but be safe and choose the larger size of 2.
                int max = mGarbage + leftSpace + rightSpace - (2 + 2 + 2);
                return max <= 0 ? -1 : ~max;
            }

            int vecLen = searchVecEnd - searchVecStart + 2;
            int newSearchVecStart;

            if (remaining > 0 || (mRightSegTail & 1) != 0) {
                // Re-center search vector, biased to the right, ensuring proper alignment.
                newSearchVecStart = (mRightSegTail - vecLen - 1 - (remaining >> 1)) & ~1;

                // Allocate entry from left segment.
                entryLoc = mLeftSegTail;
                mLeftSegTail = entryLoc + encodedLen;
            } else if ((mLeftSegTail & 1) == 0) {
                // Move search vector left, ensuring proper alignment.
                newSearchVecStart = mLeftSegTail + ((remaining >> 1) & ~1);

                // Allocate entry from right segment.
                entryLoc = mRightSegTail - encodedLen + 1;
                mRightSegTail = entryLoc - 1;
            } else {
                // Search vector is misaligned, so do full compaction.
                return compactLeaf(tree, encodedLen, pos, true);
            }

            arrayCopies(page,
                        searchVecStart, newSearchVecStart, pos,
                        searchVecStart + pos, newSearchVecStart + pos + 2, vecLen - pos);

            pos += newSearchVecStart;
            mSearchVecStart = newSearchVecStart;
            mSearchVecEnd = newSearchVecStart + vecLen;
        }

        // Write pointer to new allocation.
        encodeShortLE(page, pos, entryLoc);
        return entryLoc;
    }

    /**
     * Insert into an internal node following a child node split. This parent
     * node and child node must have an exclusive latch held. Child latch is
     * released, unless an exception is thrown.
     *
     * @param keyPos position to insert split key
     * @param splitChild child node which split
     */
    void insertSplitChildRef(Tree tree, int keyPos, Node splitChild)
        throws IOException
    {
        Database db = tree.mDatabase;
        if (db.shouldMarkDirty(splitChild)) {
            // It should be dirty as a result of the split itself.
            throw new AssertionError("Split child is not already marked dirty");
        }

        final Split split = splitChild.mSplit;
        final Node newChild = splitChild.rebindSplitFrames(split);
        splitChild.mSplit = null;

        //final Node leftChild;
        final Node rightChild;
        int newChildPos = keyPos >> 1;
        if (split.mSplitRight) {
            //leftChild = splitChild;
            rightChild = newChild;
            newChildPos++;
        } else {
            //leftChild = newChild;
            rightChild = splitChild;
        }

        // Positions of frames higher than split key need to be incremented.
        for (TreeCursorFrame frame = mLastCursorFrame; frame != null; ) {
            int framePos = frame.mNodePos;
            if (framePos > keyPos) {
                frame.mNodePos = framePos + 2;
            }
            frame = frame.mPrevCousin;
        }

        // Positions of frames equal to split key are in the split itself. Only
        // frames for the right split need to be incremented.
        for (TreeCursorFrame childFrame = rightChild.mLastCursorFrame; childFrame != null; ) {
            TreeCursorFrame frame = childFrame.mParentFrame;
            if (frame.mNode != this) {
                throw new AssertionError("Invalid cursor frame parent");
            }
            frame.mNodePos += 2;
            childFrame = childFrame.mPrevCousin;
        }

        // Update references to child node instances.
        {
            // TODO: recycle child node arrays
            Node[] newChildNodes = new Node[mChildNodes.length + 1];
            arraycopy(mChildNodes, 0, newChildNodes, 0, newChildPos);
            arraycopy(mChildNodes, newChildPos, newChildNodes, newChildPos + 1,
                      mChildNodes.length - newChildPos);
            newChildNodes[newChildPos] = newChild;
            mChildNodes = newChildNodes;

            // Rescale for long ids as encoded in page.
            newChildPos <<= 3;
        }

        // FIXME: IOException; how to rollback the damage?
        InResult result = createInternalEntry
            (tree, keyPos, split.splitKeyEncodedLength(), newChildPos, splitChild);

        // Write new child id.
        encodeLongLE(result.mPage, result.mNewChildLoc, newChild.mId);
        // Write key entry itself.
        split.copySplitKeyToParent(result.mPage, result.mEntryLoc);

        splitChild.releaseExclusive();
        newChild.releaseExclusive();

        // Split complete, so allow new node to be evictable.
        db.makeEvictable(newChild);
    }

    /**
     * Insert into an internal node following a child node split. This parent
     * node and child node must have an exclusive latch held. Child latch is
     * released, unless an exception is thrown.
     *
     * @param keyPos 2-based position
     * @param newChildPos 8-based position
     * @param splitChild pass null if split not allowed
     * @return null if entry must be split, but split is not allowed
     */
    private InResult createInternalEntry(Tree tree, int keyPos, int encodedLen,
                                         int newChildPos, Node splitChild)
        throws IOException
    {
        InResult result = null;

        int searchVecStart = mSearchVecStart;
        int searchVecEnd = mSearchVecEnd;

        int leftSpace = searchVecStart - mLeftSegTail;
        int rightSpace = mRightSegTail - searchVecEnd
            - ((searchVecEnd - searchVecStart) << 2) - 17;

        byte[] page = mPage;

        int entryLoc;
        alloc: {
            // Need to make room for one new search vector entry (2 bytes) and one new child
            // id entry (8 bytes). Determine which shift operations minimize movement.
            if (newChildPos < ((3 * (searchVecEnd - searchVecStart + 2) + keyPos + 8) >> 1)) {
                // Attempt to shift search vector left by 10, shift child ids left by 8.

                if ((leftSpace -= 10) >= 0 &&
                    (entryLoc = allocPageEntry(encodedLen, leftSpace, rightSpace)) >= 0)
                {
                    arraycopy(page, searchVecStart, page, searchVecStart - 10, keyPos);
                    arraycopy(page, searchVecStart + keyPos,
                              page, searchVecStart + keyPos - 8,
                              searchVecEnd - searchVecStart + 2 - keyPos + newChildPos);
                    mSearchVecStart = searchVecStart -= 10;
                    keyPos += searchVecStart;
                    mSearchVecEnd = searchVecEnd -= 8;
                    newChildPos += searchVecEnd + 2;
                    break alloc;
                }

                // Need to make space, but restore leftSpace value first.
                leftSpace += 10;
            } else {
                // Attempt to shift search vector left by 2, shift child ids right by 8.

                leftSpace -= 2;
                rightSpace -= 8;

                if (leftSpace >= 0 && rightSpace >= 0 &&
                    (entryLoc = allocPageEntry(encodedLen, leftSpace, rightSpace)) >= 0)
                {
                    arraycopy(page, searchVecStart, page, searchVecStart -= 2, keyPos);
                    mSearchVecStart = searchVecStart;
                    keyPos += searchVecStart;
                    arraycopy(page, searchVecEnd + newChildPos + 2,
                              page, searchVecEnd + newChildPos + (2 + 8),
                              ((searchVecEnd - searchVecStart) << 2) + 8 - newChildPos);
                    newChildPos += searchVecEnd + 2;
                    break alloc;
                }

                // Need to make space, but restore space values first.
                leftSpace += 2;
                rightSpace += 8;
            }

            // Compute remaining space surrounding search vector after insert completes.
            int remaining = leftSpace + rightSpace - encodedLen - 10;

            if (mGarbage > remaining) {
                // Do full compaction and free up the garbage, or split the node.
                if ((mGarbage + remaining) >= 0) {
                    return compactInternal(tree, encodedLen, keyPos, newChildPos);
                }

                // Node is full, so split it.

                if (splitChild == null) {
                    // Caller doesn't allow split.
                    return null;
                }

                // FIXME: IOException; how to rollback the damage?
                result = splitInternal(tree, keyPos, newChildPos, encodedLen);
                page = result.mPage;
                keyPos = result.mKeyLoc;
                newChildPos = result.mNewChildLoc;
                entryLoc = result.mEntryLoc;
                break alloc;
            }

            int vecLen = searchVecEnd - searchVecStart + 2;
            int childIdsLen = (vecLen << 2) + 8;
            int newSearchVecStart;

            if (remaining > 0 || (mRightSegTail & 1) != 0) {
                // Re-center search vector, biased to the right, ensuring proper alignment.
                newSearchVecStart =
                    (mRightSegTail - vecLen - childIdsLen - 9 - (remaining >> 1)) & ~1;

                // Allocate entry from left segment.
                entryLoc = mLeftSegTail;
                mLeftSegTail = entryLoc + encodedLen;
            } else if ((mLeftSegTail & 1) == 0) {
                // Move search vector left, ensuring proper alignment.
                newSearchVecStart = mLeftSegTail + ((remaining >> 1) & ~1);

                // Allocate entry from right segment.
                entryLoc = mRightSegTail - encodedLen + 1;
                mRightSegTail = entryLoc - 1;
            } else {
                // Search vector is misaligned, so do full compaction.
                return compactInternal(tree, encodedLen, keyPos, newChildPos);
            }

            int newSearchVecEnd = newSearchVecStart + vecLen;

            arrayCopies(page,
                        // Move search vector up to new key position.
                        searchVecStart, newSearchVecStart, keyPos,

                        // Move search vector after new key position, to new child
                        // id position.
                        searchVecStart + keyPos,
                        newSearchVecStart + keyPos + 2,
                        vecLen - keyPos + newChildPos,

                        // Move search vector after new child id position.
                        searchVecEnd + 2 + newChildPos,
                        newSearchVecEnd + 10 + newChildPos,
                        childIdsLen - newChildPos);

            keyPos += newSearchVecStart;
            newChildPos += newSearchVecEnd + 2;
            mSearchVecStart = newSearchVecStart;
            mSearchVecEnd = newSearchVecEnd;
        }

        // Write pointer to key entry.
        encodeShortLE(page, keyPos, entryLoc);

        if (result == null) {
            result = new InResult();
            result.mPage = page;
            result.mKeyLoc = keyPos;
            result.mNewChildLoc = newChildPos;
            result.mEntryLoc = entryLoc;
        }

        return result;
    }

    /**
     * Rebind cursor frames affected by split to correct node and
     * position. Caller must hold exclusive latch.
     *
     * @return latched sibling
     */
    private Node rebindSplitFrames(Split split) {
        final Node sibling = split.latchSibling();
        for (TreeCursorFrame frame = mLastCursorFrame; frame != null; ) {
            // Capture previous frame from linked list before changing the links.
            TreeCursorFrame prev = frame.mPrevCousin;
            split.rebindFrame(frame, sibling);
            frame = prev;
        }
        return sibling;
    }

    /**
     * @param pos position as provided by binarySearch; must be positive
     * @param fragmented 0 or VALUE_FRAGMENTED
     */
    void updateLeafValue(Tree tree, int pos, int fragmented, byte[] value) throws IOException {
        final byte[] page = mPage;
        final int searchVecStart = mSearchVecStart;

        final int start;
        final int keyLen;
        final int garbage;
        quick: {
            int loc;
            start = loc = decodeUnsignedShortLE(page, searchVecStart + pos);
            int header = page[loc++];
            loc += (header >= 0 ? header : (((header & 0x3f) << 8) | (page[loc] & 0xff))) + 1;

            final int valueHeaderLoc = loc;

            // Note: Similar to leafEntryLengthAtLoc and retrieveLeafValueAtLoc.
            int len = page[loc++];
            if (len < 0) largeValue: {
                if ((len & 0x20) == 0) {
                    header = len;
                    len = 1 + (((len & 0x1f) << 8) | (page[loc++] & 0xff));
                } else if (len != -1) {
                    header = len;
                    len = 1 + (((len & 0x0f) << 16)
                               | ((page[loc++] & 0xff) << 8) | (page[loc++] & 0xff));
                } else {
                    // ghost
                    len = 0;
                    break largeValue;
                }
                if ((header & VALUE_FRAGMENTED) != 0) {
                    tree.mDatabase.deleteFragments(this, page, loc, len);
                    // TODO: If new value needs to be fragmented too, try to
                    // re-use existing value slot.
                    if (fragmented == 0) {
                        // Clear fragmented bit in case new value can be quick copied.
                        page[valueHeaderLoc] = (byte) (header & ~VALUE_FRAGMENTED);
                    }
                }
            }

            final int valueLen = value.length;
            if (valueLen > len) {
                // Old entry is too small, and so it becomes garbage.
                keyLen = valueHeaderLoc - start;
                garbage = mGarbage + loc + len - start;
                break quick;
            }

            if (valueLen == len) {
                // Quick copy with no garbage created.
                if (valueLen == 0) {
                    // Ensure ghost is replaced.
                    page[valueHeaderLoc] = 0;
                } else {
                    arraycopy(value, 0, page, loc, valueLen);
                    if (fragmented != 0) {
                        page[valueHeaderLoc] |= fragmented;
                    }
                }
            } else {
                mGarbage += loc + len - copyToLeafValue
                    (page, fragmented, value, valueHeaderLoc) - valueLen;
            }

            return;
        }

        // What follows is similar to createLeafEntry method, except the search
        // vector doesn't grow.

        int searchVecEnd = mSearchVecEnd;

        int leftSpace = searchVecStart - mLeftSegTail;
        int rightSpace = mRightSegTail - searchVecEnd - 1;

        int encodedLen;
        if (fragmented != 0) {
            encodedLen = keyLen + calculateFragmentedValueLength(value);
        } else {
            encodedLen = keyLen + calculateLeafValueLength(value);
            if (encodedLen > tree.mMaxEntrySize) {
                Database db = tree.mDatabase;
                value = db.fragment(this, value, db.mMaxFragmentedEntrySize - keyLen);
                if (value == null) {
                    // TODO: Supply proper key length, not the encoded
                    // length. Subtracting 2 is just a guess.
                    throw new LargeKeyException(keyLen - 2);
                }
                encodedLen = keyLen + calculateFragmentedValueLength(value);
                fragmented = VALUE_FRAGMENTED;
            }
        }

        int entryLoc;
        alloc: {
            if ((entryLoc = allocPageEntry(encodedLen, leftSpace, rightSpace)) >= 0) {
                pos += searchVecStart;
                break alloc;
            }

            // Compute remaining space surrounding search vector after update completes.
            int remaining = leftSpace + rightSpace - encodedLen;

            if (garbage > remaining) {
                // Do full compaction and free up the garbage, or split the node.
                byte[] key = retrieveKey(pos);
                if ((garbage + remaining) < 0) {
                    if (mSplit == null) {
                        // Node is full, so split it.
                        splitLeafAndCreateEntry
                            (tree, key, fragmented, value, encodedLen, pos, false);
                        return;
                    }

                    // Node is already split, and so value is too large.
                    if (fragmented != 0) {
                        // FIXME: Can this happen?
                        throw new DatabaseException("Fragmented entry doesn't fit");
                    }
                    Database db = tree.mDatabase;
                    int max = Math.min(db.mMaxFragmentedEntrySize,
                                       garbage + leftSpace + rightSpace);
                    value = db.fragment(this, value, max);
                    if (value == null) {
                        throw new LargeKeyException(key.length);
                    }
                    encodedLen = keyLen + calculateFragmentedValueLength(value);
                    fragmented = VALUE_FRAGMENTED;
                }

                mGarbage = garbage;
                copyToLeafEntry(key, fragmented, value, compactLeaf(tree, encodedLen, pos, false));
                return;
            }

            int vecLen = searchVecEnd - searchVecStart + 2;
            int newSearchVecStart;

            if (remaining > 0 || (mRightSegTail & 1) != 0) {
                // Re-center search vector, biased to the right, ensuring proper alignment.
                newSearchVecStart = (mRightSegTail - vecLen - 1 - (remaining >> 1)) & ~1;

                // Allocate entry from left segment.
                entryLoc = mLeftSegTail;
                mLeftSegTail = entryLoc + encodedLen;
            } else if ((mLeftSegTail & 1) == 0) {
                // Move search vector left, ensuring proper alignment.
                newSearchVecStart = mLeftSegTail + ((remaining >> 1) & ~1);

                // Allocate entry from right segment.
                entryLoc = mRightSegTail - encodedLen + 1;
                mRightSegTail = entryLoc - 1;
            } else {
                // Search vector is misaligned, so do full compaction.
                byte[] key = retrieveKey(pos);
                mGarbage = garbage;
                copyToLeafEntry(key, fragmented, value, compactLeaf(tree, encodedLen, pos, false));
                return;
            }

            arraycopy(page, searchVecStart, page, newSearchVecStart, vecLen);

            pos += newSearchVecStart;
            mSearchVecStart = newSearchVecStart;
            mSearchVecEnd = newSearchVecStart + vecLen - 2;
        }

        // Copy existing key, and then copy value.
        arraycopy(page, start, page, entryLoc, keyLen);
        copyToLeafValue(page, fragmented, value, entryLoc + keyLen);
        encodeShortLE(page, pos, entryLoc);

        mGarbage = garbage;
    }

    /**
     * @param pos position as provided by binarySearch; must be positive
     */
    void updateChildRefId(int pos, long id) {
        encodeLongLE(mPage, mSearchVecEnd + 2 + (pos << 2), id);
    }

    /**
     * @param pos position as provided by binarySearch; must be positive
     */
    void deleteLeafEntry(Tree tree, int pos) throws IOException {
        final byte[] page = mPage;

        int searchVecStart = mSearchVecStart;
        final int entryLoc = decodeUnsignedShortLE(page, searchVecStart + pos);

        // Note: Similar to leafEntryLengthAtLoc and retrieveLeafValueAtLoc.
        int loc = entryLoc;
        int header = page[loc++];
        loc += (header >= 0 ? (header & 0x3f) : (((header & 0x3f) << 8) | (page[loc] & 0xff))) + 1;
        header = page[loc++];
        if (header >= 0) {
            loc += header;
        } else largeValue: {
            int len;
            if ((header & 0x20) == 0) {
                len = 1 + (((header & 0x1f) << 8) | (page[loc++] & 0xff));
            } else if (header != -1) {
                len = 1 + (((header & 0x0f) << 16)
                           | ((page[loc++] & 0xff) << 8) | (page[loc++] & 0xff));
            } else {
                // ghost
                break largeValue;
            }
            if ((header & VALUE_FRAGMENTED) != 0) {
                tree.mDatabase.deleteFragments(this, page, loc, len);
            }
            loc += len;
        }

        // Increment garbage by the size of the encoded entry.
        mGarbage += loc - entryLoc;

        int searchVecEnd = mSearchVecEnd;

        if (pos < ((searchVecEnd - searchVecStart + 2) >> 1)) {
            // Shift left side of search vector to the right.
            arraycopy(page, searchVecStart, page, searchVecStart += 2, pos);
            mSearchVecStart = searchVecStart;
        } else {
            // Shift right side of search vector to the left.
            pos += searchVecStart;
            arraycopy(page, pos + 2, page, pos, searchVecEnd - pos);
            mSearchVecEnd = searchVecEnd - 2;
        }
    }

    /**
     * Moves all the entries from the right node into the tail of the given
     * left node, and then deletes the right node node. Caller must ensure that
     * left node has enough room, and that both nodes are latched exclusively.
     * Caller must also hold commit lock. The right node is always released as
     * a side effect, but left node is never released by this method.
     */
    static void moveLeafToLeftAndDelete(Tree tree, Node leftNode, Node rightNode)
        throws IOException
    {
        tree.mDatabase.prepareToDelete(rightNode);

        final byte[] rightPage = rightNode.mPage;
        final int searchVecEnd = rightNode.mSearchVecEnd;
        final int leftEndPos = leftNode.highestLeafPos() + 2;

        int searchVecStart = rightNode.mSearchVecStart;
        while (searchVecStart <= searchVecEnd) {
            int entryLoc = decodeUnsignedShortLE(rightPage, searchVecStart);
            int encodedLen = leafEntryLengthAtLoc(rightPage, entryLoc);
            int leftEntryLoc = leftNode.createLeafEntry
                (tree, leftNode.highestLeafPos() + 2, encodedLen);
            // Note: Must access left page each time, since compaction can replace it.
            arraycopy(rightPage, entryLoc, leftNode.mPage, leftEntryLoc, encodedLen);
            searchVecStart += 2;
        }

        // All cursors in the right node must be moved to the left node.
        for (TreeCursorFrame frame = rightNode.mLastCursorFrame; frame != null; ) {
            // Capture previous frame from linked list before changing the links.
            TreeCursorFrame prev = frame.mPrevCousin;
            int framePos = frame.mNodePos;
            frame.unbind();
            frame.bind(leftNode, framePos + (framePos < 0 ? (-leftEndPos) : leftEndPos));
            frame = prev;
        }

        tree.mDatabase.deleteNode(rightNode);
    }

    /**
     * Moves all the entries from the right node into the tail of the given
     * left node, and then deletes the right node node. Caller must ensure that
     * left node has enough room, and that both nodes are latched exclusively.
     * Caller must also hold commit lock. The right node is always released as
     * a side effect, but left node is never released by this method.
     *
     * @param parentPage source of entry to merge from parent
     * @param parentLoc location of parent entry
     * @param parentLen length of parent entry
     */
    static void moveInternalToLeftAndDelete(Tree tree, Node leftNode, Node rightNode,
                                            byte[] parentPage, int parentLoc, int parentLen)
        throws IOException
    {
        tree.mDatabase.prepareToDelete(rightNode);

        // Create space to absorb parent key.
        int leftEndPos = leftNode.highestInternalPos();
        InResult result = leftNode.createInternalEntry
            (tree, leftEndPos, parentLen, (leftEndPos += 2) << 2, null);

        // Copy child id associated with parent key.
        final byte[] rightPage = rightNode.mPage;
        int rightChildIdsLoc = rightNode.mSearchVecEnd + 2;
        arraycopy(rightPage, rightChildIdsLoc, result.mPage, result.mNewChildLoc, 8);
        rightChildIdsLoc += 8;

        // Write parent key.
        arraycopy(parentPage, parentLoc, result.mPage, result.mEntryLoc, parentLen);

        final int searchVecEnd = rightNode.mSearchVecEnd;

        int searchVecStart = rightNode.mSearchVecStart;
        while (searchVecStart <= searchVecEnd) {
            int entryLoc = decodeUnsignedShortLE(rightPage, searchVecStart);
            int encodedLen = internalEntryLengthAtLoc(rightPage, entryLoc);

            // Allocate entry for left node.
            int pos = leftNode.highestInternalPos();
            result = leftNode.createInternalEntry(tree, pos, encodedLen, (pos + 2) << 2, null);

            // Copy child id.
            arraycopy(rightPage, rightChildIdsLoc, result.mPage, result.mNewChildLoc, 8);
            rightChildIdsLoc += 8;

            // Copy key.
            // Note: Must access left page each time, since compaction can replace it.
            arraycopy(rightPage, entryLoc, result.mPage, result.mEntryLoc, encodedLen);
            searchVecStart += 2;
        }

        // TODO: recycle child node arrays
        int leftLen = leftNode.mChildNodes.length;
        Node[] newChildNodes = new Node[leftLen + rightNode.mChildNodes.length];
        arraycopy(leftNode.mChildNodes, 0, newChildNodes, 0, leftLen);
        arraycopy(rightNode.mChildNodes, 0, newChildNodes, leftLen,
                  rightNode.mChildNodes.length);
        leftNode.mChildNodes = newChildNodes;

        // All cursors in the right node must be moved to the left node.
        for (TreeCursorFrame frame = rightNode.mLastCursorFrame; frame != null; ) {
            // Capture previous frame from linked list before changing the links.
            TreeCursorFrame prev = frame.mPrevCousin;
            int framePos = frame.mNodePos;
            frame.unbind();
            frame.bind(leftNode, leftEndPos + framePos);
            frame = prev;
        }

        tree.mDatabase.deleteNode(rightNode);
    }

    /**
     * Delete a parent reference to a merged child.
     *
     * @param childPos two-based position
     */
    void deleteChildRef(int childPos) {
        // Fix affected cursors.
        for (TreeCursorFrame frame = mLastCursorFrame; frame != null; ) {
            int framePos = frame.mNodePos;
            if (framePos >= childPos) {
                frame.mNodePos = framePos - 2;
            }
            frame = frame.mPrevCousin;
        }

        final byte[] page = mPage;
        int keyPos = childPos == 0 ? 0 : (childPos - 2);
        int searchVecStart = mSearchVecStart;

        int entryLoc = decodeUnsignedShortLE(page, searchVecStart + keyPos);
        // Increment garbage by the size of the encoded entry.
        mGarbage += internalEntryLengthAtLoc(page, entryLoc);

        // Update references to child node instances.
        // TODO: recycle child node arrays
        childPos >>= 1;
        Node[] newChildNodes = new Node[mChildNodes.length - 1];
        arraycopy(mChildNodes, 0, newChildNodes, 0, childPos);
        arraycopy(mChildNodes, childPos + 1, newChildNodes, childPos,
                  newChildNodes.length - childPos);
        mChildNodes = newChildNodes;
        // Rescale for long ids as encoded in page.
        childPos <<= 3;

        int searchVecEnd = mSearchVecEnd;

        // Remove search vector entry (2 bytes) and remove child id entry
        // (8 bytes). Determine which shift operations minimize movement.
        if (childPos < (3 * (searchVecEnd - searchVecStart) + keyPos + 8) >> 1) {
            // Shift child ids right by 8, shift search vector right by 10.
            arraycopy(page, searchVecStart + keyPos + 2,
                      page, searchVecStart + keyPos + (2 + 8),
                      searchVecEnd - searchVecStart - keyPos + childPos);
            arraycopy(page, searchVecStart, page, searchVecStart += 10, keyPos);
            mSearchVecEnd = searchVecEnd + 8;
        } else {
            // Shift child ids left by 8, shift search vector right by 2.
            arraycopy(page, searchVecEnd + childPos + (2 + 8),
                      page, searchVecEnd + childPos + 2,
                      ((searchVecEnd - searchVecStart) << 2) + 8 - childPos);
            arraycopy(page, searchVecStart, page, searchVecStart += 2, keyPos);
        }

        mSearchVecStart = searchVecStart;
    }

    /**
     * Delete this non-leaf root node, after all keys have been deleted. The
     * state of the lone child is swapped with this root node, and the child
     * node is repurposed into a stub root node. The old page used by the child
     * node is deleted. This design allows active cursors to still function
     * normally until they can unbind.
     *
     * <p>Caller must hold exclusive latches for root node and lone child.
     * Caller must also ensure that both nodes are not splitting. No latches
     * are released by this method.
     */
    void rootDelete(Tree tree) throws IOException {
        byte[] page = mPage;
        Node[] childNodes = mChildNodes;
        TreeCursorFrame lastCursorFrame = mLastCursorFrame;

        Node child = childNodes[0];
        tree.mDatabase.prepareToDelete(child);
        long toDelete = child.mId;
        int toDeleteState = child.mCachedState;

        mPage = child.mPage;
        byte stubType = mType;
        mType = child.mType;
        mGarbage = child.mGarbage;
        mLeftSegTail = child.mLeftSegTail;
        mRightSegTail = child.mRightSegTail;
        mSearchVecStart = child.mSearchVecStart;
        mSearchVecEnd = child.mSearchVecEnd;
        mChildNodes = child.mChildNodes;
        mLastCursorFrame = child.mLastCursorFrame;

        // Repurpose the child node into a stub root node. Stub is assigned a
        // reserved id (1) and a clean cached state. It cannot be marked dirty,
        // but it can be evicted when all cursors have unbound from it.
        child.mPage = page;
        child.mId = STUB_ID;
        child.mCachedState = CACHED_CLEAN;
        child.mType = stubType;
        child.clearEntries();
        child.mChildNodes = childNodes;
        child.mLastCursorFrame = lastCursorFrame;
        // Lone child of stub root points to actual root.
        childNodes[0] = this;
        // Search vector also needs to point to root.
        encodeLongLE(page, child.mSearchVecEnd + 2, this.mId);

        // Fix cursor bindings for this, the real root node.
        for (TreeCursorFrame frame = mLastCursorFrame; frame != null; ) {
            frame.mNode = this;
            frame = frame.mPrevCousin;
        }
        // Fix cursor bindings for the stub root node.
        for (TreeCursorFrame frame = lastCursorFrame; frame != null; ) {
            frame.mNode = child;
            frame = frame.mPrevCousin;
        }

        tree.addStub(child);

        // The page can be deleted earlier in the method, but doing it here
        // might prevent corruption if an unexpected exception occurs.
        tree.mDatabase.deletePage(toDelete, toDeleteState);
    }

    /**
     * Calculate encoded key length, including header.
     */
    static int calculateKeyLength(byte[] key) {
        int len = key.length;
        return len + ((len <= 64 & len > 0) ? 1 : 2);
    }

    /**
     * Calculate encoded value length for leaf, including header.
     */
    private static int calculateLeafValueLength(byte[] value) {
        int len = value.length;
        return len + ((len <= 127) ? 1 : ((len <= 8192) ? 2 : 3));
    }

    /**
     * Calculate encoded value length for leaf, including header.
     */
    private static int calculateFragmentedValueLength(byte[] value) {
        int len = value.length;
        return len + ((len <= 8192) ? 2 : 3);
    }

    /**
     * @return -1 if not enough contiguous space surrounding search vector
     */
    private int allocPageEntry(int encodedLen, int leftSpace, int rightSpace) {
        final int entryLoc;
        if (encodedLen <= leftSpace && leftSpace >= rightSpace) {
            // Allocate entry from left segment.
            entryLoc = mLeftSegTail;
            mLeftSegTail = entryLoc + encodedLen;
        } else if (encodedLen <= rightSpace) {
            // Allocate entry from right segment.
            entryLoc = mRightSegTail - encodedLen + 1;
            mRightSegTail = entryLoc - 1;
        } else {
            // No room.
            return -1;
        }
        return entryLoc;
    }

    /**
     * @param fragmented 0 or VALUE_FRAGMENTED
     */
    private void copyToLeafEntry(byte[] key, int fragmented, byte[] value, int entryLoc) {
        final byte[] page = mPage;

        final int len = key.length;
        if (len <= 64 && len > 0) {
            page[entryLoc++] = (byte) (len - 1);
        } else {
            page[entryLoc++] = (byte) (0x80 | (len >> 8));
            page[entryLoc++] = (byte) len;
        }
        arraycopy(key, 0, page, entryLoc, len);

        copyToLeafValue(page, fragmented, value, entryLoc + len);
    }

    /**
     * @param fragmented 0 or VALUE_FRAGMENTED
     * @return page location for first byte of value (first location after header)
     */
    private static int copyToLeafValue(byte[] page, int fragmented, byte[] value, int valueLoc) {
        final int len = value.length;
        if (len <= 127 && fragmented == 0) {
            page[valueLoc++] = (byte) len;
        } else if (len <= 8192) {
            page[valueLoc++] = (byte) (0x80 | fragmented | ((len - 1) >> 8));
            page[valueLoc++] = (byte) (len - 1);
        } else {
            page[valueLoc++] = (byte) (0xa0 | fragmented | ((len - 1) >> 16));
            page[valueLoc++] = (byte) ((len - 1) >> 8);
            page[valueLoc++] = (byte) (len - 1);
        }
        arraycopy(value, 0, page, valueLoc, len);
        return valueLoc;
    }

    /**
     * Compact leaf by reclaiming garbage and moving search vector towards
     * tail. Caller is responsible for ensuring that new entry will fit after
     * compaction. Space is allocated for new entry, and the search vector
     * points to it.
     *
     * @param encodedLen length of new entry to allocate
     * @param pos normalized search vector position of entry to insert/update
     * @return location for newly allocated entry, already pointed to by search vector
     */
    private int compactLeaf(Tree tree, int encodedLen, int pos, boolean forInsert) {
        byte[] page = mPage;

        int searchVecLoc = mSearchVecStart;
        // Size of search vector, possibly with new entry.
        int newSearchVecSize = mSearchVecEnd - searchVecLoc + 2;
        if (forInsert) {
            newSearchVecSize += 2;
        }
        pos += searchVecLoc;

        // Determine new location of search vector, with room to grow on both ends.
        int newSearchVecStart;
        // Capacity available to search vector after compaction.
        int searchVecCap = mGarbage + mRightSegTail + 1 - mLeftSegTail - encodedLen;
        newSearchVecStart = page.length - (((searchVecCap + newSearchVecSize) >> 1) & ~1);

        // Copy into a fresh buffer.

        int destLoc = TN_HEADER_SIZE;
        int newSearchVecLoc = newSearchVecStart;
        int newLoc = 0;
        final int searchVecEnd = mSearchVecEnd;

        Database db = tree.mDatabase;
        byte[] dest = db.removeSpareBuffer();

        for (; searchVecLoc <= searchVecEnd; searchVecLoc += 2, newSearchVecLoc += 2) {
            if (searchVecLoc == pos) {
                newLoc = newSearchVecLoc;
                if (forInsert) {
                    newSearchVecLoc += 2;
                } else {
                    continue;
                }
            }
            encodeShortLE(dest, newSearchVecLoc, destLoc);
            int sourceLoc = decodeUnsignedShortLE(page, searchVecLoc);
            int len = leafEntryLengthAtLoc(page, sourceLoc);
            arraycopy(page, sourceLoc, dest, destLoc, len);
            destLoc += len;
        }

        // Recycle old page buffer.
        db.addSpareBuffer(page);

        // Write pointer to new allocation.
        encodeShortLE(dest, newLoc == 0 ? newSearchVecLoc : newLoc, destLoc);

        mPage = dest;
        mGarbage = 0;
        mLeftSegTail = destLoc + encodedLen;
        mRightSegTail = dest.length - 1;
        mSearchVecStart = newSearchVecStart;
        mSearchVecEnd = newSearchVecStart + newSearchVecSize - 2;

        return destLoc;
    }

    /**
     * @param fragmented 0 or VALUE_FRAGMENTED
     * @param encodedLen length of new entry to allocate
     * @param pos normalized search vector position of entry to insert/update
     */
    private void splitLeafAndCreateEntry(Tree tree, byte[] key, int fragmented, byte[] value,
                                         int encodedLen, int pos, boolean forInsert)
        throws IOException
    {
        if (mSplit != null) {
            throw new AssertionError("Node is already split");
        }

        // Split can move node entries to a new left or right node. Choose such that the
        // new entry is more likely to go into the new node. This distributes the cost of
        // the split by postponing compaction of this node.

        // Since the split key and final node sizes are not known in advance, don't
        // attempt to properly center the new search vector. Instead, minimize
        // fragmentation to ensure that split is successful.

        byte[] page = mPage;

        if (page == EMPTY_BYTES) {
            // Node is a closed tree root.
            throw new ClosedIndexException();
        }

        Node newNode = tree.mDatabase.allocUnevictableNode();
        newNode.mType = TYPE_TN_LEAF;
        newNode.mGarbage = 0;

        byte[] newPage = newNode.mPage;

        if (forInsert && pos == 0) {
            // Inserting into left edge of node, possibly because inserts are
            // descending. Split into new left node, but only the new entry
            // goes into the new node. Split key is copied from this, the right
            // node. Because internal nodes point to the left child based on a
            // less-than comparison, future in-between inserts will go into the
            // left node.

            mSplit = new Split(false, newNode);
            mSplit.setKey(retrieveKey(0));

            // Position search vector at extreme left, allowing new entries to
            // be placed in a natural descending order.
            newNode.mLeftSegTail = TN_HEADER_SIZE;
            newNode.mSearchVecStart = TN_HEADER_SIZE;
            newNode.mSearchVecEnd = TN_HEADER_SIZE;

            int destLoc = newPage.length - encodedLen;
            newNode.copyToLeafEntry(key, fragmented, value, destLoc);
            encodeShortLE(newPage, TN_HEADER_SIZE, destLoc);

            newNode.mRightSegTail = destLoc - 1;
            newNode.releaseExclusive();

            return;
        }

        final int searchVecStart = mSearchVecStart;
        final int searchVecEnd = mSearchVecEnd;

        pos += searchVecStart;

        if (forInsert && pos == searchVecEnd + 2) {
            // Inserting into right edge of node, possibly because inserts are
            // ascending. Split into new right node, but only the new entry
            // goes into the new node. Split key is defined as the next higher
            // key from this node. Because internal nodes point to the right
            // child based on a greater-than-or-equal comparison, future
            // in-between inserts will go into the left node.

            mSplit = new Split(true, newNode);

            // Next higher key might be same as the new key. This is fine,
            // because the greater-than-or-equal comparison will still be
            // correct. Note that a "lowerKey" operation is not possible. It
            // would be like a repeating 0.999... decimal. This is why a
            // greater-than-or-equal rule is better than less-than-or-equal.
            mSplit.setKey(higherKey());

            // Position search vector at extreme right, allowing new entries to
            // be placed in a natural ascending order.
            newNode.mRightSegTail = newPage.length - 1;
            newNode.mSearchVecStart =
                newNode.mSearchVecEnd = newPage.length - 2;

            newNode.copyToLeafEntry(key, fragmented, value, TN_HEADER_SIZE);
            encodeShortLE(newPage, newPage.length - 2, TN_HEADER_SIZE);

            newNode.mLeftSegTail = TN_HEADER_SIZE + encodedLen;
            newNode.releaseExclusive();

            return;
        }

        // Amount of bytes available in unsplit node.
        int avail = availableLeafBytes();

        int garbageAccum = 0;
        int newLoc = 0;
        int newAvail = newPage.length - TN_HEADER_SIZE;

        // Guess which way to split by examining search position. This doesn't take into
        // consideration the variable size of the entries. If the guess is wrong, the new
        // entry is inserted into original node, which now has space.

        if ((pos - searchVecStart) < (searchVecEnd - pos)) {
            // Split into new left node.

            int destLoc = newPage.length;
            int newSearchVecLoc = TN_HEADER_SIZE;

            int searchVecLoc = searchVecStart;
            for (; newAvail > avail; searchVecLoc += 2, newSearchVecLoc += 2) {
                int entryLoc = decodeUnsignedShortLE(page, searchVecLoc);
                int entryLen = leafEntryLengthAtLoc(page, entryLoc);

                if (searchVecLoc == pos) {
                    if ((newAvail -= encodedLen + 2) < 0) {
                        // Entry doesn't fit into new node.
                        break;
                    }
                    newLoc = newSearchVecLoc;
                    if (forInsert) {
                        // Reserve slot in vector for new entry.
                        newSearchVecLoc += 2;
                        if (newAvail <= avail) {
                            // Balanced enough.
                            break;
                        }
                    } else {
                        // Don't copy old entry.
                        garbageAccum += entryLen;
                        avail += entryLen;
                        continue;
                    }
                }

                if ((newAvail -= entryLen + 2) < 0) {
                    // Entry doesn't fit into new node.
                    break;
                }

                // Copy entry and point to it.
                destLoc -= entryLen;
                arraycopy(page, entryLoc, newPage, destLoc, entryLen);
                encodeShortLE(newPage, newSearchVecLoc, destLoc);

                garbageAccum += entryLen;
                avail += entryLen + 2;
            }

            // Allocate Split object first, in case it throws an OutOfMemoryError.
            mSplit = new Split(false, newNode);

            // Prune off the left end of this node.
            mSearchVecStart = searchVecLoc;
            mGarbage += garbageAccum;

            newNode.mLeftSegTail = TN_HEADER_SIZE;
            newNode.mSearchVecStart = TN_HEADER_SIZE;
            newNode.mSearchVecEnd = newSearchVecLoc - 2;

            try {
                if (newLoc == 0) {
                    // Unable to insert new entry into left node. Insert it
                    // into the right node, which should have space now.
                    storeIntoSplitLeaf(tree, key, fragmented, value, encodedLen, forInsert);
                } else {
                    // Create new entry and point to it.
                    destLoc -= encodedLen;
                    newNode.copyToLeafEntry(key, fragmented, value, destLoc);
                    encodeShortLE(newPage, newLoc, destLoc);
                }
            } finally {
                // Split key is copied from this, the right node.
                mSplit.setKey(retrieveKey(0));
                newNode.mRightSegTail = destLoc - 1;
                newNode.releaseExclusive();
            }
        } else {
            // Split into new right node.

            int destLoc = TN_HEADER_SIZE;
            int newSearchVecLoc = newPage.length - 2;

            int searchVecLoc = searchVecEnd;
            for (; newAvail > avail; searchVecLoc -= 2, newSearchVecLoc -= 2) {
                int entryLoc = decodeUnsignedShortLE(page, searchVecLoc);
                int entryLen = leafEntryLengthAtLoc(page, entryLoc);

                if (forInsert) {
                    if (searchVecLoc + 2 == pos) {
                        if ((newAvail -= encodedLen + 2) < 0) {
                            // Inserted entry doesn't fit into new node.
                            break;
                        }
                        // Reserve spot in vector for new entry.
                        newLoc = newSearchVecLoc;
                        newSearchVecLoc -= 2;
                        if (newAvail <= avail) {
                            // Balanced enough.
                            break;
                        }
                    }
                } else {
                    if (searchVecLoc == pos) {
                        if ((newAvail -= encodedLen + 2) < 0) {
                            // Updated entry doesn't fit into new node.
                            break;
                        }
                        // Don't copy old entry.
                        newLoc = newSearchVecLoc;
                        garbageAccum += entryLen;
                        avail += entryLen;
                        continue;
                    }
                }

                if ((newAvail -= entryLen + 2) < 0) {
                    // Entry doesn't fit into new node.
                    break;
                }

                // Copy entry and point to it.
                arraycopy(page, entryLoc, newPage, destLoc, entryLen);
                encodeShortLE(newPage, newSearchVecLoc, destLoc);
                destLoc += entryLen;

                garbageAccum += entryLen;
                avail += entryLen + 2;
            }

            // Allocate Split object first, in case it throws an OutOfMemoryError.
            mSplit = new Split(true, newNode);

            // Prune off the right end of this node.
            mSearchVecEnd = searchVecLoc;
            mGarbage += garbageAccum;

            newNode.mRightSegTail = newPage.length - 1;
            newNode.mSearchVecStart = newSearchVecLoc + 2;
            newNode.mSearchVecEnd = newPage.length - 2;

            try {
                if (newLoc == 0) {
                    // Unable to insert new entry into new right node. Insert
                    // it into the left node, which should have space now.
                    storeIntoSplitLeaf(tree, key, fragmented, value, encodedLen, forInsert);
                } else {
                    // Create new entry and point to it.
                    newNode.copyToLeafEntry(key, fragmented, value, destLoc);
                    encodeShortLE(newPage, newLoc, destLoc);
                    destLoc += encodedLen;
                }
            } finally {
                // Split key is copied from the new right node.
                mSplit.setKey(newNode.retrieveKey(0));
                newNode.mLeftSegTail = destLoc;
                newNode.releaseExclusive();
            }
        }
    }

    /**
     * Store an entry into a node which has just been split and has room.
     *
     * @param fragmented 0 or VALUE_FRAGMENTED
     */
    private void storeIntoSplitLeaf(Tree tree, byte[] key, int fragmented, byte[] value,
                                    int encodedLen, boolean forInsert)
        throws IOException
    {
        int pos = binarySearch(key);
        if (forInsert) {
            if (pos >= 0) {
                throw new AssertionError("Key exists");
            }
            int entryLoc = createLeafEntry(tree, ~pos, encodedLen);
            while (entryLoc < 0) {
                if (fragmented != 0) {
                    // FIXME: Can this happen?
                    throw new DatabaseException("Fragmented entry doesn't fit");
                }
                Database db = tree.mDatabase;
                int max = Math.min(~entryLoc, db.mMaxFragmentedEntrySize);
                int encodedKeyLen = calculateKeyLength(key);
                value = db.fragment(this, value, max - encodedKeyLen);
                if (value == null) {
                    throw new LargeKeyException(key.length);
                }
                fragmented = VALUE_FRAGMENTED;
                encodedLen = encodedKeyLen + calculateFragmentedValueLength(value);
                entryLoc = createLeafEntry(tree, ~pos, encodedLen);
            }
            copyToLeafEntry(key, fragmented, value, entryLoc);
        } else {
            if (pos < 0) {
                throw new AssertionError("Key not found");
            }
            updateLeafValue(tree, pos, fragmented, value);
        }
    }

    private InResult splitInternal
        (final Tree tree, final int keyPos, final int newChildPos, final int encodedLen)
        throws IOException
    {
        if (mSplit != null) {
            throw new AssertionError("Node is already split");
        }

        // Split can move node entries to a new left or right node. Choose such that the
        // new entry is more likely to go into the new node. This distributes the cost of
        // the split by postponing compaction of this node.

        final byte[] page = mPage;

        final Node newNode = tree.mDatabase.allocUnevictableNode();
        newNode.mType = mType;
        newNode.mGarbage = 0;

        final byte[] newPage = newNode.mPage;

        final InResult result = new InResult();
        result.mPage = newPage;

        final int searchVecStart = mSearchVecStart;
        final int searchVecEnd = mSearchVecEnd;
        final int keyLoc = keyPos + searchVecStart;

        int garbageAccum;
        int newKeyLoc;

        // Guess which way to split by examining search position. This doesn't take into
        // consideration the variable size of the entries. If the guess is wrong, do over
        // the other way. Internal splits are infrequent, and split guesses are usually
        // correct. For these reasons, it isn't worth the trouble to create a special case
        // to charge ahead with the wrong guess. Leaf node splits are more frequent, and
        // incorrect guesses are easily corrected due to the simpler leaf node structure.

        // -2: left
        // -1: guess left
        // +1: guess right
        // +2: right
        int splitSide = (keyPos < (searchVecEnd - searchVecStart - keyPos)) ? -1 : 1;

        Split split;
        doSplit: while (true) {
            garbageAccum = 0;
            newKeyLoc = 0;

            // Amount of bytes used in unsplit node, including the page header.
            int size = 5 * (searchVecEnd - searchVecStart) + (1 + 8 + 8)
                + mLeftSegTail + page.length - mRightSegTail - mGarbage;

            int newSize = TN_HEADER_SIZE;

            // Adjust sizes for extra child id -- always one more than number of keys.
            size -= 8;
            newSize += 8;

            if (splitSide < 0) {
                // Split into new left node.

                // Since the split key and final node sizes are not known in advance,
                // don't attempt to properly center the new search vector. Instead,
                // minimize fragmentation to ensure that split is successful.

                int destLoc = newPage.length;
                int newSearchVecLoc = TN_HEADER_SIZE;

                int searchVecLoc = searchVecStart;
                while (true) {
                    if (searchVecLoc == keyLoc) {
                        newKeyLoc = newSearchVecLoc;
                        newSearchVecLoc += 2;
                        // Reserve slot in vector for new entry and account for size increase.
                        newSize += encodedLen + (2 + 8);
                    }

                    int entryLoc = decodeUnsignedShortLE(page, searchVecLoc);
                    int entryLen = internalEntryLengthAtLoc(page, entryLoc);

                    // Size change must incorporate child id, although they are copied later.
                    int sizeChange = entryLen + (2 + 8);
                    size -= sizeChange;
                    newSize += sizeChange;

                    sizeCheck: {
                        if (size <= TN_HEADER_SIZE || newSize >= newPage.length) {
                            // Moved too many entries to new node, so undo. Code can probably
                            // be written such that undo is not required, but this case is only
                            // expected to occur when using large keys.
                            if (searchVecLoc == keyLoc) {
                                // New entry doesn't fit.
                                newKeyLoc = 0;
                            }
                            newSearchVecLoc -= 2;
                            entryLoc = decodeUnsignedShortLE(page, searchVecLoc - 2);
                            entryLen = internalEntryLengthAtLoc(page, entryLoc);
                            destLoc += entryLen;
                        } else {
                            searchVecLoc += 2;

                            // Note that last examined key is not moved but instead
                            // dropped. Garbage must account for this.
                            garbageAccum += entryLen;

                            if (newSize < size) {
                                // Keep moving entries until balanced.
                                break sizeCheck;
                            }
                        }

                        // New node has accumlated enough entries...

                        if (newKeyLoc != 0) {
                            // ...and split key has been found.
                            split = new Split(false, newNode);
                            split.setKey(retrieveKeyAtLoc(page, entryLoc));
                            break;
                        }

                        if (splitSide == -1) {
                            // Guessed wrong; do over on right side.
                            splitSide = 2;
                            continue doSplit;
                        }

                        // Keep searching on this side for new entry location.
                        if (splitSide != -2) {
                            throw new AssertionError();
                        }
                    }

                    // Copy key entry and point to it.
                    destLoc -= entryLen;
                    arraycopy(page, entryLoc, newPage, destLoc, entryLen);
                    encodeShortLE(newPage, newSearchVecLoc, destLoc);
                    newSearchVecLoc += 2;
                }

                result.mEntryLoc = destLoc - encodedLen;

                // Copy existing child ids and insert new child id.
                {
                    arraycopy(page, searchVecEnd + 2,
                              newPage, newSearchVecLoc, newChildPos);

                    // Leave gap for new child id, to be set by caller.
                    result.mNewChildLoc = newSearchVecLoc + newChildPos;

                    int tailChildIdsLen = ((searchVecLoc - searchVecStart) << 2) - newChildPos;
                    arraycopy(page, searchVecEnd + 2 + newChildPos,
                              newPage, newSearchVecLoc + newChildPos + 8, tailChildIdsLen);

                    // Split references to child node instances. New child node has already
                    // been placed into mChildNodes by caller.
                    // TODO: recycle child node arrays
                    int leftLen = ((newSearchVecLoc - TN_HEADER_SIZE) >> 1) + 1;
                    Node[] leftChildNodes = new Node[leftLen];
                    Node[] rightChildNodes = new Node[mChildNodes.length - leftLen];
                    arraycopy(mChildNodes, 0, leftChildNodes, 0, leftLen);
                    arraycopy(mChildNodes, leftLen,
                              rightChildNodes, 0, rightChildNodes.length);
                    newNode.mChildNodes = leftChildNodes;
                    mChildNodes = rightChildNodes;
                }

                newNode.mLeftSegTail = TN_HEADER_SIZE;
                newNode.mRightSegTail = destLoc - encodedLen - 1;
                newNode.mSearchVecStart = TN_HEADER_SIZE;
                newNode.mSearchVecEnd = newSearchVecLoc - 2;
                newNode.releaseExclusive();

                // Prune off the left end of this node by shifting vector towards child ids.
                int shift = (searchVecLoc - searchVecStart) << 2;
                int len = searchVecEnd - searchVecLoc + 2;
                arraycopy(page, searchVecLoc,
                          page, mSearchVecStart = searchVecLoc + shift, len);
                mSearchVecEnd = searchVecEnd + shift;
            } else {
                // Split into new right node.

                // First copy keys and not the child ids. After keys are copied, shift to
                // make room for child ids and copy them in place.

                int destLoc = TN_HEADER_SIZE;
                int newSearchVecLoc = newPage.length;

                int searchVecLoc = searchVecEnd + 2;
                moveEntries: while (true) {
                    if (searchVecLoc == keyLoc) {
                        newSearchVecLoc -= 2;
                        newKeyLoc = newSearchVecLoc;
                        // Reserve slot in vector for new entry and account for size increase.
                        newSize += encodedLen + (2 + 8);
                    }

                    searchVecLoc -= 2;

                    int entryLoc = decodeUnsignedShortLE(page, searchVecLoc);
                    int entryLen = internalEntryLengthAtLoc(page, entryLoc);

                    // Size change must incorporate child id, although they are copied later.
                    int sizeChange = entryLen + (2 + 8);
                    size -= sizeChange;
                    newSize += sizeChange;

                    sizeCheck: {
                        if (size <= TN_HEADER_SIZE || newSize >= newPage.length) {
                            // Moved too many entries to new node, so undo. Code can probably
                            // be written such that undo is not required, but this case is only
                            // expected to occur when using large keys.
                            searchVecLoc += 2;
                            if (searchVecLoc == keyLoc) {
                                // New entry doesn't fit.
                                newKeyLoc = 0;
                            }
                            newSearchVecLoc += 2;
                            entryLoc = decodeUnsignedShortLE(page, searchVecLoc);
                            entryLen = internalEntryLengthAtLoc(page, entryLoc);
                            destLoc -= entryLen;
                        } else {
                            // Note that last examined key is not moved but instead
                            // dropped. Garbage must account for this.
                            garbageAccum += entryLen;

                            if (newSize < size) {
                                // Keep moving entries until balanced.
                                break sizeCheck;
                            }
                        }

                        // New node has accumlated enough entries...

                        if (newKeyLoc != 0) {
                            // ...and split key has been found.
                            split = new Split(true, newNode);
                            split.setKey(retrieveKeyAtLoc(page, entryLoc));
                            break moveEntries;
                        }

                        if (splitSide == 1) {
                            // Guessed wrong; do over on left side.
                            splitSide = -2;
                            continue doSplit;
                        }

                        // Keep searching on this side for new entry location.
                        if (splitSide != 2) {
                            throw new AssertionError();
                        }
                    }

                    // Copy key entry and point to it.
                    arraycopy(page, entryLoc, newPage, destLoc, entryLen);
                    newSearchVecLoc -= 2;
                    encodeShortLE(newPage, newSearchVecLoc, destLoc);
                    destLoc += entryLen;
                }

                result.mEntryLoc = destLoc;

                // Move new search vector to make room for child ids and be centered between
                // the segments.
                int newVecLen = page.length - newSearchVecLoc;
                {
                    int highestLoc = newPage.length - (5 * newVecLen) - 8;
                    int midLoc = ((destLoc + encodedLen + highestLoc + 1) >> 1) & ~1;
                    arraycopy(newPage, newSearchVecLoc, newPage, midLoc, newVecLen);
                    newKeyLoc -= newSearchVecLoc - midLoc;
                    newSearchVecLoc = midLoc;
                }

                int newSearchVecEnd = newSearchVecLoc + newVecLen - 2;

                // Copy existing child ids and insert new child id.
                {
                    int headChildIdsLen = newChildPos - ((searchVecLoc - searchVecStart + 2) << 2);
                    int newDestLoc = newSearchVecEnd + 2;
                    arraycopy(page, searchVecEnd + 2 + newChildPos - headChildIdsLen,
                              newPage, newDestLoc, headChildIdsLen);

                    // Leave gap for new child id, to be set by caller.
                    newDestLoc += headChildIdsLen;
                    result.mNewChildLoc = newDestLoc;

                    int tailChildIdsLen =
                        ((searchVecEnd - searchVecStart) << 2) + 16 - newChildPos;
                    arraycopy(page, searchVecEnd + 2 + newChildPos,
                              newPage, newDestLoc + 8, tailChildIdsLen);

                    // Split references to child node instances. New child node has already
                    // been placed into mChildNodes by caller.
                    // TODO: recycle child node arrays
                    int rightLen = ((newSearchVecEnd - newSearchVecLoc) >> 1) + 2;
                    Node[] rightChildNodes = new Node[rightLen];
                    Node[] leftChildNodes = new Node[mChildNodes.length - rightLen];
                    arraycopy(mChildNodes, leftChildNodes.length,
                              rightChildNodes, 0, rightLen);
                    arraycopy(mChildNodes, 0, leftChildNodes, 0, leftChildNodes.length);
                    newNode.mChildNodes = rightChildNodes;
                    mChildNodes = leftChildNodes;
                }

                newNode.mLeftSegTail = destLoc + encodedLen;
                newNode.mRightSegTail = newPage.length - 1;
                newNode.mSearchVecStart = newSearchVecLoc;
                newNode.mSearchVecEnd = newSearchVecEnd;
                newNode.releaseExclusive();

                // Prune off the right end of this node by shifting vector towards child ids.
                int len = searchVecLoc - searchVecStart;
                arraycopy(page, searchVecStart,
                          page, mSearchVecStart = searchVecEnd + 2 - len, len);
            }

            break;
        } // end doSplit

        mGarbage += garbageAccum;
        mSplit = split;

        result.mKeyLoc = newKeyLoc;
        return result;
    }

    /**
     * Compact internal node by reclaiming garbage and moving search vector
     * towards tail. Caller is responsible for ensuring that new entry will fit
     * after compaction. Space is allocated for new entry, and the search
     * vector points to it.
     *
     * @param encodedLen length of new entry to allocate
     * @param keyPos normalized search vector position of key to insert
     * @param childPos normalized search vector position of child node id to insert
     */
    private InResult compactInternal(Tree tree, int encodedLen, int keyPos, int childPos) {
        byte[] page = mPage;

        int searchVecLoc = mSearchVecStart;
        keyPos += searchVecLoc;
        // Size of search vector, with new entry.
        int newSearchVecSize = mSearchVecEnd - searchVecLoc + (2 + 2);

        // Determine new location of search vector, with room to grow on both ends.
        int newSearchVecStart;
        // Capacity available to search vector after compaction.
        int searchVecCap = mGarbage + mRightSegTail + 1 - mLeftSegTail - encodedLen;
        newSearchVecStart = page.length -
            (((searchVecCap + newSearchVecSize + ((newSearchVecSize + 2) << 2)) >> 1) & ~1);

        // Copy into a fresh buffer.

        int destLoc = TN_HEADER_SIZE;
        int newSearchVecLoc = newSearchVecStart;
        int newLoc = 0;
        final int searchVecEnd = mSearchVecEnd;

        Database db = tree.mDatabase;
        byte[] dest = db.removeSpareBuffer();

        for (; searchVecLoc <= searchVecEnd; searchVecLoc += 2, newSearchVecLoc += 2) {
            if (searchVecLoc == keyPos) {
                newLoc = newSearchVecLoc;
                newSearchVecLoc += 2;
            }
            encodeShortLE(dest, newSearchVecLoc, destLoc);
            int sourceLoc = decodeUnsignedShortLE(page, searchVecLoc);
            int len = internalEntryLengthAtLoc(page, sourceLoc);
            arraycopy(page, sourceLoc, dest, destLoc, len);
            destLoc += len;
        }

        if (newLoc == 0) {
            newLoc = newSearchVecLoc;
            newSearchVecLoc += 2;
        }

        // Copy child ids, and leave room for inserted child id.
        arraycopy(page, mSearchVecEnd + 2, dest, newSearchVecLoc, childPos);
        arraycopy(page, mSearchVecEnd + 2 + childPos,
                  dest, newSearchVecLoc + childPos + 8,
                  (newSearchVecSize << 2) - childPos);

        // Recycle old page buffer.
        db.addSpareBuffer(page);

        // Write pointer to key entry.
        encodeShortLE(dest, newLoc, destLoc);

        mPage = dest;
        mGarbage = 0;
        mLeftSegTail = destLoc + encodedLen;
        mRightSegTail = dest.length - 1;
        mSearchVecStart = newSearchVecStart;
        mSearchVecEnd = newSearchVecLoc - 2;

        InResult result = new InResult();
        result.mPage = dest;
        result.mKeyLoc = newLoc;
        result.mNewChildLoc = newSearchVecLoc + childPos;
        result.mEntryLoc = destLoc;

        return result;
    }

    /**
     * Provides information necessary to complete split by copying split key, pointer to
     * split key, and pointer to new child id.
     */
    static final class InResult {
        byte[] mPage;
        int mKeyLoc;
        int mNewChildLoc;
        int mEntryLoc;
    }

    /**
     * Count the number of cursors bound to this node.
     */
    long countCursors() {
        long count = 0;

        acquireShared();
        try {
            TreeCursorFrame frame = mLastCursorFrame;
            while (frame != null) {
                count++;
                frame = frame.mPrevCousin;
            }
        } finally {
            releaseShared();
        }

        return count;
    }

    /**
     * No latches are acquired by this method -- it is only used for debugging.
     */
    @Override
    public String toString() {
        String prefix;

        switch (mType) {
        default:
            return "Node: {id=" + mId +
                ", cachedState=" + mCachedState +
                ", lockState=" + super.toString() +
                '}';
        case TYPE_UNDO_LOG:
            return "UndoNode: {id=" + mId +
                ", cachedState=" + mCachedState +
                ", topEntry=" + mGarbage +
                ", lowerNodeId=" + + decodeLongLE(mPage, 4) +
                ", lockState=" + super.toString() +
                '}';
        case TYPE_FRAGMENT:
            return "FragmentNode: {id=" + mId +
                ", cachedState=" + mCachedState +
                ", lockState=" + super.toString() +
                '}';
        case TYPE_TN_IN:
            prefix = "Internal";
            break;

        case TYPE_TN_BIN:
            prefix = "BottomInternal";
            break;

        case TYPE_TN_LEAF:
            prefix = "Leaf";
            break;
        }

        return prefix + "Node: {id=" + mId +
            ", cachedState=" + mCachedState +
            ", isSplit=" + (mSplit != null) +
            ", availableBytes=" + availableBytes() +
            ", lockState=" + super.toString() +
            '}';
    }

    /**
     * Caller must acquired shared latch before calling this method. Latch is
     * released unless an exception is thrown. If an exception is thrown by the
     * observer, the latch would have already been released.
     *
     * @return false if should stop
     */
    boolean verifyTreeNode(int level, VerificationObserver observer) {
        if (mType != TYPE_TN_IN && mType != TYPE_TN_BIN && mType != TYPE_TN_LEAF) {
            return verifyFailed(level, observer, "Not a tree node");
        }

        final byte[] page = mPage;

        if (mLeftSegTail < TN_HEADER_SIZE) {
            return verifyFailed(level, observer, "Left segment tail: " + mLeftSegTail);
        }

        if (mSearchVecStart < mLeftSegTail) {
            return verifyFailed(level, observer, "Search vector start: " + mSearchVecStart);
        }

        if (mSearchVecEnd < (mSearchVecStart - 2)) {
            return verifyFailed(level, observer, "Search vector end: " + mSearchVecEnd);
        }

        if (mRightSegTail < mSearchVecEnd || mRightSegTail > (page.length - 1)) {
            return verifyFailed(level, observer, "Right segment tail: " + mRightSegTail);
        }

        if (!isLeaf()) {
            if (numKeys() + 1 != mChildNodes.length) {
                return verifyFailed(level, observer, "Wrong number of child nodes: " +
                                    (numKeys() + 1) + " != " + mChildNodes.length);
            }

            int childIdsStart = mSearchVecEnd + 2;
            int childIdsEnd = childIdsStart + ((childIdsStart - mSearchVecStart) << 2) + 8;
            if (childIdsEnd > (mRightSegTail + 1)) {
                return verifyFailed(level, observer, "Child ids end: " + childIdsEnd);
            }

            LHashTable.Int childIds = new LHashTable.Int(512);

            for (int i = childIdsStart; i < childIdsEnd; i += 8) {
                long childId = decodeLongLE(page, i);
                if (childId < 0 || childId == 0 || childId == 1) {
                    return verifyFailed(level, observer, "Illegal child id: " + childId);
                }
                LHashTable.IntEntry e = childIds.insert(childId);
                if (e.value != 0) {
                    return verifyFailed(level, observer, "Duplicate child id: " + childId);
                }
                e.value = 1;
            }
        }

        int used = TN_HEADER_SIZE + mRightSegTail + 1 - mLeftSegTail;

        int largeValueCount = 0;

        int lastKeyLoc = 0;
        int lastKeyLen = 0;

        for (int i = mSearchVecStart; i <= mSearchVecEnd; i += 2) {
            int loc = decodeUnsignedShortLE(page, i);

            if (loc < TN_HEADER_SIZE || loc >= page.length ||
                (loc >= mLeftSegTail && loc <= mRightSegTail))
            {
                return verifyFailed(level, observer, "Entry location: " + loc);
            }

            if (isLeaf()) {
                used += leafEntryLengthAtLoc(page, loc);
            } else {
                used += internalEntryLengthAtLoc(page, loc);
            }

            int keyLen;
            try {
                keyLen = page[loc++];
                keyLen = keyLen >= 0 ? ((keyLen & 0x3f) + 1)
                    : (((keyLen & 0x3f) << 8) | ((page[loc++]) & 0xff));
            } catch (IndexOutOfBoundsException e) {
                return verifyFailed(level, observer, "Key location out of bounds");
            }

            if (loc + keyLen > page.length) {
                return verifyFailed(level, observer, "Key end location: " + (loc + keyLen));
            }

            if (lastKeyLoc != 0) {
                int result = compareKeys(page, lastKeyLoc, lastKeyLen, page, loc, keyLen);
                if (result >= 0) {
                    return verifyFailed(level, observer, "Key order: " + result);
                }
            }

            lastKeyLoc = loc;
            lastKeyLoc = keyLen;

            if (isLeaf()) value: {
                int len;
                try {
                    loc += keyLen;
                    int header = page[loc++];
                    if (header >= 0) {
                        len = header;
                    } else {
                        if ((header & 0x20) == 0) {
                            len = 1 + (((header & 0x1f) << 8) | (page[loc++] & 0xff));
                        } else if (header != -1) {
                            len = 1 + (((header & 0x0f) << 16)
                                       | ((page[loc++] & 0xff) << 8) | (page[loc++] & 0xff));
                        } else {
                            // ghost
                            break value;
                        }
                        if ((header & VALUE_FRAGMENTED) != 0) {
                            largeValueCount++;
                        }
                    }
                } catch (IndexOutOfBoundsException e) {
                    return verifyFailed(level, observer, "Value location out of bounds");
                }
                if (loc + len > page.length) {
                    return verifyFailed(level, observer, "Value end location: " + (loc + len));
                }
            }
        }

        int garbage = page.length - used;

        if (mGarbage != garbage) {
            return verifyFailed(level, observer, "Garbage: " + mGarbage + " != " + garbage);
        }

        int entryCount = numKeys();
        int freeBytes = availableBytes();

        long id = mId;
        releaseShared();
        return observer.indexNodePassed(id, level, entryCount, freeBytes, largeValueCount);
    }

    private boolean verifyFailed(int level, VerificationObserver observer, String message) {
        long id = mId;
        releaseShared();
        observer.failed = true;
        return observer.indexNodeFailed(id, level, message);
    }

    /**
     * Counts all the enties in the tree rooted at this node. No latches are
     * acquired by this method -- it is only used for debugging.
     */
    /*
    long countEntries(Database db) throws IOException {
        if (isLeaf()) {
            return 1 + ((mSearchVecEnd - mSearchVecStart) >> 1);
        }

        Node child = mChildNodes[mChildNodes.length - 1];
        long childId = retrieveChildRefIdFromIndex(mChildNodes.length - 1);

        if (child == null || childId != child.mId) {
            child = new Node(db.pageSize());
            child.read(db, childId);
        }

        long count = child.countEntries(db);

        for (int pos = mSearchVecEnd - mSearchVecStart; pos >= 0; pos -= 2) {
            child = mChildNodes[pos >> 1];
            childId = retrieveChildRefId(pos);

            if (child == null || childId != child.mId) {
                child = new Node(db.pageSize());
                child.read(db, childId);
            }

            count += child.countEntries(db);
        }

        return count;
    }
    */

    /**
     * Counts all the pages used to store the tree rooted at this node. No
     * latches are acquired by this method -- it is only used for debugging.
     */
    /*
    long countPages(Database db) throws IOException {
        if (isLeaf()) {
            return 1;
        }

        Node child = mChildNodes[mChildNodes.length - 1];
        long childId = retrieveChildRefIdFromIndex(mChildNodes.length - 1);

        if (child == null || childId != child.mId) {
            child = new Node(db.pageSize());
            child.read(db, childId);
        }

        long count = child.countPages(db);

        for (int pos = mSearchVecEnd - mSearchVecStart; pos >= 0; pos -= 2) {
            child = mChildNodes[pos >> 1];
            childId = retrieveChildRefId(pos);

            if (child == null || childId != child.mId) {
                child = new Node(db.pageSize());
                child.read(db, childId);
            }

            count += child.countPages(db);
        }

        return count + 1;
    }
    */

    /**
     * Clears a bit for each page used to store the tree rooted at this node. No
     * latches are acquired by this method -- it is only used for debugging.
     */
    /*
    void tracePages(Database db, java.util.BitSet bits) throws IOException {
        if (mId == 0) {
            return;
        }

        if (!bits.get((int) mId)) {
            throw new CorruptDatabaseException("Page already seen: " + mId);
        }
        bits.clear((int) mId);

        if (isLeaf()) {
            return;
        }

        Node child = mChildNodes[mChildNodes.length - 1];
        long childId = retrieveChildRefIdFromIndex(mChildNodes.length - 1);

        if (child == null || childId != child.mId) {
            child = new Node(db.pageSize());
            child.read(db, childId);
        }

        child.tracePages(db, bits);

        for (int pos = mSearchVecEnd - mSearchVecStart; pos >= 0; pos -= 2) {
            child = mChildNodes[pos >> 1];
            childId = retrieveChildRefId(pos);

            if (child == null || childId != child.mId) {
                child = new Node(db.pageSize());
                child.read(db, childId);
            }

            child.tracePages(db, bits);
        }
    }
    */

    /**
     * Prints the contents of tree rooted at this node. No latches are acquired
     * by this method -- it is only used for debugging.
     */
    /*
    void dump(Tree tree, String indent) throws IOException {
        Database db = tree.mDatabase;
        verify0();

        if (!hasKeys()) {
            System.out.println(indent + mId + ": (empty)");
            return;
        }

        if (isLeaf()) {
            if (!hasKeys()) {
                System.out.println(indent + mId + ": (empty)");
                return;
            }
            for (int pos = mSearchVecEnd - mSearchVecStart; pos >= 0; pos -= 2) {
                byte[] key = retrieveKey(pos);
                byte[] value = retrieveLeafValue(tree, pos);
                System.out.println(indent + mId + ": " +
                                   dumpToString(key) + " = " + dumpToString(value));
            }
            return;
        }

        Node child = mChildNodes[mChildNodes.length - 1];
        long childId = retrieveChildRefIdFromIndex(mChildNodes.length - 1);

        if (child == null || childId != child.mId) {
            child = new Node(db.pageSize());
            child.read(db, childId);
        }

        if (child != null) {
            child.dump(tree, indent + "  ");
        }

        for (int pos = mSearchVecEnd - mSearchVecStart; pos >= 0; pos -= 2) {
            System.out.println(indent + mId + ": " + dumpToString(retrieveKey(pos)));

            child = mChildNodes[pos >> 1];
            childId = retrieveChildRefId(pos);

            if (child == null || childId != child.mId) {
                child = new Node(db.pageSize());
                child.read(db, childId);
            }

            if (child != null) {
                child.dump(tree, indent + "  ");
            }
        }
    }

    private static String dumpToString(byte[] bytes) {
        if (bytes == null) {
            return "null";
        }
        for (byte b : bytes) {
            if (b < '-' || b > 'z') {
                throw new AssertionError(Arrays.toString(bytes));
            }
        }
        return new String(bytes);
    }
    */
}
