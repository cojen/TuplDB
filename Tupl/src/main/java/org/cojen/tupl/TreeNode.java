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

import java.io.InterruptedIOException;
import java.io.IOException;

import java.util.Arrays;

import java.util.concurrent.locks.Lock;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class TreeNode extends Latch {
    // Note: Changing these values affects how Database handles the commit flag.
    static final byte CACHED_CLEAN = 0, CACHED_DIRTY_0 = 1, CACHED_DIRTY_1 = 2;

    static final byte TYPE_LEAF = 0, TYPE_INTERNAL = 1;

    static final int HEADER_SIZE = 12;

    static final int STUB_ID = 1;

    private static final int FAILED = 0, SUCCESS = 1, SPLIT = 2;

    // These fields are managed exclusively by Database.
    TreeNode mMoreUsed; // points to more recently used node
    TreeNode mLessUsed; // points to less recently used node

    /*
      All node types have a similar structure and support a maximum page size of 65536
      bytes. The ushort type is an unsigned byte pair, and the ulong type is eight
      bytes. All multibyte types are big endian encoded.

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
      need to be repaied. A compaction operation reclaims garbage by rebuilding the
      segments and search vector. A copying garbage collection algorithm is used for this.

      The compaction implementation allocates all surviving entries in the left segment,
      leaving an empty right segment. There is no requirement that the segments be
      balanced -- this only applies to the free space surrounding the search vector.

      Leaf nodes support variable length keys and values, encoded as a pair, within the
      segments. Entries in the search vector are ushort pointers into the segments. No
      distinction is made between the segments because the pointers are absolute.

      Key-value pairs entries start with a one byte header:

      0x00..0x3f: key is 1..64 bytes, value is >0 bytes.
      0x40..0x7f: key is 1..64 bytes, value is 0 bytes.
      0x80..0xbf: key is 0..16383 bytes, value is >0 bytes.
      0xc0..0xff: key is 0..16383 bytes, value is 0 bytes.

      For keys 1..64 bytes in length, the length is defined as ((header & 0x3f) + 1). For
      keys 0..16383 bytes in length, a second header byte is used. The second byte is
      unsigned, and the length is defined as (((header & 0x3f) << 8) | header2). The key
      contents immediately follow the header byte(s).

      Special support for zero-length values is provided as an optimization for supporting
      non-unique indexes. Such an index has no use for encoding values.

      If a value exists, it immediately follows the key. The value header encodes its length:

      0x00..0x7f: value is 1..128 bytes
      0x80..0xff: value is 129..32896 bytes

      For values 1..128 bytes in length, the length is defined as ((header & 0x7f) +
      1). For values 129..32896 bytes in length, a second header byte is used. The length
      is then defined as ((((header & 0x7f) << 8) | header2) + 129). The value contents
      immediately follow the header byte(s).

      The "values" for internal nodes are actually identifiers for child nodes. The number
      of child nodes is always one more than the number of keys. For this reason, the
      key-value format used by leaf nodes cannot be applied to internal nodes. Also, the
      identifiers are always a fixed length, ulong type.

      Child node identifiers are encoded immediately following the search vector. Free space
      management must account for this, treating it as an extention to the search vector.

      Key entries for internal nodes start with a one byte header:

      0x00..0x7f: key is 1..128 bytes
      0x80..0xff: key is 0..32767 bytes

      For keys 1..128 bytes in length, the length is defined as (header + 1). For
      keys 0..32767 bytes in length, a second header byte is used. The length is then
      defined as (((header & 0x7f) << 8) | header2). The key contents immediately follow
      the header byte(s).

      Note that internal nodes can support keys larger than leaf nodes, but this extra
      length cannot be used. The maximum key length is limited by leaf encoding. A future
      encoding format might support very large keys.

     */

    // Raw contents of node.
    byte[] mPage;

    long mId;
    byte mCachedState;

    // Entries from header, available as fields for quick access.
    byte mType;
    int mGarbage;
    int mLeftSegTail;
    int mRightSegTail;
    int mSearchVecStart;
    int mSearchVecEnd;

    // References to child nodes currently available. Is null for leaf nodes.
    TreeNode[] mChildNodes;

    // Linked stack of TreeCursorFrames bound to this TreeNode.
    TreeCursorFrame mLastCursorFrame;

    // Set by a partially completed split.
    Split mSplit;

    TreeNode(int pageSize, boolean newEmptyRoot) {
        mPage = new byte[pageSize];

        if (newEmptyRoot) {
            mId = 0;
            mCachedState = CACHED_CLEAN;
            mType = TYPE_LEAF;
            clearEntries();
        }
    }

    private void clearEntries() {
        mGarbage = 0;
        mLeftSegTail = HEADER_SIZE;
        int pageSize = mPage.length;
        mRightSegTail = pageSize - 1;
        // Search vector location must be even.
        mSearchVecStart = (HEADER_SIZE + ((pageSize - HEADER_SIZE) >> 1)) & ~1;
        mSearchVecEnd = mSearchVecStart - 2; // inclusive
    }

    /**
     * Root search.
     *
     * @param key search key
     * @return copy of value or null if not found
     */
    byte[] search(Tree tree, byte[] key) throws IOException {
        acquireSharedUnfair();
        // Note: No need to check if root has split, since root splits are always
        // completed before releasing the root latch.
        return isLeaf() ? subSearchLeaf(key) : subSearch(tree, this, null, key, false);
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
    private static byte[] subSearch(Tree tree, TreeNode node, Latch parentLatch,
                                    byte[] key, boolean exclusiveHeld)
        throws IOException
    {
        // Caller invokes Database.used for this TreeNode. Root node is not
        // managed in usage list, because it cannot be evicted.

        int childPos;
        long childId;

        loop: while (true) {
            childPos = internalPos(node.binarySearchInternal(key));

            TreeNode childNode = node.mChildNodes[childPos >> 1];
            childId = node.retrieveChildRefId(childPos);

            childCheck: if (childNode != null && childId == childNode.mId) {
                childNode.acquireSharedUnfair();

                // Need to check again in case evict snuck in.
                if (childId != childNode.mId) {
                    childNode.releaseShared();
                    break childCheck;
                }

                if (!exclusiveHeld && parentLatch != null) {
                    parentLatch.releaseShared();
                }

                if (childNode.mSplit != null) {
                    childNode = childNode.mSplit.selectNodeShared(tree.mDatabase, childNode, key);
                }

                if (childNode.isLeaf()) {
                    node.release(exclusiveHeld);
                    tree.mDatabase.used(childNode);
                    return childNode.subSearchLeaf(key);
                } else {
                    // Keep shared latch on this parent node, in case sub search
                    // needs to upgrade its shared latch.
                    if (exclusiveHeld) {
                        node.downgrade();
                    }
                    tree.mDatabase.used(childNode);
                    return subSearch(tree, childNode, node, key, false);
                }
            } // end childCheck

            // Child needs to be loaded.

            if (exclusiveHeld = node.tryUpgrade(parentLatch, exclusiveHeld)) {
                // Succeeded in upgrading latch, so break out to load child.
                parentLatch = null;
                break loop;
            }

            // Release shared latch, re-acquire exclusive latch, and start over.

            node.releaseShared();
            node.acquireExclusiveUnfair();
            exclusiveHeld = true;
            if (parentLatch != null) {
                parentLatch.releaseShared();
                parentLatch = null;
            }

            if (node.mSplit != null) {
                // Node might have split while shared latch was not held.
                node = node.mSplit.selectNodeExclusive(tree.mDatabase, node, key);
            }

            if (node == tree.mRoot) {
                // This is the root node, and so no parent latch exists. It is
                // possible that a delete slipped in when the latch was
                // released, and that the root is now a leaf.
                if (node.isLeaf()) {
                    node.downgrade();
                    return node.subSearchLeaf(key);
                }
            }
        } // end loop

        // If this point is reached, exclusive latch for this node is held and
        // child needs to be loaded. Parent latch has been released.

        TreeNode childNode = tree.mDatabase.allocLatchedNode();
        childNode.mId = childId;
        node.mChildNodes[childPos >> 1] = childNode;

        // Release parent latch before child has been loaded. Any threads
        // which wish to access the same child will block until this thread
        // has finished loading the child and released its exclusive latch.
        node.releaseExclusive();

        try {
            childNode.read(tree.mDatabase, childId);
        } catch (IOException e) {
            // Another thread might access child and see that it is invalid because
            // id is zero. It will assume it got evicted and will load child again.
            childNode.mId = 0;
            childNode.releaseExclusive();
            throw e;
        }

        if (childNode.isLeaf()) {
            childNode.downgrade();
            return childNode.subSearchLeaf(key);
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
    private byte[] subSearchLeaf(byte[] key) {
        int childPos = binarySearchLeaf(key);
        if (childPos < 0) {
            releaseShared();
            return null;
        }
        byte[] value = retrieveLeafValue(childPos);
        releaseShared();
        return value;
    }

    /**
     * Caller must hold exclusive root latch and it must verify that root has split.
     *
     * @param stub Old root node stub, latched exclusively, whose cursors must
     * transfer into the new root. Stub latch is released by this method.
     */
    void finishSplitRoot(Database db, TreeNode stub) throws IOException {
        // Create a child node and copy this root node state into it. Then update this
        // root node to point to new and split child nodes. New root is always an internal node.

        TreeNode child = db.newNodeForSplit();

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
        final TreeNode sibling = rebindSplitFrames(db, split);
        mSplit = null;

        TreeNode left, right;
        if (split.mSplitRight) {
            left = child;
            right = sibling;
        } else {
            left = sibling;
            right = child;
        }

        int keyLen = split.copySplitKeyToParent(newPage, HEADER_SIZE);

        // Create new single-element search vector.
        final int searchVecStart =
            ((newPage.length - HEADER_SIZE - keyLen - (2 + 8 + 8)) >> 1) & ~1;
        DataIO.writeShort(newPage, searchVecStart, HEADER_SIZE);
        DataIO.writeLong(newPage, searchVecStart + 2, left.mId);
        DataIO.writeLong(newPage, searchVecStart + 2 + 8, right.mId);

        // FIXME: recycle these arrays
        mChildNodes = new TreeNode[] {left, right};

        mPage = newPage;
        mType = TYPE_INTERNAL;
        mGarbage = 0;
        mLeftSegTail = HEADER_SIZE + keyLen;
        mRightSegTail = newPage.length - 1;
        mSearchVecStart = searchVecStart;
        mSearchVecEnd = searchVecStart;
        mLastCursorFrame = null;

        // Add a parent cursor frame for all left and right node cursors.
        addParentFrames(stub, left, 0);
        addParentFrames(stub, right, 2);

        child.releaseExclusive();
        sibling.releaseExclusive();

        if (stub != null) {
            stub.releaseExclusive();
        }
    }

    private void addParentFrames(TreeNode stub, TreeNode child, int pos) {
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

        db.readPage(id, page);

        mId = id;
        mCachedState = CACHED_CLEAN;

        byte type = page[0];
        if (type != TYPE_INTERNAL && type != TYPE_LEAF) {
            throw new CorruptTreeNodeException("Unknown type: " + type);
        }
        if (page[1] != 0) {
            throw new CorruptTreeNodeException("Illegal reserved byte: " + page[1]);
        }

        mType = type;
        mGarbage = DataIO.readUnsignedShort(page, 2);
        mLeftSegTail = DataIO.readUnsignedShort(page, 4);
        mRightSegTail = DataIO.readUnsignedShort(page, 6);
        mSearchVecStart = DataIO.readUnsignedShort(page, 8);
        mSearchVecEnd = DataIO.readUnsignedShort(page, 10);

        if (type == TYPE_INTERNAL) {
            // FIXME: recycle child node arrays
            mChildNodes = new TreeNode[numKeys() + 1];
        }
    }

    /**
     * Caller must hold any latch, which is not released, even if an exception is thrown.
     */
    void write(Database db) throws IOException {
        if (mSplit != null) {
            throw new AssertionError("Cannot write partially split node");
        }

        byte[] page = mPage;

        page[0] = mType;
        page[1] = 0; // reserved
        DataIO.writeShort(page, 2, mGarbage);
        DataIO.writeShort(page, 4, mLeftSegTail);
        DataIO.writeShort(page, 6, mRightSegTail);
        DataIO.writeShort(page, 8, mSearchVecStart);
        DataIO.writeShort(page, 10, mSearchVecEnd);

        db.writeReservedPage(mId, page);
    }

    /**
     * Caller must hold exclusive latch on node. Latch is never released by
     * this method, even if an exception is thrown.
     *
     * @return false if node cannot be evicted
     */
    boolean evict(Database db) throws IOException {
        if (!canEvict()) {
            return false;
        }

        int state = mCachedState;
        if (state != CACHED_CLEAN) {
            // TODO: Keep some sort of cache of ids known to be dirty. If
            // reloaded before commit, then they're still dirty. Without this
            // optimization, too many pages are allocated when: evictions are
            // high, write rate is high, and commits are bogged down. A Bloom
            // filter is not appropriate, because of false positives.

            write(db);
            mCachedState = CACHED_CLEAN;
        }

        mId = 0;
        // FIXME: child node array should be recycled
        mChildNodes = null;

        return true;
    }

    /**
     * Caller must hold any latch.
     */
    private boolean canEvict() {
        if (mLastCursorFrame != null || mSplit != null) {
            return false;
        }

        if (mId == STUB_ID) {
            return true;
        }

        TreeNode[] childNodes = mChildNodes;
        if (childNodes != null) {
            for (int i=0; i<childNodes.length; i++) {
                TreeNode child = mChildNodes[i];
                if (child != null) {
                    if (child.tryAcquireSharedUnfair()) {
                        long childId = retrieveChildRefIdFromIndex(i);
                        try {
                            if (childId == child.mId && mCachedState != CACHED_CLEAN) {
                                // Cannot evict if a child is dirty. It must be
                                // evicted first.
                                // FIXME: Retry evict with child instead.
                                return false;
                            }
                        } finally {
                            child.releaseShared();
                        }
                    } else {
                        // If latch cannot be acquired, assume child is still in
                        // use, and so this parent node should be kept.
                        return false;
                    }
                }
            }
        }

        return true;
    }

    /**
     * Caller must hold any latch.
     */
    boolean isLeaf() {
        return mType == TYPE_LEAF;
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
     * Caller must hold any latch.
     */
    int highestPos() {
        int pos = mSearchVecEnd - mSearchVecStart;
        if (!isLeaf()) {
            pos += 2;
        }
        return pos;
    }

    /**
     * Caller must hold any latch.
     */
    int highestLeafPos() {
        return mSearchVecEnd - mSearchVecStart;
    }

    /**
     * Caller must hold any latch.
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
     * Returns true if leaf is not split and underutilized. If so, it should be
     * merged with its neighbors, and possibly deleted. Caller must hold any latch.
     */
    boolean shouldInternalMerge() {
        return shouldMerge(availableInternalBytes());
    }

    boolean shouldMerge(int availBytes) {
        return mSplit == null && availBytes >= ((mPage.length - HEADER_SIZE) >> 1);
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
    // FIXME: Binary search can be optimized by not always starting key compare at first byte.
    int binarySearchLeaf(byte[] key) {
        final byte[] page = mPage;
        final int keyLen = key.length;
        int lowPos = mSearchVecStart;
        int highPos = mSearchVecEnd;

        outer: while (lowPos <= highPos) {
            int midPos = ((lowPos + highPos) >> 1) & ~1;

            int compareLoc = DataIO.readUnsignedShort(page, midPos);
            int compareLen = page[compareLoc++];
            compareLen = compareLen >= 0 ? ((compareLen & 0x3f) + 1)
                : (((compareLen & 0x3f) << 8) | ((page[compareLoc++]) & 0xff));

            int minLen = Math.min(compareLen, keyLen);
            for (int i=0; i<minLen; i++) {
                byte cb = page[compareLoc + i];
                byte kb = key[i];
                if (cb != kb) {
                    if ((cb & 0xff) < (kb & 0xff)) {
                        lowPos = midPos + 2;
                    } else {
                        highPos = midPos - 2;
                    }
                    continue outer;
                }
            }

            if (compareLen < keyLen) {
                lowPos = midPos + 2;
            } else if (compareLen > keyLen) {
                highPos = midPos - 2;
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
    // FIXME: Binary search can be optimized by not always starting key compare at first byte.
    int binarySearchLeaf(byte[] key, int midPos) {
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

        while (true) {
            compare: {
                int compareLoc = DataIO.readUnsignedShort(page, midPos);
                int compareLen = page[compareLoc++];
                compareLen = compareLen >= 0 ? ((compareLen & 0x3f) + 1)
                    : (((compareLen & 0x3f) << 8) | ((page[compareLoc++]) & 0xff));

                int minLen = Math.min(compareLen, keyLen);
                for (int i=0; i<minLen; i++) {
                    byte cb = page[compareLoc + i];
                    byte kb = key[i];
                    if (cb != kb) {
                        if ((cb & 0xff) < (kb & 0xff)) {
                            lowPos = midPos + 2;
                        } else {
                            highPos = midPos - 2;
                        }
                        break compare;
                    }
                }

                if (compareLen < keyLen) {
                    lowPos = midPos + 2;
                } else if (compareLen > keyLen) {
                    highPos = midPos - 2;
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
     * @return negative if page entry is less, zero if equal, more than zero if greater
     * /
    static int compareToInternalKey(byte[] page, int entryLoc, byte[] key) {
        int entryLen = page[entryLoc++];
        entryLen = entryLen >= 0 ? (entryLen + 1)
            : (((entryLen & 0x7f) << 8) | ((page[entryLoc++]) & 0xff));

        return Utils.compareKeys(page, entryLoc, entryLen, key, 0, key.length);
    }
    */

    /**
     * @return 2-based insertion pos, which is negative if key not found
     */
    // FIXME: Binary search can be optimized by not always starting key compare at first byte.
    int binarySearchInternal(byte[] key) {
        final byte[] page = mPage;
        final int keyLen = key.length;
        int lowPos = mSearchVecStart;
        int highPos = mSearchVecEnd;

        outer: while (lowPos <= highPos) {
            int midPos = ((lowPos + highPos) >> 1) & ~1;

            int compareLoc = DataIO.readUnsignedShort(page, midPos);
            int compareLen = page[compareLoc++];
            compareLen = compareLen >= 0 ? (compareLen + 1)
                : (((compareLen & 0x7f) << 8) | ((page[compareLoc++]) & 0xff));

            int minLen = Math.min(compareLen, keyLen);
            for (int i=0; i<minLen; i++) {
                byte cb = page[compareLoc + i];
                byte kb = key[i];
                if (cb != kb) {
                    if ((cb & 0xff) < (kb & 0xff)) {
                        lowPos = midPos + 2;
                    } else {
                        highPos = midPos - 2;
                    }
                    continue outer;
                }
            }

            if (compareLen < keyLen) {
                lowPos = midPos + 2;
            } else if (compareLen > keyLen) {
                highPos = midPos - 2;
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
    // FIXME: Binary search can be optimized by not always starting key compare at first byte.
    int binarySearchInternal(byte[] key, int midPos) {
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

        while (true) {
            compare: {
                int compareLoc = DataIO.readUnsignedShort(page, midPos);
                int compareLen = page[compareLoc++];
                compareLen = compareLen >= 0 ? (compareLen + 1)
                    : (((compareLen & 0x7f) << 8) | ((page[compareLoc++]) & 0xff));

                int minLen = Math.min(compareLen, keyLen);
                for (int i=0; i<minLen; i++) {
                    byte cb = page[compareLoc + i];
                    byte kb = key[i];
                    if (cb != kb) {
                        if ((cb & 0xff) < (kb & 0xff)) {
                            lowPos = midPos + 2;
                        } else {
                            highPos = midPos - 2;
                        }
                        break compare;
                    }
                }

                if (compareLen < keyLen) {
                    lowPos = midPos + 2;
                } else if (compareLen > keyLen) {
                    highPos = midPos - 2;
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

    private byte[] retrieveFirstLeafKey() {
        final byte[] page = mPage;

        int loc = DataIO.readUnsignedShort(page, mSearchVecStart);
        int keyLen = page[loc++];
        keyLen = keyLen >= 0 ? ((keyLen & 0x3f) + 1)
            : (((keyLen & 0x3f) << 8) | ((page[loc++]) & 0xff));
        byte[] key = new byte[keyLen];
        System.arraycopy(page, loc, key, 0, keyLen);

        return key;
    }

    /**
     * @param pos position as provided by binarySearchLeaf; must be positive
     */
    byte[] retrieveLeafKey(int pos) {
        final byte[] page = mPage;

        int loc = DataIO.readUnsignedShort(page, mSearchVecStart + pos);
        int header = page[loc++];
        int keyLen = header >= 0 ? ((header & 0x3f) + 1)
            : (((header & 0x3f) << 8) | ((page[loc++]) & 0xff));
        byte[] key = new byte[keyLen];
        System.arraycopy(page, loc, key, 0, keyLen);

        return key;
    }

    /**
     * @param pos position as provided by binarySearchLeaf; must be positive
     */
    byte[] retrieveLeafValue(int pos) {
        final byte[] page = mPage;

        int loc = DataIO.readUnsignedShort(page, mSearchVecStart + pos);
        int header = page[loc++];
        if ((header & 0x40) != 0) {
            return Utils.EMPTY_BYTES;
        }
        loc += (header >= 0 ? header : (((header & 0x3f) << 8) | (page[loc] & 0xff))) + 1;
        int len = page[loc++];
        len = len >= 0 ? (len + 1) : ((((len & 0x7f) << 8) | (page[loc++] & 0xff)) + 129);
        byte[] value = new byte[len];
        System.arraycopy(page, loc, value, 0, len);

        return value;
    }

    /**
     * @param pos position as provided by binarySearchLeaf; must be positive
     * @param value non-null value to compare to
     */
    /*
    boolean equalsLeafValue(int pos, byte[] value) {
        final byte[] page = mPage;

        int loc = DataIO.readUnsignedShort(page, mSearchVecStart + pos);
        int header = page[loc++];
        if ((header & 0x40) != 0) {
            return value.length == 0;
        }
        loc += (header >= 0 ? header : (((header & 0x3f) << 8) | (page[loc] & 0xff))) + 1;
        int len = page[loc++];
        len = len >= 0 ? (len + 1) : ((((len & 0x7f) << 8) | (page[loc++] & 0xff)) + 129);

        return Utils.compareKeys(page, loc, len, value, 0, value.length) == 0;
    }
    */

    /**
     * @param pos position as provided by binarySearchLeaf; must be positive
     */
    void retrieveLeafEntry(int pos, TreeCursor cursor) {
        final byte[] page = mPage;

        int loc = DataIO.readUnsignedShort(page, mSearchVecStart + pos);
        int header = page[loc++];
        int keyLen = header >= 0 ? ((header & 0x3f) + 1)
            : (((header & 0x3f) << 8) | ((page[loc++]) & 0xff));
        byte[] key = new byte[keyLen];
        System.arraycopy(page, loc, key, 0, keyLen);
        cursor.mKey = key;

        loc += keyLen;
        byte[] value;
        if ((header & 0x40) != 0) {
            value = Utils.EMPTY_BYTES;
        } else {
            int len = page[loc++];
            len = len >= 0 ? (len + 1) : ((((len & 0x7f) << 8) | (page[loc++] & 0xff)) + 129);
            value = new byte[len];
            System.arraycopy(page, loc, value, 0, len);
        }
        cursor.mValue = value;
    }

    /**
     * @param pos position as provided by binarySearchInternal; must be positive
     */
    long retrieveChildRefId(int pos) {
        return DataIO.readLong(mPage, mSearchVecEnd + 2 + (pos << 2));
    }

    /**
     * @param index index in child node array
     */
    long retrieveChildRefIdFromIndex(int index) {
        return DataIO.readLong(mPage, mSearchVecEnd + 2 + (index << 3));
    }

    /**
     * @param pos position as provided by binarySearchInternal
     */
    byte[] retrieveInternalKey(int pos) {
        byte[] page = mPage;
        return retrieveInternalKeyAtLocation
            (page, DataIO.readUnsignedShort(page, mSearchVecStart + pos));
    }

    /**
     * @param loc absolute location of internal entry
     */
    static byte[] retrieveInternalKeyAtLocation(final byte[] page, int loc) {
        int header = page[loc++];
        int keyLen = header >= 0 ? ((header & 0x7f) + 1)
            : (((header & 0x7f) << 8) | ((page[loc++]) & 0xff));
        byte[] key = new byte[keyLen];
        System.arraycopy(page, loc, key, 0, keyLen);
        return key;
    }

    /**
     * @return length of encoded entry at given location
     */
    static int leafEntryLength(byte[] page, final int entryLoc) {
        int loc = entryLoc;
        int header = page[loc++];
        loc += (header >= 0 ? (header & 0x3f) : (((header & 0x3f) << 8) | (page[loc] & 0xff))) + 1;
        if ((header & 0x40) == 0) {
            int len = page[loc++];
            loc += len >= 0 ? (len + 1) : ((((len & 0x7f) << 8) | (page[loc] & 0xff)) + 130);
        }
        return loc - entryLoc;
    }

    /**
     * @return length of encoded entry at given location
     */
    static int internalEntryLength(byte[] page, final int entryLoc) {
        int header = page[entryLoc];
        return (header >= 0 ? (header & 0x7f)
                : (((header & 0x7f) << 8) | (page[entryLoc + 1] & 0xff))) + 2;
    }

    /**
     * @param pos compliment of position as provided by binarySearchLeaf; must be positive
     */
    void insertLeafEntry(Database db, int pos, byte[] key, byte[] value)
        throws IOException
    {
        int encodedLen = calculateEncodedLength(key, value);
        int entryLoc = createLeafEntry(db, pos, encodedLen);
        if (entryLoc < 0) {
            splitLeafAndCreateEntry(db, key, value, encodedLen, pos, true);
        } else {
            copyToLeafEntry(key, value, entryLoc);
        }
    }

    /**
     * @param pos compliment of position as provided by binarySearchLeaf; must be positive
     * @return location for newly allocated entry, already pointed to by search
     * vector, or -1 if leaf must be split
     */
    private int createLeafEntry(Database db, int pos, final int encodedLen)
        throws InterruptedIOException
    {
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
                    System.arraycopy(page, searchVecStart, page, searchVecStart -= 2, pos);
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
                    System.arraycopy(page, pos, page, pos + 2, (searchVecEnd += 2) - pos);
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
                return (mGarbage + remaining) < 0 ? -1 : compactLeaf(db, encodedLen, pos, true);
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
                return compactLeaf(db, encodedLen, pos, true);
            }

            Utils.arrayCopies(page,
                              searchVecStart, newSearchVecStart, pos,
                              searchVecStart + pos, newSearchVecStart + pos + 2, vecLen - pos);

            pos += newSearchVecStart;
            mSearchVecStart = newSearchVecStart;
            mSearchVecEnd = newSearchVecStart + vecLen;
        }

        // Write pointer to new allocation.
        DataIO.writeShort(page, pos, entryLoc);
        return entryLoc;
    }

    /**
     * Insert into an internal node following a child node split. This parent node and
     * child node must have an exclusive latch held. Child latch is released.
     *
     * @param keyPos position to insert split key
     * @param splitChild child node which split
     */
    void insertSplitChildRef(Database db, int keyPos, TreeNode splitChild)
        throws IOException
    {
        if (db.shouldMarkDirty(splitChild)) {
            // It should be dirty as a result of the split itself.
            throw new AssertionError("Split child is not already marked dirty");
        }

        final Split split = splitChild.mSplit;
        final TreeNode newChild = splitChild.rebindSplitFrames(db, split);
        splitChild.mSplit = null;

        //final TreeNode leftChild;
        final TreeNode rightChild;
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
            // FIXME: recycle child node arrays
            TreeNode[] newChildNodes = new TreeNode[mChildNodes.length + 1];
            System.arraycopy(mChildNodes, 0, newChildNodes, 0, newChildPos);
            System.arraycopy(mChildNodes, newChildPos, newChildNodes, newChildPos + 1,
                             mChildNodes.length - newChildPos);
            newChildNodes[newChildPos] = newChild;
            mChildNodes = newChildNodes;

            // Rescale for long ids as encoded in page.
            newChildPos <<= 3;
        }

        InResult result = createInternalEntry
            (db, keyPos, split.splitKeyEncodedLength(), newChildPos, splitChild);

        // Write new child id.
        DataIO.writeLong(result.mPage, result.mNewChildLoc, newChild.mId);
        // Write key entry itself.
        split.copySplitKeyToParent(result.mPage, result.mEntryLoc);

        splitChild.releaseExclusive();
        newChild.releaseExclusive();
    }

    /**
     * Insert into an internal node following a child node split. This parent node and
     * child node must have an exclusive latch held. Child latch is released.
     *
     * @param keyPos 2-based position
     * @param newChildPos 8-based position
     * @param splitChild pass null if split not allowed
     * @return null if entry must be split, but no split is not allowed
     */
    private InResult createInternalEntry(Database db, int keyPos, int encodedLen,
                                         int newChildPos, TreeNode splitChild)
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
                    System.arraycopy(page, searchVecStart, page, searchVecStart - 10, keyPos);
                    System.arraycopy(page, searchVecStart + keyPos,
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
                    System.arraycopy(page, searchVecStart, page, searchVecStart -= 2, keyPos);
                    mSearchVecStart = searchVecStart;
                    keyPos += searchVecStart;
                    System.arraycopy(page, searchVecEnd + newChildPos + 2,
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
                    return compactInternal(db, encodedLen, keyPos, newChildPos);
                }

                // Node is full so split it.

                if (splitChild == null) {
                    // Caller doesn't allow split.
                    return null;
                }

                result = splitInternal(db, keyPos, splitChild, newChildPos, encodedLen);
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
                return compactInternal(db, encodedLen, keyPos, newChildPos);
            }

            int newSearchVecEnd = newSearchVecStart + vecLen;

            Utils.arrayCopies(page,
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
        DataIO.writeShort(page, keyPos, entryLoc);

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
    private TreeNode rebindSplitFrames(Database db, Split split) throws IOException {
        final TreeNode sibling = split.latchSibling(db);
        for (TreeCursorFrame frame = mLastCursorFrame; frame != null; ) {
            // Capture previous frame from linked list before changing the links.
            TreeCursorFrame prev = frame.mPrevCousin;
            split.rebindFrame(frame, sibling);
            frame = prev;
        }
        return sibling;
    }

    /**
     * @param pos position as provided by binarySearchLeaf; must be positive
     */
    void updateLeafValue(Database db, int pos, byte[] value) throws IOException {
        final byte[] page = mPage;
        final int valueLen = value.length;

        int searchVecStart = mSearchVecStart;

        final int start;
        final int keyLen;
        int loc;
        quick: {
            start = loc = DataIO.readUnsignedShort(page, searchVecStart + pos);
            final int header = page[loc++];

            if ((header & 0x40) != 0) {
                // Existing value is empty.
                if (valueLen == 0) {
                    // No change.
                    return;
                }
                // Old entry becomes garbage.
                loc += (header >= 0 ?
                        (header & 0x3f) : (((header & 0x3f) << 8) | (page[loc] & 0xff))) + 1;
                keyLen = loc - start;
                break quick;
            }

            loc += (header >= 0 ? header : (((header & 0x3f) << 8) | (page[loc] & 0xff))) + 1;
            int valueLoc = loc;
            int len = page[loc++];
            len = len >= 0 ? (len + 1) : ((((len & 0x7f) << 8) | (page[loc++] & 0xff)) + 129);

            if (valueLen > len) {
                // Old entry is too small, and so it becomes garbage.
                loc += len;
                keyLen = valueLoc - start;
                break quick;
            }

            if (valueLen == len) {
                // Copy new value with no garbage created.
                System.arraycopy(value, 0, page, loc, valueLen);
            } else {
                // Copy new value and remainder of old value becomes garbage.
                if (valueLen <= 128) {
                    if (valueLen == 0) {
                        page[start] |= 0x40;
                        mGarbage += loc + len - valueLoc;
                        return;
                    }
                    page[valueLoc++] = (byte) (valueLen - 1);
                } else {
                    page[valueLoc++] = (byte) (0x80 | ((valueLen - 129) >> 8));
                    page[valueLoc++] = (byte) (valueLen - 129);
                }
                System.arraycopy(value, 0, page, valueLoc, valueLen);
                mGarbage += loc + len - valueLoc - valueLen;
            }

            return;
        }

        // Old entry is garbage.
        mGarbage += loc - start;

        // What follows is similar to createLeafEntry method, except the search
        // vector doesn't grow.

        int searchVecEnd = mSearchVecEnd;

        int leftSpace = searchVecStart - mLeftSegTail;
        int rightSpace = mRightSegTail - searchVecEnd - 1;

        final int encodedLen = keyLen + (valueLen == 0 ? 0 : (valueLen <= 128 ? 1 : 2)) + valueLen;

        int entryLoc;
        alloc: {
            if ((entryLoc = allocPageEntry(encodedLen, leftSpace, rightSpace)) >= 0) {
                pos += searchVecStart;
                break alloc;
            }

            // Compute remaining space surrounding search vector after update completes.
            int remaining = leftSpace + rightSpace - encodedLen;

            if (mGarbage > remaining) {
                // Do full compaction and free up the garbage, or split the node.
                byte[] key = retrieveLeafKey(pos);
                if ((mGarbage + remaining) >= 0) {
                    copyToLeafEntry(key, value, compactLeaf(db, encodedLen, pos, false));
                } else {
                    // Node is full so split it.
                    splitLeafAndCreateEntry(db, key, value, encodedLen, pos, false);
                }
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
                byte[] key = retrieveLeafKey(pos);
                copyToLeafEntry(key, value, compactLeaf(db, encodedLen, pos, false));
                return;
            }

            System.arraycopy(page, searchVecStart, page, newSearchVecStart, vecLen);

            pos += newSearchVecStart;
            mSearchVecStart = newSearchVecStart;
            mSearchVecEnd = newSearchVecStart + vecLen - 2;
        }

        updateLeafEntry(page, start, keyLen, value, entryLoc);
        DataIO.writeShort(page, pos, entryLoc);
    }

    /**
     * @param keyLen includes length of header
     * @param value length must not be zero
     */
    private void updateLeafEntry(byte[] keySource, int keyStart, int keyLen,
                                 byte[] value, int entryLoc)
    {
        final byte[] page = mPage;

        // Copy existing key and indicate that value is non-empty.
        System.arraycopy(keySource, keyStart, page, entryLoc, keyLen);
        page[entryLoc] &= ~0x40;
        entryLoc += keyLen;

        final int valueLen = value.length;
        if (valueLen <= 128) {
            page[entryLoc++] = (byte) (valueLen - 1);
        } else {
            page[entryLoc++] = (byte) (0x80 | ((valueLen - 129) >> 8));
            page[entryLoc++] = (byte) (valueLen - 129);
        }
        System.arraycopy(value, 0, page, entryLoc, valueLen);
    }

    /**
     * @param pos position as provided by binarySearchInternal; must be positive
     */
    void updateChildRefId(int pos, long id) {
        DataIO.writeLong(mPage, mSearchVecEnd + 2 + (pos << 2), id);
    }

    /**
     * @param pos position as provided by binarySearchLeaf; must be positive
     */
    void deleteLeafEntry(int pos) {
        final byte[] page = mPage;

        int searchVecStart = mSearchVecStart;
        int entryLoc = DataIO.readUnsignedShort(page, searchVecStart + pos);
        // Increment garbage by the size of the encoded entry.
        mGarbage += leafEntryLength(page, entryLoc);

        int searchVecEnd = mSearchVecEnd;

        if (pos < ((searchVecEnd - searchVecStart + 2) >> 1)) {
            // Shift left side of search vector to the right.
            System.arraycopy(page, searchVecStart, page, searchVecStart += 2, pos);
            mSearchVecStart = searchVecStart;
        } else {
            // Shift right side of search vector to the left.
            pos += searchVecStart;
            System.arraycopy(page, pos + 2, page, pos, searchVecEnd - pos);
            mSearchVecEnd = searchVecEnd - 2;
        }
    }

    /**
     * Copies all the entries from this node, inserts them into the tail of the
     * given left node, and then deletes this node. Caller must ensure that
     * left node has enough room, and that both nodes are latched exclusively.
     * Caller must also hold commit lock. No latches are released by this method.
     */
    void transferLeafToLeftAndDelete(Database db, TreeNode leftNode)
        throws IOException
    {
        db.prepareToDelete(this);

        final byte[] rightPage = mPage;
        final int searchVecEnd = mSearchVecEnd;
        final int leftEndPos = leftNode.highestLeafPos() + 2;

        int searchVecStart = mSearchVecStart;
        while (searchVecStart <= searchVecEnd) {
            int entryLoc = DataIO.readUnsignedShort(rightPage, searchVecStart);
            int encodedLen = leafEntryLength(rightPage, entryLoc);
            int leftEntryLoc = leftNode.createLeafEntry
                (db, leftNode.highestLeafPos() + 2, encodedLen);
            // Note: Must access left page each time, since compaction can replace it.
            System.arraycopy(rightPage, entryLoc, leftNode.mPage, leftEntryLoc, encodedLen);
            searchVecStart += 2;
        }

        // All cursors in this node must be moved to left node.
        for (TreeCursorFrame frame = mLastCursorFrame; frame != null; ) {
            // Capture previous frame from linked list before changing the links.
            TreeCursorFrame prev = frame.mPrevCousin;
            int framePos = frame.mNodePos;
            frame.unbind();
            frame.bind(leftNode, framePos + (framePos < 0 ? (-leftEndPos) : leftEndPos));
            frame = prev;
        }

        db.deleteNode(this);
    }

    /**
     * Copies all the entries from this node, inserts them into the tail of the
     * given left node, and then deletes this node. Caller must ensure that
     * left node has enough room, and that both nodes are latched exclusively.
     * Caller must also hold commit lock. No latches are released by this method.
     *
     * @param parentPage source of entry to merge from parent
     * @param parentLoc location of parent entry
     * @param parentLen length of parent entry
     */
    void transferInternalToLeftAndDelete(Database db, TreeNode leftNode,
                                         byte[] parentPage, int parentLoc, int parentLen)
        throws IOException
    {
        db.prepareToDelete(this);

        // Create space to absorb parent key.
        int leftEndPos = leftNode.highestInternalPos();
        InResult result = leftNode.createInternalEntry
            (db, leftEndPos, parentLen, (leftEndPos += 2) << 2, null);

        // Copy child id associated with parent key.
        final byte[] rightPage = mPage;
        int rightChildIdsLoc = mSearchVecEnd + 2;
        System.arraycopy(rightPage, rightChildIdsLoc, result.mPage, result.mNewChildLoc, 8);
        rightChildIdsLoc += 8;

        // Write parent key.
        System.arraycopy(parentPage, parentLoc, result.mPage, result.mEntryLoc, parentLen);

        final int searchVecEnd = mSearchVecEnd;

        int searchVecStart = mSearchVecStart;
        while (searchVecStart <= searchVecEnd) {
            int entryLoc = DataIO.readUnsignedShort(rightPage, searchVecStart);
            int encodedLen = internalEntryLength(rightPage, entryLoc);

            // Allocate entry for left node.
            int pos = leftNode.highestInternalPos();
            result = leftNode.createInternalEntry(db, pos, encodedLen, (pos + 2) << 2, null);

            // Copy child id.
            System.arraycopy(rightPage, rightChildIdsLoc, result.mPage, result.mNewChildLoc, 8);
            rightChildIdsLoc += 8;

            // Copy key.
            // Note: Must access left page each time, since compaction can replace it.
            System.arraycopy(rightPage, entryLoc, result.mPage, result.mEntryLoc, encodedLen);
            searchVecStart += 2;
        }

        // FIXME: recycle child node arrays
        int leftLen = leftNode.mChildNodes.length;
        TreeNode[] newChildNodes = new TreeNode[leftLen + mChildNodes.length];
        System.arraycopy(leftNode.mChildNodes, 0, newChildNodes, 0, leftLen);
        System.arraycopy(mChildNodes, 0, newChildNodes, leftLen, mChildNodes.length);
        leftNode.mChildNodes = newChildNodes;

        // All cursors in this node must be moved to left node.
        for (TreeCursorFrame frame = mLastCursorFrame; frame != null; ) {
            // Capture previous frame from linked list before changing the links.
            TreeCursorFrame prev = frame.mPrevCousin;
            int framePos = frame.mNodePos;
            frame.unbind();
            frame.bind(leftNode, leftEndPos + framePos);
            frame = prev;
        }

        db.deleteNode(this);
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

        int entryLoc = DataIO.readUnsignedShort(page, searchVecStart + keyPos);
        // Increment garbage by the size of the encoded entry.
        mGarbage += internalEntryLength(page, entryLoc);

        // Update references to child node instances.
        // FIXME: recycle child node arrays
        childPos >>= 1;
        TreeNode[] newChildNodes = new TreeNode[mChildNodes.length - 1];
        System.arraycopy(mChildNodes, 0, newChildNodes, 0, childPos);
        System.arraycopy(mChildNodes, childPos + 1, newChildNodes, childPos,
                         newChildNodes.length - childPos);
        mChildNodes = newChildNodes;
        // Rescale for long ids as encoded in page.
        childPos <<= 3;

        int searchVecEnd = mSearchVecEnd;

        // Remove search vector entry (2 bytes) and remove child id entry
        // (8 bytes). Determine which shift operations minimize movement.
        if (childPos < (3 * (searchVecEnd - searchVecStart) + keyPos + 8) >> 1) {
            // Shift child ids right by 8, shift search vector right by 10.
            System.arraycopy(page, searchVecStart + keyPos + 2,
                             page, searchVecStart + keyPos + (2 + 8),
                             searchVecEnd - searchVecStart - keyPos + childPos);
            System.arraycopy(page, searchVecStart, page, searchVecStart += 10, keyPos);
            mSearchVecEnd = searchVecEnd + 8;
        } else {
            // Shift child ids left by 8, shift search vector right by 2.
            System.arraycopy(page, searchVecEnd + childPos + (2 + 8),
                             page, searchVecEnd + childPos + 2,
                             ((searchVecEnd - searchVecStart) << 2) + 8 - childPos);
            System.arraycopy(page, searchVecStart, page, searchVecStart += 2, keyPos);
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
     * <p>Caller must hold exclusive latches for root node and lone child. No
     * latches are released by this method.
     */
    void rootDelete(Tree tree) throws IOException {
        byte[] page = mPage;
        TreeNode[] childNodes = mChildNodes;
        TreeCursorFrame lastCursorFrame = mLastCursorFrame;

        TreeNode child = childNodes[0];
        tree.mDatabase.prepareToDelete(child);
        long toDelete = child.mId;
        int toDeleteState = child.mCachedState;

        mPage = child.mPage;
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
        child.mType = TYPE_INTERNAL;
        child.clearEntries();
        child.mChildNodes = childNodes;
        child.mLastCursorFrame = lastCursorFrame;
        // Lone child of stub root points to actual root.
        childNodes[0] = this;

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
     * Calculate encoded key length for leaf.
     */
    private static int calculateEncodedLength(byte[] key, byte[] value) {
        int keyLen = key.length;
        int valueLen = value.length;
        return ((keyLen <= 64 & keyLen > 0) ? 1 : 2) + keyLen
            + (valueLen == 0 ? 0 : (valueLen <= 128 ? 1 : 2)) + valueLen;
    }

    /**
     * Calculate encoded key length for internal node.
     */
    private static int calculateEncodedLength(byte[] key) {
        int keyLen = key.length;
        return ((keyLen <= 128 & keyLen > 0) ? 1 : 2) + keyLen;
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

    private void copyToLeafEntry(byte[] key, byte[] value, int entryLoc) {
        final byte[] page = mPage;
        final int keyLen = key.length;
        final int valueLen = value.length;

        if (valueLen == 0) {
            if (keyLen <= 64 && keyLen > 0) {
                page[entryLoc++] = (byte) (0x40 | (keyLen - 1));
            } else {
                page[entryLoc++] = (byte) (0xc0 | (keyLen >> 8));
                page[entryLoc++] = (byte) keyLen;
            }
            System.arraycopy(key, 0, page, entryLoc, keyLen);
        } else {
            if (keyLen <= 64 && keyLen > 0) {
                page[entryLoc++] = (byte) (keyLen - 1);
            } else {
                page[entryLoc++] = (byte) (0x80 | (keyLen >> 8));
                page[entryLoc++] = (byte) keyLen;
            }
            System.arraycopy(key, 0, page, entryLoc, keyLen);
            entryLoc += keyLen;

            if (valueLen <= 128) {
                page[entryLoc++] = (byte) (valueLen - 1);
            } else {
                page[entryLoc++] = (byte) (0x80 | ((valueLen - 129) >> 8));
                page[entryLoc++] = (byte) (valueLen - 129);
            }
            System.arraycopy(value, 0, page, entryLoc, valueLen);
        }
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
    private int compactLeaf(Database db, int encodedLen, int pos, boolean forInsert)
        throws InterruptedIOException
    {
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

        int destLoc = HEADER_SIZE;
        int newSearchVecLoc = newSearchVecStart;
        int newLoc = 0;
        final int searchVecEnd = mSearchVecEnd;

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
            DataIO.writeShort(dest, newSearchVecLoc, destLoc);
            int sourceLoc = DataIO.readUnsignedShort(page, searchVecLoc);
            int len = leafEntryLength(page, sourceLoc);
            System.arraycopy(page, sourceLoc, dest, destLoc, len);
            destLoc += len;
        }

        // Recycle old page buffer.
        db.addSpareBuffer(page);

        // Write pointer to new allocation.
        DataIO.writeShort(dest, newLoc == 0 ? newSearchVecLoc : newLoc, destLoc);

        mPage = dest;
        mGarbage = 0;
        mLeftSegTail = destLoc + encodedLen;
        mRightSegTail = dest.length - 1;
        mSearchVecStart = newSearchVecStart;
        mSearchVecEnd = newSearchVecStart + newSearchVecSize - 2;

        return destLoc;
    }

    /**
     *
     * @param encodedLen length of new entry to allocate
     * @param pos normalized search vector position of entry to insert/update
     */
    private void splitLeafAndCreateEntry(Database db, byte[] key, byte[] value,
                                         int encodedLen, int pos, boolean forInsert)
        throws IOException
    {
        if (mSplit != null) {
            throw new AssertionError("TreeNode is already split");
        }

        // Split can move node entries to a new left or right node. Choose such that the
        // new entry is more likely to go into the new node. This distributes the cost of
        // the split by postponing compaction of this node.

        // Since the split key and final node sizes are not known in advance, don't
        // attempt to properly center the new search vector. Instead, minimize
        // fragmentation to ensure that split is successful.

        byte[] page = mPage;

        TreeNode newNode = db.newNodeForSplit();
        newNode.mType = TYPE_LEAF;
        newNode.mGarbage = 0;

        byte[] newPage = newNode.mPage;

        final int searchVecStart = mSearchVecStart;
        final int searchVecEnd = mSearchVecEnd;
        pos += searchVecStart;

        // Amount of bytes used in unsplit node, including the page header.
        int size = searchVecEnd - searchVecStart + 1
            + mLeftSegTail + page.length - mRightSegTail - mGarbage;

        int garbageAccum = 0;
        int newLoc = 0;

        // Guess which way to split by examining search position. This doesn't take into
        // consideration the variable size of the entries. If the guess is wrong, the new
        // entry is inserted into original node, which now has space.

        Split split;
        if ((pos - searchVecStart) < (searchVecEnd - pos)) {
            // Split into new left node.

            int destLoc = newPage.length;
            int newSearchVecLoc = HEADER_SIZE;
            int newSize = HEADER_SIZE;

            int searchVecLoc = searchVecStart;
            for (; newSize < size; searchVecLoc += 2, newSearchVecLoc += 2) {
                int entryLoc = DataIO.readUnsignedShort(page, searchVecLoc);
                int entryLen = leafEntryLength(page, entryLoc);

                if (searchVecLoc == pos) {
                    newLoc = newSearchVecLoc;
                    if (forInsert) {
                        // Reserve spot in vector for new entry and account for size increase.
                        newSearchVecLoc += 2;
                        newSize += encodedLen + 2;
                    } else {
                        // Don't copy entry to update, but account for size change.
                        garbageAccum += entryLen;
                        size -= entryLen;
                        newSize += encodedLen;
                        continue;
                    }
                }

                // Copy entry and point to it.
                destLoc -= entryLen;
                System.arraycopy(page, entryLoc, newPage, destLoc, entryLen);
                DataIO.writeShort(newPage, newSearchVecLoc, destLoc);

                garbageAccum += entryLen;
                size -= entryLen + 2;
                newSize += entryLen + 2;
            }

            // Prune off the left end of this node.
            mSearchVecStart = searchVecLoc;

            if (newLoc == 0) {
                // Unable to insert new entry into left node. Insert it into the right
                // node, which should have space now.
                pos = binarySearchLeaf(key);
                if (forInsert) {
                    if (pos >= 0) {
                        throw new AssertionError("Key exists");
                    }
                    copyToLeafEntry(key, value, createLeafEntry(db, ~pos, encodedLen));
                } else {
                    if (pos < 0) {
                        throw new AssertionError("Key not found");
                    }
                    updateLeafValue(db, pos, value);
                }
            } else {
                // Create new entry and point to it.
                destLoc -= encodedLen;
                newNode.copyToLeafEntry(key, value, destLoc);
                DataIO.writeShort(newPage, newLoc, destLoc);
            }

            newNode.mLeftSegTail = HEADER_SIZE;
            newNode.mRightSegTail = destLoc - 1;
            newNode.mSearchVecStart = HEADER_SIZE;
            newNode.mSearchVecEnd = newSearchVecLoc - 2;

            // Split key is copied from this, the right node.
            split = new Split(false, newNode, retrieveFirstLeafKey());
        } else {
            // Split into new right node.

            int destLoc = HEADER_SIZE;
            int newSearchVecLoc = newPage.length;
            int newSize = HEADER_SIZE;

            int searchVecLoc = searchVecEnd;
            for (; newSize < size; searchVecLoc -= 2) {
                newSearchVecLoc -= 2;

                int entryLoc = DataIO.readUnsignedShort(page, searchVecLoc);
                int entryLen = leafEntryLength(page, entryLoc);

                if (forInsert) {
                    if (searchVecLoc + 2 == pos) {
                        newLoc = newSearchVecLoc;
                        // Reserve spot in vector for new entry and account for size increase.
                        newSearchVecLoc -= 2;
                        newSize += encodedLen + 2;
                    }
                } else {
                    if (searchVecLoc == pos) {
                        newLoc = newSearchVecLoc;
                        // Don't copy entry to update, but account for size change.
                        garbageAccum += entryLen;
                        size -= entryLen;
                        newSize += encodedLen;
                        continue;
                    }
                }

                // Copy entry and point to it.
                System.arraycopy(page, entryLoc, newPage, destLoc, entryLen);
                DataIO.writeShort(newPage, newSearchVecLoc, destLoc);
                destLoc += entryLen;

                garbageAccum += entryLen;
                size -= entryLen + 2;
                newSize += entryLen + 2;
            }

            // Prune off the right end of this node.
            mSearchVecEnd = searchVecLoc;

            if (newLoc == 0) {
                // Unable to insert new entry into new right node. Insert it into the left
                // node, which should have space now.
                pos = binarySearchLeaf(key);
                if (forInsert) {
                    if (pos >= 0) {
                        throw new AssertionError("Key exists");
                    }
                    copyToLeafEntry(key, value, createLeafEntry(db, ~pos, encodedLen));
                } else {
                    if (pos < 0) {
                        throw new AssertionError("Key not found");
                    }
                    updateLeafValue(db, pos, value);
                }
            } else {
                // Create new entry and point to it.
                newNode.copyToLeafEntry(key, value, destLoc);
                DataIO.writeShort(newPage, newLoc, destLoc);
                destLoc += encodedLen;
            }

            newNode.mLeftSegTail = destLoc;
            newNode.mRightSegTail = newPage.length - 1;
            newNode.mSearchVecStart = newSearchVecLoc;
            newNode.mSearchVecEnd = newPage.length - 2;

            // Split key is copied from the new right node.
            split = new Split(true, newNode, newNode.retrieveFirstLeafKey());
        }

        mGarbage += garbageAccum;
        mSplit = split;
    }

    private InResult splitInternal
        (final Database db, final int keyPos,
         final TreeNode splitChild,
         final int newChildPos, final int encodedLen)
        throws IOException
    {
        if (mSplit != null) {
            throw new AssertionError("TreeNode is already split");
        }

        // Split can move node entries to a new left or right node. Choose such that the
        // new entry is more likely to go into the new node. This distributes the cost of
        // the split by postponing compaction of this node.

        final byte[] page = mPage;

        final TreeNode newNode = db.newNodeForSplit();
        newNode.mType = TYPE_INTERNAL;
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

            int newSize = HEADER_SIZE;

            // Adjust sizes for extra child id -- always one more than number of keys.
            size -= 8;
            newSize += 8;

            if (splitSide < 0) {
                // Split into new left node.

                // Since the split key and final node sizes are not known in advance,
                // don't attempt to properly center the new search vector. Instead,
                // minimize fragmentation to ensure that split is successful.

                int destLoc = newPage.length;
                int newSearchVecLoc = HEADER_SIZE;

                int searchVecLoc = searchVecStart;
                while (true) {
                    if (searchVecLoc == keyLoc) {
                        newKeyLoc = newSearchVecLoc;
                        newSearchVecLoc += 2;
                        // Reserve spot in vector for new entry and account for size increase.
                        newSize += encodedLen + (2 + 8);
                    }

                    int entryLoc = DataIO.readUnsignedShort(page, searchVecLoc);
                    int entryLen = internalEntryLength(page, entryLoc);

                    searchVecLoc += 2;

                    // Size change must incorporate child id, although they are copied later.
                    int sizeChange = entryLen + (2 + 8);
                    size -= sizeChange;

                    garbageAccum += entryLen;

                    if ((newSize += sizeChange) > size) {
                        // New node has accumlated enough entries and split key has been found.

                        if (newKeyLoc != 0) {
                            split = new Split(false, newNode,
                                              retrieveInternalKeyAtLocation(page, entryLoc));
                            break;
                        }

                        if (splitSide == -1) {
                            // Guessed wrong; do over on right side.
                            splitSide = 2;
                            continue doSplit;
                        }

                        // Keep searching on this side for new entry location.
                        assert splitSide == -2;
                    }

                    // Copy key entry and point to it.
                    destLoc -= entryLen;
                    System.arraycopy(page, entryLoc, newPage, destLoc, entryLen);
                    DataIO.writeShort(newPage, newSearchVecLoc, destLoc);
                    newSearchVecLoc += 2;
                }

                result.mEntryLoc = destLoc - encodedLen;

                // Copy existing child ids and insert new child id.
                {
                    int newDestLoc = newSearchVecLoc;
                    System.arraycopy(page, searchVecEnd + 2,
                                     newPage, newSearchVecLoc, newChildPos);

                    // Leave gap for new child id, to be set by caller.
                    result.mNewChildLoc = newSearchVecLoc + newChildPos;

                    int tailChildIdsLen = ((searchVecLoc - searchVecStart) << 2) - newChildPos;
                    System.arraycopy(page, searchVecEnd + 2 + newChildPos,
                                     newPage, newSearchVecLoc + newChildPos + 8, tailChildIdsLen);

                    // Split references to child node instances. New child node has already
                    // been placed into mChildNodes by caller.
                    // FIXME: recycle child node arrays
                    int leftLen = ((newSearchVecLoc - HEADER_SIZE) >> 1) + 1;
                    TreeNode[] leftChildNodes = new TreeNode[leftLen];
                    TreeNode[] rightChildNodes = new TreeNode[mChildNodes.length - leftLen];
                    System.arraycopy(mChildNodes, 0, leftChildNodes, 0, leftLen);
                    System.arraycopy(mChildNodes, leftLen,
                                     rightChildNodes, 0, rightChildNodes.length);
                    newNode.mChildNodes = leftChildNodes;
                    mChildNodes = rightChildNodes;
                }

                newNode.mLeftSegTail = HEADER_SIZE;
                newNode.mRightSegTail = destLoc - encodedLen - 1;
                newNode.mSearchVecStart = HEADER_SIZE;
                newNode.mSearchVecEnd = newSearchVecLoc - 2;

                // Prune off the left end of this node by shifting vector towards child ids.
                int shift = (searchVecLoc - searchVecStart) << 2;
                int len = searchVecEnd - searchVecLoc + 2;
                System.arraycopy(page, searchVecLoc,
                                 page, mSearchVecStart = searchVecLoc + shift, len);
                mSearchVecEnd = searchVecEnd + shift;
            } else {
                // Split into new right node.

                // First copy keys and not the child ids. After keys are copied, shift to
                // make room for child ids and copy them in place.

                int destLoc = HEADER_SIZE;
                int newSearchVecLoc = newPage.length;

                int searchVecLoc = searchVecEnd + 2;
                while (true) {
                    if (searchVecLoc == keyLoc) {
                        newSearchVecLoc -= 2;
                        newKeyLoc = newSearchVecLoc;
                        // Reserve spot in vector for new entry and account for size increase.
                        newSize += encodedLen + (2 + 8);
                    }

                    searchVecLoc -= 2;

                    int entryLoc = DataIO.readUnsignedShort(page, searchVecLoc);
                    int entryLen = internalEntryLength(page, entryLoc);

                    // Size change must incorporate child id, although they are copied later.
                    int sizeChange = entryLen + (2 + 8);
                    size -= sizeChange;

                    garbageAccum += entryLen;

                    if ((newSize += sizeChange) > size) {
                        // New node has accumlated enough entries and split key has been found.

                        if (newKeyLoc != 0) {
                            split = new Split(true, newNode,
                                              retrieveInternalKeyAtLocation(page, entryLoc));
                            break;
                        }

                        if (splitSide == 1) {
                            // Guessed wrong; do over on left side.
                            splitSide = -2;
                            continue doSplit;
                        }

                        // Keep searching on this side for new entry location.
                        assert splitSide == 2;
                    }

                    // Copy key entry and point to it.
                    System.arraycopy(page, entryLoc, newPage, destLoc, entryLen);
                    newSearchVecLoc -= 2;
                    DataIO.writeShort(newPage, newSearchVecLoc, destLoc);
                    destLoc += entryLen;
                }

                result.mEntryLoc = destLoc;

                // Move new search vector to make room for child ids and be centered between
                // the segments.
                int newVecLen = page.length - newSearchVecLoc;
                {
                    int highestLoc = newPage.length - (5 * newVecLen) - 8;
                    int midLoc = ((destLoc + encodedLen + highestLoc + 1) >> 1) & ~1;
                    System.arraycopy(newPage, newSearchVecLoc, newPage, midLoc, newVecLen);
                    newKeyLoc -= newSearchVecLoc - midLoc;
                    newSearchVecLoc = midLoc;
                }

                int newSearchVecEnd = newSearchVecLoc + newVecLen - 2;

                // Copy existing child ids and insert new child id.
                {
                    int headChildIdsLen = newChildPos - ((searchVecLoc - searchVecStart + 2) << 2);
                    int newDestLoc = newSearchVecEnd + 2;
                    System.arraycopy(page, searchVecEnd + 2 + newChildPos - headChildIdsLen,
                                     newPage, newDestLoc, headChildIdsLen);

                    // Leave gap for new child id, to be set by caller.
                    newDestLoc += headChildIdsLen;
                    result.mNewChildLoc = newDestLoc;

                    int tailChildIdsLen =
                        ((searchVecEnd - searchVecStart) << 2) + 16 - newChildPos;
                    System.arraycopy(page, searchVecEnd + 2 + newChildPos,
                                     newPage, newDestLoc + 8, tailChildIdsLen);

                    // Split references to child node instances. New child node has already
                    // been placed into mChildNodes by caller.
                    // FIXME: recycle child node arrays
                    int rightLen = ((newSearchVecEnd - newSearchVecLoc) >> 1) + 2;
                    TreeNode[] rightChildNodes = new TreeNode[rightLen];
                    TreeNode[] leftChildNodes = new TreeNode[mChildNodes.length - rightLen];
                    System.arraycopy(mChildNodes, leftChildNodes.length,
                                     rightChildNodes, 0, rightLen);
                    System.arraycopy(mChildNodes, 0, leftChildNodes, 0, leftChildNodes.length);
                    newNode.mChildNodes = rightChildNodes;
                    mChildNodes = leftChildNodes;
                }

                newNode.mLeftSegTail = destLoc + encodedLen;
                newNode.mRightSegTail = newPage.length - 1;
                newNode.mSearchVecStart = newSearchVecLoc;
                newNode.mSearchVecEnd = newSearchVecEnd;

                // Prune off the right end of this node by shifting vector towards child ids.
                int len = searchVecLoc - searchVecStart;
                System.arraycopy(page, searchVecStart,
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
    private InResult compactInternal(Database db, int encodedLen, int keyPos, int childPos)
        throws InterruptedIOException
    {
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

        int destLoc = HEADER_SIZE;
        int newSearchVecLoc = newSearchVecStart;
        int newLoc = 0;
        final int searchVecEnd = mSearchVecEnd;

        byte[] dest = db.removeSpareBuffer();

        for (; searchVecLoc <= searchVecEnd; searchVecLoc += 2, newSearchVecLoc += 2) {
            if (searchVecLoc == keyPos) {
                newLoc = newSearchVecLoc;
                newSearchVecLoc += 2;
            }
            DataIO.writeShort(dest, newSearchVecLoc, destLoc);
            int sourceLoc = DataIO.readUnsignedShort(page, searchVecLoc);
            int len = internalEntryLength(page, sourceLoc);
            System.arraycopy(page, sourceLoc, dest, destLoc, len);
            destLoc += len;
        }

        if (newLoc == 0) {
            newLoc = newSearchVecLoc;
            newSearchVecLoc += 2;
        }

        // Copy child ids, and leave room for inserted child id.
        System.arraycopy(page, mSearchVecEnd + 2, dest, newSearchVecLoc, childPos);
        System.arraycopy(page, mSearchVecEnd + 2 + childPos,
                         dest, newSearchVecLoc + childPos + 8,
                         (newSearchVecSize << 2) - childPos);

        // Recycle old page buffer.
        db.addSpareBuffer(page);

        // Write pointer to key entry.
        DataIO.writeShort(dest, newLoc, destLoc);

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
    private static final class InResult {
        byte[] mPage;
        int mKeyLoc;
        int mNewChildLoc;
        int mEntryLoc;
    }

    /**
     * No latches are acquired by this method -- it is only used for debugging.
     */
    @Override
    public String toString() {
        return "TreeNode: {id=" + mId +
            ", isLeaf=" + isLeaf() +
            ", cachedState=" + mCachedState +
            ", canEvict=" + canEvict() +
            ", isSplit=" + (mSplit != null) +
            ", availableBytes=" + availableBytes() +
            ", lockState=" + super.toString() +
            '}';
    }

    /**
     * Verifies the integrity of this node.
     */
    public void verify() throws CorruptTreeNodeException {
        acquireSharedUnfair();
        try {
            verify0();
        } catch (IndexOutOfBoundsException e) {
            throw new CorruptTreeNodeException(e);
        } finally {
            releaseShared();
        }
    }

    /**
     * Caller must hold any latch.
     */
    void verify0() throws CorruptTreeNodeException {
        final byte[] page = mPage;

        if (mLeftSegTail < HEADER_SIZE) {
            throw new CorruptTreeNodeException("Left segment tail: " + mLeftSegTail);
        }
        if (mSearchVecStart < mLeftSegTail) {
            throw new CorruptTreeNodeException("Search vector start: " + mSearchVecStart);
        }
        if (mSearchVecEnd < (mSearchVecStart - 2)) {
            throw new CorruptTreeNodeException("Search vector end: " + mSearchVecEnd);
        }
        if (mRightSegTail < mSearchVecEnd || mRightSegTail > (page.length - 1)) {
            throw new CorruptTreeNodeException("Right segment tail: " + mRightSegTail);
        }

        if (!isLeaf()) {
            if (numKeys() + 1 != mChildNodes.length) {
                throw new CorruptTreeNodeException
                    ("Wrong number of child nodes: " +
                     (numKeys() + 1) + " != " + mChildNodes.length);
            }

            int childIdsStart = mSearchVecEnd + 2;
            int childIdsEnd = childIdsStart + ((childIdsStart - mSearchVecStart) << 2) + 8;
            if (childIdsEnd > (mRightSegTail + 1)) {
                throw new CorruptTreeNodeException("Child ids end: " + childIdsEnd);
            }

            LIHashTable childIds = new LIHashTable(9);

            for (int i = childIdsStart; i < childIdsEnd; i += 8) {
                long childId = DataIO.readLong(page, i);

                if (childId < 0 || childId == 0 || childId == 1) {
                    throw new CorruptTreeNodeException("Illegal child id: " + childId);
                }

                LIHashTable.Entry e = childIds.insert(childId);
                if (e.value != 0) {
                    throw new CorruptTreeNodeException("Duplicate child id: " + childId);
                }
                e.value = 1;
            }
        }

        int used = HEADER_SIZE + mRightSegTail + 1 - mLeftSegTail;

        int lastKeyLoc = 0;
        int lastKeyLen = 0;

        for (int i = mSearchVecStart; i <= mSearchVecEnd; i += 2) {
            int loc = DataIO.readUnsignedShort(page, i);

            if (loc < HEADER_SIZE || loc >= page.length ||
                (loc >= mLeftSegTail && loc <= mRightSegTail))
            {
                throw new CorruptTreeNodeException("Entry location: " + loc);
            }

            int keyLen;

            if (isLeaf()) {
                used += leafEntryLength(page, loc);

                keyLen = page[loc++];
                keyLen = keyLen >= 0 ? ((keyLen & 0x3f) + 1)
                    : (((keyLen & 0x3f) << 8) | ((page[loc++]) & 0xff));
            } else {
                used += internalEntryLength(page, loc);

                keyLen = page[loc++];
                keyLen = keyLen >= 0 ? (keyLen + 1)
                    : (((keyLen & 0x7f) << 8) | ((page[loc++]) & 0xff));
            }

            if (lastKeyLoc != 0) {
                int result = Utils.compareKeys(page, lastKeyLoc, lastKeyLen, page, loc, keyLen);
                if (result >= 0) {
                    throw new CorruptTreeNodeException("Key order: " + result);
                }
            }

            lastKeyLoc = loc;
            lastKeyLoc = keyLen;
        }

        int garbage = page.length - used;

        if (mGarbage != garbage) {
            throw new CorruptTreeNodeException("Garbage: " + mGarbage + " != " + garbage);
        }
    }

    /**
     * Counts all the enties in the tree rooted at this node. No latches are
     * acquired by this method -- it is only used for debugging.
     */
    long countEntries(Database db) throws IOException {
        if (isLeaf()) {
            return 1 + ((mSearchVecEnd - mSearchVecStart) >> 1);
        }

        TreeNode child = mChildNodes[mChildNodes.length - 1];
        long childId = retrieveChildRefIdFromIndex(mChildNodes.length - 1);

        if (child == null || childId != child.mId) {
            child = new TreeNode(db.pageSize(), false);
            child.read(db, childId);
        }

        long count = child.countEntries(db);

        for (int pos = mSearchVecEnd - mSearchVecStart; pos >= 0; pos -= 2) {
            child = mChildNodes[pos >> 1];
            childId = retrieveChildRefId(pos);

            if (child == null || childId != child.mId) {
                child = new TreeNode(db.pageSize(), false);
                child.read(db, childId);
            }

            count += child.countEntries(db);
        }

        return count;
    }

    /**
     * Counts all the pages used to store the tree rooted at this node. No
     * latches are acquired by this method -- it is only used for debugging.
     */
    long countPages(Database db) throws IOException {
        if (isLeaf()) {
            return 1;
        }

        TreeNode child = mChildNodes[mChildNodes.length - 1];
        long childId = retrieveChildRefIdFromIndex(mChildNodes.length - 1);

        if (child == null || childId != child.mId) {
            child = new TreeNode(db.pageSize(), false);
            child.read(db, childId);
        }

        long count = child.countPages(db);

        for (int pos = mSearchVecEnd - mSearchVecStart; pos >= 0; pos -= 2) {
            child = mChildNodes[pos >> 1];
            childId = retrieveChildRefId(pos);

            if (child == null || childId != child.mId) {
                child = new TreeNode(db.pageSize(), false);
                child.read(db, childId);
            }

            count += child.countPages(db);
        }

        return count + 1;
    }

    /**
     * Clears a bit for each page used to store the tree rooted at this node. No
     * latches are acquired by this method -- it is only used for debugging.
     */
    void tracePages(Database db, java.util.BitSet bits) throws IOException {
        if (mId == 0) {
            return;
        }

        if (!bits.get((int) mId)) {
            throw new CorruptTreeNodeException("Page already seen: " + mId);
        }
        bits.clear((int) mId);

        if (isLeaf()) {
            return;
        }

        TreeNode child = mChildNodes[mChildNodes.length - 1];
        long childId = retrieveChildRefIdFromIndex(mChildNodes.length - 1);

        if (child == null || childId != child.mId) {
            child = new TreeNode(db.pageSize(), false);
            child.read(db, childId);
        }

        child.tracePages(db, bits);

        for (int pos = mSearchVecEnd - mSearchVecStart; pos >= 0; pos -= 2) {
            child = mChildNodes[pos >> 1];
            childId = retrieveChildRefId(pos);

            if (child == null || childId != child.mId) {
                child = new TreeNode(db.pageSize(), false);
                child.read(db, childId);
            }

            child.tracePages(db, bits);
        }
    }

    /**
     * Prints the contents of tree rooted at this node. No latches are acquired
     * by this method -- it is only used for debugging.
     */
    /* FIXME
    void dump(Database db, String indent) throws IOException {
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
            Entry entry = new Entry();
            for (int pos = mSearchVecEnd - mSearchVecStart; pos >= 0; pos -= 2) {
                retrieveLeafEntry(pos, entry);
                System.out.println(indent + mId + ": " +
                                   dumpToString(entry.key) + " = " + dumpToString(entry.value));
            }
            return;
        }

        TreeNode child = mChildNodes[mChildNodes.length - 1];
        long childId = retrieveChildRefIdFromIndex(mChildNodes.length - 1);

        if (child == null || childId != child.mId) {
            child = new TreeNode(db.pageSize(), false);
            child.read(db, childId);
        }

        if (child != null) {
            child.dump(db, indent + "  ");
        }

        for (int pos = mSearchVecEnd - mSearchVecStart; pos >= 0; pos -= 2) {
            System.out.println(indent + mId + ": " + dumpToString(retrieveInternalKey(pos)));

            child = mChildNodes[pos >> 1];
            childId = retrieveChildRefId(pos);

            if (child == null || childId != child.mId) {
                child = new TreeNode(db.pageSize(), false);
                child.read(db, childId);
            }

            if (child != null) {
                child.dump(db, indent + "  ");
            }
        }
    }
    */

    private static String dumpToString(byte[] bytes) {
        for (byte b : bytes) {
            if (b < '-' || b > 'z') {
                throw new AssertionError(Arrays.toString(bytes));
            }
        }
        return new String(bytes);
    }
}
