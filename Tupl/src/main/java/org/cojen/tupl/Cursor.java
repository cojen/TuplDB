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

import java.util.ArrayDeque;

import java.util.concurrent.locks.Lock;

/**
 * Maintains a fixed logical position in the tree. Cursors must be reset when
 * no longer needed to free up memory.
 *
 * @author Brian S O'Neill
 */
public class Cursor {
    private final TreeNodeStore mStore;

    // Top stack frame for cursor, always a leaf.
    private CursorFrame mLeaf;

    Cursor(TreeNodeStore store) {
        mStore = store;
    }

    /**
     * Returns a copy of the key at the cursor's position, never null.
     *
     * @throws IllegalStateException if position is undefined
     */
    public synchronized byte[] getKey() throws IOException {
        CursorFrame leaf = leafShared();
        TreeNode node = leaf.mNode;
        int pos = leaf.mNodePos;
        byte[] key = pos < 0 ? (leaf.mNotFoundKey.clone()) : node.retrieveLeafKey(pos);
        node.releaseShared();
        return key;
    }

    /**
     * Returns a copy of the value at the cursor's position. Null is returned
     * if entry doesn't exist.
     *
     * @throws IllegalStateException if position is undefined
     */
    public synchronized byte[] getValue() throws IOException {
        CursorFrame leaf = leafShared();
        TreeNode node = leaf.mNode;
        int pos = leaf.mNodePos;
        byte[] value = pos < 0 ? null : node.retrieveLeafValue(pos);
        node.releaseShared();
        return value;
    }

    /**
     * Returns a copy of the key and value at the cursor's position. False is
     * returned if entry doesn't exist.
     *
     * @param entry entry to fill in; pass null to just check if entry exists
     * @throws IllegalStateException if position is undefined
     */
    public synchronized boolean getEntry(Entry entry) throws IOException {
        CursorFrame leaf = leafShared();
        TreeNode node = leaf.mNode;
        int pos = leaf.mNodePos;
        if (pos < 0) {
            if (entry != null) {
                entry.key = leaf.mNotFoundKey.clone();
                entry.value = null;
            }
            node.releaseShared();
            return false;
        } else {
            if (entry != null) {
                node.retrieveLeafEntry(pos, entry);
            }
            node.releaseShared();
            return true;
        }
    }

    /**
     * Move the cursor to find the first available entry, unless none exists.
     *
     * @return false if no entries exist and position is now undefined
     */
    public synchronized boolean first() throws IOException {
        TreeNode node = mStore.root();
        CursorFrame frame = resetFind(node);

        if (!node.hasKeys()) {
            node.releaseExclusive();
            return false;
        }

        while (true) {
            frame.bind(node, 0);

            if (node.isLeaf()) {
                node.releaseExclusive();
                mLeaf = frame;
                return true;
            }

            if (node.mSplit != null) {
                node = node.mSplit.latchLeft(mStore, node);
            }

            node = latchChild(node, 0);
            frame = new CursorFrame(frame);
        }
    }

    /**
     * Move the cursor to find the last available entry, unless none exists.
     *
     * @return false if no entries exist and position is now undefined
     */
    public synchronized boolean last() throws IOException {
        TreeNode node = mStore.root();
        CursorFrame frame = resetFind(node);

        if (!node.hasKeys()) {
            node.releaseExclusive();
            return false;
        }

        while (true) {
            if (node.isLeaf()) {
                int pos;
                if (node.mSplit == null) {
                    pos = node.highestLeafPos();
                } else {
                    pos = node.mSplit.highestLeafPos(mStore, node);
                }
                frame.bind(node, pos);
                node.releaseExclusive();
                mLeaf = frame;
                return true;
            }

            Split split = node.mSplit;
            if (split == null) {
                int childPos = node.highestInternalPos();
                frame.bind(node, childPos);
                node = latchChild(node, childPos);
            } else {
                // Follow highest position of split, binding this frame to the
                // unsplit node as if it had not split. The binding will be
                // corrected when split is finished.

                final TreeNode sibling = split.latchSibling(mStore);

                final TreeNode left, right;
                if (split.mSplitRight) {
                    left = node;
                    right = sibling;
                } else {
                    left = sibling;
                    right = node;
                }

                int highestRightPos = right.highestInternalPos();
                frame.bind(node, left.highestInternalPos() + 2 + highestRightPos);
                left.releaseExclusive();

                node = latchChild(right, highestRightPos);
            }

            frame = new CursorFrame(frame);
        }
    }

    /**
     * Move the cursor to find the given key, returning true if a corresponding
     * entry exists. If false is returned, a reference to the key (uncopied) is
     * retained. Key reference is released as soon as cursor position changes
     * or corresponding entry is created.
     *
     * @return false if entry not found
     * @throws NullPointerException if key is null
     */
    public synchronized boolean find(byte[] key) throws IOException {
        return find(key, false);
    }

    // Caller must be synchronized.
    private boolean find(byte[] key, boolean retainLatch) throws IOException {
        TreeNode node = mStore.root();
        CursorFrame frame = resetFind(node);

        while (true) {
            if (node.isLeaf()) {
                int pos;
                if (node.mSplit == null) {
                    pos = node.binarySearchLeaf(key);
                } else {
                    pos = node.mSplit.binarySearchLeaf(mStore, node, key);
                }
                frame.bind(node, pos);
                if (pos < 0) {
                    frame.mNotFoundKey = key;
                }
                if (!retainLatch) {
                    node.releaseExclusive();
                }
                mLeaf = frame;
                return pos >= 0;
            }

            Split split = node.mSplit;
            if (split == null) {
                int childPos = TreeNode.internalPos(node.binarySearchInternal(key));
                frame.bind(node, childPos);
                node = latchChild(node, childPos);
            } else {
                // Follow search into split, binding this frame to the unsplit
                // node as if it had not split. The binding will be corrected
                // when split is finished.

                final TreeNode sibling = split.latchSibling(mStore);

                final TreeNode left, right;
                if (split.mSplitRight) {
                    left = node;
                    right = sibling;
                } else {
                    left = sibling;
                    right = node;
                }

                final TreeNode selected;
                final int selectedPos;

                if (split.compare(key) < 0) {
                    selected = left;
                    selectedPos = TreeNode.internalPos(left.binarySearchInternal(key));
                    frame.bind(node, selectedPos);
                    right.releaseExclusive();
                } else {
                    selected = right;
                    selectedPos = TreeNode.internalPos(right.binarySearchInternal(key));
                    frame.bind(node, left.highestInternalPos() + 2 + selectedPos);
                    left.releaseExclusive();
                }

                node = latchChild(selected, selectedPos);
            }

            frame = new CursorFrame(frame);
        }
    }

    /**
     * Move the cursor to find the first available entry greater than or equal
     * to the given key.
     *
     * @return false if entry not found and position is now undefined
     * @throws NullPointerException if key is null
     */
    public synchronized boolean findGe(byte[] key) throws IOException {
        if (find(key, true)) {
            mLeaf.mNode.releaseExclusive();
            return true;
        } else {
            return next(mLeaf);
        }
    }

    /**
     * Move the cursor to find the first available entry greater than the given
     * key.
     *
     * @return false if entry not found and position is now undefined
     * @throws NullPointerException if key is null
     */
    public synchronized boolean findGt(byte[] key) throws IOException {
        find(key, true);
        return next(mLeaf);
    }

    /**
     * Move the cursor to find the first available entry less than or equal to
     * the given key.
     *
     * @return false if entry not found and position is now undefined
     * @throws NullPointerException if key is null
     */
    public synchronized boolean findLe(byte[] key) throws IOException {
        if (find(key, true)) {
            mLeaf.mNode.releaseExclusive();
            return true;
        } else {
            return previous(mLeaf);
        }
    }

    /**
     * Move the cursor to find the first available entry less than the given
     * key.
     *
     * @return false if entry not found and position is now undefined
     * @throws NullPointerException if key is null
     */
    public synchronized boolean findLt(byte[] key) throws IOException {
        find(key, true);
        return previous(mLeaf);
    }

    /**
     * Move the cursor by a relative amount of entries, which may be less if
     * not enough entries exist. Pass a positive amount for forward movement,
     * and pass a negative amount for reverse movement.
     *
     * @return actual amount moved
     * @throws IllegalStateException if position is undefined
     */
    public synchronized long move(long amount) throws IOException {
        // FIXME
        throw null;
    }

    /**
     * Advances to the cursor to the next available entry, unless none exists.
     *
     * @return false if no next entry and position is now undefined
     * @throws IllegalStateException if position is undefined
     */
    public synchronized boolean next() throws IOException {
        // FIXME: call move, and no extra synchronization: return move(1) != 0;
        return next(leafExclusive());
    }

    /**
     * @param frame leaf frame, with exclusive latch
     */
    // Caller must be synchronized.
    private boolean next(CursorFrame frame) throws IOException {
        TreeNode node = frame.mNode;
        int pos = frame.mNodePos;
        if (pos < 0) {
            frame.mNotFoundKey = null;
            pos = (~pos) - 2;
        }

        // FIXME: check if node is split, and compare to unsplit highest pos

        if (pos < node.highestLeafPos()) {
            frame.mNodePos = pos + 2;
            node.releaseExclusive();
            return true;
        }

        while (true) {
            frame = frame.pop();
            node.releaseExclusive();
            if (frame == null) {
                mLeaf = null;
                return false;
            }

            frame.acquireExclusiveUnfair();
            node = frame.mNode;
            pos = frame.mNodePos;

            // FIXME: check if node is split, and compare to unsplit highest pos

            if (pos < node.highestInternalPos()) {
                pos += 2;
                frame.mNodePos = pos;

                // FIXME: check if node is split, and choose proper child to latch

                node = latchChild_(node, TreeNode.EMPTY_BYTES, pos);

                while (true) {
                    frame = new CursorFrame(frame);
                    frame.bind(node, 0);

                    if (node.isLeaf()) {
                        node.releaseExclusive();
                        mLeaf = frame;
                        return true;
                    }

                    if (node.mSplit != null) {
                        node = node.mSplit.latchLeft(mStore, node);
                    }

                    node = latchChild(node, 0);
                }
            }
        }
    }

    /**
     * Advances to the cursor to the previous available entry, unless none
     * exists.
     *
     * @return false if no previous entry and position is now undefined
     * @throws IllegalStateException if position is undefined
     */
    public synchronized boolean previous() throws IOException {
        // FIXME: call move, and no extra synchronization: return move(-1) != 0;
        return previous(leafExclusive());
    }

    /**
     * @param frame leaf frame, with exclusive latch
     */
    // Caller must be synchronized.
    private boolean previous(CursorFrame frame) throws IOException {
        TreeNode node = frame.mNode;
        int pos = frame.mNodePos;
        if (pos < 0) {
            frame.mNotFoundKey = null;
            pos = ~pos;
        }

        if ((pos -= 2) >= 0) {
            frame.mNodePos = pos;
            node.releaseExclusive();
            return true;
        }

        while (true) {
            frame = frame.pop();
            node.releaseExclusive();
            if (frame == null) {
                mLeaf = null;
                return false;
            }

            frame.acquireExclusiveUnfair();
            node = frame.mNode;
            pos = frame.mNodePos;

            if (pos > 0) {
                pos -= 2;
                frame.mNodePos = pos;

                // FIXME: check if node is split, and choose proper child to latch

                node = latchChild_(node, null, pos);

                while (true) {
                    frame = new CursorFrame(frame);

                    int numKeys = node.numKeys();

                    if (node.isLeaf()) {
                        if (node.mSplit == null) {
                            pos = (numKeys - 1) << 1;
                        } else {
                            // FIXME: wrong position: needs to be sum with sibling
                            pos = (numKeys - 1) << 1;
                        }
                        frame.bind(node, pos);
                        node.releaseExclusive();
                        mLeaf = frame;
                        return true;
                    }

                    if (node.mSplit == null) {
                        int childPos = numKeys << 1;
                        frame.bind(node, childPos);
                        node = latchChild(node, childPos);
                    } else {
                        TreeNode right = node.mSplit.latchRight(mStore, node);
                        // FIXME: wrong position: needs to be sum with sibling
                        frame.bind(node, numKeys << 1);
                        node = latchChild(right, right.numKeys() << 1);
                    }
                }
            }
        }
    }

    /**
     * Store a value into the current entry, leaving the position unchanged. An
     * entry may be inserted, updated or deleted by this method. A null value
     * deletes the entry.
     *
     * @throws IllegalStateException if position is undefined
     */
    public synchronized void store(byte[] value) throws IOException {
        final Lock sharedCommitLock = mStore.sharedCommitLock();
        sharedCommitLock.lock();
        try {
            final CursorFrame leaf = leafExclusiveDirty();
            final TreeNode node = leaf.mNode;
            final int pos = leaf.mNodePos;

            if (pos >= 0) {
                // FIXME
                throw new IOException("Only insert is supported");
            }

            byte[] key = leaf.mNotFoundKey;
            if (key == null) {
                throw new AssertionError();
            }

            // FIXME: Make sure that mNodePos is updated for all bound cursors
            // after entries are deleted.

            // FIXME: If mNodePos switches from positive to negative after
            // delete, create a copy of deleted key.

            int newPos = ~pos;
            node.insertLeafEntry(mStore, newPos, key, value);

            leaf.mNotFoundKey = null;
            leaf.mNodePos = newPos;

            // Fix all cursors in this node.
            CursorFrame frame = node.mLastCursorFrame;
            do {
                if (frame == leaf) {
                    // Don't need to fix self.
                    continue;
                }

                int framePos = frame.mNodePos;

                if (framePos == pos) {
                    // Other cursor is at same not-found position as this one
                    // was. If keys are the same, then other cursor switches to
                    // a found state as well. If key is greater, then position
                    // needs to be updated.

                    byte[] frameKey = frame.mNotFoundKey;
                    int compare = Utils.compareKeys
                        (frameKey, 0, frameKey.length, key, 0, key.length);
                    if (compare > 0) {
                        // Position is a complement, so subtract instead of add.
                        frame.mNodePos = framePos - 2;
                    } else if (compare == 0) {
                        frame.mNodePos = newPos;
                        frame.mNotFoundKey = null;
                    }
                } else if (framePos >= newPos) {
                    frame.mNodePos = framePos + 2;
                } else if (framePos < pos) {
                    // Position is a complement, so subtract instead of add.
                    frame.mNodePos = framePos - 2;
                }
            } while ((frame = frame.mPrevCousin) != null);

            if (node.mSplit == null) {
                node.releaseExclusive();
            } else {
                finishSplit(leaf, node, mStore);
            }
        } finally {
            sharedCommitLock.unlock();
        }
    }

    /**
     * Store a value into the current entry, and then moves to the cursor to
     * the next entry. An entry may be inserted, updated or deleted by this
     * method. A null value deletes the entry. If false is returned, the
     * position is now undefined.
     *
     * @throws IllegalStateException if position is undefined
     */
    public synchronized boolean storeAndNext(byte[] value) throws IOException {
        // FIXME
        throw null;
    }

    /**
     * Store a value into the current entry, and then moves to the cursor to
     * the previous entry. An entry may be inserted, updated or deleted by this
     * method. A null value deletes the entry. If false is returned, the
     * position is now undefined.
     *
     * @throws IllegalStateException if position is undefined
     */
    public synchronized boolean storeAndPrevious(byte[] value) throws IOException {
        // FIXME
        throw null;
    }

    /**
     * Move the cursor to the given key, and store a value only if no
     * corresponding entry exists. True is returned if entry was inserted.
     *
     * @throws NullPointerException if key or value is null
     */
    public synchronized boolean insert(byte[] key, byte[] value) throws IOException {
        // FIXME
        throw null;
    }

    // FIXME: Consider defining findNext and findPrevious methods instead of
    // special insert methods. They compare to the nearest entry, and if not
    // applicable, they perform a full find instead.

    /**
     * Move the cursor to the given key, and store a value only if no
     * corresponding entry exists. True is returned if entry was inserted.
     * This insert variant is optimized for bulk operations and assumes that
     * the key to be inserted is near the current one. If not, a normal insert
     * operation is performed.
     *
     * @throws NullPointerException if key or value is null
     */
    public synchronized boolean insertNext(byte[] key, byte[] value) throws IOException {
        // FIXME
        throw null;
    }

    /**
     * Move the cursor to the given key, and store a value only if no
     * corresponding entry exists. True is returned if entry was inserted.
     * This insert variant is optimized for bulk operations and assumes that
     * the key to be inserted is near the current one. If not, a normal insert
     * operation is performed.
     *
     * @throws NullPointerException if key or value is null
     */
    public synchronized boolean insertPrevious(byte[] key, byte[] value) throws IOException {
        // FIXME
        throw null;
    }

    // TODO: Add bulk next/previous methods which provide entries

    // TODO: Define View as primary interface, not Tree. View supports ranges,
    // count, deleteAll.

    /**
     * Resets the cursor position to be undefined.
     */
    public synchronized void reset() {
        CursorFrame frame = mLeaf;
        if (frame != null) {
            mLeaf = null;
            do {
                frame.acquireExclusiveUnfair();
                TreeNode node = frame.mNode;
                frame = frame.pop();
                node.releaseExclusive();
            } while (frame != null);
        }
    }

    /**
     * Resets all frames and latches root node, exclusively. Caller must be
     * synchronized. Although the normal reset could be called directly, this
     * variant avoids unlatching the root node, since a find operation would
     * immediately relatch it.
     *
     * @return new or recycled frame
     */
    private CursorFrame resetFind(TreeNode root) {
        CursorFrame frame = mLeaf;
        if (frame == null) {
            root.acquireExclusiveUnfair();
            return new CursorFrame();
        } else {
            mLeaf = null;
            while (true) {
                frame.acquireExclusiveUnfair();
                TreeNode node = frame.mNode;
                CursorFrame parent = frame.pop();
                if (parent != null) {
                    node.releaseExclusive();
                    frame = parent;
                } else {
                    // Usually the root frame refers to the root node, but it
                    // can be wrong if the tree height is changing.
                    if (node != root) {
                        node.releaseExclusive();
                        root.acquireExclusiveUnfair();
                    }
                    return frame;
                }
            }
        }
    }

    /**
     * Verifies that cursor state is correct by performing a find operation.
     *
     * @return false if unable to verify completely at this time
     */
    synchronized boolean verify() throws IOException, IllegalStateException {
        return verify(getKey());
    }

    /**
     * Verifies that cursor state is correct by performing a find operation.
     *
     * @return false if unable to verify completely at this time
     * @throws NullPointerException if key is null
     */
    synchronized boolean verify(byte[] key) throws IllegalStateException {
        ArrayDeque<CursorFrame> frames;
        {
            CursorFrame frame = mLeaf;
            if (frame == null) {
                return true;
            }
            frames = new ArrayDeque<CursorFrame>(10);
            do {
                frames.addFirst(frame);
                frame = frame.mParentFrame;
            } while (frame != null);
        }

        CursorFrame frame = frames.removeFirst();
        frame.acquireSharedUnfair();
        TreeNode node = frame.mNode;

        if (node.mSplit != null) {
            // Cannot verify into split nodes.
            node.releaseShared();
            return false;
        }

        /* This check cannot be reliably performed, because the snapshot of
         * frames can be stale.
        if (node != mStore.root()) {
            node.releaseShared();
            throw new IllegalStateException("Bottom frame is not at root node");
        }
        */

        while (true) {
            if (node.isLeaf()) {
                int pos = node.binarySearchLeaf(key);

                try {
                    if (frame.mNodePos != pos) {
                        throw new IllegalStateException
                            ("Leaf frame position incorrect: " + frame.mNodePos + " != " + pos);
                    }

                    if (pos < 0) {
                        if (frame.mNotFoundKey == null) {
                            throw new IllegalStateException
                                ("Leaf frame key is not set; pos=" + pos);
                        }
                    } else if (frame.mNotFoundKey != null) {
                        throw new IllegalStateException
                            ("Leaf frame key should not be set; pos=" + pos);
                    }
                } finally {
                    node.releaseShared();
                }

                return true;
            }

            int childPos = TreeNode.internalPos(node.binarySearchInternal(key));

            CursorFrame next;
            try {
                if (frame.mNodePos != childPos) {
                    throw new IllegalStateException
                        ("Internal frame position incorrect: " +
                         frame.mNodePos + " != " + childPos + ", split: " + node.mSplit +
                         //", fpos: " + fpos +
                         //", opos: " + frame.mOpos +
                         //", searchKey: " + CursorTest.string(frame.mSearchKey) +
                         ", key: " + CursorTest.string(key));
                }

                if (frame.mNotFoundKey != null) {
                    throw new IllegalStateException("Internal frame key should not be set");
                }

                next = frames.pollFirst();

                if (next == null) {
                    throw new IllegalStateException("Top frame is not a leaf node");
                }

                next.acquireSharedUnfair();
            } finally {
                node.releaseShared();
            }

            frame = next;
            node = frame.mNode;

            if (node.mSplit != null) {
                // Cannot verify into split nodes.
                node.releaseShared();
                return false;
            }
        }
    }

    /**
     * Latches and returns leaf frame, not split. Caller must be synchronized.
     */
    private CursorFrame leafShared() throws IOException {
        CursorFrame leaf = mLeaf;
        if (leaf == null) {
            throw new IllegalStateException("Position is undefined");
        }

        leaf.acquireSharedUnfair();

        {
            TreeNode node = leaf.mNode;
            if (node.mSplit == null) {
                return leaf;
            }
            node.releaseShared();
        }

        while (true) {
            leaf.acquireExclusiveUnfair();
            TreeNode node = leaf.mNode;
            if (node.mSplit == null) {
                node.downgrade();
                return leaf;
            }
            finishSplit(leaf, node, mStore);
        }
    }

    /**
     * Latches and returns leaf frame, not split. Caller must be synchronized.
     */
    private CursorFrame leafExclusive() throws IOException {
        CursorFrame leaf = mLeaf;
        if (leaf == null) {
            throw new IllegalStateException("Position is undefined");
        }

        while (true) {
            leaf.acquireExclusiveUnfair();
            TreeNode node = leaf.mNode;
            if (node.mSplit == null) {
                return leaf;
            }
            finishSplit(leaf, node, mStore);
        }
    }

    /**
     * Latches and returns leaf frame, not split. Leaf frame and all previous
     * frames are marked as dirty. Caller must be synchronized.
     */
    private CursorFrame leafExclusiveDirty() throws IOException {
        CursorFrame leaf = leafExclusive();
        markDirty(leaf, mStore);
        return leaf;
    }

    /**
     * Called with frame latch held.
     */
    private static void markDirty(final CursorFrame frame, TreeNodeStore store)
        throws IOException
    {
        TreeNode node = frame.mNode;
        if (store.shouldMarkDirty(node)) {
            CursorFrame parentFrame = frame.mParentFrame;
            if (parentFrame == null) {
                store.doMarkDirty(node);
            } else {
                node.releaseExclusive();
                parentFrame.acquireExclusiveUnfair();
                markDirty(parentFrame, store);
                TreeNode parentNode = parentFrame.mNode;
                frame.acquireExclusiveUnfair();
                node = frame.mNode;
                if (store.markDirty(node)) {
                    parentNode.updateChildRefId(parentFrame.mNodePos, node.mId);
                }
                parentNode.releaseExclusive();
            }
        }
    }

    /**
     * Caller must hold exclusive latch and it must verify that node has
     * split. Latch is released when method returns.
     */
    private static void finishSplit(final CursorFrame frame,
                                    TreeNode node,
                                    TreeNodeStore store)
        throws IOException
    {
        if (node == store.root()) {
            node.finishSplitRoot(store);
            node.releaseExclusive();
            return;
        }

        CursorFrame parentFrame = frame.mParentFrame;
        node.releaseExclusive();

        TreeNode parentNode;
        while (true) {
            // Unfair latch to boost priority of thread trying to finish the split.
            parentFrame.acquireExclusiveUnfair();
            parentNode = parentFrame.mNode;
            if (parentNode.mSplit == null) {
                break;
            }
            // Split up the tree.
            finishSplit(parentFrame, parentNode, store);
        }

        frame.acquireExclusiveUnfair();
        node = frame.mNode;

        if (node.mSplit == null) {
            node.releaseExclusive();
        } else {
            parentNode.insertSplitChildRef(store, parentFrame.mNodePos, node);
            if (parentNode.mSplit != null) {
                finishSplit(parentFrame, parentNode, store);
                return;
            }
        }

        parentNode.releaseExclusive();
    }

    /**
     * With parent held exclusively, returns child with exclusive latch held,
     * and parent latch is released.
     */
    // FIXME: remove this
    private TreeNode latchChild_(TreeNode parent, byte[] key, int childPos) throws IOException {
        TreeNode childNode = parent.mChildNodes[childPos >> 1];
        long childId = parent.retrieveChildRefId(childPos);

        check: if (childNode != null && childId == childNode.mId) {
            childNode.acquireExclusiveUnfair();

            // Need to check again in case evict snuck in.
            if (childId != childNode.mId) {
                childNode.releaseExclusive();
                break check;
            }

            parent.releaseExclusive();

            // FIXME: Must finish split now. Don't let cursor into split
            // sibling. Simpler strategy might allow cursor into original node,
            // possibly with an out-of-bounds position. This is the opposite of
            // what Split.rebindFrame does.
            Split split;
            if ((split = childNode.mSplit) != null) {
                if (key == null) {
                    childNode = split.selectRightNodeExclusive(mStore, childNode);
                } else if (key.length == 0) {
                    childNode = split.selectLeftNodeExclusive(mStore, childNode);
                } else {
                    childNode = split.selectNodeExclusive(mStore, childNode, key);
                }
            }

            mStore.used(childNode);
            return childNode;
        }

        // If this point is reached, child needs to be loaded.

        childNode = mStore.allocLatchedNode();
        childNode.mId = childId;
        parent.mChildNodes[childPos >> 1] = childNode;

        // Release parent latch before child has been loaded. Any threads
        // which wish to access the same child will block until this thread
        // has finished loading the child and released its exclusive latch.
        parent.releaseExclusive();

        try {
            childNode.read(mStore, childId);
        } catch (IOException e) {
            // Another thread might access child and see that it is invalid because
            // id is zero. It will assume it got evicted and will load child again.
            childNode.mId = 0;
            childNode.releaseExclusive();
            reset();
            throw e;
        }

        mStore.used(childNode);
        return childNode;
    }

    /**
     * With parent held exclusively, returns child with exclusive latch held,
     * and parent latch is released.
     */
    private TreeNode latchChild(TreeNode parent, int childPos) throws IOException {
        TreeNode childNode = parent.mChildNodes[childPos >> 1];
        long childId = parent.retrieveChildRefId(childPos);

        check: if (childNode != null && childId == childNode.mId) {
            childNode.acquireExclusiveUnfair();

            // Need to check again in case evict snuck in.
            if (childId != childNode.mId) {
                childNode.releaseExclusive();
                break check;
            }

            parent.releaseExclusive();

            mStore.used(childNode);
            return childNode;
        }

        // If this point is reached, child needs to be loaded.

        childNode = mStore.allocLatchedNode();
        childNode.mId = childId;
        parent.mChildNodes[childPos >> 1] = childNode;

        // Release parent latch before child has been loaded. Any threads
        // which wish to access the same child will block until this thread
        // has finished loading the child and released its exclusive latch.
        parent.releaseExclusive();

        try {
            childNode.read(mStore, childId);
        } catch (IOException e) {
            // Another thread might access child and see that it is invalid because
            // id is zero. It will assume it got evicted and will load child again.
            childNode.mId = 0;
            childNode.releaseExclusive();
            throw e;
        }

        mStore.used(childNode);
        return childNode;
    }
}
