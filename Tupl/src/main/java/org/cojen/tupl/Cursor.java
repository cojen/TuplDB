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
    public synchronized byte[] getKey() {
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
    public synchronized byte[] getValue() {
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
    public synchronized boolean getEntry(Entry entry) {
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
     * Move the cursor to find the first available entry, unless none
     * exists. If false is returned, the position is now undefined.
     */
    public synchronized boolean first() throws IOException {
        CursorFrame frame = clearFrames();
        if (frame == null) {
            frame = new CursorFrame();
        }

        TreeNode node = mStore.root();
        node.acquireExclusive();

        while (true) {
            if (node.isLeaf()) {
                if (node.hasKeys()) {
                    frame.bind(node, 0);
                    node.releaseExclusive();
                    mLeaf = frame;
                    return true;
                } else {
                    node.releaseExclusive();
                    return false;
                }
            }

            frame.bind(node, 0);

            node = latchChild(node, TreeNode.EMPTY_BYTES, 0);
            frame = new CursorFrame(frame);
        }
    }

    /**
     * Move the cursor to find the last available entry, unless none exists. If
     * false is returned, the position is now undefined.
     */
    public synchronized boolean last() throws IOException {
        CursorFrame frame = clearFrames();
        if (frame == null) {
            frame = new CursorFrame();
        }

        TreeNode node = mStore.root();
        node.acquireExclusive();

        while (true) {
            int numKeys = node.numKeys();

            if (node.isLeaf()) {
                if (numKeys > 0) {
                    frame.bind(node, (numKeys - 1) << 1);
                    node.releaseExclusive();
                    mLeaf = frame;
                    return true;
                } else {
                    node.releaseExclusive();
                    return false;
                }
            }

            int childPos = numKeys << 1;

            frame.bind(node, childPos);

            node = latchChild(node, null, childPos);
            frame = new CursorFrame(frame);
        }
    }

    /**
     * Move the cursor to find the given key, returning true if a corresponding
     * entry exists. If false is returned, a reference to the key (uncopied) is
     * retained. Key reference is released as soon as cursor position changes
     * or corresponding entry is created.
     *
     * @throws NullPointerException if key is null
     */
    public synchronized boolean find(byte[] key) throws IOException {
        return find(key, false);
    }

    // Caller must be synchronized.
    private boolean find(byte[] key, boolean retainLatch) throws IOException {
        // FIXME: Don't unregister the root frame.
        CursorFrame frame = clearFrames();
        if (frame == null) {
            frame = new CursorFrame();
        }

        TreeNode node = mStore.root();
        node.acquireExclusive();

        // Note: No need to check if root has split, since root splits are
        // always completed before releasing the root latch. Also, root node
        // is not managed in usage list, because it cannot be evicted.

        while (true) {
            if (node.isLeaf()) {
                int pos = node.binarySearchLeaf(key);
                frame.bind(node, pos);
                if (!retainLatch) {
                    node.releaseExclusive();
                }
                mLeaf = frame;
                if (pos < 0) {
                    frame.mNotFoundKey = key;
                    return false;
                } else {
                    return true;
                }
            }

            int childPos = node.binarySearchInternal(key);

            if (childPos < 0) {
                childPos = ~childPos;
            } else {
                childPos += 2;
            }

            frame.bind(node, childPos);

            node = latchChild(node, key, childPos);
            frame = new CursorFrame(frame);
        }
    }

    /**
     * Move the cursor to find the first available entry greater than or equal
     * to the given key. If false is returned, the position is now undefined.
     *
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
     * key. If false is returned, the position is now undefined.
     *
     * @throws NullPointerException if key is null
     */
    public synchronized boolean findGt(byte[] key) throws IOException {
        find(key, true);
        return next(mLeaf);
    }

    /**
     * Move the cursor to find the first available entry less than or equal to
     * the given key. If false is returned, the position is now undefined.
     *
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
     * key. If false is returned, the position is now undefined.
     *
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
     * Advances to the cursor to the next available entry, unless none
     * exists. If false is returned, the position is now undefined.
     *
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

        if (pos < node.highestPos()) {
            frame.mNodePos = pos + 2;
            node.releaseExclusive();
            return true;
        }

        while (true) {
            frame = frame.pop();
            if (frame == null) {
                mLeaf = null;
                return false;
            }

            frame.acquireExclusive();
            node = frame.mNode;
            pos = frame.mNodePos;

            if (pos <= node.highestPos()) {
                pos += 2;
                frame.mNodePos = pos;

                node = latchChild(node, TreeNode.EMPTY_BYTES, pos);

                while (true) {
                    frame = new CursorFrame(frame);

                    if (node.isLeaf()) {
                        frame.bind(node, 0);
                        node.releaseExclusive();
                        mLeaf = frame;
                        return true;
                    }

                    frame.bind(node, 0);

                    node = latchChild(node, TreeNode.EMPTY_BYTES, 0);
                }
            }
        }
    }

    /**
     * Advances to the cursor to the previous available entry, unless none
     * exists. If false is returned, the position is now undefined.
     *
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
            if (frame == null) {
                mLeaf = null;
                return false;
            }

            frame.acquireExclusive();
            node = frame.mNode;
            pos = frame.mNodePos;

            if (pos > 0) {
                pos -= 2;
                frame.mNodePos = pos;

                node = latchChild(node, null, pos);

                while (true) {
                    frame = new CursorFrame(frame);

                    int numKeys = node.numKeys();

                    if (node.isLeaf()) {
                        frame.bind(node, (numKeys - 1) << 1);
                        node.releaseExclusive();
                        mLeaf = frame;
                        return true;
                    }

                    int childPos = numKeys << 1;

                    frame.bind(node, childPos);

                    node = latchChild(node, null, childPos);
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
                        // Position is a compliment, so subtract instead of add.
                        frame.mNodePos = framePos - 2;
                    } else if (compare == 0) {
                        frame.mNodePos = newPos;
                        frame.mNotFoundKey = null;
                    }
                } else if (framePos >= newPos) {
                    frame.mNodePos = framePos + 2;
                } else if (framePos < pos) {
                    // Position is a compliment, so subtract instead of add.
                    frame.mNodePos = framePos - 2;
                }
            } while ((frame = frame.mPrevSibling) != null);

            Split split;
            if ((split = node.mSplit) == null) {
                node.releaseExclusive();
            } else {
                // FIXME: move into a new method, to be used also by finishSplit
                TreeNodeStore store = mStore;
                frame = node.mLastCursorFrame;
                do {
                    // Capture previous frame from linked list before changing the links.
                    CursorFrame prev = frame.mPrevSibling;
                    split.fixFrame(store, frame);
                    frame = prev;
                } while (frame != null);

                if (node == store.root()) {
                    node.finishSplitRoot(store);
                    node.releaseExclusive();
                } else {
                    finishSplit(leaf, node, store);
                }
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
        clearFrames();
    }

    // Caller must be synchronized.
    private CursorFrame clearFrames() {
        CursorFrame frame = mLeaf;
        if (frame == null) {
            return null;
        }

        mLeaf = null;

        while (true) {
            frame.acquireExclusive();
            CursorFrame prev = frame.pop();
            if (prev == null) {
                frame.mNotFoundKey = null;
                return frame;
            }
            frame = prev;
        }
    }

    /**
     * Latches and returns leaf frame. Caller must be synchronized.
     */
    private CursorFrame leafShared() {
        CursorFrame leaf = mLeaf;
        if (leaf == null) {
            throw new IllegalStateException("Position is undefined");
        }
        leaf.acquireShared();
        return leaf;
    }

    /**
     * Latches and returns leaf frame. Caller must be synchronized.
     */
    private CursorFrame leafExclusive() {
        CursorFrame leaf = mLeaf;
        if (leaf == null) {
            throw new IllegalStateException("Position is undefined");
        }
        leaf.acquireExclusive();
        return leaf;
    }

    /**
     * Ensure leaf frame and all previous frames are marked as dirty. Called
     * without any latch held, but exclusive latch is held as a side-effect.
     * Caller must be synchronized.
     *
     * @return leaf frame
     */
    private CursorFrame leafExclusiveDirty() throws IOException {
        CursorFrame leaf = mLeaf;
        if (leaf == null) {
            throw new IllegalStateException("Position is undefined");
        }
        markDirty(leaf, mStore);
        return leaf;
    }

    /**
     * Ensure frame and all previous frames are marked as dirty. Called without
     * any latch held, but exclusive latch is held as a side-effect. Caller
     * must be synchronized.
     */
    private static void markDirty(CursorFrame frame, TreeNodeStore store) throws IOException {
        frame.acquireExclusive();
        TreeNode node = frame.mNode;
        if (store.shouldMarkDirty(node)) {
            frame = frame.mParentFrame;
            if (frame == null) {
                store.doMarkDirty(node);
            } else {
                node.releaseExclusive();
                markDirty(frame, store);
                node = frame.mNode;
                int childPos = frame.mNodePos;
                TreeNode childNode = node.mChildNodes[childPos >> 1];
                childNode.acquireExclusiveUnfair();
                if (store.markDirty(childNode)) {
                    node.updateChildRefId(childPos, childNode.mId);
                }
                node.releaseExclusive();
            }
        }
    }

    /**
     * Caller must hold exclusive latch and it must verify that node has
     * split. Latch is released when method returns.
     */
    private static void finishSplit(CursorFrame frame, TreeNode node, TreeNodeStore store)
        throws IOException
    {
        CursorFrame parent = frame.mParentFrame;
        node.releaseExclusive();

        // Unfair latch to boost priority of thread trying to finish the split.
        parent.acquireExclusiveUnfair();

        if (parent.mNode.mSplit != null) {
            // FIXME: must fix frames too
            // finishSplit(parent, store);
            throw new IOException("FIXME");
        }

        TreeNode parentNode = parent.mNode;
        int pos = parent.mNodePos;
        node = parentNode.mChildNodes[pos >> 1];
        node.acquireExclusiveUnfair();

        if (node.mSplit == null) {
            node.releaseExclusive();
        } else {
            parentNode.insertSplitChildRef(store, pos, node);
        }

        parentNode.releaseExclusive();
    }

    /**
     * Parent must be held exclusively, returns child with exclusive latch
     * held, parent latch is released.
     */
    private TreeNode latchChild(TreeNode parent, byte[] key, int childPos) throws IOException {
        TreeNode childNode = parent.mChildNodes[childPos >> 1];
        long childId = parent.retrieveChildRefId(childPos);

        check: if (childNode != null && childId == childNode.mId) {
            childNode.acquireExclusive();

            // Need to check again in case evict snuck in.
            if (childId != childNode.mId) {
                childNode.releaseExclusive();
                break check;
            }

            // FIXME: Don't follow split, finish it.
            Split split;
            if ((split = childNode.mSplit) != null) {
                if (key == null) {
                    childNode = split.selectRightNodeExclusive(mStore, childNode);
                } else if (key.length == 0) {
                    childNode = split.selectLeftNodeExclusive(mStore, childNode);
                } else {
                    childNode = split.selectNodeExclusive(mStore, key, childNode);
                }
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
            reset();
            throw e;
        }

        mStore.used(childNode);
        return childNode;
    }
}
