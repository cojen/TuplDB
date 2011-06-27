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

/**
 * Maintains a fixed logical position in the tree. Cursors must be reset when
 * no longer needed to free up memory.
 *
 * @author Brian S O'Neill
 */
public class Cursor {
    private final TreeNodeStore mStore;
    private final TreeNode mRoot;

    // Top stack frame for cursor.
    private CursorFrame mTopFrame;

    // Reference to key which wasn't found.
    private byte[] mNotFoundKey;

    Cursor(TreeNodeStore store, TreeNode root) {
        mStore = store;
        mRoot = root;
    }

    // FIXME: Make sure that mNodePos is updated for all bound cursors after
    // entries are inserted and deleted.

    // FIXME: If mNodePos switches from negative to positive, clear reference
    // to mNotFoundKey. If switches other way, create a copy of deleted key.

    /**
     * Returns a copy of the key at the cursor's position, never null.
     *
     * @throws IllegalStateException if position is undefined
     */
    public synchronized byte[] getKey() {
        CursorFrame frame = acquireShared();
        TreeNode node = frame.mNode;
        int pos = frame.mNodePos;
        byte[] key = pos < 0 ? (mNotFoundKey.clone()) : node.retrieveLeafKey(pos);
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
        CursorFrame frame = acquireShared();
        TreeNode node = frame.mNode;
        int pos = frame.mNodePos;
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
        CursorFrame frame = acquireShared();
        TreeNode node = frame.mNode;
        int pos = frame.mNodePos;
        if (pos < 0) {
            if (entry != null) {
                entry.key = mNotFoundKey.clone();
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

        TreeNode node = mRoot;
        node.acquireExclusive();

        while (true) {
            if (node.isLeaf()) {
                if (node.hasKeys()) {
                    frame.bind(node, 0);
                    node.releaseExclusive();
                    mTopFrame = frame;
                    return true;
                } else {
                    node.releaseExclusive();
                    return false;
                }
            }

            frame.bind(node, 0);

            node = fetchChild(node, TreeNode.EMPTY_BYTES, 0);
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

        TreeNode node = mRoot;
        node.acquireExclusive();

        while (true) {
            int numKeys = node.numKeys();

            if (node.isLeaf()) {
                if (numKeys > 0) {
                    frame.bind(node, (numKeys - 1) << 1);
                    node.releaseExclusive();
                    mTopFrame = frame;
                    return true;
                } else {
                    node.releaseExclusive();
                    return false;
                }
            }

            int childPos = numKeys << 1;

            frame.bind(node, childPos);

            node = fetchChild(node, null, childPos);
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
        CursorFrame frame = clearFrames();
        if (frame == null) {
            frame = new CursorFrame();
        }

        TreeNode node = mRoot;
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
                mTopFrame = frame;
                if (pos < 0) {
                    mNotFoundKey = key;
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

            node = fetchChild(node, key, childPos);
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
            mTopFrame.mNode.releaseExclusive();
            return true;
        } else {
            return next(mTopFrame);
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
        return next(mTopFrame);
    }

    /**
     * Move the cursor to find the first available entry less than or equal to
     * the given key. If false is returned, the position is now undefined.
     *
     * @throws NullPointerException if key is null
     */
    public synchronized boolean findLe(byte[] key) throws IOException {
        if (find(key, true)) {
            mTopFrame.mNode.releaseExclusive();
            return true;
        } else {
            return previous(mTopFrame);
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
        return previous(mTopFrame);
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
        return next(acquireExclusive());
    }

    /**
     * @param frame top frame, with exclusive latch
     */
    // Caller must be synchronized.
    private boolean next(CursorFrame frame) throws IOException {
        mNotFoundKey = null;

        TreeNode node = frame.mNode;
        int pos = frame.mNodePos;
        if (pos < 0) {
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
                mTopFrame = null;
                return false;
            }

            frame.acquireExclusive();
            node = frame.mNode;
            pos = frame.mNodePos;

            if (pos <= node.highestPos()) {
                pos += 2;
                frame.mNodePos = pos;

                node = fetchChild(node, TreeNode.EMPTY_BYTES, pos);

                while (true) {
                    frame = new CursorFrame(frame);

                    if (node.isLeaf()) {
                        frame.bind(node, 0);
                        node.releaseExclusive();
                        mTopFrame = frame;
                        return true;
                    }

                    frame.bind(node, 0);

                    node = fetchChild(node, TreeNode.EMPTY_BYTES, 0);
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
        return previous(acquireExclusive());
    }

    /**
     * @param frame top frame, with exclusive latch
     */
    // Caller must be synchronized.
    private boolean previous(CursorFrame frame) throws IOException {
        mNotFoundKey = null;

        TreeNode node = frame.mNode;
        int pos = frame.mNodePos;
        if (pos < 0) {
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
                mTopFrame = null;
                return false;
            }

            frame.acquireExclusive();
            node = frame.mNode;
            pos = frame.mNodePos;

            if (pos > 0) {
                pos -= 2;
                frame.mNodePos = pos;

                node = fetchChild(node, null, pos);

                while (true) {
                    frame = new CursorFrame(frame);

                    int numKeys = node.numKeys();

                    if (node.isLeaf()) {
                        frame.bind(node, (numKeys - 1) << 1);
                        node.releaseExclusive();
                        mTopFrame = frame;
                        return true;
                    }

                    int childPos = numKeys << 1;

                    frame.bind(node, childPos);

                    node = fetchChild(node, null, childPos);
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
        // FIXME
        throw null;
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
        CursorFrame frame = mTopFrame;
        if (frame == null) {
            return null;
        }

        mTopFrame = null;
        mNotFoundKey = null;

        while (true) {
            frame.acquireExclusive();
            CursorFrame prev = frame.pop();
            if (prev == null) {
                return frame;
            }
            frame = prev;
        }
    }

    // Caller must be synchronized.
    private CursorFrame acquireShared() {
        CursorFrame top = mTopFrame;
        if (top == null) {
            throw new IllegalStateException("Position is undefined");
        }
        top.acquireShared();
        return top;
    }

    // Caller must be synchronized.
    private CursorFrame acquireExclusive() {
        CursorFrame top = mTopFrame;
        if (top == null) {
            throw new IllegalStateException("Position is undefined");
        }
        top.acquireExclusive();
        return top;
    }

    /**
     * Parent must be held exclusively, returns child with exclusive latch
     * held, parent latch is released.
     */
    private TreeNode fetchChild(TreeNode parent, byte[] key, int childPos) throws IOException {
        TreeNode childNode = parent.mChildNodes[childPos >> 1];
        long childId = parent.retrieveChildRefId(childPos);

        check: if (childNode != null && childId == childNode.mId) {
            childNode.acquireExclusive();

            // Need to check again in case evict snuck in.
            if (childId != childNode.mId) {
                childNode.releaseExclusive();
                break check;
            }

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
