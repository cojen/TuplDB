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
 * Maintains a logical position in the tree. Cursors must be {@link #reset
 * reset} when no longer needed to free up memory. Although not necessarily
 * practical, multiple threads may safely interact with Cursor instances. Only
 * one thread has access when the Cursor is synchronized.
 *
 * @author Brian S O'Neill
 */
public final class Cursor {
    private final TreeNodeStore mStore;

    // Top stack frame for cursor, always a leaf.
    private CursorFrame mLeaf;

    Cursor(TreeNodeStore store) {
        mStore = store;
    }

    /**
     * Returns a copy of the key at the Cursor's current position, never null.
     *
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public synchronized byte[] getKey() throws IOException {
        CursorFrame leaf = leafSharedNotSplit();
        TreeNode node = leaf.mNode;
        int pos = leaf.mNodePos;
        byte[] key = pos < 0 ? (leaf.mNotFoundKey.clone()) : node.retrieveLeafKey(pos);
        node.releaseShared();
        return key;
    }

    /**
     * Returns a copy of the value at the Cursor's current position. Null is
     * returned if entry doesn't exist.
     *
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public synchronized byte[] getValue() throws IOException {
        CursorFrame leaf = leafSharedNotSplit();
        TreeNode node = leaf.mNode;
        int pos = leaf.mNodePos;
        byte[] value = pos < 0 ? null : node.retrieveLeafValue(pos);
        node.releaseShared();
        return value;
    }

    /**
     * Returns a copy of the key and value at the Cursor's current position. If
     * entry doesn't exist, value is assigned null and false is returned. A
     * non-null key is always available, even for entries which don't exist.
     *
     * @param entry entry to fill in; pass null to just check if entry exists
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public synchronized boolean getEntry(Entry entry) throws IOException {
        CursorFrame leaf = leafSharedNotSplit();
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
     * Returns a copy of the key and value at the Cursor's current
     * position. Entry value is null if it doesn't exist. A non-null key is
     * always available, even for entries which don't exist.
     *
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public Entry getEntry() throws IOException {
        Entry entry = new Entry();
        getEntry(entry);
        return entry;
    }

    /**
     * Moves the Cursor to find the first available entry, unless none exists.
     *
     * @return false if no entries exist and position is now undefined
     */
    public synchronized boolean first() throws IOException {
        TreeNode root = mStore.root();
        CursorFrame frame = resetForFind(root);

        if (!root.hasKeys()) {
            root.releaseExclusive();
            return false;
        }

        return toFirst(root, frame);
    }

    /**
     * Moves the Cursor to the first subtree entry. Caller must be synchronized.
     *
     * @param node latched node
     * @param frame frame to bind node to
     */
    private boolean toFirst(TreeNode node, CursorFrame frame) throws IOException {
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
     * Moves the Cursor to find the last available entry, unless none exists.
     *
     * @return false if no entries exist and position is now undefined
     */
    public synchronized boolean last() throws IOException {
        TreeNode root = mStore.root();
        CursorFrame frame = resetForFind(root);

        if (!root.hasKeys()) {
            root.releaseExclusive();
            return false;
        }

        return toLast(root, frame);
    }

    /**
     * Moves the Cursor to the last subtree entry. Caller must be synchronized.
     *
     * @param node latched node
     * @param frame frame to bind node to
     */
    private boolean toLast(TreeNode node, CursorFrame frame) throws IOException {
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
     * Moves the Cursor by a relative amount of entries. Pass a positive amount
     * for forward movement, and pass a negative amount for reverse
     * movement. The actual movement amount can be less than the requested
     * amount if the start or end is reached. After this happens, the position
     * is undefined.
     *
     * @return actual amount moved; if less, position is now undefined
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public synchronized long move(long amount) throws IOException {
        // TODO: optimize and also utilize counts embedded in the tree
        final long originalAmount = amount;
        if (amount > 0) {
            while (next() && --amount > 0);
        } else if (amount < 0) {
            while (previous() && ++amount < 0);
        }
        return originalAmount - amount;
    }

    /**
     * Advances to the Cursor to the next available entry, unless none
     * exists. Equivalent to:
     *
     * <pre>
     * return cursor.move(1) != 0;</pre>
     *
     * @return false if no next entry and position is now undefined
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public synchronized boolean next() throws IOException {
        return next(leafExclusiveNotSplit());
    }

    /**
     * @param frame leaf frame, not split, with exclusive latch
     */
    // Caller must be synchronized.
    private boolean next(CursorFrame frame) throws IOException {
        TreeNode node = frame.mNode;
        int pos = frame.mNodePos;
        if (pos < 0) {
            frame.mNotFoundKey = null;
            pos = (~pos) - 2;
        }

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

            node = frame.acquireExclusiveUnfair();

            if (node.mSplit != null) {
                node = finishSplit(frame, node, mStore);
            }

            pos = frame.mNodePos;

            if (pos < node.highestInternalPos()) {
                pos += 2;
                frame.mNodePos = pos;
                return toFirst(latchChild(node, pos), new CursorFrame(frame));
            }
        }
    }

    /**
     * Advances to the Cursor to the previous available entry, unless none
     * exists. Equivalent to:
     *
     * <pre>
     * return cursor.move(-1) != 0;</pre>
     *
     * @return false if no previous entry and position is now undefined
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public synchronized boolean previous() throws IOException {
        return previous(leafExclusiveNotSplit());
    }

    /**
     * @param frame leaf frame, not split, with exclusive latch
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

            node = frame.acquireExclusiveUnfair();

            if (node.mSplit != null) {
                node = finishSplit(frame, node, mStore);
            }

            pos = frame.mNodePos;

            if (pos > 0) {
                pos -= 2;
                frame.mNodePos = pos;
                return toLast(latchChild(node, pos), new CursorFrame(frame));
            }
        }
    }

    /**
     * Moves the Cursor to find the given key, returning true if a matching
     * entry exists. If false is returned, an uncopied reference to the key is
     * retained. The key reference is released when the Cursor position changes
     * or a matching entry is created.
     *
     * @return false if entry not found
     * @throws NullPointerException if key is null
     */
    public synchronized boolean find(byte[] key) throws IOException {
        return find(key, false);
    }

    // Caller must be synchronized.
    private boolean find(byte[] key, boolean retainLatch) throws IOException {
        if (key == null) {
            throw new NullPointerException("Cannot find a null key");
        }
        TreeNode root = mStore.root();
        return find(key, root, resetForFind(root), retainLatch);
    }

    /**
     * @param key non-null key, unchecked
     * @param node latched unbound node
     * @param frame frame to bind to
     * @param retainLatch true to keep leaf frame latched
     */
    // Caller must be synchronized.
    private boolean find(byte[] key, TreeNode node, CursorFrame frame, boolean retainLatch)
        throws IOException
    {
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
     * Moves the Cursor to find the first available entry greater than or equal
     * to the given key. Equivalent to:
     *
     * <pre>
     * return cursor.find(key) ? true : cursor.next();</pre>
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
     * Moves the Cursor to find the first available entry greater than the
     * given key. Equivalent to:
     *
     * <pre>
     * cursor.find(key); return cursor.next();</pre>
     *
     * @return false if entry not found and position is now undefined
     * @throws NullPointerException if key is null
     */
    public synchronized boolean findGt(byte[] key) throws IOException {
        find(key, true);
        return next(mLeaf);
    }

    /**
     * Moves the Cursor to find the first available entry less than or equal to
     * the given key. Equivalent to:
     *
     * <pre>
     * return cursor.find(key) ? true : cursor.previous();</pre>
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
     * Moves the Cursor to find the first available entry less than the given
     * key. Equivalent to:
     *
     * <pre>
     * cursor.find(key); return cursor.previous();</pre>
     *
     * @return false if entry not found and position is now undefined
     * @throws NullPointerException if key is null
     */
    public synchronized boolean findLt(byte[] key) throws IOException {
        find(key, true);
        return previous(mLeaf);
    }

    /**
     * Optimized version of the regular find method, which can perform fewer
     * search steps if the given key is in close proximity to the current one.
     * Even if not in close proximity, the find behavior is still identicial,
     * although it may perform more slowly.
     *
     * @return false if entry not found
     * @throws NullPointerException if key is null
     */
    public synchronized boolean findNearby(byte[] key) throws IOException {
        if (key == null) {
            throw new NullPointerException("Cannot find a null key");
        }

        CursorFrame frame = mLeaf;
        if (frame == null) {
            TreeNode root = mStore.root();
            root.acquireExclusiveUnfair();
            return find(key, root, new CursorFrame(), false);
        }

        TreeNode node = frame.acquireExclusiveUnfair();
        if (node.mSplit != null) {
            node = finishSplit(frame, node, mStore);
        }

        int startPos = frame.mNodePos;
        if (startPos < 0) {
            startPos = ~startPos;
        }

        int pos = node.binarySearchLeaf(key, startPos);

        if (pos >= 0) {
            frame.mNotFoundKey = null;
            frame.mNodePos = pos;
            node.releaseExclusive();
            return true;
        } else if (pos != ~0 && ~pos <= node.highestLeafPos()) {
            // Not found, but insertion pos is in bounds.
            frame.mNotFoundKey = key;
            frame.mNodePos = pos;
            node.releaseExclusive();
            return false;
        }

        // Cannot be certain if position is in leaf node, so pop up.

        mLeaf = null;

        while (true) {
            CursorFrame parent = frame.pop();

            if (parent == null) {
                // Usually the root frame refers to the root node, but it
                // can be wrong if the tree height is changing.
                TreeNode root = mStore.root();
                if (node != root) {
                    node.releaseExclusive();
                    root.acquireExclusiveUnfair();
                    node = root;
                }
                break;
            }

            node.releaseExclusive();
            frame = parent;
            node = frame.acquireExclusiveUnfair();

            // Only search inside non-split nodes. It's easier to just pop up
            // rather than finish or search the split.
            if (node.mSplit != null) {
                continue;
            }

            pos = TreeNode.internalPos(node.binarySearchInternal(key, frame.mNodePos));

            if (pos == 0 || pos >= node.highestInternalPos()) {
                // Cannot be certain if position is in this node, so pop up.
                continue;
            }

            frame.mNodePos = pos;
            node = latchChild(node, pos);
            frame = new CursorFrame(frame);
            break;
        }

        return find(key, node, frame, false);
    }

    /**
     * Stores a value into the current entry, leaving the position
     * unchanged. An entry may be inserted, updated or deleted by this
     * method. A null value deletes the entry.
     *
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public synchronized void store(byte[] value) throws IOException {
        final Lock sharedCommitLock = mStore.sharedCommitLock();
        sharedCommitLock.lock();
        try {
            final CursorFrame leaf = leafExclusive();

            if (value == null) {
                // Delete entry.

                if (leaf.mNodePos < 0) {
                    // Entry doesn't exist, so nothing to do.
                    leaf.mNode.releaseExclusive();
                    return;
                }

                TreeNode node = notSplitDirty(leaf, mStore);
                final int pos = leaf.mNodePos;
                final byte[] key = node.retrieveLeafKey(pos);

                node.deleteLeafEntry(pos);
                int newPos = ~pos;

                leaf.mNodePos = newPos;
                leaf.mNotFoundKey = key;

                // Fix all cursors bound to the node.
                CursorFrame frame = node.mLastCursorFrame;
                do {
                    if (frame == leaf) {
                        // Don't need to fix self.
                        continue;
                    }

                    int framePos = frame.mNodePos;

                    if (framePos == pos) {
                        frame.mNodePos = newPos;
                        frame.mNotFoundKey = key;
                    } else if (framePos > pos) {
                        frame.mNodePos = framePos - 2;
                    } else if (framePos < newPos) {
                        // Position is a complement, so add instead of subtract.
                        frame.mNodePos = framePos + 2;
                    }
                } while ((frame = frame.mPrevCousin) != null);

                // FIXME: Merge or delete node if too small now.

                node.releaseExclusive();
                return;
            }

            // Update and insert always dirty the node.
            TreeNode node = notSplitDirty(leaf, mStore);
            final int pos = leaf.mNodePos;

            if (pos >= 0) {
                // Update entry.

                node.updateLeafValue(mStore, pos, value);

                if (node.mSplit != null) {
                    node = finishSplit(leaf, node, mStore);
                } else {
                    // FIXME: Merge or delete node if too small now.
                }

                node.releaseExclusive();
                return;
            }

            // Insert entry.

            final byte[] key = leaf.mNotFoundKey;
            if (key == null) {
                throw new AssertionError();
            }

            int newPos = ~pos;
            node.insertLeafEntry(mStore, newPos, key, value);

            leaf.mNodePos = newPos;
            leaf.mNotFoundKey = null;

            // Fix all cursors bound to the node.
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

            if (node.mSplit != null) {
                node = finishSplit(leaf, node, mStore);
            }

            node.releaseExclusive();
        } catch (Throwable e) {
            // Any unexpected exception can leave the internal state
            // corrupt. Closing down protects the persisted state.
            throw Utils.closeOnFailure(mStore, e);
        } finally {
            sharedCommitLock.unlock();
        }
    }

    // TODO: Define View as primary interface, not Tree. View supports ranges,
    // count, deleteAll.

    /**
     * Returns a new independent Cursor which exactly matches the state of this
     * one. The original and copied Cursor can be acted upon without affecting
     * each other's state.
     */
    public Cursor copy() {
        Cursor copy = new Cursor(mStore);

        CursorFrame frame;
        synchronized (this) {
            frame = mLeaf;
        }

        if (frame == null) {
            return copy;
        }

        CursorFrame frameCopy = new CursorFrame();
        frame.copyInto(frameCopy);

        synchronized (copy) {
            copy.mLeaf = frameCopy;
        }

        return copy;
    }

    /**
     * Returns true if Cursor is currently at a defined position.
     */
    public synchronized boolean isPositioned() {
        return mLeaf != null;
    }

    /**
     * Resets the Cursor position to be undefined.
     */
    public void reset() {
        CursorFrame frame;
        synchronized (this) {
            frame = mLeaf;
            if (frame == null) {
                return;
            }
            mLeaf = null;
        }
        CursorFrame.popAll(frame);
    }

    /**
     * Resets all frames and latches root node, exclusively. Caller must be
     * synchronized. Although the normal reset could be called directly, this
     * variant avoids unlatching the root node, since a find operation would
     * immediately relatch it.
     *
     * @return new or recycled frame
     */
    private CursorFrame resetForFind(TreeNode root) {
        CursorFrame frame = mLeaf;
        if (frame == null) {
            root.acquireExclusiveUnfair();
            return new CursorFrame();
        }

        mLeaf = null;

        while (true) {
            TreeNode node = frame.acquireExclusiveUnfair();
            CursorFrame parent = frame.pop();

            if (parent == null) {
                // Usually the root frame refers to the root node, but it
                // can be wrong if the tree height is changing.
                if (node != root) {
                    node.releaseExclusive();
                    root.acquireExclusiveUnfair();
                }
                return frame;
            }

            node.releaseExclusive();
            frame = parent;
        }
    }

    /**
     * Verifies that Cursor state is correct by performing a find operation.
     *
     * @return false if unable to verify completely at this time
     */
    synchronized boolean verify() throws IOException, IllegalStateException {
        return verify(getKey());
    }

    /**
     * Verifies that Cursor state is correct by performing a find operation.
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
        TreeNode node = frame.acquireSharedUnfair();

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
    private CursorFrame leafSharedNotSplit() throws IOException {
        CursorFrame leaf = mLeaf;
        if (leaf == null) {
            throw new IllegalStateException("Position is undefined");
        }

        TreeNode node = leaf.acquireSharedUnfair();

        if (node.mSplit == null) {
            return leaf;
        }

        node.releaseShared();
        node = leaf.acquireExclusiveUnfair();

        if (node.mSplit != null) {
            node = finishSplit(leaf, node, mStore);
        }

        node.downgrade();
        return leaf;
    }

    /**
     * Latches and returns leaf frame, which might be split. Caller must be
     * synchronized.
     */
    private CursorFrame leafExclusive() {
        CursorFrame leaf = mLeaf;
        if (leaf == null) {
            throw new IllegalStateException("Position is undefined");
        }
        leaf.acquireExclusiveUnfair();
        return leaf;
    }

    /**
     * Latches and returns leaf frame, not split. Caller must be synchronized.
     */
    private CursorFrame leafExclusiveNotSplit() throws IOException {
        CursorFrame leaf = leafExclusive();
        if (leaf.mNode.mSplit != null) {
            finishSplit(leaf, leaf.mNode, mStore);
        }
        return leaf;
    }

    /**
     * Called with exclusive frame latch held, which is retained. Leaf frame is
     * dirtied, any split is finished, and the same applies to all parent
     * nodes. Caller must hold shared commit lock, to prevent deadlock.
     *
     * @return replacement node, still latched
     */
    private static TreeNode notSplitDirty(final CursorFrame frame, TreeNodeStore store)
        throws IOException
    {
        TreeNode node = frame.mNode;

        if (node.mSplit != null) {
            // Already dirty, but finish the split.
            return finishSplit(frame, node, store);
        }

        if (!store.shouldMarkDirty(node)) {
            return node;
        }

        CursorFrame parentFrame = frame.mParentFrame;
        if (parentFrame == null) {
            store.doMarkDirty(node);
            return node;
        }

        // Make sure the parent is not split and dirty too.
        node.releaseExclusive();
        parentFrame.acquireExclusiveUnfair();
        TreeNode parentNode = notSplitDirty(parentFrame, store);
        node = frame.acquireExclusiveUnfair();

        while (node.mSplit != null) {
            // Already dirty now, but finish the split. Since parent latch is
            // already held, no need to call into the regular finishSplit
            // method. It would release latches and recheck everything.
            parentNode.insertSplitChildRef(store, parentFrame.mNodePos, node);
            if (parentNode.mSplit != null) {
                parentNode = finishSplit(parentFrame, parentNode, store);
            }
            node = frame.acquireExclusiveUnfair();
        }
        
        if (store.markDirty(node)) {
            parentNode.updateChildRefId(parentFrame.mNodePos, node.mId);
        }

        parentNode.releaseExclusive();
        return node;
    }

    /**
     * Caller must hold exclusive latch and it must verify that node has split.
     *
     * @return replacement node, still latched
     */
    private static TreeNode finishSplit(final CursorFrame frame,
                                        TreeNode node,
                                        TreeNodeStore store)
        throws IOException
    {
        // FIXME: How to acquire shared commit lock without deadlock?
        if (node == store.root()) {
            node.finishSplitRoot(store);
            return node;
        }

        final CursorFrame parentFrame = frame.mParentFrame;
        node.releaseExclusive();

        // To avoid deadlock, ensure shared commit lock is held. Not all
        // callers acquire the shared lock first, since they usually only read
        // from the tree. Node latch has now been released, which should have
        // been the only latch held, and so commit lock can be acquired without
        // deadlock.

        final Lock sharedCommitLock = store.sharedCommitLock();
        sharedCommitLock.lock();
        try {
            TreeNode parentNode = parentFrame.acquireExclusiveUnfair();
            while (true) {
                if (parentNode.mSplit != null) {
                    parentNode = finishSplit(parentFrame, parentNode, store);
                }
                node = frame.acquireExclusiveUnfair();
                if (node.mSplit == null) {
                    parentNode.releaseExclusive();
                    return node;
                }
                parentNode.insertSplitChildRef(store, parentFrame.mNodePos, node);
            }
        } finally {
            sharedCommitLock.unlock();
        }
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
