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
    public byte[] getKey() throws IOException {
        return get(Entry.GET_KEY);
    }

    /**
     * Returns a copy of the value at the Cursor's current position. Null is
     * returned if entry doesn't exist.
     *
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public byte[] getValue() throws IOException {
        return get(Entry.GET_VALUE);
    }

    /**
     * Returns a copy of the key and value at the Cursor's current position. If
     * entry doesn't exist, value is assigned null and false is returned. A
     * non-null key is always available, even for entries which don't exist.
     *
     * @param entry entry to fill in; pass null to just check if entry exists
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public boolean getEntry(Entry entry) throws IOException {
        return get(entry) != null;
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
        get(entry);
        return entry;
    }

    private synchronized byte[] get(Entry entry) throws IOException {
        CursorFrame leaf = leaf();
        TreeNode node = leaf.acquireSharedUnfair();

        if (node.mSplit == null) {
            byte[] result = entry != null ? entry.get(leaf, node)
                : (leaf.mNodePos < 0 ? null : Utils.EMPTY_BYTES);
            node.releaseShared();
            return result;
        }

        finishSplit: {
            if (!node.tryUpgrade()) {
                node.releaseShared();
                node = leaf.acquireExclusiveUnfair();
                if (node.mSplit == null) {
                    break finishSplit;
                }
            }
            node = finishSplit(leaf, node, mStore);
        }

        byte[] result = entry != null ? entry.get(leaf, node)
            : (leaf.mNodePos < 0 ? null : Utils.EMPTY_BYTES);
        node.releaseExclusive();
        return result;
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

            node = latchChild(node, 0, true);
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
                node = latchChild(node, childPos, true);
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

                node = latchChild(right, highestRightPos, true);
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
                return toFirst(latchChild(node, pos, true), new CursorFrame(frame));
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
                return toLast(latchChild(node, pos, true), new CursorFrame(frame));
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
                node = latchChild(node, childPos, true);
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

                node = latchChild(selected, selectedPos, true);
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
            node = latchChild(node, pos, true);
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
                // Delete entry...

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

                if (node.shouldLeafMerge()) {
                    mergeLeaf(leaf, node);
                } else {
                    node.releaseExclusive();
                }

                return;
            }

            // Update and insert always dirty the node.
            TreeNode node = notSplitDirty(leaf, mStore);
            final int pos = leaf.mNodePos;

            if (pos >= 0) {
                // Update entry...

                node.updateLeafValue(mStore, pos, value);

                if (node.shouldLeafMerge()) {
                    // FIXME: Merge or delete node if too small now.
                } else if (node.mSplit != null) {
                    node = finishSplit(leaf, node, mStore);
                }

                node.releaseExclusive();
                return;
            }

            // Insert entry...

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
            // Any unexpected exception can corrupt the internal store state.
            // Closing down protects the persisted state.
            if (mLeaf == null && e instanceof IllegalStateException) {
                // Exception is caused by cursor state; store is safe.
                throw (IllegalStateException) e;
            }
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
     * Checks that leaf is defined and returns it. Caller must be synchronized.
     */
    private CursorFrame leaf() {
        CursorFrame leaf = mLeaf;
        if (leaf == null) {
            throw new IllegalStateException("Cursor position is undefined");
        }
        return leaf;
    }

    /**
     * Latches and returns leaf frame, which might be split. Caller must be
     * synchronized.
     */
    private CursorFrame leafExclusive() {
        CursorFrame leaf = leaf();
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
     * Caller must hold exclusive latch, which is released by this method.
     */
    private void mergeLeaf(final CursorFrame leaf, TreeNode node) throws IOException {
        CursorFrame parentFrame = leaf.mParentFrame;
        node.releaseExclusive();

        if (parentFrame == null) {
            // Root node cannot merge into anything.
            return;
        }

        TreeNode parentNode = parentFrame.acquireExclusiveUnfair();

        TreeNode leftNode, rightNode;
        int nodeAvail;
        while (true) {
            if (parentNode.mSplit != null) {
                parentNode = finishSplit(parentFrame, parentNode, mStore);
            }

            if (parentNode.numKeys() <= 0) {
                // FIXME: This shouldn't be a problem when internal nodes can be rebalanced.
                System.out.println("tiny internal node: " + (parentNode == mStore.root()));
                parentNode.releaseExclusive();
                return;
            }

            // Latch leaf and siblings in a strict left-to-right order to avoid deadlock.
            int pos = parentFrame.mNodePos;
            if (pos == 0) {
                leftNode = null;
            } else {
                leftNode = latchChild(parentNode, pos - 2, false);
                if (leftNode.mSplit != null) {
                    // Finish sibling split.
                    parentNode.insertSplitChildRef(mStore, pos - 2, leftNode);
                    continue;
                }
            }

            node = leaf.acquireExclusiveUnfair();

            // Double check that node should still merge.
            if (!node.shouldMerge(nodeAvail = node.availableLeafBytes())) {
                if (leftNode != null) {
                    leftNode.releaseExclusive();
                }
                node.releaseExclusive();
                parentNode.releaseExclusive();
                return;
            }

            if (pos >= parentNode.highestInternalPos()) {
                rightNode = null;
            } else {
                rightNode = latchChild(parentNode, pos + 2, false);
                if (rightNode.mSplit != null) {
                    // Finish sibling split.
                    if (leftNode != null) {
                        leftNode.releaseExclusive();
                    }
                    node.releaseExclusive();
                    parentNode.insertSplitChildRef(mStore, pos + 2, rightNode);
                    continue;
                }
            }

            break;
        }

        // Select a left and right pair, and then don't operate directly on the
        // original node and leaf parameters afterwards. The original node ends
        // up being referenced as a left or right member of the pair.

        int leftAvail = leftNode == null ? 0 : leftNode.availableLeafBytes();
        int rightAvail = rightNode == null ? 0 : rightNode.availableLeafBytes();

        // Choose adjacent node pair which has the most available space. If
        // only a rebalance can be performed on the pair, operating on
        // underutilized nodes continues them on a path to deletion.

        int leftPos;
        if (leftAvail < rightAvail) {
            if (leftNode != null) {
                leftNode.releaseExclusive();
            }
            leftPos = parentFrame.mNodePos;
            leftNode = node;
            leftAvail = nodeAvail;
        } else {
            if (rightNode != null) {
                rightNode.releaseExclusive();
            }
            leftPos = parentFrame.mNodePos - 2;
            rightNode = node;
            rightAvail = nodeAvail;
        }

        // Left node must always be marked dirty. Parent is already expected to be dirty.
        if (mStore.markDirty(leftNode)) {
            parentNode.updateChildRefId(leftPos, leftNode.mId);
        }

        // Determine if both nodes can fit in one node. If so, migrate and
        // delete the right node.
        int remaining = leftAvail + rightAvail - node.mPage.length + TreeNode.HEADER_SIZE;

        if (remaining >= 0) {
            // Migrate the entire contents of the right node into the left
            // node, and then delete the right node.
            rightNode.transferLeafToLeftAndDelete(mStore, leftNode);
            parentNode.deleteChildRef(leftPos + 2);
        } else {
            // Rebalance nodes, but don't delete anything. Right node must be dirtied too.

            if (mStore.markDirty(rightNode)) {
                parentNode.updateChildRefId(leftPos + 2, rightNode.mId);
            }

            // FIXME: testing
            if (leftNode.numKeys() == 1 || rightNode.numKeys() == 1) {
                System.out.println("left avail: " + leftAvail + ", right avail: " + rightAvail +
                                   ", left pos: " + leftPos);
                throw new Error("MUST REBALANCE: " + leftNode.numKeys() + ", " + 
                                rightNode.numKeys());
            }

            /*
            System.out.println("left avail: " + leftAvail + ", right avail: " + rightAvail +
                               ", left pos: " + leftPos + ", mode: " + migrateMode);
            */

            if (leftNode == node) {
                // Rebalance towards left node, which is smaller.
                // FIXME
            } else {
                // Rebalance towards right node, which is smaller.
                // FIXME
            }
        }

        mergeInternal(parentFrame, parentNode, leftNode, rightNode);
    }

    /**
     * Caller must hold exclusive latch, which is released by this method.
     */
    private void mergeInternal(CursorFrame frame, TreeNode node,
                               TreeNode leftChildNode, TreeNode rightChildNode)
        throws IOException
    {
        up: {
            if (node.shouldInternalMerge()) {
                if (node.numKeys() > 0) {
                    // Continue merging up the tree.
                    break up;
                }

                // Delete the root node, eliminating a tree level.

                if (node != mStore.root()) {
                    throw new AssertionError("Non-root empty node");
                }

                // Note: By retaining child latches (although one has already
                // been deleted), another thread is prevented from splitting
                // the lone child. The lone child will become the new root.
                node.rootDelete(mStore);
            }

            rightChildNode.releaseExclusive();
            leftChildNode.releaseExclusive();
            node.releaseExclusive();
            return;
        }
            
        rightChildNode.releaseExclusive();
        leftChildNode.releaseExclusive();

        // At this point, only one node latch is held, and it should merge with
        // a sibling node. Node is guaranteed to be a internal node.

        CursorFrame parentFrame = frame.mParentFrame;
        node.releaseExclusive();

        if (parentFrame == null) {
            // Root node cannot merge into anything.
            return;
        }

        TreeNode parentNode = parentFrame.acquireExclusiveUnfair();

        TreeNode leftNode, rightNode;
        int nodeAvail;
        while (true) {
            if (parentNode.mSplit != null) {
                parentNode = finishSplit(parentFrame, parentNode, mStore);
            }

            if (parentNode.numKeys() <= 0) {
                // FIXME: This shouldn't be a problem when internal nodes can be rebalanced.
                System.out.println("tiny internal node (2): " + (parentNode == mStore.root()));
                parentNode.releaseExclusive();
                return;
            }

            // Latch node and siblings in a strict left-to-right order to avoid deadlock.
            int pos = parentFrame.mNodePos;
            if (pos == 0) {
                leftNode = null;
            } else {
                leftNode = latchChild(parentNode, pos - 2, false);
                if (leftNode.mSplit != null) {
                    // Finish sibling split.
                    parentNode.insertSplitChildRef(mStore, pos - 2, leftNode);
                    continue;
                }
            }

            node = frame.acquireExclusiveUnfair();

            // Double check that node should still merge.
            if (!node.shouldMerge(nodeAvail = node.availableInternalBytes())) {
                if (leftNode != null) {
                    leftNode.releaseExclusive();
                }
                node.releaseExclusive();
                parentNode.releaseExclusive();
                return;
            }

            if (pos >= parentNode.highestInternalPos()) {
                rightNode = null;
            } else {
                rightNode = latchChild(parentNode, pos + 2, false);
                if (rightNode.mSplit != null) {
                    // Finish sibling split.
                    if (leftNode != null) {
                        leftNode.releaseExclusive();
                    }
                    node.releaseExclusive();
                    parentNode.insertSplitChildRef(mStore, pos + 2, rightNode);
                    continue;
                }
            }

            break;
        }

        // Select a left and right pair, and then don't operate directly on the
        // original node and frame parameters afterwards. The original node
        // ends up being referenced as a left or right member of the pair.

        int leftAvail = leftNode == null ? 0 : leftNode.availableInternalBytes();
        int rightAvail = rightNode == null ? 0 : rightNode.availableInternalBytes();

        // Choose adjacent node pair which has the most available space. If
        // only a rebalance can be performed on the pair, operating on
        // underutilized nodes continues them on a path to deletion.

        int leftPos;
        if (leftAvail < rightAvail) {
            if (leftNode != null) {
                leftNode.releaseExclusive();
            }
            leftPos = parentFrame.mNodePos;
            leftNode = node;
            leftAvail = nodeAvail;
        } else {
            if (rightNode != null) {
                rightNode.releaseExclusive();
            }
            leftPos = parentFrame.mNodePos - 2;
            rightNode = node;
            rightAvail = nodeAvail;
        }

        // Left node must always be marked dirty. Parent is already expected to be dirty.
        if (mStore.markDirty(leftNode)) {
            parentNode.updateChildRefId(leftPos, leftNode.mId);
        }

        // Determine if both nodes plus parent key can fit in one node. If so,
        // migrate and delete the right node.
        byte[] parentPage = parentNode.mPage;
        int parentEntryLoc = DataIO.readUnsignedShort
            (parentPage, parentNode.mSearchVecStart + leftPos);
        int parentEntryLen = TreeNode.internalEntryLength(parentPage, parentEntryLoc);
        int remaining = leftAvail - parentEntryLen
            + rightAvail - parentPage.length + (TreeNode.HEADER_SIZE - 2);

        if (remaining >= 0) {
            // Migrate the entire contents of the right node into the left
            // node, and then delete the right node.
            rightNode.transferInternalToLeftAndDelete
                (mStore, leftNode, parentPage, parentEntryLoc, parentEntryLen);
            parentNode.deleteChildRef(leftPos + 2);
        } else {
            // Rebalance nodes, but don't delete anything. Right node must be dirtied too.

            if (mStore.markDirty(rightNode)) {
                parentNode.updateChildRefId(leftPos + 2, rightNode.mId);
            }

            // FIXME: testing
            if (leftNode.numKeys() == 1 || rightNode.numKeys() == 1) {
                System.out.println("left avail: " + leftAvail + ", right avail: " + rightAvail +
                                   ", left pos: " + leftPos);
                throw new Error("MUST REBALANCE: " + leftNode.numKeys() + ", " + 
                                rightNode.numKeys());
            }

            /*
            System.out.println("left avail: " + leftAvail + ", right avail: " + rightAvail +
                               ", left pos: " + leftPos + ", mode: " + migrateMode);
            */

            if (leftNode == node) {
                // Rebalance towards left node, which is smaller.
                // FIXME
            } else {
                // Rebalance towards right node, which is smaller.
                // FIXME
            }
        }

        // Tail call. I could just loop here, but this is simpler.
        mergeInternal(parentFrame, parentNode, leftNode, rightNode);
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
     * With parent held exclusively, returns child with exclusive latch held.
     */
    private TreeNode latchChild(TreeNode parent, int childPos, boolean releaseParent)
        throws IOException
    {
        TreeNode childNode = parent.mChildNodes[childPos >> 1];
        long childId = parent.retrieveChildRefId(childPos);

        check: if (childNode != null && childId == childNode.mId) {
            childNode.acquireExclusiveUnfair();

            // Need to check again in case evict snuck in.
            if (childId != childNode.mId) {
                childNode.releaseExclusive();
                break check;
            }

            if (releaseParent) {
                parent.releaseExclusive();
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
        if (releaseParent) {
            parent.releaseExclusive();
        }

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
