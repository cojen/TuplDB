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

import java.util.Arrays;
import java.util.Random;

import java.util.concurrent.locks.Lock;

import org.cojen.tupl.io.CauseCloseable;

import static org.cojen.tupl.PageOps.*;
import static org.cojen.tupl.Utils.*;

/**
 * Internal cursor implementation, which can be used by one thread at a time.
 *
 * @author Brian S O'Neill
 */
class TreeCursor implements CauseCloseable, Cursor {
    // Sign is important because values are passed to Node.retrieveKeyCmp
    // method. Bit 0 is set for inclusive variants and clear for exclusive.
    private static final int LIMIT_LE = 1, LIMIT_LT = 2, LIMIT_GE = -1, LIMIT_GT = -2;

    final Tree mTree;
    Transaction mTxn;

    // Top stack frame for cursor, always a leaf.
    private TreeCursorFrame mLeaf;

    byte[] mKey;
    byte[] mValue;

    boolean mKeyOnly;

    // Hashcode is defined by LockManager.
    private int mKeyHash;

    TreeCursor(Tree tree, Transaction txn) {
        tree.check(txn);
        mTree = tree;
        mTxn = txn;
    }

    TreeCursor(Tree tree) {
        mTree = tree;
    }

    @Override
    public final Ordering getOrdering() {
        return Ordering.ASCENDING;
    }

    @Override
    public final Transaction link(Transaction txn) {
        mTree.check(txn);
        Transaction old = mTxn;
        mTxn = txn;
        return old;
    }

    @Override
    public final Transaction link() {
        return mTxn;
    }

    @Override
    public final byte[] key() {
        return mKey;
    }

    @Override
    public final byte[] value() {
        return mValue;
    }

    @Override
    public final boolean autoload(boolean mode) {
        boolean old = mKeyOnly;
        mKeyOnly = !mode;
        return !old;
    }

    @Override
    public final boolean autoload() {
        return !mKeyOnly;
    }

    @Override
    public final int compareKeyTo(byte[] rkey) {
        byte[] lkey = mKey;
        return compareKeys(lkey, 0, lkey.length, rkey, 0, rkey.length);
    }

    @Override
    public final int compareKeyTo(byte[] rkey, int offset, int length) {
        byte[] lkey = mKey;
        return compareKeys(lkey, 0, lkey.length, rkey, offset, length);
    }

    protected final int keyHash() {
        int hash = mKeyHash;
        if (hash == 0) {
            mKeyHash = hash = LockManager.hash(mTree.mId, mKey);
        }
        return hash;
    }

    @Override
    public final LockResult first() throws IOException {
        Node root = mTree.mRoot;
        TreeCursorFrame frame = reset(root);

        if (!toFirst(root, frame)) {
            return LockResult.UNOWNED;
        }

        Transaction txn = mTxn;
        LockResult result = tryCopyCurrent(txn);

        if (result != null) {
            // Extra check for filtering ghosts.
            if (mKey == null || mValue != null) {
                return result;
            }
        } else if ((result = lockAndCopyIfExists(txn)) != null) {
            return result;
        }

        // If this point is reached, then entry was deleted after latch was
        // released. Move to next entry, which is consistent with findGe.
        // First means, "find greater than or equal to lowest possible key".
        return next();
    }

    /**
     * Moves the cursor to the first subtree entry. Leaf frame remains latched
     * when method returns normally.
     *
     * @param node latched node; can have no keys
     * @param frame frame to bind node to
     * @return false if nothing left
     */
    private boolean toFirst(Node node, TreeCursorFrame frame) throws IOException {
        try {
            while (true) {
                Split split = node.mSplit;
                if (split != null) {
                    node = split.latchLeft(node);
                }
                frame.bind(node, 0);
                if (node.isLeaf()) {
                    mLeaf = frame;
                    return node.hasKeys() ? true : toNext(frame);
                }
                node = latchChild(node, 0, true);
                frame = new TreeCursorFrame(frame);
            }
        } catch (Throwable e) {
            throw cleanup(e, frame);
        }
    }

    @Override
    public final LockResult last() throws IOException {
        Node root = mTree.mRoot;
        TreeCursorFrame frame = reset(root);

        if (!toLast(root, frame)) {
            return LockResult.UNOWNED;
        }

        Transaction txn = mTxn;
        LockResult result = tryCopyCurrent(txn);

        if (result != null) {
            // Extra check for filtering ghosts.
            if (mKey == null || mValue != null) {
                return result;
            }
        } else if ((result = lockAndCopyIfExists(txn)) != null) {
            return result;
        }

        // If this point is reached, then entry was deleted after latch was
        // released. Move to previous entry, which is consistent with findLe.
        // Last means, "find less than or equal to highest possible key".
        return previous();
    }

    /**
     * Moves the cursor to the last subtree entry. Leaf frame remains latched
     * when method returns normally.
     *
     * @param node latched node; can have no keys
     * @param frame frame to bind node to
     * @return false if nothing left
     */
    private boolean toLast(Node node, TreeCursorFrame frame) throws IOException {
        try {
            while (true) {
                Split split = node.mSplit;
                if (split != null) {
                    node = split.latchRight(node);
                }

                if (node.isLeaf()) {
                    // Note: Highest pos is -2 if leaf node has no keys.
                    int pos = node.highestLeafPos();
                    mLeaf = frame;
                    if (pos < 0) {
                        frame.bind(node, 0);
                        return toPrevious(frame);
                    } else {
                        frame.bind(node, pos);
                        return true;
                    }
                }

                // Note: Highest pos is 0 if internal node has no keys.
                int childPos = node.highestInternalPos();
                frame.bind(node, childPos);
                node = latchChild(node, childPos, true);

                frame = new TreeCursorFrame(frame);
            }
        } catch (Throwable e) {
            throw cleanup(e, frame);
        }
    }

    @Override
    public final LockResult skip(long amount) throws IOException {
        if (amount == 0) {
            Transaction txn = mTxn;
            if (txn != null && txn != Transaction.BOGUS) {
                byte[] key = mKey;
                if (key != null) {
                    return txn.mManager.check(txn, mTree.mId, key, keyHash());
                }
            }
            return LockResult.UNOWNED;
        }

        try {
            TreeCursorFrame frame = leafExclusiveNotSplit();
            if (amount > 0) {
                if (amount > 1 && (frame = skipNextGap(frame, amount - 1)) == null) {
                    return LockResult.UNOWNED;
                }
                return next(mTxn, frame);
            } else {
                if (amount < -1 && (frame = skipPreviousGap(frame, -1 - amount)) == null) {
                    return LockResult.UNOWNED;
                }
                return previous(mTxn, frame);
            }
        } catch (Throwable e) {
            throw handleException(e, false);
        }
    }

    @Override
    public final LockResult next() throws IOException {
        return next(mTxn, leafExclusiveNotSplit());
    }

    @Override
    public final LockResult nextLe(byte[] limitKey) throws IOException {
        return nextCmp(limitKey, LIMIT_LE);
    }

    @Override
    public final LockResult nextLt(byte[] limitKey) throws IOException {
        return nextCmp(limitKey, LIMIT_LT);
    }

    private LockResult nextCmp(byte[] limitKey, int limitMode) throws IOException {
        if (limitKey == null) {
            throw new NullPointerException("Key is null");
        }

        Transaction txn = mTxn;
        TreeCursorFrame frame = leafExclusiveNotSplit();

        while (true) {
            if (!toNext(frame)) {
                return LockResult.UNOWNED;
            }
            LockResult result = tryCopyCurrentCmp(txn, limitKey, limitMode);
            if (result != null) {
                // Extra check for filtering ghosts.
                if (mKey == null || mValue != null) {
                    return result;
                }
            } else if ((result = lockAndCopyIfExists(txn)) != null) {
                return result;
            }
            frame = leafExclusiveNotSplit();
        }
    }

    /**
     * Note: When method returns, frame is unlatched and may no longer be valid.
     *
     * @param frame leaf frame, not split, with exclusive latch
     */
    private LockResult next(Transaction txn, TreeCursorFrame frame) throws IOException {
        while (true) {
            if (!toNext(frame)) {
                return LockResult.UNOWNED;
            }
            LockResult result = tryCopyCurrent(txn);
            if (result != null) {
                // Extra check for filtering ghosts.
                if (mKey == null || mValue != null) {
                    return result;
                }
            } else if ((result = lockAndCopyIfExists(txn)) != null) {
                return result;
            }
            frame = leafExclusiveNotSplit();
        }
    }

    /**
     * Note: When method returns, frame is unlatched and may no longer be
     * valid. Leaf frame remains latched when method returns true.
     *
     * @param frame leaf frame, not split, with exclusive latch
     * @return false if nothing left
     */
    private boolean toNext(TreeCursorFrame frame) throws IOException {
        Node node = frame.mNode;

        quick: {
            int pos = frame.mNodePos;
            if (pos < 0) {
                pos = ~2 - pos; // eq: (~pos) - 2;
                if (pos >= node.highestLeafPos()) {
                    break quick;
                }
                frame.mNotFoundKey = null;
            } else if (pos >= node.highestLeafPos()) {
                break quick;
            }
            frame.mNodePos = pos + 2;
            return true;
        }

        while (true) {
            TreeCursorFrame parentFrame = frame.peek();

            if (parentFrame == null) {
                frame.popv();
                node.releaseExclusive();
                mLeaf = null;
                mKey = null;
                mKeyHash = 0;
                mValue = null;
                return false;
            }

            Node parentNode;
            int parentPos;

            latchParent: {
                splitCheck: {
                    // Latch coupling up the tree usually works, so give it a
                    // try. If it works, then there's no need to worry about a
                    // node merge.
                    parentNode = parentFrame.tryAcquireExclusive();

                    if (parentNode == null) {
                        // Latch coupling failed, and so acquire parent latch
                        // without holding child latch. The child might have
                        // changed, and so it must be checked again.
                        node.releaseExclusive();
                        parentNode = parentFrame.acquireExclusive();
                        if (parentNode.mSplit == null) {
                            break splitCheck;
                        }
                    } else {
                        if (parentNode.mSplit == null) {
                            frame.popv();
                            node.releaseExclusive();
                            parentPos = parentFrame.mNodePos;
                            break latchParent;
                        }
                        node.releaseExclusive();
                    }

                    // When this point is reached, parent node must be split.
                    // Parent latch is held, child latch is not held, but the
                    // frame is still valid.

                    parentNode = mTree.finishSplit(parentFrame, parentNode);
                }

                // When this point is reached, child must be relatched. Parent
                // latch is held, and the child frame is still valid.

                parentPos = parentFrame.mNodePos;
                node = latchChild(parentNode, parentPos, false);

                // Quick check again, in case node got bigger due to merging.
                // Unlike the earlier quick check, this one must handle
                // internal nodes too.
                quick: {
                    int pos = frame.mNodePos;

                    if (pos < 0) {
                        pos = ~2 - pos; // eq: (~pos) - 2;
                        if (pos >= node.highestLeafPos()) {
                            break quick;
                        }
                        frame.mNotFoundKey = null;
                    } else if (pos >= node.highestPos()) {
                        break quick;
                    }

                    parentNode.releaseExclusive();
                    frame.mNodePos = (pos += 2);

                    if (frame != mLeaf) {
                        return toFirst(latchChild(node, pos, true), new TreeCursorFrame(frame));
                    }

                    return true;
                }

                frame.popv();
                node.releaseExclusive();
            }

            // When this point is reached, only the parent latch is held. Child
            // frame is no longer valid.

            if (parentPos < parentNode.highestInternalPos()) {
                parentFrame.mNodePos = (parentPos += 2);
                // Recycle old frame.
                frame.mParentFrame = parentFrame;
                return toFirst(latchChild(parentNode, parentPos, true), frame);
            }

            frame = parentFrame;
            node = parentNode;
        }
    }

    /**
     * @param frame leaf frame, not split, with exclusive latch
     * @return latched leaf frame or null if reached end
     */
    private TreeCursorFrame skipNextGap(TreeCursorFrame frame, long amount) throws IOException {
        outer: while (true) {
            Node node = frame.mNode;

            quick: {
                int pos = frame.mNodePos;

                int highest;
                if (pos < 0) {
                    pos = ~2 - pos; // eq: (~pos) - 2;
                    if (pos >= (highest = node.highestLeafPos())) {
                        break quick;
                    }
                    frame.mNotFoundKey = null;
                } else if (pos >= (highest = node.highestLeafPos())) {
                    break quick;
                }

                int avail = (highest - pos) >> 1;
                if (avail >= amount) {
                    frame.mNodePos = pos + (((int) amount) << 1);
                    return frame;
                } else {
                    frame.mNodePos = highest;
                    amount -= avail;
                }
            }

            while (true) {
                TreeCursorFrame parentFrame = frame.peek();

                if (parentFrame == null) {
                    frame.popv();
                    node.releaseExclusive();
                    mLeaf = null;
                    mKey = null;
                    mKeyHash = 0;
                    mValue = null;
                    return null;
                }

                Node parentNode;
                int parentPos;

                latchParent: {
                    splitCheck: {
                        // Latch coupling up the tree usually works, so give it a
                        // try. If it works, then there's no need to worry about a
                        // node merge.
                        parentNode = parentFrame.tryAcquireExclusive();

                        if (parentNode == null) {
                            // Latch coupling failed, and so acquire parent latch
                            // without holding child latch. The child might have
                            // changed, and so it must be checked again.
                            node.releaseExclusive();
                            parentNode = parentFrame.acquireExclusive();
                            if (parentNode.mSplit == null) {
                                break splitCheck;
                            }
                        } else {
                            if (parentNode.mSplit == null) {
                                frame.popv();
                                node.releaseExclusive();
                                parentPos = parentFrame.mNodePos;
                                break latchParent;
                            }
                            node.releaseExclusive();
                        }

                        // When this point is reached, parent node must be split.
                        // Parent latch is held, child latch is not held, but the
                        // frame is still valid.

                        parentNode = mTree.finishSplit(parentFrame, parentNode);
                    }

                    // When this point is reached, child must be relatched. Parent
                    // latch is held, and the child frame is still valid.

                    parentPos = parentFrame.mNodePos;
                    node = latchChild(parentNode, parentPos, false);

                    // Quick check again, in case node got bigger due to merging.
                    // Unlike the earlier quick check, this one must handle
                    // internal nodes too.
                    quick: {
                        int pos = frame.mNodePos;

                        int highest;
                        if (pos < 0) {
                            pos = ~2 - pos; // eq: (~pos) - 2;
                            if (pos >= (highest = node.highestLeafPos())) {
                                break quick;
                            }
                            frame.mNotFoundKey = null;
                        } else if (pos >= (highest = node.highestPos())) {
                            break quick;
                        }

                        parentNode.releaseExclusive();

                        if (frame == mLeaf) {
                            int avail = (highest - pos) >> 1;
                            if (avail >= amount) {
                                frame.mNodePos = pos + (((int) amount) << 1);
                                return frame;
                            } else {
                                frame.mNodePos = highest;
                                amount -= avail;
                            }
                        }

                        // Increment position of internal node.
                        frame.mNodePos = (pos += 2);

                        if (!toFirst(latchChild(node, pos, true), new TreeCursorFrame(frame))) {
                            return null;
                        }
                        frame = mLeaf;
                        if (--amount <= 0) {
                            return frame;
                        }
                        continue outer;
                    }

                    frame.popv();
                    node.releaseExclusive();
                }

                // When this point is reached, only the parent latch is held. Child
                // frame is no longer valid.

                if (parentPos < parentNode.highestInternalPos()) {
                    parentFrame.mNodePos = (parentPos += 2);
                    // Recycle old frame.
                    frame.mParentFrame = parentFrame;
                    if (!toFirst(latchChild(parentNode, parentPos, true), frame)) {
                        return null;
                    }
                    frame = mLeaf;
                    if (--amount <= 0) {
                        return frame;
                    }
                    continue outer;
                }

                frame = parentFrame;
                node = parentNode;
            }
        }
    }

    @Override
    public final LockResult previous() throws IOException {
        return previous(mTxn, leafExclusiveNotSplit());
    }

    @Override
    public final LockResult previousGe(byte[] limitKey) throws IOException {
        return previousCmp(limitKey, LIMIT_GE);
    }

    @Override
    public final LockResult previousGt(byte[] limitKey) throws IOException {
        return previousCmp(limitKey, LIMIT_GT);
    }

    private LockResult previousCmp(byte[] limitKey, int limitMode) throws IOException {
        if (limitKey == null) {
            throw new NullPointerException("Key is null");
        }

        Transaction txn = mTxn;
        TreeCursorFrame frame = leafExclusiveNotSplit();

        while (true) {
            if (!toPrevious(frame)) {
                return LockResult.UNOWNED;
            }
            LockResult result = tryCopyCurrentCmp(txn, limitKey, limitMode);
            if (result != null) {
                // Extra check for filtering ghosts.
                if (mKey == null || mValue != null) {
                    return result;
                }
            } else if ((result = lockAndCopyIfExists(txn)) != null) {
                return result;
            }
            frame = leafExclusiveNotSplit();
        }
    }

    /**
     * Note: When method returns, frame is unlatched and may no longer be valid.
     *
     * @param frame leaf frame, not split, with exclusive latch
     */
    private LockResult previous(Transaction txn, TreeCursorFrame frame) throws IOException {
        while (true) {
            if (!toPrevious(frame)) {
                return LockResult.UNOWNED;
            }
            LockResult result = tryCopyCurrent(txn);
            if (result != null) {
                // Extra check for filtering ghosts.
                if (mKey == null || mValue != null) {
                    return result;
                }
            } else if ((result = lockAndCopyIfExists(txn)) != null) {
                return result;
            }
            frame = leafExclusiveNotSplit();
        }
    }

    /**
     * Note: When method returns, frame is unlatched and may no longer be
     * valid. Leaf frame remains latched when method returns true.
     *
     * @param frame leaf frame, not split, with exclusive latch
     * @return false if nothing left
     */
    private boolean toPrevious(TreeCursorFrame frame) throws IOException {
        Node node = frame.mNode;

        quick: {
            int pos = frame.mNodePos;
            if (pos < 0) {
                pos = ~pos;
                if (pos == 0) {
                    break quick;
                }
                frame.mNotFoundKey = null;
            } else if (pos == 0) {
                break quick;
            }
            frame.mNodePos = pos - 2;
            return true;
        }

        while (true) {
            TreeCursorFrame parentFrame = frame.peek();

            if (parentFrame == null) {
                frame.popv();
                node.releaseExclusive();
                mLeaf = null;
                mKey = null;
                mKeyHash = 0;
                mValue = null;
                return false;
            }

            Node parentNode;
            int parentPos;

            latchParent: {
                splitCheck: {
                    // Latch coupling up the tree usually works, so give it a
                    // try. If it works, then there's no need to worry about a
                    // node merge.
                    parentNode = parentFrame.tryAcquireExclusive();

                    if (parentNode == null) {
                        // Latch coupling failed, and so acquire parent latch
                        // without holding child latch. The child might have
                        // changed, and so it must be checked again.
                        node.releaseExclusive();
                        parentNode = parentFrame.acquireExclusive();
                        if (parentNode.mSplit == null) {
                            break splitCheck;
                        }
                    } else {
                        if (parentNode.mSplit == null) {
                            frame.popv();
                            node.releaseExclusive();
                            parentPos = parentFrame.mNodePos;
                            break latchParent;
                        }
                        node.releaseExclusive();
                    }

                    // When this point is reached, parent node must be split.
                    // Parent latch is held, child latch is not held, but the
                    // frame is still valid.

                    parentNode = mTree.finishSplit(parentFrame, parentNode);
                }

                // When this point is reached, child must be relatched. Parent
                // latch is held, and the child frame is still valid.

                parentPos = parentFrame.mNodePos;
                node = latchChild(parentNode, parentPos, false);

                // Quick check again, in case node got bigger due to merging.
                // Unlike the earlier quick check, this one must handle
                // internal nodes too.
                quick: {
                    int pos = frame.mNodePos;

                    if (pos < 0) {
                        pos = ~pos;
                        if (pos == 0) {
                            break quick;
                        }
                        frame.mNotFoundKey = null;
                    } else if (pos == 0) {
                        break quick;
                    }

                    parentNode.releaseExclusive();
                    frame.mNodePos = (pos -= 2);

                    if (frame != mLeaf) {
                        return toLast(latchChild(node, pos, true), new TreeCursorFrame(frame));
                    }

                    return true;
                }

                frame.popv();
                node.releaseExclusive();
            }

            // When this point is reached, only the parent latch is held. Child
            // frame is no longer valid.

            if (parentPos > 0) {
                parentFrame.mNodePos = (parentPos -= 2);
                // Recycle old frame.
                frame.mParentFrame = parentFrame;
                return toLast(latchChild(parentNode, parentPos, true), frame);
            }

            frame = parentFrame;
            node = parentNode;
        }
    }

    /**
     * @param frame leaf frame, not split, with exclusive latch
     * @return latched leaf frame or null if reached end
     */
    private TreeCursorFrame skipPreviousGap(TreeCursorFrame frame, long amount)
        throws IOException
    {
        outer: while (true) {
            Node node = frame.mNode;

            quick: {
                int pos = frame.mNodePos;

                if (pos < 0) {
                    pos = ~pos;
                    if (pos == 0) {
                        break quick;
                    }
                    frame.mNotFoundKey = null;
                } else if (pos == 0) {
                    break quick;
                }

                int avail = pos >> 1;
                if (avail >= amount) {
                    frame.mNodePos = pos - (((int) amount) << 1);
                    return frame;
                } else {
                    frame.mNodePos = 0;
                    amount -= avail;
                }
            }

            while (true) {
                TreeCursorFrame parentFrame = frame.peek();

                if (parentFrame == null) {
                    frame.popv();
                    node.releaseExclusive();
                    mLeaf = null;
                    mKey = null;
                    mKeyHash = 0;
                    mValue = null;
                    return null;
                }

                Node parentNode;
                int parentPos;

                latchParent: {
                    splitCheck: {
                        // Latch coupling up the tree usually works, so give it a
                        // try. If it works, then there's no need to worry about a
                        // node merge.
                        parentNode = parentFrame.tryAcquireExclusive();

                        if (parentNode == null) {
                            // Latch coupling failed, and so acquire parent latch
                            // without holding child latch. The child might have
                            // changed, and so it must be checked again.
                            node.releaseExclusive();
                            parentNode = parentFrame.acquireExclusive();
                            if (parentNode.mSplit == null) {
                                break splitCheck;
                            }
                        } else {
                            if (parentNode.mSplit == null) {
                                frame.popv();
                                node.releaseExclusive();
                                parentPos = parentFrame.mNodePos;
                                break latchParent;
                            }
                            node.releaseExclusive();
                        }

                        // When this point is reached, parent node must be split.
                        // Parent latch is held, child latch is not held, but the
                        // frame is still valid.

                        parentNode = mTree.finishSplit(parentFrame, parentNode);
                    }

                    // When this point is reached, child must be relatched. Parent
                    // latch is held, and the child frame is still valid.

                    parentPos = parentFrame.mNodePos;
                    node = latchChild(parentNode, parentPos, false);

                    // Quick check again, in case node got bigger due to merging.
                    // Unlike the earlier quick check, this one must handle
                    // internal nodes too.
                    quick: {
                        int pos = frame.mNodePos;

                        if (pos < 0) {
                            pos = ~pos;
                            if (pos == 0) {
                                break quick;
                            }
                            frame.mNotFoundKey = null;
                        } else if (pos == 0) {
                            break quick;
                        }

                        parentNode.releaseExclusive();

                        if (frame == mLeaf) {
                            int avail = pos >> 1;
                            if (avail >= amount) {
                                frame.mNodePos = pos - (((int) amount) << 1);
                                return frame;
                            } else {
                                frame.mNodePos = 0;
                                amount -= avail;
                            }
                        }

                        // Decrement position of internal node.
                        frame.mNodePos = (pos -= 2);

                        if (!toLast(latchChild(node, pos, true), new TreeCursorFrame(frame))) {
                            return null;
                        }
                        frame = mLeaf;
                        if (--amount <= 0) {
                            return frame;
                        }
                        continue outer;
                    }

                    frame.popv();
                    node.releaseExclusive();
                }

                // When this point is reached, only the parent latch is held. Child
                // frame is no longer valid.

                if (parentPos > 0) {
                    parentFrame.mNodePos = (parentPos -= 2);
                    // Recycle old frame.
                    frame.mParentFrame = parentFrame;
                    if (!toLast(latchChild(parentNode, parentPos, true), frame)) {
                        return null;
                    }
                    frame = mLeaf;
                    if (--amount <= 0) {
                        return frame;
                    }
                    continue outer;
                }

                frame = parentFrame;
                node = parentNode;
            }
        }
    }

    /**
     * Try to copy the current entry, locking it if required. Null is returned
     * if lock is not immediately available and only the key was copied. Node
     * latch is always released by this method, even if an exception is thrown.
     *
     * @return null, UNOWNED, INTERRUPTED, TIMED_OUT_LOCK, ACQUIRED,
     * OWNED_SHARED, OWNED_UPGRADABLE, or OWNED_EXCLUSIVE
     * @param txn optional
     */
    private LockResult tryCopyCurrent(Transaction txn) throws IOException {
        final Node node;
        final int pos;
        {
            TreeCursorFrame leaf = mLeaf;
            node = leaf.mNode;
            pos = leaf.mNodePos;
        }

        try {
            mKeyHash = 0;

            final LockMode mode;
            if (txn == null) {
                mode = LockMode.READ_COMMITTED;
            } else if ((mode = txn.lockMode()).noReadLock) {
                node.retrieveLeafEntry(pos, this);
                return LockResult.UNOWNED;
            }

            // Copy key for now, because lock might not be available. Value
            // might change after latch is released. Assign NOT_LOADED, in case
            // lock cannot be granted at all. This prevents uncommited value
            // from being exposed.
            mKey = node.retrieveKey(pos);
            mValue = NOT_LOADED;

            try {
                LockResult result;

                switch (mode) {
                default:
                    if (mTree.isLockAvailable(txn, mKey, keyHash())) {
                        // No need to acquire full lock.
                        mValue = mKeyOnly ? node.hasLeafValue(pos) : node.retrieveLeafValue(pos);
                        return LockResult.UNOWNED;
                    } else {
                        return null;
                    }

                case REPEATABLE_READ:
                    result = txn.tryLockShared(mTree.mId, mKey, keyHash(), 0L);
                    break;

                case UPGRADABLE_READ:
                    result = txn.tryLockUpgradable(mTree.mId, mKey, keyHash(), 0L);
                    break;
                }

                if (result.isHeld()) {
                    mValue = mKeyOnly ? node.hasLeafValue(pos) : node.retrieveLeafValue(pos);
                    return result;
                } else {
                    return null;
                }
            } catch (DeadlockException e) {
                // Not expected with timeout of zero anyhow.
                return null;
            }
        } finally {
            node.releaseExclusive();
        }
    }

    /**
     * Variant of tryCopyCurrent used by iteration methods which have a
     * limit. If limit is reached, cursor is reset and UNOWNED is returned.
     */
    private LockResult tryCopyCurrentCmp(Transaction txn, byte[] limitKey, int limitMode)
        throws IOException
    {
        try {
            return doTryCopyCurrentCmp(txn, limitKey, limitMode);
        } catch (Throwable e) {
            mLeaf.mNode.releaseExclusive();
            throw e;
        }
    }

    /**
     * Variant of tryCopyCurrent used by iteration methods which have a
     * limit. If limit is reached, cursor is reset and UNOWNED is returned.
     */
    private LockResult doTryCopyCurrentCmp(Transaction txn, byte[] limitKey, int limitMode)
        throws IOException
    {
        final Node node;
        final int pos;
        {
            TreeCursorFrame leaf = mLeaf;
            node = leaf.mNode;
            pos = leaf.mNodePos;
        }

        byte[] key = node.retrieveKeyCmp(pos, limitKey, limitMode);

        check: {
            if (key != null) {
                if (key != limitKey) {
                    mKey = key;
                    break check;
                } else if ((limitMode & 1) != 0) {
                    // Cursor contract does not claim ownership of limitKey instance.
                    mKey = key.clone();
                    break check;
                }
            }

            // Limit has been reached.
            node.releaseExclusive();
            reset();
            return LockResult.UNOWNED;
        }

        mKeyHash = 0;

        LockResult result;
        obtainResult: {
            final LockMode mode;
            if (txn == null) {
                mode = LockMode.READ_COMMITTED;
            } else if ((mode = txn.lockMode()).noReadLock) {
                mValue = mKeyOnly ? node.hasLeafValue(pos) : node.retrieveLeafValue(pos);
                result = LockResult.UNOWNED;
                break obtainResult;
            }

            mValue = NOT_LOADED;
        
            switch (mode) {
            default:
                if (mTree.isLockAvailable(txn, mKey, keyHash())) {
                    // No need to acquire full lock.
                    mValue = mKeyOnly ? node.hasLeafValue(pos) : node.retrieveLeafValue(pos);
                    result = LockResult.UNOWNED;
                } else {
                    result = null;
                }
                break obtainResult;

            case REPEATABLE_READ:
                result = txn.tryLockShared(mTree.mId, mKey, keyHash(), 0L);
                break;

            case UPGRADABLE_READ:
                result = txn.tryLockUpgradable(mTree.mId, mKey, keyHash(), 0L);
                break;
            }

            if (result.isHeld()) {
                mValue = mKeyOnly ? node.hasLeafValue(pos) : node.retrieveLeafValue(pos);
            } else {
                result = null;
            }
        }

        node.releaseExclusive();
        return result;
    }

    /**
     * With node latch not held, lock the current key. Returns the lock result
     * if entry exists, null otherwise. Method is intended to be called for
     * operations which move the position, and so it should not retain locks
     * for entries which were concurrently deleted. The find operation is
     * required to lock entries which don't exist.
     *
     * @param txn optional
     * @return null if current entry has been deleted
     */
    private LockResult lockAndCopyIfExists(Transaction txn) throws IOException {
        if (txn == null) {
            Locker locker = mTree.lockSharedLocal(mKey, keyHash());
            try {
                if (copyIfExists() != null) {
                    return LockResult.UNOWNED;
                }
            } finally {
                locker.unlock();
            }
        } else {
            LockResult result;

            switch (txn.lockMode()) {
                // Default case should only capture READ_COMMITTED, since the
                // no-lock modes were already handled.
            default:
                if ((result = txn.lockShared(mTree.mId, mKey, keyHash())) == LockResult.ACQUIRED) {
                    result = LockResult.UNOWNED;
                }
                break;

            case REPEATABLE_READ:
                result = txn.lockShared(mTree.mId, mKey, keyHash());
                break;

            case UPGRADABLE_READ:
                result = txn.lockUpgradable(mTree.mId, mKey, keyHash());
                break;
            }

            if (copyIfExists() != null) {
                if (result == LockResult.UNOWNED) {
                    txn.unlock();
                }
                return result;
            }

            if (result == LockResult.UNOWNED || result == LockResult.ACQUIRED) {
                txn.unlock();
            }
        }

        // Entry does not exist, and lock has been released if was just acquired.
        return null;
    }

    private byte[] copyIfExists() throws IOException {
        byte[] value;

        TreeCursorFrame frame = leafSharedNotSplit();
        Node node = frame.mNode;
        try {
            int pos = frame.mNodePos;
            if (pos < 0) {
                value = null;
            } else {
                value = mKeyOnly ? node.hasLeafValue(pos) : node.retrieveLeafValue(pos);
            }
        } finally {
            node.releaseShared();
        }

        mValue = value;
        return value;
    }

    /**
     * Checks validity of key, assigns key and hash code to cursor, and returns the linked
     * transaction.
     */
    private Transaction prepareFind(byte[] key) {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }
        Transaction txn = mTxn;
        int hash;
        selectHash: {
            if (txn != null) {
                LockMode mode = txn.lockMode();
                if (mode == LockMode.READ_UNCOMMITTED || mode == LockMode.UNSAFE) {
                    hash = 0;
                    break selectHash;
                }
            }
            hash = LockManager.hash(mTree.mId, key);
        }
        mKey = key;
        mKeyHash = hash;
        return txn;
    }

    /**
     * Assigns key and hash code to cursor, and returns the hash code.
     *
     * @param key must not be null
     * @return 0 if store operation does not acquire a lock
     */
    private int prepareFindForStore(Transaction txn, byte[] key) {
        int hash = (txn != null && txn.lockMode() == LockMode.UNSAFE) ? 0
            : LockManager.hash(mTree.mId, key);
        mKey = key;
        mKeyHash = hash;
        return hash;
    }

    private static final int
        VARIANT_REGULAR = 0,
        VARIANT_RETAIN  = 1, // retain node latch only if value is null
        VARIANT_NO_LOCK = 2, // retain node latch always, don't lock entry
        VARIANT_CHECK   = 3; // retain node latch always, don't lock entry, don't load entry

    @Override
    public final LockResult find(byte[] key) throws IOException {
        return find(prepareFind(key), key, VARIANT_REGULAR);
    }

    @Override
    public final LockResult findGe(byte[] key) throws IOException {
        // If isolation level is read committed, then key must be
        // locked. Otherwise, an uncommitted delete could be observed.
        Transaction txn = prepareFind(key);
        LockResult result = find(txn, key, VARIANT_RETAIN);
        if (mValue != null) {
            return result;
        } else {
            if (result == LockResult.ACQUIRED) {
                txn.unlock();
            }
            return next(txn, mLeaf);
        }
    }

    @Override
    public final LockResult findGt(byte[] key) throws IOException {
        // Never lock the requested key.
        findNoLock(key, VARIANT_CHECK);
        return next(mTxn, mLeaf);
    }

    @Override
    public final LockResult findLe(byte[] key) throws IOException {
        // If isolation level is read committed, then key must be
        // locked. Otherwise, an uncommitted delete could be observed.
        Transaction txn = prepareFind(key);
        LockResult result = find(txn, key, VARIANT_RETAIN);
        if (mValue != null) {
            return result;
        } else {
            if (result == LockResult.ACQUIRED) {
                txn.unlock();
            }
            return previous(txn, mLeaf);
        }
    }

    @Override
    public final LockResult findLt(byte[] key) throws IOException {
        // Never lock the requested key.
        findNoLock(key, VARIANT_CHECK);
        return previous(mTxn, mLeaf);
    }

    @Override
    public final LockResult findNearby(byte[] key) throws IOException {
        Transaction txn = prepareFind(key);

        Node node;
        TreeCursorFrame frame = mLeaf;
        if (frame == null) {
            // Allocate new frame before latching root -- allocation can block.
            frame = new TreeCursorFrame();
            node = mTree.mRoot;
            node.acquireExclusive();
        } else {
            node = frame.acquireExclusive();
            if (node.mSplit != null) {
                node = mTree.finishSplit(frame, node);
            }

            int startPos = frame.mNodePos;
            if (startPos < 0) {
                startPos = ~startPos;
            }

            int pos = node.binarySearch(key, startPos);

            if (pos >= 0) {
                frame.mNotFoundKey = null;
                frame.mNodePos = pos;
                try {
                    LockResult result = tryLockKey(txn);
                    if (result == null) {
                        mValue = NOT_LOADED;
                    } else {
                        try {
                            mValue = mKeyOnly ? node.hasLeafValue(pos)
                                : node.retrieveLeafValue(pos);
                            return result;
                        } catch (Throwable e) {
                            mValue = NOT_LOADED;
                            throw e;
                        }
                    }
                } finally {
                    node.releaseExclusive();
                }
                return doLoad(txn);
            } else if ((pos != ~0 || (node.mType & Node.LOW_EXTREMITY) != 0) &&
                       (~pos <= node.highestLeafPos() || (node.mType & Node.HIGH_EXTREMITY) != 0))
            {
                // Not found, but insertion pos is in bounds.
                frame.mNotFoundKey = key;
                frame.mNodePos = pos;
                LockResult result = tryLockKey(txn);
                if (result == null) {
                    mValue = NOT_LOADED;
                    node.releaseExclusive();
                } else {
                    mValue = null;
                    node.releaseExclusive();
                    return result;
                }
                return doLoad(txn);
            }

            // Cannot be certain if position is in leaf node, so pop up.

            mLeaf = null;

            while (true) {
                TreeCursorFrame parent = frame.pop();

                if (parent == null) {
                    // Usually the root frame refers to the root node, but it
                    // can be wrong if the tree height is changing.
                    Node root = mTree.mRoot;
                    if (node != root) {
                        node.releaseExclusive();
                        root.acquireExclusive();
                        node = root;
                    }
                    break;
                }

                node.releaseExclusive();
                frame = parent;
                node = frame.acquireExclusive();

                if (node.mSplit != null) {
                    node = mTree.finishSplit(frame, node);
                }

                try {
                    pos = Node.internalPos(node.binarySearch(key, frame.mNodePos));
                } catch (Throwable e) {
                    node.releaseExclusive();
                    throw cleanup(e, frame);
                }

                if ((pos == 0 && (node.mType & Node.LOW_EXTREMITY) == 0) ||
                    (pos >= node.highestInternalPos() && (node.mType & Node.HIGH_EXTREMITY) == 0))
                {
                    // Cannot be certain if position is in this node, so pop up.
                    continue;
                }

                frame.mNodePos = pos;
                try {
                    node = latchChild(node, pos, true);
                } catch (Throwable e) {
                    throw cleanup(e, frame);
                }
                frame = new TreeCursorFrame(frame);
                break;
            }
        }

        return find(txn, key, VARIANT_REGULAR, node, frame);
    }

    private void findNoLock(byte[] key, int variant) throws IOException {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }
        mKey = key;
        mKeyHash = 0;
        find(null, key, variant);
    }
    
    private LockResult find(Transaction txn, byte[] key, int variant) throws IOException {
        Node node = mTree.mRoot;
        return find(txn, key, variant, node, reset(node));
    }

    /**
     * @param node search node to start from
     * @param frame fresh frame for node
     */
    private LockResult find(Transaction txn, byte[] key, int variant,
                            Node node, TreeCursorFrame frame)
        throws IOException
    {
        while (true) {
            if (node.isLeaf()) {
                int pos;
                if (node.mSplit == null) {
                    try {
                        pos = node.binarySearch(key);
                    } catch (Throwable e) {
                        node.releaseExclusive();
                        throw cleanup(e, frame);
                    }
                    frame.bind(node, pos);
                } else {
                    try {
                        pos = node.mSplit.binarySearch(node, key);
                    } catch (Throwable e) {
                        node.releaseExclusive();
                        throw cleanup(e, frame);
                    }
                    frame.bind(node, pos);
                    if (pos < 0) {
                        // The finishSplit method will release the latch, and
                        // so the frame must be completely defined first.
                        frame.mNotFoundKey = key;
                    }
                    node = mTree.finishSplit(frame, node);
                    pos = frame.mNodePos;
                }

                mLeaf = frame;

                LockResult result;
                if (variant >= VARIANT_NO_LOCK) {
                    result = LockResult.UNOWNED;
                } else if ((result = tryLockKey(txn)) == null) {
                    // Unable to immediately acquire the lock.
                    if (pos < 0) {
                        frame.mNotFoundKey = key;
                    }
                    mValue = NOT_LOADED;
                    node.releaseExclusive();
                    // This might fail to acquire the lock too, but the cursor
                    // is at the proper position, and with the proper state.
                    return doLoad(txn);
                }

                if (pos < 0) {
                    frame.mNotFoundKey = key;
                    mValue = null;
                    if (variant < VARIANT_RETAIN) {
                        node.releaseExclusive();
                    }
                } else {
                    if (variant == VARIANT_CHECK) {
                        mValue = NOT_LOADED;
                    } else {
                        try {
                            mValue = mKeyOnly ? node.hasLeafValue(pos)
                                : node.retrieveLeafValue(pos);
                        } catch (Throwable e) {
                            mValue = NOT_LOADED;
                            node.releaseExclusive();
                            throw e;
                        }
                        if (variant < VARIANT_NO_LOCK) {
                            node.releaseExclusive();
                        }
                    }
                }
                return result;
            }

            Split split = node.mSplit;
            if (split == null) {
                int childPos;
                try {
                    childPos = Node.internalPos(node.binarySearch(key));
                } catch (Throwable e) {
                    node.releaseExclusive();
                    throw cleanup(e, frame);
                }
                frame.bind(node, childPos);
                try {
                    node = latchChild(node, childPos, true);
                } catch (Throwable e) {
                    throw cleanup(e, frame);
                }
            } else {
                // Follow search into split, binding this frame to the unsplit
                // node as if it had not split. The binding will be corrected
                // when split is finished.

                final Node sibling = split.latchSibling();

                final Node left, right;
                if (split.mSplitRight) {
                    left = node;
                    right = sibling;
                } else {
                    left = sibling;
                    right = node;
                }

                final Node selected;
                final int selectedPos;

                try {
                    if (split.compare(key) < 0) {
                        selected = left;
                        selectedPos = Node.internalPos(left.binarySearch(key));
                        frame.bind(node, selectedPos);
                        right.releaseExclusive();
                    } else {
                        selected = right;
                        selectedPos = Node.internalPos(right.binarySearch(key));
                        frame.bind(node, left.highestInternalPos() + 2 + selectedPos);
                        left.releaseExclusive();
                    }
                } catch (Throwable e) {
                    node.releaseExclusive();
                    sibling.releaseExclusive();
                    throw cleanup(e, frame);
                }

                try {
                    node = latchChild(selected, selectedPos, true);
                } catch (Throwable e) {
                    throw cleanup(e, frame);
                }
            }

            frame = new TreeCursorFrame(frame);
        }
    }

    /**
     * With node latched, try to lock the current key. Method expects mKeyHash
     * to be valid. Returns null if lock is required but not immediately available.
     *
     * @param txn can be null
     */
    private LockResult tryLockKey(Transaction txn) {
        LockMode mode;

        if (txn == null || (mode = txn.lockMode()) == LockMode.READ_COMMITTED) {
            // If lock is available, no need to acquire full lock and
            // immediately release it because node is latched.
            return mTree.isLockAvailable(txn, mKey, mKeyHash) ? LockResult.UNOWNED : null;
        }

        try {
            LockResult result;

            switch (mode) {
            default: // no read lock requested by READ_UNCOMMITTED or UNSAFE
                return LockResult.UNOWNED;

            case REPEATABLE_READ:
                result = txn.tryLockShared(mTree.mId, mKey, mKeyHash, 0L);
                break;

            case UPGRADABLE_READ:
                result = txn.tryLockUpgradable(mTree.mId, mKey, mKeyHash, 0L);
                break;
            }

            return result.isHeld() ? result : null;
        } catch (DeadlockException e) {
            // Not expected with timeout of zero anyhow.
            return null;
        }
    }

    @Override
    public final LockResult random(byte[] lowKey, byte[] highKey) throws IOException {
        Random rnd = Utils.random();

        start: while (true) {
            mKey = null;
            mKeyHash = 0;
            mValue = null;

            Node node = mTree.mRoot;
            TreeCursorFrame frame = reset(node);

            search: while (true) {
                if (node.mSplit != null) {
                    // Bind to anything to finish the split.
                    frame.bind(node, 0);
                    node = mTree.finishSplit(frame, node);
                }

                int pos;
                select: {
                    if (highKey == null) {
                        pos = node.highestPos() + 2;
                    } else {
                        try {
                            pos = node.binarySearch(highKey);
                        } catch (Throwable e) {
                            node.releaseExclusive();
                            throw cleanup(e, frame);
                        }
                        if (!node.isLeaf()) {
                            pos = Node.internalPos(pos);
                        } else if (pos < 0) {
                            pos = ~pos;
                        }
                    }

                    if (lowKey == null) {
                        if (pos > 0) {
                            pos = (pos == 2) ? 0 : (rnd.nextInt(pos >> 1) << 1);
                            break select;
                        }
                    } else {
                        int lowPos;
                        try {
                            lowPos = node.binarySearch(lowKey);
                        } catch (Throwable e) {
                            node.releaseExclusive();
                            throw cleanup(e, frame);
                        }
                        if (!node.isLeaf()) {
                            lowPos = Node.internalPos(lowPos);
                        } else if (lowPos < 0) {
                            lowPos = ~lowPos;
                        }
                        int range = pos - lowPos;
                        if (range > 0) {
                            pos = (range == 2) ? lowPos : lowPos + (rnd.nextInt(range >> 1) << 1);
                            break select;
                        }
                    }

                    // Node is empty or out of bounds, so pop up the tree.
                    TreeCursorFrame parent = frame.mParentFrame;
                    node.releaseExclusive();

                    if (parent == null) {
                        // Usually the root frame refers to the root node, but
                        // it can be wrong if the tree height is changing.
                        Node root = mTree.mRoot;
                        if (node == root) {
                            return LockResult.UNOWNED;
                        }
                        root.acquireExclusive();
                        node = root;
                    } else {
                        frame = parent;
                        node = frame.acquireExclusive();
                    }

                    continue search;
                }

                frame.bind(node, pos);

                if (node.isLeaf()) {
                    mLeaf = frame;
                    Transaction txn;
                    try {
                        txn = prepareFind(node.retrieveKey(pos));
                    } catch (Throwable e) {
                        resetLatched(node);
                        throw e;
                    }

                    LockResult result;
                    if ((result = tryLockKey(txn)) == null) {
                        // Unable to immediately acquire the lock.
                        mValue = NOT_LOADED;
                        node.releaseExclusive();
                        // This might fail to acquire the lock too, but the cursor
                        // is at the proper position, and with the proper state.
                        result = doLoad(txn);
                    } else {
                        try {
                            mValue = mKeyOnly ? node.hasLeafValue(pos)
                                : node.retrieveLeafValue(pos);
                        } catch (Throwable e) {
                            mValue = NOT_LOADED;
                            node.releaseExclusive();
                            throw e;
                        }
                        node.releaseExclusive();
                    }

                    if (mValue == null) {
                        // Skip over ghosts. Attempting to lock ghosts in the
                        // first place is correct behavior, avoiding bias.
                        if (result == LockResult.ACQUIRED) {
                            txn.unlock();
                        }
                        frame = leafExclusiveNotSplit();
                        result = rnd.nextBoolean() ? next(txn, frame) : previous(txn, frame);
                        if (mValue == null) {
                            // Nothing but ghosts in selected direction, so start over.
                            continue start;
                        }
                    }

                    return result;
                } else {
                    try {
                        node = latchChild(node, pos, true);
                    } catch (Throwable e) {
                        throw cleanup(e, frame);
                    }
                }

                frame = new TreeCursorFrame(frame);
            }
        }
    }

    @Override
    public final LockResult load() throws IOException {
        // This will always acquire a lock if required to. A try-lock pattern
        // can skip the lock acquisition in certain cases, but the optimization
        // doesn't seem worth the trouble.
        try {
            return doLoad(mTxn);
        } catch (LockFailureException e) {
            mValue = NOT_LOADED;
            throw e;
        }
    }

    /**
     * Must be called with node latch not held.
     */
    private LockResult doLoad(Transaction txn) throws IOException {
        byte[] key = mKey;
        if (key == null) {
            throw new IllegalStateException("Cursor position is undefined");
        }

        LockResult result;
        Locker locker;

        if (txn == null) {
            result = LockResult.UNOWNED;
            locker = mTree.lockSharedLocal(key, keyHash());
        } else {
            switch (txn.lockMode()) {
            default: // no read lock requested by READ_UNCOMMITTED or UNSAFE
                result = LockResult.UNOWNED;
                locker = null;
                break;

            case READ_COMMITTED:
                if ((result = txn.lockShared(mTree.mId, key, keyHash())) == LockResult.ACQUIRED) {
                    result = LockResult.UNOWNED;
                    locker = txn;
                } else {
                    locker = null;
                }
                break;

            case REPEATABLE_READ:
                result = txn.lockShared(mTree.mId, key, keyHash());
                locker = null;
                break;

            case UPGRADABLE_READ:
                result = txn.lockUpgradable(mTree.mId, key, keyHash());
                locker = null;
                break;
            }
        }

        try {
            TreeCursorFrame frame = leafSharedNotSplit();
            Node node = frame.mNode;
            try {
                int pos = frame.mNodePos;
                mValue = pos >= 0 ? node.retrieveLeafValue(pos) : null;
            } finally {
                node.releaseShared();
            }
            return result;
        } finally {
            if (locker != null) {
                locker.unlock();
            }
        }
    }

    @Override
    public void store(byte[] value) throws IOException {
        byte[] key = mKey;
        if (key == null) {
            throw new IllegalStateException("Cursor position is undefined");
        }

        try {
            final Transaction txn = mTxn;
            final Locker locker = mTree.lockExclusive(txn, key, keyHash());
            try {
                store(txn, leafExclusive(), value, false);
            } finally {
                if (locker != null) {
                    locker.unlock();
                }
            }
        } catch (Throwable e) {
            throw handleException(e, false);
        }
    }

    /**
     * Atomic find and store operation. Cursor is reset as a side-effect.
     *
     * @param key must not be null
     */
    final byte[] findAndStore(byte[] key, byte[] value) throws IOException {
        try {
            final Transaction txn = mTxn;
            final int hash = prepareFindForStore(txn, key);
            final Locker locker = mTree.lockExclusive(txn, key, hash);
            try {
                // Find with no lock because it has already been acquired.
                find(null, key, VARIANT_NO_LOCK);
                byte[] oldValue = mValue;
                store(txn, mLeaf, value, true);
                return oldValue;
            } finally {
                if (locker != null) {
                    locker.unlock();
                }
            }
        } catch (Throwable e) {
            throw handleException(e, true);
        }
    }

    static final byte[] MODIFY_INSERT = new byte[0], MODIFY_REPLACE = new byte[0];

    /**
     * Atomic find and modify operation. Cursor is reset as a side-effect.
     *
     * @param key must not be null
     * @param oldValue MODIFY_INSERT, MODIFY_REPLACE, else update mode
     */
    final boolean findAndModify(byte[] key, byte[] oldValue, byte[] newValue) throws IOException {
        final Transaction txn = mTxn;

        try {
            // Note: Acquire exclusive lock instead of performing upgrade
            // sequence. The upgrade would need to be performed with the node
            // latch held, which is deadlock prone.

            if (txn == null) {
                int hash = LockManager.hash(mTree.mId, key);
                mKey = key;
                mKeyHash = hash;
                Locker locker = mTree.lockExclusiveLocal(key, hash);
                try {
                    return doFindAndModify(null, key, oldValue, newValue);
                } finally {
                    locker.unlock();
                }
            }

            int hash = prepareFindForStore(txn, key);
            LockResult result;

            LockMode mode = txn.lockMode();
            if (mode == LockMode.UNSAFE) {
                // Indicate that no unlock should be performed.
                result = LockResult.OWNED_EXCLUSIVE;
            } else {
                result = txn.lockExclusive(mTree.mId, key, hash);
                if (result == LockResult.ACQUIRED && mode.repeatable) {
                    // Downgrade to upgradable when no modification is made, to
                    // preserve repeatable semantics and allow upgrade later.
                    result = LockResult.UPGRADED;
                }
            }

            try {
                if (doFindAndModify(txn, key, oldValue, newValue)) {
                    return true;
                }
            } catch (Throwable e) {
                if (result == LockResult.ACQUIRED) {
                    txn.unlock();
                } else if (result == LockResult.UPGRADED) {
                    txn.unlockToUpgradable();
                }
                throw e;
            }

            if (result == LockResult.ACQUIRED) {
                txn.unlock();
            } else if (result == LockResult.UPGRADED) {
                txn.unlockToUpgradable();
            }

            return false;
        } catch (Throwable e) {
            throw handleException(e, true);
        }
    }

    private boolean doFindAndModify(Transaction txn, byte[] key, byte[] oldValue, byte[] newValue)
        throws IOException
    {
        // Find with no lock because caller must already acquire exclusive lock.
        find(null, key, VARIANT_NO_LOCK);

        check: {
            if (oldValue == MODIFY_INSERT) {
                if (mValue == null) {
                    // Insert allowed.
                    break check;
                }
            } else if (oldValue == MODIFY_REPLACE) {
                if (mValue != null) {
                    // Replace allowed.
                    break check;
                }
            } else {
                if (mValue != null) {
                    if (Arrays.equals(oldValue, mValue)) {
                        // Update allowed.
                        break check;
                    }
                } else if (oldValue == null) {
                    if (newValue == null) {
                        // Update allowed, but nothing changed.
                        resetLatched(mLeaf.mNode);
                        return true;
                    } else {
                        // Update allowed.
                        break check;
                    }
                }
            }

            resetLatched(mLeaf.mNode);
            return false;
        }

        store(txn, mLeaf, newValue, true);
        return true;
    }

    /**
     * Non-transactional ghost delete. Caller is expected to hold exclusive key lock. Method
     * does nothing if a value exists. Cursor is always reset as a side-effect.
     *
     * @return false if Tree is closed
     */
    final boolean deleteGhost(byte[] key) throws IOException {
        try {
            // Find with no lock because it has already been acquired.
            // TODO: Use nearby optimization when used with transactional Index.clear.
            find(null, key, VARIANT_NO_LOCK);

            TreeCursorFrame leaf = mLeaf;
            if (leaf.mNode.mPage == p_empty()) {
                resetLatched(leaf.mNode);
                return false;
            }

            if (mValue == null) {
                mKey = key;
                mKeyHash = 0;
                store(Transaction.BOGUS, leaf, null, true);
            } else {
                resetLatched(leaf.mNode);
            }

            return true;
        } catch (Throwable e) {
            throw handleException(e, true);
        }
    }

    /**
     * Shared commit lock is acquired, to prevent checkpoints from observing in-progress
     * splits.
     *
     * @param leaf leaf frame, latched exclusively, which is always released by this method
     * @param reset true to reset cursor when finished
     */
    protected final void store(final Transaction txn, final TreeCursorFrame leaf,
                               final byte[] value, final boolean reset)
        throws IOException
    {
        byte[] key = mKey;
        Node node;
        long commitPos = 0;

        if (value == null) doDelete: {
            // Delete entry...

            if (leaf.mNodePos < 0) {
                // Entry doesn't exist, so nothing to do.
                node = leaf.mNode;
                break doDelete;
            }

            final Lock sharedCommitLock = sharedCommitLock(leaf);
            try {
                // Releases latch if an exception is thrown.
                node = notSplitDirty(leaf);
                final int pos = leaf.mNodePos;

                try {
                    if (txn == null) {
                        commitPos = mTree.redoStore(key, null);
                    } else if (txn.lockMode() != LockMode.UNSAFE) {
                        node.txnDeleteLeafEntry(txn, mTree, key, keyHash(), pos);
                        // Above operation leaves a ghost, so no cursors to fix.
                        break doDelete;
                    } else if (txn.mDurabilityMode != DurabilityMode.NO_REDO) {
                        commitPos = mTree.redoStoreNoLock(key, null);
                    }

                    node.deleteLeafEntry(pos);
                } catch (Throwable e) {
                    node.releaseExclusive();
                    throw e;
                }

                int newPos = ~pos;
                leaf.mNodePos = newPos;
                leaf.mNotFoundKey = key;

                // Fix all cursors bound to the node.
                TreeCursorFrame frame = node.mLastCursorFrame;
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
                    // Always released by mergeLeaf.
                    node = null;
                }
            } finally {
                sharedCommitLock.unlock();
            }
        } else {
            final Lock sharedCommitLock = sharedCommitLock(leaf);
            try {
                // Update and insert always dirty the node. Releases latch if an exception is
                // thrown.
                node = notSplitDirty(leaf);
                final int pos = leaf.mNodePos;

                if (pos >= 0) {
                    // Update entry...

                    try {
                        if (txn == null) {
                            commitPos = mTree.redoStore(key, value);
                        } else if (txn.lockMode() != LockMode.UNSAFE) {
                            node.txnPreUpdateLeafEntry(txn, mTree, key, pos);
                            if (txn.mDurabilityMode != DurabilityMode.NO_REDO) {
                                txn.redoStore(mTree.mId, key, value);
                            }
                        } else if (txn.mDurabilityMode != DurabilityMode.NO_REDO) {
                            commitPos = mTree.redoStoreNoLock(key, value);
                        }

                        node.updateLeafValue(mTree, pos, 0, value);
                    } catch (Throwable e) {
                        node.releaseExclusive();
                        throw e;
                    }

                    if (node.shouldLeafMerge()) {
                        mergeLeaf(leaf, node);
                        // Always released by mergeLeaf.
                        node = null;
                    } else {
                        if (node.mSplit != null) {
                            // Releases latch if an exception is thrown.
                            node = mTree.finishSplit(leaf, node);
                        }
                    }
                } else {
                    // Insert entry...

                    try {
                        if (txn == null) {
                            commitPos = mTree.redoStore(key, value);
                        } else if (txn.lockMode() != LockMode.UNSAFE) {
                            txn.pushUninsert(mTree.mId, key);
                            if (txn.mDurabilityMode != DurabilityMode.NO_REDO) {
                                txn.redoStore(mTree.mId, key, value);
                            }
                        } else if (txn.mDurabilityMode != DurabilityMode.NO_REDO) {
                            commitPos = mTree.redoStoreNoLock(key, value);
                        }

                        node.insertLeafEntry(mTree, ~pos, key, value);
                    } catch (Throwable e) {
                        node.releaseExclusive();
                        throw e;
                    }

                    // Releases latch if an exception is thrown.
                    node = postInsert(leaf, node, key);
                }
            } finally {
                sharedCommitLock.unlock();
            }
        }

        if (reset) {
            if (node == null) {
                reset();
            } else {
                resetLatched(node);
            }
        } else if (node != null) {
            node.releaseExclusive();
            mValue = value;
        }

        if (commitPos != 0) {
            // Wait for commit sync without holding commit lock and node latch.
            mTree.txnCommitSync(txn, commitPos);
        }
    }

    /**
     * Fixes this and all bound cursors after an insert.
     *
     * @param leaf latched leaf frame; released if an exception is thrown
     * @return replacement node
     */
    private Node postInsert(TreeCursorFrame leaf, Node node, byte[] key) throws IOException {
        int pos = leaf.mNodePos;
        int newPos = ~pos;

        leaf.mNodePos = newPos;
        leaf.mNotFoundKey = null;

        // Fix all cursors bound to the node.
        TreeCursorFrame frame = node.mLastCursorFrame;
        do {
            if (frame == leaf) {
                // Don't need to fix self.
                continue;
            }

            int framePos = frame.mNodePos;

            if (framePos == pos) {
                // Other cursor is at same not-found position as this one was. If keys are the
                // same, then other cursor switches to a found state as well. If key is
                // greater, then position needs to be updated.

                byte[] frameKey = frame.mNotFoundKey;
                int compare = compareKeys(frameKey, 0, frameKey.length, key, 0, key.length);
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
            node = mTree.finishSplit(leaf, node);
        }

        return node;
    }

    /**
     * Non-transactional store of a fragmented value as an undo action. Cursor value is
     * NOT_LOADED as a side-effect.
     */
    final void storeFragmented(byte[] value) throws IOException {
        if (mKey == null) {
            throw new IllegalStateException("Cursor position is undefined");
        }
        if (value == null) {
            throw new IllegalArgumentException("Value is null");
        }

        final TreeCursorFrame leaf = leafExclusive();

        final Lock sharedCommitLock = sharedCommitLock(leaf);
        try {
            Node node = notSplitDirty(leaf);

            final int pos = leaf.mNodePos;
            if (pos >= 0) {
                try {
                    node.updateLeafValue(mTree, pos, Node.ENTRY_FRAGMENTED, value);
                } catch (Throwable e) {
                    node.releaseExclusive();
                    throw e;
                }
                if (node.mSplit != null) {
                    // Releases latch if an exception is thrown.
                    node = mTree.finishSplit(leaf, node);
                }
            } else {
                // This case is possible when entry was deleted concurrently without a lock.
                byte[] key = mKey;
                try {
                    node.insertFragmentedLeafEntry(mTree, ~pos, key, value);
                } catch (Throwable e) {
                    node.releaseExclusive();
                    throw e;
                }
                // Releases latch if an exception is thrown.
                node = postInsert(leaf, node, key);
            }

            mValue = NOT_LOADED;

            node.releaseExclusive();
        } catch (Throwable e) {
            throw handleException(e, false);
        } finally {
            sharedCommitLock.unlock();
        }
    }

    /**
     * Non-transactional insert of a blank value. Caller must hold shared commmit lock and
     * have verified that insert is a valid operation.
     *
     * @param leaf leaf frame, latched exclusively, which is released by this
     * method if an exception is thrown
     * @param node frame node, not split, dirtied
     * @param vlength length of blank value
     * @return replacement node, latched
     */
    final Node insertBlank(TreeCursorFrame leaf, Node node, long vlength) throws IOException {
        byte[] key = mKey;
        try {
            node.insertBlankLeafEntry(mTree, ~leaf.mNodePos, key, vlength);
        } catch (Throwable e) {
            node.releaseExclusive();
            throw e;
        }
        // Releases latch if an exception is thrown.
        return postInsert(leaf, node, key);
    }

    /**
     * Non-transactionally deletes the lowest entry and moves to the next entry. This cursor
     * must be positioned at the lowest entry, and no other cursors or threads can be active in
     * the tree.
     */
    final void trim() throws IOException {
        final TreeCursorFrame leaf = leafExclusive();

        final Lock sharedCommitLock = sharedCommitLock(leaf);
        try {
            // Releases latch if an exception is thrown.
            Node node = notSplitDirty(leaf);

            try {
                node.deleteLeafEntry(0);
            } catch (Throwable e) {
                node.releaseExclusive();
                throw e;
            }

            if (node.hasKeys()) {
                leaf.mNodePos = ~0;
            } else {
                node = trimNode(leaf, node);

                if (node == null) {
                    mLeaf = null;
                    reset();
                    return;
                }

                try {
                    mKeyHash = 0;
                    node.retrieveLeafEntry(0, this);
                    // Extra check for filtering ghosts.
                    if (mValue != null) {
                        return;
                    }
                } finally {
                    node.releaseExclusive();
                }
            }
        } finally {
            sharedCommitLock.unlock();
        }

        next(Transaction.BOGUS, leaf);
    }

    /**
     * @param frame node frame
     * @param node latched node, with no keys, and dirty; released by this method
     * @return replacement node, latched exclusively; null if tree is empty
     */
    private Node trimNode(final TreeCursorFrame frame, final Node node) throws IOException {
        node.mLastCursorFrame = null;

        Database db = mTree.mDatabase;
        // Always prepare to delete, even though caller will delete the root.
        db.prepareToDelete(node);

        if (node == mTree.mRoot) {
            try {
                node.asTrimmedRoot();
            } finally {
                node.releaseExclusive();
            }
            return null;
        }

        TreeCursorFrame parentFrame = frame.mParentFrame;
        Node parentNode = parentFrame.acquireExclusive();

        if (parentNode.hasKeys()) {
            parentNode.deleteLeftChildRef(0);
        } else {
            parentNode = trimNode(parentFrame, parentNode);
            if (parentNode == null) {
                db.deleteNode(node);
                return null;
            }
        }

        Node next = latchChild(parentNode, 0, false);

        try {
            if (db.markDirty(mTree, next)) {
                parentNode.updateChildRefId(0, next.mId);
            }
        } finally {
            parentNode.releaseExclusive();
        }

        frame.mNode = next;
        frame.mNodePos = 0;
        next.mLastCursorFrame = frame;
        next.mType |= Node.LOW_EXTREMITY;

        db.deleteNode(node);

        return next;
    }

    /**
     * Safely acquire shared commit lock while node latch is held exclusively. Latch might need
     * to be released and relatched in order to obtain shared commit lock without deadlocking.
     * As a result, the caller must not rely on any existing node reference. It must be
     * accessed again from the leaf frame instance.
     *
     * @param leaf leaf frame, latched exclusively, which might be released and relatched
     * @return held sharedCommitLock
     */
    final Lock sharedCommitLock(final TreeCursorFrame leaf) {
        Lock sharedCommitLock = mTree.mDatabase.sharedCommitLock();
        if (!sharedCommitLock.tryLock()) {
            leaf.mNode.releaseExclusive();
            sharedCommitLock.lock();
            leaf.acquireExclusive();
        }
        return sharedCommitLock;
    }

    protected final IOException handleException(Throwable e, boolean reset) throws IOException {
        if (mLeaf == null && e instanceof IllegalStateException) {
            // Exception is caused by cursor state; store is safe.
            if (reset) {
                reset();
            }
            throw (IllegalStateException) e;
        }

        if (e instanceof DatabaseException) {
            DatabaseException de = (DatabaseException) e;
            if (de.isRecoverable()) {
                if (reset) {
                    reset();
                }
                throw de;
            }
        }

        // Any unexpected exception can corrupt the internal store state. Closing down
        // protects the persisted state.

        try {
            throw closeOnFailure(mTree.mDatabase, e);
        } finally {
            reset();
        }
    }

    @Override
    public final Stream newStream() {
        TreeCursor copy = copyNoValue();
        copy.mKeyOnly = true;
        return new TreeValueStream(copy);
    }

    @Override
    public final TreeCursor copy() {
        TreeCursor copy = copyNoValue();
        if (!(copy.mKeyOnly = mKeyOnly)) {
            copy.mValue = cloneArray(mValue);
        }
        return copy;
    }

    private TreeCursor copyNoValue() {
        TreeCursor copy = new TreeCursor(mTree, mTxn);
        TreeCursorFrame frame = mLeaf;
        if (frame != null) {
            TreeCursorFrame frameCopy = new TreeCursorFrame();
            frame.copyInto(frameCopy);
            copy.mLeaf = frameCopy;
        }
        copy.mKey = mKey;
        copy.mKeyHash = mKeyHash;
        return copy;
    }

    @Override
    public final void reset() {
        TreeCursorFrame frame = mLeaf;
        mLeaf = null;
        mKey = null;
        mKeyHash = 0;
        mValue = null;
        if (frame != null) {
            TreeCursorFrame.popAll(frame);
        }
    }

    /**
     * Reset with leaf already latched exclusively.
     */
    private void resetLatched(Node node) {
        TreeCursorFrame frame = mLeaf;
        mLeaf = null;
        mKey = null;
        mKeyHash = 0;
        mValue = null;
        frame = frame.pop();
        node.releaseExclusive();
        if (frame != null) {
            TreeCursorFrame.popAll(frame);
        }
    }

    /**
     * Called if an exception is thrown while frames are being constructed.
     * Given frame does not need to be bound, but it must not be latched.
     */
    private RuntimeException cleanup(Throwable e, TreeCursorFrame frame) {
        mLeaf = frame;
        reset();
        return rethrow(e);
    }

    @Override
    public final void close() {
        reset();
    }

    @Override
    public final void close(Throwable cause) {
        try {
            if (cause instanceof DatabaseException) {
                DatabaseException de = (DatabaseException) cause;
                if (de.isRecoverable()) {
                    return;
                }
            }
            throw closeOnFailure(mTree.mDatabase, cause);
        } catch (IOException e) {
            // Ignore.
        } finally {
            reset();
        }
    }

    /**
     * Resets all frames and latches root node, exclusively. Although the
     * normal reset could be called directly, this variant avoids unlatching
     * the root node, since a find operation would immediately relatch it.
     *
     * @return new or recycled frame
     */
    private TreeCursorFrame reset(Node root) {
        TreeCursorFrame frame = mLeaf;
        if (frame == null) {
            // Allocate new frame before latching root -- allocation can block.
            frame = new TreeCursorFrame();
            root.acquireExclusive();
            return frame;
        }

        mLeaf = null;

        while (true) {
            Node node = frame.acquireExclusive();
            TreeCursorFrame parent = frame.pop();

            if (parent == null) {
                // Usually the root frame refers to the root node, but it
                // can be wrong if the tree height is changing.
                if (node != root) {
                    node.releaseExclusive();
                    root.acquireExclusive();
                }
                return frame;
            }

            node.releaseExclusive();
            frame = parent;
        }
    }

    final int height() {
        int height = 0;
        TreeCursorFrame frame = mLeaf;
        while (frame != null) {
            height++;
            frame = frame.mParentFrame;
        }
        return height;
    }

    /**
     * Move to the next tree node, loading it if necessary.
     */
    final void nextNode() throws IOException {
        // Move to next node by first setting current node position higher than possible.
        mLeaf.mNodePos = Integer.MAX_VALUE - 1;
        // FIXME: skips nodes that are full of ghosts
        next();
    }

    /**
     * Used by file compaction mode. Compacts from the current node to the last, unless stopped
     * by observer or aborted. Caller must issue a checkpoint after entering compaction mode
     * and before calling this method. This ensures completion of any splits in progress before
     * compaction began. Any new splits will be created during compaction and so they will
     * allocate pages outside of the compaction zone.
     *
     * @param highestNodeId defines the highest node before the compaction zone; anything
     * higher is in the compaction zone
     * @return false if compaction should stop
     */
    final boolean compact(long highestNodeId, CompactionObserver observer) throws IOException {
        int height = height();

        // Reference to frame nodes, to detect when cursor has moved past a node. Level 0
        // represents the leaf node. Detection may also be triggered by concurrent
        // modifications to the tree, but this is not harmful.
        Node[] frameNodes = new Node[height];

        TreeCursorFrame frame = mLeaf;

        outer: while (true) {
            for (int level = 0; level < height; level++) {
                Node node = frame.acquireShared();
                if (frameNodes[level] == node) {
                    // No point in checking upwards if this level is unchanged.
                    node.releaseShared();
                    break;
                } else {
                    frameNodes[level] = node;
                    long id = compactFrame(highestNodeId, frame, node);
                    if (id > highestNodeId) {
                        // Abort compaction.
                        return false;
                    }
                    try {
                        if (!observer.indexNodeVisited(id)) {
                            return false;
                        }
                    } catch (Throwable e) {
                        uncaught(e);
                        return false;
                    }
                }
                frame = frame.mParentFrame;
            }

            // Search leaf for fragmented values.

            frame = leafSharedNotSplit();
            Node node = frame.mNode;

            // Quick check avoids excessive node re-latching.
            quick: {
                final int end = node.highestLeafPos();
                int pos = frame.mNodePos;
                if (pos < 0) {
                    pos = ~pos;
                }
                for (; pos <= end; pos += 2) {
                    if (node.isFragmentedLeafValue(pos)) {
                        // Found one, so abort the quick check.
                        break quick;
                    }
                }
                // No fragmented values found.
                node.releaseShared();
                nextNode();
                if ((frame = mLeaf) == null) {
                    // No more entries to examine.
                    return true;
                }
                continue outer;
            }

            while (true) {
                try {
                    int nodePos = frame.mNodePos;
                    if (nodePos >= 0 && node.isFragmentedLeafValue(nodePos)) {
                        int pLen = p_length(node.mPage);
                        TreeValueStream stream = new TreeValueStream(this);
                        long pos = 0;
                        while (true) {
                            int result = stream.compactCheck(frame, pos, highestNodeId);
                            if (result < 0) {
                                break;
                            }
                            if (result > 0) {
                                node.releaseShared();
                                node = null;
                                stream.doWrite(pos, TreeValueStream.TOUCH_VALUE, 0, 0);
                                frame = leafSharedNotSplit();
                                node = frame.mNode;
                                if (node.mId > highestNodeId) {
                                    // Abort compaction.
                                    return false;
                                }
                            }
                            pos += pLen;
                        }
                    }
                } finally {
                    if (node != null) {
                        node.releaseShared();
                    }
                }

                next();

                if (mLeaf == null) {
                    // No more entries to examine.
                    return true;
                }

                frame = leafSharedNotSplit();
                Node next = frame.mNode;

                if (next != node) {
                    next.releaseShared();
                    break;
                }
            }
        }
    }

    /**
     * Moves the frame's node out of the compaction zone if necessary.
     *
     * @param frame frame with shared latch held; always released as a side-effect
     * @return new node id; caller must check if it is outside compaction zone and abort
     */
    private long compactFrame(long highestNodeId, TreeCursorFrame frame, Node node)
        throws IOException
    {
        long id = node.mId;
        node.releaseShared();

        if (id > highestNodeId) {
            Database db = mTree.mDatabase;
            Lock sharedCommitLock = db.sharedCommitLock();
            sharedCommitLock.lock();
            try {
                node = frame.acquireExclusive();
                id = node.mId;
                if (id > highestNodeId) {
                    // Marking as dirty forces an allocation, which should be outside the
                    // compaction zone.
                    node = notSplitDirty(frame);
                    id = node.mId;
                }
                node.releaseExclusive();
            } finally {
                sharedCommitLock.unlock();
            }
        }

        return id;
    }

    /**
     * Test method which confirms that the given cursor is positioned exactly the same as this
     * one.
     */
    public final boolean equalPositions(TreeCursor other) {
        if (this == other) {
            return true;
        }

        TreeCursorFrame thisFrame = mLeaf;
        TreeCursorFrame otherFrame = other.mLeaf;
        while (true) {
            if (thisFrame == null) {
                return otherFrame == null;
            } else if (otherFrame == null) {
                return false;
            }
            if (thisFrame.mNode != otherFrame.mNode) {
                return false;
            }
            if (thisFrame.mNodePos != otherFrame.mNodePos) {
                return false;
            }
            thisFrame = thisFrame.mParentFrame;
            otherFrame = otherFrame.mParentFrame;
        }
    }

    /**
     * Test method which confirms that cursor frame nodes are at an extremity. Method is not
     * tolerant of split nodes.
     *
     * @param extremity LOW_EXTREMITY or HIGH_EXTREMITY
     */
    public final boolean verifyExtremities(byte extremity) throws IOException {
        Node node = mTree.mRoot;
        node.acquireExclusive();
        try {
            while (true) {
                if ((node.mType & extremity) == 0) {
                    return false;
                }
                if (node.isLeaf()) {
                    return true;
                }
                int pos = 0;
                if (extremity == Node.HIGH_EXTREMITY) {
                    pos = node.highestInternalPos();
                }
                node = latchChild(node, pos, true);
            }
        } finally {
            node.releaseExclusive();
        }
    }

    /**
     * Verifies from the current node to the last, unless stopped by observer.
     *
     * @return false if should stop
     */
    final boolean verify(final int height, VerificationObserver observer) throws IOException {
        if (height > 0) {
            final Node[] stack = new Node[height];
            while (key() != null) {
                if (!verifyFrames(height, stack, mLeaf, observer)) {
                    return false;
                }
                nextNode();
            }
        }
        return true;
    }

    private boolean verifyFrames(int level, Node[] stack, TreeCursorFrame frame,
                                 VerificationObserver observer)
        throws IOException
    {
        TreeCursorFrame parentFrame = frame.mParentFrame;

        if (parentFrame != null) {
            Node parentNode = parentFrame.mNode;
            int parentLevel = level - 1;
            if (parentLevel > 0 && stack[parentLevel] != parentNode) {
                parentNode = parentFrame.acquireShared();
                parentNode.releaseShared();
                if (stack[parentLevel] != parentNode) {
                    stack[parentLevel] = parentNode;
                    if (!verifyFrames(parentLevel, stack, parentFrame, observer)) {
                        return false;
                    }
                }
            }

            // Verify child node keys are lower/higher than parent node.

            parentNode = parentFrame.acquireShared();
            Node childNode = frame.acquireShared();
            long childId = childNode.mId;

            if (!childNode.hasKeys() || !parentNode.hasKeys()) {
                // Nodes can be empty before they're deleted.
                childNode.releaseShared();
                parentNode.releaseShared();
            } else {
                int parentPos = parentFrame.mNodePos;

                int childPos;
                boolean left;
                if (parentPos >= parentNode.highestInternalPos()) {
                    // Verify lowest child key is greater than or equal to parent key.
                    parentPos = parentNode.highestKeyPos();
                    childPos = 0;
                    left = false;
                } else {
                    // Verify highest child key is lower than parent key.
                    childPos = childNode.highestKeyPos();
                    left = true;
                }

                byte[] parentKey = parentNode.retrieveKey(parentPos);
                byte[] childKey = childNode.retrieveKey(childPos);

                childNode.releaseShared();
                parentNode.releaseShared();

                int compare = compareKeys(childKey, parentKey);

                if (left) {
                    if (compare >= 0) {
                        observer.failed = true;
                        if (!observer.indexNodeFailed
                            (childId, level, "Child keys are not less than parent key"))
                        {
                            return false;
                        }
                    }
                } else if (compare < 0) {
                    observer.failed = true;
                    if (!observer.indexNodeFailed
                        (childId, level, "Child keys are not greater than or equal to parent key"))
                    {
                        return false;
                    }
                }
            }

            // Verify node level types.

            switch (parentNode.mType) {
            case Node.TYPE_TN_IN:
                if (childNode.isLeaf()) {
                    observer.failed = true;
                    if (!observer.indexNodeFailed
                        (childId, level,
                         "Child is a leaf, but parent is a regular internal node"))
                    {
                        return false;
                    }
                }
                break;
            case Node.TYPE_TN_BIN:
                if (!childNode.isLeaf()) {
                    observer.failed = true;
                    if (!observer.indexNodeFailed
                        (childId, level,
                         "Child is not a leaf, but parent is a bottom internal node"))
                    {
                        return false;
                    }
                }
                break;
            default:
                if (!parentNode.isLeaf()) {
                    break;
                }
                // Fallthrough...
            case Node.TYPE_TN_LEAF:
                observer.failed = true;
                if (!observer.indexNodeFailed(childId, level, "Child parent is a leaf node")) {
                    return false;
                }
                break;
            }

            // Verify extremities.

            if ((childNode.mType & Node.LOW_EXTREMITY) != 0
                && (parentNode.mType & Node.LOW_EXTREMITY) == 0)
            {
                observer.failed = true;
                if (!observer.indexNodeFailed
                    (childId, level, "Child is low extremity but parent is not"))
                {
                    return false;
                }
            }

            if ((childNode.mType & Node.HIGH_EXTREMITY) != 0
                && (parentNode.mType & Node.HIGH_EXTREMITY) == 0)
            {
                observer.failed = true;
                if (!observer.indexNodeFailed
                    (childId, level, "Child is high extremity but parent is not"))
                {
                    return false;
                }
            }
        }

        return frame.acquireShared().verifyTreeNode(level, observer);
    }

    /**
     * Checks that leaf is defined and returns it.
     */
    private TreeCursorFrame leaf() {
        TreeCursorFrame leaf = mLeaf;
        if (leaf == null) {
            throw new IllegalStateException("Cursor position is undefined");
        }
        return leaf;
    }

    /**
     * Latches and returns leaf frame, which might be split.
     */
    protected final TreeCursorFrame leafExclusive() {
        TreeCursorFrame leaf = leaf();
        leaf.acquireExclusive();
        return leaf;
    }

    /**
     * Latches and returns leaf frame, not split.
     *
     * @throws IllegalStateException if unpositioned
     */
    final TreeCursorFrame leafExclusiveNotSplit() throws IOException {
        TreeCursorFrame leaf = leaf();
        Node node = leaf.acquireExclusive();
        if (node.mSplit != null) {
            mTree.finishSplit(leaf, node);
        }
        return leaf;
    }

    /**
     * Latches and returns leaf frame, not split.
     *
     * @throws IllegalStateException if unpositioned
     */
    final TreeCursorFrame leafSharedNotSplit() throws IOException {
        TreeCursorFrame leaf = leaf();
        Node node = leaf.acquireShared();
        if (node.mSplit != null) {
            doSplit: {
                if (!node.tryUpgrade()) {
                    node.releaseShared();
                    node = leaf.acquireExclusive();
                    if (node.mSplit == null) {
                        break doSplit;
                    }
                }
                node = mTree.finishSplit(leaf, node);
            }
            node.downgrade();
        }
        return leaf;
    }

    /**
     * Called with exclusive frame latch held, which is retained. Leaf frame is
     * dirtied, any split is finished, and the same applies to all parent
     * nodes. Caller must hold shared commit lock, to prevent deadlock. Node
     * latch is released if an exception is thrown.
     *
     * @return replacement node, still latched
     */
    final Node notSplitDirty(final TreeCursorFrame frame) throws IOException {
        Node node = frame.mNode;

        if (node.mSplit != null) {
            // Already dirty, but finish the split.
            return mTree.finishSplit(frame, node);
        }

        Database db = mTree.mDatabase;
        if (!db.shouldMarkDirty(node)) {
            return node;
        }

        TreeCursorFrame parentFrame = frame.mParentFrame;
        if (parentFrame == null) {
            try {
                db.doMarkDirty(mTree, node);
                return node;
            } catch (Throwable e) {
                node.releaseExclusive();
                throw e;
            }
        }

        // Make sure the parent is not split and dirty too.
        Node parentNode;
        doParent: {
            parentNode = parentFrame.tryAcquireExclusive();
            if (parentNode == null) {
                node.releaseExclusive();
                parentFrame.acquireExclusive();
            } else if (parentNode.mSplit != null || db.shouldMarkDirty(parentNode)) {
                node.releaseExclusive();
            } else {
                break doParent;
            }
            parentNode = notSplitDirty(parentFrame);
            node = frame.acquireExclusive();
        }

        while (node.mSplit != null) {
            // Already dirty now, but finish the split. Since parent latch is
            // already held, no need to call into the regular finishSplit
            // method. It would release latches and recheck everything.
            parentNode.insertSplitChildRef(mTree, parentFrame.mNodePos, node);
            if (parentNode.mSplit != null) {
                parentNode = mTree.finishSplit(parentFrame, parentNode);
            }
            node = frame.acquireExclusive();
        }
        
        try {
            if (db.markDirty(mTree, node)) {
                parentNode.updateChildRefId(parentFrame.mNodePos, node.mId);
            }
            return node;
        } catch (Throwable e) {
            node.releaseExclusive();
            throw e;
        } finally {
            parentNode.releaseExclusive();
        }
    }

    /**
     * Caller must hold exclusive latch, which is released by this method.
     */
    private void mergeLeaf(final TreeCursorFrame leaf, Node node) throws IOException {
        final TreeCursorFrame parentFrame = leaf.mParentFrame;
        node.releaseExclusive();

        if (parentFrame == null) {
            // Root node cannot merge into anything.
            return;
        }

        Node parentNode = parentFrame.acquireExclusive();

        Node leftNode, rightNode;
        int nodeAvail;
        while (true) {
            if (parentNode.mSplit != null) {
                parentNode = mTree.finishSplit(parentFrame, parentNode);
            }

            if (!parentNode.hasKeys()) {
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
                    parentNode.insertSplitChildRef(mTree, pos - 2, leftNode);
                    continue;
                }
            }

            node = leaf.acquireExclusive();

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
                try {
                    rightNode = latchChild(parentNode, pos + 2, false);
                } catch (Throwable e) {
                    if (leftNode != null) {
                        leftNode.releaseExclusive();
                    }
                    node.releaseExclusive();
                    throw e;
                }

                if (rightNode.mSplit != null) {
                    // Finish sibling split.
                    if (leftNode != null) {
                        leftNode.releaseExclusive();
                    }
                    node.releaseExclusive();
                    parentNode.insertSplitChildRef(mTree, pos + 2, rightNode);
                    continue;
                }
            }

            break;
        }

        // Select a left and right pair, and then don't operate directly on the
        // original node and leaf parameters afterwards. The original node ends
        // up being referenced as a left or right member of the pair.

        int leftAvail = leftNode == null ? -1 : leftNode.availableLeafBytes();
        int rightAvail = rightNode == null ? -1 : rightNode.availableLeafBytes();

        // Choose adjacent node pair which has the most available space, and then determine if
        // both nodes can fit in one node. If so, migrate and delete the right node. Leave
        // unbalanced otherwise.

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

        int remaining = leftAvail + rightAvail - p_length(node.mPage) + Node.TN_HEADER_SIZE;

        if (remaining >= 0) {
            // Migrate the entire contents of the right node into the left node, and then
            // delete the right node. Left must be marked dirty, and parent is already
            // expected to be dirty.

            try {
                if (mTree.markDirty(leftNode)) {
                    parentNode.updateChildRefId(leftPos, leftNode.mId);
                }
            } catch (Throwable e) {
                leftNode.releaseExclusive();
                rightNode.releaseExclusive();
                parentNode.releaseExclusive();
                throw e;
            }

            try {
                Node.moveLeafToLeftAndDelete(mTree, leftNode, rightNode);
            } catch (Throwable e) {
                leftNode.releaseExclusive();
                parentNode.releaseExclusive();
                throw e;
            }
            rightNode = null;
            parentNode.deleteRightChildRef(leftPos + 2);
        }

        mergeInternal(parentFrame, parentNode, leftNode, rightNode);
    }

    /**
     * Caller must hold exclusive latch, which is released by this method.
     *
     * @param leftChildNode never null, latched exclusively
     * @param rightChildNode null if contents merged into left node, otherwise latched
     * exclusively and should simply be unlatched
     */
    private void mergeInternal(TreeCursorFrame frame, Node node,
                               Node leftChildNode, Node rightChildNode)
        throws IOException
    {
        up: {
            if (node.shouldInternalMerge()) {
                if (node.hasKeys() || node != mTree.mRoot) {
                    // Continue merging up the tree.
                    break up;
                }

                // Delete the empty root node, eliminating a tree level.

                if (rightChildNode != null) {
                    throw new AssertionError();
                }

                // By retaining child latch, another thread is prevented from splitting it. The
                // lone child will become the new root.
                try {
                    node.rootDelete(mTree, leftChildNode);
                } catch (Throwable e) {
                    leftChildNode.releaseExclusive();
                    node.releaseExclusive();
                    throw e;
                }
            }

            if (rightChildNode != null) {
                rightChildNode.releaseExclusive();
            }
            leftChildNode.releaseExclusive();
            node.releaseExclusive();
            return;
        }

        if (rightChildNode != null) {
            rightChildNode.releaseExclusive();
        }
        leftChildNode.releaseExclusive();

        // At this point, only one node latch is held, and it should merge with
        // a sibling node. Node is guaranteed to be an internal node.

        TreeCursorFrame parentFrame = frame.mParentFrame;
        node.releaseExclusive();

        if (parentFrame == null) {
            // Root node cannot merge into anything.
            return;
        }

        Node parentNode = parentFrame.acquireExclusive();
        if (parentNode.isLeaf()) {
            throw new AssertionError("Parent node is a leaf");
        }

        Node leftNode, rightNode;
        int nodeAvail;
        while (true) {
            if (parentNode.mSplit != null) {
                parentNode = mTree.finishSplit(parentFrame, parentNode);
            }

            if (!parentNode.hasKeys()) {
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
                    parentNode.insertSplitChildRef(mTree, pos - 2, leftNode);
                    continue;
                }
            }

            node = frame.acquireExclusive();

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
                try {
                    rightNode = latchChild(parentNode, pos + 2, false);
                } catch (Throwable e) {
                    if (leftNode != null) {
                        leftNode.releaseExclusive();
                    }
                    node.releaseExclusive();
                    throw e;
                }

                if (rightNode.mSplit != null) {
                    // Finish sibling split.
                    if (leftNode != null) {
                        leftNode.releaseExclusive();
                    }
                    node.releaseExclusive();
                    parentNode.insertSplitChildRef(mTree, pos + 2, rightNode);
                    continue;
                }
            }

            break;
        }

        // Select a left and right pair, and then don't operate directly on the
        // original node and frame parameters afterwards. The original node
        // ends up being referenced as a left or right member of the pair.

        int leftAvail = leftNode == null ? -1 : leftNode.availableInternalBytes();
        int rightAvail = rightNode == null ? -1 : rightNode.availableInternalBytes();

        // Choose adjacent node pair which has the most available space, and then determine if
        // both nodes can fit in one node. If so, migrate and delete the right node. Leave
        // unbalanced otherwise.

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

        /*P*/ byte[] parentPage = parentNode.mPage;
        int parentEntryLoc = p_ushortGetLE(parentPage, parentNode.mSearchVecStart + leftPos);
        int parentEntryLen = Node.keyLengthAtLoc(parentPage, parentEntryLoc);
        int remaining = leftAvail - parentEntryLen
            + rightAvail - p_length(parentPage) + (Node.TN_HEADER_SIZE - 2);

        if (remaining >= 0) {
            // Migrate the entire contents of the right node into the left node, and then
            // delete the right node. Left must be marked dirty, and parent is already
            // expected to be dirty.

            try {
                if (mTree.markDirty(leftNode)) {
                    parentNode.updateChildRefId(leftPos, leftNode.mId);
                }
            } catch (Throwable e) {
                leftNode.releaseExclusive();
                rightNode.releaseExclusive();
                parentNode.releaseExclusive();
                throw e;
            }

            try {
                Node.moveInternalToLeftAndDelete
                    (mTree, leftNode, rightNode, parentPage, parentEntryLoc, parentEntryLen);
            } catch (Throwable e) {
                leftNode.releaseExclusive();
                parentNode.releaseExclusive();
                throw e;
            }
            rightNode = null;
            parentNode.deleteRightChildRef(leftPos + 2);
        }

        // Tail call. I could just loop here, but this is simpler.
        mergeInternal(parentFrame, parentNode, leftNode, rightNode);
    }

    /**
     * With parent held exclusively, returns child with exclusive latch held.
     * If an exception is thrown, parent and child latches are always released.
     *
     * @return child node, possibly split
     */
    private Node latchChild(Node parent, int childPos, boolean releaseParent)
        throws IOException
    {
        long childId = parent.retrieveChildRefId(childPos);
        Node childNode = mTree.mDatabase.mTreeNodeMap.get(childId);

        if (childNode != null) {
            childNode.acquireExclusive();
            // Need to check again in case evict snuck in.
            if (childId != childNode.mId) {
                childNode.releaseExclusive();
            } else {
                if (childNode.mCachedState != Node.CACHED_CLEAN
                    && parent.mCachedState == Node.CACHED_CLEAN)
                {
                    // Parent was evicted before child. Evict child now and mark as clean. If
                    // this isn't done, the notSplitDirty method will short-circuit and not
                    // ensure that all the parent nodes are dirty. The splitting and merging
                    // code assumes that all nodes referenced by the cursor are dirty. The
                    // short-circuit check could be skipped, but then every change would
                    // require a full latch up the tree. Another option is to remark the parent
                    // as dirty, but this is dodgy and also requires a full latch up the tree.
                    // Parent-before-child eviction is infrequent, and so simple is better.
                    if (releaseParent) {
                        parent.releaseExclusive();
                    }
                    childNode.write(mTree.mDatabase.mPageDb);
                    childNode.mCachedState = Node.CACHED_CLEAN;
                } else if (releaseParent) {
                    parent.releaseExclusive();
                }
                childNode.used();
                return childNode;
            }
        }

        return parent.loadChild(mTree.mDatabase, childId, releaseParent);
    }
}
