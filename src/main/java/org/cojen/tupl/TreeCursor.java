/*
 *  Copyright 2011-2015 Cojen.org
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
import java.util.concurrent.ThreadLocalRandom;

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
    LocalTransaction mTxn;

    // Top stack frame for cursor, always a leaf except during cleanup.
    private CursorFrame mLeaf;

    byte[] mKey;
    byte[] mValue;

    boolean mKeyOnly;

    // Hashcode is defined by LockManager.
    private int mKeyHash;

    TreeCursor(Tree tree, Transaction txn) {
        mTxn = tree.check(txn);
        mTree = tree;
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
        LocalTransaction old = mTxn;
        mTxn = mTree.check(txn);
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
        return compareUnsigned(lkey, 0, lkey.length, rkey, 0, rkey.length);
    }

    @Override
    public final int compareKeyTo(byte[] rkey, int offset, int length) {
        byte[] lkey = mKey;
        return compareUnsigned(lkey, 0, lkey.length, rkey, offset, length);
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
        reset();

        if (!toFirst(latchRootNode(), new CursorFrame())) {
            return LockResult.UNOWNED;
        }

        LocalTransaction txn = mTxn;
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
    private boolean toFirst(Node node, CursorFrame frame) throws IOException {
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
                node = latchToChild(node, 0);
                frame = new CursorFrame(frame);
            }
        } catch (Throwable e) {
            throw cleanup(e, frame);
        }
    }

    @Override
    public final LockResult last() throws IOException {
        reset();

        if (!toLast(latchRootNode(), new CursorFrame())) {
            return LockResult.UNOWNED;
        }

        LocalTransaction txn = mTxn;
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
    private boolean toLast(Node node, CursorFrame frame) throws IOException {
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
                node = latchToChild(node, childPos);

                frame = new CursorFrame(frame);
            }
        } catch (Throwable e) {
            throw cleanup(e, frame);
        }
    }

    @Override
    public final LockResult skip(long amount) throws IOException {
        if (amount == 0) {
            LocalTransaction txn = mTxn;
            if (txn != null && txn != Transaction.BOGUS) {
                byte[] key = mKey;
                if (key != null) {
                    return txn.mManager.check(txn, mTree.mId, key, keyHash());
                }
            }
            return LockResult.UNOWNED;
        }

        try {
            CursorFrame frame = leafSharedNotSplit();
            if (amount > 0) {
                if (amount > 1 && (frame = skipNextGap(frame, amount - 1, null)) == null) {
                    return LockResult.UNOWNED;
                }
                return next(mTxn, frame);
            } else {
                if (amount < -1 && (frame = skipPreviousGap(frame, -1 - amount, null)) == null) {
                    return LockResult.UNOWNED;
                }
                return previous(mTxn, frame);
            }
        } catch (Throwable e) {
            throw handleException(e, false);
        }
    }

    @Override
    public final LockResult skip(long amount, byte[] limitKey, boolean inclusive)
        throws IOException
    {
        if (amount == 0 || limitKey == null) {
            return skip(amount);
        }

        try {
            CursorFrame frame = leafSharedNotSplit();
            if (amount > 0) {
                if (amount > 1 && (frame = skipNextGap(frame, amount - 1, limitKey)) == null) {
                    return LockResult.UNOWNED;
                }
                return nextCmp(limitKey, inclusive ? LIMIT_LE : LIMIT_LT, frame);
            } else {
                if (amount < -1
                    && (frame = skipPreviousGap(frame, -1 - amount, limitKey)) == null)
                {
                    return LockResult.UNOWNED;
                }
                return previousCmp(limitKey, inclusive ? LIMIT_GE : LIMIT_GT, frame);
            }
        } catch (Throwable e) {
            throw handleException(e, false);
        }
    }

    @Override
    public final LockResult next() throws IOException {
        return next(mTxn, leafSharedNotSplit());
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
        return nextCmp(limitKey, limitMode, leafSharedNotSplit());
    }

    private LockResult nextCmp(byte[] limitKey, int limitMode, CursorFrame frame)
        throws IOException
    {
        LocalTransaction txn = mTxn;

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
            frame = leafSharedNotSplit();
        }
    }

    /**
     * Note: When method returns, frame is unlatched and may no longer be valid.
     *
     * @param frame leaf frame, not split, with shared latch
     */
    private LockResult next(LocalTransaction txn, CursorFrame frame) throws IOException {
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
            frame = leafSharedNotSplit();
        }
    }

    /**
     * Note: When method returns, frame is unlatched and may no longer be
     * valid. Leaf frame remains latched when method returns true.
     *
     * @param frame leaf frame, not split, with shared latch
     * @return false if nothing left
     */
    private boolean toNext(CursorFrame frame) throws IOException {
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
            CursorFrame parentFrame = frame.peek();

            if (parentFrame == null) {
                frame.popv();
                node.releaseShared();
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
                    // Latch coupling up the tree usually works, so give it a try. If it works,
                    // then there's no need to worry about a node merge.
                    parentNode = parentFrame.tryAcquireShared();

                    if (parentNode == null) {
                        // Latch coupling failed, and so acquire parent latch
                        // without holding child latch. The child might have
                        // changed, and so it must be checked again.
                        node.releaseShared();
                        parentNode = parentFrame.acquireShared();
                        if (parentNode.mSplit == null) {
                            break splitCheck;
                        }
                    } else {
                        if (parentNode.mSplit == null) {
                            frame.popv();
                            node.releaseShared();
                            parentPos = parentFrame.mNodePos;
                            break latchParent;
                        }
                        node.releaseShared();
                    }

                    // When this point is reached, parent node must be split. Parent latch is
                    // held, child latch is not held, but the frame is still valid.

                    parentNode = finishSplitShared(parentFrame, parentNode);
                }

                // When this point is reached, child must be relatched. Parent
                // latch is held, and the child frame is still valid.

                parentPos = parentFrame.mNodePos;
                node = latchChildRetainParent(parentNode, parentPos);

                // Quick check again, in case node got bigger due to merging.  Unlike the
                // earlier quick check, this one must handle internal nodes too.
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

                    parentNode.releaseShared();
                    frame.mNodePos = (pos += 2);

                    if (frame != mLeaf) {
                        return toFirst(latchToChild(node, pos), new CursorFrame(frame));
                    }

                    return true;
                }

                frame.popv();
                node.releaseShared();
            }

            // When this point is reached, only the shared parent latch is held. Child frame is
            // no longer valid.

            if (parentPos < parentNode.highestInternalPos()) {
                parentFrame.mNodePos = (parentPos += 2);
                // Always create a new cursor frame. See CursorFrame.unbind.
                frame = new CursorFrame(parentFrame);
                return toFirst(latchToChild(parentNode, parentPos), frame);
            }

            frame = parentFrame;
            node = parentNode;
        }
    }

    /**
     * @param frame leaf frame, not split, with shared latch
     * @param inLimit inclusive highest allowed internal key; null for no limit
     * @return latched leaf frame or null if reached end
     */
    private CursorFrame skipNextGap(CursorFrame frame, long amount, byte[] inLimit)
        throws IOException
    {
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
                CursorFrame parentFrame = frame.peek();

                if (parentFrame == null) {
                    frame.popv();
                    node.releaseShared();
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
                        // Latch coupling up the tree usually works, so give it a try. If it
                        // works, then there's no need to worry about a node merge.
                        parentNode = parentFrame.tryAcquireShared();

                        if (parentNode == null) {
                            // Latch coupling failed, and so acquire parent latch without
                            // holding child latch. The child might have changed, and so it
                            // must be checked again.
                            node.releaseShared();
                            parentNode = parentFrame.acquireShared();
                            if (parentNode.mSplit == null) {
                                break splitCheck;
                            }
                        } else {
                            if (parentNode.mSplit == null) {
                                frame.popv();
                                node.releaseShared();
                                parentPos = parentFrame.mNodePos;
                                break latchParent;
                            }
                            node.releaseShared();
                        }

                        // When this point is reached, parent node must be split. Parent latch
                        // is held, child latch is not held, but the frame is still valid.

                        parentNode = finishSplitShared(parentFrame, parentNode);
                    }

                    // When this point is reached, child must be relatched. Parent latch is
                    // held, and the child frame is still valid.

                    parentPos = parentFrame.mNodePos;
                    node = latchChildRetainParent(parentNode, parentPos);

                    // Quick check again, in case node got bigger due to merging. Unlike the
                    // earlier quick check, this one must handle internal nodes too.
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

                        parentNode.releaseShared();

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

                        if (inLimit != null) {
                            try {
                                if (node.compareKey(pos, inLimit) > 0) {
                                    mLeaf = frame;
                                    resetLatched(node);
                                    return null;
                                }
                            } catch (Throwable e) {
                                mLeaf = frame;
                                resetLatched(node);
                                throw e;
                            }
                        }

                        // Increment position of internal node.
                        frame.mNodePos = (pos += 2);

                        if (!toFirst(latchToChild(node, pos), new CursorFrame(frame))) {
                            return null;
                        }
                        frame = mLeaf;
                        if (--amount <= 0) {
                            return frame;
                        }
                        continue outer;
                    }

                    frame.popv();
                    node.releaseShared();
                }

                // When this point is reached, only the shared parent latch is held. Child
                // frame is no longer valid.

                while (parentPos < parentNode.highestInternalPos()) {
                    if (inLimit != null) {
                        try {
                            if (parentNode.compareKey(parentPos, inLimit) > 0) {
                                mLeaf = parentFrame;
                                resetLatched(parentNode);
                                return null;
                            }
                        } catch (Throwable e) {
                            mLeaf = parentFrame;
                            resetLatched(parentNode);
                            throw e;
                        }
                    }

                    parentFrame.mNodePos = (parentPos += 2);

                    Node childNode;

                    // Note: Same code as in skipPreviousGap.
                    loadChild: {
                        if (parentNode.isBottomInternal()) {
                            int childCount = parentNode.retrieveChildEntryCount(parentPos);

                            if (childCount >= 0) {
                                if (childCount < amount) {
                                    amount -= childCount;
                                    continue;
                                }
                            } else if (mTree.allowStoredCounts()) {
                                childNode = latchChildRetainParent(parentNode, parentPos);

                                if (childNode.mCachedState != Node.CACHED_CLEAN ||
                                    !parentNode.tryUpgrade())
                                {
                                    parentNode.releaseShared();
                                } else try {
                                    CommitLock commitLock = mTree.mDatabase.commitLock();
                                    if (commitLock.tryAcquireShared()) try {
                                        parentNode = notSplitDirty(parentFrame);
                                        childCount = childNode.countNonGhostKeys();
                                        parentNode.storeChildEntryCount(parentPos, childCount);
                                        if (childCount < amount) {
                                            amount -= childCount;
                                            childNode.releaseShared();
                                            parentNode.downgrade();
                                            continue;
                                        }
                                    } finally {
                                        commitLock.releaseShared();
                                    }
                                    parentNode.releaseExclusive();
                                } catch (Throwable e) {
                                    parentNode.releaseExclusive();
                                    throw e;
                                }

                                break loadChild;
                            }
                        }

                        childNode = latchToChild(parentNode, parentPos);
                    }

                    // Always create a new cursor frame. See CursorFrame.unbind.
                    frame = new CursorFrame(parentFrame);

                    if (!toFirst(childNode, frame)) {
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
        return previous(mTxn, leafSharedNotSplit());
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
        return previousCmp(limitKey, limitMode, leafSharedNotSplit());
    }

    private LockResult previousCmp(byte[] limitKey, int limitMode, CursorFrame frame)
        throws IOException
    {
        LocalTransaction txn = mTxn;

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
            frame = leafSharedNotSplit();
        }
    }

    /**
     * Note: When method returns, frame is unlatched and may no longer be valid.
     *
     * @param frame leaf frame, not split, with shared latch
     */
    private LockResult previous(LocalTransaction txn, CursorFrame frame)
        throws IOException
    {
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
            frame = leafSharedNotSplit();
        }
    }

    /**
     * Note: When method returns, frame is unlatched and may no longer be
     * valid. Leaf frame remains latched when method returns true.
     *
     * @param frame leaf frame, not split, with shared latch
     * @return false if nothing left
     */
    private boolean toPrevious(CursorFrame frame) throws IOException {
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
            CursorFrame parentFrame = frame.peek();

            if (parentFrame == null) {
                frame.popv();
                node.releaseShared();
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
                    // Latch coupling up the tree usually works, so give it a try. If it works,
                    // then there's no need to worry about a node merge.
                    parentNode = parentFrame.tryAcquireShared();

                    if (parentNode == null) {
                        // Latch coupling failed, and so acquire parent latch
                        // without holding child latch. The child might have
                        // changed, and so it must be checked again.
                        node.releaseShared();
                        parentNode = parentFrame.acquireShared();
                        if (parentNode.mSplit == null) {
                            break splitCheck;
                        }
                    } else {
                        if (parentNode.mSplit == null) {
                            frame.popv();
                            node.releaseShared();
                            parentPos = parentFrame.mNodePos;
                            break latchParent;
                        }
                        node.releaseShared();
                    }

                    // When this point is reached, parent node must be split. Parent latch is
                    // held, child latch is not held, but the frame is still valid.

                    parentNode = finishSplitShared(parentFrame, parentNode);
                }

                // When this point is reached, child must be relatched. Parent latch is held,
                // and the child frame is still valid.

                parentPos = parentFrame.mNodePos;
                node = latchChildRetainParent(parentNode, parentPos);

                // Quick check again, in case node got bigger due to merging.  Unlike the
                // earlier quick check, this one must handle internal nodes too.
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

                    parentNode.releaseShared();
                    frame.mNodePos = (pos -= 2);

                    if (frame != mLeaf) {
                        return toLast(latchToChild(node, pos), new CursorFrame(frame));
                    }

                    return true;
                }

                frame.popv();
                node.releaseShared();
            }

            // When this point is reached, only the shared parent latch is held. Child frame is
            // no longer valid.

            if (parentPos > 0) {
                parentFrame.mNodePos = (parentPos -= 2);
                // Always create a new cursor frame. See CursorFrame.unbind.
                frame = new CursorFrame(parentFrame);
                return toLast(latchToChild(parentNode, parentPos), frame);
            }

            frame = parentFrame;
            node = parentNode;
        }
    }

    /**
     * @param frame leaf frame, not split, with shared latch
     * @param inLimit inclusive lowest allowed internal key; null for no limit
     * @return latched leaf frame or null if reached end
     */
    private CursorFrame skipPreviousGap(CursorFrame frame, long amount, byte[] inLimit)
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
                CursorFrame parentFrame = frame.peek();

                if (parentFrame == null) {
                    frame.popv();
                    node.releaseShared();
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
                        // Latch coupling up the tree usually works, so give it a try. If it
                        // works, then there's no need to worry about a node merge.
                        parentNode = parentFrame.tryAcquireShared();

                        if (parentNode == null) {
                            // Latch coupling failed, and so acquire parent latch without
                            // holding child latch. The child might have changed, and so it
                            // must be checked again.
                            node.releaseShared();
                            parentNode = parentFrame.acquireShared();
                            if (parentNode.mSplit == null) {
                                break splitCheck;
                            }
                        } else {
                            if (parentNode.mSplit == null) {
                                frame.popv();
                                node.releaseShared();
                                parentPos = parentFrame.mNodePos;
                                break latchParent;
                            }
                            node.releaseShared();
                        }

                        // When this point is reached, parent node must be split. Parent latch
                        // is held, child latch is not held, but the frame is still valid.

                        parentNode = finishSplitShared(parentFrame, parentNode);
                    }

                    // When this point is reached, child must be relatched. Parent latch is
                    // held, and the child frame is still valid.

                    parentPos = parentFrame.mNodePos;
                    node = latchChildRetainParent(parentNode, parentPos);

                    // Quick check again, in case node got bigger due to merging. Unlike the
                    // earlier quick check, this one must handle internal nodes too.
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

                        parentNode.releaseShared();

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

                        if (inLimit != null) {
                            try {
                                if (node.compareKey(pos, inLimit) < 0) {
                                    mLeaf = frame;
                                    resetLatched(node);
                                    return null;
                                }
                            } catch (Throwable e) {
                                mLeaf = frame;
                                resetLatched(node);
                                throw e;
                            }
                        }

                        if (!toLast(latchToChild(node, pos), new CursorFrame(frame))) {
                            return null;
                        }
                        frame = mLeaf;
                        if (--amount <= 0) {
                            return frame;
                        }
                        continue outer;
                    }

                    frame.popv();
                    node.releaseShared();
                }

                // When this point is reached, only the shared parent latch is held. Child
                // frame is no longer valid.

                while (parentPos > 0) {
                    parentFrame.mNodePos = (parentPos -= 2);

                    if (inLimit != null) {
                        try {
                            if (parentNode.compareKey(parentPos, inLimit) < 0) {
                                mLeaf = parentFrame;
                                resetLatched(parentNode);
                                return null;
                            }
                        } catch (Throwable e) {
                            mLeaf = parentFrame;
                            resetLatched(parentNode);
                            throw e;
                        }
                    }

                    Node childNode;

                    // Note: Same code as in skipNextGap.
                    loadChild: {
                        if (parentNode.isBottomInternal()) {
                            int childCount = parentNode.retrieveChildEntryCount(parentPos);

                            if (childCount >= 0) {
                                if (childCount < amount) {
                                    amount -= childCount;
                                    continue;
                                }
                            } else if (mTree.allowStoredCounts()) {
                                childNode = latchChildRetainParent(parentNode, parentPos);

                                if (childNode.mCachedState != Node.CACHED_CLEAN ||
                                    !parentNode.tryUpgrade())
                                {
                                    parentNode.releaseShared();
                                } else try {
                                    CommitLock commitLock = mTree.mDatabase.commitLock();
                                    if (commitLock.tryAcquireShared()) try {
                                        parentNode = notSplitDirty(parentFrame);
                                        childCount = childNode.countNonGhostKeys();
                                        parentNode.storeChildEntryCount(parentPos, childCount);
                                        if (childCount < amount) {
                                            amount -= childCount;
                                            childNode.releaseShared();
                                            parentNode.downgrade();
                                            continue;
                                        }
                                    } finally {
                                        commitLock.releaseShared();
                                    }
                                    parentNode.releaseExclusive();
                                } catch (Throwable e) {
                                    parentNode.releaseExclusive();
                                    throw e;
                                }

                                break loadChild;
                            }
                        }

                        childNode = latchToChild(parentNode, parentPos);
                    }

                    // Always create a new cursor frame. See CursorFrame.unbind.
                    frame = new CursorFrame(parentFrame);

                    if (!toLast(childNode, frame)) {
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
     * Try to copy the current entry, locking it if required. Null is returned if lock is not
     * immediately available and only the key was copied. Node latch is always released by this
     * method, even if an exception is thrown.
     *
     * @return null, UNOWNED, INTERRUPTED, TIMED_OUT_LOCK, ACQUIRED,
     * OWNED_SHARED, OWNED_UPGRADABLE, or OWNED_EXCLUSIVE
     * @param txn optional
     */
    private LockResult tryCopyCurrent(LocalTransaction txn) throws IOException {
        final Node node;
        final int pos;
        {
            CursorFrame leaf = mLeaf;
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
            node.releaseShared();
        }
    }

    /**
     * Variant of tryCopyCurrent used by iteration methods which have a
     * limit. If limit is reached, cursor is reset and UNOWNED is returned.
     */
    private LockResult tryCopyCurrentCmp(LocalTransaction txn, byte[] limitKey, int limitMode)
        throws IOException
    {
        try {
            return doTryCopyCurrentCmp(txn, limitKey, limitMode);
        } catch (Throwable e) {
            mLeaf.mNode.releaseShared();
            throw e;
        }
    }

    /**
     * Variant of tryCopyCurrent used by iteration methods which have a
     * limit. If limit is reached, cursor is reset and UNOWNED is returned.
     */
    private LockResult doTryCopyCurrentCmp(LocalTransaction txn, byte[] limitKey, int limitMode)
        throws IOException
    {
        final Node node;
        final int pos;
        {
            CursorFrame leaf = mLeaf;
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
            node.releaseShared();
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

        node.releaseShared();
        return result;
    }

    /**
     * With node latch not held, lock the current key. Returns the lock result if entry exists,
     * null otherwise. Method is intended to be called for operations which move the position,
     * and so it should not retain locks for entries which were concurrently deleted. The find
     * operation is required to lock entries which don't exist.
     *
     * @param txn optional
     * @return null if current entry has been deleted
     */
    private LockResult lockAndCopyIfExists(LocalTransaction txn) throws IOException {
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

        CursorFrame frame = leafSharedNotSplit();
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
    private LocalTransaction prepareFind(byte[] key) {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }
        LocalTransaction txn = mTxn;
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

    private static final int
        VARIANT_REGULAR = 0,
        VARIANT_RETAIN  = 1, // retain node latch only if value is null
        VARIANT_NO_LOCK = 2, // retain node latch always, don't lock entry
        VARIANT_CHECK   = 3; // retain node latch always, don't lock entry, don't load entry

    @Override
    public final LockResult find(byte[] key) throws IOException {
        reset();
        return find(prepareFind(key), key, VARIANT_REGULAR,
                    latchRootNode(), new CursorFrame());
    }

    @Override
    public final LockResult findGe(byte[] key) throws IOException {
        // If isolation level is read committed, then key must be
        // locked. Otherwise, an uncommitted delete could be observed.
        reset();
        LocalTransaction txn = prepareFind(key);
        LockResult result = find(txn, key, VARIANT_RETAIN, latchRootNode(), new CursorFrame());
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
    public final LockResult findLe(byte[] key) throws IOException {
        // If isolation level is read committed, then key must be
        // locked. Otherwise, an uncommitted delete could be observed.
        reset();
        LocalTransaction txn = prepareFind(key);
        LockResult result = find(txn, key, VARIANT_RETAIN, latchRootNode(), new CursorFrame());
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
    public final LockResult findGt(byte[] key) throws IOException {
        findNoLock(key);
        return next(mTxn, mLeaf);
    }

    @Override
    public final LockResult findLt(byte[] key) throws IOException {
        findNoLock(key);
        return previous(mTxn, mLeaf);
    }

    private void findNoLock(byte[] key) throws IOException {
        reset();
        if (key == null) {
            throw new NullPointerException("Key is null");
        }
        // Never lock the requested key.
        find(null, key, VARIANT_CHECK, latchRootNode(), new CursorFrame());
    }

    @Override
    public final LockResult findNearby(byte[] key) throws IOException {
        LocalTransaction txn = prepareFind(key);

        Node node;
        CursorFrame frame = mLeaf;
        if (frame == null) {
            // Allocate new frame before latching root -- allocation can block.
            frame = new CursorFrame();
            node = latchRootNode();
        } else {
            node = frame.acquireShared();
            if (node.mSplit != null) {
                node = finishSplitShared(frame, node);
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
                    node.releaseShared();
                }
                return doLoad(txn);
            } else if ((pos != ~0 || (node.type() & Node.LOW_EXTREMITY) != 0) &&
                       (~pos <= node.highestLeafPos() || (node.type() & Node.HIGH_EXTREMITY) != 0))
            {
                // Not found, but insertion pos is in bounds.
                frame.mNotFoundKey = key;
                frame.mNodePos = pos;
                LockResult result = tryLockKey(txn);
                if (result == null) {
                    mValue = NOT_LOADED;
                    node.releaseShared();
                } else {
                    mValue = null;
                    node.releaseShared();
                    return result;
                }
                return doLoad(txn);
            }

            // Cannot be certain if position is in leaf node, so pop up.

            mLeaf = null;

            while (true) {
                CursorFrame parent = frame.pop();

                if (parent == null) {
                    // Usually the root frame refers to the root node, but it
                    // can be wrong if the tree height is changing.
                    Node root = mTree.mRoot;
                    if (node != root) {
                        node.releaseShared();
                        root.acquireShared();
                        node = root;
                    }
                    break;
                }

                node.releaseShared();
                frame = parent;
                node = frame.acquireShared();

                if (node.mSplit != null) {
                    node = finishSplitShared(frame, node);
                }

                try {
                    pos = Node.internalPos(node.binarySearch(key, frame.mNodePos));
                } catch (Throwable e) {
                    node.releaseShared();
                    throw cleanup(e, frame);
                }

                if ((pos == 0 && (node.type() & Node.LOW_EXTREMITY) == 0) ||
                    (pos >= node.highestInternalPos() && (node.type() & Node.HIGH_EXTREMITY) == 0))
                {
                    // Cannot be certain if position is in this node, so pop up.
                    continue;
                }

                frame.mNodePos = pos;
                try {
                    node = latchToChild(node, pos);
                } catch (Throwable e) {
                    throw cleanup(e, frame);
                }
                frame = new CursorFrame(frame);
                break;
            }
        }

        return find(txn, key, VARIANT_REGULAR, node, frame);
    }

    /**
     * @param node search node to start from
     * @param frame new frame for node
     */
    private LockResult find(LocalTransaction txn, byte[] key, int variant,
                            Node node, CursorFrame frame)
        throws IOException
    {
        while (true) {
            if (node.isLeaf()) {
                int pos;
                if (node.mSplit == null) {
                    try {
                        pos = node.binarySearch(key);
                    } catch (Throwable e) {
                        node.releaseShared();
                        throw cleanup(e, frame);
                    }
                    frame.bind(node, pos);
                } else {
                    try {
                        pos = node.mSplit.binarySearch(node, key);
                    } catch (Throwable e) {
                        node.releaseShared();
                        throw cleanup(e, frame);
                    }
                    frame.bind(node, pos);
                    if (pos < 0) {
                        // The finishSplit method will release the latch, and so the frame must
                        // be completely defined first.
                        frame.mNotFoundKey = key;
                    }
                    node = finishSplitShared(frame, node);
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
                    node.releaseShared();
                    // This might fail to acquire the lock too, but the cursor is at the proper
                    // position, and with the proper state.
                    return doLoad(txn);
                }

                if (pos < 0) {
                    frame.mNotFoundKey = key;
                    mValue = null;
                    if (variant < VARIANT_RETAIN) {
                        node.releaseShared();
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
                            node.releaseShared();
                            throw e;
                        }
                        if (variant < VARIANT_NO_LOCK) {
                            node.releaseShared();
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
                    node.releaseShared();
                    throw cleanup(e, frame);
                }
                frame.bind(node, childPos);
                try {
                    node = latchToChild(node, childPos);
                } catch (Throwable e) {
                    throw cleanup(e, frame);
                }
            } else {
                // Follow search into split, binding this frame to the unsplit node as if it
                // had not split. The binding will be corrected when the split is finished.

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
                        right.releaseShared();
                    } else {
                        selected = right;
                        selectedPos = Node.internalPos(right.binarySearch(key));
                        frame.bind(node, left.highestInternalPos() + 2 + selectedPos);
                        left.releaseShared();
                    }
                } catch (Throwable e) {
                    node.releaseShared();
                    sibling.releaseShared();
                    throw cleanup(e, frame);
                }

                try {
                    node = latchToChild(selected, selectedPos);
                } catch (Throwable e) {
                    throw cleanup(e, frame);
                }
            }

            frame = new CursorFrame(frame);
        }
    }

    /**
     * With node latched, try to lock the current key. Method expects mKeyHash
     * to be valid. Returns null if lock is required but not immediately available.
     *
     * @param txn can be null
     */
    private LockResult tryLockKey(LocalTransaction txn) {
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
        if (lowKey != null && highKey != null && compareUnsigned(lowKey, highKey) >= 0) {
            // Cannot find anything if range is empty.
            reset();
            return LockResult.UNOWNED;
        }

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        start: while (true) {
            reset();
            CursorFrame frame = new CursorFrame();
            Node node = latchRootNode();

            search: while (true) {
                if (node.mSplit != null) {
                    // Bind to anything to finish the split.
                    frame.bind(node, 0);
                    node = finishSplitShared(frame, node);
                }

                int pos;
                try {
                    pos = randomPosition(rnd, node, lowKey, highKey);
                } catch (Throwable e) {
                    node.releaseShared();
                    throw cleanup(e, frame);
                }
                if (pos < 0) {   // Node is empty or out of bounds, so start over.
                    mLeaf = frame;
                    resetLatched(node);
                    // Before continuing, check if range has anything in it at all. This must
                    // be performed each time, to account for concurrent updates.
                    if (isRangeEmpty(lowKey, highKey)) {
                        return LockResult.UNOWNED;
                    }
                    continue start;
                }

                // Need to bind specially in case split handling above already bound the frame.
                frame.bindOrReposition(node, pos);

                if (node.isLeaf()) {
                    mLeaf = frame;
                    LocalTransaction txn;
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
                        node.releaseShared();
                        // This might fail to acquire the lock too, but the cursor
                        // is at the proper position, and with the proper state.
                        result = doLoad(txn);
                    } else {
                        try {
                            mValue = mKeyOnly ? node.hasLeafValue(pos)
                                : node.retrieveLeafValue(pos);
                        } catch (Throwable e) {
                            mValue = NOT_LOADED;
                            node.releaseShared();
                            throw e;
                        }
                        node.releaseShared();
                    }

                    if (mValue == null) {
                        // Skip over ghosts. Attempting to lock ghosts in the
                        // first place is correct behavior, avoiding bias.
                        if (result == LockResult.ACQUIRED) {
                            txn.unlock();
                        }

                        frame = leafSharedNotSplit();

                        if (rnd.nextBoolean()) {
                            result = highKey == null ? next(txn, frame)
                                : nextCmp(highKey, LIMIT_LT, frame);
                        } else {
                            result = lowKey == null ? previous(txn, frame)
                                : previousCmp(lowKey, LIMIT_GE, frame);
                        }

                        if (mValue == null) {
                            // Nothing but ghosts in selected direction, so start over.
                            continue start;
                        }
                    }

                    return result;
                }

                try {
                    node = latchToChild(node, pos);
                } catch (Throwable e) {
                    throw cleanup(e, frame);
                }

                frame = new CursorFrame(frame);
            }
        }
    }

    /**
     * Analyze at the current position. Cursor is reset as a side-effect.
     */
    Index.Stats analyze() throws IOException {
        double entryCount, keyBytes, valueBytes, freeBytes, totalBytes;

        CursorFrame parent;

        CursorFrame frame = leafSharedNotSplit();
        Node node = frame.mNode;
        try {
            entryCount = node.numKeys();

            int pos = frame.mNodePos;
            int numKeys;

            freeBytes = node.availableBytes();
            totalBytes = pageSize(node.mPage);

            if (pos < 0 || (numKeys = node.numKeys()) <= 0) {
                keyBytes = 0;
                valueBytes = 0;
            } else {
                long[] stats = new long[2];

                node.retrieveKeyStats(pos, stats);
                keyBytes = ((double) stats[0]) * numKeys;
                totalBytes += ((double) stats[1]) * pageSize(node.mPage);

                node.retrieveLeafValueStats(pos, stats);
                valueBytes = ((double) stats[0]) * numKeys;
                totalBytes += ((double) stats[1]) * pageSize(node.mPage);
            }

            frame = frame.pop();
        } catch (Throwable e) {
            resetLatched(node);
            throw e;
        }

        node.releaseShared();

        while (frame != null) {
            double scalar;
            int availBytes;
            int pageSize;

            node = frame.acquireShared();
            try {
                scalar = node.numKeys() + 1; // internal nodes have +1 children
                availBytes = node.availableInternalBytes();
                pageSize = pageSize(node.mPage);
                frame = frame.pop();
            } finally {
                node.releaseShared();
            }

            entryCount *= scalar;
            keyBytes *= scalar;
            valueBytes *= scalar;
            freeBytes *= scalar;
            totalBytes *= scalar;

            freeBytes += availBytes;
            totalBytes += pageSize;
        }

        return new Index.Stats(entryCount, keyBytes, valueBytes, freeBytes, totalBytes);
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
    private LockResult doLoad(LocalTransaction txn) throws IOException {
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
            CursorFrame frame = leafSharedNotSplit();
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
            final LocalTransaction txn = mTxn;
            if (txn == null) {
                final Locker locker = mTree.lockExclusiveLocal(key, keyHash());
                try {
                    store(txn, leafExclusive(), value);
                } finally {
                    locker.unlock();
                }
            } else {
                if (txn.lockMode() != LockMode.UNSAFE) {
                    txn.lockExclusive(mTree.mId, key, keyHash());
                }
                store(txn, leafExclusive(), value);
            }
        } catch (Throwable e) {
            throw handleException(e, false);
        }
    }

    @Override
    public void commit(byte[] value) throws IOException {
        byte[] key = mKey;
        if (key == null) {
            throw new IllegalStateException("Cursor position is undefined");
        }

        try {
            final LocalTransaction txn = mTxn;
            if (txn == null) {
                final Locker locker = mTree.lockExclusiveLocal(key, keyHash());
                try {
                    store(txn, leafExclusive(), value);
                } finally {
                    locker.unlock();
                }
            } else {
                if (txn.lockMode() == LockMode.UNSAFE) {
                    store(txn, leafExclusive(), value);
                    txn.commit();
                } else {
                    txn.lockExclusive(mTree.mId, key, keyHash());
                    txn.storeCommit(this, value);
                }
            }
        } catch (Throwable e) {
            throw handleException(e, false);
        }
    }

    /**
     * Atomic find and store operation. Cursor must be in a reset state when called, and cursor
     * is also reset as a side-effect.
     *
     * @param key must not be null
     */
    final byte[] findAndStore(byte[] key, byte[] value) throws IOException {
        try {
            mKey = key;
            final LocalTransaction txn = mTxn;
            if (txn == null) {
                final int hash = LockManager.hash(mTree.mId, key);
                mKeyHash = hash;
                final Locker locker = mTree.lockExclusiveLocal(key, hash);
                try {
                    return doFindAndStore(txn, key, value);
                } finally {
                    locker.unlock();
                }
            } else {
                if (txn.lockMode() == LockMode.UNSAFE) {
                    mKeyHash = 0;
                } else {
                    final int hash = LockManager.hash(mTree.mId, key);
                    mKeyHash = hash;
                    txn.lockExclusive(mTree.mId, key, hash);
                }
                return doFindAndStore(txn, key, value);
            }
        } catch (Throwable e) {
            throw handleException(e, true);
        }
    }

    private byte[] doFindAndStore(LocalTransaction txn, byte[] key, byte[] value)
        throws IOException
    {
        // Find with no lock because it has already been acquired. Leaf latch is retained too.
        find(null, key, VARIANT_NO_LOCK, latchRootNode(), new CursorFrame());
        byte[] oldValue = mValue;

        CursorFrame leaf = mLeaf;
        if (!leaf.mNode.tryUpgrade()) {
            leaf.mNode.releaseShared();
            leaf.acquireExclusive();
        }

        store(txn, leaf, value);
        reset();

        return oldValue;
    }

    static final byte[] MODIFY_INSERT = new byte[0], MODIFY_REPLACE = new byte[0];

    /**
     * Atomic find and modify operation. Cursor must be in a reset state when called, and
     * cursor is also reset as a side-effect.
     *
     * @param key must not be null
     * @param oldValue MODIFY_INSERT, MODIFY_REPLACE, else update mode
     */
    final boolean findAndModify(byte[] key, byte[] oldValue, byte[] newValue) throws IOException {
        final LocalTransaction txn = mTxn;

        try {
            // Note: Acquire exclusive lock instead of performing upgrade sequence. The upgrade
            // would need to be performed with the node latch held, which is deadlock prone.

            mKey = key;

            if (txn == null) {
                final int hash = LockManager.hash(mTree.mId, key);
                mKeyHash = hash;
                final Locker locker = mTree.lockExclusiveLocal(key, hash);
                try {
                    return doFindAndModify(null, key, oldValue, newValue);
                } finally {
                    locker.unlock();
                }
            }

            LockResult result;

            LockMode mode = txn.lockMode();
            if (mode == LockMode.UNSAFE) {
                mKeyHash = 0;
                // Indicate that no unlock should be performed.
                result = LockResult.OWNED_EXCLUSIVE;
            } else {
                final int hash = LockManager.hash(mTree.mId, key);
                mKeyHash = hash;
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

    private boolean doFindAndModify(LocalTransaction txn,
                                    byte[] key, byte[] oldValue, byte[] newValue)
        throws IOException
    {
        // Find with no lock because caller must already acquire exclusive lock.
        find(null, key, VARIANT_NO_LOCK, latchRootNode(), new CursorFrame());

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

        CursorFrame leaf = mLeaf;
        if (!leaf.mNode.tryUpgrade()) {
            leaf.mNode.releaseShared();
            leaf.acquireExclusive();
        }

        store(txn, leaf, newValue);
        reset();

        return true;
    }

    /**
     * Non-transactional ghost delete. Caller is expected to hold exclusive key lock. Method
     * does nothing if a value exists. Cursor must be in a reset state when called, and cursor
     * is also reset as a side-effect.
     *
     * @return false if Tree is closed
     */
    final boolean deleteGhost(byte[] key) throws IOException {
        try {
            // Find with no lock because it has already been acquired.
            // TODO: Use nearby optimization when used with transactional Index.clear.
            find(null, key, VARIANT_NO_LOCK, latchRootNode(), new CursorFrame());

            CursorFrame leaf = mLeaf;
            if (leaf.mNode.mPage == p_closedTreePage()) {
                resetLatched(leaf.mNode);
                return false;
            }

            if (mValue == null) {
                mKey = key;
                mKeyHash = 0;
                if (!leaf.mNode.tryUpgrade()) {
                    leaf.mNode.releaseShared();
                    leaf.acquireExclusive();
                }
                store(LocalTransaction.BOGUS, leaf, null);
                reset();
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
     */
    protected final void store(final LocalTransaction txn, final CursorFrame leaf,
                               final byte[] value)
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

            final CommitLock commitLock = mTree.mDatabase.commitLock();

            if (!commitLock.tryAcquireShared()) {
                leaf.mNode.releaseExclusive();
                commitLock.acquireShared();
                leaf.acquireExclusive();

                // Need to check if exists again.
                if (leaf.mNodePos < 0) {
                    node = leaf.mNode;
                    commitLock.releaseShared();
                    break doDelete;
                }
            }

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
                    // Always released by mergeLeaf.
                    node = null;
                }
            } finally {
                commitLock.releaseShared();
            }
        } else {
            final CommitLock commitLock = commitLock(leaf);
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

                        node.updateLeafValue(leaf, mTree, pos, 0, value);
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

                        node.insertLeafEntry(leaf, mTree, ~pos, key, value);
                    } catch (Throwable e) {
                        node.releaseExclusive();
                        throw e;
                    }

                    // Releases latch if an exception is thrown.
                    node = postInsert(leaf, node, key);
                }
            } finally {
                commitLock.releaseShared();
            }
        }

        if (node != null) {
            node.releaseExclusive();
        }
        mValue = value;

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
    private Node postInsert(CursorFrame leaf, Node node, byte[] key) throws IOException {
        int pos = leaf.mNodePos;
        int newPos = ~pos;

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
                // Other cursor is at same not-found position as this one was. If keys are the
                // same, then other cursor switches to a found state as well. If key is
                // greater, then position needs to be updated.

                byte[] frameKey = frame.mNotFoundKey;
                int compare = compareUnsigned(frameKey, 0, frameKey.length, key, 0, key.length);
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

        final CursorFrame leaf = leafExclusive();

        final CommitLock commitLock = commitLock(leaf);
        try {
            Node node = notSplitDirty(leaf);

            final int pos = leaf.mNodePos;
            if (pos >= 0) {
                try {
                    node.updateLeafValue(leaf, mTree, pos, Node.ENTRY_FRAGMENTED, value);
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
                    node.insertFragmentedLeafEntry(leaf, mTree, ~pos, key, value);
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
            commitLock.releaseShared();
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
    final Node insertBlank(CursorFrame leaf, Node node, long vlength) throws IOException {
        byte[] key = mKey;
        try {
            node.insertBlankLeafEntry(leaf, mTree, ~leaf.mNodePos, key, vlength);
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
        final CursorFrame leaf = leafExclusive();

        final CommitLock commitLock = commitLock(leaf);
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
            commitLock.releaseShared();
        }

        leaf.mNode.downgrade();
        next(LocalTransaction.BOGUS, leaf);
    }

    /**
     * @param frame node frame
     * @param node latched node, with no keys, and dirty; released by this method
     * @return replacement node, latched exclusively; null if tree is empty
     */
    private Node trimNode(final CursorFrame frame, final Node node) throws IOException {
        node.mLastCursorFrame = null;

        LocalDatabase db = mTree.mDatabase;
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

        CursorFrame parentFrame = frame.mParentFrame;
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

        Node next = latchChildRetainParentEx(parentNode, 0);

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
        next.type((byte) (next.type() | Node.LOW_EXTREMITY));

        db.deleteNode(node);

        return next;
    }

    /**
     * Select an entry to delete from the index, at random. All frames are unbound and cursor
     * is reset.
     *
     * @param lowKey inclusive lowest key in the evictable range; pass null for open range
     * @param highKey exclusive highest key in the evictable range; pass null for open range
     * @param keyRef optional, pass non-null to receive a copy of the evicted key
     * @param valueRef optional, pass non-null to receive a copy of the evicted value
     * @return sum of the key and value lengths which were evicted, 0 if no records are evicted
     * @throws IOException
     */
    final long evict(byte[] lowKey, byte[] highKey, byte[][] keyRef, byte[][] valueRef)
        throws IOException
    {
        if ((keyRef != null && keyRef.length == 0) || (valueRef != null && valueRef.length == 0)) {
            throw new IllegalArgumentException("Key/value reference param cannot be empty");
        }
        if (lowKey != null && highKey != null && compareUnsigned(lowKey, highKey) >= 0) {
            reset();
            return 0;
        }

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        start: while (true) {
            // The only scenario in which the following is executed more than once is when the
            // inner "search" infinite loop is broken. For all executions after the first,
            // reset() is invoked before reaching this point of execution.
            reset();
            CursorFrame frame = new CursorFrame();
            Node node = mTree.mRoot;
            node.acquireExclusive();
            int remainingAttempsLN = 2;
            int remainingAttemptsBIN = 2;

            search: while (true) {
                if (node.mSplit != null) {
                    frame.bind(node, 0);
                    node = mTree.finishSplit(frame, node);
                }

                int pos;
                try {
                    pos = randomPosition(rnd, node, lowKey, highKey);
                } catch (Throwable t) {
                    node.releaseExclusive();
                    throw cleanup(t, frame);
                }

                if (pos < 0) {
                    // node is empty or out of bounds
                    mLeaf = frame;
                    resetLatchedEx(node);

                    // Before continuing, check if range has anything in it at all. This must
                    // be performed each time, to account for concurrent updates.
                    if (isRangeEmpty(lowKey, highKey)) {
                        return 0;
                    }
                    continue start;
                }

                frame.bindOrReposition(node, pos);

                if (node.isLeaf()) {
                    mLeaf = frame;
                    try {
                        LocalTransaction txn = prepareFind(node.retrieveKey(pos));
                        LockResult result;
                        if ((result = tryLockKey(txn)) == null) {
                            // Some other transaction is operating on the key. Unlikely to happen
                            // as seek is steered towards nodes which are not in cache
                            if (keyRef != null) {
                                keyRef[0] = null;
                            }
                            if (valueRef != null) {
                                 valueRef[0] = null;
                            }
                            return 0;
                        }

                        byte[] value = valueRef != null ? node.retrieveLeafValue(pos) : node.hasLeafValue(pos);
                        if (value == null) { // ghost record
                            // unlikely to happen as seek is steered towards nodes which are not in cache
                            // hence not making extra effort to find another record to evict.
                            if (result == LockResult.ACQUIRED) {
                                txn.unlock();
                            }
                            if (keyRef != null) {
                                keyRef[0] = null;
                            }
                            if (valueRef != null) {
                                valueRef[0] = null;
                            }
                            return 0;
                        }

                        long length = mKey.length;
                        if (keyRef != null) {
                            keyRef[0] = mKey;
                        }
                        if (valueRef != null) {
                            valueRef[0] = value;
                            length += value.length;
                        } else {
                            long[] valLen = new long[2];
                            node.retrieveLeafValueStats(pos, valLen);
                            length += valLen[0];
                        }

                        store(txn, frame, null);
                        reset();

                        // Make an attempt to mark node as LRU
                        if (remainingAttempsLN >= 0 && node.tryAcquireExclusive()) {
                            node.unused();
                        }
                        return length;
                    } finally {
                        // reset() is invoked after store(), in which case mLeaf will
                        // be set to null. mLeaf is guaranteed to be non-null otherwise.
                        if (mLeaf != null) {
                            resetLatchedEx(node);
                        }
                    }
                } else if (node.isBottomInternal()) {
                    long childId = node.retrieveChildRefId(pos);
                    Node child = mTree.mDatabase.nodeMapGet(childId);
                    if (child != null) { // node is cached
                        if (remainingAttempsLN-->0) {
                            continue search;
                        }

                        // used up max random selection attempts for non-cached leaf node.
                        // scan sequentially for a non-cached leaf node.
                        try {
                            int spos = (lowKey == null) ? 0 : Node.internalPos(node.binarySearch(lowKey));
                            int highestInternalPos = node.highestInternalPos();
                            int highestKeyPos = node.highestKeyPos();
                            for (; spos <= highestInternalPos; spos+=2) {
                                childId = node.retrieveChildRefId(spos);
                                child = mTree.mDatabase.nodeMapGet(childId);
                                if (child == null) { // node is not cached
                                    pos = spos;
                                    frame.bindOrReposition(node, pos);
                                    break;
                                }
                                if (highKey != null && spos <= highestKeyPos && node.compareKey(spos, highKey) >= 0) {
                                    break;
                                }
                            }
                        } catch (Throwable t) {
                            // continue with the randomly selected node
                        }
                    }
                    try {
                        node = latchToChildEx(node, pos);
                    } catch (Throwable t) {
                        throw cleanup(t, frame);
                    }
                } else {    // non-bottom internal node
                    long childId = node.retrieveChildRefId(pos);
                    Node child = mTree.mDatabase.nodeMapGet(childId);
                    if (child != null) {
                        /* Node.evict() calls db.nodeMapRemove(), followed by mId = 0. Child can get evicted anytime
                         * case 1: Before id check: No change in result
                         * case 2: After id check, !BIN: No change in result
                         * case 3a: After id check, BIN, remainingBottomInternalNodeAttempts>0: Skipped a node un-necessarily
                         * case 3b: After id check, BIN, remainingBottomInternalNodeAttempts=0: no change in result
                         */
                        if (child.isBottomInternal() && child.mId == childId && remainingAttemptsBIN-->0) {
                            continue search;
                        }
                    }
                    try {
                        node = latchToChildEx(node, pos);
                    } catch (Throwable t) {
                        throw cleanup(t, frame);
                    }
                }
                frame = new CursorFrame(frame);
            }   // search
        }   // start
        // unreachable code
    }

    /**
     * Find and return a random position in the node. The node should be latched.
     * @param rnd random number generator
     * @param node non-null latched node
     * @param lowKey start of range, inclusive. pass null for open range
     * @param highKey end of range, exclusive. pass null for open range
     * @return <0 if node is empty or out of bounds
     */
    private int randomPosition(Random rnd, Node node, byte[] lowKey, byte[] highKey)
        throws IOException
    {
       int pos = 0;
       if (highKey == null) {
           pos = node.highestPos() + 2;
       } else {
           pos = node.binarySearch(highKey);
           if (pos < 0) {    // highKey is not found
               pos = ~pos;
           }
           if (!node.isLeaf()) {
               pos += 2;
           }
       }

       if (lowKey == null) {
           if (pos > 0) {
               // search vector has 2 byte long entries
               pos = (pos == 2) ? 0 : (rnd.nextInt(pos >> 1) << 1);
               return pos;
           }
       } else {
           int lowPos = node.binarySearch(lowKey);
           if (!node.isLeaf()) {
               lowPos = Node.internalPos(lowPos);
           } else if (lowPos < 0) {  // lowKey not found
               lowPos = ~lowPos;
           }
           int range = pos - lowPos;
           if (range > 0) {
               // search vector has 2 byte long entries
               pos = (range == 2) ? lowPos : lowPos + (rnd.nextInt(range >> 1) << 1);
               return pos;
           }
       }
       // node is empty or out of bounds
       return -1;
    }

    /**
     * Check if there are any keys in the range. Cursor is reset as a side effect.
     * @param lowKey start of range, inclusive
     * @param highKey end of range, exclusive
     * @return
     */
    private boolean isRangeEmpty(byte[] lowKey, byte[] highKey) throws IOException {
        boolean oldKeyOnly = mKeyOnly;
        LocalTransaction oldTxn = mTxn;
        try {
            mTxn = LocalTransaction.BOGUS;
            mKeyOnly = true;
            if (lowKey == null) {
                first();
            } else {
                findGe(lowKey);
            }
            if (mKey == null || (highKey != null && Utils.compareUnsigned(mKey, highKey) >= 0)) {
                return true;
            }
            return false;
        } finally {
            reset();
            mKeyOnly = oldKeyOnly;
            mTxn = oldTxn;
        }
    }

    /**
     * Safely acquire shared commit lock while node latch is held exclusively. Latch might need
     * to be released and relatched in order to obtain shared commit lock without deadlocking.
     * As a result, the caller must not rely on any existing node reference. It must be
     * accessed again from the leaf frame instance.
     *
     * @param leaf leaf frame, latched exclusively, which might be released and relatched
     * @return held commitLock
     */
    final CommitLock commitLock(final CursorFrame leaf) {
        CommitLock commitLock = mTree.mDatabase.commitLock();
        if (!commitLock.tryAcquireShared()) {
            leaf.mNode.releaseExclusive();
            commitLock.acquireShared();
            leaf.acquireExclusive();
        }
        return commitLock;
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
        CursorFrame frame = mLeaf;
        if (frame != null) {
            CursorFrame frameCopy = new CursorFrame();
            frame.copyInto(frameCopy);
            copy.mLeaf = frameCopy;
        }
        copy.mKey = mKey;
        copy.mKeyHash = mKeyHash;
        return copy;
    }

    /**
     * Return root node latched shared.
     */
    private Node latchRootNode() {
        Node root = mTree.mRoot;
        root.acquireShared();
        return root;
    }

    @Override
    public final void reset() {
        mKey = null;
        mKeyHash = 0;
        mValue = null;

        CursorFrame frame = mLeaf;
        mLeaf = null;

        if (frame != null) {
            CursorFrame.popAll(frame);
        }
    }

    /**
     * Reset with leaf already latched shared.
     */
    private void resetLatched(Node node) {
        node.releaseShared();
        reset();
    }

    /**
     * Reset with leaf already latched exclusively.
     */
    private void resetLatchedEx(Node node) {
        node.releaseExclusive();
        reset();
    }

    /**
     * Called if an exception is thrown while frames are being constructed.
     * Given frame does not need to be bound, but it must not be latched.
     */
    private RuntimeException cleanup(Throwable e, CursorFrame frame) {
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

    final int height() {
        int height = 0;
        CursorFrame frame = mLeaf;
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

        CursorFrame frame = mLeaf;

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
                        int pLen = pageSize(node.mPage);
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
    private long compactFrame(long highestNodeId, CursorFrame frame, Node node)
        throws IOException
    {
        long id = node.mId;
        node.releaseShared();

        if (id > highestNodeId) {
            LocalDatabase db = mTree.mDatabase;
            CommitLock commitLock = db.commitLock();
            commitLock.acquireShared();
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
                commitLock.releaseShared();
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

        CursorFrame thisFrame = mLeaf;
        CursorFrame otherFrame = other.mLeaf;
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
        Node node = latchRootNode();
        try {
            while (true) {
                if ((node.type() & extremity) == 0) {
                    return false;
                }
                if (node.isLeaf()) {
                    return true;
                }
                int pos = 0;
                if (extremity == Node.HIGH_EXTREMITY) {
                    pos = node.highestInternalPos();
                }
                node = latchToChild(node, pos);
            }
        } finally {
            node.releaseShared();
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

    @SuppressWarnings("fallthrough")
    private boolean verifyFrames(int level, Node[] stack, CursorFrame frame,
                                 VerificationObserver observer)
        throws IOException
    {
        CursorFrame parentFrame = frame.mParentFrame;

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

                int compare = compareUnsigned(childKey, parentKey);

                if (left) {
                    if (compare >= 0) {
                        observer.failed = true;
                        if (!observer.indexNodeFailed
                            (childId, level,
                             "Child keys are not less than parent key: " + parentNode))
                        {
                            return false;
                        }
                    }
                } else if (compare < 0) {
                    observer.failed = true;
                    if (!observer.indexNodeFailed
                        (childId, level,
                         "Child keys are not greater than or equal to parent key: " + parentNode))
                    {
                        return false;
                    }
                }
            }

            // Verify node level types.

            switch (parentNode.type()) {
            case Node.TYPE_TN_IN:
                if (childNode.isLeaf()) {
                    observer.failed = true;
                    if (!observer.indexNodeFailed
                        (childId, level,
                         "Child is a leaf, but parent is a regular internal node: " + parentNode))
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
                         "Child is not a leaf, but parent is a bottom internal node: "
                         + parentNode))
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
                if (!observer.indexNodeFailed(childId, level,
                                              "Child parent is a leaf node: " + parentNode))
                {
                    return false;
                }
                break;
            }

            // Verify extremities.

            if ((childNode.type() & Node.LOW_EXTREMITY) != 0
                && (parentNode.type() & Node.LOW_EXTREMITY) == 0)
            {
                observer.failed = true;
                if (!observer.indexNodeFailed
                    (childId, level, "Child is low extremity but parent is not: " + parentNode))
                {
                    return false;
                }
            }

            if ((childNode.type() & Node.HIGH_EXTREMITY) != 0
                && (parentNode.type() & Node.HIGH_EXTREMITY) == 0)
            {
                observer.failed = true;
                if (!observer.indexNodeFailed
                    (childId, level, "Child is high extremity but parent is not: " + parentNode))
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
    private CursorFrame leaf() {
        CursorFrame leaf = mLeaf;
        if (leaf == null) {
            throw new IllegalStateException("Cursor position is undefined");
        }
        return leaf;
    }

    /**
     * Latches and returns leaf frame, which might be split.
     */
    protected final CursorFrame leafExclusive() {
        CursorFrame leaf = leaf();
        leaf.acquireExclusive();
        return leaf;
    }

    /**
     * Latches and returns leaf frame, not split.
     *
     * @throws IllegalStateException if unpositioned
     */
    final CursorFrame leafExclusiveNotSplit() throws IOException {
        CursorFrame leaf = leaf();
        Node node = leaf.acquireExclusive();
        if (node.mSplit != null) {
            mTree.finishSplit(leaf, node);
        }
        return leaf;
    }

    /**
     * Latches and returns the leaf frame, not split.
     *
     * @throws IllegalStateException if unpositioned
     */
    final CursorFrame leafSharedNotSplit() throws IOException {
        CursorFrame leaf = leaf();
        Node node = leaf.acquireShared();
        if (node.mSplit != null) {
            finishSplitShared(leaf, node);
        }
        return leaf;
    }

    /**
     * Caller must hold shared latch and it must verify that node has split. Node latch is
     * released if an exception is thrown.
     *
     * @param frame bound cursor frame
     * @param node node which is bound to the frame, latched shared
     * @return replacement node, still latched
     */
    final Node finishSplitShared(final CursorFrame frame, Node node) throws IOException {
        doSplit: {
            if (!node.tryUpgrade()) {
                node.releaseShared();
                node = frame.acquireExclusive();
                if (node.mSplit == null) {
                    break doSplit;
                }
            }
            node = mTree.finishSplit(frame, node);
        }
        node.downgrade();
        return node;
    }

    /**
     * Called with exclusive frame latch held, which is retained. Leaf frame is dirtied, any
     * split is finished, and the same applies to all parent nodes. Caller must hold shared
     * commit lock, to prevent deadlock. Node latch is released if an exception is thrown.
     *
     * @return replacement node, still latched
     */
    final Node notSplitDirty(final CursorFrame frame) throws IOException {
        Node node = frame.mNode;

        if (node.mSplit != null) {
            // Already dirty, but finish the split.
            return mTree.finishSplit(frame, node);
        }

        LocalDatabase db = mTree.mDatabase;
        if (!db.shouldMarkDirty(node)) {
            return node;
        }

        CursorFrame parentFrame = frame.mParentFrame;
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
            parentNode.insertSplitChildRef(parentFrame, mTree, parentFrame.mNodePos, node);
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
    private void mergeLeaf(final CursorFrame leaf, Node node) throws IOException {
        final CursorFrame parentFrame = leaf.mParentFrame;
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
                leftNode = latchChildRetainParentEx(parentNode, pos - 2);
                if (leftNode.mSplit != null) {
                    // Finish sibling split.
                    parentNode.insertSplitChildRef(parentFrame, mTree, pos - 2, leftNode);
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
                    rightNode = latchChildRetainParentEx(parentNode, pos + 2);
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
                    parentNode.insertSplitChildRef(parentFrame, mTree, pos + 2, rightNode);
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

        int remaining = leftAvail + rightAvail - pageSize(node.mPage) + Node.TN_HEADER_SIZE;

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
     * @param leftChildNode never null, latched exclusively, always released by this method
     * @param rightChildNode null if contents merged into left node, otherwise latched
     * exclusively and should simply be unlatched
     */
    private void mergeInternal(CursorFrame frame, Node node,
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
                node.rootDelete(mTree, leftChildNode);
                return;
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

        CursorFrame parentFrame = frame.mParentFrame;
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
                leftNode = latchChildRetainParentEx(parentNode, pos - 2);
                if (leftNode.mSplit != null) {
                    // Finish sibling split.
                    parentNode.insertSplitChildRef(parentFrame, mTree, pos - 2, leftNode);
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
                    rightNode = latchChildRetainParentEx(parentNode, pos + 2);
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
                    parentNode.insertSplitChildRef(parentFrame, mTree, pos + 2, rightNode);
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
        int parentEntryLoc = p_ushortGetLE(parentPage, parentNode.searchVecStart() + leftPos);
        int parentEntryLen = Node.keyLengthAtLoc(parentPage, parentEntryLoc);
        int remaining = leftAvail - parentEntryLen
            + rightAvail - pageSize(parentPage) + (Node.TN_HEADER_SIZE - 2);

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

    private int pageSize(/*P*/ byte[] page) {
        /*P*/ // [
        return page.length;
        /*P*/ // |
        /*P*/ // return mTree.pageSize();
        /*P*/ // ]
    }

    /**
     * With parent held shared, returns child with shared latch held, releasing the parent
     * latch. If an exception is thrown, parent and child latches are always released.
     *
     * @return child node, possibly split
     */
    private Node latchToChild(Node parent, int childPos) throws IOException {
        return latchChild(parent, childPos, Node.OPTION_PARENT_RELEASE_SHARED);
    }

    /**
     * With parent held shared, returns child with shared latch held, retaining the parent
     * latch. If an exception is thrown, parent and child latches are always released.
     *
     * @return child node, possibly split
     */
    private Node latchChildRetainParent(Node parent, int childPos) throws IOException {
        return latchChild(parent, childPos, 0);
    }

    /**
     * With parent held shared, returns child with shared latch held. If an exception is
     * thrown, parent and child latches are always released.
     *
     * @param options Node.OPTION_PARENT_RELEASE_SHARED or 0 to retain latch
     * @return child node, possibly split
     */
    private Node latchChild(Node parent, int childPos, int options) throws IOException {
        long childId = parent.retrieveChildRefId(childPos);
        Node childNode = mTree.mDatabase.nodeMapGet(childId);

        tryFind: if (childNode != null) {
            childNode.acquireShared();
            // Need to check again in case evict snuck in.
            if (childId != childNode.mId) {
                childNode.releaseShared();
                break tryFind;
            }

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

                if (!childNode.tryUpgrade()) {
                    childNode.releaseShared();
                    childNode = mTree.mDatabase.nodeMapGet(childId);                        
                    if (childNode == null) {
                        break tryFind;
                    }
                    childNode.acquireExclusive();
                    if (childId != childNode.mId) {
                        childNode.releaseExclusive();
                        break tryFind;
                    }
                }

                if ((options & Node.OPTION_PARENT_RELEASE_SHARED) != 0) {
                    parent.releaseShared();
                }

                try {
                    childNode.write(mTree.mDatabase.mPageDb);
                } catch (Throwable e) {
                    childNode.releaseExclusive();
                    throw e;
                }

                childNode.mCachedState = Node.CACHED_CLEAN;
                childNode.downgrade();
            } else if ((options & Node.OPTION_PARENT_RELEASE_SHARED) != 0) {
                parent.releaseShared();
            }

            childNode.used();
            return childNode;
        }

        return parent.loadChild(mTree.mDatabase, childId, options);
    }

    /**
     * Variant of latchTooChild which uses exclusive latches.
     */
    private Node latchToChildEx(Node parent, int childPos) throws IOException {
        return latchChildEx
            (parent, childPos,
             Node.OPTION_CHILD_ACQUIRE_EXCLUSIVE | Node.OPTION_PARENT_RELEASE_EXCLUSIVE);
    }

    /**
     * Variant of latchChildRetainParent which uses exclusive latches.
     */
    private Node latchChildRetainParentEx(Node parent, int childPos) throws IOException {
        return latchChildEx(parent, childPos, Node.OPTION_CHILD_ACQUIRE_EXCLUSIVE);
    }

    /**
     * Variant of latchChild which uses exclusive latches.
     *
     * @param options Node.OPTION_CHILD_ACQUIRE_EXCLUSIVE optionally combined with
     * Node.OPTION_PARENT_RELEASE_EXCLUSIVE
     */
    private Node latchChildEx(Node parent, int childPos, int options) throws IOException {
        long childId = parent.retrieveChildRefId(childPos);
        Node childNode = mTree.mDatabase.nodeMapGet(childId);

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
                    if ((options & Node.OPTION_PARENT_RELEASE_EXCLUSIVE) != 0) {
                        parent.releaseExclusive();
                    }
                    try {
                        childNode.write(mTree.mDatabase.mPageDb);
                    } catch (Throwable e) {
                        childNode.releaseExclusive();
                        throw e;
                    }
                    childNode.mCachedState = Node.CACHED_CLEAN;
                } else if ((options & Node.OPTION_PARENT_RELEASE_EXCLUSIVE) != 0) {
                    parent.releaseExclusive();
                }
                childNode.used();
                return childNode;
            }
        }

        return parent.loadChild(mTree.mDatabase, childId, options);
    }
}
