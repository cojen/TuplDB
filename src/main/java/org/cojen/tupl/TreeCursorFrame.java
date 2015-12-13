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

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 *
 *
 * @author Brian S O'Neill
 */
// Note: Atomic reference is to the next frame bound to a Node.
final class TreeCursorFrame extends AtomicReference<TreeCursorFrame> {
    private static final int SPIN_LIMIT = Runtime.getRuntime().availableProcessors();

    private static final AtomicReferenceFieldUpdater<Node, TreeCursorFrame>
        cLastUpdater = AtomicReferenceFieldUpdater.newUpdater
        (Node.class, TreeCursorFrame.class, "mLastCursorFrame");

    private static final AtomicReferenceFieldUpdater<TreeCursorFrame, TreeCursorFrame>
        cPrevUpdater = AtomicReferenceFieldUpdater.newUpdater
        (TreeCursorFrame.class, TreeCursorFrame.class, "mPrevCousin");

    // Linked list of TreeCursorFrames bound to a Node. Atomic reference is the next frame.
    volatile TreeCursorFrame mPrevCousin;

    // Node and position this TreeCursorFrame is bound to.
    Node mNode;
    int mNodePos;

    // Parent stack frame. A TreeCursorFrame which is bound to the root
    // Node has no parent frame.
    TreeCursorFrame mParentFrame;

    // Reference to key which wasn't found. Only used by leaf frames.
    byte[] mNotFoundKey;

    TreeCursorFrame() {
    }

    TreeCursorFrame(TreeCursorFrame parentFrame) {
        mParentFrame = parentFrame;
    }

    /**
     * Acquire a shared latch on this frame's bound node.
     *
     * @return frame node
     */
    Node acquireShared() {
        Node node = mNode;
        while (true) {
            node.acquireShared();
            Node actualNode = mNode;
            if (actualNode == node) {
                return actualNode;
            }
            node.releaseShared();
            node = actualNode;
        }
    }

    /**
     * Acquire an exclusive latch on this frame's bound node.
     *
     * @return frame node
     */
    Node acquireExclusive() {
        Node node = mNode;
        while (true) {
            node.acquireExclusive();
            Node actualNode = mNode;
            if (actualNode == node) {
                return actualNode;
            }
            node.releaseExclusive();
            node = actualNode;
        }
    }

    /**
     * Acquire an exclusive latch on this frame's bound node.
     *
     * @return frame node, or null if not acquired
     */
    Node tryAcquireExclusive() {
        Node node = mNode;
        while (node.tryAcquireExclusive()) {
            Node actualNode = mNode;
            if (actualNode == node) {
                return actualNode;
            }
            node.releaseExclusive();
            node = actualNode;
        }
        return null;
    }

    /** 
     * Bind this unbound frame to a tree node. Node should be held with a shared latch.
     */
    void bind(Node node, int nodePos) {
        mNode = node;
        mNodePos = nodePos;

        // Next is set to self to indicate that this frame is the last.
        this.lazySet(this);

        int trials = 0;
        while (true) {
            TreeCursorFrame last = node.mLastCursorFrame;
            cPrevUpdater.lazySet(this, last);
            if (last == null) {
                if (cLastUpdater.compareAndSet(node, null, this)) {
                    return;
                }
            } else if (last.get() == last) {
                if (last.compareAndSet(last, this)) {
                    // Catch up before replacing last frame reference.
                    while (node.mLastCursorFrame != last);
                    node.mLastCursorFrame = this;
                    return;
                }
            }

            trials++;

            if (trials >= SPIN_LIMIT) {
                // Spinning too much due to high contention. Back off a tad.
                Thread.yield();
                trials = 0;
            }
        }
    }

    /** 
     * Bind or rebind this frame to a tree node. Node should be held with a shared latch.
     */
    void rebind(Node node, int nodePos) {
        unbind();
        bind(node, nodePos);
    }

    /** 
     * Unbind this frame from a tree node. No latch is required.
     */
    private void unbind() {
        int trials = 0;
        while (true) {
            TreeCursorFrame n = this.get(); // get next frame

            if (n == null) {
                // Not in the list.
                return;
            }

            if (n == this) {
                // Unbinding the last frame.
                if (this.compareAndSet(n, null)) {
                    // Update previous frame to be the new last frame.
                    TreeCursorFrame p;
                    do {
                        p = this.mPrevCousin;
                    } while  (p != null && (p.get() != this || !p.compareAndSet(this, p)));
                    // Catch up before replacing last frame reference.
                    Node node = mNode;
                    while (node.mLastCursorFrame != this);
                    node.mLastCursorFrame = p;
                    return;
                }
            } else {
                // Unbinding an interior or first frame.
                if (n.mPrevCousin == this && this.compareAndSet(n, null)) {
                    // Update next reference chain to skip over the unbound frame.
                    TreeCursorFrame p;
                    do {
                        p = this.mPrevCousin;
                    } while (p != null && (p.get() != this || !p.compareAndSet(this, n)));
                    // Update previous reference chain to skip over the unbound frame.
                    cPrevUpdater.lazySet(n, p);
                    return;
                }
            }

            trials++;

            if (trials >= SPIN_LIMIT) {
                // Spinning too much due to high contention. Back off a tad.
                Thread.yield();
                trials = 0;
            }
        }
    }

    /**
     * Uncleanly unlink this frame, for performing cursor invalidation. Node must be
     * exclusively held.
     *
     * @return previous frame, possibly null
     */
    TreeCursorFrame unlink() {
        lazySet(null);
        TreeCursorFrame prev = mPrevCousin;
        cPrevUpdater.lazySet(this, null);
        return prev;
    }

    /**
     * Returns the parent frame. No latch is required.
     */
    TreeCursorFrame peek() {
        return mParentFrame;
    }

    /**
     * Pop this, the leaf frame, returning the parent frame. No latch is required.
     */
    TreeCursorFrame pop() {
        unbind();
        TreeCursorFrame parent = mParentFrame;
        mNode = null;
        mParentFrame = null;
        mNotFoundKey = null;
        return parent;
    }

    /**
     * Pop this, the leaf frame, returning void. No latch is required.
     */
    void popv() {
        unbind();
        mNode = null;
        mParentFrame = null;
        mNotFoundKey = null;
    }

    /**
     * Pop the given non-null frame and all parent frames. No latch is required.
     */
    static void popAll(TreeCursorFrame frame) {
        do {
            frame = frame.mNode == null ? frame.mParentFrame : frame.pop();
        } while (frame != null);
    }

    /**
     * Copy this frame and all parent frames.
     *
     * @param dest new frame instance to receive copy
     */
    void copyInto(TreeCursorFrame dest) {
        Node node = acquireShared();
        TreeCursorFrame parent = mParentFrame;

        if (parent != null) {
            node.releaseShared();
            TreeCursorFrame parentCopy = new TreeCursorFrame();

            while (true) {
                // Need to check if parent is null, when looping back.
                if (parent != null) {
                    parent.copyInto(parentCopy);
                }

                // Parent can change when tree height is concurrently changing.
                node = acquireShared();
                final TreeCursorFrame actualParent = mParentFrame;

                if (actualParent == parent) {
                    // Parent frame hasn't changed, so use the copy.
                    if (parent != null) {
                        dest.mParentFrame = parentCopy;
                    }
                    break;
                }

                // Get rid of the stale copy and do over, which should be rare.
                node.releaseShared();
                popAll(parentCopy);
                parent = actualParent;
            }
        }

        dest.mNotFoundKey = mNotFoundKey;
        dest.bind(node, mNodePos);
        node.releaseShared();
    }

    @Override
    public String toString() {
        return getClass().getName() + '@' + Integer.toHexString(hashCode());
    }
}
