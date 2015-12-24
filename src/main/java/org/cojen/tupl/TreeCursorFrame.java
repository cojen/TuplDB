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

    private static final TreeCursorFrame REBIND_FRAME = new TreeCursorFrame();

    static final AtomicReferenceFieldUpdater<Node, TreeCursorFrame>
        cLastUpdater = AtomicReferenceFieldUpdater.newUpdater
        (Node.class, TreeCursorFrame.class, "mLastCursorFrame");

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
     * Acquire a shared latch on this frame's bound node.
     *
     * @return frame node, or null if not acquired
     */
    Node tryAcquireShared() {
        Node node = mNode;
        while (node.tryAcquireShared()) {
            Node actualNode = mNode;
            if (actualNode == node) {
                return actualNode;
            }
            node.releaseShared();
            node = actualNode;
        }
        return null;
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
     * @param amount +/- 2
     */
    void adjustParentPosition(int amount) {
        TreeCursorFrame parent = mParentFrame;
        if (parent != null) {
            parent.mNodePos += amount;
        }
    }

    /**
     * Bind this unbound frame to a tree node. Node should be held with a shared or exclusive
     * latch.
     */
    void bind(Node node, int nodePos) {
        mNode = node;
        mNodePos = nodePos;

        // Next is set to self to indicate that this frame is the last.
        this.set(this);

        int trials = 0;
        while (true) {
            TreeCursorFrame last = node.mLastCursorFrame;
            mPrevCousin = last;
            if (last == null) {
                if (cLastUpdater.compareAndSet(node, null, this)) {
                    return;
                }
            } else if (last.get() == last) {
                if (last.compareAndSet(last, this)) {
                    // Note: The above check gets confused if the frame was recycled.
                    // Converting a last frame to an interior frame doesn't imply that the
                    // frame is owned by the same linked list.

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
     * Bind this frame to a tree node, or moves the position if already bound. Node should be
     * held with a shared or exclusive latch.
     *
     * @throws IllegalStateException if bound to another node
     */
    void bindOrReposition(Node node, int nodePos) {
        if (mNode == null) {
            bind(node, nodePos);
        } else if (mNode == node) {
            mNodePos = nodePos;
        } else {
            throw new IllegalStateException();
        }
    }

    /** 
     * Rebind this already bound frame to another tree node, unless this frame is no longer
     * valid. Both Nodes should be held with an exclusive latch.
     */
    void rebind(Node node, int nodePos) {
        // Unbind with a special marker, to prevent a concurrent full unbind operation from
        // thinking that the node is already unbound. The marker will force the other thread to
        // wait until the rebind is complete before unbinding.
        if (unbind(REBIND_FRAME)) {
            bind(node, nodePos);
        }
    }

    /**
     * Unbind this frame from a tree node. No latch is required. Unbound frames must not be
     * recycled, unless all nodes involved are latched exclusively. Concurrent recycling can
     * cause the bind method to believe that an observed last frame belongs to its linked
     * list. See comment in the bind method.
     *
     * @param to null to fully unbind and never use frame again, or REBIND_FRAME if rebinding
     */
    private boolean unbind(TreeCursorFrame to) {
        int trials = 0;
        while (true) {
            TreeCursorFrame n = this.get(); // get next frame

            if (n == null) {
                // Not in the list.
                return false;
            }

            if (n == this) {
                // Unbinding the last frame.
                if (this.compareAndSet(n, to)) {
                    // Update previous frame to be the new last frame.
                    TreeCursorFrame p;
                    do {
                        p = this.mPrevCousin;
                    } while (p != null && (p.get() != this || !p.compareAndSet(this, p)));
                    // Catch up before replacing last frame reference.
                    Node node = mNode;
                    while (node.mLastCursorFrame != this);
                    node.mLastCursorFrame = p;
                    return true;
                }
            } else {
                // Unbinding an interior or first frame.
                if (n.mPrevCousin == this && this.compareAndSet(n, to)) {
                    // Update next reference chain to skip over the unbound frame.
                    TreeCursorFrame p;
                    do {
                        p = this.mPrevCousin;
                    } while (p != null && (p.get() != this || !p.compareAndSet(this, n)));
                    // Update previous reference chain to skip over the unbound frame.
                    n.mPrevCousin = p;
                    return true;
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
     * Lock this frame to prevent it from being concurrently unbound. No latch is required.
     *
     * @param lock non-null temporary frame to represent locked state
     * @return frame to pass to unlock method, or null if frame is not bound
     */
    TreeCursorFrame tryLock(TreeCursorFrame lock) {
        // Note: Implementation is a modified version of the unbind method.

        int trials = 0;
        while (true) {
            TreeCursorFrame n = this.get(); // get next frame

            if (n == null) {
                // Not in the list.
                return null;
            }

            if (n == this) {
                // Unbinding the last frame.
                if (this.compareAndSet(n, lock)) {
                    return n;
                }
            } else {
                // Unbinding an interior or first frame.
                if (n.mPrevCousin == this && this.compareAndSet(n, lock)) {
                    return n;
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
     * With this frame locked, lock the previous frame. Unlock it using this frame.
     *
     * @param lock non-null temporary frame to represent locked state
     * @return previous frame, or null if no previous frame exists
     */
    TreeCursorFrame tryLockPrevious(TreeCursorFrame lock) {
        TreeCursorFrame p;
        do {
            p = this.mPrevCousin;
        } while (p != null && (p.get() != this || !p.compareAndSet(this, lock)));
        return p;
    }

    /**
     * Unlock a locked frame.
     *
     * @param n non-null next frame, as provided by the tryLock method
     */
    void unlock(TreeCursorFrame n) {
        this.set(n);
    }

    /**
     * Uncleanly unlink this frame, for performing cursor invalidation. Node must be
     * exclusively held.
     *
     * @return previous frame, possibly null
     */
    TreeCursorFrame unlink() {
        this.set(null);
        TreeCursorFrame prev = mPrevCousin;
        mPrevCousin = null;
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
        unbind(null);
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
        unbind(null);
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
