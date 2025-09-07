/*
 *  Copyright (C) 2011-2017 Cojen.org
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.core;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import static org.cojen.tupl.io.Utils.rethrow;

/**
 * Used to bind a cursor to nodes within the tree. A recursive search algorithm creates stack
 * frames as at goes, and a cursor frame is a heap allocated equivalent. The difference is that
 * cursor frames stick around and can move more freely. If another thread concurrently modifies
 * a tree, it sees all the bound cursor frames and makes necessary adjustments to them. This
 * way, a bound cursor never needs to be invalidated even when structural changes are made to
 * the tree.
 *
 * <p>Cursor frames are bound to Nodes, managed by a concurrent linked list. The CursorFrame
 * itself is a node in the linked list. The unbind operation can be performed without holding
 * any sort of latch on the Node, but other operations can require a latch. Although the
 * concurrent linked list design doesn't require this, the latching is necessary to ensure that
 * the Node itself is stable.
 *
 * <p>Nodes which have CursorFrames bound to them cannot be evicted. This ensures that cursors
 * won't malfunction, but it also means that too many bound cursors can cause the cache to
 * become exhausted. In addition, the node eviction prevention allows the unbind operation be
 * safely performed without a node latch. No race condition exists in which a frame binding to
 * a node is mistaken for a binding to another node.
 *
 * @author Brian S O'Neill
 */
class CursorFrame {
    // Under contention a thread will initially spin up to SPIN_LIMIT before yielding, after
    // which it more aggressively spins up to 2 * SPIN_LIMIT before additional yields.
    static final int SPIN_LIMIT = Runtime.getRuntime().availableProcessors() > 1 ? 1 << 10 : 0;

    private static final CursorFrame REBIND_FRAME = new CursorFrame();

    static final VarHandle cNextCousinHandle, cLastHandle;

    static {
        try {
            var lookup = MethodHandles.lookup();

            cNextCousinHandle = lookup.findVarHandle
                (CursorFrame.class, "mNextCousin", CursorFrame.class);

            cLastHandle = lookup.findVarHandle
                (Node.class, "mLastCursorFrame", CursorFrame.class);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    // Linked list of CursorFrames bound to a Node.
    volatile CursorFrame mPrevCousin;
    volatile CursorFrame mNextCousin;

    // Node and position this CursorFrame is bound to.
    Node mNode;
    int mNodePos;

    // Parent stack frame. A CursorFrame which is bound to the root
    // Node has no parent frame.
    CursorFrame mParentFrame;

    // Reference to key which wasn't found. Only used by leaf frames.
    byte[] mNotFoundKey;

    CursorFrame() {
    }

    CursorFrame(CursorFrame parentFrame) {
        mParentFrame = parentFrame;
    }

    /**
     * Acquire a shared latch on this frame's bound node.
     *
     * @return frame node
     */
    final Node acquireShared() {
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
    final Node tryAcquireShared() {
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
    final Node acquireExclusive() {
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
     * @return frame node, or null if not bound
     */
    final Node acquireExclusiveIfBound() {
        Node node = mNode;
        while (node != null) {
            node.acquireExclusive();
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
     * Acquire an exclusive latch on this frame's bound node.
     *
     * @return frame node, or null if not acquired
     */
    final Node tryAcquireExclusive() {
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
    final void adjustParentPosition(int amount) {
        CursorFrame parent = mParentFrame;
        if (parent != null) {
            parent.mNodePos += amount;
        }
    }

    /**
     * Bind this unbound frame to a tree node. Node should be held with a shared or exclusive
     * latch.
     */
    final void bind(Node node, int nodePos) {
        mNode = node;
        mNodePos = nodePos;

        // Next is set to self to indicate that this frame is the last.
        mNextCousin = this;

        for (int trials = SPIN_LIMIT;;) {
            CursorFrame last = node.mLastCursorFrame;
            mPrevCousin = last;
            if (last == null) {
                if (cLastHandle.compareAndSet(node, null, this)) {
                    return;
                }
            } else if (last.mNextCousin == last
                       && cNextCousinHandle.compareAndSet(last, last, this))
            {
                // Note: The above check gets confused if the frame was recycled. Converting a
                // last frame to an interior frame doesn't imply that the frame is owned by the
                // same linked list.

                /*
                  Catch up before replacing the last frame reference. Here's why:

                  (T1 == thread 1 and T2 == thread 2)

                  1. T1 calls bind.
                  2. T1 observes last frame A.
                  3.                                    T2 calls bind.
                  4.                                    T2 observes last frame A.
                  5.                                    T2 appends B to the end. (CAS)
                  6.                                    T2 sets the last frame to B.
                  7.                                    T2 calls unbind.
                  8.                                    T2 observes previous frame A.
                  9.                                    T2 unbinds B as the last frame.
                  10. T1 appends C to the end. (CAS)
                  11. T1 sets the last frame to C.
                  12.                                   T2 sets the last frame to A!

                  Step 12 should ideally occur before step 10, but it absolutely must be
                  performed before step 11. Step 12 could be performed with a CAS, simply
                  giving up if the CAS fails. CAS is expensive, but volatile reads are cheap.

                  This loop stalls T1 before step 11 can proceed. The T1 CAS already succeeded
                  because step 9 completed, having undone the actions of step 5. The next
                  cousin matched A again, allowing the CAS to succeed. However, the unbind has
                  one more step, to set the last frame. By spinning, the effects of step 6 are
                  observed. The last frame was assumed to be A, but now it's B. The spin
                  continues until the effects of step 12 are observed.

                  Note that after the last frame reference has been confirmed to be correct,
                  there's no chance that it can change again. Another thread cannot sneak in
                  because step 10 atomically claims the new last frame within the list.

                */
                while (node.mLastCursorFrame != last) Thread.onSpinWait();

                node.mLastCursorFrame = this;
                return;
            }

            if (--trials < 0) {
                // Spinning too much due to high contention. Back off a tad.
                Thread.yield();
                trials = SPIN_LIMIT << 1;
            } else {
                Thread.onSpinWait();
            }
        }
    }

    /**
     * Bind this frame to a tree node, or moves the position if already bound. Node should be
     * held with a shared or exclusive latch.
     *
     * @throws IllegalStateException if bound to another node
     */
    final void bindOrReposition(Node node, int nodePos) {
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
    final void rebind(Node node, int nodePos) {
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
    private boolean unbind(CursorFrame to) {
        start: for (int trials = SPIN_LIMIT;;) {
            CursorFrame n = this.mNextCousin;

            if (n == null) {
                // Not in the list.
                return false;
            }

            if (n == this) {
                // Unbinding the last frame.
                Node node = mNode;
                if (node != null && node.mLastCursorFrame == this
                    && cNextCousinHandle.compareAndSet(this, n, to))
                {
                    if (node != mNode || node.mLastCursorFrame != this) {
                        // Frame is now locked, but node has changed due to a concurrent
                        // rebinding of this frame. Unlock and try again.
                        mNextCousin = n;
                    } else {
                        // Update previous frame to be the new last frame.
                        while (true) {
                            CursorFrame p = mPrevCousin;
                            if (p == null || (p.mNextCousin == this
                                              && cNextCousinHandle.compareAndSet(p, this, p)))
                            {
                                // Update the last frame reference.
                                node.mLastCursorFrame = p;
                                return true;
                            }
                            Thread.onSpinWait();
                            if (--trials < 0) {
                                // Unlock and try again.
                                mNextCousin = n;
                                continue start;
                            }
                        }
                    }
                }
            } else {
                // Unbinding an interior or first frame.
                if (n.mPrevCousin == this && cNextCousinHandle.compareAndSet(this, n, to)) {
                    // Update next reference chain to skip over the unbound frame.
                    while (true) {
                        CursorFrame p = mPrevCousin;
                        if (p == null || (p.mNextCousin == this
                                          && cNextCousinHandle.compareAndSet(p, this, n)))
                        {
                            // Update previous reference chain to skip over the unbound frame.
                            n.mPrevCousin = p;
                            return true;
                        }
                        Thread.onSpinWait();
                        if (--trials < 0) {
                            // Unlock and try again.
                            mNextCousin = n;
                            continue start;
                        }
                    }
                }
            }

            if (--trials < 0) {
                // Spinning too much due to high contention. Back off a tad.
                Thread.yield();
                trials = SPIN_LIMIT << 1;
            } else {
                Thread.onSpinWait();
            }
        }
    }

    /**
     * Lock this frame to prevent it from being concurrently unbound. No latch is required.
     *
     * @param lock non-null temporary frame to represent locked state
     * @return frame to pass to unlock method, or null if frame is not bound
     */
    final CursorFrame tryLock(CursorFrame lock) {
        for (int trials = SPIN_LIMIT;;) {
            CursorFrame n = this.mNextCousin;

            if (n == null) {
                // Not in the list.
                return null;
            }

            if (n == this) {
                // Locking the last frame.
                Node node = mNode;
                if (node != null && node.mLastCursorFrame == this
                    && cNextCousinHandle.compareAndSet(this, n, lock))
                {
                    if (node != mNode || node.mLastCursorFrame != this) {
                        // Frame is now locked, but node has changed due to a concurrent
                        // rebinding of this frame. Unlock and try again.
                        mNextCousin = n;
                    } else {
                        return n;
                    }
                }
            } else {
                // Locking an interior or first frame.
                if (n.mPrevCousin == this && cNextCousinHandle.compareAndSet(this, n, lock)) {
                    return n;
                }
            }

            if (--trials < 0) {
                // Spinning too much due to high contention. Back off a tad.
                Thread.yield();
                trials = SPIN_LIMIT << 1;
            } else {
                Thread.onSpinWait();
            }
        }
    }

    /**
     * With this frame locked, lock the previous frame. Unlock it using this frame.
     *
     * @param lock non-null temporary frame to represent locked state
     * @return previous frame, or null if no previous frame exists
     */
    final CursorFrame tryLockPrevious(CursorFrame lock) {
        CursorFrame p;
        do {
            p = this.mPrevCousin;
        } while (p != null && (p.mNextCousin != this
                               || !cNextCousinHandle.compareAndSet(p, this, lock)));
        return p;
    }

    /**
     * Unlock a locked frame.
     *
     * @param n non-null next frame, as provided by the tryLock method
     */
    final void unlock(CursorFrame n) {
        mNextCousin = n;
    }

    /**
     * Pop this frame, returning the parent frame. No latch is required.
     */
    final CursorFrame pop() {
        unbind(null);
        CursorFrame parent = mParentFrame;
        mNode = null;
        mParentFrame = null;
        mNotFoundKey = null;
        return parent;
    }

    /**
     * Pop the given non-null frame and all parent frames. No latch is required.
     */
    static void popAll(CursorFrame frame) {
        do {
            frame = frame.mNode == null ? frame.mParentFrame : frame.pop();
        } while (frame != null);
    }

    /**
     * Starting from this parent frame, pop all child frames reaching up to it, keeping this
     * parent frame. No latches are required. Unlike the popAll method, this variant assumes
     * that all frames are properly bound, and that this parent frame will be reached as frames
     * are popped.
     *
     * @param child must be a bound frame of this parent, and must not be the parent itself
     */
    final void popChildren(CursorFrame child) {
        do {
            child = child.pop();
        } while (child != this);
    }

    /**
     * Copy this frame and all parent frames.
     *
     * @param dest new frame instance to receive copy
     */
    final void copyInto(CursorFrame dest) {
        Node node = acquireShared();
        CursorFrame parent = mParentFrame;

        if (parent != null) {
            node.releaseShared();
            var parentCopy = new CursorFrame();

            while (true) {
                // Need to check if parent is null, when looping back.
                if (parent != null) {
                    parent.copyInto(parentCopy);
                }

                // Parent can change when tree height is concurrently changing.
                node = acquireShared();
                final CursorFrame actualParent = mParentFrame;

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
    public final String toString() {
        return Utils.toMiniString(this);
    }
}
