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
 * 
 *
 * @author Brian S O'Neill
 */
final class CursorFrame {
    // TreeNode and position this CursorFrame is bound to.
    TreeNode mNode;
    int mNodePos;

    // Parent stack frame. A CursorFrame which is bound to the root TreeNode
    // has no parent frame.
    CursorFrame mParentFrame;

    // Linked list of CursorFrames bound to a TreeNode.
    CursorFrame mPrevCousin;
    CursorFrame mNextCousin;

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
    TreeNode acquireSharedUnfair() {
        TreeNode node = mNode;
        while (true) {
            node.acquireSharedUnfair();
            TreeNode actualNode = mNode;
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
    TreeNode acquireExclusiveUnfair() {
        TreeNode node = mNode;
        while (true) {
            node.acquireExclusiveUnfair();
            TreeNode actualNode = mNode;
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
    TreeNode tryAcquireExclusiveUnfair() {
        TreeNode node = mNode;
        while (node.tryAcquireExclusiveUnfair()) {
            TreeNode actualNode = mNode;
            if (actualNode == node) {
                return actualNode;
            }
            node.releaseExclusive();
            node = actualNode;
        }
        return null;
    }

    /** 
     * Bind this frame to a tree node. Called with exclusive latch held.
     */
    void bind(TreeNode node, int nodePos) {
        mNode = node;
        mNodePos = nodePos;
        CursorFrame last = node.mLastCursorFrame;
        if (last != null) {
            mPrevCousin = last;
            last.mNextCousin = this;
        }
        node.mLastCursorFrame = this;
    }

    /** 
     * Unbind this frame from a tree node. Called with exclusive latch held.
     */
    void unbind() {
        CursorFrame prev = mPrevCousin;
        CursorFrame next = mNextCousin;
        if (prev != null) {
            prev.mNextCousin = next;
            mPrevCousin = null;
        }
        if (next != null) {
            next.mPrevCousin = prev;
            mNextCousin = null;
        } else {
            mNode.mLastCursorFrame = prev;
        }
    }

    /**
     * Returns the parent frame. Called with exclusive latch held, which is
     * retained.
     */
    CursorFrame peek() {
        return mParentFrame;
    }

    /**
     * Pop this, the leaf frame, returning the parent frame. Called with
     * exclusive latch held, which is retained.
     */
    CursorFrame pop() {
        unbind();
        CursorFrame parent = mParentFrame;
        mNode = null;
        mParentFrame = null;
        mNotFoundKey = null;
        return parent;
    }

    /**
     * Pop this, the leaf frame, returning void. Called with exclusive latch
     * held, which is retained.
     */
    void popv() {
        unbind();
        mNode = null;
        mParentFrame = null;
        mNotFoundKey = null;
    }

    /**
     * Pop given non-null frame and all parent frames.
     */
    static void popAll(CursorFrame frame) {
        do {
            TreeNode node = frame.acquireExclusiveUnfair();
            frame = frame.pop();
            node.releaseExclusive();
        } while (frame != null);
    }

    /**
     * Copy this frame and all parent frames.
     *
     * @param dest new frame instance to receive copy
     */
    void copyInto(CursorFrame dest) {
        TreeNode node = acquireExclusiveUnfair();
        CursorFrame parent = mParentFrame;

        if (parent != null) {
            node.releaseExclusive();
            CursorFrame parentCopy = new CursorFrame();

            while (true) {
                // Need to check if parent is null, when looping back.
                if (parent != null) {
                    parent.copyInto(parentCopy);
                }

                // Parent can change when tree height is concurrently changing.
                node = acquireExclusiveUnfair();
                final CursorFrame actualParent = mParentFrame;

                if (actualParent == parent) {
                    // Parent frame hasn't changed, so use the copy.
                    if (parent != null) {
                        dest.mParentFrame = parentCopy;
                    }
                    break;
                }

                // Get rid of the stale copy and do over, which should be rare.
                node.releaseExclusive();
                popAll(parentCopy);
                parent = actualParent;
            }
        }

        dest.mNotFoundKey = mNotFoundKey;
        dest.bind(node, mNodePos);
        node.releaseExclusive();
    }
}
