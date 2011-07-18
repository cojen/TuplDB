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
class CursorFrame {
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
}
