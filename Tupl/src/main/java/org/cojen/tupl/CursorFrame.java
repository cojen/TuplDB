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

/**
 * 
 *
 * @author Brian S O'Neill
 */
class CursorFrame {
    // TreeNode and position this CursorFrame is bound to.
    TreeNode mNode;
    int mNodePos;

    // Previous stack frame. A CursorFrame which is bound to the root TreeNode
    // has no previous frame.
    CursorFrame mPrevFrame;

    // Linked list of CursorFrames bound to a TreeNode.
    CursorFrame mPrevSibling;
    CursorFrame mNextSibling;

    CursorFrame() {
    }

    CursorFrame(CursorFrame prevFrame) {
        mPrevFrame = prevFrame;
    }

    void acquireShared() {
        TreeNode node = mNode;
        while (true) {
            node.acquireShared();
            TreeNode actualNode = mNode;
            if (actualNode == node) {
                return;
            }
            node.releaseShared();
            node = actualNode;
        }
    }

    void acquireExclusive() {
        TreeNode node = mNode;
        while (true) {
            node.acquireExclusive();
            TreeNode actualNode = mNode;
            if (actualNode == node) {
                return;
            }
            node.releaseExclusive();
            node = actualNode;
        }
    }

    /*
    void acquireExclusiveUnfair() {
        TreeNode node = mNode;
        while (true) {
            node.acquireExclusiveUnfair();
            TreeNode actualNode = mNode;
            if (actualNode == node) {
                return;
            }
            node.releaseExclusive();
            node = actualNode;
        }
    }
    */

    // Called with exclusive latch held.
    void bind(TreeNode node, int nodePos) {
        mNode = node;
        mNodePos = nodePos;
        CursorFrame last = node.mLastCursorFrame;
        if (last != null) {
            mPrevSibling = last;
            last.mNextSibling = this;
        }
        node.mLastCursorFrame = this;
    }

    /**
     * Pop this, the top frame, returning the previous one. Called with
     * exclusive latch held, which is released.
     */
    CursorFrame pop() {
        TreeNode node = mNode;

        CursorFrame prev = mPrevSibling;
        CursorFrame next = mNextSibling;
        if (prev != null) {
            prev.mNextSibling = next;
            mPrevSibling = null;
        }
        if (next != null) {
            next.mPrevSibling = prev;
            mNextSibling = null;
        } else {
            node.mLastCursorFrame = prev;
        }

        prev = mPrevFrame;
        node.releaseExclusive();
        return prev;
    }
}
