/*
 *  Copyright 2011-2012 Brian S O'Neill
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
final class TreeCursorFrame implements java.io.Serializable {
    private static final long serialVersionUID = 1L;

    // Node and position this TreeCursorFrame is bound to.
    Node mNode;
    int mNodePos;

    // Parent stack frame. A TreeCursorFrame which is bound to the root
    // Node has no parent frame.
    TreeCursorFrame mParentFrame;

    // Linked list of TreeCursorFrames bound to a Node.
    TreeCursorFrame mPrevCousin;
    TreeCursorFrame mNextCousin;

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
     * Bind this frame to a tree node. Called with exclusive latch held.
     */
    void bind(Node node, int nodePos) {
        mNode = node;
        mNodePos = nodePos;
        TreeCursorFrame last = node.mLastCursorFrame;
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
        TreeCursorFrame prev = mPrevCousin;
        TreeCursorFrame next = mNextCousin;
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
    TreeCursorFrame peek() {
        return mParentFrame;
    }

    /**
     * Pop this, the leaf frame, returning the parent frame. Called with
     * exclusive latch held, which is retained.
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
    static void popAll(TreeCursorFrame frame) {
        outer: do {
            Node node = frame.mNode;
            while (true) {
                if (node == null) {
                    // Frame was not bound properly, suggesting that cursor is
                    // being cleaned up in response to an exception. Frame
                    // cannot be latched, so just go to the parent.
                    frame = frame.mParentFrame;
                    continue outer;
                }
                node.acquireExclusive();
                Node actualNode = frame.mNode;
                if (actualNode == node) {
                    frame = frame.pop();
                    node.releaseExclusive();
                    continue outer;
                }
                node.releaseExclusive();
                node = actualNode;
            }
        } while (frame != null);
    }

    /**
     * Copy this frame and all parent frames.
     *
     * @param dest new frame instance to receive copy
     */
    void copyInto(TreeCursorFrame dest) {
        Node node = acquireExclusive();
        TreeCursorFrame parent = mParentFrame;

        if (parent != null) {
            node.releaseExclusive();
            TreeCursorFrame parentCopy = new TreeCursorFrame();

            while (true) {
                // Need to check if parent is null, when looping back.
                if (parent != null) {
                    parent.copyInto(parentCopy);
                }

                // Parent can change when tree height is concurrently changing.
                node = acquireExclusive();
                final TreeCursorFrame actualParent = mParentFrame;

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
