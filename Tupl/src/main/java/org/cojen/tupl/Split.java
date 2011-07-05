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
 * Short-lived object for capturing the state of a partially completed node split.
 *
 * @author Brian S O'Neill
 */
class Split {
    final boolean mSplitRight;
    private final long mSiblingId;
    private volatile TreeNode mSibling;

    // In many cases a copy of the key is not necessary; a simple reference to
    // the appropriate sub node works fine. This strategy assumes that the sub
    // node will not be compacted before the split is completed, and it cannot
    // tolerate splits which themselves must be split. For this reason, Split
    // is constructed with a copied key.
    private final byte[] mSplitKey;

    /**
     * @param sibling must have exclusive lock when called; is released as a side-effect
     */
    Split(boolean splitRight, TreeNode sibling, byte[] splitKey) {
        mSplitRight = splitRight;
        mSiblingId = sibling.mId;
        mSibling = sibling;
        mSplitKey = splitKey;
        sibling.releaseExclusive();
    }

    /**
     * Allows a search to continue into a split node by selecting the original node or the
     * sibling. If the original node is returned, its shared lock is still held. If the
     * sibling is returned, it will have a shared latch held and the original node's latch
     * is released.
     *
     * @param node node which was split; shared latch must be held
     * @return original node or sibling
     */
    TreeNode selectNodeShared(TreeNodeStore store, byte[] key, TreeNode node) throws IOException {
        TreeNode sibling = mSibling;
        sibling.acquireShared();

        if (mSiblingId != sibling.mId) {
            // Sibling was evicted, which is extremely rare.
            synchronized (this) {
                sibling = mSibling;
                if (mSiblingId != sibling.mId) {
                    sibling.releaseShared();
                    sibling = store.allocLatchedNode();
                    sibling.read(store, mSiblingId);
                    sibling.downgrade();
                    mSibling = sibling;
                }
            }
        }

        TreeNode left, right;
        if (mSplitRight) {
            left = node;
            right = sibling;
        } else {
            left = sibling;
            right = node;
        }

        // Choose right node if split key is less than or equal to search key.

        int compare = Utils.compareKeys(mSplitKey, 0, mSplitKey.length, key, 0, key.length);

        if (compare <= 0) {
            left.releaseShared();
            return right;
        } else {
            right.releaseShared();
            return left;
        }
    }

    /**
     * Allows a search/insert/update to continue into a split node by selecting the
     * original node or the sibling. If the original node is returned, its exclusive lock
     * is still held. If the sibling is returned, it will have an exclusive latch held and
     * the original node's latch is released.
     *
     * @param node node which was split; exclusive latch must be held
     * @return original node or sibling
     */
    TreeNode selectNodeExclusive(TreeNodeStore store, byte[] key, TreeNode node)
        throws IOException
    {
        TreeNode sibling = latchSibling(store);

        TreeNode left, right;
        if (mSplitRight) {
            left = node;
            right = sibling;
        } else {
            left = sibling;
            right = node;
        }

        // Choose right node if split key is less than or equal to search key.

        int compare = Utils.compareKeys(mSplitKey, 0, mSplitKey.length, key, 0, key.length);

        if (compare <= 0) {
            left.releaseExclusive();
            return right;
        } else {
            right.releaseExclusive();
            return left;
        }
    }

    /**
     * Allows a search to continue into a split node by selecting the original
     * node or the sibling. If the original node is returned, its exclusive
     * lock is still held. If the sibling is returned, it will have an
     * exclusive latch held and the original node's latch is released.
     *
     * @param node node which was split; exclusive latch must be held
     * @return original node or sibling
     */
    TreeNode selectLeftNodeExclusive(TreeNodeStore store, TreeNode node) throws IOException {
        if (mSplitRight) {
            return node;
        }
        TreeNode sibling = latchSibling(store);
        node.releaseExclusive();
        return sibling;
    }

    /**
     * Allows a search to continue into a split node by selecting the original
     * node or the sibling. If the original node is returned, its exclusive
     * lock is still held. If the sibling is returned, it will have an
     * exclusive latch held and the original node's latch is released.
     *
     * @param node node which was split; exclusive latch must be held
     * @return original node or sibling
     */
    TreeNode selectRightNodeExclusive(TreeNodeStore store, TreeNode node) throws IOException {
        if (!mSplitRight) {
            return node;
        }
        TreeNode sibling = latchSibling(store);
        node.releaseExclusive();
        return sibling;
    }

    /**
     * @return sibling with exclusive latch held
     */
    TreeNode latchSibling(TreeNodeStore store) throws IOException {
        TreeNode sibling = mSibling;
        sibling.acquireExclusiveUnfair();
        if (mSiblingId != sibling.mId) {
            // Sibling was evicted, which is extremely rare.
            synchronized (this) {
                sibling = mSibling;
                if (mSiblingId != sibling.mId) {
                    sibling.releaseExclusive();
                    sibling = store.allocLatchedNode();
                    sibling.read(store, mSiblingId);
                    mSibling = sibling;
                }
            }
        }
        return sibling;
    }

    /**
     * @param frame frame affected by split; exclusive latch must be held
     */
    void fixFrame(TreeNodeStore store, CursorFrame frame) throws IOException {
        TreeNode node = frame.mNode;
        int pos = frame.mNodePos;

        if (node.isLeaf()) {
            if (pos < 0) {
                // FIXME
                throw new IOException("negative pos");
            }

            if (mSplitRight) {
                int highestPos = node.highestLeafPos();
                if (pos > highestPos) {
                    TreeNode sibling = latchSibling(store);
                    frame.unbind();
                    frame.bind(sibling, pos - highestPos - 2);
                    sibling.releaseExclusive();
                }
            } else {
                TreeNode sibling = latchSibling(store);
                int highestPos = mSibling.highestLeafPos();
                if (pos > highestPos) {
                    frame.mNodePos = pos - highestPos - 2;
                } else {
                    frame.unbind();
                    frame.bind(sibling, pos);
                }
                sibling.releaseExclusive();
            }
            return;
        }

        if (mSplitRight) {
            int highestPos = node.highestInternalPos();
            if (pos > highestPos) {
                TreeNode sibling = latchSibling(store);
                frame.unbind();
                frame.bind(sibling, pos - highestPos - 2);
                sibling.releaseExclusive();
            }
        } else {
            TreeNode sibling = latchSibling(store);
            int highestPos = mSibling.highestInternalPos();
            if (pos > highestPos) {
                frame.mNodePos = pos - highestPos - 2;
            } else {
                frame.unbind();
                frame.bind(sibling, pos);
            }
            sibling.releaseExclusive();
        }
    }

    /**
     * @return length of entry generated by copySplitKeyToParent
     */
    int splitKeyEncodedLength() {
        final int keyLen = mSplitKey.length;
        return ((keyLen <= 128 && keyLen > 0) ? 1 : 2) + keyLen;
    }

    /**
     * @param dest destination page of parent internal node
     * @param destLoc location in destination page
     * @return length of internal node encoded key entry
     */
    int copySplitKeyToParent(final byte[] dest, final int destLoc) {
        final byte[] key = mSplitKey;
        final int keyLen = key.length;

        int loc = destLoc;
        if (keyLen <= 128 && keyLen > 0) {
            dest[loc++] = (byte) (keyLen - 1);
        } else {
            dest[loc++] = (byte) (0x80 | (keyLen >> 8));
            dest[loc++] = (byte) keyLen;
        }
        System.arraycopy(key, 0, dest, loc, keyLen);

        return loc + keyLen - destLoc;
    }
}
