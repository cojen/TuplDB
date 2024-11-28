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

import java.io.IOException;

import java.util.Arrays;

/**
 * Short-lived object for capturing the state of a partially completed node split.
 *
 * @author Brian S O'Neill
 */
final class Split {
    final boolean mSplitRight;
    private final Node mSibling;

    // In many cases a copy of the key is not necessary; a simple reference to
    // the appropriate sub node works fine. This strategy assumes that the sub
    // node will not be compacted before the split is completed. For this
    // reason, Split is always constructed with a copied key.

    private byte[] mFullKey;
    private byte[] mActualKey; // might be fragmented

    Split(boolean splitRight, Node sibling) {
        mSplitRight = splitRight;
        mSibling = sibling;
    }

    /**
     * Set full and actual key.
     */
    final void setKey(Split split) {
        mFullKey = split.mFullKey;
        mActualKey = split.mActualKey;
    }

    /**
     * Set full and actual key.
     */
    final void setKey(BTree tree, byte[] fullKey) throws IOException {
        setKey(tree.mDatabase, fullKey);
    }

    /**
     * Set full and actual key.
     */
    final void setKey(LocalDatabase db, byte[] fullKey) throws IOException {
        byte[] actualKey = fullKey;

        if (Node.calculateAllowedKeyLength(db, fullKey) < 0) {
            // Key must be fragmented.
            actualKey = db.fragmentKey(fullKey);
        }

        mFullKey = fullKey;
        mActualKey = actualKey;
    }

    /**
     * Set full and actual key. Use same instance if not fragmented.
     */
    final void setKey(byte[] fullKey, byte[] actualKey) {
        mFullKey = fullKey;
        mActualKey = actualKey;
    }

    /**
     * @return null if key is not fragmented
     */
    final byte[] fragmentedKey() {
        return mFullKey == mActualKey ? null : mActualKey;
    }

    /**
     * {@literal Compares to the split key, returning <0 if given key is lower, 0 if
     * equal, >0 if greater.}
     */
    final int compare(byte[] key) {
        return Arrays.compareUnsigned(key, mFullKey);
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
    final Node selectNode(Node node, byte[] key) {
        Node sibling = mSibling;
        sibling.acquireShared();

        Node left, right;
        if (mSplitRight) {
            left = node;
            right = sibling;
        } else {
            left = sibling;
            right = node;
        }

        if (compare(key) < 0) {
            right.releaseShared();
            return left;
        } else {
            left.releaseShared();
            return right;
        }
    }

    /**
     * When binding to a node which is in a split state, the position must be adjusted in order
     * for rebindFrame to work properly. The position cannot be used for accessing entries
     * until after rebindFrame is called, or if retrieveLeafValue is called.
     *
     * @param pos non-negative bind position
     * @return adjusted bind position
     */
    final int adjustBindPosition(int pos) {
        if (!mSplitRight) {
            // To prevent the rebind operation from breaking things, the position must be
            // defined as though it was created before the node was split. When rebindFrame is
            // called, the position is moved to the correct location.
            Node sibling = latchSibling();
            pos += sibling.highestPos() + 2;
            sibling.releaseShared();
        }

        return pos;
    }

    /**
     * Retrieves a value from a split node, by selecting the sibling node or by adjusting the
     * bind position. Given position must not be negative.
     *
     * @param node split applies to this node
     * @param pos non-negative bind position
     */
    final byte[] retrieveLeafValue(Node node, int pos) throws IOException {
        if (mSplitRight) {
            int highestPos = node.highestPos();
            if (pos > highestPos) {
                Node sibling = latchSibling();
                try {
                    return sibling.retrieveLeafValue(pos - highestPos - 2);
                } finally {
                    sibling.releaseShared();
                }
            }
        } else {
            Node sibling = latchSibling();
            try {
                int highestPos = sibling.highestPos();
                if (pos <= highestPos) {
                    return sibling.retrieveLeafValue(pos);
                }
                pos = pos - highestPos - 2;
            } finally {
                sibling.releaseShared();
            }
        }

        return node.retrieveLeafValue(pos);
    }

    /**
     * Performs a binary search against the split, returning the position
     * within the original node as if it had not split.
     */
    final int binarySearchLeaf(Node node, byte[] key) throws IOException {
        Node sibling = latchSibling();

        Node left, right;
        if (mSplitRight) {
            left = node;
            right = sibling;
        } else {
            left = sibling;
            right = node;
        }

        int searchPos;
        if (compare(key) < 0) {
            searchPos = left.binarySearch(key);
        } else {
            int highestPos = left.highestLeafPos();
            searchPos = right.binarySearch(key);
            if (searchPos < 0) {
                searchPos = searchPos - highestPos - 2;
            } else {
                searchPos = highestPos + 2 + searchPos;
            }
        }

        sibling.releaseShared();

        return searchPos;
    }

    /**
     * Returns the highest position within the original node as if it had not split.
     */
    final int highestPos(Node node) {
        int pos;
        Node sibling = latchSibling();
        if (node.isLeaf()) {
            pos = node.highestLeafPos() + 2 + sibling.highestLeafPos();
        } else {
            pos = node.highestInternalPos() + 2 + sibling.highestInternalPos();
        }
        sibling.releaseShared();
        return pos;
    }

    /**
     * @return sibling with shared latch held
     */
    final Node latchSibling() {
        Node sibling = mSibling;
        sibling.acquireShared();
        return sibling;
    }

    /**
     * @return sibling with exclusive latch held
     */
    final Node latchSiblingEx() {
        Node sibling = mSibling;
        sibling.acquireExclusive();
        return sibling;
    }

    /**
     * @param frame frame affected by split; exclusive latch for sibling must also be held
     */
    final void rebindFrame(CursorFrame frame, Node sibling) {
        int pos = frame.mNodePos;

        if (mSplitRight) {
            Node frameNode = frame.mNode;
            if (frameNode == null) {
                // Frame is being concurrently unbound.
                return;
            }

            int highestPos = frameNode.highestPos();

            if (pos >= 0) {
                if (pos <= highestPos) {
                    // Nothing to do.
                } else {
                    frame.rebind(sibling, pos - highestPos - 2);
                }
                return;
            }

            pos = ~pos;

            if (pos <= highestPos) {
                // Nothing to do.
                return;
            }

            if (pos == highestPos + 2) {
                byte[] key = frame.mNotFoundKey;
                if (key == null || compare(key) < 0) {
                    // Nothing to do.
                    return;
                }
            }

            frame.rebind(sibling, ~(pos - highestPos - 2));
        } else {
            int highestPos = sibling.highestPos();

            if (pos >= 0) {
                if (pos <= highestPos) {
                    frame.rebind(sibling, pos);
                } else {
                    frame.mNodePos = pos - highestPos - 2;
                }
                return;
            }

            pos = ~pos;

            if (pos <= highestPos) {
                frame.rebind(sibling, ~pos);
                return;
            }

            if (pos == highestPos + 2) {
                byte[] key = frame.mNotFoundKey;
                if (key == null) {
                    return;
                }
                if (compare(key) < 0) {
                    frame.rebind(sibling, ~pos);
                    return;
                }
            }

            frame.mNodePos = ~(pos - highestPos - 2);
        }
    }

    /**
     * Reverses the action of the rebindFrame method. Original and sibling nodes must be held
     * exclusively.
     *
     * @param frame frame still bound to the original node
     */
    final void unrebindOriginalFrame(CursorFrame frame) {
        if (!mSplitRight) {
            int pos = frame.mNodePos;
            int adjust = mSibling.highestPos() + 2;
            if (pos >= 0) {
                pos += adjust;
            } else {
                pos -= adjust;
            }
            frame.mNodePos = pos;
        }
    }

    /**
     * Reverses the action of the rebindFrame method. Original and sibling nodes must be held
     * exclusively.
     *
     * @param frame frame bound to the sibling node
     */
    final void unrebindSiblingFrame(CursorFrame frame, Node original) {
        int pos = frame.mNodePos;
        if (mSplitRight) {
            int adjust = original.highestPos() + 2;
            if (pos >= 0) {
                pos += adjust;
            } else {
                pos -= adjust;
            }
        }
        frame.rebind(original, pos);
    }

    /**
     * @return length of entry generated by copySplitKeyToParent
     */
    final int splitKeyEncodedLength() {
        byte[] actualKey = mActualKey;
        if (actualKey == mFullKey) {
            return Node.calculateKeyLength(actualKey);
        } else {
            return 2 + actualKey.length;
        }
    }

    /**
     * @param destAddr destination page of parent internal node
     * @param destLoc location in destination page
     * @return updated destLoc
     */
    final int copySplitKeyToParent(final long destAddr, final int destLoc) {
        byte[] actualKey = mActualKey;
        if (actualKey == mFullKey) {
            return Node.encodeNormalKey(actualKey, destAddr, destLoc);
        } else {
            return Node.encodeFragmentedKey(actualKey, destAddr, destLoc);
        }
    }
}
