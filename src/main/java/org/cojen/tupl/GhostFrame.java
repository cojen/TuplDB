/*
 *  Copyright (C) 2018 Cojen.org
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

package org.cojen.tupl;

import org.cojen.tupl.util.Latch;

/**
 * Special frame type for tracking ghosted entries within leaf nodes. Unlike regular frames,
 * ghost frames don't prevent the bound node from being evicted. This class can also be used to
 * perform custom unlock actions. Override the action method, and don't actually bind the frame
 * to anything. Register the custom action with the LockManager.ghosted method.
 *
 * @author Brian S O'Neill
 */
/*P*/
class GhostFrame extends CursorFrame {
    /**
     * @param latch latch which guards the lock; might be briefly released and re-acquired
     * @param lock lock which references this ghost frame
     */
    void action(LocalDatabase db, Latch latch, Lock lock) {
        byte[] key = lock.mKey;
        boolean unlatched = false;

        CommitLock.Shared shared = db.commitLock().tryAcquireShared();
        if (shared == null) {
            // Release lock management latch to prevent deadlock.
            latch.releaseExclusive();
            unlatched = true;
            shared = db.commitLock().acquireShared();
        }

        // Note: Unlike regular frames, ghost frames cannot be unbound (popAll) from the node
        // after the node latch is released. If the node latch is released before the frame is
        // unbound, another thread can then evict the node and unbind the ghost frame instances
        // concurrently, which isn't thread-safe and can corrupt the cursor frame list.

        doDelete: try {
            Node node = this.mNode;
            if (node != null) latchNode: {
                if (!unlatched) {
                    while (node.tryAcquireExclusive()) {
                        Node actualNode = this.mNode;
                        if (actualNode == node) {
                            break latchNode;
                        }
                        node.releaseExclusive();
                        node = actualNode;
                        if (node == null) {
                            break latchNode;
                        }
                    }

                    // Release lock management latch to prevent deadlock.
                    latch.releaseExclusive();
                    unlatched = true;
                }

                node = this.acquireExclusiveIfBound();
            }

            if (node == null) {
                // Will need to delete the slow way.
            } else if (!db.isMutable(node)) {
                // Node cannot be dirtied without a full cursor, so delete the slow way.
                CursorFrame.popAll(this);
                node.releaseExclusive();
            } else {
                // Frame is still valid and node is mutable, so perform a quick delete.

                int pos = this.mNodePos;
                if (pos < 0) {
                    // Already deleted.
                    CursorFrame.popAll(this);
                    node.releaseExclusive();
                    break doDelete;
                }

                Split split = node.mSplit;
                if (split == null) {
                    try {
                        if (node.hasLeafValue(pos) == null) {
                            // Ghost still exists, so delete it.
                            node.deleteLeafEntry(pos);
                            node.postDelete(pos, key);
                        }
                    } finally {
                        CursorFrame.popAll(this);
                        node.releaseExclusive();
                    }
                } else {
                    Node sibling;
                    try {
                        sibling = split.latchSiblingEx();
                    } catch (Throwable e) {
                        CursorFrame.popAll(this);
                        node.releaseExclusive();
                        throw e;
                    }

                    try {
                        split.rebindFrame(this, sibling);

                        Node actualNode = this.mNode;
                        int actualPos = this.mNodePos;

                        if (actualNode.hasLeafValue(actualPos) == null) {
                            // Ghost still exists, so delete it.
                            actualNode.deleteLeafEntry(actualPos);
                            // Fix existing frames on original node. Other than potentially the
                            // ghost frame, no frames exist on the sibling.
                            node.postDelete(pos, key);
                        }
                    } finally {
                        // Pop the frames before releasing the latches, preventing other
                        // threads from observing a frame bound to the sibling too soon.
                        CursorFrame.popAll(this);
                        sibling.releaseExclusive();
                        node.releaseExclusive();
                    }
                }

                break doDelete;
            }

            // Delete the ghost the slow way. Open the index, and then search for the ghost.

            if (!unlatched) {
                // Release lock management latch to prevent deadlock.
                latch.releaseExclusive();
                unlatched = true;
            }

            while (true) {
                Index ix = db.anyIndexById(lock.mIndexId);
                if (!(ix instanceof Tree)) {
                    // Assume index was deleted.
                    break;
                }
                TreeCursor c = new TreeCursor((Tree) ix);
                if (c.deleteGhost(key)) {
                    break;
                }
                // Reopen a closed index.
            }
        } catch (Throwable e) {
            // Exception indicates that database is borked. Ghost will get cleaned up when
            // database is re-opened.
            shared.release();
            if (!unlatched) {
                // Release lock management latch to prevent deadlock.
                latch.releaseExclusive();
            }
            try {
                Utils.closeQuietly(lock.mOwner.getDatabase(), e);
            } finally {
                latch.acquireExclusive();
            }
            return;
        }

        shared.release();
        if (unlatched) {
            latch.acquireExclusive();
        }
    }
}
