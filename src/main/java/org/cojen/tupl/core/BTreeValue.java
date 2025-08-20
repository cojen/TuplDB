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

import java.math.BigInteger;

import java.util.Arrays;

import org.cojen.tupl.LargeKeyException;
import org.cojen.tupl.LargeValueException;

import static org.cojen.tupl.core.PageOps.*;
import static org.cojen.tupl.core.Utils.*;

/**
 * Core BTreeCursor ValueAccessor implementation.
 *
 * @author Brian S O'Neill
 */
final class BTreeValue {
    // Op ordinals are relevant.
    static final int OP_LENGTH = 0, OP_READ = 1, OP_READ_TO_GAP = 2, OP_SKIP_GAP = 3,
        OP_CLEAR = 4, OP_SET_LENGTH = 5, OP_WRITE = 6;

    private BTreeValue() {}

    /**
     * Determine if any fragment nodes at the given position are outside the compaction zone.
     * Should only be called when an entry exists and is fragmented, although this method
     * double checks these conditions in case the node latch was released and re-acquired.
     *
     * @param frame latched leaf, not split, never released by this method
     * @param isKey true to examine the key instead of the value
     * @param pos logical position within the key or value
     * @param highestNodeId defines the highest node before the compaction zone; anything
     * higher is in the compaction zone
     * @return -2 if position is too high, -1 if no compaction required, 0 if any nodes are
     * in the compaction zone, or else the inline content length to skip
     */
    static int compactCheck(final CursorFrame frame, boolean isKey, long pos, long highestNodeId)
        throws IOException
    {
        final Node node = frame.mNode;

        int nodePos = frame.mNodePos;
        if (nodePos < 0) {
            // Entry doesn't exist.
            return -2;
        }

        final long pageAddr = node.mPageAddr;
        int loc = p_ushortGetLE(pageAddr, node.searchVecStart() + nodePos);

        final int header, len;
        if (isKey) {
            header = p_byteGet(pageAddr, loc++);
            if (header >= 0) {
                // Not fragmented.
                return pos >= (header + 1) ? -2 : -1;
            }

            len = ((header & 0x3f) << 8) | p_ubyteGet(pageAddr, loc++);
        } else {
            // Skip the key.
            loc += Node.keyLengthAtLoc(pageAddr, loc);

            header = p_byteGet(pageAddr, loc++);
            if (header >= 0) {
                // Not fragmented.
                return pos >= header ? -2 : -1;
            }

            if ((header & 0x20) == 0) {
                len = 1 + (((header & 0x1f) << 8) | p_ubyteGet(pageAddr, loc++));
            } else if (header != -1) {
                len = 1 + (((header & 0x0f) << 16)
                           | (p_ubyteGet(pageAddr, loc++) << 8) | p_ubyteGet(pageAddr, loc++));
            } else {
                // Ghost.
                return -2;
            }
        }

        if ((header & Node.ENTRY_FRAGMENTED) == 0) {
            // Not fragmented.
            return pos >= len ? -2 : -1;
        }

        // Read the fragment header, as described by the LocalDatabase.fragment method.

        final int fHeader = p_byteGet(pageAddr, loc++);
        long fLen = LocalDatabase.decodeFullFragmentedValueLength(fHeader, pageAddr, loc);

        if (pos >= fLen) {
            return -2;
        }

        loc = skipFragmentedLengthField(loc, fHeader);

        if ((fHeader & 0x02) != 0) {
            // Inline content.
            final int fInlineLen = p_ushortGetLE(pageAddr, loc);
            pos -= fInlineLen;
            if (pos < 0) {
                // Positioned within inline content.
                return fInlineLen;
            }
            fLen -= fInlineLen;
            loc = loc + 2 + fInlineLen;
        }

        LocalDatabase db = node.getDatabase();

        if ((fHeader & 0x01) == 0) {
            // Direct pointers.
            loc += (((int) pos) / db.pageSize()) * 6;
            final long fNodeId = p_uint48GetLE(pageAddr, loc);
            return fNodeId > highestNodeId ? 0 : -1; 
        }

        // Indirect pointers.

        final long inodeId = p_uint48GetLE(pageAddr, loc);
        if (inodeId == 0) {
            // Sparse value.
            return -1;
        }

        if (inodeId > highestNodeId) {
            return 0;
        }

        Node inode = db.nodeMapLoadFragment(inodeId);
        int level = db.calculateInodeLevels(fLen);

        while (true) {
            level--;
            long levelCap = db.levelCap(level);
            long childNodeId = p_uint48GetLE(inode.mPageAddr, ((int) (pos / levelCap)) * 6);
            inode.releaseShared();
            if (childNodeId > highestNodeId) {
                return 0;
            }
            if (level <= 0 || childNodeId == 0) {
                return -1;
            }
            inode = db.nodeMapLoadFragment(childNodeId);
            pos %= levelCap;
        }
    }

    /**
     * Touches a fragmented key or value by marking the node at the given position dirty.
     * Caller must hold shared commit lock.
     *
     * @param frame latched exclusive; released only if an exception is thrown
     * @param isKey true to examine the key instead of the value
     */
    static void touch(CursorFrame frame, boolean isKey, final long pos) throws IOException {
        Node node = frame.mNode;

        int nodePos = frame.mNodePos;
        if (nodePos < 0) {
            // Value doesn't exist.
            return;
        }

        final long pageAddr = node.mPageAddr;
        int loc = p_ushortGetLE(pageAddr, node.searchVecStart() + nodePos);

        final int header;
        if (isKey) {
            header = p_byteGet(pageAddr, loc);
            if (header >= 0) {
                // Not fragmented.
                return;
            }
            loc += 2;
        } else {
            // Skip the key.
            loc += Node.keyLengthAtLoc(pageAddr, loc);
            header = p_byteGet(pageAddr, loc);
            if (header >= 0) {
                // Not fragmented.
                return;
            }
            if ((header & 0x20) == 0) {
                loc += 2;
            } else if (header != -1) {
                loc += 3;
            } else {
                // ghost
                return;
            }
        }

        if ((header & Node.ENTRY_FRAGMENTED) == 0) {
            // Not fragmented.
            return;
        }

        // Read the fragment header, as described by the LocalDatabase.fragment method.

        final int fHeader = p_byteGet(pageAddr, loc++);
        final long fLen; // length of fragmented value (when fully reconstructed)

        switch ((fHeader >> 2) & 0x03) {
            default -> fLen = p_ushortGetLE(pageAddr, loc);
            case 1 -> fLen = p_intGetLE(pageAddr, loc) & 0xffffffffL;
            case 2 -> fLen = p_uint48GetLE(pageAddr, loc);
            case 3 -> {
                fLen = p_longGetLE(pageAddr, loc);
                if (fLen < 0) {
                    node.releaseExclusive();
                    if (isKey) {
                        throw new LargeKeyException(fLen);
                    } else {
                        throw new LargeValueException(fLen);
                    }
                }
            }
        }

        loc = skipFragmentedLengthField(loc, fHeader);

        int fInlineLen = 0;
        if ((fHeader & 0x02) != 0) {
            // Inline content.
            fInlineLen = p_ushortGetLE(pageAddr, loc);
            if (pos < fInlineLen) {
                return;
            }
            loc += 2;
            // Move location to first page pointer.
            loc += fInlineLen;
        }

        if (pos > fLen) {
            return;
        }

        final LocalDatabase db = node.getDatabase();

        try {
            if ((fHeader & 0x01) == 0) {
                // Direct pointers.
                loc += (((int) (pos - fInlineLen)) / db.pageSize()) * 6;
                final long fNodeId = p_uint48GetLE(pageAddr, loc);
                if (fNodeId != 0) {
                    final Node fNode = db.nodeMapLoadFragmentExclusive(fNodeId, true);
                    try {
                        if (db.markFragmentDirty(fNode)) {
                            p_int48PutLE(pageAddr, loc, fNode.id());
                        }
                    } finally {
                        fNode.releaseExclusive();
                    }
                }
                return;
            }

            // Indirect pointers.

            final long inodeId = p_uint48GetLE(pageAddr, loc);
            if (inodeId == 0) {
                // Sparse value.
                return;
            }

            final Node inode = db.nodeMapLoadFragmentExclusive(inodeId, true);
            try {
                if (db.markFragmentDirty(inode)) {
                    p_int48PutLE(pageAddr, loc, inode.id());
                }
            } catch (Throwable e) {
                throw releaseExclusive(inode, e);
            }

            final int levels = db.calculateInodeLevels(fLen - fInlineLen);

            touchMultilevelFragment(pos - fInlineLen, levels, inode);
        } catch (Throwable e) {
            throw releaseExclusive(node, e);
        }
    }

    /**
     * Caller must hold shared commit lock when using OP_SET_LENGTH or OP_WRITE.
     *
     * @param txn optional transaction for undo operations
     * @param cursor never used by read op (OP_LENGTH and OP_READ)
     * @param frame latched shared for read op, exclusive for write op; released only if an
     * exception is thrown
     * @param b ignored by OP_LENGTH and OP_SKIP_GAP; OP_SET_LENGTH must pass EMPTY_BYTES
     * @return applicable to OP_LENGTH, OP_READ, OP_READ_TO_GAP, and OP_SKIP_GAP
     */
    @SuppressWarnings("fallthrough")
    static long action(LocalTransaction txn, BTreeCursor cursor, CursorFrame frame,
                       int op, long pos, byte[] b, int bOff, long bLen)
        throws IOException
    {
        while (true) {
            Node node = frame.mNode;

            int nodePos = frame.mNodePos;
            if (nodePos < 0) {
                // Value doesn't exist.

                if (op <= OP_CLEAR) {
                    // Handle OP_LENGTH, OP_READ, OP_READ_TO_GAP, OP_SKIP_GAP, and OP_CLEAR.
                    return -1;
                }

                // Handle OP_SET_LENGTH and OP_WRITE.

                if (txn != null) try {
                    txn.pushUncreate(cursor.mTree.mId, cursor.mKey);
                    // No more undo operations to push.
                    txn = null;
                } catch (Throwable e) {
                    throw releaseExclusive(node, e);
                }

                // Method releases latch if an exception is thrown.
                node = cursor.insertBlank(frame, node, pos + bLen);

                if (bLen <= 0) {
                    return 0;
                }

                // Fallthrough and complete the write operation. Need to re-assign nodePos,
                // because the insert operation changed it.
                nodePos = frame.mNodePos;

                if (nodePos < 0) {
                    // Concurrently deleted.
                    return 0;
                }
            }

            long pageAddr = node.mPageAddr;
            int loc = p_ushortGetLE(pageAddr, node.searchVecStart() + nodePos);

            final int kHeaderLoc = loc;

            // Skip the key.
            loc += Node.keyLengthAtLoc(pageAddr, loc);

            final int vHeaderLoc = loc; // location of raw value header
            final int vLen;             // length of raw value sans header

            nonFrag: {
                final int vHeader = p_byteGet(pageAddr, loc++); // header byte of raw value

                decodeLen: if (vHeader >= 0) {
                    vLen = vHeader;
                } else {
                    if ((vHeader & 0x20) == 0) {
                        vLen = 1 + (((vHeader & 0x1f) << 8) | p_ubyteGet(pageAddr, loc++));
                    } else if (vHeader != -1) {
                        vLen = 1 + (((vHeader & 0x0f) << 16)
                                    | (p_ubyteGet(pageAddr, loc++) << 8)
                                    | p_ubyteGet(pageAddr, loc++));
                    } else {
                        // ghost
                        if (op <= OP_CLEAR) {
                            // Handle OP_LENGTH, OP_READ, OP_READ_TO_GAP, and OP_CLEAR.
                            return -1;
                        }

                        // Replace the ghost with an empty value.
                        p_bytePut(pageAddr, vHeaderLoc, 0);
                        vLen = 0;
                        break decodeLen;
                    }

                    if ((vHeader & Node.ENTRY_FRAGMENTED) != 0) {
                        // Value is fragmented.
                        break nonFrag;
                    }
                }

                // Operate against a non-fragmented value.

                switch (op) {
                case OP_LENGTH: default:
                    return vLen;

                case OP_READ: case OP_READ_TO_GAP:
                    if (bLen <= 0 || pos >= vLen) {
                        bLen = 0;
                    } else {
                        bLen = Math.min(vLen - pos, bLen);
                        p_copy(pageAddr, (int) (loc + pos), b, bOff, (int) bLen);
                    }
                    return bLen;

                case OP_SKIP_GAP:
                    return 0;

                case OP_CLEAR:
                    if (pos < vLen) {
                        int iLoc = (int) (loc + pos);
                        int iLen = (int) Math.min(bLen, vLen - pos);
                        if (txn != null) try {
                            txn.pushUnwrite(cursor.mTree.mId, cursor.mKey, pos,
                                            pageAddr, iLoc, iLen);
                        } catch (Throwable e) {
                            throw releaseExclusive(node, e);
                        }
                        p_clear(pageAddr, iLoc, iLoc + iLen);
                    }
                    return 0;

                case OP_SET_LENGTH:
                    if (pos <= vLen) {
                        if (pos == vLen) {
                            return 0;
                        }

                        // Truncate non-fragmented value. 

                        int newLen = (int) pos;
                        int oldLen = vLen;
                        int garbageAccum = oldLen - newLen;

                        if (txn != null) try {
                            txn.pushUnwrite(cursor.mTree.mId, cursor.mKey, pos,
                                            pageAddr, loc + newLen, garbageAccum);
                        } catch (Throwable e) {
                            throw releaseExclusive(node, e);
                        }

                        shift: {
                            final int vShift;

                            if (newLen <= 127) {
                                p_bytePut(pageAddr, vHeaderLoc, newLen);
                                vShift = loc - (vHeaderLoc + 1);
                            } else if (newLen <= 8192) {
                                p_bytePut(pageAddr, vHeaderLoc, 0x80 | ((newLen - 1) >> 8));
                                p_bytePut(pageAddr, vHeaderLoc + 1, newLen - 1);
                                vShift = loc - (vHeaderLoc + 2);
                            } else {
                                p_bytePut(pageAddr, vHeaderLoc, 0xa0 | ((newLen - 1) >> 16));
                                p_bytePut(pageAddr, vHeaderLoc + 1, (newLen - 1) >> 8);
                                p_bytePut(pageAddr, vHeaderLoc + 2, newLen - 1);
                                break shift;
                            }

                            if (vShift > 0) {
                                garbageAccum += vShift;
                                p_copy(pageAddr, loc, pageAddr, loc - vShift, newLen);
                            }
                        }

                        node.garbage(node.garbage() + garbageAccum);
                        return 0;
                    }

                    // Break out for length increase, by appending an empty value.
                    break;

                case OP_WRITE:
                    if (pos < vLen) {
                        final long end = pos + bLen;
                        if (end <= vLen) {
                            // Writing within existing value region.
                            int iLoc = (int) (loc + pos);
                            int iLen = (int) bLen;
                            if (txn != null) try {
                                txn.pushUnwrite(cursor.mTree.mId, cursor.mKey, pos,
                                                pageAddr, iLoc, iLen);
                            } catch (Throwable e) {
                                throw releaseExclusive(node, e);
                            }
                            p_copy(b, bOff, pageAddr, iLoc, iLen);
                            return 0;
                        } else if (pos == 0 && bOff == 0 && bLen == b.length) {
                            // Writing over the entire value and extending.
                            try {
                                BTree tree = cursor.mTree;
                                if (txn != null) {
                                    // Copy whole entry into undo log.
                                    txn.pushUndoStore(tree.mId, UndoLog.OP_UNUPDATE,
                                                      pageAddr, kHeaderLoc,
                                                      loc + vLen - kHeaderLoc);
                                }
                                node.updateLeafValue(tree, nodePos, 0, b);
                            } catch (Throwable e) {
                                throw releaseExclusive(node, e);
                            }
                            if (node.mSplit != null) {
                                // Releases latch if an exception is thrown.
                                node = cursor.mTree.finishSplitCritical(frame, node);
                            }
                            return 0;
                        } else {
                            // Write the overlapping region, and then append the rest.
                            int iLoc = (int) (loc + pos);
                            int iLen = (int) (vLen - pos);
                            if (txn != null) try {
                                txn.pushUnwrite(cursor.mTree.mId, cursor.mKey, pos,
                                                pageAddr, iLoc, iLen);
                            } catch (Throwable e) {
                                throw releaseExclusive(node, e);
                            }
                            p_copy(b, bOff, pageAddr, iLoc, iLen);
                            pos = vLen;
                            bOff += iLen;
                            bLen -= iLen;
                        }
                    }

                    // Break out for append.
                    break;
                }

                // This point is reached for appending to a non-fragmented value. There's all
                // kinds of optimizations that can be performed here, but keep things
                // simple. Delete the old value, insert a blank value, and then update it.

                byte[] oldValue;
                try {
                    if (txn != null) {
                        txn.pushUnextend(cursor.mTree.mId, cursor.mKey, vLen);
                        // No more undo operations to push.
                        txn = null;
                    }

                    oldValue = new byte[vLen];
                    p_copy(pageAddr, loc, oldValue, 0, oldValue.length);

                    node.deleteLeafEntry(nodePos);
                } catch (Throwable e) {
                    throw releaseExclusive(node, e);
                }

                // Fix all bound cursors, including the current one.
                node.postDelete(nodePos, cursor.mKey);

                // Method releases latch if an exception is thrown.
                cursor.insertBlank(frame, node, pos + bLen);

                op = OP_WRITE;

                if (bLen <= 0) {
                    pos = 0;
                    b = oldValue;
                    bOff = 0;
                    bLen = oldValue.length;
                } else {
                    action(null, cursor, frame, OP_WRITE, 0, oldValue, 0, oldValue.length);
                }

                continue;
            }

            // Operate against a fragmented value. First read the fragment header, as described
            // by the LocalDatabase.fragment method.

            int fHeaderLoc; // location of fragmented value header
            int fHeader;    // header byte of fragmented value
            long fLen;      // length of fragmented value (when fully reconstructed)

            fHeaderLoc = loc;
            fHeader = p_byteGet(pageAddr, loc++);

            switch ((fHeader >> 2) & 0x03) {
                default -> fLen = p_ushortGetLE(pageAddr, loc);
                case 1 -> fLen = p_intGetLE(pageAddr, loc) & 0xffffffffL;
                case 2 -> fLen = p_uint48GetLE(pageAddr, loc);
                case 3 -> {
                    fLen = p_longGetLE(pageAddr, loc);
                    if (fLen < 0) {
                        node.release(op >= OP_CLEAR);
                        throw new LargeValueException(fLen);
                    }
                }
            }

            loc = skipFragmentedLengthField(loc, fHeader);

            switch (op) {
            case OP_LENGTH: default:
                return fLen;

            case OP_READ: case OP_READ_TO_GAP: try {
                if (bLen <= 0 || pos >= fLen) {
                    return 0;
                }

                bLen = Math.min(fLen - pos, bLen);
                int total = (int) bLen;

                if ((fHeader & 0x02) != 0) {
                    // Inline content.
                    final int fInlineLen = p_ushortGetLE(pageAddr, loc);
                    loc += 2;
                    final int amt = (int) (fInlineLen - pos);
                    if (amt <= 0) {
                        // Not reading any inline content.
                        pos -= fInlineLen;
                    } else if (bLen <= amt) {
                        p_copy(pageAddr, (int) (loc + pos), b, bOff, (int) bLen);
                        return bLen;
                    } else {
                        p_copy(pageAddr, (int) (loc + pos), b, bOff, amt);
                        bLen -= amt;
                        bOff += amt;
                        pos = 0;
                    }
                    loc += fInlineLen;
                }

                if ((fHeader & 0x01) == 0) {
                    // Direct pointers.
                    final int ipos = (int) pos;
                    final LocalDatabase db = node.getDatabase();
                    final int pageSize = db.pageSize();
                    loc += (ipos / pageSize) * 6;
                    int fNodeOff = ipos % pageSize;
                    while (true) {
                        final int amt = Math.min((int) bLen, pageSize - fNodeOff);
                        final long fNodeId = p_uint48GetLE(pageAddr, loc);
                        if (fNodeId == 0) {
                            // Reading a sparse value.
                            if (op == OP_READ) {
                                Arrays.fill(b, bOff, bOff + amt, (byte) 0);
                            } else {
                                // OP_READ_TO_GAP
                                return total - bLen;
                            }
                        } else {
                            final Node fNode = db.nodeMapLoadFragment(fNodeId);
                            p_copy(fNode.mPageAddr, fNodeOff, b, bOff, amt);
                            fNode.releaseShared();
                        }
                        bLen -= amt;
                        if (bLen <= 0) {
                            return total;
                        }
                        bOff += amt;
                        loc += 6;
                        fNodeOff = 0;
                    }
                }

                // Indirect pointers.

                final long inodeId = p_uint48GetLE(pageAddr, loc);
                if (inodeId == 0) {
                    // Reading a sparse value.
                    if (op == OP_READ) {
                        Arrays.fill(b, bOff, bOff + (int) bLen, (byte) 0);
                    } else {
                        // OP_READ_TO_GAP
                        return total - bLen;
                    }
                } else {
                    LocalDatabase db = node.getDatabase();
                    int levels = db.calculateInodeLevels(fLen);
                    Node inode = db.nodeMapLoadFragment(inodeId);
                    total -= readMultilevelFragments(op, pos, levels, inode, b, bOff, (int) bLen);
                }

                return total;
            } catch (Throwable e) {
                node.releaseShared();
                throw e;
            }

            case OP_SKIP_GAP: {
                long max = fLen - pos;

                if (max <= 0) {
                    return 0;
                }

                if ((fHeader & 0x02) != 0) {
                    // Inline content.
                    final int fInlineLen = p_ushortGetLE(pageAddr, loc);
                    if ((pos -= fInlineLen) < 0) {
                        // Positioned within the inline content area, so no gap.
                        return 0;
                    }
                    loc = loc + 2 + fInlineLen;
                }

                if ((fHeader & 0x01) == 0) {
                    // Direct pointers.
                    final int ipos = (int) pos;
                    final int pageSize = node.getDatabase().pageSize();
                    loc += (ipos / pageSize) * 6;
                    long total = 0;
                    while (p_uint48GetLE(pageAddr, loc) == 0) {
                        // Skipping a sparse value gap.
                        loc += 6;
                        if (total == 0) {
                            // First position might not be at the start of the page, so apply a
                            // correction.
                            total -= ipos % pageSize;
                        }
                        total += pageSize;
                        if (total >= max) {
                            return max;
                        }
                    }
                    return total;
                }

                // Indirect pointers.

                final long inodeId = p_uint48GetLE(pageAddr, loc);
                if (inodeId == 0) {
                    // Skipping a sparse value gap (the whole thing).
                    return max;
                } else {
                    LocalDatabase db = node.getDatabase();
                    int levels = db.calculateInodeLevels(fLen);
                    Node inode = db.nodeMapLoadFragment(inodeId);
                    long remaining = skipMultilevelFragmentGap(pos, levels, inode, max);
                    return max - (remaining & ~(1L << 63));
                }
            }

            case OP_CLEAR: case OP_SET_LENGTH: clearOrTruncate: {
                if (op == OP_CLEAR) {
                    bLen = Math.min(bLen, fLen - pos);
                    if (bLen <= 0) {
                        return 0;
                    }
                } else {
                    if (pos >= fLen) {
                        // Fallthrough to the next case (OP_WRITE) to extend the length.
                        break clearOrTruncate;
                    }

                    if (txn != null) {
                        // When truncating a value which might be sparse at the end, ensure that
                        // the length is fully restored. In all other cases, this is redundant
                        // because the length is restored by writing back the old content. Note
                        // that the "unextend" operation is used here to actually untruncate.
                        txn.pushUnextend(cursor.mTree.mId, cursor.mKey, fLen);
                    }

                    bLen = fLen - pos;
                }

                // Clear range of fragmented value, and then truncate if OP_SET_LENGTH.

                final long finalLength = pos;
                int fInlineLoc = loc;
                int fInlineLen = 0;

                if ((fHeader & 0x02) != 0) {
                    // Inline content.
                    fInlineLen = p_ushortGetLE(pageAddr, loc);
                    fInlineLoc += 2;
                    loc += 2;

                    final long amt = fInlineLen - pos;
                    if (amt > 0) {
                        int iLoc = (int) (loc + pos);
                        if (bLen <= amt) {
                            // Only clearing inline content.
                            int iLen = (int) bLen;
                            if (txn != null) try {
                                txn.pushUnwrite(cursor.mTree.mId, cursor.mKey, pos,
                                                pageAddr, iLoc, iLen);
                            } catch (Throwable e) {
                                throw releaseExclusive(node, e);
                            }
                            p_clear(pageAddr, iLoc, iLoc + iLen);
                            if (op == OP_SET_LENGTH) {
                                // Fragmented value encoding isn't used for pure inline
                                // content, and so this case isn't expected.
                                fHeaderLoc = truncateFragmented
                                    (node, pageAddr, vHeaderLoc, vLen, iLen);
                                updateLengthField(pageAddr, fHeaderLoc, finalLength);
                            }
                            return 0;
                        }
                        int iLen = (int) amt;
                        if (txn != null) {
                            txn.pushUnwrite(cursor.mTree.mId, cursor.mKey, pos,
                                            pageAddr, iLoc, iLen);
                        }
                        p_clear(pageAddr, iLoc, iLoc + iLen);
                        bLen -= amt;
                        pos = fInlineLen;
                    }

                    fLen -= fInlineLen;
                    // Move location to first page pointer.
                    loc += fInlineLen;
                }

                final boolean toEnd = (pos - fInlineLen + bLen) >= fLen;

                if ((fHeader & 0x01) != 0) try {
                    // Indirect pointers.

                    long inodeId = p_uint48GetLE(pageAddr, loc);

                    if (inodeId == 0) {
                        if (op == OP_CLEAR) {
                            return 0;
                        }
                    } else clearNodes: {
                        final LocalDatabase db = node.getDatabase();

                        Node inode = db.nodeMapLoadFragmentExclusive(inodeId, true);
                        try {
                            if (db.markFragmentDirty(inode)) {
                                p_int48PutLE(pageAddr, loc, inode.id());
                            }

                            int levels = db.calculateInodeLevels(fLen - fInlineLen);

                            clearMultilevelFragments
                                (txn, cursor, pos, pos - fInlineLen, levels, inode, bLen, toEnd);

                            if (op == OP_CLEAR) {
                                return 0;
                            }

                            int newLevels = db.calculateInodeLevels(finalLength - fInlineLen);

                            if (newLevels >= levels) {
                                break clearNodes;
                            }

                            do {
                                long childNodeId = p_uint48GetLE(inode.mPageAddr, 0);

                                if (childNodeId == 0) {
                                    inodeId = 0;
                                    break;
                                }

                                Node childNode;
                                try {
                                    childNode = db.nodeMapLoadFragmentExclusive(childNodeId, true);
                                } catch (Throwable e) {
                                    inode.releaseExclusive();
                                    // Panic.
                                    db.close(e);
                                    throw e;
                                }

                                Node toDelete = inode;
                                inode = childNode;
                                db.deleteNode(toDelete);
                                inodeId = inode.id();
                            } while (--levels > newLevels);

                            p_int48PutLE(pageAddr, loc, inodeId);

                            if (newLevels <= 0) {
                                if (pos == 0) {
                                    // Convert to an empty value.
                                    p_bytePut(pageAddr, vHeaderLoc, 0);
                                    int garbageAccum = fHeaderLoc - vHeaderLoc + vLen - 1;
                                    node.garbage(node.garbage() + garbageAccum);
                                    db.deleteNode(inode);
                                    break clearNodes;
                                }
                                // Convert to direct pointer format.
                                p_bytePut(pageAddr, fHeaderLoc, fHeader & ~0x01);
                            }
                        } finally {
                            inode.releaseExclusive();
                        }
                    }

                    updateLengthField(pageAddr, fHeaderLoc, finalLength);
                    return 0;
                } catch (Throwable e) {
                    throw releaseExclusive(node, e);
                }

                // Direct pointers.

                final LocalDatabase db = node.getDatabase();
                final int ipos = (int) (pos - fInlineLen);
                final int pageSize = db.pageSize();
                loc += (ipos / pageSize) * 6;
                int fNodeOff = ipos % pageSize;

                int firstDeletedLoc = loc;

                while (true) try {
                    final int amt = Math.min((int) bLen, pageSize - fNodeOff);
                    final long fNodeId = p_uint48GetLE(pageAddr, loc);

                    if (fNodeId != 0) {
                        if (amt >= pageSize || toEnd && fNodeOff <= 0) {
                            // Full clear of inode.
                            if (txn == null) {
                                db.deleteFragment(fNodeId);
                            } else {
                                Node fNode = db.nodeMapLoadFragmentExclusive(fNodeId, true);
                                try {
                                    txn.pushUnwrite(cursor.mTree.mId, cursor.mKey, pos,
                                                    fNode.mPageAddr, 0, amt);
                                } catch (Throwable e) {
                                    fNode.releaseExclusive();
                                }
                                db.deleteNode(fNode);
                            }
                            p_int48PutLE(pageAddr, loc, 0);
                        } else {
                            // Partial clear of inode.
                            Node fNode = db.nodeMapLoadFragmentExclusive(fNodeId, true);
                            try {
                                if (db.markFragmentDirty(fNode)) {
                                    p_int48PutLE(pageAddr, loc, fNode.id());
                                }
                                var fNodePageAddr = fNode.mPageAddr;
                                if (txn != null) {
                                    txn.pushUnwrite(cursor.mTree.mId, cursor.mKey, pos,
                                                    fNodePageAddr, fNodeOff, amt);
                                }
                                p_clear(fNode.mPageAddr, fNodeOff, fNodeOff + amt);
                            } finally {
                                fNode.releaseExclusive();
                            }
                            firstDeletedLoc += 6;
                        }
                    }

                    bLen -= amt;
                    loc += 6;

                    if (bLen <= 0) {
                        if (op == OP_SET_LENGTH) {
                            int shrinkage = loc - firstDeletedLoc;
                            if (ipos <= 0) {
                                // All pointers are gone, so convert to a normal value.
                                int len = (int) finalLength;
                                shrinkage = shrinkage - ipos + fInlineLen - len;
                                fragmentedToNormal
                                    (node, pageAddr, vHeaderLoc, fInlineLoc, len, shrinkage);
                            } else {
                                fHeaderLoc = truncateFragmented
                                    (node, pageAddr, vHeaderLoc, vLen, shrinkage);
                                updateLengthField(pageAddr, fHeaderLoc, finalLength);
                            }
                        }

                        return 0;
                    }

                    pos += amt;
                    fNodeOff = 0;
                } catch (Throwable e) {
                    throw releaseExclusive(node, e);
                }
            }

            case OP_WRITE:
                int fInlineLen = 0;
                if ((fHeader & 0x02) != 0) {
                    // Inline content.
                    fInlineLen = p_ushortGetLE(pageAddr, loc);
                    loc += 2;
                    final long amt = fInlineLen - pos;
                    if (amt > 0) {
                        int iLoc = (int) (loc + pos);
                        if (bLen <= amt) {
                            // Only writing inline content.
                            int iLen = (int) bLen;
                            if (txn != null) {
                                txn.pushUnwrite(cursor.mTree.mId, cursor.mKey, pos,
                                                pageAddr, iLoc, iLen);
                            }
                            p_copy(b, bOff, pageAddr, iLoc, iLen);
                            return 0;
                        }
                        // Write just the inline region, and then never touch it again if
                        // continued to the outermost loop.
                        int iLen = (int) amt;
                        if (txn != null) try {
                            txn.pushUnwrite(cursor.mTree.mId, cursor.mKey, pos,
                                            pageAddr, iLoc, iLen);
                        } catch (Throwable e) {
                            throw releaseExclusive(node, e);
                        }
                        p_copy(b, bOff, pageAddr, iLoc, iLen);
                        bLen -= iLen;
                        bOff += iLen;
                        pos = fInlineLen;
                    }
                    // Move location to first page pointer.
                    loc += fInlineLen;
                }

                final long endPos = pos + bLen;
                final LocalDatabase db;
                final int pageSize;

                if (endPos <= fLen) {
                    if (endPos < 0) {
                        node.releaseExclusive();
                        throw new IllegalArgumentException("Length overflow");
                    }

                    // Value doesn't need to be extended.

                    if (bLen == 0) {
                        return 0;
                    }

                    db = node.getDatabase();

                    if ((fHeader & 0x01) != 0) try {
                        // Indirect pointers.

                        final int levels = db.calculateInodeLevels(fLen - fInlineLen);
                        final Node inode = prepareMultilevelWrite(db, pageAddr, loc);
                        writeMultilevelFragments
                            (txn, cursor, pos, pos - fInlineLen,
                             levels, inode, b, bOff, (int) bLen);

                        return 0;
                    } catch (Throwable e) {
                        throw releaseExclusive(node, e);
                    }

                    pageSize = db.pageSize();
                } else {
                    // Extend the value.

                    int fieldGrowth = lengthFieldGrowth(fHeader, endPos);

                    if (fieldGrowth > 0) {
                        tryIncreaseLengthField(cursor, frame, kHeaderLoc, vHeaderLoc, vLen,
                                               fHeaderLoc, fieldGrowth);
                        continue;
                    }

                    db = node.getDatabase();

                    if ((fHeader & 0x01) != 0) try {
                        // Extend the value with indirect pointers.

                        if (txn != null) {
                            txn.pushUnextend(cursor.mTree.mId, cursor.mKey, fLen);
                            if (pos >= fLen) {
                                // Write is fully contained in the extended region, so no more
                                // undo is required.
                                txn = null;
                            }
                        }

                        Node inode = prepareMultilevelWrite(db, pageAddr, loc);

                        // Levels required before extending...
                        int levels = db.calculateInodeLevels(fLen - fInlineLen);

                        // Compare to new full indirect length.
                        long newLen = endPos - fInlineLen;

                        if (db.levelCap(levels) < newLen) {
                            // Need to add more inode levels.
                            int newLevels = db.calculateInodeLevels(newLen);
                            if (newLevels <= levels) {
                                throw new AssertionError();
                            }

                            pageSize = db.pageSize();

                            var newNodes = new Node[newLevels - levels];
                            for (int i=0; i<newNodes.length; i++) {
                                try {
                                    newNodes[i] = db.allocDirtyFragmentNode();
                                } catch (Throwable e) {
                                    try {
                                        // Clean up the mess.
                                        while (--i >= 0) {
                                            db.deleteNode(newNodes[i], true);
                                        }
                                    } catch (Throwable e2) {
                                        suppress(e, e2);
                                        db.close(e);
                                    }
                                    throw e;
                                }
                            }

                            for (Node upper : newNodes) {
                                var upageAddr = upper.mPageAddr;
                                p_int48PutLE(upageAddr, 0, inode.id());
                                inode.releaseExclusive();
                                // Zero-fill the rest.
                                p_clear(upageAddr, 6, pageSize);
                                inode = upper;
                            }

                            levels = newLevels;

                            p_int48PutLE(pageAddr, loc, inode.id());
                        }

                        updateLengthField(pageAddr, fHeaderLoc, endPos);

                        writeMultilevelFragments
                            (txn, cursor, pos, pos - fInlineLen,
                             levels, inode, b, bOff, (int) bLen);

                        return 0;
                    } catch (Throwable e) {
                        throw releaseExclusive(node, e);
                    }

                    // Extend the value with direct pointers.

                    pageSize = db.pageSize();

                    long ptrGrowth = pointerCount(pageSize, endPos - fInlineLen)
                        - pointerCount(pageSize, fLen - fInlineLen);

                    if (ptrGrowth > 0) {
                        int newLoc = tryExtendDirect(cursor, frame, kHeaderLoc, vHeaderLoc, vLen,
                                                     fHeaderLoc, ptrGrowth * 6);

                        // Note: vHeaderLoc is now wrong and cannot be used.

                        if (newLoc < 0) {
                            // Format conversion or latch re-acquisition.
                            continue;
                        }

                        // Page reference might have changed.
                        pageAddr = node.mPageAddr;

                        // Fix broken location variables that are still required.
                        int delta = newLoc - fHeaderLoc;
                        loc += delta;

                        fHeaderLoc = newLoc;
                    }

                    if (txn != null) try {
                        txn.pushUnextend(cursor.mTree.mId, cursor.mKey, fLen);
                        if (pos >= fLen) {
                            // Write is fully contained in the extended region, so no more
                            // undo is required.
                            txn = null;
                        }
                    } catch (Throwable e) {
                        throw releaseExclusive(node, e);
                    }

                    updateLengthField(pageAddr, fHeaderLoc, endPos);
                }

                // Direct pointers.

                final int ipos = (int) (pos - fInlineLen);
                loc += (ipos / pageSize) * 6;
                int fNodeOff = ipos % pageSize;

                while (true) try {
                    final int amt = Math.min((int) bLen, pageSize - fNodeOff);
                    if (amt > 0) {
                        final long fNodeId = p_uint48GetLE(pageAddr, loc);
                        if (fNodeId == 0) {
                            // Writing into a sparse value. Allocate a node and point to it.
                            if (txn != null) {
                                txn.pushUnalloc(cursor.mTree.mId, cursor.mKey, pos, amt);
                            }
                            final Node fNode = db.allocDirtyFragmentNode();
                            try {
                                p_int48PutLE(pageAddr, loc, fNode.id());

                                // Now write to the new page, zero-filling the gaps.
                                var fNodePageAddr = fNode.mPageAddr;
                                p_clear(fNodePageAddr, 0, fNodeOff);
                                p_copy(b, bOff, fNodePageAddr, fNodeOff, amt);
                                p_clear(fNodePageAddr, fNodeOff + amt, pageSize);
                            } finally {
                                fNode.releaseExclusive();
                            }
                        } else {
                            // Obtain node from cache, or read it only for partial write.
                            final Node fNode;
                            if (txn == null) {
                                fNode = db.nodeMapLoadFragmentExclusive(fNodeId, amt < pageSize);
                            } else {
                                fNode = db.nodeMapLoadFragmentExclusive(fNodeId, true);
                                try {
                                    txn.pushUnwrite(cursor.mTree.mId, cursor.mKey, pos,
                                                    fNode.mPageAddr, fNodeOff, amt);
                                } catch (Throwable e) {
                                    fNode.releaseExclusive();
                                    throw e;
                                }
                            }

                            try {
                                if (db.markFragmentDirty(fNode)) {
                                    p_int48PutLE(pageAddr, loc, fNode.id());
                                }
                                p_copy(b, bOff, fNode.mPageAddr, fNodeOff, amt);
                            } finally {
                                fNode.releaseExclusive();
                            }
                        }
                        bLen -= amt;
                    }
                    if (bLen <= 0) {
                        return 0;
                    }
                    bOff += amt;
                    pos += pageSize;
                    loc += 6;
                    fNodeOff = 0;
                } catch (Throwable e) {
                    throw releaseExclusive(node, e);

                }
            } // end switch(op)
        }
    }

    /**
     * @param pos value position being read
     * @param level inode level; at least 1
     * @param inode shared latched parent inode; always released by this method
     * @param b slice of complete value being reconstructed
     * @param bLen must be more than zero
     * @return remaining length (is always zero for OP_READ, can be non-zero for OP_READ_TO_GAP)
     */
    private static int readMultilevelFragments(int op, long pos, int level, Node inode,
                                               final byte[] b, int bOff, int bLen)
        throws IOException
    {
        LocalDatabase db = inode.getDatabase();

        start: while (true) {
            long pageAddr = inode.mPageAddr;
            level--;
            long levelCap = db.levelCap(level);

            int poffset = ((int) (pos / levelCap)) * 6;

            // Handle a possible partial read from the first page.
            long ppos = pos % levelCap;

            while (true) {
                long childNodeId = p_uint48GetLE(pageAddr, poffset);
                int len = (int) Math.min(levelCap - ppos, bLen);

                bLen -= len;

                if (childNodeId == 0) {
                    // Reading a sparse value.
                    if (op == OP_READ) {
                        Arrays.fill(b, bOff, bOff + len, (byte) 0);
                        if (bLen <= 0) {
                            inode.releaseShared();
                            return 0;
                        }
                    } else {
                        // OP_READ_TO_GAP
                        inode.releaseShared();
                        return bLen + len;
                    }
                } else {
                    Node childNode;
                    try {
                        childNode = db.nodeMapLoadFragment(childNodeId);
                    } catch (Throwable e) {
                        inode.releaseShared();
                        throw e;
                    }
                    if (level <= 0) {
                        p_copy(childNode.mPageAddr, (int) ppos, b, bOff, len);
                        childNode.releaseShared();
                        if (bLen <= 0) {
                            inode.releaseShared();
                            return 0;
                        }
                    } else {
                        if (bLen <= 0) {
                            // Tail call.
                            inode.releaseShared(); // latch coupling release
                            pos = ppos;
                            inode = childNode;
                            bLen = len;
                            continue start;
                        }
                        int amt;
                        try {
                            amt = readMultilevelFragments(op, ppos, level, childNode, b, bOff, len);
                        } catch (Throwable e) {
                            inode.releaseShared();
                            throw e;
                        }
                        if (amt != 0) {
                            inode.releaseShared();
                            return bLen + amt;
                        }
                    }
                }

                bOff += len;
                poffset += 6;

                // Remaining reads begin at the start of the page.
                ppos = 0;
            }
        }
    }

    /**
     * @param pos value position being read
     * @param level inode level; at least 1
     * @param inode shared latched parent inode; always released by this method
     * @param max max possible length
     * @return remaining length; bit 63 is set if a non-gap was found
     */
    private static long skipMultilevelFragmentGap(long pos, int level, Node inode, long max)
        throws IOException
    {
        LocalDatabase db = inode.getDatabase();

        long pageAddr = inode.mPageAddr;
        level--;
        long levelCap = db.levelCap(level);

        int poffset = ((int) (pos / levelCap)) * 6;

        // Handle a possible partial read from the first page.
        long ppos = pos % levelCap;
        long levelMax = levelCap - ppos;

        while (true) {
            long childNodeId = p_uint48GetLE(pageAddr, poffset);

            if (childNodeId == 0) {
                // Skipping a sparse value gap.
                max -= levelMax;
                if (max <= 0) {
                    inode.releaseShared();
                    return 0;
                }
            } else {
                if (level <= 0) {
                    inode.releaseShared();
                    return max | (1L << 63);
                }

                Node childNode;
                try {
                    childNode = db.nodeMapLoadFragment(childNodeId);
                } catch (Throwable e) {
                    inode.releaseShared();
                    throw e;
                }

                long levelRemaining;
                try {
                    levelRemaining = skipMultilevelFragmentGap(ppos, level, childNode, levelMax);
                } catch (Throwable e) {
                    inode.releaseShared();
                    throw e;
                }

                max -= (levelMax - (levelRemaining & ~(1L << 63)));

                if (max <= 0) {
                    inode.releaseShared();
                    return levelRemaining & (1L << 63);
                }

                if (levelRemaining < 0) {
                    inode.releaseShared();
                    return max | (1L << 63);
                }
            }

            poffset += 6;

            // Remaining reads begin at the start of the page.
            ppos = 0;
            levelMax = levelCap;
        }
    }

    /**
     * @param loc location of root inode in the page
     * @return dirtied root inode with exclusive latch held
     */
    private static Node prepareMultilevelWrite(LocalDatabase db, long pageAddr, int loc)
        throws IOException
    {
        final Node inode;
        final long inodeId = p_uint48GetLE(pageAddr, loc);

        if (inodeId == 0) {
            // Writing into a sparse value. Allocate a node and point to it.
            inode = db.allocDirtyFragmentNode();
            p_clear(inode.mPageAddr, 0, db.pageSize());
        } else {
            inode = db.nodeMapLoadFragmentExclusive(inodeId, true);
            try {
                if (!db.markFragmentDirty(inode)) {
                    // Already dirty, so no need to update the pointer.
                    return inode;
                }
            } catch (Throwable e) {
                throw releaseExclusive(inode, e);
            }
        }

        p_int48PutLE(pageAddr, loc, inode.id());

        return inode;
    }

    /**
     * @param txn optional transaction for undo operations
     * @param pos value position being written
     * @param ppos partial value position being written (same as pos - fInlineLen initially)
     * @param level inode level; at least 1
     * @param inode exclusively latched parent inode; always released by this method
     * @param b slice of complete value being written
     * @param bLen can be zero
     */
    private static void writeMultilevelFragments(LocalTransaction txn, BTreeCursor cursor,
                                                 long pos, long ppos, int level, Node inode,
                                                 final byte[] b, int bOff, int bLen)
        throws IOException
    {
        LocalDatabase db = inode.getDatabase();

        start: while (true) {
            long pageAddr = inode.mPageAddr;
            level--;
            long levelCap = db.levelCap(level);

            int poffset = ((int) (ppos / levelCap)) * 6;

            // Handle a possible partial write to the first page.
            ppos %= levelCap;

            final int pageSize = db.pageSize();

            while (true) {
                long childNodeId = p_uint48GetLE(pageAddr, poffset);
                int len = (int) Math.min(levelCap - ppos, bLen);

                bLen -= len;

                if (level <= 0) {
                    final Node childNode;
                    setPtr: {
                        try {
                            if (childNodeId == 0) {
                                // Writing into a sparse value. Allocate a node and point to it.
                                if (txn != null) {
                                    txn.pushUnalloc(cursor.mTree.mId, cursor.mKey, pos, len);
                                }
                                childNode = db.allocDirtyFragmentNode();
                                if (ppos > 0 || len < pageSize) {
                                    // New page must be zero-filled.
                                    p_clear(childNode.mPageAddr, 0, pageSize);
                                }
                            } else {
                                if (txn == null) {
                                    // Obtain node from cache, or read it only for partial write.
                                    childNode = db.nodeMapLoadFragmentExclusive
                                        (childNodeId, ppos > 0 || len < pageSize);
                                } else {
                                    // Obtain node from cache, fully reading it for the undo log.
                                    childNode = db.nodeMapLoadFragmentExclusive(childNodeId, true);
                                    txn.pushUnwrite(cursor.mTree.mId, cursor.mKey, pos,
                                                    childNode.mPageAddr, (int) ppos, len);
                                }

                                try {
                                    if (!db.markFragmentDirty(childNode)) {
                                        // Already dirty, so no need to update the pointer.
                                        break setPtr;
                                    }
                                } catch (Throwable e) {
                                    throw releaseExclusive(childNode, e);
                                }
                            }
                        } catch (Throwable e) {
                            throw releaseExclusive(inode, e);
                        }

                        p_int48PutLE(pageAddr, poffset, childNode.id());
                    }

                    p_copy(b, bOff, childNode.mPageAddr, (int) ppos, len);
                    childNode.releaseExclusive();

                    if (bLen <= 0) {
                        inode.releaseExclusive();
                        return;
                    }
                } else {
                    final Node childNode;
                    setPtr: {
                        try {
                            if (childNodeId == 0) {
                                // Writing into a sparse value. Allocate a node and point to it.
                                childNode = db.allocDirtyFragmentNode();
                                // New page must be zero-filled.
                                p_clear(childNode.mPageAddr, 0, pageSize);
                            } else {
                                // Obtain node from cache, fully reading if necessary.
                                childNode = db.nodeMapLoadFragmentExclusive(childNodeId, true);
                                try {
                                    if (!db.markFragmentDirty(childNode)) {
                                        // Already dirty, so no need to update the pointer.
                                        break setPtr;
                                    }
                                } catch (Throwable e) {
                                    throw releaseExclusive(childNode, e);
                                }
                            }
                        } catch (Throwable e) {
                            throw releaseExclusive(inode, e);
                        }

                        p_int48PutLE(pageAddr, poffset, childNode.id());
                    }

                    if (bLen <= 0) {
                        // Tail call.
                        inode.releaseExclusive(); // latch coupling release
                        inode = childNode;
                        bLen = len;
                        continue start;
                    }

                    try {
                        writeMultilevelFragments
                            (txn, cursor, pos, ppos, level, childNode, b, bOff, len);
                    } catch (Throwable e) {
                        throw releaseExclusive(inode, e);
                    }
                }

                pos += len;
                bOff += len;
                poffset += 6;

                // Remaining writes begin at the start of the page.
                ppos = 0;
            }
        }
    }

    /**
     * @param txn optional transaction for undo operations
     * @param pos value position being cleared
     * @param ppos partial value position being cleared (same as pos - fInlineLen initially)
     * @param level inode level; at least 1
     * @param inode exclusively latched parent inode; never released by this method
     * @param clearLen length to clear
     * @param toEnd true if clearing to the end of the value
     */
    private static void clearMultilevelFragments(LocalTransaction txn, BTreeCursor cursor,
                                                 long pos, long ppos, int level, final Node inode,
                                                 long clearLen, boolean toEnd)
        throws IOException
    {
        LocalDatabase db = inode.getDatabase();

        long pageAddr = inode.mPageAddr;
        level--;
        long levelCap = db.levelCap(level);

        int poffset = ((int) (ppos / levelCap)) * 6;

        // Handle a possible partial clear of the first page.
        ppos %= levelCap;

        while (true) {
            long len = Math.min(levelCap - ppos, clearLen);
            long childNodeId = p_uint48GetLE(pageAddr, poffset);

            if (childNodeId != 0) {
                if (len >= levelCap || toEnd && ppos <= 0) {
                    // Full clear of inode.
                    full: {
                        Node childNode = null;
                        try {
                            if (level <= 0) {
                                if (txn == null) {
                                    db.deleteFragment(childNodeId);
                                    break full;
                                }
                                childNode = db.nodeMapLoadFragmentExclusive(childNodeId, true);
                                txn.pushUnwrite(cursor.mTree.mId, cursor.mKey, pos,
                                                childNode.mPageAddr, 0, (int) len);
                            } else {
                                childNode = db.nodeMapLoadFragmentExclusive(childNodeId, true);
                                clearMultilevelFragments
                                    (txn, cursor, pos, ppos, level, childNode, len, toEnd);
                            }
                        } catch (Throwable e) {
                            if (childNode != null) {
                                childNode.releaseExclusive();
                            }
                            throw e;
                        }

                        db.deleteNode(childNode);
                    }

                    p_int48PutLE(pageAddr, poffset, 0);
                } else {
                    // Partial clear of inode.
                    Node childNode = db.nodeMapLoadFragmentExclusive(childNodeId, true);
                    try {
                        if (db.markFragmentDirty(childNode)) {
                            p_int48PutLE(pageAddr, poffset, childNode.id());
                        }
                        if (level <= 0) {
                            var childPageAddr = childNode.mPageAddr;
                            if (txn != null) {
                                txn.pushUnwrite(cursor.mTree.mId, cursor.mKey, pos,
                                                childPageAddr, (int) ppos, (int) len);
                            }
                            p_clear(childPageAddr, (int) ppos, (int) (ppos + len));
                        } else {
                            clearMultilevelFragments
                                (txn, cursor, pos, ppos, level, childNode, len, toEnd);
                        }
                    } finally {
                        childNode.releaseExclusive();
                    }
                }
            }

            clearLen -= len;
            if (clearLen <= 0) {
                break;
            }

            pos += len;
            poffset += 6;

            // Remaining clear steps begin at the start of the page.
            ppos = 0;
        }
    }

    /**
     * @param ppos partial value position being written (same as pos - fInlineLen initially)
     * @param level inode level; at least 1
     * @param inode exclusively latched parent inode; always released by this method
     */
    private static void touchMultilevelFragment(long ppos, int level, Node inode)
        throws IOException
    {
        LocalDatabase db = inode.getDatabase();

        while (true) {
            long pageAddr = inode.mPageAddr;
            level--;
            long levelCap = db.levelCap(level);

            int poffset = ((int) (ppos / levelCap)) * 6;

            final long childNodeId = p_uint48GetLE(pageAddr, poffset);
            if (childNodeId == 0) {
                // Sparse value.
                return;
            }

            final Node childNode;
            try {
                childNode = db.nodeMapLoadFragmentExclusive(childNodeId, true);
                try {
                    if (db.markFragmentDirty(childNode)) {
                        p_int48PutLE(pageAddr, poffset, childNode.id());
                    }
                } catch (Throwable e) {
                    throw releaseExclusive(childNode, e);
                }
            } catch (Throwable e) {
                throw releaseExclusive(inode, e);
            }

            if (level <= 0) {
                childNode.releaseExclusive();
                inode.releaseExclusive();
                return;
            }

            inode.releaseExclusive(); // latch coupling release
            inode = childNode;

            // Handle a possible partial write to the first page.
            ppos %= levelCap;
        }
    }

    /**
     * Returns the amount of bytes which must be added to the length field for the given
     * length.
     *
     * @param fHeader header byte of fragmented value
     * @param fLen new fragmented value length
     * @return 0, 2, 4, or 6
     */
    @SuppressWarnings("fallthrough")
    private static int lengthFieldGrowth(int fHeader, long fLen) {
        int growth = 0;

        switch ((fHeader >> 2) & 0x03) {
        case 0: // (2 byte length field)
            if (fLen < (1L << (2 * 8))) {
                break;
            }
            growth = 2;
        case 1: // (4 byte length field)
            if (fLen < (1L << (4 * 8))) {
                break;
            }
            growth += 2;
        case 2: // (6 byte length field)
            if (fLen < (1L << (6 * 8))) {
                break;
            }
            growth += 2;
        }

        return growth;
    }

    /**
     * Updates the fragmented value length, blindly assuming that the field is large enough.
     *
     * @param fHeaderLoc location of fragmented value header
     * @param fLen new fragmented value length
     */
    private static void updateLengthField(long pageAddr, int fHeaderLoc, long fLen) {
        int fHeader = p_byteGet(pageAddr, fHeaderLoc);

        switch ((fHeader >> 2) & 0x03) {
            case 0 -> // (2 byte length field)
                    p_shortPutLE(pageAddr, fHeaderLoc + 1, (int) fLen);
            case 1 -> // (4 byte length field)
                    p_intPutLE(pageAddr, fHeaderLoc + 1, (int) fLen);
            case 2 -> // (6 byte length field)
                    p_int48PutLE(pageAddr, fHeaderLoc + 1, fLen);
            default -> // (8 byte length field)
                    p_longPutLE(pageAddr, fHeaderLoc + 1, fLen);
        }
    }

    /**
     * Attempt to increase the size of the fragmented length field, for supporting larger
     * sizes. If not enough space exists to increase the size, the value format is converted
     * and the field size doesn't change.
     *
     * As a side effect of calling this method, the page referenced by the node can change when
     * a non-negative value is returned. Caller should always continue from the beginning after
     * calling this method.
     *
     * The node latch is released if an exception is thrown.
     *
     * @param frame latched exclusive and dirtied
     * @param kHeaderLoc location of key header
     * @param vHeaderLoc location of raw value header
     * @param vLen length of raw value sans header
     * @param fHeaderLoc location of fragmented value header
     * @param growth amount of zero bytes to add to the length field (2, 4, or 6)
     */
    private static void tryIncreaseLengthField(final BTreeCursor cursor, final CursorFrame frame,
                                               final int kHeaderLoc,
                                               final int vHeaderLoc, final int vLen,
                                               final int fHeaderLoc, final long growth)
        throws IOException
    {
        final int fOffset = fHeaderLoc - kHeaderLoc;
        final long newEntryLen = fOffset + vLen + growth;
        final Node node = frame.mNode;

        if (newEntryLen > node.getDatabase().mMaxFragmentedEntrySize) {
            compactDirectFormat(cursor, frame, kHeaderLoc, vHeaderLoc, vLen, fHeaderLoc);
            return;
        }

        final BTree tree = cursor.mTree;

        try {
            final int igrowth = (int) growth;
            final var newValue = new byte[vLen + igrowth];
            final long pageAddr = node.mPageAddr;

            // Update the header with the new field size.
            int fHeader = p_byteGet(pageAddr, fHeaderLoc);
            newValue[0] = (byte) (fHeader + (igrowth << 1));

            // Copy the length field.
            int srcLoc = fHeaderLoc + 1;
            int fieldLen = skipFragmentedLengthField(0, fHeader);
            p_copy(pageAddr, srcLoc, newValue, 1, fieldLen);

            // Copy the rest.
            srcLoc += fieldLen;
            int dstLoc = 1 + fieldLen + igrowth;
            p_copy(pageAddr, srcLoc, newValue, dstLoc, newValue.length - dstLoc);

            // Clear the fragmented bit so that the update method doesn't delete the pages.
            p_bytePut(pageAddr, vHeaderLoc,
                      p_byteGet(pageAddr, vHeaderLoc) & ~Node.ENTRY_FRAGMENTED);

            // The fragmented bit is set again by this call.
            node.updateLeafValue(tree, frame.mNodePos, Node.ENTRY_FRAGMENTED, newValue);
        } catch (Throwable e) {
            throw releaseExclusive(node, e);
        }

        if (node.mSplit != null) {
            // Releases latch if an exception is thrown.
            tree.finishSplitCritical(frame, node);
            // Finishing the split causes the node latch to be re-acquired, so start over.
        }

    }

    private static long pointerCount(long pageSize, long len) {
        long count = (len + pageSize - 1) / pageSize;
        if (count < 0) {
            count = pointerCountOverflow(pageSize, len);
        }
        return count;
    }

    private static long pointerCountOverflow(long pageSize, long len) {
        return BigInteger.valueOf(len).add(BigInteger.valueOf(pageSize - 1))
            .subtract(BigInteger.ONE).divide(BigInteger.valueOf(pageSize)).longValue();
    }

    /**
     * Attempt to add bytes to a fragmented value which uses the direct format. If not enough
     * space exists to add direct pointers, the value format is converted and no new pointers
     * are added.
     *
     * As a side effect of calling this method, the page referenced by the node can change when
     * a non-negative value is returned. Also, any locations within the page may changed.
     * Specifically kHeaderLoc, vHeaderLoc, and fHeaderLoc. All will have moved by the same
     * amount as fHeaderLoc.
     *
     * The node latch is released if an exception is thrown.
     *
     * @param frame latched exclusive and dirtied
     * @param kHeaderLoc location of key header
     * @param vHeaderLoc location of raw value header
     * @param vLen length of raw value sans header
     * @param fHeaderLoc location of fragmented value header
     * @param growth amount of zero bytes to append
     * @return updated fHeaderLoc, or negative if caller must continue from the beginning
     * (caused by format conversion or latch re-acquisition)
     */
    private static int tryExtendDirect(final BTreeCursor cursor, final CursorFrame frame,
                                       final int kHeaderLoc,
                                       final int vHeaderLoc, final int vLen,
                                       final int fHeaderLoc, final long growth)
        throws IOException
    {
        int fOffset = fHeaderLoc - kHeaderLoc;
        final long newEntryLen = fOffset + vLen + growth;
        final Node node = frame.mNode;

        if (newEntryLen > node.getDatabase().mMaxFragmentedEntrySize) {
            compactDirectFormat(cursor, frame, kHeaderLoc, vHeaderLoc, vLen, fHeaderLoc);
            return -1;
        }

        final BTree tree = cursor.mTree;

        int newValueLen = vLen + (int) growth;

        try {
            final var newValue = new byte[newValueLen];
            final long pageAddr = node.mPageAddr;
            p_copy(pageAddr, fHeaderLoc, newValue, 0, vLen);

            // Clear the fragmented bit so that the update method doesn't delete the pages.
            p_bytePut(pageAddr, vHeaderLoc,
                      p_byteGet(pageAddr, vHeaderLoc) & ~Node.ENTRY_FRAGMENTED);

            // The fragmented bit is set again by this call.
            node.updateLeafValue(tree, frame.mNodePos, Node.ENTRY_FRAGMENTED, newValue);
        } catch (Throwable e) {
            throw releaseExclusive(node, e);
        }

        if (node.mSplit != null) {
            // Releases latch if an exception is thrown.
            tree.finishSplitCritical(frame, node);
            // Finishing the split causes the node latch to be re-acquired, so start over.
            return -2;
        }

        if (newValueLen > 8192 && vLen <= 8192) {
            // Raw value header likely changed format and might be one byte longer. This never
            // happens when using the default page size of 4096 bytes, because the raw value
            // doesn't fit. Because the header value might have already been using the longer
            // format, start over. Also see truncateFragmented.
            return -1;
        }

        return p_ushortGetLE(node.mPageAddr, node.searchVecStart() + frame.mNodePos) + fOffset;
    }

    /**
     * @param loc location of fragmented length field
     * @return location of inline content length field or first pointer
     */
    private static int skipFragmentedLengthField(int loc, int fHeader) {
        return loc + 2 + ((fHeader >> 1) & 0x06);
    }

    /**
     * Compacts a fragmented value of direct pointers. Either its inline content is moved
     * completely, or the value is converted to indirect format. Caller should always continue
     * from the beginning after calling this method.
     *
     * The node latch is released if an exception is thrown.
     *
     * @param frame latched exclusive and dirtied
     * @param vHeaderLoc location of raw value header
     * @param vLen length of raw value sans header
     * @param fHeaderLoc location of fragmented value header
     */
    private static void compactDirectFormat(final BTreeCursor cursor, final CursorFrame frame,
                                            final int kHeaderLoc,
                                            final int vHeaderLoc, final int vLen,
                                            final int fHeaderLoc)
        throws IOException
    {
        final Node node = frame.mNode;
        final long pageAddr = node.mPageAddr;

        int loc = fHeaderLoc;
        final int fHeader = p_byteGet(pageAddr, loc++);
        final long fLen = LocalDatabase.decodeFullFragmentedValueLength(fHeader, pageAddr, loc);

        loc = skipFragmentedLengthField(loc, fHeader);

        final int fInlineLen;
        if ((fHeader & 0x02) == 0) {
            fInlineLen = 0;
        } else {
            fInlineLen = p_ushortGetLE(pageAddr, loc);
            loc = loc + 2 + fInlineLen;
        }

        // At this point, loc is at the first direct pointer.

        final int tailLen = fHeaderLoc + vLen - loc; // length of all the direct pointers, in bytes

        final LocalDatabase db = node.getDatabase();
        final int pageSize = db.pageSize();
        final int shrinkage;

        if (fInlineLen > 0) {
            // Move all inline content into the fragment pages, and keep the direct format for
            // now. This avoids pathological cases where so much inline content exists that
            // converting to indirect format doesn't shrink the value. It also means that
            // inline content should never exist when using the indirect format, because the
            // initial store of a large value never creates inline content when indirect.

            if (fInlineLen < 4) {
                // Cannot add a new direct pointer, because there's no room for it in the
                // current entry. So reconstruct the full value and update it.
                byte[] newValue;
                try {
                    byte[] fullValue = db.reconstruct(pageAddr, fHeaderLoc, vLen);
                    int max = db.mMaxFragmentedEntrySize - (vHeaderLoc - kHeaderLoc);
                    // Encode it this time without any inline content.
                    newValue = db.fragment(fullValue, fullValue.length, max, 0);
                } catch (Throwable e) {
                    throw releaseExclusive(node, e);
                }

                try {
                    node.updateLeafValue(cursor.mTree, frame.mNodePos,
                                         Node.ENTRY_FRAGMENTED, newValue);
                } catch (Throwable e) {
                    throw releaseExclusive(node, e);
                }

                if (node.mSplit != null) {
                    // Releases latch if an exception is thrown.
                    cursor.mTree.finishSplitCritical(frame, node);
                }

                return;
            }

            Node leftNode;
            Node rightNode = null;

            try {
                if (pointerCount(pageSize, fLen) * 6 <= tailLen) {
                    // Highest node is underutilized (caused by an earlier truncation), and so
                    // a new node isn't required.
                    shrinkage = 2 + fInlineLen;
                } else {
                    rightNode = db.allocDirtyFragmentNode();
                    p_clear(rightNode.mPageAddr, fInlineLen, pageSize);
                    shrinkage = 2 + fInlineLen - 6;
                }
                leftNode = shiftDirectRight
                    (db, pageAddr, loc, loc + tailLen, fInlineLen, rightNode);
            } catch (Throwable e) {
                node.releaseExclusive();
                try {
                    // Clean up the mess.
                    if (rightNode != null) {
                        db.deleteNode(rightNode, true);
                    }
                } catch (Throwable e2) {
                    suppress(e, e2);
                    db.close(e);
                }
                throw e;
            }

            // Move the inline content in place now that room has been made.
            p_copy(pageAddr, loc - fInlineLen, leftNode.mPageAddr, 0, fInlineLen);
            leftNode.releaseExclusive();

            // Shift page contents over inline content and inline length header.
            p_copy(pageAddr, loc, pageAddr, loc - fInlineLen - 2, tailLen);

            if (rightNode != null) {
                // Reference the new node.
                p_int48PutLE(pageAddr, loc - fInlineLen - 2 + tailLen, rightNode.id());
            }

            // Clear the inline length field.
            p_bytePut(pageAddr, fHeaderLoc, fHeader & ~0x02);
        } else {
            // Convert to indirect format.

            if ((fLen - fInlineLen) > pageSize) {
                Node inode;
                try {
                    inode = db.allocDirtyFragmentNode();
                } catch (Throwable e) {
                    throw releaseExclusive(node, e);
                }

                var ipageAddr = inode.mPageAddr;
                p_copy(pageAddr, loc, ipageAddr, 0, tailLen);
                // Zero-fill the rest.
                p_clear(ipageAddr, tailLen, pageSize);

                // Reference the root inode.
                p_int48PutLE(pageAddr, loc, inode.id());
                inode.releaseExclusive();
            }

            // Switch to indirect format.
            p_bytePut(pageAddr, fHeaderLoc, fHeader | 0x01);

            shrinkage = tailLen - 6;
        }

        // Update the raw value length.
        int newLen = vLen - shrinkage - 1; // minus one as required by field encoding
        int header = p_byteGet(pageAddr, vHeaderLoc);
        if ((header & 0x20) == 0) {
            // Field length is 1..8192.
            p_bytePut(pageAddr, vHeaderLoc, (header & 0xe0) | (newLen >> 8));
            p_bytePut(pageAddr, vHeaderLoc + 1, newLen);
        } else {
            // Field length is 1..1048576.
            p_bytePut(pageAddr, vHeaderLoc, (header & 0xf0) | (newLen >> 16));
            p_bytePut(pageAddr, vHeaderLoc + 1, newLen >> 8);
            p_bytePut(pageAddr, vHeaderLoc + 2, newLen);
        }

        // Update the garbage field.
        node.garbage(node.garbage() + shrinkage);

        if (node.shouldLeafMerge()) {
            // Method always releases the node latch, even if an exception is thrown.
            cursor.mergeLeaf(frame, node);
            frame.acquireExclusive();
            cursor.notSplitDirty(frame);
        }
    }

    /**
     * Shift the entire contents of a direct-format fragmented value to the right.
     *
     * @param startLoc first direct pointer location
     * @param endLoc last direct pointer location (exclusive)
     * @param amount shift amount in bytes
     * @param dstNode optional rightmost fragment node to shift into, latched exclusively
     * @return leftmost fragment node, latched exclusively
     */
    private static Node shiftDirectRight(LocalDatabase db, final long pageAddr,
                                         int startLoc, int endLoc, int amount,
                                         Node dstNode)
        throws IOException
    {
        // First make sure all the fragment nodes are dirtied, in case of an exception.

        final var fNodes = new Node[(endLoc - startLoc) / 6];
        final int pageSize = db.pageSize();

        try {
            boolean requireDest = true;
            for (int i = 0, loc = startLoc; loc < endLoc; i++, loc += 6) {
                long fNodeId = p_uint48GetLE(pageAddr, loc);
                if (fNodeId != 0) {
                    Node fNode = db.nodeMapLoadFragmentExclusive(fNodeId, true);
                    fNodes[i] = fNode;
                    if (db.markFragmentDirty(fNode)) {
                        p_int48PutLE(pageAddr, loc, fNode.id());
                    }
                    requireDest = true;
                } else if (requireDest) {
                    Node fNode = db.allocDirtyFragmentNode();
                    p_clear(fNode.mPageAddr, 0, pageSize);
                    fNodes[i] = fNode;
                    p_int48PutLE(pageAddr, loc, fNode.id());
                    requireDest = false;
                }
            }
        } catch (Throwable e) {
            for (Node fNode : fNodes) {
                if (fNode != null) {
                    fNode.releaseExclusive();
                }
            }
            throw e;
        }

        for (int i = fNodes.length; --i >= 0; ) {
            Node fNode = fNodes[i];
            if (fNode == null) {
                if (dstNode != null) {
                    p_clear(dstNode.mPageAddr, 0, amount);
                    dstNode.releaseExclusive();
                }
            } else {
                var fPageAddr = fNode.mPageAddr;
                if (dstNode != null) {
                    p_copy(fPageAddr, pageSize - amount, dstNode.mPageAddr, 0, amount);
                    dstNode.releaseExclusive();
                }
                p_copy(fPageAddr, 0, fPageAddr, amount, pageSize - amount);
            }
            dstNode = fNode;
        }

        return dstNode;
    }

    /**
     * Convert a fragmented value which has no pointers into a normal non-fragmented value.
     *
     * @param fInlineLoc location of inline content
     * @param fInlineLen length of inline content to keep (normal value length)
     * @param shrinkage amount of bytes freed due to pointer deletion and inline reduction
     */
    private static void fragmentedToNormal(final Node node, final long pageAddr,
                                           final int vHeaderLoc, final int fInlineLoc,
                                           final int fInlineLen, final int shrinkage)
    {
        int loc = vHeaderLoc;

        if (fInlineLen <= 127) {
            p_bytePut(pageAddr, loc++, fInlineLen);
        } else if (fInlineLen <= 8192) {
            p_bytePut(pageAddr, loc++, 0x80 | ((fInlineLen - 1) >> 8));
            p_bytePut(pageAddr, loc++, fInlineLen - 1);
        } else {
            p_bytePut(pageAddr, loc++, 0xa0 | ((fInlineLen - 1) >> 16));
            p_bytePut(pageAddr, loc++, (fInlineLen - 1) >> 8);
            p_bytePut(pageAddr, loc++, fInlineLen - 1);
        }

        p_copy(pageAddr, fInlineLoc, pageAddr, loc, fInlineLen);

        node.garbage(node.garbage() + shrinkage + (fInlineLoc - loc));
    }

    /**
     * Truncate the raw value which encodes a fragmented value.
     *
     * @return updated fHeaderLoc
     */
    private static int truncateFragmented(final Node node, final long pageAddr,
                                          final int vHeaderLoc, final int vLen, int shrinkage)
    {
        final int newLen = vLen - shrinkage;
        int loc = vHeaderLoc;

        // Note: It's sometimes possible to convert from the 3-byte field to the 2-byte field.
        // It requires that the value be shifted over, and so it's not worth the trouble.

        if ((p_byteGet(pageAddr, loc) & 0x20) == 0) {
            p_bytePut(pageAddr, loc++, 0xc0 | ((newLen - 1) >> 8));
        } else {
            p_bytePut(pageAddr, loc++, 0xe0 | ((newLen - 1) >> 16));
            p_bytePut(pageAddr, loc++, (newLen - 1) >> 8);
        }
        p_bytePut(pageAddr, loc++, newLen - 1);

        node.garbage(node.garbage() + shrinkage);

        return loc;
    }

    /**
     * Releases the exclusive latch and rethrows the exception.
     */
    private static RuntimeException releaseExclusive(Node node, Throwable cause) {
        node.releaseExclusive();
        throw rethrow(cause);
    }
}
