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

package org.cojen.tupl;

import java.io.IOException;

import java.math.BigInteger;

import java.util.Arrays;

import static org.cojen.tupl.PageOps.*;
import static org.cojen.tupl.Utils.*;

/**
 * Core TreeCursor Blob implementation.
 *
 * @author Brian S O'Neill
 */
final class TreeValueBlob {
    // Op ordinals are relevant.
    static final int OP_LENGTH = 0, OP_READ = 1, OP_SET_LENGTH = 2, OP_WRITE = 3;

    // Touches a fragment without extending the value length. Used for file compaction.
    static final byte[] TOUCH_VALUE = new byte[0];

    /**
     * Determine if any fragment nodes at the given position are outside the compaction zone.
     *
     * @param frame latched leaf, not split, never released by this method
     * @param highestNodeId defines the highest node before the compaction zone; anything
     * higher is in the compaction zone
     * @return -1 if position is too high, 0 if no compaction required, or 1 if any nodes are
     * in the compaction zone
     */
    static int compactCheck(final CursorFrame frame, long pos, final long highestNodeId)
        throws IOException
    {
        final Node node = frame.mNode;

        int nodePos = frame.mNodePos;
        if (nodePos < 0) {
            // Value doesn't exist.
            return -1;
        }

        final /*P*/ byte[] page = node.mPage;
        int loc = p_ushortGetLE(page, node.searchVecStart() + nodePos);
        // Skip the key.
        loc += Node.keyLengthAtLoc(page, loc);

        int vHeader = p_byteGet(page, loc++);
        if (vHeader >= 0) {
            // Not fragmented.
            return pos >= vHeader ? -1 : 0;
        }

        int len;
        if ((vHeader & 0x20) == 0) {
            len = 1 + (((vHeader & 0x1f) << 8) | p_ubyteGet(page, loc++));
        } else if (vHeader != -1) {
            len = 1 + (((vHeader & 0x0f) << 16)
                       | (p_ubyteGet(page, loc++) << 8) | p_ubyteGet(page, loc++));
        } else {
            // ghost
            return -1;
        }

        if ((vHeader & Node.ENTRY_FRAGMENTED) == 0) {
            // Not fragmented.
            return pos >= len ? -1 : 0;
        }

        // Read the fragment header, as described by the LocalDatabase.fragment method.

        final int fHeader = p_byteGet(page, loc++);
        final long fLen = LocalDatabase.decodeFullFragmentedValueLength(fHeader, page, loc);

        if (pos >= fLen) {
            return -1;
        }

        loc = skipFragmentedLengthField(loc, fHeader);

        if ((fHeader & 0x02) != 0) {
            // Inline content.
            final int fInline = p_ushortGetLE(page, loc);
            if (pos < fInline) {
                // Positioned within inline content.
                return 0;
            }
            pos -= fInline;
            loc = loc + 2 + fInline;
        }

        LocalDatabase db = node.getDatabase();

        if ((fHeader & 0x01) == 0) {
            // Direct pointers.
            loc += (((int) pos) / pageSize(db, page)) * 6;
            final long fNodeId = p_uint48GetLE(page, loc);
            return fNodeId > highestNodeId ? 1 : 0; 
        }

        // Indirect pointers.

        final long inodeId = p_uint48GetLE(page, loc);
        if (inodeId == 0) {
            // Sparse value.
            return 0;
        }

        Node inode = db.nodeMapLoadFragment(inodeId);
        // FIXME: Support zero levels.
        int level = db.calculateInodeLevels(fLen);

        while (true) {
            level--;
            long levelCap = db.levelCap(level);
            long childNodeId = p_uint48GetLE(inode.mPage, ((int) (pos / levelCap)) * 6);
            inode.releaseShared();
            if (childNodeId > highestNodeId) {
                return 1;
            }
            if (level <= 0 || childNodeId == 0) {
                return 0;
            }
            inode = db.nodeMapLoadFragment(childNodeId);
            pos %= levelCap;
        }
    }

    /**
     * Caller must hold shared commit lock when using OP_SET_LENGTH or OP_WRITE.
     *
     * @param frame latched shared for read op, exclusive for write op; released only if an
     * exception is thrown
     * @param b ignored by OP_LENGTH; OP_SET_LENGTH must pass EMPTY_BYTES
     * @return applicable only to OP_LENGTH and OP_READ
     */
    @SuppressWarnings("fallthrough")
    static long action(TreeCursor cursor, CursorFrame frame, int op,
                       long pos, byte[] b, int bOff, int bLen)
        throws IOException
    {
        while (true) {
            Node node = frame.mNode;

            int nodePos = frame.mNodePos;
            if (nodePos < 0) {
                // Value doesn't exist.

                if (op <= OP_READ) {
                    // Handle OP_LENGTH and OP_READ.
                    return -1;
                }

                // Handle OP_SET_LENGTH and OP_WRITE.

                if (b == TOUCH_VALUE) {
                    return 0;
                }

                // Method releases latch if an exception is thrown.
                node = cursor.insertBlank(frame, node, pos + bLen);

                if (bLen <= 0) {
                    return 0;
                }

                // Fallthrough and complete the write operation. Need to re-assign nodePos,
                // because the insert operation changed it.
                nodePos = frame.mNodePos;
            }

            /*P*/ byte[] page = node.mPage;
            int loc = p_ushortGetLE(page, node.searchVecStart() + nodePos);

            final int kHeaderLoc = loc;

            // Skip the key.
            loc += Node.keyLengthAtLoc(page, loc);

            final int vHeaderLoc; // location of raw value header
            final int vHeader;    // header byte of raw value
            final int vLen;       // length of raw value sans header

            vHeaderLoc = loc;
            vHeader = p_byteGet(page, loc++);

            nf: {
                if (vHeader >= 0) {
                    vLen = vHeader;
                } else {
                    if ((vHeader & 0x20) == 0) {
                        vLen = 1 + (((vHeader & 0x1f) << 8) | p_ubyteGet(page, loc++));
                    } else if (vHeader != -1) {
                        vLen = 1 + (((vHeader & 0x0f) << 16)
                                    | (p_ubyteGet(page, loc++) << 8) | p_ubyteGet(page, loc++));
                    } else {
                        // ghost
                        if (op <= OP_READ) {
                            // Handle OP_LENGTH and OP_READ.
                            return -1;
                        }

                        if (b == TOUCH_VALUE) {
                            return 0;
                        }

                        // FIXME: write ops; create the value
                        node.releaseExclusive();
                        throw null;
                    }

                    if ((vHeader & Node.ENTRY_FRAGMENTED) != 0) {
                        // Value is fragmented.
                        break nf;
                    }
                }

                // Operate against a non-fragmented value.

                switch (op) {
                case OP_LENGTH: default:
                    return vLen;

                case OP_READ:
                    if (bLen <= 0 || pos >= vLen) {
                        bLen = 0;
                    } else {
                        bLen = Math.min((int) (vLen - pos), bLen);
                        p_copyToArray(page, (int) (loc + pos), b, bOff, bLen);
                    }
                    return bLen;

                case OP_SET_LENGTH:
                    if (pos <= vLen) {
                        // Truncate length. 

                        int newLen = (int) pos;
                        int oldLen = (int) vLen;
                        int garbageAccum = oldLen - newLen;

                        shift: {
                            final int vLoc;
                            final int vShift;

                            if (newLen <= 127) {
                                p_bytePut(page, vHeaderLoc, newLen);
                                if (oldLen <= 127) {
                                    break shift;
                                } else if (oldLen <= 8192) {
                                    vLoc = vHeaderLoc + 2;
                                    vShift = 1;
                                } else {
                                    vLoc = vHeaderLoc + 3;
                                    vShift = 2;
                                }
                            } else if (newLen <= 8192) {
                                p_bytePut(page, vHeaderLoc, 0x80 | ((newLen - 1) >> 8));
                                p_bytePut(page, vHeaderLoc + 1, newLen - 1);
                                if (oldLen <= 8192) {
                                    break shift;
                                } else {
                                    vLoc = vHeaderLoc + 3;
                                    vShift = 1;
                                }
                            } else {
                                p_bytePut(page, vHeaderLoc, 0xa0 | ((newLen - 1) >> 16));
                                p_bytePut(page, vHeaderLoc + 1, (newLen - 1) >> 8);
                                p_bytePut(page, vHeaderLoc + 2, newLen - 1);
                                break shift;
                            }

                            garbageAccum += vShift;
                            p_copy(page, vLoc, page, vLoc - vShift, newLen);
                        }

                        node.garbage(node.garbage() + garbageAccum);
                        return 0;
                    }

                    // Break out for length increase, by appending an empty value.
                    break;

                case OP_WRITE:
                    if (b == TOUCH_VALUE) {
                        return 0;
                    }

                    if (pos < vLen) {
                        final long end = pos + bLen;
                        if (end <= vLen) {
                            // Writing within existing value region.
                            p_copyFromArray(b, bOff, page, (int) (loc + pos), bLen);
                            return 0;
                        } else if (pos == 0 && bOff == 0 && bLen == b.length) {
                            // Writing over the entire value.
                            try {
                                node.updateLeafValue(frame, cursor.mTree, nodePos, 0, b);
                            } catch (IOException e) {
                                node.releaseExclusive();
                                throw e;
                            }
                            return 0;
                        } else {
                            // Write the overlapping region, and then append the rest.
                            int len = (int) (vLen - pos);
                            p_copyFromArray(b, bOff, page, (int) (loc + pos), len);
                            pos = vLen;
                            bOff += len;
                            bLen -= len;
                        }
                    }

                    // Break out for append.
                    break;
                }

                // This point is reached for appending to a non-fragmented value. There's all
                // kinds of optimizations that can be performed here, but keep things
                // simple. Delete the old value, insert a blank value, and then update it.

                byte[] oldValue = new byte[vLen];
                p_copyToArray(page, loc, oldValue, 0, oldValue.length);

                node.deleteLeafEntry(nodePos);
                frame.mNodePos = ~nodePos;

                // Method releases latch if an exception is thrown.
                cursor.insertBlank(frame, node, pos + bLen);

                op = OP_WRITE;

                if (bLen <= 0) {
                    pos = 0;
                    b = oldValue;
                    bOff = 0;
                    bLen = oldValue.length;
                } else {
                    action(cursor, frame, OP_WRITE, 0, oldValue, 0, oldValue.length);
                }

                continue;
            }

            // Operate against a fragmented value. First read the fragment header, as described
            // by the LocalDatabase.fragment method.

            int fHeaderLoc; // location of fragmented value header
            int fHeader;    // header byte of fragmented value
            long fLen;      // length of fragmented value (when fully reconstructed)

            fHeaderLoc = loc;
            fHeader = p_byteGet(page, loc++);

            switch ((fHeader >> 2) & 0x03) {
            default:
                fLen = p_ushortGetLE(page, loc);
                break;
            case 1:
                fLen = p_intGetLE(page, loc) & 0xffffffffL;
                break;
            case 2:
                fLen = p_uint48GetLE(page, loc);
                break;
            case 3:
                fLen = p_longGetLE(page, loc);
                if (fLen < 0) {
                    node.release(op > OP_READ);
                    throw new LargeValueException(fLen);
                }
                break;
            }

            loc = skipFragmentedLengthField(loc, fHeader);

            switch (op) {
            case OP_LENGTH: default:
                return fLen;

            case OP_READ: try {
                if (bLen <= 0 || pos >= fLen) {
                    return 0;
                }

                bLen = (int) Math.min(fLen - pos, bLen);
                final int total = bLen;

                if ((fHeader & 0x02) != 0) {
                    // Inline content.
                    final int fInline = p_ushortGetLE(page, loc);
                    loc += 2;
                    final int amt = (int) (fInline - pos);
                    if (amt <= 0) {
                        // Not reading any inline content.
                        pos -= fInline;
                    } else if (bLen <= amt) {
                        p_copyToArray(page, (int) (loc + pos), b, bOff, bLen);
                        return bLen;
                    } else {
                        p_copyToArray(page, (int) (loc + pos), b, bOff, amt);
                        bLen -= amt;
                        bOff += amt;
                        pos = 0;
                    }
                    loc += fInline;
                }

                final LocalDatabase db = node.getDatabase();

                if ((fHeader & 0x01) == 0) {
                    // Direct pointers.
                    final int ipos = (int) pos;
                    final int pageSize = pageSize(db, page);
                    loc += (ipos / pageSize) * 6;
                    int fNodeOff = ipos % pageSize;
                    while (true) {
                        final int amt = Math.min(bLen, pageSize - fNodeOff);
                        final long fNodeId = p_uint48GetLE(page, loc);
                        if (fNodeId == 0) {
                            // Reading a sparse value.
                            Arrays.fill(b, bOff, bOff + amt, (byte) 0);
                        } else {
                            final Node fNode = db.nodeMapLoadFragment(fNodeId);
                            p_copyToArray(fNode.mPage, fNodeOff, b, bOff, amt);
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

                final long inodeId = p_uint48GetLE(page, loc);
                if (inodeId == 0) {
                    // Reading a sparse value.
                    Arrays.fill(b, bOff, bOff + bLen, (byte) 0);
                } else {
                    final Node inode = db.nodeMapLoadFragment(inodeId);
                    // FIXME: Support zero levels.
                    final int levels = db.calculateInodeLevels(fLen);
                    readMultilevelFragments(db, pos, levels, inode, b, bOff, bLen);
                }

                return total;
            } catch (IOException e) {
                node.releaseShared();
                throw e;
            }

            case OP_SET_LENGTH:
                long endPos = pos + bLen;
                if (endPos <= fLen) {
                    if (endPos == fLen) {
                        return 0;
                    }
                    // FIXME: truncate
                    node.releaseExclusive();
                    throw null;
                }
                // Fall through to extend the length.

            case OP_WRITE: try {
                endPos = pos + bLen;

                int fInline = 0;
                if ((fHeader & 0x02) != 0) {
                    // Inline content.
                    fInline = p_ushortGetLE(page, loc);
                    loc += 2;
                    final long amt = fInline - pos;
                    if (amt > 0) {
                        if (bLen <= amt) {
                            // Only writing inline content.
                            p_copyFromArray(b, bOff, page, (int) (loc + pos), bLen);
                            return 0;
                        }
                        // Write just the inline region, and then never touch it again if
                        // continued to the outermost loop.
                        p_copyFromArray(b, bOff, page, (int) (loc + pos), (int) amt);
                        bLen -= amt;
                        bOff += amt;
                        pos = fInline;
                    }
                    // Move location to first page pointer.
                    loc += fInline;
                }

                final LocalDatabase db;
                final int pageSize;

                if (endPos <= fLen) {
                    // Value doesn't need to be extended.

                    if (bLen == 0 && b != TOUCH_VALUE) {
                        return 0;
                    }

                    db = node.getDatabase();

                    if ((fHeader & 0x01) != 0) {
                        // Indirect pointers.

                        pos -= fInline; // safe to update now that outermost loop won't continue
                        fLen -= fInline;

                        final Node inode = prepareMultilevelWrite(db, page, loc);

                        // FIXME: Support zero levels.
                        final int levels = db.calculateInodeLevels(fLen);
                        writeMultilevelFragments(pos, levels, inode, b, bOff, bLen);

                        return 0;
                    }

                    pageSize = pageSize(db, page);
                } else {
                    if (b == TOUCH_VALUE) {
                        // Don't extend the value.
                        return 0;
                    }

                    // Extend the value.

                    int fieldGrowth = lengthFieldGrowth(fHeader, endPos);

                    if (fieldGrowth > 0) {
                        // FIXME: Fix exception handling.
                        tryIncreaseLengthField(cursor, frame, kHeaderLoc, vHeaderLoc, vLen,
                                               fHeaderLoc, fieldGrowth);
                        continue;
                    }

                    db = node.getDatabase();

                    if ((fHeader & 0x01) != 0) {
                        // Extend the value with indirect pointers.

                        pos -= fInline; // safe to update now that outermost loop won't continue

                        // FIXME: update length later (after inode allocations)
                        updateLengthField(page, fHeaderLoc, endPos);

                        Node inode = prepareMultilevelWrite(db, page, loc);

                        // Levels required before extending...
                        int levels = db.calculateInodeLevels(fLen - fInline);

                        // Compare to new full indirect length.
                        long newLen = endPos - fInline;

                        if (db.levelCap(levels) < newLen) {
                            // Need to add more inode levels.
                            int newLevels = db.calculateInodeLevels(newLen);
                            if (newLevels <= levels) {
                                throw new AssertionError();
                            }

                            pageSize = pageSize(db, page);

                            // FIXME: alloc all first; delete if an exception
                            do {
                                Node upper = db.allocDirtyFragmentNode();
                                /*P*/ byte[] upage = upper.mPage;
                                p_int48PutLE(upage, 0, inode.mId);
                                inode.releaseExclusive();
                                // Zero-fill the rest.
                                p_clear(upage, 6, pageSize);
                                inode = upper;
                                levels++;
                            } while (newLevels > levels);

                            p_int48PutLE(page, loc, inode.mId);
                        }

                        writeMultilevelFragments(pos, levels, inode, b, bOff, bLen);

                        return 0;
                    }

                    // Extend the value with direct pointers.

                    pageSize = pageSize(db, page);

                    long ptrGrowth = directPointerGrowth
                        (pageSize, fLen - fInline, endPos - fInline);

                    if (ptrGrowth > 0) {
                        // FIXME: Fix exception handling.
                        int newLoc = tryExtendDirect(cursor, frame, kHeaderLoc, vHeaderLoc, vLen,
                                                     fHeaderLoc, ptrGrowth * 6);

                        // Note: vHeaderLoc is now wrong and cannot be used.

                        if (newLoc < 0) {
                            // Format conversion or latch re-acquisition.
                            continue;
                        }

                        // Page reference might have changed.
                        page = node.mPage;

                        // Fix broken location variables that are still required.
                        int delta = newLoc - fHeaderLoc;
                        loc += delta;

                        fHeaderLoc = newLoc;
                    }

                    updateLengthField(page, fHeaderLoc, endPos);
                }

                // Direct pointers.

                pos -= fInline; // safe to update now that outermost loop won't continue

                final int ipos = (int) pos;
                loc += (ipos / pageSize) * 6;
                int fNodeOff = ipos % pageSize;

                while (true) {
                    final int amt = Math.min(bLen, pageSize - fNodeOff);
                    final long fNodeId = p_uint48GetLE(page, loc);
                    if (fNodeId == 0) {
                        if (amt > 0) {
                            // Writing into a sparse value. Allocate a node and point to it.
                            final Node fNode = db.allocDirtyFragmentNode();
                            try {
                                p_int48PutLE(page, loc, fNode.mId);

                                // Now write to the new page, zero-filling the gaps.
                                /*P*/ byte[] fNodePage = fNode.mPage;
                                p_clear(fNodePage, 0, fNodeOff);
                                p_copyFromArray(b, bOff, fNodePage, fNodeOff, amt);
                                p_clear(fNodePage, fNodeOff + amt, pageSize);
                            } finally {
                                fNode.releaseExclusive();
                            }
                        }
                    } else {
                        if (amt > 0 || b == TOUCH_VALUE) {
                            // Obtain node from cache, or read it only for partial write.
                            final Node fNode = db
                                .nodeMapLoadFragmentExclusive(fNodeId, amt < pageSize);
                            try {
                                if (db.markFragmentDirty(fNode)) {
                                    p_int48PutLE(page, loc, fNode.mId);
                                }
                                p_copyFromArray(b, bOff, fNode.mPage, fNodeOff, amt);
                            } finally {
                                fNode.releaseExclusive();
                            }
                        }
                    }
                    bLen -= amt;
                    if (bLen <= 0) {
                        return 0;
                    }
                    bOff += amt;
                    loc += 6;
                    fNodeOff = 0;
                }
            } catch (IOException e) {
                node.releaseExclusive();
                throw e;
            }
            } // end switch(op)
        }
    }

    /**
     * @param pos value position being read
     * @param level inode level; at least 1
     * @param inode shared latched parent inode; always released by this method
     * @param b slice of complete value being reconstructed
     */
    private static void readMultilevelFragments(LocalDatabase db, long pos, int level, Node inode,
                                                byte[] b, int bOff, int bLen)
        throws IOException
    {
        try {
            /*P*/ byte[] page = inode.mPage;
            level--;
            long levelCap = db.levelCap(level);

            int firstChild = (int) (pos / levelCap);
            int lastChild = (int) ((pos + bLen - 1) / levelCap);

            int childNodeCount = lastChild - firstChild + 1;

            // Handle a possible partial read from the first page.
            long ppos = pos % levelCap;

            for (int poffset = firstChild * 6, i=0; i<childNodeCount; poffset += 6, i++) {
                long childNodeId = p_uint48GetLE(page, poffset);
                int len = (int) Math.min(levelCap - ppos, bLen);

                if (childNodeId == 0) {
                    // Reading a sparse value.
                    Arrays.fill(b, bOff, bOff + len, (byte) 0);
                } else {
                    Node childNode = db.nodeMapLoadFragment(childNodeId);
                    if (level <= 0) {
                        p_copyToArray(childNode.mPage, (int) ppos, b, bOff, len);
                        childNode.releaseShared();
                    } else {
                        readMultilevelFragments(db, ppos, level, childNode, b, bOff, len);
                    }
                }
                bLen -= len;
                if (bLen <= 0) {
                    break;
                }
                bOff += len;
                // Remaining reads begin at the start of the page.
                ppos = 0;
            }
        } finally {
            inode.releaseShared();
        }
    }

    /**
     * @param loc location of root inode in the page
     * @return dirtied root inode with exclusive latch held
     */
    private static Node prepareMultilevelWrite(LocalDatabase db, /*P*/ byte[] page, int loc)
        throws IOException
    {
        final Node inode;

        setPtr: {
            final long inodeId = p_uint48GetLE(page, loc);

            if (inodeId == 0) {
                // Writing into a sparse value. Allocate a node and point to it.
                inode = db.allocDirtyFragmentNode();
                p_clear(inode.mPage, 0, pageSize(db, inode.mPage));
            } else {
                inode = db.nodeMapLoadFragmentExclusive(inodeId, true);
                try {
                    if (!db.markFragmentDirty(inode)) {
                        // Already dirty, so no need to update the pointer.
                        break setPtr;
                    }
                } catch (Throwable e) {
                    inode.releaseExclusive();
                    throw e;
                }
            }

            p_int48PutLE(page, loc, inode.mId);
        }

        return inode;
    }

    /**
     * @param pos value position being read
     * @param level inode level; at least 1
     * @param inode exclusively latched parent inode; always released by this method
     * @param value slice of complete value being written
     */
    private static void writeMultilevelFragments(long pos, int level, Node inode,
                                                 byte[] b, int bOff, int bLen)
        throws IOException
    {
        final LocalDatabase db = inode.getDatabase();

        try {
            /*P*/ byte[] page = inode.mPage;
            level--;
            long levelCap = db.levelCap(level);

            int firstChild = (int) (pos / levelCap);
            int lastChild = bLen == 0 ? firstChild : ((int) ((pos + bLen - 1) / levelCap));

            int childNodeCount = lastChild - firstChild + 1;

            // Handle a possible partial write to the first page.
            long ppos = pos % levelCap;

            for (int poffset = firstChild * 6, i=0; i<childNodeCount; poffset += 6, i++) {
                int len = (int) Math.min(levelCap - ppos, bLen);
                int off = (int) ppos;

                final int pageSize = pageSize(db, page);

                final Node childNode;
                setPtr: {
                    long childNodeId = p_uint48GetLE(page, poffset);
                    boolean partial = level > 0 | off > 0 | len < pageSize;

                    if (childNodeId == 0) {
                        // Writing into a sparse value. Allocate a node and point to it.
                        childNode = db.allocDirtyFragmentNode();
                        if (partial) {
                            // New page must be zero-filled.
                            p_clear(childNode.mPage, 0, pageSize);
                        }
                    } else {
                        // Obtain node from cache, or read it only for partial write.
                        childNode = db.nodeMapLoadFragmentExclusive(childNodeId, partial);
                        try {
                            if (!db.markFragmentDirty(childNode)) {
                                // Already dirty, so no need to update the pointer.
                                break setPtr;
                            }
                        } catch (Throwable e) {
                            childNode.releaseExclusive();
                            throw e;
                        }
                    }

                    p_int48PutLE(page, poffset, childNode.mId);
                }

                if (level <= 0) {
                    p_copyFromArray(b, bOff, childNode.mPage, off, len);
                    childNode.releaseExclusive();
                } else {
                    writeMultilevelFragments(ppos, level, childNode, b, bOff, len);
                }

                bLen -= len;
                if (bLen <= 0) {
                    break;
                }
                bOff += len;
                // Remaining writes begin at the start of the page.
                ppos = 0;
            }
        } catch (Throwable e) {
            // Panic.
            db.close(e);
            throw e;
        } finally {
            inode.releaseExclusive();
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
    private static void updateLengthField(/*P*/ byte[] page, int fHeaderLoc, long fLen) {
        int fHeader = p_byteGet(page, fHeaderLoc);

        switch ((fHeader >> 2) & 0x03) {
        case 0: // (2 byte length field)
            p_shortPutLE(page, fHeaderLoc + 1, (int) fLen);
            break;
        case 1: // (4 byte length field)
            p_intPutLE(page, fHeaderLoc + 1, (int) fLen);
            break;
        case 2: // (6 byte length field)
            p_int48PutLE(page, fHeaderLoc + 1, fLen);
            break;
        default: // (8 byte length field)
            p_longPutLE(page, fHeaderLoc + 1, fLen);
            break;
        }
    }

    /**
     * Attempt to increase the size of the fragmented length field, for supporting larger
     * sizes. If not enough space exists to increase the size, the value format is converted
     * and the field size doesn't change.
     *
     * As a side effect of calling this method, the page referenced by the node can change when
     * a non-negative value is returned. Also, any locations within the page may changed.
     * Caller should always continue from the beginning after calling this method.
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
    private static int tryIncreaseLengthField(final TreeCursor cursor, final CursorFrame frame,
                                              final int kHeaderLoc,
                                              final int vHeaderLoc, final int vLen,
                                              final int fHeaderLoc, final long growth)
        throws IOException
    {
        final int fOffset = fHeaderLoc - kHeaderLoc;
        final long newEntryLen = fOffset + vLen + growth;
        final Node node = frame.mNode;

        if (newEntryLen > node.getDatabase().mMaxFragmentedEntrySize) {
            compactDirectFormat(node, vHeaderLoc, vLen, fHeaderLoc);
            return -1;
        }

        final Tree tree = cursor.mTree;

        try {
            final int igrowth = (int) growth;
            final byte[] newValue = new byte[vLen + igrowth];
            final /*P*/ byte[] page = node.mPage;

            // Update the header with the new field size.
            int fHeader = p_byteGet(page, fHeaderLoc);
            newValue[0] = (byte) (fHeader + (igrowth << 1));

            // Copy the length field.
            int srcLoc = fHeaderLoc + 1;
            int fieldLen = skipFragmentedLengthField(0, fHeader);
            p_copyToArray(page, srcLoc, newValue, 1, fieldLen);

            // Copy the rest.
            srcLoc += fieldLen;
            int dstLoc = 1 + fieldLen + igrowth;
            p_copyToArray(page, srcLoc, newValue, dstLoc, newValue.length - dstLoc);

            // Clear the fragmented bit so that the update method doesn't delete the pages.
            p_bytePut(page, vHeaderLoc, p_byteGet(page, vHeaderLoc) & ~Node.ENTRY_FRAGMENTED);

            // The fragmented bit is set again by this call.
            node.updateLeafValue(frame, tree, frame.mNodePos, Node.ENTRY_FRAGMENTED, newValue);
        } catch (Throwable e) {
            node.releaseExclusive();
            throw e;
        }

        if (node.mSplit != null) {
            // Releases latch if an exception is thrown.
            tree.finishSplit(frame, node);
            // Finishing the split causes the node latch to be re-acquired, so start over.
            return -2;
        }

        return 0;
    }

    /**
     * Returns the amount of direct pointers to be added, which is <= 0 if none.
     */
    private static long directPointerGrowth(long pageSize, long oldLen, long newLen) {
        long oldCount = (oldLen + pageSize - 1) / pageSize;
        long newCount = (newLen + pageSize - 1) / pageSize;

        long growth = newCount - oldCount;

        if (growth <= 0 && newCount <= 0) {
            // Overflow.
            newCount = BigInteger.valueOf(newLen).add(BigInteger.valueOf(pageSize - 1))
                .subtract(BigInteger.ONE).divide(BigInteger.valueOf(pageSize))
                .longValue();
            growth = newCount - oldCount;
        }

        return growth;
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
    private static int tryExtendDirect(final TreeCursor cursor, final CursorFrame frame,
                                       final int kHeaderLoc,
                                       final int vHeaderLoc, final int vLen,
                                       final int fHeaderLoc, final long growth)
        throws IOException
    {
        final int fOffset = fHeaderLoc - kHeaderLoc;
        final long newEntryLen = fOffset + vLen + growth;
        final Node node = frame.mNode;

        if (newEntryLen > node.getDatabase().mMaxFragmentedEntrySize) {
            compactDirectFormat(node, vHeaderLoc, vLen, fHeaderLoc);
            return -1;
        }

        final Tree tree = cursor.mTree;

        try {
            final byte[] newValue = new byte[vLen + (int) growth];
            final /*P*/ byte[] page = node.mPage;
            p_copyToArray(page, fHeaderLoc, newValue, 0, vLen);

            // Clear the fragmented bit so that the update method doesn't delete the pages.
            p_bytePut(page, vHeaderLoc, p_byteGet(page, vHeaderLoc) & ~Node.ENTRY_FRAGMENTED);

            // The fragmented bit is set again by this call.
            node.updateLeafValue(frame, tree, frame.mNodePos, Node.ENTRY_FRAGMENTED, newValue);
        } catch (Throwable e) {
            node.releaseExclusive();
            throw e;
        }

        if (node.mSplit != null) {
            // Releases latch if an exception is thrown.
            tree.finishSplit(frame, node);
            // Finishing the split causes the node latch to be re-acquired, so start over.
            return -2;
        }

        return p_ushortGetLE(node.mPage, node.searchVecStart() + frame.mNodePos) + fOffset;
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
     * completely, or the value is converted to indirect format.
     *
     * The node latch is released if an exception is thrown.
     *
     * @param node latched exclusive and dirtied
     * @param vHeaderLoc location of raw value header
     * @param vLen length of raw value sans header
     * @param fHeaderLoc location of fragmented value header
     */
    private static void compactDirectFormat(final Node node,
                                            final int vHeaderLoc, final int vLen,
                                            final int fHeaderLoc)
        throws IOException
    {
        // FIXME: If inline content is at least 6 bytes, then move all inline content and keep
        // direct format for now. Else, convert to indirect and move all inline content.
        // Because indirect format isn't currently created with inline content, and the earlier
        // conversion moved it, no inline content should exist at this point.

        final /*P*/ byte[] page = node.mPage;

        int loc = fHeaderLoc;
        final int fHeader = p_byteGet(page, loc++);
        final long fLen = LocalDatabase.decodeFullFragmentedValueLength(fHeader, page, loc);

        loc = skipFragmentedLengthField(loc, fHeader);

        final int fInline;
        if ((fHeader & 0x02) == 0) {
            fInline = 0;
        } else {
            fInline = p_ushortGetLE(page, loc);
            loc = loc + 2 + fInline;
        }

        // At this point, loc is at the first direct pointer.

        int tailLen = fHeaderLoc + vLen - loc; // length of all the direct pointers, in bytes

        LocalDatabase db = node.getDatabase();
        int levels = db.calculateInodeLevels(fLen - fInline);

        if (levels > 0) {
            Node[] inodes = new Node[levels];

            try {
                for (int i=0; i<inodes.length; i++) {
                    inodes[i] = db.allocDirtyFragmentNode();
                }
            } catch (Throwable e) {
                node.releaseExclusive();

                for (Node inode : inodes) {
                    if (inode != null) {
                        db.deleteNode(inode, true);
                    }
                }

                throw e;
            }

            // Copy direct pointers to inode.
            Node inode = inodes[--levels];
            /*P*/ byte[] ipage = inode.mPage;
            p_copy(page, loc, ipage, 0, tailLen);
            // Zero-fill the rest.
            p_clear(ipage, tailLen, pageSize(db, ipage));

            while (levels > 0) {
                Node upper = inodes[--levels];
                /*P*/ byte[] upage = upper.mPage;
                p_int48PutLE(upage, 0, inode.mId);
                inode.releaseExclusive();
                // Zero-fill the rest.
                p_clear(upage, 6, pageSize(db, upage));
                inode = upper;
            }

            // Reference the root inode.
            p_int48PutLE(page, loc, inode.mId);
            inode.releaseExclusive();
        }

        // Switch to indirect format.
        p_bytePut(page, fHeaderLoc, fHeader | 0x01);

        // Update the raw value length.
        int shrinkage = tailLen - 6;
        int newLen = vLen - shrinkage - 1; // minus one as required by field encoding
        int header = p_byteGet(page, vHeaderLoc);
        if ((header & 0x20) == 0) {
            // Field length is 1..8192.
            p_bytePut(page, vHeaderLoc, (header & 0xe0) | (newLen >> 8));
            p_bytePut(page, vHeaderLoc + 1, newLen);
        } else {
            // Field length is 1..1048576.
            p_bytePut(page, vHeaderLoc, (header & 0xf0) | (newLen >> 16));
            p_bytePut(page, vHeaderLoc + 1, newLen >> 8);
            p_bytePut(page, vHeaderLoc + 2, newLen);
        }

        // Update the garbage field.
        node.garbage(node.garbage() + shrinkage);
    }

    private static int pageSize(LocalDatabase db, /*P*/ byte[] page) {
        /*P*/ // [
        return page.length;
        /*P*/ // |
        /*P*/ // return db.pageSize();
        /*P*/ // ]
    }
}
