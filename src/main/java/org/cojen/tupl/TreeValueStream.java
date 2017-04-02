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

import java.util.Arrays;

import static org.cojen.tupl.PageOps.*;
import static org.cojen.tupl.Utils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class TreeValueStream extends AbstractStream {
    // Op ordinals are relevant.
    private static final int OP_LENGTH = 0, OP_READ = 1, OP_SET_LENGTH = 2, OP_WRITE = 3;

    // Touches a fragment without extending the value length. Used for file compaction.
    static final byte[] TOUCH_VALUE = new byte[0];

    private final TreeCursor mCursor;
    private final LocalDatabase mDatabase;

    /**
     * @param cursor positioned or unpositioned cursor, not autoloading
     */
    TreeValueStream(TreeCursor cursor) {
        mCursor = cursor;
        mDatabase = cursor.mTree.mDatabase;
    }

    @Override
    public LockResult open(Transaction txn, byte[] key) throws IOException {
        TreeCursor cursor = mCursor;
        if (cursor.key() != null) {
            close();
        }
        cursor.link(txn);
        try {
            return cursor.find(key);
        } catch (Throwable e) {
            mCursor.reset();
            throw e;
        }
    }

    @Override
    public Transaction link(Transaction txn) {
        return mCursor.link(txn);
    }

    @Override
    public Transaction link() {
        return mCursor.link();
    }

    @Override
    public long length() throws IOException {
        CursorFrame frame;
        try {
            frame = mCursor.leafSharedNotSplit();
        } catch (IllegalStateException e) {
            checkOpen();
            throw e;
        }

        long result = action(frame, OP_LENGTH, 0, null, 0, 0);
        frame.mNode.releaseShared();
        return result;
    }

    @Override
    public void setLength(long length) throws IOException {
        // FIXME: txn undo/redo
        try {
            if (length < 0) {
                mCursor.store(null);
                return;
            }

            final CursorFrame leaf = mCursor.leafExclusive();

            final CommitLock.Shared shared = mCursor.commitLock(leaf);
            try {
                mCursor.notSplitDirty(leaf);
                action(leaf, OP_SET_LENGTH, length, EMPTY_BYTES, 0, 0);
                leaf.mNode.releaseExclusive();
            } finally {
                shared.release();
            }
        } catch (IllegalStateException e) {
            checkOpen();
            throw e;
        }
    }

    @Override
    int doRead(long pos, byte[] buf, int off, int len) throws IOException {
        CursorFrame frame;
        try {
            frame = mCursor.leafSharedNotSplit();
        } catch (IllegalStateException e) {
            checkOpen();
            throw e;
        }

        int result = (int) action(frame, OP_READ, pos, buf, off, len);
        frame.mNode.releaseShared();
        return result;
    }

    @Override
    void doWrite(long pos, byte[] buf, int off, int len) throws IOException {
        // FIXME: txn undo/redo
        try {
            final CursorFrame leaf = mCursor.leafExclusive();

            final CommitLock.Shared shared = mCursor.commitLock(leaf);
            try {
                mCursor.notSplitDirty(leaf);
                action(leaf, OP_WRITE, pos, buf, off, len);
                leaf.mNode.releaseExclusive();
            } finally {
                shared.release();
            }
        } catch (IllegalStateException e) {
            checkOpen();
            throw e;
        }
    }

    @Override
    int selectBufferSize(int bufferSize) {
        if (bufferSize <= 1) {
            if (bufferSize < 0) {
                bufferSize = mDatabase.mPageSize;
            } else {
                bufferSize = 1;
            }
        } else if (bufferSize >= 65536) {
            bufferSize = 65536;
        }
        return bufferSize;
    }

    @Override
    void checkOpen() {
        if (mCursor.key() == null) {
            throw new IllegalStateException("Stream closed");
        }
    }

    @Override
    void doClose() {
        mCursor.reset();
    }

    /**
     * Determine if any fragment nodes at the given position are outside the compaction zone.
     *
     * @param frame latched leaf, not split, never released by this method
     * @param highestNodeId defines the highest node before the compaction zone; anything
     * higher is in the compaction zone
     * @return -1 if position is too high, 0 if no compaction required, or 1 if any nodes are
     * in the compaction zone
     */
    int compactCheck(final CursorFrame frame, long pos, final long highestNodeId)
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

        int header = p_byteGet(page, loc++);
        if (header >= 0) {
            // Not fragmented.
            return pos >= header ? -1 : 0;
        }

        int len;
        if ((header & 0x20) == 0) {
            len = 1 + (((header & 0x1f) << 8) | p_ubyteGet(page, loc++));
        } else if (header != -1) {
            len = 1 + (((header & 0x0f) << 16)
                       | (p_ubyteGet(page, loc++) << 8) | p_ubyteGet(page, loc++));
        } else {
            // ghost
            return -1;
        }

        if ((header & Node.ENTRY_FRAGMENTED) == 0) {
            // Not fragmented.
            return pos >= len ? -1 : 0;
        }

        // Read the fragment header, as described by the Database.fragment method.
        header = p_byteGet(page, loc++);

        final long vLen = LocalDatabase.decodeFullFragmentedValueLength(header, page, loc);

        if (pos >= vLen) {
            return -1;
        }

        // Advance past the value length field.
        loc += 2 + ((header >> 1) & 0x06);

        if ((header & 0x02) != 0) {
            // Inline content.
            final int inLen = p_ushortGetLE(page, loc);
            if (pos < inLen) {
                // Positioned within inline content.
                return 0;
            }
            pos -= inLen;
            loc = loc + 2 + inLen;
        }

        if ((header & 0x01) == 0) {
            // Direct pointers.
            loc += (((int) pos) / pageSize(page)) * 6;
            final long fNodeId = p_uint48GetLE(page, loc);
            return fNodeId > highestNodeId ? 1 : 0; 
        }

        // Indirect pointers.

        final long inodeId = p_uint48GetLE(page, loc);
        if (inodeId == 0) {
            // Sparse value.
            return 0;
        }

        Node inode = mDatabase.nodeMapLoadFragment(inodeId);
        int level = mDatabase.calculateInodeLevels(vLen);

        while (true) {
            level--;
            long levelCap = mDatabase.levelCap(level);
            long childNodeId = p_uint48GetLE(inode.mPage, ((int) (pos / levelCap)) * 6);
            inode.releaseShared();
            if (childNodeId > highestNodeId) {
                return 1;
            }
            if (level <= 0 || childNodeId == 0) {
                return 0;
            }
            inode = mDatabase.nodeMapLoadFragment(childNodeId);
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
    private long action(final CursorFrame frame,
                        final int op, long pos, final byte[] b, int bOff, int bLen)
        throws IOException
    {
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
            node = mCursor.insertBlank(frame, node, pos + bLen);

            if (bLen <= 0) {
                return 0;
            }

            // Fallthrough and complete the write operation. Need to re-assign nodePos, because
            // the insert operation changed it.
            nodePos = frame.mNodePos;
        }

        /*P*/ byte[] page = node.mPage;
        int loc = p_ushortGetLE(page, node.searchVecStart() + nodePos);
        // Skip the key.
        loc += Node.keyLengthAtLoc(page, loc);

        int vHeaderLoc = loc;
        long vLen;

        int header = p_byteGet(page, loc++);
        nf: {
            if (header >= 0) {
                vLen = header;
            } else {
                int len;
                if ((header & 0x20) == 0) {
                    len = 1 + (((header & 0x1f) << 8) | p_ubyteGet(page, loc++));
                } else if (header != -1) {
                    len = 1 + (((header & 0x0f) << 16)
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

                if ((header & Node.ENTRY_FRAGMENTED) != 0) {
                    // Value is fragmented.
                    break nf;
                }

                vLen = len;
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
                            // TODO: need frame for rebalancing to work
                            node.updateLeafValue(null, mCursor.mTree, nodePos, 0, b);
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

            // This point is reached for appending to a non-fragmented value. There's all kinds
            // of optimizations that can be performed here, but keep things simple. Delete the
            // old value, insert a blank value, and then update it.

            byte[] oldValue = new byte[(int) vLen];
            p_copyToArray(page, loc, oldValue, 0, oldValue.length);

            node.deleteLeafEntry(nodePos);
            frame.mNodePos = ~nodePos;

            // Method releases latch if an exception is thrown.
            mCursor.insertBlank(frame, node, pos + bLen);

            action(frame, OP_WRITE, 0, oldValue, 0, oldValue.length);

            if (bLen > 0) {
                action(frame, OP_WRITE, pos, b, bOff, bLen);
            }

            return 0;
        }

        // Operate against a fragmented value. First read the fragment header, as described by
        // the Database.fragment method.
        int fHeaderLoc = loc;
        header = p_byteGet(page, loc++);

        switch ((header >> 2) & 0x03) {
        default:
            vLen = p_ushortGetLE(page, loc);
            break;
        case 1:
            vLen = p_intGetLE(page, loc) & 0xffffffffL;
            break;
        case 2:
            vLen = p_uint48GetLE(page, loc);
            break;
        case 3:
            vLen = p_longGetLE(page, loc);
            if (vLen < 0) {
                if (op <= OP_READ) {
                    node.releaseShared();
                } else {
                    node.releaseExclusive();
                }
                throw new LargeValueException(vLen);
            }
            break;
        }

        // Advance past the value length field.
        loc += 2 + ((header >> 1) & 0x06);

        switch (op) {
        case OP_LENGTH: default:
            return vLen;

        case OP_READ: try {
            if (bLen <= 0 || pos >= vLen) {
                return 0;
            }

            bLen = (int) Math.min(vLen - pos, bLen);
            final int total = bLen;

            if ((header & 0x02) != 0) {
                // Inline content.
                final int inLen = p_ushortGetLE(page, loc);
                loc += 2;
                final int amt = (int) (inLen - pos);
                if (amt <= 0) {
                    // Not reading any inline content.
                    pos -= inLen;
                } else if (bLen <= amt) {
                    p_copyToArray(page, (int) (loc + pos), b, bOff, bLen);
                    return bLen;
                } else {
                    p_copyToArray(page, (int) (loc + pos), b, bOff, amt);
                    bLen -= amt;
                    bOff += amt;
                    pos = 0;
                }
                loc += inLen;
            }

            if ((header & 0x01) == 0) {
                // Direct pointers.
                final int ipos = (int) pos;
                loc += (ipos / pageSize(page)) * 6;
                int fNodeOff = ipos % pageSize(page);
                while (true) {
                    final int amt = Math.min(bLen, pageSize(page) - fNodeOff);
                    final long fNodeId = p_uint48GetLE(page, loc);
                    if (fNodeId == 0) {
                        // Reading a sparse value.
                        Arrays.fill(b, bOff, bOff + amt, (byte) 0);
                    } else {
                        final Node fNode = mDatabase.nodeMapLoadFragment(fNodeId);
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
                LocalDatabase db = mDatabase;
                final Node inode = db.nodeMapLoadFragment(inodeId);
                final int levels = db.calculateInodeLevels(vLen);
                readMultilevelFragments(pos, levels, inode, b, bOff, bLen);
            }

            return total;
        } catch (IOException e) {
            node.releaseShared();
            throw e;
        }

        case OP_SET_LENGTH:
            long endPos = pos + bLen;
            if (endPos <= vLen) {
                if (endPos == vLen) {
                    return 0;
                }
                // FIXME: truncate
                node.releaseExclusive();
                throw null;
            }
            // Fall through to extend length.

        case OP_WRITE: try {
            endPos = pos + bLen;

            int inlineLen = 0;
            if ((header & 0x02) != 0) {
                // Inline content.
                inlineLen = p_ushortGetLE(page, loc);
                loc += 2;
                final long amt = inlineLen - pos;
                if (amt <= 0) {
                    // Not writing any inline content.
                    pos -= inlineLen;
                } else if (bLen <= amt) {
                    p_copyFromArray(b, bOff, page, (int) (loc + pos), bLen);
                    return 0;
                } else {
                    p_copyFromArray(b, bOff, page, (int) (loc + pos), (int) amt);
                    bLen -= amt;
                    bOff += amt;
                    pos = 0;
                }
                // FIXME: needs to be re-applied when loc is re-assigned
                loc += inlineLen;
                // FIXME: later
                // vLen -= inLen;
            }

            if (endPos <= vLen) {
                if (bLen == 0 & b != TOUCH_VALUE) {
                    return 0;
                }
            } else extend: {
                if (b == TOUCH_VALUE) {
                    // Don't extend the value.
                    return 0;
                }

                int newLoc = updateLengthField(frame, page, fHeaderLoc, endPos);

                if (newLoc < 0) {
                    // FIXME: Expand length field; what if inline is too big? Need to make sure
                    // that if two max entries exist in the node that split still works. Might
                    // only be a problem with large keys and internal nodes.
                    // mMaxFragmentedEntrySize is 2037. Enough space can be reclaimed by
                    // removing one pointer. If indirect this is not possible, and also not
                    // possible if one direct pointer. The Database.fragment method must limit
                    // the inline content in these two cases. Note that it doesn't inline when
                    // creating with indirect pointers. However, the magic size that cannot be
                    // easily repaired is about 6000 bytes. If converting to indirect doesn't
                    // free up space, then a simple delete and re-insert might be fine.

                    loc = ~newLoc;
                    fHeaderLoc = loc;
                    header = p_byteGet(page, loc++);
                    // Advance past the value length field.
                    loc += 2 + ((header >> 1) & 0x06);
                } else if (newLoc != fHeaderLoc) { // FIXME: might not have moved!
                    // FIXME: looks the same as above...
                    loc = newLoc;
                    fHeaderLoc = loc;
                    header = p_byteGet(page, loc++);
                    // Advance past the value length field.
                    loc += 2 + ((header >> 1) & 0x06);
                }

                // Due to compaction. FIXME: messy, this is
                page = node.mPage;

                // Note: vHeaderLoc is now wrong. Any code below this point cannot use it.

                if ((header & 0x01) == 0) {
                    // Extend value with direct pointers.

                    long growth;
                    {
                        long p = pageSize(page);
                        growth = ((endPos + p - 1) / p) - ((vLen + p - 1) / p);
                    }

                    vLen = endPos;

                    if (growth <= 0) {
                        break extend;
                    }

                    newLoc = extendFragmentedValue(node, mCursor.mTree, nodePos, growth * 6, true);

                    if (newLoc >= 0) {
                        int delta = newLoc - fHeaderLoc;
                        loc += delta;
                        //fHeaderLoc = newLoc;
                        // Note: fHeaderLoc is now wrong. Any code below this point
                        // cannot use it.
                        // Due to compaction. FIXME: messy, this is
                        page = node.mPage;
                        break extend;
                    }

                    // FIXME: messy, this is
                    page = node.mPage;
                    newLoc = ~newLoc;
                    header = p_byteGet(page, newLoc);
                    int delta = newLoc - fHeaderLoc;
                    loc += delta;

                    break extend;
                }

                // Extend value with indirect pointers.

                Node inode = prepareMultilevelWrite(page, loc);

                // Levels required before extending...
                int levels = mDatabase.calculateInodeLevels(vLen - inlineLen);

                // Compare to new full indirect length.
                vLen = endPos - inlineLen;

                if (mDatabase.levelCap(levels) < vLen) {
                    // Need to add more inode levels.
                    int newLevels = mDatabase.calculateInodeLevels(vLen);
                    if (newLevels <= levels) {
                        throw new AssertionError();
                    }

                    do {
                        Node upper = mDatabase.allocDirtyFragmentNode();
                        /*P*/ byte[] upage = upper.mPage;
                        p_int48PutLE(upage, 0, inode.mId);
                        inode.releaseExclusive();
                        // Zero-fill the rest.
                        p_clear(upage, 6, pageSize(upage));
                        inode = upper;
                        levels++;
                    } while (newLevels > levels);

                    p_int48PutLE(page, loc, inode.mId);
                }

                writeMultilevelFragments(pos, levels, inode, b, bOff, bLen);

                return 0;
            }

            vLen -= inlineLen;

            if ((header & 0x01) == 0) {
                // Direct pointers.
                final int ipos = (int) pos;
                loc += (ipos / pageSize(page)) * 6;
                int fNodeOff = ipos % pageSize(page);
                while (true) {
                    final int amt = Math.min(bLen, pageSize(page) - fNodeOff);
                    final long fNodeId = p_uint48GetLE(page, loc);
                    if (fNodeId == 0) {
                        // Writing into a sparse value. Allocate a node and point to it.
                        final Node fNode = mDatabase.allocDirtyFragmentNode();
                        try {
                            p_int48PutLE(page, loc, fNode.mId);

                            // Now write to the new page, zero-filling the gaps.
                            /*P*/ byte[] fNodePage = fNode.mPage;
                            p_clear(fNodePage, 0, fNodeOff);
                            p_copyFromArray(b, bOff, fNodePage, fNodeOff, amt);
                            p_clear(fNodePage, fNodeOff + amt, pageSize(fNodePage));
                        } finally {
                            fNode.releaseExclusive();
                        }
                    } else {
                        // Obtain node from cache, or read it only for partial write.
                        LocalDatabase db = mDatabase;
                        final Node fNode =
                            db.nodeMapLoadFragmentExclusive(fNodeId, amt < pageSize(page));
                        try {
                            if (db.markFragmentDirty(fNode)) {
                                p_int48PutLE(page, loc, fNode.mId);
                            }
                            p_copyFromArray(b, bOff, fNode.mPage, fNodeOff, amt);
                        } finally {
                            fNode.releaseExclusive();
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
            }

            // Indirect pointers.

            final Node inode = prepareMultilevelWrite(page, loc);

            final int levels = mDatabase.calculateInodeLevels(vLen);
            writeMultilevelFragments(pos, levels, inode, b, bOff, bLen);

            return 0;
        } catch (IOException e) {
            node.releaseExclusive();
            throw e;
        }
        } // end switch(op)
    }

    /**
     * @param loc fragmented value header location
     * @return new value location (after header), negated if converted to indirect format
     */
    private int updateLengthField(CursorFrame frame, /*P*/ byte[] page, int loc, long len)
        throws IOException
    {
        int growth;
        int header = p_byteGet(page, loc);

        switch ((header >> 2) & 0x03) {
        default: // (2 byte length field)
            if (len < (1L << (2 * 8))) {
                p_shortPutLE(page, loc + 1, (int) len);
                return loc;
            }
            growth = (len < (1L << (4 * 8))) ? 2 : ((len < (1L << (6 * 8))) ? 2 : 4);
            break;
        case 1: // (4 byte length field)
            if (len < (1L << (4 * 8))) {
                p_intPutLE(page, loc + 1, (int) len);
                return loc;
            }
            growth = (len < (1L << (6 * 8))) ? 2 : 4;
            break;
        case 2: // (6 byte length field)
            if (len < (1L << (6 * 8))) {
                p_int48PutLE(page, loc + 1, len);
                return loc;
            }
            growth = 2;
            break;
        case 3: // (8 byte length field)
            p_longPutLE(page, loc + 1, len);
            return loc;
        }

        Node node = frame.mNode;
        int nodePos = frame.mNodePos;

        int newLoc = extendFragmentedValue(node, mCursor.mTree, nodePos, growth, false);

        // FIXME: messy, this is
        page = node.mPage;

        if (newLoc < 0) {
            loc = ~newLoc;
            header |= 0x01; // now indirect
        } else {
            loc = newLoc;
        }

        // Clear only the field size bits.
        header &= ~0b0000_1100;

        if (len < (1L << (4 * 8))) {
            header |= 0x04; // ff = 1 (4 byte length field)
            p_intPutLE(page, loc + 1, (int) len);
        } else if (len < (1L << (6 * 8))) {
            header |= 0x08; // ff = 2 (6 byte length field)
            p_int48PutLE(page, loc + 1, len);
        } else {
            header |= 0x0c; // ff = 3 (8 byte length field)
            p_longPutLE(page, loc + 1, len);
        }

        p_bytePut(page, loc, header);

        return newLoc;
    }

    /**
     * @param pos value position being read
     * @param level inode level; at least 1
     * @param inode shared latched parent inode; always released by this method
     * @param b slice of complete value being reconstructed
     */
    private void readMultilevelFragments(long pos, int level, Node inode,
                                         byte[] b, int bOff, int bLen)
        throws IOException
    {
        try {
            /*P*/ byte[] page = inode.mPage;
            level--;
            long levelCap = mDatabase.levelCap(level);

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
                    Node childNode = mDatabase.nodeMapLoadFragment(childNodeId);
                    if (level <= 0) {
                        p_copyToArray(childNode.mPage, (int) ppos, b, bOff, len);
                        childNode.releaseShared();
                    } else {
                        readMultilevelFragments(ppos, level, childNode, b, bOff, len);
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
    private Node prepareMultilevelWrite(/*P*/ byte[] page, int loc) throws IOException {
        final Node inode;

        setPtr: {
            final long inodeId = p_uint48GetLE(page, loc);

            if (inodeId == 0) {
                // Writing into a sparse value. Allocate a node and point to it.
                inode = mDatabase.allocDirtyFragmentNode();
                p_clear(inode.mPage, 0, pageSize(inode.mPage));
            } else {
                LocalDatabase db = mDatabase;
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
    private void writeMultilevelFragments(long pos, int level, Node inode,
                                          byte[] b, int bOff, int bLen)
        throws IOException
    {
        final LocalDatabase db = mDatabase;

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

                final Node childNode;
                setPtr: {
                    long childNodeId = p_uint48GetLE(page, poffset);
                    boolean partial = level > 0 | off > 0 | len < pageSize(page);

                    if (childNodeId == 0) {
                        // Writing into a sparse value. Allocate a node and point to it.
                        childNode = db.allocDirtyFragmentNode();
                        if (partial) {
                            // New page must be zero-filled.
                            p_clear(childNode.mPage, 0, pageSize(childNode.mPage));
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
     * Attempt to extend a fragmented leaf value. Caller must ensure that value is fragmented.
     * If extending at the tail, growth region is filled with zeros.
     *
     * @param pos position as provided by binarySearch; must be positive
     * @param growth value length increase
     * @param tail true if growth is at tail end of value
     * @return new value location (after header), negated if converted to indirect format
     */
    private int extendFragmentedValue(final Node node, final Tree tree, final int pos,
                                      final long growth, final boolean tail)
        throws IOException
    {
        // FIXME: allow split
        long avail = node.availableLeafBytes() - growth;
        if (avail < 0) {
            throw new Error("split 1: " + node.availableLeafBytes() + ", " + growth);
        }

        int igrowth = (int) growth;
        int searchVecStart = node.searchVecStart();

        /*P*/ byte[] page = node.mPage;
        int entryLoc = p_ushortGetLE(page, searchVecStart + pos);

        int loc = entryLoc;

        final byte[] key; // encoded key
        {
            final int len = Node.keyLengthAtLoc(page, loc);
            key = new byte[len];
            p_copyToArray(page, loc, key, 0, len);
            loc += len;
        }

        byte[] value; // unencoded value
        {
            final int len;
            final int header = p_byteGet(page, loc++);
            if ((header & 0x20) == 0) {
                len = 1 + (((header & 0x1f) << 8) | p_ubyteGet(page, loc++));
            } else {
                len = 1 + (((header & 0x0f) << 16)
                           | (p_ubyteGet(page, loc++) << 8) | p_ubyteGet(page, loc++));
            }
            value = new byte[len];
            p_copyToArray(page, loc, value, 0, value.length);
            loc += len;
        }

        int retMask = 0;

        int newValueLen = Node.calculateFragmentedValueLength(value.length + igrowth);

        if ((key.length + newValueLen) > tree.mDatabase.mMaxFragmentedEntrySize) {
            // Too big, and so value encoding must be modified.
            final int header = value[0];

            if ((header & 0x01) != 0) {
                // FIXME: Already indirect and too much inline content (caused by length field
                // increase).
                throw new Error("too big and indirect: " + header);
            }

            long vLen;
            int off;

            switch ((header >> 2) & 0x03) {
            default:
                vLen = decodeUnsignedShortLE(value, 1);
                off = 3;
                break;
            case 1:
                vLen = decodeIntLE(value, 1) & 0xffffffffL;
                off = 5;
                break;
            case 2:
                vLen = decodeUnsignedInt48LE(value, 1);
                off = 7;
                break;
            case 3:
                vLen = decodeLongLE(value, 1);
                off = 9;
                break;
            }

            // FIXME: need length and offset and skip inline
            int levels = tree.mDatabase.calculateInodeLevels(vLen);

            if (levels <= 0) {
                // FIXME: Too much inline content.
                throw new Error("too big and direct: " + header + ", " + levels);
            }

            if ((header & 0x02) != 0) {
                // Skip over inline content.
                off += decodeUnsignedShortLE(value, off);
            }

            Node inode = tree.mDatabase.allocDirtyFragmentNode();

            // Copy direct pointers to inode.
            /*P*/ byte[] ipage = inode.mPage;
            p_copyFromArray(value, off, ipage, 0, value.length - off);
            // Zero-fill the rest.
            p_clear(ipage, value.length - off, pageSize(ipage));

            while (--levels != 0) {
                Node upper = tree.mDatabase.allocDirtyFragmentNode();
                /*P*/ byte[] upage = upper.mPage;
                p_int48PutLE(upage, 0, inode.mId);
                inode.releaseExclusive();
                // Zero-fill the rest.
                p_clear(upage, 6, pageSize(upage));
                inode = upper;
            }

            // Trim value of direct pointers and convert to indirect.
            byte[] ivalue = new byte[off + 6];
            // Copy header and inline content.
            System.arraycopy(value, 0, ivalue, 0, off);
            // Indirect format.
            ivalue[0] |= 0x01;
            // Indirect pointer.
            encodeInt48LE(ivalue, off, inode.mId);

            inode.releaseExclusive();

            // Negative result indicates that node is now indirect.
            retMask = ~0;

            value = ivalue;

            if (!tail) {
                // FIXME: tail yes, head no. Growth must be honored for head.
                throw new Error("head");
            }

            newValueLen = Node.calculateFragmentedValueLength(value.length);
            igrowth = 0;
        }

        // Note: As an optimization, search vector can be left as-is for new entry. Full delete
        // is simpler and re-uses existing code.
        node.finishDeleteLeafEntry(pos, loc - entryLoc);

        // TODO: need frame for rebalancing to work
        entryLoc = node.createLeafEntry(null, tree, pos, key.length + newValueLen);

        if (entryLoc < 0) {
            // FIXME: allow split or convert to indirect
            throw new Error("split 2");
        } else {
            // Node might have been compacted, so a capture a fresh page reference.
            page = node.mPage;

            p_copyFromArray(key, 0, page, entryLoc, key.length);
            entryLoc += key.length;

            entryLoc = Node.encodeLeafValueHeader
                (page, Node.ENTRY_FRAGMENTED, value.length + igrowth, entryLoc);

            // Copy existing value.
            if (tail) {
                p_copyFromArray(value, 0, page, entryLoc, value.length);
                int valueLoc = entryLoc + value.length;
                // Zero fill the extended region.
                p_clear(page, valueLoc, valueLoc + igrowth);
            } else {
                p_copyFromArray(value, 0, page, entryLoc + igrowth, value.length);
            }
        }

        return entryLoc ^ retMask;
    }

    private int pageSize(/*P*/ byte[] page) {
        /*P*/ // [
        return page.length;
        /*P*/ // |
        /*P*/ // return mDatabase.pageSize();
        /*P*/ // ]
    }
}
