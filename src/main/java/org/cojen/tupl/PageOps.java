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

import java.nio.ByteBuffer;

import java.util.zip.CRC32;

import static org.cojen.tupl.Utils.*;

/**
 * Low-level methods for operating against a database page.
 *
 * @author Brian S O'Neill
 */
final class PageOps {
    /*
     * Approximate byte overhead per Node, assuming 32-bit pointers. Overhead is determined by
     * examining all the fields in the Node class, including inherited ones. In addition, each
     * Node is referenced by mNodeMapTable.
     *
     * References: 1 field per Node instance
     * Node class: 18 fields (mId is counted twice)
     * Latch class: 0 fields
     * AbstractQueuedSynchronizer class: 3 fields
     * AbstractOwnableSynchronizer class: 1 field
     * Object class: Minimum 8 byte overhead
     * Total: (23 * 4 + 8) = 100
     */
    static final int NODE_OVERHEAD = 100;

    private static final byte[] CLOSED_TREE_PAGE;
    private static final byte[] STUB_TREE_PAGE;

    static {
        CLOSED_TREE_PAGE = newEmptyTreeLeafPage();
        STUB_TREE_PAGE = newEmptyTreePage(Node.TN_HEADER_SIZE + 8, Node.TYPE_TN_IN);
    }

    private static /*P*/ byte[] newEmptyTreeLeafPage() {
        return newEmptyTreePage
            (Node.TN_HEADER_SIZE, Node.TYPE_TN_LEAF | Node.LOW_EXTREMITY | Node.HIGH_EXTREMITY);
    }

    private static /*P*/ byte[] newEmptyTreePage(int pageSize, int type) {
        byte[] empty = new byte[pageSize];

        empty[0] = (byte) type;

        // Set fields such that binary search returns ~0 and availableBytes returns 0.

        // Note: Same as Node.clearEntries.
        p_shortPutLE(empty, 4,  Node.TN_HEADER_SIZE);     // leftSegTail
        p_shortPutLE(empty, 6,  pageSize - 1);            // rightSegTail
        p_shortPutLE(empty, 8,  Node.TN_HEADER_SIZE);     // searchVecStart
        p_shortPutLE(empty, 10, Node.TN_HEADER_SIZE - 2); // searchVecEnd

        return empty;
    }

    static /*P*/ byte[] p_null() {
        return null;
    }

    /**
     * Returned page is 12 bytes, defining a closed tree leaf node. Contents must not be
     * modified.
     */
    static /*P*/ byte[] p_closedTreePage() {
        return CLOSED_TREE_PAGE;
    }

    /**
     * Returned page is 20 bytes, defining a tree stub node. Contents must not be modified.
     *
     * A stub is an internal node (TYPE_TN_IN), no extremity bits set, with a single child id
     * of zero. Stubs are encountered by cursors when popping up, which only happens during
     * cursor iteration (next/previous), findNearby, and reset. Cursor iteration stops when it
     * encounters a stub node, because it has no more children. The findNearby method might
     * search into the child node, but this is prohibited. When the extremity bits are clear,
     * findNearby keeps popping up until no more nodes are found. Then it starts over from the
     * root node.
     */
    static /*P*/ byte[] p_stubTreePage() {
        return STUB_TREE_PAGE;
    }

    static /*P*/ byte[] p_alloc(int size) {
        return new byte[size];
    }

    static /*P*/ byte[] p_calloc(int size) {
        return new byte[size];
    }

    static /*P*/ byte[][] p_allocArray(int size) {
        return new byte[size][];
    }

    static void p_delete(/*P*/ byte[] page) {
    }

    /**
     * Allocates an "arena", which contains a fixed number of pages. Pages in an arena cannot
     * be deleted, and calling p_delete on arena pages does nothing. Call p_arenaDelete to
     * fully delete the entire arena when not used anymore.
     *
     * @return null if not supported
     */
    static Object p_arenaAlloc(int pageSize, long pageCount) throws IOException {
        return null;
    }

    /**
     * @throws IllegalArgumentException if unknown arena
     */
    static void p_arenaDelete(Object arena) throws IOException {
        if (arena != null) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Allocate a zero-filled page from an arena. If arena is null or depleted, then a regular
     * page is allocated.
     *
     * @throws IllegalArgumentException if unknown arena or if page size doesn't match
     */
    static /*P*/ byte[] p_calloc(Object arena, int size) {
        return p_calloc(size);
    }

    static /*P*/ byte[] p_clone(/*P*/ byte[] page, int length) {
        return page.clone();
    }

    /**
     * Allocates a clone if the page type is not an array. Must be deleted.
     *
     * @return original array or a newly allocated page
     */
    static /*P*/ byte[] p_transfer(byte[] array) {
        return array;
    }

    /**
     * Copies from an array to a page, but only if the page type is not an array.
     *
     * @return original array or page with copied data
     */
    static /*P*/ byte[] p_transferTo(byte[] array, /*P*/ byte[] page) {
        return array;
    }

    static byte p_byteGet(/*P*/ byte[] page, int index) {
        return page[index];
    }

    static int p_ubyteGet(/*P*/ byte[] page, int index) {
        return page[index] & 0xff;
    }

    static void p_bytePut(/*P*/ byte[] page, int index, byte v) {
        page[index] = v;
    }

    static void p_bytePut(/*P*/ byte[] page, int index, int v) {
        page[index] = (byte) v;
    }

    static int p_ushortGetLE(/*P*/ byte[] page, int index) {
        return decodeUnsignedShortLE(page, index);
    }

    static void p_shortPutLE(/*P*/ byte[] page, int index, int v) {
        encodeShortLE(page, index, v);
    }

    static int p_intGetLE(/*P*/ byte[] page, int index) {
        return decodeIntLE(page, index);
    }

    static void p_intPutLE(/*P*/ byte[] page, int index, int v) {
        encodeIntLE(page, index, v);
    }

    static int p_uintGetVar(/*P*/ byte[] page, int index) {
        return decodeUnsignedVarInt(page, index);
    }

    static int p_uintPutVar(/*P*/ byte[] page, int index, int v) {
        return encodeUnsignedVarInt(page, index, v);
    }

    static int p_uintVarSize(int v) {
        return calcUnsignedVarIntLength(v);
    }

    static long p_uint48GetLE(/*P*/ byte[] page, int index) {
        return decodeUnsignedInt48LE(page, index);
    }

    static void p_int48PutLE(/*P*/ byte[] page, int index, long v) {
        encodeInt48LE(page, index, v);
    }

    static long p_longGetLE(/*P*/ byte[] page, int index) {
        return decodeLongLE(page, index);
    }

    static void p_longPutLE(/*P*/ byte[] page, int index, long v) {
        encodeLongLE(page, index, v);
    }

    static long p_longGetBE(/*P*/ byte[] page, int index) {
        return decodeLongBE(page, index);
    }

    static void p_longPutBE(/*P*/ byte[] page, int index, long v) {
        encodeLongBE(page, index, v);
    }

    static long p_ulongGetVar(/*P*/ byte[] page, IntegerRef ref) {
        return decodeUnsignedVarLong(page, ref);
    }

    static int p_ulongPutVar(/*P*/ byte[] page, int index, long v) {
        return encodeUnsignedVarLong(page, index, v);
    }

    static int p_ulongVarSize(long v) {
        return calcUnsignedVarLongLength(v);
    }

    static void p_clear(/*P*/ byte[] page, int fromIndex, int toIndex) {
        java.util.Arrays.fill(page, fromIndex, toIndex, (byte) 0);
    }

    /**
     * Returns page if it's an array, else copies to given array and returns that.
     */
    static /*P*/ byte[] p_copyIfNotArray(/*P*/ byte[] page, byte[] array) {
        return page;
    }

    static void p_copyFromArray(byte[] src, int srcStart,
                                /*P*/ byte[] dstPage, int dstStart, int len)
    {
        System.arraycopy(src, srcStart, dstPage, dstStart, len);
    }

    static void p_copyToArray(/*P*/ byte[] srcPage, int srcStart,
                              byte[] dst, int dstStart, int len)
    {
        System.arraycopy(srcPage, srcStart, dst, dstStart, len);
    }

    static void p_copyFromBB(ByteBuffer src, /*P*/ byte[] dstPage, int dstStart, int len) {
        src.get(dstPage, dstStart, len);
    }

    static void p_copyToBB(/*P*/ byte[] srcPage, int srcStart, ByteBuffer dst, int len) {
        dst.put(srcPage, srcStart, len);
    }

    static void p_copy(/*P*/ byte[] srcPage, int srcStart,
                       /*P*/ byte[] dstPage, int dstStart, int len)
    {
        System.arraycopy(srcPage, srcStart, dstPage, dstStart, len);
    }

    /**
     * Performs multiple array copies, correctly ordered to prevent clobbering. The copies
     * must not overlap, and start1 must be less than start2.
     */
    static void p_copies(/*P*/ byte[] page,
                         int start1, int dest1, int length1,
                         int start2, int dest2, int length2)
    {
        if (dest1 < start1) {
            p_copy(page, start1, page, dest1, length1);
            p_copy(page, start2, page, dest2, length2);
        } else {
            p_copy(page, start2, page, dest2, length2);
            p_copy(page, start1, page, dest1, length1);
        }
    }

    /**
     * Performs multiple array copies, correctly ordered to prevent clobbering. The copies
     * must not overlap, start1 must be less than start2, and start2 be less than start3.
     */
    static void p_copies(/*P*/ byte[] page,
                         int start1, int dest1, int length1,
                         int start2, int dest2, int length2,
                         int start3, int dest3, int length3)
    {
        if (dest1 < start1) {
            p_copy(page, start1, page, dest1, length1);
            p_copies(page, start2, dest2, length2, start3, dest3, length3);
        } else {
            p_copies(page, start2, dest2, length2, start3, dest3, length3);
            p_copy(page, start1, page, dest1, length1);
        }
    }

    static int p_compareKeysPageToArray(/*P*/ byte[] apage, int aoff, int alen,
                                        byte[] b, int boff, int blen)
    {
        return compareUnsigned(apage, aoff, alen, b, boff, blen);
    }

    static int p_compareKeysPageToPage(/*P*/ byte[] apage, int aoff, int alen,
                                       /*P*/ byte[] bpage, int boff, int blen)
    {
        return compareUnsigned(apage, aoff, alen, bpage, boff, blen);
    }

    static byte[] p_midKeyLowPage(/*P*/ byte[] lowPage, int lowOff, int lowLen,
                                  byte[] high, int highOff)
    {
        return midKey(lowPage, lowOff, lowLen, high, highOff);
    }

    static byte[] p_midKeyHighPage(byte[] low, int lowOff, int lowLen,
                                   /*P*/ byte[] highPage, int highOff)
    {
        return midKey(low, lowOff, lowLen, highPage, highOff);
    }

    static byte[] p_midKeyLowHighPage(/*P*/ byte[] lowPage, int lowOff, int lowLen,
                                      /*P*/ byte[] highPage, int highOff)
    {
        return midKey(lowPage, lowOff, lowLen, highPage, highOff);
    }

    static int p_crc32(/*P*/ byte[] srcPage, int srcStart, int len) {
        CRC32 crc = new CRC32();
        crc.update(srcPage, srcStart, len);
        return (int) crc.getValue();
    }
}
