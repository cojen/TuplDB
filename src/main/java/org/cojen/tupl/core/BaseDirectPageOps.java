/*
 *  Copyright (C) 2024 Cojen.org
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
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.core;

import java.io.IOException;

import java.lang.foreign.MemorySegment;

import java.util.Arrays;

import java.util.zip.CRC32;

import org.cojen.tupl.ClosedIndexException;
import org.cojen.tupl.DeletedIndexException;

import org.cojen.tupl.diag.EventListener;

import org.cojen.tupl.io.MappedPageArray;

import org.cojen.tupl.util.Latch;
import org.cojen.tupl.util.Runner;

import static org.cojen.tupl.core.Node.*;

/**
 * @author Brian S. O'Neill
 * @see DirectPageOps
 */
class BaseDirectPageOps {
    static final int NODE_OVERHEAD = 100 - 24; // 6 fewer fields

    private static final long EMPTY_TREE_LEAF, CLOSED_TREE_PAGE, DELETED_TREE_PAGE, STUB_TREE_PAGE;

    static {
        EMPTY_TREE_LEAF = newEmptyTreePage
            (TN_HEADER_SIZE, TYPE_TN_LEAF | LOW_EXTREMITY | HIGH_EXTREMITY);

        CLOSED_TREE_PAGE = newEmptyTreePage(TN_HEADER_SIZE + 8, TYPE_TN_IN);

        DELETED_TREE_PAGE = newEmptyTreePage(TN_HEADER_SIZE + 8, TYPE_TN_IN);

        STUB_TREE_PAGE = newEmptyTreePage(TN_HEADER_SIZE + 8, TYPE_TN_IN);
    }

    private static long newEmptyTreePage(int pageSize, int type) {
        long empty = p_callocPage(pageSize);

        DirectPageOps.p_bytePut(empty, 0, type);

        // Set fields such that binary search returns ~0 and availableBytes returns 0.

        // Note: Same as Node.clearEntries.
        DirectPageOps.p_shortPutLE(empty, 4,  TN_HEADER_SIZE);     // leftSegTail
        DirectPageOps.p_shortPutLE(empty, 6,  pageSize - 1);       // rightSegTail
        DirectPageOps.p_shortPutLE(empty, 8,  TN_HEADER_SIZE);     // searchVecStart
        DirectPageOps.p_shortPutLE(empty, 10, TN_HEADER_SIZE - 2); // searchVecEnd

        return empty;
    }

    static long p_null() {
        return 0;
    }

    static long p_nonTreePage() {
        return EMPTY_TREE_LEAF;
    }

    static long p_closedTreePage() {
        return CLOSED_TREE_PAGE;
    }

    static long p_deletedTreePage() {
        return DELETED_TREE_PAGE;
    }

    static boolean isClosedOrDeleted(long page) {
        return page == CLOSED_TREE_PAGE || page == DELETED_TREE_PAGE;
    }

    /**
     * Throws a ClosedIndexException or a DeletedIndexException, depending on the page type.
     */
    static void checkClosedIndexException(long page) throws ClosedIndexException {
        if (isClosedOrDeleted(page)) {
            throw newClosedIndexException(page);
        }
    }

    /**
     * Returns a ClosedIndexException or a DeletedIndexException, depending on the page type.
     */
    static ClosedIndexException newClosedIndexException(long page) {
        return page == DELETED_TREE_PAGE ? new DeletedIndexException() : new ClosedIndexException();
    }

    static long p_stubTreePage() {
        return STUB_TREE_PAGE;
    }

    public static long p_alloc(int size) {
        return DirectMemory.malloc(size);
    }

    static long p_allocPage(int size) {
        return DirectMemory.malloc(Math.abs(size), size < 0); // aligned if negative
    }

    static long p_callocPage(int size) {
        return DirectMemory.calloc(Math.abs(size), size < 0); // aligned if negative
    }

    static long[] p_allocArray(int size) {
        return new long[size];
    }

    public static void p_delete(final long page) {
        // Only delete pages that were allocated from the Unsafe class and aren't globals.
        if (page != CLOSED_TREE_PAGE && page != EMPTY_TREE_LEAF && !inArena(page)) {
            DirectMemory.free(page);
        }
    }

    static class Arena implements Comparable<Arena> {
        private final MappedPageArray mPageArray;
        private final long mStartPtr;
        private final long mEndPtr; // exclusive

        private long mNextPtr;

        Arena(int pageSize, long pageCount, EventListener listener) throws IOException {
            pageSize = Math.abs(pageSize);
            mPageArray = MappedPageArray.open(pageSize, pageCount, null, null, listener);
            mStartPtr = mPageArray.directPagePointer(0);
            mEndPtr = mStartPtr + (pageSize * pageCount);
            synchronized (this) {
                mNextPtr = mStartPtr;
            }

            if (true) {
                // Pre-touch the pages, using the OS page size.

                final int numThreads = Runtime.getRuntime().availableProcessors();
                final var latch = new Latch(numThreads);
                final int osPageSize = SysInfo.pageSize();
                final long osPageCount = (mEndPtr - mStartPtr) / osPageSize;

                long startPtr = mStartPtr;
                for (int i=1; i<numThreads; i++) {
                    final long fstartPtr = startPtr;
                    final long endPtr = mStartPtr + (i * osPageCount / numThreads) * osPageSize;
                    Runner.start(() -> preTouch(fstartPtr, endPtr, osPageSize, latch));
                    startPtr = endPtr;
                }

                // Do the last range in this thread.
                preTouch(startPtr, mEndPtr, osPageSize, latch);

                // Wait for all threads to finish.
                latch.acquireExclusive();
            }
        }

        private static void preTouch(long startPtr, long endPtr, int pageSize, Latch notify) {
            for (long ptr = startPtr; ptr < endPtr; ptr += pageSize) {
                DirectPageOps.p_bytePut(ptr, 0, 0);
            }
            notify.releaseShared();
        }

        @Override
        public int compareTo(Arena other) {
            return Long.compareUnsigned(mStartPtr, other.mStartPtr);
        }

        synchronized long p_calloc(int size) {
            int pageSize = mPageArray.pageSize();
            if (size != pageSize) {
                throw new IllegalArgumentException();
            }
            long ptr = mNextPtr;
            if (ptr >= mEndPtr) {
                return p_null();
            }
            mNextPtr = ptr + pageSize;
            return ptr;
        }

        synchronized void close() throws IOException {
            mNextPtr = mEndPtr;
            mPageArray.close();
        }
    }

    private static volatile Arena[] cArenas;

    static boolean inArena(final long page) {
        Arena[] arenas = cArenas;

        if (arenas != null) {
            // Binary search.

            int low = 0;
            int high = arenas.length - 1;

            while (low <= high) {
                int mid = (low + high) >>> 1;
                int cmp = Long.compareUnsigned(arenas[mid].mStartPtr, page);
                if (cmp < 0) {
                    low = mid + 1;
                } else if (cmp > 0) {
                    high = mid - 1;
                } else {
                    return true;
                }
            }

            if (low > 0 && Long.compareUnsigned(page, arenas[low - 1].mEndPtr) < 0) {
                return true;
            }
        }

        return false;
    }

    private static synchronized void registerArena(Arena arena) {
        Arena[] existing = cArenas;
        if (existing == null) {
            cArenas = new Arena[] {arena};
        } else {
            // Arenas are searchable in a sorted array, and nothing special needs to be done to
            // handle overlapping ranges. We trust that the operating system doesn't do this.
            var arenas = new Arena[existing.length + 1];
            System.arraycopy(existing, 0, arenas, 0, existing.length);
            arenas[arenas.length - 1] = arena;
            Arrays.sort(arenas);
            cArenas = arenas;
        }
    }

    private static synchronized void unregisterArena(Arena arena) {
        Arena[] existing = cArenas;

        if (existing == null) {
            return;
        }

        if (existing.length == 1) {
            if (existing[0] == arena) {
                cArenas = null;
            }
            return;
        }

        try {
            var arenas = new Arena[existing.length - 1];
            for (int i=0,j=0; i<existing.length; i++) {
                Arena a = existing[i];
                if (a != arena) {
                    arenas[j++] = a;
                }
            }
            cArenas = arenas;
        } catch (IndexOutOfBoundsException e) {
            // Not found.
        }
    }

    static Object p_arenaAlloc(int pageSize, long pageCount, EventListener listener)
        throws IOException
    {
        try {
            var arena = new Arena(pageSize, pageCount, listener);
            registerArena(arena);
            return arena;
        } catch (UnsupportedOperationException e) {
            // Not a 64-bit platform, so allocate pages using calloc.
            return null;
        }
    }

    static void p_arenaDelete(Object arena) throws IOException {
        if (arena instanceof Arena a) {
            // Unregister before closing, in case new allocations are allowed in the recycled
            // memory range and then deleted. The delete method would erroneously think the page
            // is still in an arena and do nothing.
            unregisterArena(a);
            a.close();
        } else if (arena != null) {
            throw new IllegalArgumentException();
        }
    }

    static long p_callocPage(Object arena, int size) {
        if (arena instanceof Arena a) {
            // Assume arena allocations are always aligned.
            final long page = a.p_calloc(Math.abs(size));
            if (page != p_null()) {
                return page;
            }
        } else if (arena != null) {
            throw new IllegalArgumentException();
        }

        return p_callocPage(size);
    }

    static long p_clonePage(long page, int pageSize) {
        long dst = p_allocPage(pageSize);
        pageSize = Math.abs(pageSize);
        DirectPageOps.p_copy(page, 0, dst, 0, pageSize);
        return dst;
    }

    static long p_transfer(byte[] array) {
        int length = array.length;
        final long page = p_alloc(length);
        DirectPageOps.p_copyFromArray(array, 0, page, 0, length);
        return page;
    }

    static long p_transferPage(byte[] array, int pageSize) {
        final long page = p_allocPage(pageSize);
        DirectPageOps.p_copyFromArray(array, 0, page, 0, Math.abs(pageSize));
        return page;
    }

    static long p_transferArrayToPage(byte[] array, long page) {
        DirectPageOps.p_copyFromArray(array, 0, page, 0, array.length);
        return page;
    }

    static void p_transferPageToArray(long page, byte[] array) {
        DirectPageOps.p_copyToArray(page, 0, array, 0, array.length);
    }

    static int p_ubyteGet(final long page, int index) {
        return DirectPageOps.p_byteGet(page, index) & 0xff;
    }

    static void p_bytePut(final long page, int index, int v) {
        DirectPageOps.p_bytePut(page, index, (byte) v);
    }

    static long p_uintGetVar(final long page, int index) {
        int v = DirectPageOps.p_byteGet(page, index++);
        if (v < 0) {
            switch ((v >> 4) & 0x07) {
            case 0x00: case 0x01: case 0x02: case 0x03:
                v = (1 << 7)
                    + (((v & 0x3f) << 8)
                       | p_ubyteGet(page, index++));
                break;
            case 0x04: case 0x05:
                v = ((1 << 14) + (1 << 7))
                    + (((v & 0x1f) << 16)
                       | (p_ubyteGet(page, index++) << 8)
                       | p_ubyteGet(page, index++));
                break;
            case 0x06:
                v = ((1 << 21) + (1 << 14) + (1 << 7))
                    + (((v & 0x0f) << 24)
                       | (p_ubyteGet(page, index++) << 16)
                       | (p_ubyteGet(page, index++) << 8)
                       | p_ubyteGet(page, index++));
                break;
            default:
                v = ((1 << 28) + (1 << 21) + (1 << 14) + (1 << 7))
                    + ((DirectPageOps.p_byteGet(page, index++) << 24)
                       | (p_ubyteGet(page, index++) << 16)
                       | (p_ubyteGet(page, index++) << 8)
                       | p_ubyteGet(page, index++));
                break;
            }
        }
        return (((long) index) << 32L) | (v & 0xffff_ffffL);
    }

    static int p_uintPutVar(final long page, int index, int v) {
        if (v < (1 << 7)) {
            if (v < 0) {
                v -= (1 << 28) + (1 << 21) + (1 << 14) + (1 << 7);
                DirectPageOps.p_bytePut(page, index++, 0xff);
                DirectPageOps.p_bytePut(page, index++, v >> 24);
                DirectPageOps.p_bytePut(page, index++, v >> 16);
                DirectPageOps.p_bytePut(page, index++, v >> 8);
            }
        } else {
            v -= (1 << 7);
            if (v < (1 << 14)) {
                DirectPageOps.p_bytePut(page, index++, 0x80 | (v >> 8));
            } else {
                v -= (1 << 14);
                if (v < (1 << 21)) {
                    DirectPageOps.p_bytePut(page, index++, 0xc0 | (v >> 16));
                } else {
                    v -= (1 << 21);
                    if (v < (1 << 28)) {
                        DirectPageOps.p_bytePut(page, index++, 0xe0 | (v >> 24));
                    } else {
                        v -= (1 << 28);
                        DirectPageOps.p_bytePut(page, index++, 0xf0);
                        DirectPageOps.p_bytePut(page, index++, v >> 24);
                    }
                    DirectPageOps.p_bytePut(page, index++, v >> 16);
                }
                DirectPageOps.p_bytePut(page, index++, v >> 8);
            }
        }
        DirectPageOps.p_bytePut(page, index++, v);
        return index;
    }

    static long p_uint48GetLE(final long page, int index) {
        return DirectPageOps.p_intGetLE(page, index) & 0xffff_ffffL
            | (((long) DirectPageOps.p_ushortGetLE(page, index + 4)) << 32);
    }

    static void p_int48PutLE(final long page, int index, long v) {
        DirectPageOps.p_intPutLE(page, index, (int) v);
        DirectPageOps.p_shortPutLE(page, index + 4, (short) (v >> 32));
    }

    static long p_ulongGetVar(final long page, IntegerRef ref) {
        int offset = ref.get();
        int val = DirectPageOps.p_byteGet(page, offset++);
        if (val >= 0) {
            ref.set(offset);
            return val;
        }
        long decoded;
        switch ((val >> 4) & 0x07) {
        case 0x00: case 0x01: case 0x02: case 0x03:
            decoded = (1L << 7) +
                (((val & 0x3f) << 8)
                 | p_ubyteGet(page, offset++));
            break;
        case 0x04: case 0x05:
            decoded = ((1L << 14) + (1L << 7))
                + (((val & 0x1f) << 16)
                   | (p_ubyteGet(page, offset++) << 8)
                   | p_ubyteGet(page, offset++));
            break;
        case 0x06:
            decoded = ((1L << 21) + (1L << 14) + (1L << 7))
                + (((val & 0x0f) << 24)
                   | (p_ubyteGet(page, offset++) << 16)
                   | (p_ubyteGet(page, offset++) << 8)
                   | p_ubyteGet(page, offset++));
            break;
        default:
            switch (val & 0x0f) {
            default:
                decoded = ((1L << 28) + (1L << 21) + (1L << 14) + (1L << 7))
                    + (((val & 0x07L) << 32)
                       | (((long) p_ubyteGet(page, offset++)) << 24)
                       | (((long) p_ubyteGet(page, offset++)) << 16)
                       | (((long) p_ubyteGet(page, offset++)) << 8)
                       | ((long) p_ubyteGet(page, offset++)));
                break;
            case 0x08: case 0x09: case 0x0a: case 0x0b:
                decoded = ((1L << 35)
                           + (1L << 28) + (1L << 21) + (1L << 14) + (1L << 7))
                    + (((val & 0x03L) << 40)
                       | (((long) p_ubyteGet(page, offset++)) << 32)
                       | (((long) p_ubyteGet(page, offset++)) << 24)
                       | (((long) p_ubyteGet(page, offset++)) << 16)
                       | (((long) p_ubyteGet(page, offset++)) << 8)
                       | ((long) p_ubyteGet(page, offset++)));
                break;
            case 0x0c: case 0x0d:
                decoded = ((1L << 42) + (1L << 35)
                           + (1L << 28) + (1L << 21) + (1L << 14) + (1L << 7))
                    + (((val & 0x01L) << 48)
                       | (((long) p_ubyteGet(page, offset++)) << 40)
                       | (((long) p_ubyteGet(page, offset++)) << 32)
                       | (((long) p_ubyteGet(page, offset++)) << 24)
                       | (((long) p_ubyteGet(page, offset++)) << 16)
                       | (((long) p_ubyteGet(page, offset++)) << 8)
                       | ((long) p_ubyteGet(page, offset++)));
                break;
            case 0x0e:
                decoded = ((1L << 49) + (1L << 42) + (1L << 35)
                           + (1L << 28) + (1L << 21) + (1L << 14) + (1L << 7))
                    + ((((long) p_ubyteGet(page, offset++)) << 48)
                       | (((long) p_ubyteGet(page, offset++)) << 40)
                       | (((long) p_ubyteGet(page, offset++)) << 32)
                       | (((long) p_ubyteGet(page, offset++)) << 24)
                       | (((long) p_ubyteGet(page, offset++)) << 16)
                       | (((long) p_ubyteGet(page, offset++)) << 8)
                       | ((long) p_ubyteGet(page, offset++)));
                break;
            case 0x0f:
                decoded = ((1L << 56) + (1L << 49) + (1L << 42) + (1L << 35)
                           + (1L << 28) + (1L << 21) + (1L << 14) + (1L << 7))
                    + ((((long) p_ubyteGet(page, offset++)) << 56)
                       | (((long) p_ubyteGet(page, offset++)) << 48)
                       | (((long) p_ubyteGet(page, offset++)) << 40)
                       | (((long) p_ubyteGet(page, offset++)) << 32)
                       | (((long) p_ubyteGet(page, offset++)) << 24)
                       | (((long) p_ubyteGet(page, offset++)) << 16)
                       | (((long) p_ubyteGet(page, offset++)) << 8L)
                       | ((long) p_ubyteGet(page, offset++)));
                break;
            }
            break;
        }

        ref.set(offset);
        return decoded;
    }

    static int p_ulongPutVar(final long page, int index, long v) {
        if (v < (1L << 7)) {
            if (v < 0) {
                v -= (1L << 56) + (1L << 49) + (1L << 42) + (1L << 35)
                    + (1L << 28) + (1L << 21) + (1L << 14) + (1L << 7);
                DirectPageOps.p_bytePut(page, index++, 0xff);
                DirectPageOps.p_bytePut(page, index++, (byte) (v >> 56));
                DirectPageOps.p_bytePut(page, index++, (byte) (v >> 48));
                DirectPageOps.p_bytePut(page, index++, (byte) (v >> 40));
                DirectPageOps.p_bytePut(page, index++, (byte) (v >> 32));
                DirectPageOps.p_bytePut(page, index++, (byte) (v >> 24));
                DirectPageOps.p_bytePut(page, index++, (byte) (v >> 16));
                DirectPageOps.p_bytePut(page, index++, (byte) (v >> 8));
            }
        } else {
            v -= (1L << 7);
            if (v < (1L << 14)) {
                DirectPageOps.p_bytePut(page, index++, 0x80 | (int) (v >> 8));
            } else {
                v -= (1L << 14);
                if (v < (1L << 21)) {
                    DirectPageOps.p_bytePut(page, index++, 0xc0 | (int) (v >> 16));
                } else {
                    v -= (1L << 21);
                    if (v < (1L << 28)) {
                        DirectPageOps.p_bytePut(page, index++, 0xe0 | (int) (v >> 24));
                    } else {
                        v -= (1L << 28);
                        if (v < (1L << 35)) {
                            DirectPageOps.p_bytePut(page, index++, 0xf0 | (int) (v >> 32));
                        } else {
                            v -= (1L << 35);
                            if (v < (1L << 42)) {
                                DirectPageOps.p_bytePut(page, index++, 0xf8 | (int) (v >> 40));
                            } else {
                                v -= (1L << 42);
                                if (v < (1L << 49)) {
                                    DirectPageOps.p_bytePut(page, index++, 0xfc | (int) (v >> 48));
                                } else {
                                    v -= (1L << 49);
                                    if (v < (1L << 56)) {
                                        DirectPageOps.p_bytePut(page, index++, 0xfe);
                                    } else {
                                        v -= (1L << 56);
                                        DirectPageOps.p_bytePut(page, index++, 0xff);
                                        DirectPageOps.p_bytePut(page, index++, (byte) (v >> 56));
                                    }
                                    DirectPageOps.p_bytePut(page, index++, (byte) (v >> 48));
                                }
                                DirectPageOps.p_bytePut(page, index++, (byte) (v >> 40));
                            }
                            DirectPageOps.p_bytePut(page, index++, (byte) (v >> 32));
                        }
                        DirectPageOps.p_bytePut(page, index++, (byte) (v >> 24));
                    }
                    DirectPageOps.p_bytePut(page, index++, (byte) (v >> 16));
                }
                DirectPageOps.p_bytePut(page, index++, (byte) (v >> 8));
            }
        }
        DirectPageOps.p_bytePut(page, index++, (byte) v);
        return index;
    }

    static int p_ulongVarSize(long v) {
        return Utils.calcUnsignedVarLongLength(v);
    }

    static byte[] p_copyIfNotArray(final long page, byte[] dstArray) {
        DirectPageOps.p_copyToArray(page, 0, dstArray, 0, dstArray.length);
        return dstArray;
    }

    static void p_copies(final long page,
                         int start1, int dest1, int length1,
                         int start2, int dest2, int length2)
    {
        if (dest1 < start1) {
            DirectPageOps.p_copy(page, start1, page, dest1, length1);
            DirectPageOps.p_copy(page, start2, page, dest2, length2);
        } else {
            DirectPageOps.p_copy(page, start2, page, dest2, length2);
            DirectPageOps.p_copy(page, start1, page, dest1, length1);
        }
    }

    static void p_copies(final long page,
                         int start1, int dest1, int length1,
                         int start2, int dest2, int length2,
                         int start3, int dest3, int length3)
    {
        if (dest1 < start1) {
            DirectPageOps.p_copy(page, start1, page, dest1, length1);
            p_copies(page, start2, dest2, length2, start3, dest3, length3);
        } else {
            p_copies(page, start2, dest2, length2, start3, dest3, length3);
            DirectPageOps.p_copy(page, start1, page, dest1, length1);
        }
    }

    static int p_compareKeysPageToArray(final long apage, int aoff, int alen,
                                        byte[] b, int boff, int blen)
    {
        int minLen = Math.min(alen, blen);
        for (int i=0; i<minLen; i++) {
            byte ab = DirectPageOps.p_byteGet(apage, aoff++);
            byte bb = b[boff + i];
            if (ab != bb) {
                return (ab & 0xff) - (bb & 0xff);
            }
        }
        return alen - blen;
    }

    static int p_compareKeysPageToPage(final long apage, int aoff, int alen,
                                       final long bpage, int boff, int blen)
    {
        int minLen = Math.min(alen, blen);
        for (int i=0; i<minLen; i++) {
            byte ab = DirectPageOps.p_byteGet(apage, aoff++);
            byte bb = DirectPageOps.p_byteGet(bpage, boff++);
            if (ab != bb) {
                return (ab & 0xff) - (bb & 0xff);
            }
        }
        return alen - blen;
    }

    static byte[] p_midKeyLowPage(final long lowPage, int lowOff, int lowLen,
                                  byte[] high, int highOff)
    {
        for (int i=0; i<lowLen; i++) {
            byte lo = DirectPageOps.p_byteGet(lowPage, lowOff + i);
            byte hi = high[highOff + i];
            if (lo != hi) {
                var mid = new byte[i + 1];
                DirectPageOps.p_copyToArray(lowPage, lowOff, mid, 0, i);
                mid[i] = (byte) (((lo & 0xff) + (hi & 0xff) + 1) >> 1);
                return mid;
            }
        }
        var mid = new byte[lowLen + 1];
        System.arraycopy(high, highOff, mid, 0, mid.length);
        return mid;
    }

    static byte[] p_midKeyHighPage(byte[] low, int lowOff, int lowLen,
                                   final long highPage, int highOff)
    {
        for (int i=0; i<lowLen; i++) {
            byte lo = low[lowOff + i];
            byte hi = DirectPageOps.p_byteGet(highPage, highOff + i);
            if (lo != hi) {
                var mid = new byte[i + 1];
                System.arraycopy(low, lowOff, mid, 0, i);
                mid[i] = (byte) (((lo & 0xff) + (hi & 0xff) + 1) >> 1);
                return mid;
            }
        }
        var mid = new byte[lowLen + 1];
        DirectPageOps.p_copyToArray(highPage, highOff, mid, 0, mid.length);
        return mid;
    }

    static byte[] p_midKeyLowHighPage(final long lowPage, int lowOff, int lowLen,
                                      final long highPage, int highOff)
    {
        for (int i=0; i<lowLen; i++) {
            byte lo = DirectPageOps.p_byteGet(lowPage, lowOff + i);
            byte hi = DirectPageOps.p_byteGet(highPage, highOff + i);
            if (lo != hi) {
                var mid = new byte[i + 1];
                DirectPageOps.p_copyToArray(lowPage, lowOff, mid, 0, i);
                mid[i] = (byte) (((lo & 0xff) + (hi & 0xff) + 1) >> 1);
                return mid;
            }
        }
        var mid = new byte[lowLen + 1];
        DirectPageOps.p_copyToArray(highPage, highOff, mid, 0, mid.length);
        return mid;
    }

    static int p_crc32(long srcPage, int srcStart, int len) {
        var crc = new CRC32();
        crc.update(MemorySegment.ofAddress(srcPage + srcStart).reinterpret(len).asByteBuffer());
        return (int) crc.getValue();
    }
}
