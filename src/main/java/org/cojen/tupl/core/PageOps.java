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

import java.util.Arrays;

import org.cojen.tupl.ClosedIndexException;
import org.cojen.tupl.DeletedIndexException;

import org.cojen.tupl.diag.EventListener;

import org.cojen.tupl.io.MappedPageArray;

import org.cojen.tupl.util.Latch;
import org.cojen.tupl.util.Runner;

import static org.cojen.tupl.core.Node.*;

/**
 * Defines low-level methods for operating against database pages, as represented by off-heap
 * memory.
 *
 * @author Brian S. O'Neill
 */
final class PageOps extends DirectPageOps {
    /*
     * Approximate byte overhead per Node, assuming 32-bit pointers. Overhead is determined by
     * examining all the fields in the Node class, including inherited ones. In addition, each
     * Node is referenced by mNodeMapTable.
     *
     * References: 1 field per Node instance
     * Node class: 12 fields (mId is counted twice)
     * Clutch class: 1 field
     * Latch class: 3 fields
     * Object class: Minimum 8 byte overhead
     * Total: (17 * 4 + 8) = 76
     */
    static final int NODE_OVERHEAD = 76;

    private static final long EMPTY_TREE_LEAF, CLOSED_TREE_PAGE, DELETED_TREE_PAGE, STUB_TREE_PAGE;

    static {
        EMPTY_TREE_LEAF = newEmptyTreePage
            (TN_HEADER_SIZE, TYPE_TN_LEAF | LOW_EXTREMITY | HIGH_EXTREMITY);

        // Note that none of these special nodes set the extremity bits. See p_stubTreePage.

        CLOSED_TREE_PAGE = newEmptyTreePage(TN_HEADER_SIZE + 8, TYPE_TN_IN);

        DELETED_TREE_PAGE = newEmptyTreePage(TN_HEADER_SIZE + 8, TYPE_TN_IN);

        STUB_TREE_PAGE = newEmptyTreePage(TN_HEADER_SIZE + 8, TYPE_TN_IN);
    }

    private static long newEmptyTreePage(int pageSize, int type) {
        long empty = p_callocPage(pageSize);

        p_bytePut(empty, 0, type);

        // Set fields such that binary search returns ~0 and availableBytes returns 0.

        // Note: Same as Node.clearEntries.
        p_shortPutLE(empty, 4,  TN_HEADER_SIZE);     // leftSegTail
        p_shortPutLE(empty, 6,  pageSize - 1);       // rightSegTail
        p_shortPutLE(empty, 8,  TN_HEADER_SIZE);     // searchVecStart
        p_shortPutLE(empty, 10, TN_HEADER_SIZE - 2); // searchVecEnd

        return empty;
    }

    static long p_null() {
        return 0;
    }

    static long p_nonTreePage() {
        return EMPTY_TREE_LEAF;
    }

    /**
     * Returned page is 20 bytes, defining a closed tree internal node. Contents must not be
     * modified. The page can also be acted upon as a leaf node, considering that an empty leaf
     * node has the same structure. The extra 8 bytes at the end will simply be ignored.
     */
    static long p_closedTreePage() {
        return CLOSED_TREE_PAGE;
    }

    /**
     * See p_closedTreePage.
     */
    static long p_deletedTreePage() {
        return DELETED_TREE_PAGE;
    }

    static boolean isClosedOrDeleted(long pageAddr) {
        return pageAddr == CLOSED_TREE_PAGE || pageAddr == DELETED_TREE_PAGE;
    }

    /**
     * Throws a ClosedIndexException or a DeletedIndexException, depending on the page type.
     */
    static void checkClosedIndexException(long pageAddr) throws ClosedIndexException {
        if (isClosedOrDeleted(pageAddr)) {
            throw newClosedIndexException(pageAddr);
        }
    }

    /**
     * Returns a ClosedIndexException or a DeletedIndexException, depending on the page type.
     */
    static ClosedIndexException newClosedIndexException(long pageAddr) {
        return pageAddr == DELETED_TREE_PAGE
            ? new DeletedIndexException() : new ClosedIndexException();
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
    static long p_stubTreePage() {
        return STUB_TREE_PAGE;
    }

    public static long p_alloc(int size) {
        return DirectMemory.malloc(size);
    }

    /**
     * @param size pass negative for size for aligned allocation (if possible)
     */
    static long p_allocPage(int size) {
        return DirectMemory.malloc(Math.abs(size), size < 0); // aligned if negative
    }

    /**
     * @param size pass negative for size for aligned allocation (if possible)
     */
    static long p_callocPage(int size) {
        return DirectMemory.calloc(Math.abs(size), size < 0); // aligned if negative
    }

    public static void p_delete(long pageAddr) {
        // Only delete pages that were allocated from the Unsafe class and aren't globals.
        if (pageAddr != CLOSED_TREE_PAGE && pageAddr != EMPTY_TREE_LEAF && !inArena(pageAddr)) {
            DirectMemory.free(pageAddr);
        }
    }

    static class Arena implements Comparable<Arena> {
        private final MappedPageArray mPageArray;
        private final long mStartAddr;
        private final long mEndAddr; // exclusive

        private long mNextAddr;

        Arena(int pageSize, long pageCount, EventListener listener) throws IOException {
            pageSize = Math.abs(pageSize);
            mPageArray = MappedPageArray.factory(pageSize, pageCount, null, null, listener).get();
            mStartAddr = mPageArray.directPageAddress(0);
            mEndAddr = mStartAddr + (pageSize * pageCount);
            synchronized (this) {
                mNextAddr = mStartAddr;
            }

            if (true) {
                // Pre-touch the pages, using the OS page size.

                final int numThreads = Runtime.getRuntime().availableProcessors();
                final var latch = new Latch(numThreads);
                final int osPageSize = SysInfo.pageSize();
                final long osPageCount = (mEndAddr - mStartAddr) / osPageSize;

                long startAddr = mStartAddr;
                for (int i=1; i<numThreads; i++) {
                    final long fstartAddr = startAddr;
                    final long endAddr = mStartAddr + (i * osPageCount / numThreads) * osPageSize;
                    Runner.start(() -> preTouch(fstartAddr, endAddr, osPageSize, latch));
                    startAddr = endAddr;
                }

                // Do the last range in this thread.
                preTouch(startAddr, mEndAddr, osPageSize, latch);

                // Wait for all threads to finish.
                latch.acquireExclusive();
            }
        }

        private static void preTouch(long startAddr, long endAddr, int pageSize, Latch notify) {
            for (long addr = startAddr; addr < endAddr; addr += pageSize) {
                p_bytePut(addr, 0, 0);
            }
            notify.releaseShared();
        }

        @Override
        public int compareTo(Arena other) {
            return Long.compareUnsigned(mStartAddr, other.mStartAddr);
        }

        synchronized long p_calloc(int size) {
            int pageSize = mPageArray.pageSize();
            if (size != pageSize) {
                throw new IllegalArgumentException();
            }
            long addr = mNextAddr;
            if (addr >= mEndAddr) {
                return p_null();
            }
            mNextAddr = addr + pageSize;
            return addr;
        }

        synchronized void close() throws IOException {
            mNextAddr = mEndAddr;
            mPageArray.close();
        }
    }

    private static volatile Arena[] cArenas;

    static boolean inArena(final long pageAddr) {
        Arena[] arenas = cArenas;

        if (arenas != null) {
            // Binary search.

            int low = 0;
            int high = arenas.length - 1;

            while (low <= high) {
                int mid = (low + high) >>> 1;
                int cmp = Long.compareUnsigned(arenas[mid].mStartAddr, pageAddr);
                if (cmp < 0) {
                    low = mid + 1;
                } else if (cmp > 0) {
                    high = mid - 1;
                } else {
                    return true;
                }
            }

            if (low > 0 && Long.compareUnsigned(pageAddr, arenas[low - 1].mEndAddr) < 0) {
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

    /**
     * Allocates an "arena", which contains a fixed number of pages. Pages in an arena cannot
     * be deleted, and calling p_delete on arena pages does nothing. Call p_arenaDelete to
     * fully delete the entire arena when not used anymore.
     *
     * @param listener optional
     * @return null if not supported
     */
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

    /**
     * @throws IllegalArgumentException if unknown arena
     */
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

    /**
     * Allocate a zero-filled page from an arena. If arena is null or depleted, then a regular
     * page is allocated.
     *
     * @param size pass negative for size for aligned allocation (if possible)
     * @throws IllegalArgumentException if unknown arena or if page size doesn't match
     */
    static long p_callocPage(Object arena, int size) {
        if (arena instanceof Arena a) {
            // Assume arena allocations are always aligned.
            final long pageAddr = a.p_calloc(Math.abs(size));
            if (pageAddr != p_null()) {
                return pageAddr;
            }
        } else if (arena != null) {
            throw new IllegalArgumentException();
        }

        return p_callocPage(size);
    }

    /**
     * @param pageSize pass negative for size for aligned allocation (if possible)
     */
    static long p_clonePage(long pageAddr, int pageSize) {
        long dst = p_allocPage(pageSize);
        pageSize = Math.abs(pageSize);
        p_copy(pageAddr, 0, dst, 0, pageSize);
        return dst;
    }

    /**
     * Allocates a clone if the page type is not an array. Must be deleted.
     *
     * @return original array or a newly allocated page
     */
    static long p_transfer(byte[] array) {
        int length = array.length;
        final long pageAddr = p_alloc(length);
        p_copy(array, 0, pageAddr, 0, length);
        return pageAddr;
    }

    /**
     * @param pageSize pass negative for size for aligned allocation (if possible)
     */
    static long p_transferPage(byte[] array, int pageSize) {
        final long pageAddr = p_allocPage(pageSize);
        p_copy(array, 0, pageAddr, 0, Math.abs(pageSize));
        return pageAddr;
    }

    /**
     * Copies from an array to a page.
     *
     * @return page with copied data
     */
    static long p_transferArrayToPage(byte[] array, long pageAddr) {
        p_copy(array, 0, pageAddr, 0, array.length);
        return pageAddr;
    }

    /**
     * Copies from a page to an array.
     */
    static void p_transferPageToArray(long pageAddr, byte[] array) {
        p_copy(pageAddr, 0, array, 0, array.length);
    }

    static int p_ubyteGet(final long pageAddr, int index) {
        return p_byteGet(pageAddr, index) & 0xff;
    }

    static void p_bytePut(final long pageAddr, int index, int v) {
        p_bytePut(pageAddr, index, (byte) v);
    }

    /**
     * Value is in the lower word, and updated index is in the upper word.
     */
    static long p_uintGetVar(final long pageAddr, int index) {
        int v = p_byteGet(pageAddr, index++);
        if (v < 0) {
            switch ((v >> 4) & 0x07) {
            case 0x00: case 0x01: case 0x02: case 0x03:
                v = (1 << 7)
                    + (((v & 0x3f) << 8)
                       | p_ubyteGet(pageAddr, index++));
                break;
            case 0x04: case 0x05:
                v = ((1 << 14) + (1 << 7))
                    + (((v & 0x1f) << 16)
                       | (p_ubyteGet(pageAddr, index++) << 8)
                       | p_ubyteGet(pageAddr, index++));
                break;
            case 0x06:
                v = ((1 << 21) + (1 << 14) + (1 << 7))
                    + (((v & 0x0f) << 24)
                       | (p_ubyteGet(pageAddr, index++) << 16)
                       | (p_ubyteGet(pageAddr, index++) << 8)
                       | p_ubyteGet(pageAddr, index++));
                break;
            default:
                v = ((1 << 28) + (1 << 21) + (1 << 14) + (1 << 7))
                    + ((p_byteGet(pageAddr, index++) << 24)
                       | (p_ubyteGet(pageAddr, index++) << 16)
                       | (p_ubyteGet(pageAddr, index++) << 8)
                       | p_ubyteGet(pageAddr, index++));
                break;
            }
        }
        return (((long) index) << 32L) | (v & 0xffff_ffffL);
    }

    static int p_uintPutVar(final long pageAddr, int index, int v) {
        if (v < (1 << 7)) {
            if (v < 0) {
                v -= (1 << 28) + (1 << 21) + (1 << 14) + (1 << 7);
                p_bytePut(pageAddr, index++, 0xff);
                p_bytePut(pageAddr, index++, v >> 24);
                p_bytePut(pageAddr, index++, v >> 16);
                p_bytePut(pageAddr, index++, v >> 8);
            }
        } else {
            v -= (1 << 7);
            if (v < (1 << 14)) {
                p_bytePut(pageAddr, index++, 0x80 | (v >> 8));
            } else {
                v -= (1 << 14);
                if (v < (1 << 21)) {
                    p_bytePut(pageAddr, index++, 0xc0 | (v >> 16));
                } else {
                    v -= (1 << 21);
                    if (v < (1 << 28)) {
                        p_bytePut(pageAddr, index++, 0xe0 | (v >> 24));
                    } else {
                        v -= (1 << 28);
                        p_bytePut(pageAddr, index++, 0xf0);
                        p_bytePut(pageAddr, index++, v >> 24);
                    }
                    p_bytePut(pageAddr, index++, v >> 16);
                }
                p_bytePut(pageAddr, index++, v >> 8);
            }
        }
        p_bytePut(pageAddr, index++, v);
        return index;
    }

    static long p_uint48GetLE(final long pageAddr, int index) {
        return p_intGetLE(pageAddr, index) & 0xffff_ffffL
            | (((long) p_ushortGetLE(pageAddr, index + 4)) << 32);
    }

    static void p_int48PutLE(final long pageAddr, int index, long v) {
        p_intPutLE(pageAddr, index, (int) v);
        p_shortPutLE(pageAddr, index + 4, (short) (v >> 32));
    }

    static long p_ulongGetVar(final long pageAddr, IntegerRef ref) {
        int offset = ref.get();
        int val = p_byteGet(pageAddr, offset++);
        if (val >= 0) {
            ref.set(offset);
            return val;
        }
        long decoded;
        switch ((val >> 4) & 0x07) {
        case 0x00: case 0x01: case 0x02: case 0x03:
            decoded = (1L << 7) +
                (((val & 0x3f) << 8)
                 | p_ubyteGet(pageAddr, offset++));
            break;
        case 0x04: case 0x05:
            decoded = ((1L << 14) + (1L << 7))
                + (((val & 0x1f) << 16)
                   | (p_ubyteGet(pageAddr, offset++) << 8)
                   | p_ubyteGet(pageAddr, offset++));
            break;
        case 0x06:
            decoded = ((1L << 21) + (1L << 14) + (1L << 7))
                + (((val & 0x0f) << 24)
                   | (p_ubyteGet(pageAddr, offset++) << 16)
                   | (p_ubyteGet(pageAddr, offset++) << 8)
                   | p_ubyteGet(pageAddr, offset++));
            break;
        default:
            switch (val & 0x0f) {
            default:
                decoded = ((1L << 28) + (1L << 21) + (1L << 14) + (1L << 7))
                    + (((val & 0x07L) << 32)
                       | (((long) p_ubyteGet(pageAddr, offset++)) << 24)
                       | (((long) p_ubyteGet(pageAddr, offset++)) << 16)
                       | (((long) p_ubyteGet(pageAddr, offset++)) << 8)
                       | ((long) p_ubyteGet(pageAddr, offset++)));
                break;
            case 0x08: case 0x09: case 0x0a: case 0x0b:
                decoded = ((1L << 35)
                           + (1L << 28) + (1L << 21) + (1L << 14) + (1L << 7))
                    + (((val & 0x03L) << 40)
                       | (((long) p_ubyteGet(pageAddr, offset++)) << 32)
                       | (((long) p_ubyteGet(pageAddr, offset++)) << 24)
                       | (((long) p_ubyteGet(pageAddr, offset++)) << 16)
                       | (((long) p_ubyteGet(pageAddr, offset++)) << 8)
                       | ((long) p_ubyteGet(pageAddr, offset++)));
                break;
            case 0x0c: case 0x0d:
                decoded = ((1L << 42) + (1L << 35)
                           + (1L << 28) + (1L << 21) + (1L << 14) + (1L << 7))
                    + (((val & 0x01L) << 48)
                       | (((long) p_ubyteGet(pageAddr, offset++)) << 40)
                       | (((long) p_ubyteGet(pageAddr, offset++)) << 32)
                       | (((long) p_ubyteGet(pageAddr, offset++)) << 24)
                       | (((long) p_ubyteGet(pageAddr, offset++)) << 16)
                       | (((long) p_ubyteGet(pageAddr, offset++)) << 8)
                       | ((long) p_ubyteGet(pageAddr, offset++)));
                break;
            case 0x0e:
                decoded = ((1L << 49) + (1L << 42) + (1L << 35)
                           + (1L << 28) + (1L << 21) + (1L << 14) + (1L << 7))
                    + ((((long) p_ubyteGet(pageAddr, offset++)) << 48)
                       | (((long) p_ubyteGet(pageAddr, offset++)) << 40)
                       | (((long) p_ubyteGet(pageAddr, offset++)) << 32)
                       | (((long) p_ubyteGet(pageAddr, offset++)) << 24)
                       | (((long) p_ubyteGet(pageAddr, offset++)) << 16)
                       | (((long) p_ubyteGet(pageAddr, offset++)) << 8)
                       | ((long) p_ubyteGet(pageAddr, offset++)));
                break;
            case 0x0f:
                decoded = ((1L << 56) + (1L << 49) + (1L << 42) + (1L << 35)
                           + (1L << 28) + (1L << 21) + (1L << 14) + (1L << 7))
                    + ((((long) p_ubyteGet(pageAddr, offset++)) << 56)
                       | (((long) p_ubyteGet(pageAddr, offset++)) << 48)
                       | (((long) p_ubyteGet(pageAddr, offset++)) << 40)
                       | (((long) p_ubyteGet(pageAddr, offset++)) << 32)
                       | (((long) p_ubyteGet(pageAddr, offset++)) << 24)
                       | (((long) p_ubyteGet(pageAddr, offset++)) << 16)
                       | (((long) p_ubyteGet(pageAddr, offset++)) << 8L)
                       | ((long) p_ubyteGet(pageAddr, offset++)));
                break;
            }
            break;
        }

        ref.set(offset);
        return decoded;
    }

    static int p_ulongPutVar(final long pageAddr, int index, long v) {
        if (v < (1L << 7)) {
            if (v < 0) {
                v -= (1L << 56) + (1L << 49) + (1L << 42) + (1L << 35)
                    + (1L << 28) + (1L << 21) + (1L << 14) + (1L << 7);
                p_bytePut(pageAddr, index++, 0xff);
                p_bytePut(pageAddr, index++, (byte) (v >> 56));
                p_bytePut(pageAddr, index++, (byte) (v >> 48));
                p_bytePut(pageAddr, index++, (byte) (v >> 40));
                p_bytePut(pageAddr, index++, (byte) (v >> 32));
                p_bytePut(pageAddr, index++, (byte) (v >> 24));
                p_bytePut(pageAddr, index++, (byte) (v >> 16));
                p_bytePut(pageAddr, index++, (byte) (v >> 8));
            }
        } else {
            v -= (1L << 7);
            if (v < (1L << 14)) {
                p_bytePut(pageAddr, index++, 0x80 | (int) (v >> 8));
            } else {
                v -= (1L << 14);
                if (v < (1L << 21)) {
                    p_bytePut(pageAddr, index++, 0xc0 | (int) (v >> 16));
                } else {
                    v -= (1L << 21);
                    if (v < (1L << 28)) {
                        p_bytePut(pageAddr, index++, 0xe0 | (int) (v >> 24));
                    } else {
                        v -= (1L << 28);
                        if (v < (1L << 35)) {
                            p_bytePut(pageAddr, index++, 0xf0 | (int) (v >> 32));
                        } else {
                            v -= (1L << 35);
                            if (v < (1L << 42)) {
                                p_bytePut(pageAddr, index++, 0xf8 | (int) (v >> 40));
                            } else {
                                v -= (1L << 42);
                                if (v < (1L << 49)) {
                                    p_bytePut(pageAddr, index++, 0xfc | (int) (v >> 48));
                                } else {
                                    v -= (1L << 49);
                                    if (v < (1L << 56)) {
                                        p_bytePut(pageAddr, index++, 0xfe);
                                    } else {
                                        v -= (1L << 56);
                                        p_bytePut(pageAddr, index++, 0xff);
                                        p_bytePut(pageAddr, index++, (byte) (v >> 56));
                                    }
                                    p_bytePut(pageAddr, index++, (byte) (v >> 48));
                                }
                                p_bytePut(pageAddr, index++, (byte) (v >> 40));
                            }
                            p_bytePut(pageAddr, index++, (byte) (v >> 32));
                        }
                        p_bytePut(pageAddr, index++, (byte) (v >> 24));
                    }
                    p_bytePut(pageAddr, index++, (byte) (v >> 16));
                }
                p_bytePut(pageAddr, index++, (byte) (v >> 8));
            }
        }
        p_bytePut(pageAddr, index++, (byte) v);
        return index;
    }

    static int p_ulongVarSize(long v) {
        return Utils.calcUnsignedVarLongLength(v);
    }

    /**
     * Returns page if it's an array, else copies to given array and returns that.
     */
    static byte[] p_copyIfNotArray(final long pageAddr, byte[] dstArray) {
        p_copy(pageAddr, 0, dstArray, 0, dstArray.length);
        return dstArray;
    }

    /**
     * Performs multiple array copies, correctly ordered to prevent clobbering. The copies
     * must not overlap, and start1 must be less than start2.
     */
    static void p_copies(final long pageAddr,
                         int start1, int dest1, int length1,
                         int start2, int dest2, int length2)
    {
        if (dest1 < start1) {
            p_copy(pageAddr, start1, pageAddr, dest1, length1);
            p_copy(pageAddr, start2, pageAddr, dest2, length2);
        } else {
            p_copy(pageAddr, start2, pageAddr, dest2, length2);
            p_copy(pageAddr, start1, pageAddr, dest1, length1);
        }
    }

    /**
     * Performs multiple array copies, correctly ordered to prevent clobbering. The copies
     * must not overlap, start1 must be less than start2, and start2 be less than start3.
     */
    static void p_copies(final long pageAddr,
                         int start1, int dest1, int length1,
                         int start2, int dest2, int length2,
                         int start3, int dest3, int length3)
    {
        if (dest1 < start1) {
            p_copy(pageAddr, start1, pageAddr, dest1, length1);
            p_copies(pageAddr, start2, dest2, length2, start3, dest3, length3);
        } else {
            p_copies(pageAddr, start2, dest2, length2, start3, dest3, length3);
            p_copy(pageAddr, start1, pageAddr, dest1, length1);
        }
    }

    static int p_compareKeysPageToArray(final long apageAddr, int aoff, int alen,
                                        byte[] b, int boff, int blen)
    {
        int minLen = Math.min(alen, blen);
        for (int i=0; i<minLen; i++) {
            byte ab = p_byteGet(apageAddr, aoff++);
            byte bb = b[boff + i];
            if (ab != bb) {
                return (ab & 0xff) - (bb & 0xff);
            }
        }
        return alen - blen;
    }

    static int p_compareKeysPageToPage(final long apageAddr, int aoff, int alen,
                                       final long bpageAddr, int boff, int blen)
    {
        int minLen = Math.min(alen, blen);
        for (int i=0; i<minLen; i++) {
            byte ab = p_byteGet(apageAddr, aoff++);
            byte bb = p_byteGet(bpageAddr, boff++);
            if (ab != bb) {
                return (ab & 0xff) - (bb & 0xff);
            }
        }
        return alen - blen;
    }

    static byte[] p_midKeyLowPage(final long lowPageAddr, int lowOff, int lowLen,
                                  byte[] high, int highOff)
    {
        for (int i=0; i<lowLen; i++) {
            byte lo = p_byteGet(lowPageAddr, lowOff + i);
            byte hi = high[highOff + i];
            if (lo != hi) {
                var mid = new byte[i + 1];
                p_copy(lowPageAddr, lowOff, mid, 0, i);
                mid[i] = (byte) (((lo & 0xff) + (hi & 0xff) + 1) >> 1);
                return mid;
            }
        }
        var mid = new byte[lowLen + 1];
        System.arraycopy(high, highOff, mid, 0, mid.length);
        return mid;
    }

    static byte[] p_midKeyHighPage(byte[] low, int lowOff, int lowLen,
                                   final long highPageAddr, int highOff)
    {
        for (int i=0; i<lowLen; i++) {
            byte lo = low[lowOff + i];
            byte hi = p_byteGet(highPageAddr, highOff + i);
            if (lo != hi) {
                var mid = new byte[i + 1];
                System.arraycopy(low, lowOff, mid, 0, i);
                mid[i] = (byte) (((lo & 0xff) + (hi & 0xff) + 1) >> 1);
                return mid;
            }
        }
        var mid = new byte[lowLen + 1];
        p_copy(highPageAddr, highOff, mid, 0, mid.length);
        return mid;
    }

    static byte[] p_midKeyLowHighPage(final long lowPageAddr, int lowOff, int lowLen,
                                      final long highPageAddr, int highOff)
    {
        for (int i=0; i<lowLen; i++) {
            byte lo = p_byteGet(lowPageAddr, lowOff + i);
            byte hi = p_byteGet(highPageAddr, highOff + i);
            if (lo != hi) {
                var mid = new byte[i + 1];
                p_copy(lowPageAddr, lowOff, mid, 0, i);
                mid[i] = (byte) (((lo & 0xff) + (hi & 0xff) + 1) >> 1);
                return mid;
            }
        }
        var mid = new byte[lowLen + 1];
        p_copy(highPageAddr, highOff, mid, 0, mid.length);
        return mid;
    }
}
