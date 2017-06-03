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

import java.util.Arrays;

import java.util.zip.CRC32;

import org.cojen.tupl.io.DirectAccess;
import org.cojen.tupl.io.MappedPageArray;
import org.cojen.tupl.io.UnsafeAccess;

/**
 * 
 *
 * @author Brian S O'Neill
 * @see PageOps
 */
@SuppressWarnings("restriction")
final class DirectPageOps {
    static final int NODE_OVERHEAD = 100 - 24; // 6 fewer fields

    private static final boolean CHECK_BOUNDS;
    private static final int CHECKED_PAGE_SIZE;

    private static final sun.misc.Unsafe UNSAFE = UnsafeAccess.obtain();
    private static final long BYTE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
    private static final long EMPTY_TREE_LEAF;
    private static final long CLOSED_TREE_PAGE;
    private static final long STUB_TREE_PAGE;

    static {
        Integer checkedPageSize = Integer.getInteger
            (DirectPageOps.class.getName() + ".checkedPageSize");

        if (checkedPageSize == null) {
            CHECK_BOUNDS = false;
            CHECKED_PAGE_SIZE = 0;
        } else {
            CHECK_BOUNDS = true;
            CHECKED_PAGE_SIZE = checkedPageSize;
        }

        EMPTY_TREE_LEAF = newEmptyTreeLeafPage();
        CLOSED_TREE_PAGE = newEmptyTreeLeafPage();
        STUB_TREE_PAGE = newEmptyTreePage(Node.TN_HEADER_SIZE + 8, Node.TYPE_TN_IN);
    }

    private static long newEmptyTreeLeafPage() {
        return newEmptyTreePage
            (Node.TN_HEADER_SIZE, Node.TYPE_TN_LEAF | Node.LOW_EXTREMITY | Node.HIGH_EXTREMITY);
    }

    private static long newEmptyTreePage(int pageSize, int type) {
        long empty = p_calloc(pageSize);

        p_bytePut(empty, 0, type);

        // Set fields such that binary search returns ~0 and availableBytes returns 0.

        // Note: Same as Node.clearEntries.
        p_shortPutLE(empty, 4,  Node.TN_HEADER_SIZE);     // leftSegTail
        p_shortPutLE(empty, 6,  pageSize - 1);            // rightSegTail
        p_shortPutLE(empty, 8,  Node.TN_HEADER_SIZE);     // searchVecStart
        p_shortPutLE(empty, 10, Node.TN_HEADER_SIZE - 2); // searchVecEnd

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

    static long p_stubTreePage() {
        return STUB_TREE_PAGE;
    }

    static long p_alloc(int size) {
        return UNSAFE.allocateMemory(size);
    }

    static long p_calloc(int size) {
        long ptr = p_alloc(size);
        UNSAFE.setMemory(ptr, size, (byte) 0);
        return ptr;
    }

    static long[] p_allocArray(int size) {
        return new long[size];
    }

    static void p_delete(final long page) {
        // Only delete pages that were allocated from the Unsafe class and aren't globals.
        if (page != CLOSED_TREE_PAGE && page != EMPTY_TREE_LEAF && !inArena(page)) {
            UNSAFE.freeMemory(page);
        }
    }

    static class Arena implements Comparable<Arena> {
        private final MappedPageArray mPageArray;
        private final long mStartPtr;
        private final long mEndPtr; // exclusive

        private long mNextPtr;

        Arena(int pageSize, long pageCount) throws IOException {
            mPageArray = MappedPageArray.open(pageSize, pageCount, null, null);
            mStartPtr = mPageArray.directPagePointer(0);
            mEndPtr = mStartPtr + (pageSize * pageCount);
            synchronized (this) {
                mNextPtr = mStartPtr;
            }
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
            Arena[] arenas = new Arena[existing.length + 1];
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
            Arena[] arenas = new Arena[existing.length - 1];
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

    static Object p_arenaAlloc(int pageSize, long pageCount) throws IOException {
        Arena arena = new Arena(pageSize, pageCount);
        registerArena(arena);
        return arena;
    }

    static void p_arenaDelete(Object arena) throws IOException {
        if (arena instanceof Arena) {
            Arena a = (Arena) arena;
            // Unregister before closing, in case new allocations are allowed in the recycled
            // memory range and then deleted. The delete method would erroneously think the page
            // is still in an arena and do nothing.
            unregisterArena(a);
            a.close();
        } else if (arena != null) {
            throw new IllegalArgumentException();
        }
    }

    static long p_calloc(Object arena, int size) {
        if (arena instanceof Arena) {
            final long page = ((Arena) arena).p_calloc(size);
            if (page != p_null()) {
                return page;
            }
        } else if (arena != null) {
            throw new IllegalArgumentException();
        }

        return p_calloc(size);
    }

    static long p_clone(final long page, int length) {
        long dst = p_alloc(length);
        UNSAFE.copyMemory(page, dst, length);
        return dst;
    }

    static long p_transfer(byte[] array) {
        int length = array.length;
        final long page = p_alloc(length);
        p_copyFromArray(array, 0, page, 0, length);
        return page;
    }

    static long p_transferTo(byte[] array, final long page) {
        int length = array.length;
        p_copyFromArray(array, 0, page, 0, length);
        return page;
    }

    static byte p_byteGet(final long page, int index) {
        if (CHECK_BOUNDS && Long.compareUnsigned(index, CHECKED_PAGE_SIZE) >= 0) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        return UNSAFE.getByte(page + index);
    }

    static int p_ubyteGet(final long page, int index) {
        return p_byteGet(page, index) & 0xff;
    }

    static void p_bytePut(final long page, int index, byte v) {
        if (CHECK_BOUNDS && Long.compareUnsigned(index, CHECKED_PAGE_SIZE) >= 0) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        UNSAFE.putByte(page + index, v);
    }

    static void p_bytePut(final long page, int index, int v) {
        p_bytePut(page, index, (byte) v);
    }

    static int p_ushortGetLE(final long page, int index) {
        if (CHECK_BOUNDS && (index < 0 || index + 2 > CHECKED_PAGE_SIZE)) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        return UNSAFE.getChar(page + index);
    }

    static void p_shortPutLE(final long page, int index, int v) {
        if (CHECK_BOUNDS && (index < 0 || index + 2 > CHECKED_PAGE_SIZE)) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        UNSAFE.putShort(page + index, (short) v);
    }

    static int p_intGetLE(final long page, int index) {
        if (CHECK_BOUNDS && (index < 0 || index + 4 > CHECKED_PAGE_SIZE)) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        return UNSAFE.getInt(page + index);
    }

    static void p_intPutLE(final long page, int index, int v) {
        if (CHECK_BOUNDS && (index < 0 || index + 4 > CHECKED_PAGE_SIZE)) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        UNSAFE.putInt(page + index, v);
    }

    static int p_uintGetVar(final long page, int index) {
        int v = p_byteGet(page, index);
        if (v >= 0) {
            return v;
        }
        switch ((v >> 4) & 0x07) {
        case 0x00: case 0x01: case 0x02: case 0x03:
            return (1 << 7)
                + (((v & 0x3f) << 8)
                   | p_ubyteGet(page, index + 1));
        case 0x04: case 0x05:
            return ((1 << 14) + (1 << 7))
                + (((v & 0x1f) << 16)
                   | (p_ubyteGet(page, ++index) << 8)
                   | p_ubyteGet(page, index + 1));
        case 0x06:
            return ((1 << 21) + (1 << 14) + (1 << 7))
                + (((v & 0x0f) << 24)
                   | (p_ubyteGet(page, ++index) << 16)
                   | (p_ubyteGet(page, ++index) << 8)
                   | p_ubyteGet(page, index + 1));
        default:
            return ((1 << 28) + (1 << 21) + (1 << 14) + (1 << 7)) 
                + ((p_byteGet(page, ++index) << 24)
                   | (p_ubyteGet(page, ++index) << 16)
                   | (p_ubyteGet(page, ++index) << 8)
                   | p_ubyteGet(page, index + 1));
        }
    }

    static int p_uintPutVar(final long page, int index, int v) {
        if (v < (1 << 7)) {
            if (v < 0) {
                v -= (1 << 28) + (1 << 21) + (1 << 14) + (1 << 7);
                p_bytePut(page, index++, 0xff);
                p_bytePut(page, index++, v >> 24);
                p_bytePut(page, index++, v >> 16);
                p_bytePut(page, index++, v >> 8);
            }
        } else {
            v -= (1 << 7);
            if (v < (1 << 14)) {
                p_bytePut(page, index++, 0x80 | (v >> 8));
            } else {
                v -= (1 << 14);
                if (v < (1 << 21)) {
                    p_bytePut(page, index++, 0xc0 | (v >> 16));
                } else {
                    v -= (1 << 21);
                    if (v < (1 << 28)) {
                        p_bytePut(page, index++, 0xe0 | (v >> 24));
                    } else {
                        v -= (1 << 28);
                        p_bytePut(page, index++, 0xf0);
                        p_bytePut(page, index++, v >> 24);
                    }
                    p_bytePut(page, index++, v >> 16);
                }
                p_bytePut(page, index++, v >> 8);
            }
        }
        p_bytePut(page, index++, v);
        return index;
    }

    static int p_uintVarSize(int v) {
        return Utils.calcUnsignedVarIntLength(v);
    }

    static long p_uint48GetLE(final long page, int index) {
        return p_intGetLE(page, index) & 0xffff_ffffL
            | (((long) p_ushortGetLE(page, index + 4)) << 32);
    }

    static void p_int48PutLE(final long page, int index, long v) {
        p_intPutLE(page, index, (int) v);
        p_shortPutLE(page, index + 4, (short) (v >> 32));
    }

    static long p_longGetLE(final long page, int index) {
        if (CHECK_BOUNDS && (index < 0 || index + 8 > CHECKED_PAGE_SIZE)) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        return UNSAFE.getLong(page + index);
    }

    static void p_longPutLE(final long page, int index, long v) {
        if (CHECK_BOUNDS && (index < 0 || index + 8 > CHECKED_PAGE_SIZE)) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        UNSAFE.putLong(page + index, v);
    }

    static long p_longGetBE(final long page, int index) {
        return Long.reverseBytes(p_longGetLE(page, index));
    }

    static void p_longPutBE(final long page, int index, long v) {
        p_longPutLE(page, index, Long.reverseBytes(v));
    }

    static long p_ulongGetVar(final long page, IntegerRef ref) {
        int offset = ref.get();
        int val = p_byteGet(page, offset++);
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
                    + ((((long) p_byteGet(page, offset++)) << 56)
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
                p_bytePut(page, index++, 0xff);
                p_bytePut(page, index++, (byte) (v >> 56));
                p_bytePut(page, index++, (byte) (v >> 48));
                p_bytePut(page, index++, (byte) (v >> 40));
                p_bytePut(page, index++, (byte) (v >> 32));
                p_bytePut(page, index++, (byte) (v >> 24));
                p_bytePut(page, index++, (byte) (v >> 16));
                p_bytePut(page, index++, (byte) (v >> 8));
            }
        } else {
            v -= (1L << 7);
            if (v < (1L << 14)) {
                p_bytePut(page, index++, 0x80 | (int) (v >> 8));
            } else {
                v -= (1L << 14);
                if (v < (1L << 21)) {
                    p_bytePut(page, index++, 0xc0 | (int) (v >> 16));
                } else {
                    v -= (1L << 21);
                    if (v < (1L << 28)) {
                        p_bytePut(page, index++, 0xe0 | (int) (v >> 24));
                    } else {
                        v -= (1L << 28);
                        if (v < (1L << 35)) {
                            p_bytePut(page, index++, 0xf0 | (int) (v >> 32));
                        } else {
                            v -= (1L << 35);
                            if (v < (1L << 42)) {
                                p_bytePut(page, index++, 0xf8 | (int) (v >> 40));
                            } else {
                                v -= (1L << 42);
                                if (v < (1L << 49)) {
                                    p_bytePut(page, index++, 0xfc | (int) (v >> 48));
                                } else {
                                    v -= (1L << 49);
                                    if (v < (1L << 56)) {
                                        p_bytePut(page, index++, 0xfe | (int) (v >> 56));
                                    } else {
                                        v -= (1L << 56);
                                        p_bytePut(page, index++, 0xff);
                                        p_bytePut(page, index++, (byte) (v >> 56));
                                    }
                                    p_bytePut(page, index++, (byte) (v >> 48));
                                }
                                p_bytePut(page, index++, (byte) (v >> 40));
                            }
                            p_bytePut(page, index++, (byte) (v >> 32));
                        }
                        p_bytePut(page, index++, (byte) (v >> 24));
                    }
                    p_bytePut(page, index++, (byte) (v >> 16));
                }
                p_bytePut(page, index++, (byte) (v >> 8));
            }
        }
        p_bytePut(page, index++, (byte) v);
        return index;
    }

    static int p_ulongVarSize(long v) {
        return Utils.calcUnsignedVarLongLength(v);
    }

    static void p_clear(final long page, int fromIndex, int toIndex) {
        int len = toIndex - fromIndex;
        if (len > 0) {
            if (CHECK_BOUNDS) {
                if (Long.compareUnsigned(fromIndex, CHECKED_PAGE_SIZE) >= 0) {
                    throw new ArrayIndexOutOfBoundsException(fromIndex);
                }
                if (Long.compareUnsigned(toIndex, CHECKED_PAGE_SIZE) > 0) {
                    throw new ArrayIndexOutOfBoundsException(toIndex);
                }
            }
            UNSAFE.setMemory(page + fromIndex, len, (byte) 0);
        }
    }

    static byte[] p_copyIfNotArray(final long page, byte[] dstArray) {
        p_copyToArray(page, 0, dstArray, 0, dstArray.length);
        return dstArray;
    }

    static void p_copyFromArray(byte[] src, int srcStart,
                                final long dstPage, int dstStart, int len)
    {
        if (CHECK_BOUNDS) {
            if (len < 0) {
                throw new IndexOutOfBoundsException("len: " + len);
            }
            if (srcStart < 0 || srcStart + len > src.length) {
                throw new IndexOutOfBoundsException("src: " + srcStart + ", " + len);
            }
            if (dstStart < 0 || dstStart + len > CHECKED_PAGE_SIZE) {
                throw new IndexOutOfBoundsException("dst: " + dstStart + ", " + len);
            }
        }
        UNSAFE.copyMemory(src, BYTE_ARRAY_OFFSET + srcStart, null, dstPage + dstStart, len);
    }

    static void p_copyToArray(final long srcPage, int srcStart,
                              byte[] dst, int dstStart, int len)
    {
        if (CHECK_BOUNDS) {
            if (len < 0) {
                throw new IndexOutOfBoundsException("len: " + len);
            }
            if (srcStart < 0 || srcStart + len > CHECKED_PAGE_SIZE) {
                throw new IndexOutOfBoundsException("src: " + srcStart + ", " + len);
            }
            if (dstStart < 0 || dstStart + len > dst.length) {
                throw new IndexOutOfBoundsException("dst: " + dstStart + ", " + len);
            }
        }
        UNSAFE.copyMemory(null, srcPage + srcStart, dst, BYTE_ARRAY_OFFSET + dstStart, len);
    }

    static void p_copyFromBB(ByteBuffer src, final long dstPage, int dstStart, int len) {
        src.limit(src.position() + len);
        DirectAccess.ref(dstPage + dstStart, len).put(src);
        src.limit(src.capacity());
    }

    static void p_copyToBB(final long srcPage, int srcStart, ByteBuffer dst, int len) {
        dst.put(DirectAccess.ref(srcPage + srcStart, len));
    }

    static void p_copy(final long srcPage, int srcStart, long dstPage, int dstStart, int len) {
        if (CHECK_BOUNDS) {
            if (len < 0) {
                throw new IndexOutOfBoundsException("len: " + len);
            }
            if (srcStart < 0 || srcStart + len > CHECKED_PAGE_SIZE) {
                throw new IndexOutOfBoundsException("src: " + srcStart + ", " + len);
            }
            if (dstStart < 0 || dstStart + len > CHECKED_PAGE_SIZE) {
                throw new IndexOutOfBoundsException("dst: " + dstStart + ", " + len);
            }
        }
        UNSAFE.copyMemory(srcPage + srcStart, dstPage + dstStart, len);
    }

    static void p_copies(final long page,
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

    static void p_copies(final long page,
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

    static int p_compareKeysPageToArray(final long apage, int aoff, int alen,
                                        byte[] b, int boff, int blen)
    {
        int minLen = Math.min(alen, blen);
        for (int i=0; i<minLen; i++) {
            byte ab = p_byteGet(apage, aoff++);
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
            byte ab = p_byteGet(apage, aoff++);
            byte bb = p_byteGet(bpage, boff++);
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
            byte lo = p_byteGet(lowPage, lowOff + i);
            byte hi = high[highOff + i];
            if (lo != hi) {
                byte[] mid = new byte[i + 1];
                p_copyToArray(lowPage, lowOff, mid, 0, i);
                mid[i] = (byte) (((lo & 0xff) + (hi & 0xff) + 1) >> 1);
                return mid;
            }
        }
        byte[] mid = new byte[lowLen + 1];
        System.arraycopy(high, highOff, mid, 0, mid.length);
        return mid;
    }

    static byte[] p_midKeyHighPage(byte[] low, int lowOff, int lowLen,
                                   final long highPage, int highOff)
    {
        for (int i=0; i<lowLen; i++) {
            byte lo = low[lowOff + i];
            byte hi = p_byteGet(highPage, highOff + i);
            if (lo != hi) {
                byte[] mid = new byte[i + 1];
                System.arraycopy(low, lowOff, mid, 0, i);
                mid[i] = (byte) (((lo & 0xff) + (hi & 0xff) + 1) >> 1);
                return mid;
            }
        }
        byte[] mid = new byte[lowLen + 1];
        p_copyToArray(highPage, highOff, mid, 0, mid.length);
        return mid;
    }

    static byte[] p_midKeyLowHighPage(final long lowPage, int lowOff, int lowLen,
                                      final long highPage, int highOff)
    {
        for (int i=0; i<lowLen; i++) {
            byte lo = p_byteGet(lowPage, lowOff + i);
            byte hi = p_byteGet(highPage, highOff + i);
            if (lo != hi) {
                byte[] mid = new byte[i + 1];
                p_copyToArray(lowPage, lowOff, mid, 0, i);
                mid[i] = (byte) (((lo & 0xff) + (hi & 0xff) + 1) >> 1);
                return mid;
            }
        }
        byte[] mid = new byte[lowLen + 1];
        p_copyToArray(highPage, highOff, mid, 0, mid.length);
        return mid;
    }

    static int p_crc32(long srcPage, int srcStart, int len) {
        CRC32 crc = new CRC32();
        crc.update(DirectAccess.ref(srcPage + srcStart, len));
        return (int) crc.getValue();
    }
}
