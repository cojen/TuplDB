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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;

import java.nio.ByteOrder;

import static org.cojen.tupl.core.DirectMemory.ALL;

/**
 * Implements low-level methods for operating against database pages using the FFM API.
 *
 * @author Brian S O'Neill
 * @see DirectPageOpsSelector
 */
class DirectPageOps {
    private static final MethodHandle OF_ADDRESS_UNSAFE;

    private static final VarHandle BYTE_H, CHAR_LE_H, INT_LE_H, LONG_LE_H, LONG_BE_H;

    private static final boolean CHECK_BOUNDS;
    private static final int CHECKED_PAGE_SIZE;

    static {
        try {
            OF_ADDRESS_UNSAFE = MethodHandles.lookup().findStatic
                (DirectPageOps.class, "ofAddressUnsafe",
                 MethodType.methodType(MemorySegment.class, long.class));
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }

        var CHAR_LE = ValueLayout.JAVA_CHAR_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
        var INT_LE = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
        var LONG_LE = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
        var LONG_BE = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);

        BYTE_H = adaptSegmentHandle(ValueLayout.JAVA_BYTE.varHandle());
        CHAR_LE_H = adaptSegmentHandle(CHAR_LE.varHandle());
        INT_LE_H = adaptSegmentHandle(INT_LE.varHandle());
        LONG_LE_H = adaptSegmentHandle(LONG_LE.varHandle());
        LONG_BE_H = adaptSegmentHandle(LONG_BE.varHandle());

        Integer checkedPageSize = Integer.getInteger
            (DirectPageOps.class.getName() + ".checkedPageSize");

        if (checkedPageSize == null) {
            CHECK_BOUNDS = false;
            CHECKED_PAGE_SIZE = 0;
        } else {
            CHECK_BOUNDS = true;
            CHECKED_PAGE_SIZE = checkedPageSize;
        }
    }

    private static VarHandle adaptSegmentHandle(VarHandle vh) {
        vh = MethodHandles.insertCoordinates(vh, 1, 0L);
        vh = MethodHandles.filterCoordinates(vh, 0, OF_ADDRESS_UNSAFE);
        //vh = vh.withInvokeExactBehavior();
        return vh;
    }

    private static MemorySegment ofAddressUnsafe(long address) {
        return MemorySegment.ofAddress(address).reinterpret(8);
    }

    static int kind() {
        return 0;
    }

    static byte p_byteGet(long pageAddr, int index) {
        if (CHECK_BOUNDS && Long.compareUnsigned(index, CHECKED_PAGE_SIZE) >= 0) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        return (byte) BYTE_H.get(pageAddr + index);
    }

    static void p_bytePut(long pageAddr, int index, byte v) {
        if (CHECK_BOUNDS && Long.compareUnsigned(index, CHECKED_PAGE_SIZE) >= 0) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        BYTE_H.set(pageAddr + index, v);
    }

    static int p_ushortGetLE(long pageAddr, int index) {
        if (CHECK_BOUNDS && (index < 0 || index + 2 > CHECKED_PAGE_SIZE)) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        return (char) CHAR_LE_H.get(pageAddr + index);
    }

    static void p_shortPutLE(long pageAddr, int index, int v) {
        if (CHECK_BOUNDS && (index < 0 || index + 2 > CHECKED_PAGE_SIZE)) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        CHAR_LE_H.set(pageAddr + index, (char) v);
    }

    static int p_intGetLE(long pageAddr, int index) {
        if (CHECK_BOUNDS && (index < 0 || index + 4 > CHECKED_PAGE_SIZE)) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        return (int) INT_LE_H.get(pageAddr + index);
    }

    static void p_intPutLE(long pageAddr, int index, int v) {
        if (CHECK_BOUNDS && (index < 0 || index + 4 > CHECKED_PAGE_SIZE)) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        INT_LE_H.set(pageAddr + index, v);
    }

    static long p_longGetLE(long pageAddr, int index) {
        if (CHECK_BOUNDS && (index < 0 || index + 8 > CHECKED_PAGE_SIZE)) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        return (long) LONG_LE_H.get(pageAddr + index);
    }

    static void p_longPutLE(long pageAddr, int index, long v) {
        if (CHECK_BOUNDS && (index < 0 || index + 8 > CHECKED_PAGE_SIZE)) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        LONG_LE_H.set(pageAddr + index, v);
    }

    static long p_longGetBE(long pageAddr, int index) {
        if (CHECK_BOUNDS && (index < 0 || index + 8 > CHECKED_PAGE_SIZE)) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        return (long) LONG_BE_H.get(pageAddr + index);
    }

    static void p_longPutBE(long pageAddr, int index, long v) {
        if (CHECK_BOUNDS && (index < 0 || index + 8 > CHECKED_PAGE_SIZE)) {
            throw new ArrayIndexOutOfBoundsException(index);
        }
        LONG_BE_H.set(pageAddr + index, v);
    }

    static void p_clear(long pageAddr, int fromIndex, int toIndex) {
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
            MemorySegment.ofAddress(pageAddr + fromIndex).reinterpret(len).fill((byte) 0);
        }
    }

    static void p_copy(byte[] src, int srcStart, long dstPageAddr, long dstStart, int len) {
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
        MemorySegment.copy(src, srcStart, ALL, ValueLayout.JAVA_BYTE, dstPageAddr + dstStart, len);
    }

    static void p_copy(long srcPageAddr, long srcStart, byte[] dst, int dstStart, int len) {
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
        MemorySegment.copy(ALL, ValueLayout.JAVA_BYTE, srcPageAddr + srcStart, dst, dstStart, len);
    }

    static void p_copy(long srcPageAddr, int srcStart, long dstPageAddr, long dstStart, long len) {
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
        MemorySegment.copy(ALL, srcPageAddr + srcStart, ALL, dstPageAddr + dstStart, len);
    }
}
