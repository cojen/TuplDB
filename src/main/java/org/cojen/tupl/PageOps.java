/*
 *  Copyright 2015 Brian S O'Neill
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.cojen.tupl;

import static org.cojen.tupl.Utils.*;

/**
 * Low-level methods for operating against a database page.
 *
 * @author Brian S O'Neill
 */
final class PageOps {
    static byte[] p_empty() {
        return EMPTY_BYTES;
    }

    static byte[] p_alloc(int size) {
        return new byte[size];
    }

    static byte[][] p_allocArray(int size) {
        return new byte[size][];
    }

    static void p_delete(byte[] page) {
    }

    static byte[] p_clone(byte[] page) {
        return page.clone();
    }

    static int p_length(byte[] page) {
        return page.length;
    }

    static byte p_byteGet(byte[] page, int index) {
        return page[index];
    }

    static int p_ubyteGet(byte[] page, int index) {
        return page[index] & 0xff;
    }

    static void p_bytePut(byte[] page, int index, byte v) {
        page[index] = v;
    }

    static void p_bytePut(byte[] page, int index, int v) {
        page[index] = (byte) v;
    }

    static int p_ushortGetLE(byte[] page, int index) {
        return decodeUnsignedShortLE(page, index);
    }

    static void p_shortPutLE(byte[] page, int index, int v) {
        encodeShortLE(page, index, v);
    }

    static int p_intGetLE(byte[] page, int index) {
        return decodeIntLE(page, index);
    }

    static void p_intPutLE(byte[] page, int index, int v) {
        encodeIntLE(page, index, v);
    }

    static int p_uintGetVar(byte[] page, int index) {
        return decodeUnsignedVarInt(page, index);
    }

    static int p_uintPutVar(byte[] page, int index, int v) {
        return encodeUnsignedVarInt(page, index, v);
    }

    static int p_uintVarSize(int v) {
        return calcUnsignedVarIntLength(v);
    }

    static long p_uint48GetLE(byte[] page, int index) {
        return decodeUnsignedInt48LE(page, index);
    }

    static void p_int48PutLE(byte[] page, int index, long v) {
        encodeInt48LE(page, index, v);
    }

    static long p_longGetLE(byte[] page, int index) {
        return decodeLongLE(page, index);
    }

    static void p_longPutLE(byte[] page, int index, long v) {
        encodeLongLE(page, index, v);
    }

    static long p_longGetBE(byte[] page, int index) {
        return decodeLongBE(page, index);
    }

    static void p_longPutBE(byte[] page, int index, long v) {
        encodeLongBE(page, index, v);
    }

    static long p_ulongGetVar(byte[] page, IntegerRef ref) {
        return decodeUnsignedVarLong(page, ref);
    }

    static int p_ulongPutVar(byte[] page, int index, long v) {
        return encodeUnsignedVarLong(page, index, v);
    }

    static int p_ulongVarSize(long v) {
        return calcUnsignedVarLongLength(v);
    }

    static void p_clear(byte[] page) {
        java.util.Arrays.fill(page, (byte) 0);
    }

    static void p_clear(byte[] page, int fromIndex, int toIndex) {
        java.util.Arrays.fill(page, fromIndex, toIndex, (byte) 0);
    }

    static void p_copyFromArray(byte[] src, int srcStart, byte[] dstPage, int dstStart, int len) {
        System.arraycopy(src, srcStart, dstPage, dstStart, len);
    }

    static void p_copyToArray(byte[] srcPage, int srcStart, byte[] dst, int dstStart, int len) {
        System.arraycopy(srcPage, srcStart, dst, dstStart, len);
    }

    static void p_copy(byte[] srcPage, int srcStart, byte[] dstPage, int dstStart, int len) {
        System.arraycopy(srcPage, srcStart, dstPage, dstStart, len);
    }

    /**
     * Performs multiple array copies, correctly ordered to prevent clobbering. The copies
     * must not overlap, and start1 must be less than start2.
     */
    static void p_copies(byte[] page,
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
    static void p_copies(byte[] page,
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
}
