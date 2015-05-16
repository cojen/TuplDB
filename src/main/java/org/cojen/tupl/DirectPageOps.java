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

import sun.misc.Unsafe;

import java.io.IOException;

import java.lang.reflect.Method;

import java.nio.ByteBuffer;

import java.security.GeneralSecurityException;

import java.util.zip.CRC32;

import javax.crypto.Cipher;

import org.cojen.tupl.io.DirectAccess;

/**
 * 
 *
 * @author Brian S O'Neill
 * @see PageOps
 */
final class DirectPageOps {
    private static final Unsafe UNSAFE = Hasher.getUnsafe();
    private static final long BYTE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
    private static final long EMPTY = p_alloc(0);

    private static final Method CRC_BUFFER_UPDATE_METHOD;

    static {
        Method m;
        try {
            // Java 8 feature.
            m = CRC32.class.getMethod("update", ByteBuffer.class);
        } catch (Exception e) {
            m = null;
        }
        CRC_BUFFER_UPDATE_METHOD = m;
    }

    static long p_null() {
        return 4;
    }

    static long p_empty() {
        return EMPTY;
    }

    static long p_alloc(int size) {
        long ptr = UNSAFE.allocateMemory(4 + size);
        UNSAFE.putInt(ptr, size);
        return ptr + 4;
    }

    static long p_calloc(int size) {
        long ptr = p_alloc(size);
        UNSAFE.setMemory(ptr, size, (byte) 0);
        return ptr;
    }

    static long[] p_allocArray(int size) {
        return new long[size];
    }

    static void p_delete(long page) {
        if (page != EMPTY) {
            UNSAFE.freeMemory(page - 4);
        }
    }

    static long p_clone(long page) {
        int length = p_length(page);
        long dst = p_alloc(length);
        UNSAFE.copyMemory(page, dst, length);
        return dst;
    }

    static long p_transfer(byte[] array) {
        int length = array.length;
        long page = p_alloc(length);
        p_copyFromArray(array, 0, page, 0, length);
        return page;
    }

    static long p_transferTo(byte[] array, long page) {
        int length = array.length;
        p_copyFromArray(array, 0, page, 0, length);
        return page;
    }

    static int p_length(long page) {
        return UNSAFE.getInt(page - 4);
    }

    static byte p_byteGet(long page, int index) {
        return UNSAFE.getByte(page + index);
    }

    static int p_ubyteGet(long page, int index) {
        return UNSAFE.getByte(page + index) & 0xff;
    }

    static void p_bytePut(long page, int index, byte v) {
        UNSAFE.putByte(page + index, v);
    }

    static void p_bytePut(long page, int index, int v) {
        UNSAFE.putByte(page + index, (byte) v);
    }

    static int p_ushortGetLE(long page, int index) {
        return UNSAFE.getChar(page + index);
    }

    static void p_shortPutLE(long page, int index, int v) {
        UNSAFE.putShort(page + index, (short) v);
    }

    static int p_intGetLE(long page, int index) {
        return UNSAFE.getInt(page + index);
    }

    static void p_intPutLE(long page, int index, int v) {
        UNSAFE.putInt(page + index, v);
    }

    static int p_uintGetVar(long page, int index) {
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

    static int p_uintPutVar(long page, int index, int v) {
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

    static long p_uint48GetLE(long page, int index) {
        return UNSAFE.getInt(page += index) & 0xffff_ffffL
            | (((long) UNSAFE.getChar(page + 4)) << 32);
    }

    static void p_int48PutLE(long page, int index, long v) {
        UNSAFE.putInt(page += index, (int) v);
        UNSAFE.putShort(page + 4, (short) (v >> 32));
    }

    static long p_longGetLE(long page, int index) {
        return UNSAFE.getLong(page + index);
    }

    static void p_longPutLE(long page, int index, long v) {
        UNSAFE.putLong(page + index, v);
    }

    static long p_longGetBE(long page, int index) {
        return Long.reverseBytes(UNSAFE.getLong(page + index));
    }

    static void p_longPutBE(long page, int index, long v) {
        UNSAFE.putLong(page + index, Long.reverseBytes(v));
    }

    static long p_ulongGetVar(long page, IntegerRef ref) {
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

    static int p_ulongPutVar(long page, int index, long v) {
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

    static void p_clear(long page) {
        UNSAFE.setMemory(page, p_length(page), (byte) 0);
    }

    static void p_clear(long page, int fromIndex, int toIndex) {
        UNSAFE.setMemory(page + fromIndex, toIndex - fromIndex, (byte) 0);
    }

    static byte[] p_copyIfNotArray(long page, byte[] dstArray) {
        p_copyToArray(page, 0, dstArray, 0, dstArray.length);
        return dstArray;
    }

    static void p_copyFromArray(byte[] src, int srcStart, long dstPage, int dstStart, int len) {
        UNSAFE.copyMemory(src, BYTE_ARRAY_OFFSET + srcStart, null, dstPage + dstStart, len);
    }

    static void p_copyToArray(long srcPage, int srcStart, byte[] dst, int dstStart, int len) {
        UNSAFE.copyMemory(null, srcPage + srcStart, dst, BYTE_ARRAY_OFFSET + dstStart, len);
    }

    static void p_copyFromBB(ByteBuffer src, long dstPage, int dstStart, int len) {
        ByteBuffer dst = DirectAccess.ref(dstPage + dstStart, len);
        try {
            src.limit(src.position() + len);
            dst.put(src);
            src.limit(src.capacity());
        } finally {
            DirectAccess.unref(dst);
        }
    }

    static void p_copyToBB(long srcPage, int srcStart, ByteBuffer dst, int len) {
        ByteBuffer src = DirectAccess.ref(srcPage + srcStart, len);
        try {
            dst.put(src);
        } finally {
            DirectAccess.unref(src);
        }
    }

    static void p_copy(long srcPage, int srcStart, long dstPage, int dstStart, int len) {
        UNSAFE.copyMemory(srcPage + srcStart, dstPage + dstStart, len);
    }

    static void p_copies(long page,
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

    static void p_copies(long page,
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

    static int p_compareKeysPageToArray(long apage, int aoff, int alen,
                                        byte[] b, int boff, int blen)
    {
        apage += aoff;
        int minLen = Math.min(alen, blen);
        for (int i=0; i<minLen; i++) {
            byte ab = UNSAFE.getByte(apage++);
            byte bb = b[boff + i];
            if (ab != bb) {
                return (ab & 0xff) - (bb & 0xff);
            }
        }
        return alen - blen;
    }

    static int p_compareKeysPageToPage(long apage, int aoff, int alen,
                                       long bpage, int boff, int blen)
    {
        apage += aoff;
        bpage += boff;
        int minLen = Math.min(alen, blen);
        for (int i=0; i<minLen; i++) {
            byte ab = UNSAFE.getByte(apage++);
            byte bb = UNSAFE.getByte(bpage++);
            if (ab != bb) {
                return (ab & 0xff) - (bb & 0xff);
            }
        }
        return alen - blen;
    }

    static byte[] p_midKeyLowPage(long lowPage, int lowOff, int lowLen,
                                  byte[] high, int highOff, int highLen)
    {
        lowPage += lowOff;
        for (int i=0; i<lowLen; i++) {
            byte lo = UNSAFE.getByte(lowPage + i);
            byte hi = high[highOff + i];
            if (lo != hi) {
                byte[] mid = new byte[i + 1];
                p_copyToArray(lowPage, 0, mid, 0, i);
                mid[i] = (byte) (((lo & 0xff) + (hi & 0xff) + 1) >> 1);
                return mid;
            }
        }
        byte[] mid = new byte[lowLen + 1];
        System.arraycopy(high, highOff, mid, 0, mid.length);
        return mid;
    }

    static byte[] p_midKeyHighPage(byte[] low, int lowOff, int lowLen,
                                   long highPage, int highOff, int highLen)
    {
        highPage += highOff;
        for (int i=0; i<lowLen; i++) {
            byte lo = low[lowOff + i];
            byte hi = UNSAFE.getByte(highPage + i);
            if (lo != hi) {
                byte[] mid = new byte[i + 1];
                System.arraycopy(low, lowOff, mid, 0, i);
                mid[i] = (byte) (((lo & 0xff) + (hi & 0xff) + 1) >> 1);
                return mid;
            }
        }
        byte[] mid = new byte[lowLen + 1];
        p_copyToArray(highPage, 0, mid, 0, mid.length);
        return mid;
    }

    static byte[] p_midKeyLowHighPage(long lowPage, int lowOff, int lowLen,
                                      long highPage, int highOff, int highLen)
    {
        lowPage += lowOff;
        highPage += highOff;
        for (int i=0; i<lowLen; i++) {
            byte lo = UNSAFE.getByte(lowPage + i);
            byte hi = UNSAFE.getByte(highPage + i);
            if (lo != hi) {
                byte[] mid = new byte[i + 1];
                p_copyToArray(lowPage, 0, mid, 0, i);
                mid[i] = (byte) (((lo & 0xff) + (hi & 0xff) + 1) >> 1);
                return mid;
            }
        }
        byte[] mid = new byte[lowLen + 1];
        p_copyToArray(highPage, 0, mid, 0, mid.length);
        return mid;
    }

    static int p_crc32(long srcPage, int srcStart, int len) {
        CRC32 crc = new CRC32();

        if (CRC_BUFFER_UPDATE_METHOD != null) {
            ByteBuffer bb = DirectAccess.ref(srcPage + srcStart, len);
            try {
                CRC_BUFFER_UPDATE_METHOD.invoke(crc, bb);
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                DirectAccess.unref(bb);
            }
        } else {
            // Not the most efficient approach, but CRCs are only used by header pages.
            byte[] temp = new byte[len];
            p_copyToArray(srcPage, srcStart, temp, 0, len);
            crc.update(temp);
        }

        return (int) crc.getValue();
    }

    static int p_cipherDoFinal(Cipher cipher,
                               long srcPage, int srcStart, int srcLen,
                               long dstPage, int dstStart)
        throws GeneralSecurityException
    {
        ByteBuffer src = DirectAccess.ref(srcPage + srcStart, srcLen);
        try {
            ByteBuffer dst = DirectAccess.ref2(dstPage + dstStart, srcLen);
            try {
                return cipher.doFinal(src, dst);
            } finally {
                DirectAccess.unref(dst);
            }
        } finally {
            DirectAccess.unref(src);
        }
    }

    static void p_undoPush(UndoLog undo, long indexId, byte op,
                           long payload, int off, int len)
        throws IOException
    {
        byte[] temp = new byte[len];
        p_copyToArray(payload, off, temp, 0, len);
        undo.push(indexId, op, temp, 0, len);
    }
}
