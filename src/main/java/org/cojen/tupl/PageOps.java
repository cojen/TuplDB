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

import java.io.IOException;

import java.nio.ByteBuffer;

import java.security.GeneralSecurityException;

import java.util.zip.CRC32;

import javax.crypto.Cipher;

import static org.cojen.tupl.Utils.*;

/**
 * Low-level methods for operating against a database page.
 *
 * @author Brian S O'Neill
 */
final class PageOps {
    static /*P*/ byte[] p_null() {
        return null;
    }

    static /*P*/ byte[] p_empty() {
        return EMPTY_BYTES;
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

    static /*P*/ byte[] p_clone(/*P*/ byte[] page) {
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

    static int p_length(/*P*/ byte[] page) {
        return page.length;
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

    static void p_clear(/*P*/ byte[] page) {
        java.util.Arrays.fill(page, (byte) 0);
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
        return compareKeys(apage, aoff, alen, b, boff, blen);
    }

    static int p_compareKeysPageToPage(/*P*/ byte[] apage, int aoff, int alen,
                                       /*P*/ byte[] bpage, int boff, int blen)
    {
        return compareKeys(apage, aoff, alen, bpage, boff, blen);
    }

    static byte[] p_midKeyLowPage(/*P*/ byte[] lowPage, int lowOff, int lowLen,
                                  /*P*/ byte[] high, int highOff, int highLen)
    {
        return midKey(lowPage, lowOff, lowLen, high, highOff, highLen);
    }

    static byte[] p_midKeyHighPage(byte[] low, int lowOff, int lowLen,
                                   /*P*/ byte[] highPage, int highOff, int highLen)
    {
        return midKey(low, lowOff, lowLen, highPage, highOff, highLen);
    }

    static byte[] p_midKeyLowHighPage(/*P*/ byte[] lowPage, int lowOff, int lowLen,
                                      /*P*/ byte[] highPage, int highOff, int highLen)
    {
        return midKey(lowPage, lowOff, lowLen, highPage, highOff, highLen);
    }

    static int p_crc32(/*P*/ byte[] srcPage, int srcStart, int len) {
        CRC32 crc = new CRC32();
        crc.update(srcPage, srcStart, len);
        return (int) crc.getValue();
    }

    static int p_cipherDoFinal(Cipher cipher,
                               /*P*/ byte[] srcPage, int srcStart, int srcLen,
                               /*P*/ byte[] dstPage, int dstStart)
        throws GeneralSecurityException
    {
        return cipher.doFinal(srcPage, srcStart, srcLen, dstPage, dstStart);
    }

    /**
     * Not very low-level, but this is much simpler.
     */
    static void p_undoPush(UndoLog undo, long indexId, byte op,
                           /*P*/ byte[] payload, int off, int len)
        throws IOException
    {
        undo.push(indexId, op, payload, off, len);
    }
}
