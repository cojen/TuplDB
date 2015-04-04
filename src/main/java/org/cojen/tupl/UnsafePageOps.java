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

import java.io.InputStream;
import java.io.IOException;

import java.nio.ByteBuffer;

import java.security.GeneralSecurityException;

import javax.crypto.Cipher;

/**
 * 
 *
 * @author Brian S O'Neill
 * @see PageOps
 */
final class UnsafePageOps {
    static long p_null() {
        return 0;
    }

    static long p_empty() {
        // FIXME
        throw null;
    }

    static long p_alloc(int size) {
        // FIXME
        throw null;
    }

    static long[] p_allocArray(int size) {
        return new long[size];
    }

    static void p_delete(long page) {
        // FIXME
        throw null;
    }

    static long p_clone(long page) {
        // FIXME
        throw null;
    }

    static long p_transfer(byte[] array) {
        int length = array.length;
        long page = p_alloc(length);
        p_copyFromArray(array, 0, page, 0, length);
        return page;
    }

    static int p_length(long page) {
        // FIXME
        throw null;
    }

    static byte p_byteGet(long page, int index) {
        // FIXME
        throw null;
    }

    static int p_ubyteGet(long page, int index) {
        // FIXME
        throw null;
    }

    static void p_bytePut(long page, int index, byte v) {
        // FIXME
        throw null;
    }

    static void p_bytePut(long page, int index, int v) {
        // FIXME
        throw null;
    }

    static int p_ushortGetLE(long page, int index) {
        // FIXME
        throw null;
    }

    static void p_shortPutLE(long page, int index, int v) {
        // FIXME
        throw null;
    }

    static int p_intGetLE(long page, int index) {
        // FIXME
        throw null;
    }

    static void p_intPutLE(long page, int index, int v) {
        // FIXME
        throw null;
    }

    static int p_uintGetVar(long page, int index) {
        // FIXME
        throw null;
    }

    static int p_uintPutVar(long page, int index, int v) {
        // FIXME
        throw null;
    }

    static int p_uintVarSize(int v) {
        // FIXME
        throw null;
    }

    static long p_uint48GetLE(long page, int index) {
        // FIXME
        throw null;
    }

    static void p_int48PutLE(long page, int index, long v) {
        // FIXME
        throw null;
    }

    static long p_longGetLE(long page, int index) {
        // FIXME
        throw null;
    }

    static void p_longPutLE(long page, int index, long v) {
        // FIXME
        throw null;
    }

    static long p_longGetBE(long page, int index) {
        // FIXME
        throw null;
    }

    static void p_longPutBE(long page, int index, long v) {
        // FIXME
        throw null;
    }

    static long p_ulongGetVar(long page, IntegerRef ref) {
        // FIXME
        throw null;
    }

    static int p_ulongPutVar(long page, int index, long v) {
        // FIXME
        throw null;
    }

    static int p_ulongVarSize(long v) {
        // FIXME
        throw null;
    }

    static void p_clear(long page) {
        // FIXME
        throw null;
    }

    static void p_clear(long page, int fromIndex, int toIndex) {
        // FIXME
        throw null;
    }

    static void p_copyFromArray(byte[] src, int srcStart, long dstPage, int dstStart, int len) {
        // FIXME
        throw null;
    }

    static void p_copyToArray(long srcPage, int srcStart, byte[] dst, int dstStart, int len) {
        // FIXME
        throw null;
    }

    static void p_copyFromBB(ByteBuffer src, long dstPage, int dstStart, int len) {
        // FIXME
        throw null;
    }

    static void p_copyToBB(long srcPage, int srcStart, ByteBuffer dst, int len) {
        // FIXME
        throw null;
    }

    static void p_copy(long srcPage, int srcStart, long dstPage, int dstStart, int len) {
        // FIXME
        throw null;
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
        // FIXME
        throw null;
    }

    static int p_compareKeysPageToPage(long apage, int aoff, int alen,
                                       long bpage, int boff, int blen)
    {
        // FIXME
        throw null;
    }

    static byte[] p_midKeyLowPage(long lowPage, int lowOff, int lowLen,
                                  byte[] high, int highOff, int highLen)
    {
        // FIXME
        throw null;
    }

    static byte[] p_midKeyHighPage(byte[] low, int lowOff, int lowLen,
                                   long highPage, int highOff, int highLen)
    {
        // FIXME
        throw null;
    }

    static byte[] p_midKeyLowHighPage(long lowPage, int lowOff, int lowLen,
                                      long highPage, int highOff, int highLen)
    {
        // FIXME
        throw null;
    }

    static int p_crc32(long srcPage, int srcStart, int len) {
        // FIXME
        throw null;
    }

    static int p_cipherDoFinal(Cipher cipher,
                               long srcPage, int srcStart, int srcLen,
                               long dstPage, int dstStart)
        throws GeneralSecurityException
    {
        // FIXME
        throw null;
    }

    static void p_readFully(InputStream in, long page) throws IOException {
        // FIXME
        throw null;
    }

    static void p_readFully(InputStream in, long page, int off, int len) throws IOException {
        // FIXME
        throw null;
    }
}
