/*
*  xxHash - Fast Hash algorithm
*  Copyright (C) 2012-2016, Yann Collet
*
*  BSD 2-Clause License (http://www.opensource.org/licenses/bsd-license.php)
*
*  Redistribution and use in source and binary forms, with or without
*  modification, are permitted provided that the following conditions are
*  met:
*
*  * Redistributions of source code must retain the above copyright
*  notice, this list of conditions and the following disclaimer.
*  * Redistributions in binary form must reproduce the above
*  copyright notice, this list of conditions and the following disclaimer
*  in the documentation and/or other materials provided with the
*  distribution.
*
*  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
*  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
*  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
*  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
*  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
*  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
*  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
*  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
*  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
*  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
*  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*
*  You can contact the author at :
*  - xxHash homepage: http://www.xxhash.com
*  - xxHash source repository : https://github.com/Cyan4973/xxHash
*/

package org.cojen.tupl.core;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.nio.ByteOrder;

import static java.lang.Long.rotateLeft;

/**
 * Implements xxHash64. It could be a bit faster if using Unsafe access.
 *
 * @author Brian S O'Neill
 */
abstract class Hasher {
    private static final long PRIME_1 = -7046029288634856825L; // 11400714785074694791
    private static final long PRIME_2 = -4417276706812531889L; // 14029467366897019727
    private static final long PRIME_3 = 1609587929392839161L;
    private static final long PRIME_4 = -8796714831421723037L; // 9650029242287828579
    private static final long PRIME_5 = 2870177450012600261L;

    private static final VarHandle cIntArrayHandle;
    private static final VarHandle cLongArrayHandle;

    static {
        try {
            cIntArrayHandle = MethodHandles.byteArrayViewVarHandle
                (int[].class, ByteOrder.LITTLE_ENDIAN);
            cLongArrayHandle = MethodHandles.byteArrayViewVarHandle
                (long[].class, ByteOrder.LITTLE_ENDIAN);
        } catch (Throwable e) {
            throw new ExceptionInInitializerError();
        }
    }

    private Hasher() {
    }

    public static long hash(long seed, byte[] b) {
        return hash(seed, b, 0, b.length);
    }

    public static long hash(long seed, byte[] b, int off, int len) {
        int end = off + len;
        long hash;

        if (len < 32) {
            hash = seed + PRIME_5;
        } else {
            int limit = end - 32;
            long v1 = seed + PRIME_1 + PRIME_2;
            long v2 = seed + PRIME_2;
            long v3 = seed + 0;
            long v4 = seed - PRIME_1;
            do {
                v1 += ((long) cLongArrayHandle.get(b, off)) * PRIME_2;
                v1 = rotateLeft(v1, 31);
                v1 *= PRIME_1;
                off += 8;

                v2 += ((long) cLongArrayHandle.get(b, off)) * PRIME_2;
                v2 = rotateLeft(v2, 31);
                v2 *= PRIME_1;
                off += 8;

                v3 += ((long) cLongArrayHandle.get(b, off)) * PRIME_2;
                v3 = rotateLeft(v3, 31);
                v3 *= PRIME_1;
                off += 8;

                v4 += ((long) cLongArrayHandle.get(b, off)) * PRIME_2;
                v4 = rotateLeft(v4, 31);
                v4 *= PRIME_1;
                off += 8;
            } while (off <= limit);

            hash = rotateLeft(v1, 1) + rotateLeft(v2, 7) + rotateLeft(v3, 12) + rotateLeft(v4, 18);

            v1 *= PRIME_2; v1 = rotateLeft(v1, 31); v1 *= PRIME_1; hash ^= v1;
            hash = hash * PRIME_1 + PRIME_4;

            v2 *= PRIME_2; v2 = rotateLeft(v2, 31); v2 *= PRIME_1; hash ^= v2;
            hash = hash * PRIME_1 + PRIME_4;

            v3 *= PRIME_2; v3 = rotateLeft(v3, 31); v3 *= PRIME_1; hash ^= v3;
            hash = hash * PRIME_1 + PRIME_4;

            v4 *= PRIME_2; v4 = rotateLeft(v4, 31); v4 *= PRIME_1; hash ^= v4;
            hash = hash * PRIME_1 + PRIME_4;
        }

        hash += len;

        while (off <= end - 8) {
            long k1 = (long) cLongArrayHandle.get(b, off);
            k1 *= PRIME_2; k1 = rotateLeft(k1, 31); k1 *= PRIME_1; hash ^= k1;
            hash = rotateLeft(hash, 27) * PRIME_1 + PRIME_4;
            off += 8;
        }

        if (off <= end - 4) {
            hash ^= (((int) cIntArrayHandle.get(b, off)) & 0xffffffffL) * PRIME_1;
            hash = rotateLeft(hash, 23) * PRIME_2 + PRIME_3;
            off += 4;
        }

        while (off < end) {
            hash ^= (b[off] & 0xff) * PRIME_5;
            hash = rotateLeft(hash, 11) * PRIME_1;
            off++;
        }

        hash ^= hash >>> 33;
        hash *= PRIME_2;
        hash ^= hash >>> 29;
        hash *= PRIME_3;
        hash ^= hash >>> 32;

        return hash;
    }
}
