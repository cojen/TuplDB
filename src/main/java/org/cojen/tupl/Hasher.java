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

import java.lang.reflect.Field;
import java.nio.ByteOrder;

/**
 * Fast non-cryptographic hash function which computes a Wang/Jenkins hash over 8-byte
 * chunks. Chunks are combined by multiplying the cumulative hash by 31 and xor'ng the next
 * chunk hash. Technique is similar to SipHash, but computation is faster. It's also faster
 * than a simple x31 hash, except for keys less than 8 bytes in length. Small keys could use
 * x31, but it would produce more collisions.
 *
 * @author Brian S O'Neill
 */
class Hasher {
    private static final Hasher INSTANCE;

    static {
        Hasher instance = null;

        if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
            String arch = System.getProperty("os.arch");
            if (arch.equals("i386") || arch.equals("x86")
                || arch.equals("amd64") || arch.equals("x86_64"))
            {
                try {
                    instance = new UnsafeLE();
                } catch (Throwable e) {
                    // Not allowed.
                }
            }
        }

        INSTANCE = instance == null ? new Hasher() : instance;
    }

    public static long hash(long hash, byte[] b) {
        return INSTANCE.doHash(hash, b);
    }

    long doHash(long hash, byte[] b) {
        int len = b.length;
        hash ^= len;

        if (len < 8) {
            long v = 0;
            switch (len) {
            case 7:
                v = (v << 8) | (b[6] & 0xffL);
            case 6:
                v = (v << 8) | (b[5] & 0xffL);
            case 5:
                v = (v << 8) | (b[4] & 0xffL);
            case 4:
                v = (v << 8) | (b[3] & 0xffL);
            case 3:
                v = (v << 8) | (b[2] & 0xffL);
            case 2:
                v = (v << 8) | (b[1] & 0xffL);
            case 1:
                v = (v << 8) | (b[0] & 0xffL);
            }
            hash = (hash << 5) - hash + Utils.scramble(v);
            return hash;
        }

        int end = len & ~7;
        int i = 0;

        while (i < end) {
            hash = (hash << 5) - hash + Utils.scramble(Utils.decodeLongLE(b, i));
            i += 8;
        }

        if ((len & 7) != 0) {
            hash = (hash << 5) - hash + Utils.scramble(Utils.decodeLongLE(b, len - 8));
        }

        return hash;
    }

    static Unsafe getUnsafe() {
        if (INSTANCE instanceof UnsafeLE) {
            return ((UnsafeLE) INSTANCE).UNSAFE;
        }
        return null;
    }

    /**
     * Same as default implementation except longs are read directly using Unsafe to avoid the
     * shifting transformation.
     */
    private static class UnsafeLE extends Hasher {
        static final Unsafe UNSAFE;
        private static final long BYTE_ARRAY_OFFSET;

        static {
            try {
                Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
                theUnsafe.setAccessible(true);
                UNSAFE = (Unsafe) theUnsafe.get(null);
                BYTE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
            } catch (Throwable e) {
                throw new ExceptionInInitializerError();
            }
        }

        @Override
        long doHash(long hash, byte[] b) {
            int len = b.length;
            hash ^= len;

            if (len < 8) {
                long v = 0;
                switch (len) {
                case 7:
                    v = (v << 8) | (b[6] & 0xffL);
                case 6:
                    v = (v << 8) | (b[5] & 0xffL);
                case 5:
                    v = (v << 8) | (b[4] & 0xffL);
                case 4:
                    v = (v << 8) | (b[3] & 0xffL);
                case 3:
                    v = (v << 8) | (b[2] & 0xffL);
                case 2:
                    v = (v << 8) | (b[1] & 0xffL);
                case 1:
                    v = (v << 8) | (b[0] & 0xffL);
                }
                hash = ((hash << 5) - hash) ^ Utils.scramble(v);
                return hash;
            }

            int end = len & ~7;
            int i = 0;

            while (i < end) {
                hash = ((hash << 5) - hash) ^
                    Utils.scramble(UNSAFE.getLong(b, BYTE_ARRAY_OFFSET + i));
                i += 8;
            }

            if ((len & 7) != 0) {
                hash = ((hash << 5) - hash) ^
                    Utils.scramble(UNSAFE.getLong(b, BYTE_ARRAY_OFFSET + len - 8));
            }

            return hash;
        }
    }
}
