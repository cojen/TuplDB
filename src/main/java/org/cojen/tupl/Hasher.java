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

import java.nio.ByteOrder;

import org.cojen.tupl.io.UnsafeAccess;

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

    @SuppressWarnings("fallthrough")
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
            hash = (hash << 5) - hash ^ Utils.scramble(v);
            return hash;
        }

        int end = len & ~7;
        int i = 0;

        while (i < end) {
            hash = (hash << 5) - hash ^ Utils.scramble(Utils.decodeLongLE(b, i));
            i += 8;
        }

        if ((len & 7) != 0) {
            hash = (hash << 5) - hash ^ Utils.scramble(Utils.decodeLongLE(b, len - 8));
        }

        return hash;
    }

    /**
     * Same as default implementation except longs are read directly using Unsafe to avoid the
     * shifting transformation.
     */
    @SuppressWarnings("restriction")
    private static class UnsafeLE extends Hasher {
        private static final sun.misc.Unsafe UNSAFE;
        private static final long BYTE_ARRAY_OFFSET;

        static {
            try {
                UNSAFE = UnsafeAccess.tryObtain();
                BYTE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
            } catch (Throwable e) {
                throw new ExceptionInInitializerError();
            }
        }

        @Override
        @SuppressWarnings("fallthrough")
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
