/*
 *  Copyright 2011-2012 Brian S O'Neill
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

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;

import java.math.BigInteger;

import java.util.concurrent.TimeUnit;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class Utils {
    static final byte[] EMPTY_BYTES = new byte[0];

    static long toNanos(long timeout, TimeUnit unit) {
        return timeout < 0 ? -1 :
            (timeout == 0 ? 0 : (((timeout = unit.toNanos(timeout)) < 0) ? 0 : timeout));
    }

    static int roundUpPower2(int i) {
        // Hacker's Delight figure 3-3.
        i--;
        i |= i >> 1;
        i |= i >> 2;
        i |= i >> 4;
        i |= i >> 8;
        return (i | (i >> 16)) + 1;
    }

    static BigInteger valueOfUnsigned(long v) {
        byte[] temp = new byte[9];
        writeLongBE(temp, 1, v);
        return new BigInteger(temp);
    }

    /**
     * Performs multiple array copies, correctly ordered to prevent clobbering. The copies
     * must not overlap, and start1 must be less than start2.
     */
    static void arrayCopies(byte[] page,
                            int start1, int dest1, int length1,
                            int start2, int dest2, int length2)
    {
        if (dest1 < start1) {
            System.arraycopy(page, start1, page, dest1, length1);
            System.arraycopy(page, start2, page, dest2, length2);
        } else {
            System.arraycopy(page, start2, page, dest2, length2);
            System.arraycopy(page, start1, page, dest1, length1);
        }
    }

    /**
     * Performs multiple array copies, correctly ordered to prevent clobbering. The copies
     * must not overlap, start1 must be less than start2, and start2 be less than start3.
     */
    static void arrayCopies(byte[] page,
                            int start1, int dest1, int length1,
                            int start2, int dest2, int length2,
                            int start3, int dest3, int length3)
    {
        if (dest1 < start1) {
            System.arraycopy(page, start1, page, dest1, length1);
            arrayCopies(page, start2, dest2, length2, start3, dest3, length3);
        } else {
            arrayCopies(page, start2, dest2, length2, start3, dest3, length3);
            System.arraycopy(page, start1, page, dest1, length1);
        }
    }

    /**
     * @param a key 'a'
     * @param aoff key 'a' offset
     * @param alen key 'a' length
     * @param b key 'b'
     * @param boff key 'b' offset
     * @param blen key 'b' length
     * @return negative if 'a' is less, zero if equal, greater than zero if greater
     */
    static int compareKeys(byte[] a, int aoff, int alen, byte[] b, int boff, int blen) {
        int minLen = Math.min(alen, blen);
        for (int i=0; i<minLen; i++) {
            byte ab = a[aoff + i];
            byte bb = b[boff + i];
            if (ab != bb) {
                return ((ab & 0xff) < (bb & 0xff)) ? -1 : 1;
            }
        }
        return alen - blen;
    }

    public static final int readUnsignedShortBE(byte[] b, int offset) {
        return ((b[offset] & 0xff) << 8) | ((b[offset + 1] & 0xff));
    }

    public static final int readUnsignedShortLE(byte[] b, int offset) {
        return ((b[offset] & 0xff)) | ((b[offset + 1] & 0xff) << 8);
    }

    public static final int readIntBE(byte[] b, int offset) {
        return (b[offset] << 24) | ((b[offset + 1] & 0xff) << 16) |
            ((b[offset + 2] & 0xff) << 8) | (b[offset + 3] & 0xff);
    }

    public static final int readIntLE(byte[] b, int offset) {
        return (b[offset] & 0xff) | ((b[offset + 1] & 0xff) << 8) |
            ((b[offset + 2] & 0xff) << 16) | (b[offset + 3] << 24);
    }

    public static final long readUnsignedInt48BE(byte[] b, int offset) {
        return
            (((long)(((b[offset    ] & 0xff) << 8 ) |
                     ((b[offset + 1] & 0xff)      ))              ) << 32) |
            (((long)(((b[offset + 2]       ) << 24) |
                     ((b[offset + 3] & 0xff) << 16) |
                     ((b[offset + 4] & 0xff) << 8 ) |
                     ((b[offset + 5] & 0xff)      )) & 0xffffffffL)      );
    }

    public static final long readUnsignedInt48LE(byte[] b, int offset) {
        return
            (((long)(((b[offset    ] & 0xff)      ) |
                     ((b[offset + 1] & 0xff) << 8 ) |
                     ((b[offset + 2] & 0xff) << 16) |
                     ((b[offset + 3]       ) << 24)) & 0xffffffffL)      ) |
            (((long)(((b[offset + 4] & 0xff)      ) |
                     ((b[offset + 5] & 0xff) << 8 ))              ) << 32);
    }

    public static final long readLongBE(byte[] b, int offset) {
        return
            (((long)(((b[offset    ]       ) << 24) |
                     ((b[offset + 1] & 0xff) << 16) |
                     ((b[offset + 2] & 0xff) << 8 ) |
                     ((b[offset + 3] & 0xff)      ))              ) << 32) |
            (((long)(((b[offset + 4]       ) << 24) |
                     ((b[offset + 5] & 0xff) << 16) |
                     ((b[offset + 6] & 0xff) << 8 ) |
                     ((b[offset + 7] & 0xff)      )) & 0xffffffffL)      );
    }

    public static final long readLongLE(byte[] b, int offset) {
        return
            (((long)(((b[offset    ] & 0xff)      ) |
                     ((b[offset + 1] & 0xff) << 8 ) |
                     ((b[offset + 2] & 0xff) << 16) |
                     ((b[offset + 3]       ) << 24)) & 0xffffffffL)      ) |
            (((long)(((b[offset + 4] & 0xff)      ) |
                     ((b[offset + 5] & 0xff) << 8 ) |
                     ((b[offset + 6] & 0xff) << 16) |
                     ((b[offset + 7]       ) << 24))              ) << 32);
    }

    /**
     * Reads an integer as encoded by writeUnsignedVarInt.
     */
    public static int readUnsignedVarInt(byte[] b, int offset) {
        int v = b[offset];
        if (v >= 0) {
            return v;
        }
        switch ((v >> 4) & 0x07) {
        case 0x00: case 0x01: case 0x02: case 0x03:
            return (1 << 7)
                + (((v & 0x3f) << 8)
                   | (b[offset + 1] & 0xff));
        case 0x04: case 0x05:
            return ((1 << 14) + (1 << 7))
                + (((v & 0x1f) << 16)
                   | ((b[++offset] & 0xff) << 8)
                   | (b[offset + 1] & 0xff));
        case 0x06:
            return ((1 << 21) + (1 << 14) + (1 << 7))
                + (((v & 0x0f) << 24)
                   | ((b[++offset] & 0xff) << 16)
                   | ((b[++offset] & 0xff) << 8)
                   | (b[offset + 1] & 0xff));
        default:
            return ((1 << 28) + (1 << 21) + (1 << 14) + (1 << 7)) 
                + ((b[++offset] << 24)
                   | ((b[++offset] & 0xff) << 16)
                   | ((b[++offset] & 0xff) << 8)
                   | (b[offset + 1] & 0xff));
        }
    }

    /**
     * Reads an integer as encoded by writeUnsignedVarInt.
     */
    public static int readUnsignedVarInt(byte[] b, int start, int end) throws EOFException {
        if (start >= end) {
            throw new EOFException();
        }
        int v = b[start];
        if (v >= 0) {
            return v;
        }
        switch ((v >> 4) & 0x07) {
        case 0x00: case 0x01: case 0x02: case 0x03:
            if (++start >= end) {
                throw new EOFException();
            }
            return (1 << 7)
                + (((v & 0x3f) << 8)
                   | (b[start] & 0xff));
        case 0x04: case 0x05:
            if (start + 2 >= end) {
                throw new EOFException();
            }
            return ((1 << 14) + (1 << 7))
                + (((v & 0x1f) << 16)
                   | ((b[++start] & 0xff) << 8)
                   | (b[start + 1] & 0xff));
        case 0x06:
            if (start + 3 >= end) {
                throw new EOFException();
            }
            return ((1 << 21) + (1 << 14) + (1 << 7))
                + (((v & 0x0f) << 24)
                   | ((b[++start] & 0xff) << 16)
                   | ((b[++start] & 0xff) << 8)
                   | (b[start + 1] & 0xff));
        default:
            if (start + 4 >= end) {
                throw new EOFException();
            }
            return ((1 << 28) + (1 << 21) + (1 << 14) + (1 << 7)) 
                + ((b[++start] << 24)
                   | ((b[++start] & 0xff) << 16)
                   | ((b[++start] & 0xff) << 8)
                   | (b[start + 1] & 0xff));
        }
    }

    /**
     * Reads an integer as encoded by writeSignedVarInt.
     */
    public static int readSignedVarInt(byte[] b, int offset) {
        int v = readUnsignedVarInt(b, offset);
        return ((v & 1) != 0) ? ((~(v >> 1)) | (1 << 31)) : (v >>> 1);
    }

    /**
     * Reads a long integer as encoded by writeUnsignedVarLong.
     */
    public static long readUnsignedVarLong(byte[] b, IntegerRef offsetRef) {
        int offset = offsetRef.get();
        int length;
        int val = b[offset++];
        if (val >= 0) {
            offsetRef.set(offset);
            return val;
        }
        long decoded;
        switch ((val >> 4) & 0x07) {
        case 0x00: case 0x01: case 0x02: case 0x03:
            decoded = (1L << 7) +
                (((val & 0x3f) << 8)
                 | (b[offset++] & 0xff));
            break;
        case 0x04: case 0x05:
            decoded = ((1L << 14) + (1L << 7))
                + (((val & 0x1f) << 16)
                   | ((b[offset++] & 0xff) << 8)
                   | (b[offset++] & 0xff));
            break;
        case 0x06:
            decoded = ((1L << 21) + (1L << 14) + (1L << 7))
                + (((val & 0x0f) << 24)
                   | ((b[offset++] & 0xff) << 16)
                   | ((b[offset++] & 0xff) << 8)
                   | (b[offset++] & 0xff));
            break;
        default:
            switch (val & 0x0f) {
            default:
                decoded = ((1L << 28) + (1L << 21) + (1L << 14) + (1L << 7))
                    + (((val & 0x07L) << 32)
                       | (((long) (b[offset++] & 0xff)) << 24)
                       | (((long) (b[offset++] & 0xff)) << 16)
                       | (((long) (b[offset++] & 0xff)) << 8)
                       | ((long) (b[offset++] & 0xff)));
                break;
            case 0x08: case 0x09: case 0x0a: case 0x0b:
                decoded = ((1L << 35)
                           + (1L << 28) + (1L << 21) + (1L << 14) + (1L << 7))
                    + (((val & 0x03L) << 40)
                       | (((long) (b[offset++] & 0xff)) << 32)
                       | (((long) (b[offset++] & 0xff)) << 24)
                       | (((long) (b[offset++] & 0xff)) << 16)
                       | (((long) (b[offset++] & 0xff)) << 8)
                       | ((long) (b[offset++] & 0xff)));
                break;
            case 0x0c: case 0x0d:
                decoded = ((1L << 42) + (1L << 35)
                           + (1L << 28) + (1L << 21) + (1L << 14) + (1L << 7))
                    + (((val & 0x01L) << 48)
                       | (((long) (b[offset++] & 0xff)) << 40)
                       | (((long) (b[offset++] & 0xff)) << 32)
                       | (((long) (b[offset++] & 0xff)) << 24)
                       | (((long) (b[offset++] & 0xff)) << 16)
                       | (((long) (b[offset++] & 0xff)) << 8)
                       | ((long) (b[offset++] & 0xff)));
                break;
            case 0x0e:
                decoded = ((1L << 49) + (1L << 42) + (1L << 35)
                           + (1L << 28) + (1L << 21) + (1L << 14) + (1L << 7))
                    + ((((long) (b[offset++] & 0xff)) << 48)
                       | (((long) (b[offset++] & 0xff)) << 40)
                       | (((long) (b[offset++] & 0xff)) << 32)
                       | (((long) (b[offset++] & 0xff)) << 24)
                       | (((long) (b[offset++] & 0xff)) << 16)
                       | (((long) (b[offset++] & 0xff)) << 8)
                       | ((long) (b[offset++] & 0xff)));
                break;
            case 0x0f:
                decoded = ((1L << 56) + (1L << 49) + (1L << 42) + (1L << 35)
                           + (1L << 28) + (1L << 21) + (1L << 14) + (1L << 7))
                    + ((((long) b[offset++]) << 56)
                       | (((long) (b[offset++] & 0xff)) << 48)
                       | (((long) (b[offset++] & 0xff)) << 40)
                       | (((long) (b[offset++] & 0xff)) << 32)
                       | (((long) (b[offset++] & 0xff)) << 24)
                       | (((long) (b[offset++] & 0xff)) << 16)
                       | (((long) (b[offset++] & 0xff)) << 8L)
                       | ((long) (b[offset++] & 0xff)));
                break;
            }
            break;
        }

        offsetRef.set(offset);
        return decoded;
    }

    public static final void writeShortBE(byte[] b, int offset, int v) {
        b[offset    ] = (byte)(v >> 8);
        b[offset + 1] = (byte)v;
    }

    public static final void writeShortLE(byte[] b, int offset, int v) {
        b[offset    ] = (byte)v;
        b[offset + 1] = (byte)(v >> 8);
    }

    public static final void writeIntBE(byte[] b, int offset, int v) {
        b[offset    ] = (byte)(v >> 24);
        b[offset + 1] = (byte)(v >> 16);
        b[offset + 2] = (byte)(v >> 8);
        b[offset + 3] = (byte)v;
    }

    public static final void writeIntLE(byte[] b, int offset, int v) {
        b[offset    ] = (byte)v;
        b[offset + 1] = (byte)(v >> 8);
        b[offset + 2] = (byte)(v >> 16);
        b[offset + 3] = (byte)(v >> 24);
    }

    public static final void writeInt48BE(byte[] b, int offset, long v) {
        int w = (int)(v >> 32);
        b[offset    ] = (byte)(w >> 8);
        b[offset + 1] = (byte)w;
        w = (int)v;
        b[offset + 2] = (byte)(w >> 24);
        b[offset + 3] = (byte)(w >> 16);
        b[offset + 4] = (byte)(w >> 8);
        b[offset + 5] = (byte)w;
    }

    public static final void writeInt48LE(byte[] b, int offset, long v) {
        int w = (int)v;
        b[offset    ] = (byte)w;
        b[offset + 1] = (byte)(w >> 8);
        b[offset + 2] = (byte)(w >> 16);
        b[offset + 3] = (byte)(w >> 24);
        w = (int)(v >> 32);
        b[offset + 4] = (byte)w;
        b[offset + 5] = (byte)(w >> 8);
    }

    public static final void writeLongBE(byte[] b, int offset, long v) {
        int w = (int)(v >> 32);
        b[offset    ] = (byte)(w >> 24);
        b[offset + 1] = (byte)(w >> 16);
        b[offset + 2] = (byte)(w >> 8);
        b[offset + 3] = (byte)w;
        w = (int)v;
        b[offset + 4] = (byte)(w >> 24);
        b[offset + 5] = (byte)(w >> 16);
        b[offset + 6] = (byte)(w >> 8);
        b[offset + 7] = (byte)w;
    }

    public static final void writeLongLE(byte[] b, int offset, long v) {
        int w = (int)v;
        b[offset    ] = (byte)w;
        b[offset + 1] = (byte)(w >> 8);
        b[offset + 2] = (byte)(w >> 16);
        b[offset + 3] = (byte)(w >> 24);
        w = (int)(v >> 32);
        b[offset + 4] = (byte)w;
        b[offset + 5] = (byte)(w >> 8);
        b[offset + 6] = (byte)(w >> 16);
        b[offset + 7] = (byte)(w >> 24);
    }

    public static int calcUnsignedVarIntLength(int v) {
        if (v < (1 << 7)) {
            return v < 0 ? 5 : 1;
        }
        v -= (1 << 7);
        if (v < (1 << 14)) {
            return 2;
        }
        v -= (1 << 14);
        if (v < (1 << 21)) {
            return 3;
        }
        v -= (1 << 21);
        if (v < (1 << 28)) {
            return 4;
        }
        return 5;
    }

    /**
     * Write the given integer using 1 to 5 bytes. Values closer to zero are
     * encoded in fewer bytes.
     *
     * <pre>
     * Value range                                Required bytes
     * ---------------------------------------------------------
     * 0..127                                     1
     * 128..16511                                 2
     * 16512..2113663                             3
     * 2113664..270549119                         4
     * 270549120..4294967295                      5
     * </pre>
     *
     * @return new offset
     */
    public static int writeUnsignedVarInt(byte[] b, int offset, int v) {
        if (v < (1 << 7)) {
            if (v < 0) {
                v -= (1 << 28) + (1 << 21) + (1 << 14) + (1 << 7);
                b[offset++] = (byte) (0xff);
                b[offset++] = (byte) (v >> 24);
                b[offset++] = (byte) (v >> 16);
                b[offset++] = (byte) (v >> 8);
            }
        } else {
            v -= (1 << 7);
            if (v < (1 << 14)) {
                b[offset++] = (byte) (0x80 | (v >> 8));
            } else {
                v -= (1 << 14);
                if (v < (1 << 21)) {
                    b[offset++] = (byte) (0xc0 | (v >> 16));
                } else {
                    v -= (1 << 21);
                    if (v < (1 << 28)) {
                        b[offset++] = (byte) (0xe0 | (v >> 24));
                    } else {
                        v -= (1 << 28);
                        b[offset++] = (byte) (0xf0);
                        b[offset++] = (byte) (v >> 24);
                    }
                    b[offset++] = (byte) (v >> 16);
                }
                b[offset++] = (byte) (v >> 8);
            }
        }
        b[offset++] = (byte) v;
        return offset;
    }

    /**
     * Write the given integer using 1 to 5 bytes. Values closer to zero are
     * encoded in fewer bytes.
     *
     * <pre>
     * Value range(s)                                    Required bytes
     * ----------------------------------------------------------------
     * -64..63                                           1
     * -8256..-65, 64..8255                              2
     * -1056832..-8257, 8256..1056831                    3
     * -135274560..-1056833, 1056832..135274559          4
     * -2147483648..-135274561, 135274560..2147483647    5
     * </pre>
     *
     * @return new offset
     */
    public static int writeSignedVarInt(byte[] b, int offset, int v) {
        if (v < 0) {
            // Compliment negative value to turn all the ones to zeros, which
            // can be compacted. Shift and put sign bit at LSB.
            v = ((~v) << 1) | 1;
        } else {
            // Shift and put sign bit at LSB.
            v <<= 1;
        }
        return writeUnsignedVarInt(b, offset, v);
    }

    public static int calcUnsignedVarLongLength(long v) {
        if (v < (1L << 7)) {
            return v < 0 ? 9 : 1;
        }
        v -= (1L << 7);
        if (v < (1L << 14)) {
            return 2;
        }
        v -= (1L << 14);
        if (v < (1L << 21)) {
            return 3;
        }
        v -= (1L << 21);
        if (v < (1L << 28)) {
            return 4;
        }
        v -= (1L << 28);
        if (v < (1L << 35)) {
            return 5;
        }
        v -= (1L << 35);
        if (v < (1L << 42)) {
            return 6;
        }
        v -= (1L << 42);
        if (v < (1L << 49)) {
            return 7;
        }
        v -= (1L << 49);
        if (v < (1L << 56)) {
            return 8;
        }
        return 9;
    }

    /**
     * Write the given long integer using 1 to 9 bytes. Values closer to zero
     * are encoded in fewer bytes.
     *
     * <pre>
     * Value range                                Required bytes
     * ---------------------------------------------------------
     * 0..127                                     1
     * 128..16511                                 2
     * 16512..2113663                             3
     * 2113664..270549119                         4
     * 270549120..34630287487                     5
     * 34630287488..4432676798591                 6
     * 4432676798592..567382630219903             7
     * 567382630219904..72624976668147839         8
     * 72624976668147840..18446744073709551615    9
     * </pre>
     *
     * @return new offset
     */
    public static int writeUnsignedVarLong(byte[] b, int offset, long v) {
        if (v < (1L << 7)) {
            if (v < 0) {
                v -= (1L << 56) + (1L << 49) + (1L << 42) + (1L << 35)
                    + (1L << 28) + (1L << 21) + (1L << 14) + (1L << 7);
                b[offset++] = (byte) (0xff);
                b[offset++] = (byte) (v >> 56);
                b[offset++] = (byte) (v >> 48);
                b[offset++] = (byte) (v >> 40);
                b[offset++] = (byte) (v >> 32);
                b[offset++] = (byte) (v >> 24);
                b[offset++] = (byte) (v >> 16);
                b[offset++] = (byte) (v >> 8);
            }
        } else {
            v -= (1L << 7);
            if (v < (1L << 14)) {
                b[offset++] = (byte) (0x80 | (int) (v >> 8));
            } else {
                v -= (1L << 14);
                if (v < (1L << 21)) {
                    b[offset++] = (byte) (0xc0 | (int) (v >> 16));
                } else {
                    v -= (1L << 21);
                    if (v < (1L << 28)) {
                        b[offset++] = (byte) (0xe0 | (int) (v >> 24));
                    } else {
                        v -= (1L << 28);
                        if (v < (1L << 35)) {
                            b[offset++] = (byte) (0xf0 | (int) (v >> 32));
                        } else {
                            v -= (1L << 35);
                            if (v < (1L << 42)) {
                                b[offset++] = (byte) (0xf8 | (int) (v >> 40));
                            } else {
                                v -= (1L << 42);
                                if (v < (1L << 49)) {
                                    b[offset++] = (byte) (0xfc | (int) (v >> 48));
                                } else {
                                    v -= (1L << 49);
                                    if (v < (1L << 56)) {
                                        b[offset++] = (byte) (0xfe | (int) (v >> 56));
                                    } else {
                                        v -= (1L << 56);
                                        b[offset++] = (byte) (0xff);
                                        b[offset++] = (byte) (v >> 56);
                                    }
                                    b[offset++] = (byte) (v >> 48);
                                }
                                b[offset++] = (byte) (v >> 40);
                            }
                            b[offset++] = (byte) (v >> 32);
                        }
                        b[offset++] = (byte) (v >> 24);
                    }
                    b[offset++] = (byte) (v >> 16);
                }
                b[offset++] = (byte) (v >> 8);
            }
        }
        b[offset++] = (byte) v;
        return offset;
    }

    /**
     * Adds one to an unsigned integer, represented as a byte array. If
     * overflowed, value in byte array is 0x00, 0x00, 0x00...
     *
     * @param value unsigned integer to increment
     * @return false if overflowed
     */
    public static boolean increment(byte[] value, int offset, int len) {
        while (--len >= offset) {
            byte digit = (byte) ((value[len] & 0xff) + 1);
            value[len] = digit;
            if (digit != 0) {
                // No carry bit, so done adding.
                return true;
            }
        }
        // This point is reached upon overflow.
        return false;
    }

    /**
     * Subtracts one from an unsigned integer, represented as a byte array. If
     * overflowed, value in byte array is 0xff, 0xff, 0xff...
     *
     * @param value unsigned integer to decrement
     * @return false if overflowed
     */
    public static boolean decrement(byte[] value, int offset, int len) {
        while (--len >= offset) {
            byte digit = (byte) ((value[len] & 0xff) + -1);
            value[len] = digit;
            if (digit != -1) {
                // No borrow bit, so done subtracting.
                return true;
            }
        }
        // This point is reached upon overflow.
        return false;
    }

    /**
     * Subtracts one from the given reverse unsigned variable integer, of any
     * size. A reverse unsigned variable integer is encoded such that all the
     * bits are complemented. When lexicographically sorted, the order is
     * reversed. Zero is encoded as 0xff.
     *
     * @param b always modified
     * @param offset location of value
     * @return original byte array if large enough; new copy if it needed to grow
     */
    public static byte[] decrementReverseUnsignedVar(byte[] b, int offset) {
        int len = decodeReverseUnsignedLength(b, offset);
        decrement(b, offset, len);
        if (len != decodeReverseUnsignedLength(b, offset)) {
            byte[] copy = new byte[b.length + 1];
            System.arraycopy(b, 0, copy, 0, b.length);
            copy[copy.length - 1] = (byte) 0xff;
            b = copy;
        }
        return b;
    }

    private static int decodeReverseUnsignedLength(byte[] b, int offset) {
        int g = 23;
        int h;
        while ((h = Integer.numberOfLeadingZeros(b[offset] & 0xff)) == 32) {
            g -= 8;
            offset++;
        }
        return h - g;
    }

    static String toHex(byte[] b) {
        return toHex(b, 0, b.length);
    }

    static String toHex(byte[] b, int offset, int length) {
        StringBuilder bob = new StringBuilder();

        for (int i=0; i<length; i+=16) {
            if (i > 0) {
                bob.append('\n');
            }

            String prefix = "0000000".concat(Integer.toHexString(i));
            prefix = prefix.substring(prefix.length() - 8);
            bob.append(prefix);
            bob.append(": ");

            for (int j=0; j<16; j+=2) {
                int pos = i + j;
                if (pos >= length - 1) {
                    if (pos >= length) {
                        bob.append("     ");
                    } else {
                        int v = b[offset + pos] & 0xff;
                        if (v < 0x10) {
                            bob.append('0');
                        }
                        bob.append(Integer.toHexString(v));
                        bob.append("   ");
                    }
                } else {
                    String pair = "000".concat
                        (Integer.toHexString(readUnsignedShortBE(b, offset + pos)));
                    pair = pair.substring(pair.length() - 4);
                    bob.append(pair);
                    bob.append(' ');
                }
            }
            
            bob.append(' ');

            for (int j=0; j<16; j++) {
                int pos = i + j;
                if (pos >= length) {
                    break;
                }
                char c = (char) (b[offset + pos] & 0xff);
                bob.append(Character.isISOControl(c) ? '.' : c);
            }
        }

        return bob.toString();
    }

    /**
     * Deletes all files in the base file's directory which are named like
     * "base<pattern><number>". For example, mybase.redo.123
     */
    static void deleteNumberedFiles(File baseFile, String pattern) throws IOException {
        String prefix = baseFile.getName() + pattern;
        for (File file : baseFile.getParentFile().listFiles()) {
            String name = file.getName();
            if (name.startsWith(prefix)) {
                String suffix = name.substring(prefix.length());
                try {
                    Long.parseLong(suffix);
                } catch (NumberFormatException e) {
                    continue;
                }
                file.delete();
            }
        }
    }

    static IOException closeOnFailure(final Closeable c, final Throwable e) throws IOException {
        // Close in a separate thread, in case of deadlock.
        Thread closer;
        try {
            closer = new Thread() {
                public void run() {
                    try {
                        close(c, e);
                    } catch (IOException e2) {
                        // Ignore.
                    }
                }
            };
            closer.setDaemon(true);
            closer.start();
        } catch (Throwable e2) {
            closer = null;
        }

        if (closer == null) {
            try {
                close(c, e);
            } catch (IOException e2) {
                // Ignore.
            }
        } else {
            // Block up to one second.
            try {
                closer.join(1000);
            } catch (InterruptedException e2) {
            }
        }

        if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        }
        if (e instanceof Error) {
            throw (Error) e;
        }
        if (e instanceof IOException) {
            throw (IOException) e;
        }

        throw new CorruptDatabaseException(e);
    }

    /**
     * @param first returned if non-null
     * @param c can be null
     * @return IOException which was caught, unless e was non-null
     */
    static IOException closeQuietly(IOException first, Closeable c) {
        if (c != null) {
            try {
                c.close();
            } catch (IOException e) {
                if (first == null) {
                    return e;
                }
            }
        }
        return first;
    }

    /**
     * @param first returned if non-null
     * @param c can be null
     * @return IOException which was caught, unless e was non-null
     */
    static IOException closeQuietly(IOException first, Closeable c, Throwable cause) {
        if (c != null) {
            try {
                close(c, cause);
            } catch (IOException e) {
                if (first == null) {
                    return e;
                }
            }
        }
        return first;
    }

    static void close(Closeable c, Throwable cause) throws IOException {
        if (c instanceof CauseCloseable) {
            ((CauseCloseable) c).close(cause);
        } else {
            c.close();
        }
    }

    static Throwable rootCause(Throwable e) {
        while (true) {
            Throwable cause = e.getCause();
            if (cause == null) {
                return e;
            }
            e = cause;
        }
    }

    static void uncaught(Throwable e) {
        Thread t = Thread.currentThread();
        t.getUncaughtExceptionHandler().uncaughtException(t, e);
    }

    static RuntimeException rethrow(Throwable e) {
        Utils.<RuntimeException>castAndThrow(e);
        return null;
    }

    static RuntimeException rethrow(Throwable e, Throwable cause) {
        if (e.getCause() == null && cause != null) {
            try {
                e.initCause(cause);
            } catch (Exception e2) {
            } 
        }
        Utils.<RuntimeException>castAndThrow(e);
        return null;
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void castAndThrow(Throwable e) throws T {
        throw (T) e;
    }
}
