/*
 *  Copyright 2011 Brian S O'Neill
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

import java.io.EOFException;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class DataUtils {
    public static final int readUnsignedShort(byte[] b, int offset) {
        return ((b[offset] & 0xff) << 8) | ((b[offset + 1] & 0xff));
    }

    public static final int readInt(byte[] b, int offset) {
        return (b[offset] << 24) | ((b[offset + 1] & 0xff) << 16) |
            ((b[offset + 2] & 0xff) << 8) | (b[offset + 3] & 0xff);
    }

    public static final long readInt6(byte[] b, int offset) {
        return
            (((long)(((b[offset    ] & 0xff) << 8 ) | 
                     ((b[offset + 1] & 0xff)      ))              ) << 32) |
            (((long)(((b[offset + 2]       ) << 24) |
                     ((b[offset + 3] & 0xff) << 16) |
                     ((b[offset + 4] & 0xff) << 8 ) | 
                     ((b[offset + 5] & 0xff)      )) & 0xffffffffL)      );
    }

    public static final long readLong(byte[] b, int offset) {
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

    public static final void writeShort(byte[] b, int offset, int v) {
        b[offset    ] = (byte)(v >> 8);
        b[offset + 1] = (byte)v;
    }

    public static final void writeInt(byte[] b, int offset, int v) {
        b[offset    ] = (byte)(v >> 24);
        b[offset + 1] = (byte)(v >> 16);
        b[offset + 2] = (byte)(v >> 8);
        b[offset + 3] = (byte)v;
    }

    public static final void writeInt6(byte[] b, int offset, long v) {
        int w = (int)(v >> 32);
        b[offset    ] = (byte)(w >> 8);
        b[offset + 1] = (byte)w;
        w = (int)v;
        b[offset + 2] = (byte)(w >> 24);
        b[offset + 3] = (byte)(w >> 16);
        b[offset + 4] = (byte)(w >> 8);
        b[offset + 5] = (byte)w;
    }

    public static final void writeLong(byte[] b, int offset, long v) {
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
                        (Integer.toHexString(readUnsignedShort(b, offset + pos)));
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
}
