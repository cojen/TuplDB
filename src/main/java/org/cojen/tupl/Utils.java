/*
 *  Copyright 2011-2013 Brian S O'Neill
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
import java.io.File;
import java.io.InputStream;
import java.io.IOException;

import java.math.BigInteger;

import java.util.Arrays;
import java.util.Random;

import java.util.concurrent.TimeUnit;

import static java.lang.System.arraycopy;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class Utils extends org.cojen.tupl.io.Utils {
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
        encodeLongBE(temp, 1, v);
        return new BigInteger(temp);
    }

    private static final Random cRnd = new Random();
    
    static Random random() {
        return cRnd;
    }

    private static int cSeedMix = cRnd.nextInt();

    /**
     * @return non-zero random number, suitable for Xorshift RNG or object hashcode
     */
    static int randomSeed() {
        long id = Thread.currentThread().getId();
        int seed = ((int) id) ^ ((int) (id >>> 32)) ^ cSeedMix;
        if (seed == 0) {
            while ((seed = cRnd.nextInt()) == 0);
        }
        cSeedMix = nextRandom(seed);
        return seed;
    }

    /**
     * @param seed must not be zero
     * @return next random number using Xorshift RNG by George Marsaglia (also next seed)
     */
    static int nextRandom(int seed) {
        seed ^= seed << 13;
        seed ^= seed >>> 17;
        seed ^= seed << 5;
        return seed;
    }

    /**
     * Apply Wang/Jenkins hash function to given value. Hash is invertible, and
     * so no uniqueness is lost.
     */
    static long scramble(long v) {
        v = (v << 21) - v - 1;
        v = v ^ (v >>> 24);
        v = (v + (v << 3)) + (v << 8); // v * 265
        v = v ^ (v >>> 14);
        v = (v + (v << 2)) + (v << 4); // v * 21
        v = v ^ (v >>> 28);
        return v + (v << 31);
    }

    /*
    public static long unscramble(long v) {
        // http://naml.us/blog/2012/03/inverse-of-a-hash-function

        long tmp;

        // Invert v = v + (v << 31)
        tmp = v - (v << 31);
        v = v - (tmp << 31);

        // Invert v = v ^ (v >>> 28)
        tmp = v ^ v >>> 28;
        v = v ^ tmp >>> 28;

        // Invert v *= 21
        //v *= 14933078535860113213u;
        v = (v * 7466539267930056606L) + (v * 7466539267930056607L);

        // Invert v = v ^ (v >>> 14)
        tmp = v ^ v >>> 14;
        tmp = v ^ tmp >>> 14;
        tmp = v ^ tmp >>> 14;
        v = v ^ tmp >>> 14;

        // Invert v *= 265
        //v *= 15244667743933553977u;
        v = (v * 7622333871966776988L) + (v * 7622333871966776989L);

        // Invert v = v ^ (v >>> 24)
        tmp = v ^ v >>> 24;
        v = v ^ tmp >>> 24;

        // Invert v = (~v) + (v << 21)
        tmp = ~v;
        tmp = ~(v - (tmp << 21));
        tmp = ~(v - (tmp << 21));
        v = ~(v - (tmp << 21));

        return v;
    }
    */

    static TimeUnit inferUnit(TimeUnit unit, long value) {
        infer: {
            if (value == 0) break infer;
            if ((value - (value /= 1000) * 1000) != 0) break infer;
            unit = TimeUnit.MICROSECONDS;
            if ((value - (value /= 1000) * 1000) != 0) break infer;
            unit = TimeUnit.MILLISECONDS;
            if ((value - (value /= 1000) * 1000) != 0) break infer;
            unit = TimeUnit.SECONDS;
            if ((value - (value /= 60) * 60) != 0) break infer;
            unit = TimeUnit.MINUTES;
            if ((value - (value /= 60) * 60) != 0) break infer;
            unit = TimeUnit.HOURS;
            if ((value - (value / 24) * 24) != 0) break infer;
            unit = TimeUnit.DAYS;
        }

        return unit;
    }

    static String timeoutMessage(long nanosTimeout, DatabaseException ex) {
        if (nanosTimeout == 0) {
            return "Never waited";
        } else if (nanosTimeout < 0) {
            return "Infinite wait";
        } else {
            StringBuilder b = new StringBuilder("Waited ");
            appendTimeout(b, ex.getTimeout(), ex.getUnit());
            return b.toString();
        }
    }

    static void appendTimeout(StringBuilder b, long timeout, TimeUnit unit) {
        if (timeout == 0) {
            b.append('0');
        } else if (timeout < 0) {
            b.append("infinite");
        } else {
            b.append(timeout);
            b.append(' ');
            String unitStr = unit.toString().toLowerCase();
            if (timeout == 1) {
                unitStr = unitStr.substring(0, unitStr.length() - 1);
            }
            b.append(unitStr);
        }
    }

    /**
     * Returns null if given null, or clones into a new array only if not empty.
     */
    static byte[] cloneArray(byte[] bytes) {
        return (bytes == null || bytes.length == 0) ? bytes : bytes.clone();
    }

    /**
     * Performs an array copy as usual, but if src is null, treats it as zeros.
     */
    static void arrayCopyOrFill(byte[] src, int srcPos, byte[] dest, int destPos, int length) {
        if (src == null) {
            Arrays.fill(dest, destPos, destPos + length, (byte) 0);
        } else {
            arraycopy(src, srcPos, dest, destPos, length);
        }
    }

    /**
     * @return negative if 'a' is less, zero if equal, greater than zero if greater
     */
    static int compareKeys(byte[] a, byte[] b) {
        return compareKeys(a, 0, a.length, b, 0, b.length);
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
                return (ab & 0xff) - (bb & 0xff);
            }
        }
        return alen - blen;
    }

    /**
     * Returns a new key, midway between the given low and high keys. Returned key is never
     * equal to the low key, but it might be equal to the high key. If high key is not actually
     * higher than the given low key, an ArrayIndexOfBoundException might be thrown.
     *
     * <p>Method is used for internal node suffix compression. To disable, simply return a copy
     * of the high key.
     */
    static byte[] midKey(byte[] low, byte[] high) {
        return midKey(low, 0, low.length, high, 0, high.length);
    }

    /**
     * Returns a new key, midway between the given low and high keys. Returned key is never
     * equal to the low key, but it might be equal to the high key. If high key is not actually
     * higher than the given low key, an ArrayIndexOfBoundException might be thrown.
     *
     * <p>Method is used for internal node suffix compression. To disable, simply return a copy
     * of the high key.
     */
    static byte[] midKey(byte[] low, int lowOff, int lowLen,
                         byte[] high, int highOff, int highLen)
    {
        for (int i=0; i<lowLen; i++) {
            byte lo = low[lowOff + i];
            byte hi = high[highOff + i];
            if (lo != hi) {
                byte[] mid = new byte[i + 1];
                System.arraycopy(low, lowOff, mid, 0, i);
                mid[i] = (byte) (((lo & 0xff) + (hi & 0xff) + 1) >> 1);
                return mid;
            }
        }
        byte[] mid = new byte[lowLen + 1];
        System.arraycopy(high, highOff, mid, 0, mid.length);
        return mid;
    }

    static void readFully(InputStream in, byte[] b, int off, int len) throws IOException {
        if (len > 0) {
            while (true) {
                int amt = in.read(b, off, len);
                if (amt <= 0) {
                    throw new EOFException();
                }
                if ((len -= amt) <= 0) {
                    break;
                }
                off += amt;
            }
        }
    }

    /**
     * Decodes an integer as encoded by encodeUnsignedVarInt.
     */
    public static int decodeUnsignedVarInt(byte[] b, int offset) {
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
     * Decodes an integer as encoded by encodeUnsignedVarInt.
     */
    public static int decodeUnsignedVarInt(byte[] b, int start, int end) throws EOFException {
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
     * Decodes an integer as encoded by encodeSignedVarInt.
     */
    public static int decodeSignedVarInt(byte[] b, int offset) {
        int v = decodeUnsignedVarInt(b, offset);
        return ((v & 1) != 0) ? ((~(v >> 1)) | (1 << 31)) : (v >>> 1);
    }

    /**
     * Decodes an integer as encoded by encodeSignedVarLong.
     */
    public static long decodeSignedVarLong(byte[] b, IntegerRef offsetRef) {
        long v = decodeUnsignedVarLong(b, offsetRef);
        return ((v & 1) != 0) ? ((~(v >> 1)) | (1 << 31)) : (v >>> 1);
    }

    /**
     * Decodes a long integer as encoded by encodeUnsignedVarLong.
     */
    public static long decodeUnsignedVarLong(byte[] b, IntegerRef offsetRef) {
        int offset = offsetRef.get();
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
     * Encode the given integer using 1 to 5 bytes. Values closer to zero are
     * encoded in fewer bytes.
     *
     * <pre>
     * Value range                                Required bytes  Header
     * ---------------------------------------------------------------------
     * 0..127                                     1               0b0xxxxxxx
     * 128..16511                                 2               0b10xxxxxx
     * 16512..2113663                             3               0b110xxxxx
     * 2113664..270549119                         4               0b1110xxxx
     * 270549120..4294967295                      5               0b11110000
     * </pre>
     *
     * @return new offset
     */
    public static int encodeUnsignedVarInt(byte[] b, int offset, int v) {
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
     * Encode the given integer using 1 to 5 bytes. Values closer to zero are
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
    public static int encodeSignedVarInt(byte[] b, int offset, int v) {
        if (v < 0) {
            // Complement negative value to turn all the ones to zeros, which
            // can be compacted. Shift and put sign bit at LSB.
            v = ((~v) << 1) | 1;
        } else {
            // Shift and put sign bit at LSB.
            v <<= 1;
        }
        return encodeUnsignedVarInt(b, offset, v);
    }

    /**
     * @return new offset
     */
    public static int encodeSignedVarLong(byte[] b, int offset, long v) {
        if (v < 0) {
            // Complement negative value to turn all the ones to zeros, which
            // can be compacted. Shift and put sign bit at LSB.
            v = ((~v) << 1) | 1;
        } else {
            // Shift and put sign bit at LSB.
            v <<= 1;
        }
        return encodeUnsignedVarLong(b, offset, v);
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
     * Encode the given long integer using 1 to 9 bytes. Values closer to zero
     * are encoded in fewer bytes.
     *
     * <pre>
     * Value range                                Required bytes  Header
     * ---------------------------------------------------------------------
     * 0..127                                     1               0b0xxxxxxx
     * 128..16511                                 2               0b10xxxxxx
     * 16512..2113663                             3               0b110xxxxx
     * 2113664..270549119                         4               0b1110xxxx
     * 270549120..34630287487                     5               0b11110xxx
     * 34630287488..4432676798591                 6               0b111110xx
     * 4432676798592..567382630219903             7               0b1111110x
     * 567382630219904..72624976668147839         8               0b11111110
     * 72624976668147840..18446744073709551615    9               0b11111111
     * </pre>
     *
     * @return new offset
     */
    public static int encodeUnsignedVarLong(byte[] b, int offset, long v) {
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
     * @param start inclusive index
     * @param end exclusive index
     * @return false if overflowed
     */
    public static boolean increment(byte[] value, final int start, int end) {
        while (--end >= start) {
            if (++value[end] != 0) {
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
     * @param start inclusive index
     * @param end exclusive index
     * @return false if overflowed
     */
    public static boolean decrement(byte[] value, final int start, int end) {
        while (--end >= start) {
            if (--value[end] != -1) {
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
        decrement(b, offset, offset + len);
        if (len != decodeReverseUnsignedLength(b, offset)) {
            byte[] copy = new byte[b.length + 1];
            arraycopy(b, 0, copy, 0, b.length);
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

    static String toHex(byte[] key) {
        return key == null ? "null" : toHex(key, 0, key.length);
    }

    static String toHex(byte[] key, int offset, int length) {
        if (key == null) {
            return "null";
        }
        char[] chars = new char[length << 1];
        int end = offset + length;
        for (int bi=offset, ci=0; bi<end; bi++) {
            int b = key[bi] & 0xff;
            chars[ci++] = toHexChar(b >> 4);
            chars[ci++] = toHexChar(b & 0xf);
        }
        return new String(chars);
    }

    private static char toHexChar(int b) {
        return (char) ((b < 10) ? ('0' + b) : ('a' + b - 10));
    }

    static String toHexDump(byte[] b) {
        return toHexDump(b, 0, b.length);
    }

    static String toHexDump(byte[] b, int offset, int length) {
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
                        (Integer.toHexString(decodeUnsignedShortBE(b, offset + pos)));
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
        deleteNumberedFiles(baseFile, pattern, 0);
    }

    /**
     * Deletes all files in the base file's directory which are named like
     * "base<pattern><number>". For example, mybase.redo.123
     *
     * @param min delete numbers greater than or equal to this
     */
    static void deleteNumberedFiles(File baseFile, String pattern, long min) throws IOException {
        String prefix = baseFile.getName() + pattern;
        for (File file : baseFile.getParentFile().listFiles()) {
            String name = file.getName();
            if (name.startsWith(prefix)) {
                String suffix = name.substring(prefix.length());
                long num;
                try {
                    num = Long.parseLong(suffix);
                } catch (NumberFormatException e) {
                    continue;
                }
                if (num >= min) {
                    file.delete();
                }
            }
        }
    }
}
