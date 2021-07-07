/*
 *  Copyright (C) 2021 Cojen.org
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
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.rows;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.nio.ByteOrder;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class PrimitiveArrayUtils extends RowUtils {
    private static final VarHandle cShortArrayLEHandle;
    private static final VarHandle cShortArrayBEHandle;
    private static final VarHandle cIntArrayLEHandle;
    private static final VarHandle cIntArrayBEHandle;
    private static final VarHandle cLongArrayLEHandle;
    private static final VarHandle cLongArrayBEHandle;
    private static final VarHandle cFloatArrayLEHandle;
    private static final VarHandle cFloatArrayBEHandle;
    private static final VarHandle cDoubleArrayLEHandle;
    private static final VarHandle cDoubleArrayBEHandle;

    static {
        try {
            cShortArrayLEHandle = MethodHandles.byteArrayViewVarHandle
                (short[].class, ByteOrder.LITTLE_ENDIAN);
            cShortArrayBEHandle = MethodHandles.byteArrayViewVarHandle
                (short[].class, ByteOrder.BIG_ENDIAN);
            cIntArrayLEHandle = MethodHandles.byteArrayViewVarHandle
                (int[].class, ByteOrder.LITTLE_ENDIAN);
            cIntArrayBEHandle = MethodHandles.byteArrayViewVarHandle
                (int[].class, ByteOrder.BIG_ENDIAN);
            cLongArrayLEHandle = MethodHandles.byteArrayViewVarHandle
                (long[].class, ByteOrder.LITTLE_ENDIAN);
            cLongArrayBEHandle = MethodHandles.byteArrayViewVarHandle
                (long[].class, ByteOrder.BIG_ENDIAN);
            cFloatArrayLEHandle = MethodHandles.byteArrayViewVarHandle
                (float[].class, ByteOrder.LITTLE_ENDIAN);
            cFloatArrayBEHandle = MethodHandles.byteArrayViewVarHandle
                (float[].class, ByteOrder.BIG_ENDIAN);
            cDoubleArrayLEHandle = MethodHandles.byteArrayViewVarHandle
                (double[].class, ByteOrder.LITTLE_ENDIAN);
            cDoubleArrayBEHandle = MethodHandles.byteArrayViewVarHandle
                (double[].class, ByteOrder.BIG_ENDIAN);
        } catch (Throwable e) {
            throw new ExceptionInInitializerError();
        }
    }

    /**
     * Encode a boolean array into the destination byte array, as 0 and 1, unpacked.
     */
    public static void encodeArray(byte[] dst, int offset, boolean[] a) {
        for (boolean v : a) {
            dst[offset++] = v ? (byte) 1 : (byte) 0;
        }
    }

    /**
     * Decode a boolean array from the source byte array.
     */
    public static boolean[] decodeBooleanArray(byte[] src, int offset, int length) {
        var a = new boolean[length];
        int end = offset + length;
        for (int i=0; offset < end; offset++) {
            a[i++] = src[offset] != 0;
        }
        return a;
    }


    /**
     * Encode an array into the destination byte array, in little-endian format.
     */
    public static void encodeArrayLE(byte[] dst, int offset, short[] a) {
        for (short v : a) {
            cShortArrayLEHandle.set(dst, offset, v);
            offset += 2;
        }
    }

    /**
     * Encode an array into the destination byte array, in big-endian format.
     */
    public static void encodeArrayBE(byte[] dst, int offset, short[] a) {
        for (short v : a) {
            cShortArrayBEHandle.set(dst, offset, v);
            offset += 2;
        }
    }

    /**
     * Encode an array into the destination byte array, in big-endian format, with bit flips.
     */
    public static void encodeArrayKey(byte[] dst, int offset, short[] a) {
        for (short v : a) {
            cShortArrayBEHandle.set(dst, offset, (short) (v ^ (1 << 15)));
            offset += 2;
        }
    }

    /**
     * Encode an array into the destination byte array, in little-endian format.
     */
    public static void encodeArrayLE(byte[] dst, int offset, int[] a) {
        for (int v : a) {
            cIntArrayLEHandle.set(dst, offset, v);
            offset += 4;
        }
    }

    /**
     * Encode an array into the destination byte array, in big-endian format.
     */
    public static void encodeArrayBE(byte[] dst, int offset, int[] a) {
        for (int v : a) {
            cIntArrayBEHandle.set(dst, offset, v);
            offset += 4;
        }
    }

    /**
     * Encode an array into the destination byte array, in big-endian format, with bit flips.
     */
    public static void encodeArrayKey(byte[] dst, int offset, int[] a) {
        for (int v : a) {
            cIntArrayBEHandle.set(dst, offset, v ^ (1 << 31));
            offset += 4;
        }
    }

    /**
     * Encode an array into the destination byte array, in little-endian format.
     */
    public static void encodeArrayLE(byte[] dst, int offset, long[] a) {
        for (long v : a) {
            cLongArrayLEHandle.set(dst, offset, v);
            offset += 8;
        }
    }

    /**
     * Encode an array into the destination byte array, in big-endian format.
     */
    public static void encodeArrayBE(byte[] dst, int offset, long[] a) {
        for (long v : a) {
            cLongArrayBEHandle.set(dst, offset, v);
            offset += 8;
        }
    }

    /**
     * Encode an array into the destination byte array, in big-endian format, with bit flips.
     */
    public static void encodeArrayKey(byte[] dst, int offset, long[] a) {
        for (long v : a) {
            cLongArrayBEHandle.set(dst, offset, v ^ (1L << 63));
            offset += 8;
        }
    }

    /**
     * Encode an array into the destination byte array, in little-endian format.
     */
    public static void encodeArrayLE(byte[] dst, int offset, float[] a) {
        for (float v : a) {
            cFloatArrayLEHandle.set(dst, offset, v);
            offset += 4;
        }
    }

    /**
     * Encode an array into the destination byte array, in big-endian format.
     */
    public static void encodeArrayBE(byte[] dst, int offset, float[] a) {
        for (float v : a) {
            cFloatArrayBEHandle.set(dst, offset, v);
            offset += 4;
        }
    }

    /**
     * Encode an array into the destination byte array, in big-endian format, with bit flips.
     */
    public static void encodeArrayKey(byte[] dst, int offset, float[] a) {
        for (float v : a) {
            cIntArrayBEHandle.set(dst, offset, encodeFloatSign(Float.floatToRawIntBits(v)));
            offset += 4;
        }
    }

    /**
     * Encode an array into the destination byte array, in little-endian format.
     */
    public static void encodeArrayLE(byte[] dst, int offset, double[] a) {
        for (double v : a) {
            cDoubleArrayLEHandle.set(dst, offset, v);
            offset += 8;
        }
    }

    /**
     * Encode an array into the destination byte array, in big-endian format.
     */
    public static void encodeArrayBE(byte[] dst, int offset, double[] a) {
        for (double v : a) {
            cDoubleArrayBEHandle.set(dst, offset, v);
            offset += 8;
        }
    }

    /**
     * Encode an array into the destination byte array, in big-endian format, with bit flips.
     */
    public static void encodeArrayKey(byte[] dst, int offset, double[] a) {
        for (double v : a) {
            cLongArrayBEHandle.set(dst, offset, encodeFloatSign(Double.doubleToRawLongBits(v)));
            offset += 8;
        }
    }

    /**
     * Encode an array into the destination byte array, in little-endian format.
     */
    public static void encodeArrayLE(byte[] dst, int offset, char[] a) {
        for (char v : a) {
            cShortArrayLEHandle.set(dst, offset, (short) v);
            offset += 2;
        }
    }

    /**
     * Encode an array into the destination byte array, in big-endian format.
     */
    public static void encodeArrayBE(byte[] dst, int offset, char[] a) {
        for (char v : a) {
            cShortArrayBEHandle.set(dst, offset, (short) v);
            offset += 2;
        }
    }

    /**
     * Encode an array into the destination byte array, in big-endian format, with bit flips.
     */
    public static void encodeArrayKey(byte[] dst, int offset, char[] a) {
        for (char v : a) {
            cShortArrayBEHandle.set(dst, offset, (short) (v ^ (1 << 15)));
            offset += 2;
        }
    }

    /**
     * Decode a short array from the source byte array, in little-endian format.
     */
    public static short[] decodeShortArrayLE(byte[] src, int offset, int length) {
        var a = new short[length >> 1];
        int end = offset + length;
        for (int i=0; offset < end; offset += 2) {
            a[i++] = (short) cShortArrayLEHandle.get(src, offset);
        }
        return a;
    }

    /**
     * Decode a short array from the source byte array, in big-endian format.
     */
    public static short[] decodeShortArrayBE(byte[] src, int offset, int length) {
        var a = new short[length >> 1];
        int end = offset + length;
        for (int i=0; offset < end; offset += 2) {
            a[i++] = (short) cShortArrayBEHandle.get(src, offset);
        }
        return a;
    }

    /**
     * Decode a short array from the source byte array, in big-endian format, with bit flips.
     */
    public static short[] decodeShortArrayKey(byte[] src, int offset, int length) {
        var a = new short[length >> 1];
        int end = offset + length;
        for (int i=0; offset < end; offset += 2) {
            a[i++] = (short) (((short) (cShortArrayBEHandle.get(src, offset))) ^ (1 << 15));
        }
        return a;
    }

    /**
     * Decode an int array from the source byte array, in little-endian format.
     */
    public static int[] decodeIntArrayLE(byte[] src, int offset, int length) {
        var a = new int[length >> 2];
        int end = offset + length;
        for (int i=0; offset < end; offset += 4) {
            a[i++] = (int) cIntArrayLEHandle.get(src, offset);
        }
        return a;
    }

    /**
     * Decode an int array from the source byte array, in big-endian format.
     */
    public static int[] decodeIntArrayBE(byte[] src, int offset, int length) {
        var a = new int[length >> 2];
        int end = offset + length;
        for (int i=0; offset < end; offset += 4) {
            a[i++] = (int) cIntArrayBEHandle.get(src, offset);
        }
        return a;
    }

    /**
     * Decode an int array from the source byte array, in big-endian format, with bit flips.
     */
    public static int[] decodeIntArrayKey(byte[] src, int offset, int length) {
        var a = new int[length >> 2];
        int end = offset + length;
        for (int i=0; offset < end; offset += 4) {
            a[i++] = ((int) cIntArrayBEHandle.get(src, offset)) ^ (1 << 31);
        }
        return a;
    }

    /**
     * Decode a long array from the source byte array, in little-endian format.
     */
    public static long[] decodeLongArrayLE(byte[] src, int offset, int length) {
        var a = new long[length >> 3];
        int end = offset + length;
        for (int i=0; offset < end; offset += 8) {
            a[i++] = (long) cLongArrayLEHandle.get(src, offset);
        }
        return a;
    }

    /**
     * Decode a long array from the source byte array, in big-endian format.
     */
    public static long[] decodeLongArrayBE(byte[] src, int offset, int length) {
        var a = new long[length >> 3];
        int end = offset + length;
        for (int i=0; offset < end; offset += 8) {
            a[i++] = (long) cLongArrayBEHandle.get(src, offset);
        }
        return a;
    }

    /**
     * Decode a long array from the source byte array, in big-endian format, with bit flips.
     */
    public static long[] decodeLongArrayKey(byte[] src, int offset, int length) {
        var a = new long[length >> 3];
        int end = offset + length;
        for (int i=0; offset < end; offset += 8) {
            a[i++] = ((long) cLongArrayBEHandle.get(src, offset)) ^ (1L << 63);
        }
        return a;
    }

    /**
     * Decode a float array from the source byte array, in little-endian format.
     */
    public static float[] decodeFloatArrayLE(byte[] src, int offset, int length) {
        var a = new float[length >> 2];
        int end = offset + length;
        for (int i=0; offset < end; offset += 4) {
            a[i++] = (float) cFloatArrayLEHandle.get(src, offset);
        }
        return a;
    }

    /**
     * Decode a float array from the source byte array, in big-endian format.
     */
    public static float[] decodeFloatArrayBE(byte[] src, int offset, int length) {
        var a = new float[length >> 2];
        int end = offset + length;
        for (int i=0; offset < end; offset += 4) {
            a[i++] = (float) cFloatArrayBEHandle.get(src, offset);
        }
        return a;
    }

    /**
     * Decode a float array from the source byte array, in big-endian format, with bit flips.
     */
    public static float[] decodeFloatArrayKey(byte[] src, int offset, int length) {
        var a = new float[length >> 2];
        int end = offset + length;
        for (int i=0; offset < end; offset += 4) {
            a[i++] = Float.intBitsToFloat
                (decodeFloatSign((int) cIntArrayBEHandle.get(src, offset)));
        }
        return a;
    }

    /**
     * Decode a double array from the source byte array, in little-endian format.
     */
    public static double[] decodeDoubleArrayLE(byte[] src, int offset, int length) {
        var a = new double[length >> 3];
        int end = offset + length;
        for (int i=0; offset < end; offset += 8) {
            a[i++] = (double) cDoubleArrayLEHandle.get(src, offset);
        }
        return a;
    }

    /**
     * Decode a double array from the source byte array, in big-endian format.
     */
    public static double[] decodeDoubleArrayBE(byte[] src, int offset, int length) {
        var a = new double[length >> 3];
        int end = offset + length;
        for (int i=0; offset < end; offset += 8) {
            a[i++] = (double) cDoubleArrayBEHandle.get(src, offset);
        }
        return a;
    }

    /**
     * Decode a double array from the source byte array, in big-endian format, with bit flips.
     */
    public static double[] decodeDoubleArrayKey(byte[] src, int offset, int length) {
        var a = new double[length >> 3];
        int end = offset + length;
        for (int i=0; offset < end; offset += 8) {
            a[i++] = Double.longBitsToDouble
                (decodeFloatSign((long) cLongArrayBEHandle.get(src, offset)));
        }
        return a;
    }

    /**
     * Decode a char array from the source byte array, in little-endian format.
     */
    public static char[] decodeCharArrayLE(byte[] src, int offset, int length) {
        var a = new char[length >> 1];
        int end = offset + length;
        for (int i=0; offset < end; offset += 2) {
            a[i++] = (char) (short) cShortArrayLEHandle.get(src, offset);
        }
        return a;
    }

    /**
     * Decode a char array from the source byte array, in big-endian format.
     */
    public static char[] decodeCharArrayBE(byte[] src, int offset, int length) {
        var a = new char[length >> 1];
        int end = offset + length;
        for (int i=0; offset < end; offset += 2) {
            a[i++] = (char) (short) cShortArrayBEHandle.get(src, offset);
        }
        return a;
    }

    /**
     * Decode a char array from the source byte array, in big-endian format, with bit flips.
     */
    public static char[] decodeCharArrayKey(byte[] src, int offset, int length) {
        var a = new char[length >> 1];
        int end = offset + length;
        for (int i=0; offset < end; offset += 2) {
            a[i++] = (char) (((short) (cShortArrayBEHandle.get(src, offset))) ^ (1 << 15));
        }
        return a;
    }

    /**
     * Flip the high bit for all elements of an array slice.
     */
    public static void signFlip(byte[] a, int off, int len) {
        int end = off + len;
        for (int i=off; i<end; i++) {
            a[i] = (byte) (a[i] ^ (1 << 7));
        }
    }

    /**
     * Flip the high bit for all elements of an array slice.
     */
    public static void signFlip(short[] a, int off, int len) {
        int end = off + len;
        for (int i=off; i<end; i++) {
            a[i] = (short) (a[i] ^ (1 << 15));
        }
    }

    /**
     * Flip the high bit for all elements of an array slice.
     */
    public static void signFlip(int[] a, int off, int len) {
        int end = off + len;
        for (int i=off; i<end; i++) {
            a[i] = a[i] ^ (1 << 31);
        }
    }

    /**
     * Flip the high bit for all elements of an array slice.
     */
    public static void signFlip(long[] a, int off, int len) {
        int end = off + len;
        for (int i=off; i<end; i++) {
            a[i] = a[i] ^ (1L << 63);
        }
    }

    /**
     * Flip the high bit for all elements of an array slice.
     */
    public static void signFlip(char[] a, int off, int len) {
        int end = off + len;
        for (int i=off; i<end; i++) {
            a[i] = (char) (a[i] ^ (1 << 15));
        }
    }

    /**
     * Returns the amount of bytes required to encode a byte array using base-32768 encoding.
     * If the array is null, the length is 1 to encode just a terminator.
     */
    public static int lengthBytes32K(int keyLength) {
        return (((keyLength << 4) + 29) / 15) | 1;
    }

    /**
     * Encodes the given optional unsigned byte array into a variable amount of bytes using
     * base-32768 encoding. The amount written can be determined by calling lengthBytes32K.
     *
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     * @param key byte array key to encode, may be null
     * @return new offset
     */
    public static int encodeBytes32K(byte[] dst, int dstOffset, byte[] key) {
        return encodeBytes32K(dst, dstOffset, key, 0);
    }

    public static int encodeBytes32KDesc(byte[] dst, int dstOffset, byte[] key) {
        return encodeBytes32K(dst, dstOffset, key, -1);
    }

    /**
     * @param xorMask 0 for normal encoding, -1 for descending encoding
     */
    private static int encodeBytes32K(byte[] dst, int dstOffset, byte[] key, int xorMask) {
        if (key == null) {
            dst[dstOffset++] = (byte) (NULL_BYTE_HIGH ^ xorMask);
            return dstOffset;
        }

        int terminator;

        if (key.length == 0) {
            terminator = 1;
        } else {
            int accumBits = 0;
            int accum = 0;

            for (int i=0; i<key.length; i++) {
                if (accumBits <= 7) {
                    accumBits += 8;
                    accum = (accum << 8) | (key[i] & 0xff);
                    if (accumBits == 15) {
                        encodeDigit32K(dst, dstOffset, accum, xorMask);
                        dstOffset += 2;
                        accum = 0;
                        accumBits = 0;
                    }
                } else {
                    int supply = 15 - accumBits;
                    accum = (accum << supply) | ((key[i] & 0xff) >> (8 - supply));
                    encodeDigit32K(dst, dstOffset, accum, xorMask);
                    dstOffset += 2;
                    accumBits = 8 - supply;
                    accum = key[i] & ((1 << accumBits) - 1);
                }
            }

            if (accumBits <= 0) {
                // Terminate with the bit count of the last digit (plus 1).
                terminator = 15 + 1;
            } else {
                // Terminate with the bit count of the last digit (plus 1).
                //assert 1 <= accumBits && accumBits <= 14;
                terminator = accumBits + 1;
            
                // Pad with zeros.
                accum <<= (15 - accumBits);
                encodeDigit32K(dst, dstOffset, accum, xorMask);
                dstOffset += 2;
            }
        }

        dst[dstOffset++] = (byte) (terminator ^ xorMask);

        return dstOffset;
    }

    /**
     * Writes a base-32768 digit using exactly two bytes. The first byte is in the range
     * 32..202 and the second byte is in the range 32..223.
     *
     * @param digit value in the range 0..32767
     * @param xor 0 for normal encoding, -1 for descending encoding
     */
    private static void encodeDigit32K(byte[] dst, int dstOffset, int digit, int xor) {
        // The first byte is computed as ((digit / 192) + 32) and the second byte is computed
        // as ((digit % 192) + 32). To speed things up a bit, the integer division and
        // remainder operations are replaced with a scaled multiplication.

        // divide by 192 (works for dividends up to 32831)
        int a = (digit * 21846) >> 22;

        // Remainder by 192. Note: This was chosen as a divisor because a multiply by 192 can
        // be replaced with two summed shifts.
        int b = digit - ((a << 7) + (a << 6));

        dst[dstOffset]     = (byte) ((a + 32) ^ xor);
        dst[dstOffset + 1] = (byte) ((b + 32) ^ xor);
    }

    /**
     * Returns the number of bytes used to decode a base-32768 array, which is at least one.
     *
     * @param src source of encoded data
     * @param srcOffset offset into encoded data
     */
    public static int lengthBytes32K(byte[] src, int srcOffset) {
        // Look for the terminator.
        final int start = srcOffset;
        while (true) {
            byte b = src[srcOffset++];
            if (-32 <= b && b < 32) {
                return srcOffset - start;
            }
        }
    }

    /**
     * Decodes an encoded base-32768 byte array.
     *
     * @param src source of encoded data
     * @param srcOffset offset into encoded data
     * @param length number of bytes to decode, including the terminator
     */
    public static byte[] decodeBytes32K(byte[] src, int srcOffset, int length) {
        return decodeBytes32K(src, srcOffset, length, 0);
    }

    public static byte[] decodeBytes32KDesc(byte[] src, int srcOffset, int length) {
        return decodeBytes32K(src, srcOffset, length, -1);
    }

    /**
     * @param xorMask 0 for normal encoding, -1 for descending encoding
     */
    private static byte[] decodeBytes32K(byte[] src, int srcOffset, int length, int xorMask) {
        int term = (src[srcOffset + length - 1] ^ xorMask) & 0xff;

        if (length == 1) {
            return term == 1 ? EMPTY_BYTES : null;
        }

        int keyLength = (length * 15 - 29) >> 4;

        if (term > (8 + 1)) {
            keyLength++;
        }

        var key = new byte[keyLength];
        int keyOffset = 0;

        int endOffset = srcOffset + length - 1;

        int accumBits = 0;
        int accum = 0;

        while (true) {
            int a = ((src[srcOffset++] ^ xorMask) & 0xff) - 32;
            int b = ((src[srcOffset++] ^ xorMask) & 0xff) - 32;

            // To a produce digit, multiply a by 192 and add in remainder.
            int d = ((a << 7) + (a << 6)) + b;

            // Shift decoded digit into accumulator.
            accum = (accum << 15) | d;

            // Accumulated 15 bytes, but also pre-decrement for the 8 that will be extracted.
            accumBits += (15 - 8);

            //assert 7 <= accumBits && accumBits <= 14;

            key[keyOffset++] = (byte) (accum >> accumBits);

            if (srcOffset == endOffset) {
                if (term > (8 + 1)) {
                    accumBits -= 8;
                    key[keyOffset++] = (byte) (accum >> accumBits);
                }
                break;
            }

            if (accumBits >= 8) {
                accumBits -= 8;
                key[keyOffset++] = (byte) (accum >> accumBits);
            }

            //assert 0 <= accumBits && accumBits <= 7;
        }

        return key;
    }

    public static String toUnsignedString(byte[] a) {
        if (a == null) {
            return "null";
        }
        var b = new StringBuilder();
        b.append('[');
        for (byte v : a) {
            if (b.length() > 1) {
                b.append(", ");
            }
            b.append(v & 0xff);
        }
        return b.append(']').toString();
    }

    public static String toUnsignedString(short[] a) {
        if (a == null) {
            return "null";
        }
        var b = new StringBuilder();
        b.append('[');
        for (short v : a) {
            if (b.length() > 1) {
                b.append(", ");
            }
            b.append(v & 0xffff);
        }
        return b.append(']').toString();
    }

    public static String toUnsignedString(int[] a) {
        if (a == null) {
            return "null";
        }
        var b = new StringBuilder();
        b.append('[');
        for (int v : a) {
            if (b.length() > 1) {
                b.append(", ");
            }
            b.append(Integer.toUnsignedString(v));
        }
        return b.append(']').toString();
    }

    public static String toUnsignedString(long[] a) {
        if (a == null) {
            return "null";
        }
        var b = new StringBuilder();
        b.append('[');
        for (long v : a) {
            if (b.length() > 1) {
                b.append(", ");
            }
            b.append(Long.toUnsignedString(v));
        }
        return b.append(']').toString();
    }
}
