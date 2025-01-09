/*
 *  Copyright 2021 Cojen.org
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

package org.cojen.tupl.table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.math.BigInteger;

import java.nio.charset.StandardCharsets;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.cojen.tupl.LockMode;
import org.cojen.tupl.Query;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.core.RowPredicate;
import org.cojen.tupl.core.Utils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RowUtils extends Utils {
    public static final Object[] NO_ARGS = new Object[0];

    /** Byte to use for null, low ordering */
    public static final byte NULL_BYTE_LOW = 0;

    /** Byte to use for null, high ordering */
    public static final byte NULL_BYTE_HIGH = (byte) ~NULL_BYTE_LOW; // 0xff

    /** Byte to use for not-null, high ordering */
    public static final byte NOT_NULL_BYTE_HIGH = (byte) 128; // 0x80

    /** Byte to use for not-null, low ordering */
    public static final byte NOT_NULL_BYTE_LOW = (byte) ~NOT_NULL_BYTE_HIGH; // 0x7f

    /** Byte to terminate variable data encoded for ascending order */
    static final byte TERMINATOR = 1;

    /*
      Method naming convention:

      <op> <type> <format>

      op:     length, encode, decode, skip
      type:   String, etc
      format: UTF, Lex, LexDesc, etc.

      The "Lex" format encodes into a binary format which is lexicographically ordered, and the
      "LexDesc" variant encodes in descending order.
     */

    /**
     * Returns the amount of bytes required to encode a prefix type, which is a 32-bit type
     * restricted to positive values only. "PF" means "prefix format", which uses 1 or 4
     * bytes. It's also lexicographically ordered, although it's unlikely to be used for that.
     */
    public static int lengthPrefixPF(int value) {
        return value < 128 ? 1 : 4;
    }

    /**
     * Encodes the given prefix value. The amount can be determined by calling lengthPrefixPF.
     *
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     * @param value value to encode
     * @return new offset
     */
    public static int encodePrefixPF(byte[] dst, int dstOffset, int value) {
        if (value < 128) {
            dst[dstOffset++] = (byte) value;
        } else {
            encodeIntBE(dst, dstOffset, value | (1 << 31));
            dstOffset += 4;
        }
        return dstOffset;
    }

    public static void encodePrefixPF(DataOutput out, int value) throws IOException {
        if (value < 128) {
            out.writeByte(value);
        } else {
            out.writeInt(value | (1 << 31));
        }
    }

    /**
     * Decodes a prefix value. The amount can be determined by calling lengthPrefixPF.
     *
     * @param src source of encoded data
     * @param srcOffset offset into encoded data
     */
    public static int decodePrefixPF(byte[] src, int srcOffset) {
        int value = src[srcOffset];
        if (value < 0) {
            value = decodeIntBE(src, srcOffset) & ~(1 << 31);
        }
        return value;
    }

    public static int decodePrefixPF(DataInput in) throws IOException {
        int length = in.readByte();

        if (length < 0) {
            length = ((length & 0x7f) << 24)
                | (in.readUnsignedShort() << 8)
                | in.readUnsignedByte();
        }

        return length;
    }

    /**
     * Skips a prefix value and the bytes that follow.
     *
     * @param src source of encoded data
     * @param srcOffset offset into encoded data
     * @return new offset
     */
    public static int skipBytesPF(byte[] src, int srcOffset) {
        int length = src[srcOffset++];
        if (length < 0) {
            length = decodeIntBE(src, srcOffset - 1) & ~(1 << 31);
            srcOffset += 3;
        }
        return srcOffset + length;
    }

    /**
     * Skips a prefix value and the bytes that follow.
     *
     * @param src source of encoded data
     * @param srcOffset offset into encoded data
     * @return new offset
     */
    public static int skipNullableBytesPF(byte[] src, int srcOffset) {
        int length = src[srcOffset++];
        if (length < 0) {
            length = decodeIntBE(src, srcOffset - 1) & ~(1 << 31);
            srcOffset += 3;
        }
        // Actual length is encoded plus one, and zero means null.
        if (length != 0) {
            srcOffset += length - 1;
        }
        return srcOffset;
    }

    /**
     * Returns the amount of bytes required to encode the given String, without any length
     * prefix.
     *
     * @param value non-null String to encode
     */
    public static int lengthStringUTF(String value) {
        int strLength = value.length();
        long encodedLen = strLength;

        for (int i = 0; i < strLength; i++) {
            int c = value.charAt(i);
            if (c >= 0x80) {
                if (c <= 0x7ff) {
                    encodedLen++;
                } else {
                    if (c >= 0xd800 && c <= 0xdbff) {
                        // Found a high surrogate. Verify that surrogate pair is
                        // well-formed. Low surrogate must follow high surrogate.
                        if (i + 1 < strLength) {
                            int c2 = value.charAt(i + 1);
                            if (c2 >= 0xdc00 && c2 <= 0xdfff) {
                                i++;
                            }
                        }
                    }
                    encodedLen += 2;
                }
            }
        }

        return Math.toIntExact(encodedLen);
    }

    /**
     * Encodes the given String into a variable amount of bytes without any length prefix. The
     * amount written can be determined by calling lengthStringUTF.
     *
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     * @param value non-null String value to encode
     * @return new offset
     */
    public static int encodeStringUTF(byte[] dst, int dstOffset, String value) {
        int strLength = value.length();
        for (int i = 0; i < strLength; i++) {
            int c = value.charAt(i);
            if (c <= 0x7f) {
                dst[dstOffset++] = (byte) c;
            } else if (c <= 0x7ff) {
                dst[dstOffset++] = (byte) (0xc0 | (c >> 6));
                dst[dstOffset++] = (byte) (0x80 | (c & 0x3f));
            } else {
                pair: {
                    if (c >= 0xd800 && c <= 0xdbff) {
                        // Found a high surrogate. Verify that surrogate pair is well-formed. Low
                        // surrogate must follow high surrogate.
                        if (i + 1 < strLength) {
                            int c2 = value.charAt(i + 1);
                            if (c2 >= 0xdc00 && c2 <= 0xdfff) {
                                c = 0x10000 + (((c & 0x3ff) << 10) | (c2 & 0x3ff));
                                i++;
                                dst[dstOffset++] = (byte) (0xf0 | (c >> 18));
                                dst[dstOffset++] = (byte) (0x80 | ((c >> 12) & 0x3f));
                                break pair;
                            }
                        }
                    }
                    dst[dstOffset++] = (byte) (0xe0 | (c >> 12));
                }
                dst[dstOffset++] = (byte) (0x80 | ((c >> 6) & 0x3f));
                dst[dstOffset++] = (byte) (0x80 | (c & 0x3f));
            }
        }

        return dstOffset;
    }

    /**
     * Encodes the given optional String into a variable amount of bytes without any length
     * prefix.
     */
    public static byte[] encodeStringUTF(String value) {
        return value == null ? null : value.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Decodes an encoded string from the given byte array.
     *
     * @param src source of encoded data
     * @param srcOffset offset into encoded data
     * @param length number of bytes to decode
     */
    public static String decodeStringUTF(byte[] src, int srcOffset, int length) {
        return utf8(src, srcOffset, length);
    }

    /**
     * Returns the amount of bytes required to encode the given String using "Lex" format.
     *
     * @param str String to encode, may be null
     */
    public static int lengthStringLex(String str) {
        return str == null ? 1 : doLengthStringLex(str);
    }

    private static int doLengthStringLex(String str) {
        int strLength = str.length();
        long encodedLen = 1 + strLength;

        for (int i = 0; i < strLength; i++) {
            int c = str.charAt(i);
            if (c >= (0x80 - 2)) {
                if (c <= (12415 - 2)) {
                    encodedLen++;
                } else {
                    if (c >= 0xd800 && c <= 0xdbff) {
                        // Found a high surrogate. Verify that surrogate pair is
                        // well-formed. Low surrogate must follow high surrogate.
                        if (i + 1 < strLength) {
                            int c2 = str.charAt(i + 1);
                            if (c2 >= 0xdc00 && c2 <= 0xdfff) {
                                i++;
                                encodedLen++;
                                continue;
                            }
                        }
                    }
                    encodedLen += 2;
                }
            }
        }

        return Math.toIntExact(encodedLen);
    }

    /**
     * Encodes the given non-null String into a variable amount of bytes. The amount written
     * can be determined by calling lengthStringLex. If the String is null, caller must encode
     * NULL_BYTE_HIGH or NULL_BYTE_LOW.
     *
     * <p>Strings are encoded in a fashion similar to UTF-8, in that ASCII characters are
     * usually written in one byte. This encoding is more efficient than UTF-8, but it isn't
     * compatible with UTF-8.
     *
     * @param dst destination for encoded bytes
     * @param dstOffset offset into destination array
     * @param str non-null String str to encode
     * @return new offset
     */
    public static int encodeStringLex(byte[] dst, int dstOffset, String str) {
        return encodeStringLex(dst, dstOffset, str, 0);
    }

    public static int encodeStringLexDesc(byte[] dst, int dstOffset, String str) {
        return encodeStringLex(dst, dstOffset, str, -1);
    }

    /**
     * @param xorMask 0 for normal encoding, -1 for descending encoding
     */
    private static int encodeStringLex(byte[] dst, int dstOffset, String str, int xorMask) {
        // All characters have an offset of 2 added, in order to reserve bytes 0 and 1 for
        // encoding nulls and terminators. This means the ASCII string "HelloWorld" is actually
        // encoded as "JgnnqYqtnf". This also means that the ASCII '~' and del characters are
        // encoded in two bytes.

        int length = str.length();
        for (int i = 0; i < length; i++) {
            int c = str.charAt(i) + 2;
            if (c <= 0x7f) {
                // 0xxxxxxx
                dst[dstOffset++] = (byte) (c ^ xorMask);
            } else if (c <= 12415) {
                // 10xxxxxx xxxxxxxx

                // Second byte cannot have the values 0, 1, 254, or 255 because they clash with
                // null and terminator bytes. Divide by 192 and store in the first 6 bits. The
                // remainder, plus 32, goes into the second byte. Note that (192 * 63 + 191) +
                // 128 == 12415. 63 is the maximum value that can be represented in 6 bits.

                c -= 128; // c will always be at least 128, so normalize.

                // divide by 192 (works for dividends up to 32831)
                int a = (c * 21846) >> 22;

                // calculate c % 192
                // Note: the value 192 was chosen as a divisor because a multiply by 192 can be
                // replaced with two summed shifts.
                c = c - ((a << 7) + (a << 6));

                dst[dstOffset++] = (byte) ((0x80 | a) ^ xorMask);
                dst[dstOffset++] = (byte) ((c + 32) ^ xorMask);
            } else {
                // 110xxxxx xxxxxxxx xxxxxxxx

                if ((c - 2) >= 0xd800 && (c - 2) <= 0xdbff) {
                    // Found a high surrogate. Verify that surrogate pair is well-formed. Low
                    // surrogate must follow high surrogate.
                    if (i + 1 < length) {
                        int c2 = str.charAt(i + 1);
                        if (c2 >= 0xdc00 && c2 <= 0xdfff) {
                            c = ((((c - 2) & 0x3ff) << 10) | (c2 & 0x3ff)) + 0x10002;
                            i++;
                        }
                    }
                }

                // Second and third bytes cannot have the values 0, 1, 254, or 255 because they
                // clash with null and terminator bytes. Divide by 192 twice, storing the first
                // and second remainders in the third and second bytes, respectively.  Note
                // that largest unicode value supported is 2^20 + 65535 == 1114111. When
                // divided by 192 twice, the value is 30, which just barely fits in the 5
                // available bits of the first byte.

                c -= 12416; // c will always be at least 12416, so normalize.

                // approximate value / 192
                int a = (int) ((c * 21845L) >> 22);
                c = c - ((a << 7) + (a << 6));
                if (c == 192) {
                    // Fix error.
                    a++;
                    c = 0;
                }

                dst[dstOffset + 2] = (byte) ((c + 32) ^ xorMask);

                // divide by 192 (works for dividends up to 32831, which is at most 10922 here)
                c = (a * 21846) >> 22;
                a = a - ((c << 7) + (c << 6));

                dst[dstOffset++] = (byte) ((0xc0 | c) ^ xorMask);
                dst[dstOffset++] = (byte) ((a + 32) ^ xorMask);
                dstOffset++;
            }
        }

        // Append terminator.
        dst[dstOffset++] = (byte) (TERMINATOR ^ xorMask);

        return dstOffset;
    }

    /**
     * Returns the number of bytes used to decode a string, which is at least one.
     *
     * @param src source of encoded data
     * @param srcOffset offset into encoded data
     */
    public static int lengthStringLex(byte[] src, int srcOffset) {
        // Look for the terminator.
        final int start = srcOffset;
        while (true) {
            byte b = src[srcOffset++];
            if (-2 <= b && b < 2) {
                return srcOffset - start;
            }
        }
    }

    /**
     * Decodes an encoded string from the given byte array.
     *
     * @param src source of encoded data
     * @param srcOffset offset into encoded data
     * @param length number of bytes to decode, including the terminator
     */
    public static String decodeStringLex(byte[] src, int srcOffset, int length) {
        return decodeStringLex(src, srcOffset, length, 0);
    }

    public static String decodeStringLexDesc(byte[] src, int srcOffset, int length) {
        return decodeStringLex(src, srcOffset, length, -1);
    }

    /**
     * @param xorMask 0 for normal encoding, -1 for descending encoding
     */
    private static String decodeStringLex(byte[] src, int srcOffset, int length, int xorMask) {
        if (length <= 1) {
            byte b = src[srcOffset];
            if (b == NULL_BYTE_HIGH || b == NULL_BYTE_LOW) {
                return null;
            }
            return "";
        }

        // Allocate a character array which might be longer than necessary.
        char[] chars = new char[length - 1];
        int charLen = 0;

        int endOffset = srcOffset + length - 1; // stop short of the terminator
        while (srcOffset < endOffset) {
            int c = (src[srcOffset++] ^ xorMask) & 0xff;
            switch (c >> 5) {
                case 0, 1, 2, 3 ->
                    // 0xxxxxxx
                    chars[charLen++] = (char) (c - 2);
                case 4, 5 -> {
                    // 10xxxxxx xxxxxxxx
                    c = c & 0x3f;
                    // Multiply by 192, add in remainder, remove offset of 2, and denormalize.
                    chars[charLen++] =
                        (char) ((c << 7) + (c << 6) + ((src[srcOffset++] ^ xorMask) & 0xff) + 94);
                }
                default -> {
                    // 110xxxxx xxxxxxxx xxxxxxxx

                    c = c & 0x1f;
                    // Multiply by 192, add in remainder...
                    c = (c << 7) + (c << 6) + ((src[srcOffset++] ^ xorMask) & 0xff) - 32;
                    // ...multiply by 192, add in remainder, remove offset of 2, and denormalize.
                    c = (c << 7) + (c << 6) + ((src[srcOffset++] ^ xorMask) & 0xff) + 12382;
                    if (c >= 0x10000) {
                        // Split into surrogate pair.
                        c -= 0x10000;
                        chars[charLen++] = (char) (0xd800 | ((c >> 10) & 0x3ff));
                        chars[charLen++] = (char) (0xdc00 | (c & 0x3ff));
                    } else {
                        chars[charLen++] = (char) c;
                    }
                }
            }
        }

        return new String(chars, 0, charLen);
    }

    /**
     * Encodes the given optional BigInteger into a variable amount of bytes without any length
     * prefix. If the BigInteger is null, then null is returned.
     */
    public static byte[] encodeBigInteger(BigInteger value) {
        return value == null ? null : value.toByteArray();
    }

    /**
     * Encodes the given non-null BigInteger into a variable amount of bytes without any length
     * prefix. If the BigInteger is null, caller must encode NULL_BYTE_HIGH or NULL_BYTE_LOW.
     *
     * @param src BigInteger.toByteArray
     * @return new offset
     */
    public static int encodeBigIntegerLex(byte[] dst, int dstOffset, byte[] src) {
        /* Encoding of first byte:

        0x00:       null low
        0x01:       negative signum; four bytes follow for value length
        0x02..0x7f: negative signum; value length 7e range, 1..126
        0x80..0xfd: positive signum; value length 7e range, 1..126
        0xfe:       positive signum; four bytes follow for value length
        0xff:       null high
        */

        // Length is always at least one.
        int len = src.length;
        byte msb = src[0];

        if (len < 0x7f) {
            dst[dstOffset++] = (byte) (msb < 0 ? (0x80 - len) : (len + 0x7f));
        } else {
            if (msb < 0) {
                dst[dstOffset++] = 1;
                encodeIntBE(dst, dstOffset, -len);
            } else {
                dst[dstOffset++] = (byte) 0xfe;
                encodeIntBE(dst, dstOffset, len);
            }
            dstOffset += 4;
        }

        System.arraycopy(src, 0, dst, dstOffset, len);
        dstOffset += len;

        return dstOffset;
    }

    /**
     * @param src BigInteger.toByteArray
     * @return new offset
     */
    public static int encodeBigIntegerLexDesc(byte[] dst, int dstOffset, byte[] src) {
        /* Encoding of first byte:

        0x00:       null high
        0x01:       positive signum; four bytes follow for value length
        0x02..0x7f: positive signum; value length 7e range, 1..126
        0x80..0xfd: negative signum; value length 7e range, 1..126
        0xfe:       negative signum; four bytes follow for value length
        0xff:       null low
        */

        // Length is always at least one.
        int len = src.length;
        byte msb = src[0];

        if (len < 0x7f) {
            dst[dstOffset++] = (byte) (msb < 0 ? (len + 0x7f) : (0x80 - len));
        } else {
            if (msb < 0) {
                dst[dstOffset++] = (byte) 0xfe;
                encodeIntBE(dst, dstOffset, len);
            } else {
                dst[dstOffset++] = 1;
                encodeIntBE(dst, dstOffset, -len);
            }
            dstOffset += 4;
        }

        for (byte b : src) {
            dst[dstOffset++] = (byte) ~b;
        }

        return dstOffset;
    }

    /**
     * @return new offset in the lower word, and the number of bytes remaining in the upper
     * word; if the number remaining is zero, then the BigInteger is null
     */
    public static long decodeBigIntegerLexHeader(byte[] src, int srcOffset) {
        int header = src[srcOffset++] & 0xff;
        if (header == (NULL_BYTE_HIGH & 0xff) || header == (NULL_BYTE_LOW & 0xff)) {
            return srcOffset;
        } else {
            int len;
            if (header > 1 && header < 0xfe) {
                if (header < 0x80) {
                    len = 0x80 - header;
                } else {
                    len = header - 0x7f;
                }
            } else {
                len = Math.abs(decodeIntBE(src, srcOffset));
                srcOffset += 4;
            }
            return (((long) len) << 32L) | srcOffset;
        }
    }

    /**
     * Flips all the bits.
     */
    public static void flip(byte[] b, int off, int len) {
        int end = off + len;
        for (int i=off; i<end; i++) {
            b[i] = (byte) ~b[i];
        }
    }

    /**
     * @param src source of encoded data
     * @param srcOffset offset into encoded data
     * @return new offset
     */
    public static int skipBigDecimal(byte[] src, int srcOffset) {
        return skipBytesPF(src, (int) (decodeSignedVarInt(src, srcOffset) >> 32));
    }

    /**
     * @param src source of encoded data
     * @param srcOffset offset into encoded data
     * @return new offset
     */
    public static int skipNullableBigDecimal(byte[] src, int srcOffset) {
        if ((src[srcOffset] & 0xff) < 0xf8) {
            srcOffset = skipBigDecimal(src, srcOffset);
        } else {
            srcOffset++;
        }
        return srcOffset;
    }

    /**
     * Decodes the schema version from the first 1 to 4 bytes of the given byte array. When the
     * given byte array is empty, the schema version is zero.
     */
    public static int decodeSchemaVersion(byte[] src) {
        if (src.length == 0) {
            return 0;
        }
        int version = src[0];
        if (version < 0) {
            version = decodeIntBE(src, 0) & ~(1 << 31);
        }
        return version;
    }

    /**
     * Skip the schema version pseudo field, which is encoded at the start of a value.
     *
     * @param src source of encoded data
     * @return offset of 1 or 4
     */
    public static int skipSchemaVersion(byte[] src) {
        return (src[0] >>> 30) + 1;
    }

    public static int encodeFloatSign(int bits) {
        bits ^= (bits < 0 ? 0xffffffff : 0x80000000);
        return bits;
    }

    public static long encodeFloatSign(long bits) {
        bits ^= bits < 0 ? 0xffffffffffffffffL : 0x8000000000000000L;
        return bits;
    }

    public static int encodeFloatSignDesc(int bits) {
        if (bits >= 0) {
            bits ^= 0x7fffffff;
        }
        return bits;
    }

    public static long encodeFloatSignDesc(long bits) {
        if (bits >= 0) {
            bits ^= 0x7fffffffffffffffL;
        }
        return bits;
    }

    public static int decodeFloatSign(int bits) {
        bits ^= bits < 0 ? 0x80000000 : 0xffffffff;
        return bits;
    }

    public static long decodeFloatSign(long bits) {
        bits ^= bits < 0 ? 0x8000000000000000L : 0xffffffffffffffffL;
        return bits;
    }

    public static int decodeFloatSignDesc(int bits) {
        if (bits >= 0) {
            bits ^= 0x7fffffff;
        }
        return bits;
    }

    public static long decodeFloatSignDesc(long bits) {
        if (bits >= 0) {
            bits ^= 0x7fffffffffffffffL;
        }
        return bits;
    }

    /**
     * Returns the bits in a form that can be compared as an int.
     */
    public static int floatToBitsCompare(float v) {
        int bits = Float.floatToRawIntBits(v);
        if (bits < 0) {
            bits ^= 0x7fffffff;
        }
        return bits;
    }

    /**
     * Returns the bits in a form that can be compared as a long.
     */
    public static long floatToBitsCompare(double v) {
        long bits = Double.doubleToRawLongBits(v);
        if (bits < 0) {
            bits ^= 0x7fffffffffffffffL;
        }
        return bits;
    }

    /**
     * Adds one to an unsigned integer, represented as a byte array.
     *
     * @param overflow returned instead if overflowed
     */
    public static byte[] increment(byte[] value, byte[] overflow) {
        if (!increment(value, 0, value.length)) {
            return overflow;
        }
        return value;
    }

    public static IllegalArgumentException nullColumnException(String name) { 
        return new IllegalArgumentException("Cannot be null: " + name);
   }

    public static IllegalArgumentException tooFewArgumentsException(int required, int provided) {
        return new IllegalArgumentException
            ("Not enough arguments provided. Required=" + required + ", provided=" + provided);
    }

    /**
     * Returns true for REPEATABLE_READ and UPGRADABLE_READ.
     */
    public static boolean isRepeatable(Transaction txn) {
        return txn != null && txn.lockMode().isRepeatable();
    }

    /**
     * Returns true for UNSAFE.
     */
    public static boolean isUnsafe(Transaction txn) {
        return txn != null && txn.lockMode() == LockMode.UNSAFE;
    }

    /**
     * Returns true for UNSAFE and READ_UNCOMMITTED.
     */
    public static boolean isUnlocked(Transaction txn) {
        return txn != null && txn.lockMode().noReadLock;
    }

    public static void appendQuotedString(StringBuilder bob, char c) {
        char quote = c != '\'' ? '\'' : '"';
        bob.append(quote).append(c).append(quote);
    }

    public static void appendQuotedString(StringBuilder bob, Character c) {
        if (c == null) {
            bob.append("null");
        } else {
            bob.append((char) c);
        }
    }

    public static void appendQuotedString(StringBuilder bob, String s) {
        if (s == null) {
            bob.append("null");
        } else {
            char quote = '"';
            if (s.indexOf('"') >= 0) {
                if (s.indexOf('\'') < 0) {
                    quote = '\'';
                } else {
                    s = s.replace("\"", "\\\"");
                }
            }
            bob.append(quote).append(s).append(quote);
        }
    }

    public static String scannerToString(Scanner scanner, ScanController<?> controller) {
        var b = new StringBuilder();
        appendMiniString(b, scanner);
        b.append('{');

        RowPredicate<?> predicate = controller.predicate();
        if (predicate == RowPredicate.all()) {
            b.append("unfiltered");
        } else {
            b.append("filter").append(": ").append(predicate);
        }

        return b.append('}').toString();
    }

    public static <R> Stream<R> newStream(Scanner<R> scanner) {
        return StreamSupport.stream(scanner, false).onClose(() -> {
            try {
                scanner.close();
            } catch (Throwable e) {
                Utils.rethrow(e);
            }
        });
    }

    public static <R> long deleteAll(Query<R> query, Transaction txn, Object... args)
        throws IOException
    {
        // Note: If the transaction is null, deleting in batches is an acceptable optimization.

        long total = 0;

        if (txn != null) {
            txn.enter();
        }

        try (var updater = query.newUpdater(txn, args)) {
            for (var row = updater.row(); row != null; row = updater.delete(row)) {
                total++;
            }
            if (txn != null) {
                txn.commit();
            }
        } finally {
            if (txn != null) {
                txn.exit();
            }
        }

        return total;
    }
}
