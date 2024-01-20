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

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class BigDecimalUtils extends RowUtils {
    static final BigInteger ONE_HUNDRED = BigInteger.valueOf(100);
    static final BigInteger ONE_THOUSAND = BigInteger.valueOf(1000);

    /**
     * Encodes the given non-null BigDecimal into a variable amount of bytes. If the
     * BigDecimal is null, caller must encode NULL_BYTE_HIGH or NULL_BYTE_LOW.
     */
    public static byte[] encodeBigDecimalLex(BigDecimal bd) {
        return encodeBigDecimalLex(bd, 0);
    }

    public static byte[] encodeBigDecimalLexDesc(BigDecimal bd) {
        return encodeBigDecimalLex(bd, -1);
    }

    /**
     * @param xor 0 for normal encoding, -1 for descending encoding
     */
    private static byte[] encodeBigDecimalLex(BigDecimal bd, int xor) {
        /* Header encoding:

           0x00:       null low
           0x01:       negative signum; four bytes follow for positive exponent
           0x02..0x3f: negative signum; positive exponent; 3e range, 61..0
           0x40..0x7d: negative signum; negative exponent; 3e range, -1..-62
           0x7e:       negative signum; four bytes follow for negative exponent
           0x7f:       negative zero; four bytes follow for scale
           0x80:       zero; four bytes follow for scale
           0x81:       positive signum; four bytes follow for negative exponent
           0x82..0xbf: positive signum; negative exponent; 3e range, -62..-1
           0xc0..0xfd: positive signum; positive exponent; 3e range, 0..61
           0xfe:       positive signum; four bytes follow for positive exponent
           0xff:       null high
        */

        if (bd.signum() == 0) {
            var bytes = new byte[5];
            bytes[0] = (byte) (0x80 ^ xor);
            encodeIntBE(bytes, 1, bd.scale() ^ 0x80000000 ^ xor);
            return bytes;
        }

        // Significand must be decimal encoded to maintain proper sort order. Base 1000 is
        // more efficient than base 10 and still maintains proper sort order. A minimum of two
        // bytes must be generated, however.

        BigInteger unscaled = bd.unscaledValue();
        int precision = bd.precision();

        // Ensure a non-fractional amount of base 1000 digits.
        int terminator;
        switch (precision % 3) {
        case 0: default:
            terminator = 2;
            break;
        case 1:
            terminator = 0;
            unscaled = unscaled.multiply(ONE_HUNDRED);
            break;
        case 2:
            terminator = 1;
            unscaled = unscaled.multiply(BigInteger.TEN);
            break;
        }

        long exponent = ((long) precision) - (long) bd.scale();

        // 10 bits per digit and 1 extra terminator digit. Digit values 0..999 are encoded as
        // 12..1011. Digit values 0..11 and 1012..1023 are used for terminators.

        int digitAdjust;
        if (unscaled.signum() >= 0) {
            digitAdjust = 12;
        } else {
            digitAdjust = 999 + 12;
            terminator = 1023 - terminator;
        }

        int dpos = ((unscaled.bitLength() + 9) / 10) + 2;
        var digits = new int[dpos];
        digits[--dpos] = terminator;

        while (unscaled.signum() != 0) {
            BigInteger[] divrem = unscaled.divideAndRemainder(ONE_THOUSAND);
            digits[--dpos] = divrem[1].intValue() + digitAdjust;
            unscaled = divrem[0];
        }

        // Calculate the byte[] length and then allocate.

        int bytesLength = 1;
        if (exponent < -0x3e || exponent >= 0x3e) {
            bytesLength += 4;
        }

        int digitsLength = ((digits.length - dpos) * 10 + 7) >>> 3;
        bytesLength += digitsLength;

        var bytes = new byte[bytesLength];
        int bpos;

        // Encode the header.

        if (bd.signum() < 0) {
            if (exponent >= -0x3e && exponent < 0x3e) {
                bytes[0] = (byte) ((0x3f - exponent) ^ xor);
                bpos = 1;
            } else {
                bytes[0] = (byte) ((exponent < 0 ? 0x7e : 1) ^ xor);
                encodeIntBE(bytes, 1, ((int) exponent) ^ 0x7fffffff ^ xor);
                bpos = 5;
            }
        } else {
            if (exponent >= -0x3e && exponent < 0x3e) {
                bytes[0] = (byte) ((exponent + 0xc0) ^ xor);
                bpos = 1;
            } else {
                bytes[0] = (byte) ((exponent < 0 ? 0x81 : 0xfe) ^ xor);
                encodeIntBE(bytes, 1, ((int) exponent) ^ 0x80000000 ^ xor);
                bpos = 5;
            }
        }

        // Encode digits in proper order, 10 bits per digit. 1024 possible values per 10 bits,
        // and so base 1000 is quite efficient.

        int accum = 0;
        int bits = 0;
        for (; dpos < digits.length; dpos++) {
            accum = (accum << 10) | digits[dpos];
            bits += 10;
            do {
                bytes[bpos++] = (byte) ((accum >> (bits -= 8)) ^ xor);
            } while (bits >= 8);
        }

        if (bits != 0) {
            bytes[bpos++] = (byte) ((accum << (8 - bits)) ^ xor);
        }

        assert bpos == bytes.length;

        return bytes;
    }

    /**
     * @param bdRef destination; pass null to skip
     * @return new offset
     */
    public static int decodeBigDecimalLex(byte[] src, int srcOffset, BigDecimal[] bdRef) {
        return decodeBigDecimalLex(src, srcOffset, bdRef, 0);
    }

    /**
     * @param bdRef destination; pass null to skip
     * @return new offset
     */
    public static int decodeBigDecimalLexDesc(byte[] src, int srcOffset, BigDecimal[] bdRef) {
        return decodeBigDecimalLex(src, srcOffset, bdRef, -1);
    }

    /**
     * @param bdRef destination; pass null to skip
     * @param xor 0 for normal decoding, -1 for descending decoding
     * @return new offset
     */
    private static int decodeBigDecimalLex(byte[] src, int srcOffset, BigDecimal[] bdRef, int xor) {
        int exponent = (src[srcOffset++] ^ xor) & 0xff;
        int digitAdjust;

        switch (exponent) {
        case (NULL_BYTE_HIGH & 0xff): case (NULL_BYTE_LOW & 0xff):
            if (bdRef != null) {
                bdRef[0] = null;
            }
            return srcOffset;

        case 0x7f: case 0x80:
            if (bdRef != null) {
                int scale = decodeIntBE(src, srcOffset) ^ 0x80000000 ^ xor;
                BigDecimal bd;
                if (scale == 0) {
                    bd = BigDecimal.ZERO;
                } else {
                    bd = new BigDecimal(BigInteger.ZERO, scale);
                }
                bdRef[0] = bd;
            }
            return srcOffset + 4;

        case 1: case 0x7e:
            digitAdjust = 999 + 12;
            exponent = decodeIntBE(src, srcOffset) ^ 0x7fffffff ^ xor;
            srcOffset += 4;
            break;

        case 0x81: case 0xfe:
            digitAdjust = 12;
            exponent = decodeIntBE(src, srcOffset) ^ 0x80000000 ^ xor;
            srcOffset += 4;
            break;

        default:
            if (exponent >= 0x82) {
                digitAdjust = 12;
                exponent -= 0xc0;
            } else {
                digitAdjust = 999 + 12;
                exponent = 0x3f - exponent;
            }
            break;
        }

        // Significand is base 1000 encoded, 10 bits per digit.

        int accum = 0;
        int bits = 0;

        if (bdRef == null) {
            // Skip it.
            while (true) {
                accum = (accum << 8) | ((src[srcOffset++] ^ xor) & 0xff);
                bits += 8;
                if (bits >= 10) {
                    int digit = (accum >> (bits - 10)) & 0x3ff;
                    if (digit <= 11 || digit >= 1012) {
                        // Terminator.
                        return srcOffset;
                    }
                    bits -= 10;
                }
            }
        }

        BigInteger unscaledValue = null;
        int precision = 0;
        BigInteger lastDigit = null;

        loop: while (true) {
            accum = (accum << 8) | ((src[srcOffset++] ^ xor) & 0xff);
            bits += 8;
            if (bits >= 10) {
                int digit = (accum >> (bits - 10)) & 0x3ff;

                switch (digit) {
                case 0:
                case 1023:
                    lastDigit = lastDigit.divide(ONE_HUNDRED);
                    if (unscaledValue == null) {
                        unscaledValue = lastDigit;
                    } else {
                        unscaledValue = unscaledValue.multiply(BigInteger.TEN).add(lastDigit);
                    }
                    precision += 1;
                    break loop;
                case 1:
                case 1022:
                    lastDigit = lastDigit.divide(BigInteger.TEN);
                    if (unscaledValue == null) {
                        unscaledValue = lastDigit;
                    } else {
                        unscaledValue = unscaledValue.multiply(ONE_HUNDRED).add(lastDigit);
                    }
                    precision += 2;
                    break loop;
                case 2:
                case 1021:
                    if (unscaledValue == null) {
                        unscaledValue = lastDigit;
                    } else {
                        unscaledValue = unscaledValue.multiply(ONE_THOUSAND).add(lastDigit);
                    }
                    precision += 3;
                    break loop;

                default:
                    if (unscaledValue == null) {
                        if ((unscaledValue = lastDigit) != null) {
                            precision += 3;
                        }
                    } else {
                        unscaledValue = unscaledValue.multiply(ONE_THOUSAND).add(lastDigit);
                        precision += 3;
                    }
                    bits -= 10;
                    lastDigit = BigInteger.valueOf(digit - digitAdjust);
                }
            }
        }

        bdRef[0] = new BigDecimal(unscaledValue, precision - exponent);

        return srcOffset;
    }

    /**
     * Performs a more accurate conversion because BigDecimal doesn't currently have a valueOf
     * that accepts a float.
     */
    public static BigDecimal valueOf(float f) {
        return new BigDecimal(Float.toString(f));
    }

    /**
     * Does a more accurate conversion by not adding more scale than exists.
     *
     * @throws NumberFormatException if given value is NaN or infinite
     */
    public static BigDecimal toBigDecimal(float f) {
        if (f == 0) {
            return BigDecimal.ZERO;
        } else if (f == 1) {
            return BigDecimal.ONE;
        } else {
            return toBigDecimal(Float.toString(f));
        }
    }

    /**
     * Does a more accurate conversion by not adding more scale than exists.
     *
     * @throws NumberFormatException if given value is NaN or infinite
     */
    public static BigDecimal toBigDecimal(double d) {
        if (d == 0) {
            return BigDecimal.ZERO;
        } else if (d == 1) {
            return BigDecimal.ONE;
        } else {
            return toBigDecimal(Double.toString(d));
        }
    }

    private static BigDecimal toBigDecimal(String str) {
        if (str.endsWith(".0")) {
            str = str.substring(0, str.length() - 2);
        } else {
            int ix = str.indexOf(".0E");
            if (ix > 0) {
                str = str.substring(0, ix) + str.substring(ix + 2);
            }
        }
        return new BigDecimal(str);
    }

    /**
     * Compares a column to an argument search. If both compare the same but the column has a
     * lower scale, then the column is considered lower. Note that this comparison isn't
     * symmetrical.
     *
     * @param col non-null
     * @param arg non-null
     */
    public static int matches(BigDecimal col, BigDecimal arg) {
        int cmp = col.compareTo(arg);
        if (cmp == 0 && col.scale() < arg.scale()) {
            cmp = -1;
        }
        return cmp;
    }
}
