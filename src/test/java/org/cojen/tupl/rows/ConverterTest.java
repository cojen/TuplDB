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

package org.cojen.tupl.rows;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.util.ArrayList;
import java.util.List;

import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import static org.cojen.tupl.rows.ColumnInfo.*;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class ConverterTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ConverterTest.class.getName());
    }

    @Test
    public void allNumericalConversions() throws Throwable {
        List<Type> testTypes = testTypes();
        String[] testNumbers = testNumbers();

        for (var srcType : testTypes) {
            for (var dstType : testTypes) {
                if (dstType == srcType) {
                    continue;
                }

                MethodHandle mh = make(srcType.info, dstType.info);

                for (String testNum : testNumbers) {
                    BigDecimal srcNum = srcType.number(testNum);
                    if (srcNum != null) {
                        Object srcValue = srcType.toValue(srcNum);
                        Object dstValue = mh.invoke(srcValue);
                        dstType.verify(srcType, srcValue, srcNum, dstValue);
                    }
                }
            }
        }
    }

    private static MethodHandle make(ColumnInfo srcInfo, ColumnInfo dstInfo) {
        MethodMaker mm = MethodMaker.begin(MethodHandles.lookup(), dstInfo.type, "_", srcInfo.type);
        if (dstInfo.isAssignableFrom(srcInfo)) {
            mm.return_(mm.param(0));
        } else {
            var dstVar = mm.var(dstInfo.type);
            Converter.convertLossy(mm, srcInfo, mm.param(0), dstInfo, dstVar);
            mm.return_(dstVar);
        }
        return mm.finish();
    }

    private static List<Type> testTypes() {
        List<Type> types = new ArrayList<>(30);
        for (int j=0; j<2; j++) {
            boolean nullable = j > 0;
            types.add(new Type(TYPE_BOOLEAN, nullable, "0", "1", false));
            types.add(new Type(TYPE_UBYTE, nullable, "0", "255", false));
            types.add(new Type(TYPE_USHORT, nullable, "0", "65535", false));
            types.add(new Type(TYPE_UINT, nullable, "0", "4294967295", false));
            types.add(new Type(TYPE_ULONG, nullable, "0", "18446744073709551615", false));
            types.add(new Type(TYPE_BYTE, nullable, "-128", "127", false));
            types.add(new Type(TYPE_SHORT, nullable, "-32768", "32767", false));
            types.add(new Type(TYPE_INT, nullable, "-2147483648", "2147483647", false));
            types.add(new Type(TYPE_LONG, nullable,
                               "-9223372036854775808", "9223372036854775807", false));
            types.add(new Type(TYPE_FLOAT, nullable, null, null, true));
            types.add(new Type(TYPE_DOUBLE, nullable, null, null, true));
            types.add(new Type(TYPE_CHAR, nullable, "0", "65535", false));
            types.add(new Type(TYPE_UTF8, nullable, null, null, true));
            types.add(new Type(TYPE_BIG_INTEGER, nullable, null, null, false));
            types.add(new Type(TYPE_BIG_DECIMAL, nullable, null, null, true));
        }
        return types;
    }

    private static String[] testNumbers() {
        return new String[] {
            "0", "1", "-1", "0.5", "1.5", "-1.5",

            "100", "127", "128", "255", "256", "1000",
            "-100", "-127", "-128", "-255", "-256", "-1000",
            "100.5", "127.5", "128.5", "255.5", "256.5", "1000.5",
            "-100.5", "-127.5", "-128.5", "-255.5", "-256.5", "-1000.5",

            "10000", "32767", "32768", "65535", "65536", "100000",
            "-10000", "-32767", "-32768", "-65535", "-65536", "-100000",
            "10000.5", "32767.5", "32768.5", "65535.5", "65536.5", "100000.5",
            "-10000.5", "-32767.5", "-32768.5", "-65535.5", "-65536.5", "-100000.5",

            "1000000000", "2147483647", "2147483648",
            "4294967295", "42949672956", "10000000000",
            "-1000000000", "-2147483647", "-2147483648",
            "-4294967295", "-42949672956", "-10000000000",
            "1000000000.5", "2147483647.5", "2147483648.5",
            "4294967295.5", "42949672956.5", "10000000000.5",
            "-1000000000.5", "-2147483647.5", "-2147483648.5",
            "-4294967295.5", "-42949672956.5", "-10000000000.5",

            "1000000000000000000", "9223372036854775807", "9223372036854775808",
            "18446744073709551615", "18446744073709551616", "10000000000000000000",
            "-1000000000000000000", "-9223372036854775807", "-9223372036854775808",
            "-18446744073709551615", "-18446744073709551616", "-10000000000000000000",
            "1000000000000000000.5", "9223372036854775807.5", "9223372036854775808.5",
            "18446744073709551615.5", "18446744073709551616.5", "10000000000000000000.5",
            "-1000000000000000000.5", "-9223372036854775807.5", "-9223372036854775808.5",
            "-18446744073709551615.5", "-18446744073709551616.5", "-10000000000000000000.5",
        };
    }

    static class Type {
        final ColumnInfo info;
        final BigDecimal min, max;
        final boolean fraction;

        Type(int typeCode, boolean nullable, String min, String max, boolean fraction) {
            info = new ColumnInfo();
            if (nullable) {
                typeCode |= TYPE_NULLABLE;
            }
            info.typeCode = typeCode;
            info.assignType();

            this.min = min == null ? null : new BigDecimal(min);
            this.max = max == null ? null : new BigDecimal(max);

            this.fraction = fraction;
        }

        /**
         * Return null if not supported for this type, or if out of bounds.
         */
        BigDecimal number(String num) {
            int tc = info.plainTypeCode();

            if (!fraction && num.indexOf('.') >= 0) {
                return null;
            }

            var bd = new BigDecimal(num);
            if (min != null && bd.compareTo(min) < 0) {
                return null;
            }
            if (max != null && bd.compareTo(max) > 0) {
                return null;
            }

            if (tc == TYPE_FLOAT) {
                bd = BigDecimal.valueOf(bd.floatValue());
            } else if (tc == TYPE_DOUBLE) {
                bd = BigDecimal.valueOf(bd.doubleValue());
            }

            return bd;
        }

        Object toValue(BigDecimal bd) {
            switch (info.plainTypeCode()) {
            case TYPE_BOOLEAN:
                check(bd);
                return bd.compareTo(BigDecimal.ONE) >= 0;
            case TYPE_UBYTE:
                check(bd);
                return (byte) bd.intValue();
            case TYPE_USHORT:
                check(bd);
                return (short) bd.intValue();
            case TYPE_CHAR:
                check(bd);
                return (char) bd.intValue();
            case TYPE_UINT:
                check(bd);
                return bd.intValue();
            case TYPE_ULONG:
                check(bd);
                return bd.longValue();
            case TYPE_BYTE:
                return bd.byteValueExact();
            case TYPE_SHORT:
                return bd.shortValueExact();
            case TYPE_INT:
                return bd.intValueExact();
            case TYPE_LONG:
                return bd.longValueExact();
            case TYPE_FLOAT:
                return bd.floatValue();
            case TYPE_DOUBLE:
                return bd.doubleValue();
            case TYPE_BIG_INTEGER:
                return bd.toBigIntegerExact();
            case TYPE_BIG_DECIMAL:
                return bd;
            case TYPE_UTF8:
                return bd.toString();
            default:
                throw new AssertionError();
            }
        }

        BigDecimal clamp(BigDecimal bd) {
            if (min != null && bd.compareTo(min) < 0) {
                return min;
            }
            if (max != null && bd.compareTo(max) > 0) {
                return max;
            }
            if (!fraction) {
                bd = new BigDecimal(bd.toBigInteger());
            }
            return bd;
        }

        /**
         * Verify the conversion against this destination type.
         *
         * @param srcNum cannot be null
         */
        void verify(Type srcType, Object srcValue, BigDecimal srcNum, Object dst) {
            BigDecimal dstNum;
            if (dst == null) {
                dstNum = null;
            } else if (dst instanceof BigDecimal) {
                dstNum = (BigDecimal) dst;
            } else if (dst instanceof String) {
                if (srcType.info.plainTypeCode() == TYPE_CHAR) {
                    dstNum = BigDecimal.valueOf((int) ((String) dst).charAt(0));
                } else if (srcType.info.plainTypeCode() == TYPE_BOOLEAN) {
                    dstNum = null; // special handling below
                } else {
                    dstNum = new BigDecimal((String) dst);
                }
            } else if (dst instanceof BigInteger) {
                dstNum = new BigDecimal((BigInteger) dst);
            } else if (dst instanceof Byte) {
                byte v = (byte) dst;
                dstNum = new BigDecimal(info.isUnsigned() ? (v & 0xff) : v);
            } else if (dst instanceof Short) {
                short v = (short) dst;
                dstNum = new BigDecimal(info.isUnsigned() ? (v & 0xffff) : v);
            } else if (dst instanceof Character) {
                dstNum = new BigDecimal((char) dst);
            } else if (dst instanceof Integer) {
                int v = (int) dst;
                dstNum = new BigDecimal(info.isUnsigned() ? (v & 0xffff_ffffL) : v);
            } else if (dst instanceof Long) {
                long v = (long) dst;
                if (info.isUnsigned()) {
                    BigInteger bi = BigInteger.valueOf(v >>> 32);
                    bi = bi.shiftLeft(32).add(BigInteger.valueOf(v & 0xffff_ffffL));
                    dstNum = new BigDecimal(bi);
                } else {
                    dstNum = new BigDecimal(v);
                }
            } else if (dst instanceof Float) {
                dstNum = new BigDecimal((Float) dst);
            } else if (dst instanceof Double) {
                dstNum = new BigDecimal((Double) dst);
            } else if (dst instanceof Boolean) {
                dstNum = ((boolean) dst) ? BigDecimal.ONE : BigDecimal.ZERO;
            } else {
                throw new AssertionError();
            }

            if (dstNum != null) {
                dstNum = dstNum.stripTrailingZeros();
            }

            BigDecimal expectNum = clamp(srcNum).stripTrailingZeros();

            if (expectNum.equals(dstNum)) {
                // pass
                return;
            }

            switch (info.plainTypeCode()) {
            case TYPE_FLOAT:
                switch (srcType.info.plainTypeCode()) {
                case TYPE_INT:
                    if (((int) srcValue) == (float) dst) {
                        // pass
                        return;
                    }
                    break;
                case TYPE_LONG:
                    if (((long) srcValue) == (float) dst) {
                        // pass
                        return;
                    }
                    break;
                case TYPE_UINT:
                    var bd = new BigDecimal(Integer.toUnsignedString((int) srcValue));
                    if (bd.floatValue() == (float) dst) {
                        // pass
                        return;
                    }
                    break;
                case TYPE_ULONG:
                    bd = new BigDecimal(Long.toUnsignedString((long) srcValue));
                    if (bd.floatValue() == (float) dst) {
                        // pass
                        return;
                    }
                    break;
                default:
                    bd = new BigDecimal(String.valueOf(srcValue));
                    if (bd.floatValue() == (float) dst) {
                        // pass
                        return;
                    }
                    break;
                }
                break;

            case TYPE_DOUBLE:
                switch (srcType.info.plainTypeCode()) {
                case TYPE_FLOAT:
                    if (((float) srcValue) == (double) dst) {
                        // pass
                        return;
                    }
                    break;
                case TYPE_LONG:
                    if (((long) srcValue) == (double) dst) {
                        // pass
                        return;
                    }
                    break;
                case TYPE_ULONG:
                    var bd = new BigDecimal(Long.toUnsignedString((long) srcValue));
                    if (bd.doubleValue() == (double) dst) {
                        // pass
                        return;
                    }
                    break;
                default:
                    bd = new BigDecimal(String.valueOf(srcValue));
                    if (bd.doubleValue() == (double) dst) {
                        // pass
                        return;
                    }
                    break;
                }
                break;

            case TYPE_CHAR:
                switch (srcType.info.plainTypeCode()) {
                case TYPE_UTF8:
                    if (((String) srcValue).charAt(0) == (char) dst) {
                        // pass
                        return;
                    }
                    break;
                case TYPE_BOOLEAN:
                    if ((boolean) srcValue) {
                        if ((char) dst == 't') {
                            // pass
                            return;
                        }
                    } else {
                        if ((char) dst == 'f') {
                            // pass
                            return;
                        }
                    }
                    break;
                }
                break;

            case TYPE_UTF8:
                switch (srcType.info.plainTypeCode()) {
                case TYPE_FLOAT:
                    if ((float) srcValue == new BigDecimal((String) dst).floatValue()) {
                        // pass
                        return;
                    }
                    break;
                case TYPE_BOOLEAN:
                    if ((boolean) srcValue) {
                        if (((String) dst).equals("true")) {
                            // pass
                            return;
                        }
                    } else {
                        if (((String) dst).equals("false")) {
                            // pass
                            return;
                        }
                    }
                    break;
                }
                break;

            case TYPE_BOOLEAN:
                switch (srcType.info.plainTypeCode()) {
                case TYPE_CHAR:
                    switch ((char) srcValue) {
                    case 't':
                        if (dst == Boolean.TRUE) {
                            // pass
                            return;
                        }
                        break;
                    default:
                        if (dst == Boolean.FALSE || (info.isNullable() && dst == null)) {
                            // pass
                            return;
                        }
                        break;
                    }
                    break;
                }
                break;
            }

            // fail
            String msg = "conversion failed: " + srcType + ", " + srcValue + ", " +
                srcNum + " to " + this + ", " + dst + ", " + dstNum;

            fail(msg);
        }

        private void check(BigDecimal bd) {
            if ((min != null && bd.compareTo(min) < 0) || (max != null && bd.compareTo(max) > 0)) {
                throw new ArithmeticException();
            }
        }

        public String toString() {
            var bob = new StringBuilder().append(info.type);

            if (info.isUnsigned()) {
                bob.append(" (unsigned)");
            }
            if (info.isNullable()) {
                bob.append(" (nullable)");
            }

            return bob.toString();
        }
    }
}