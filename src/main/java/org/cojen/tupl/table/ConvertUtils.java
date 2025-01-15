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

package org.cojen.tupl.table;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.util.function.Function;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.table.filter.ColumnFilter;

import static org.cojen.tupl.table.ColumnInfo.*;

/**
 * 
 *
 * @author Brian S O'Neill
 * @see Converter
 * @see ConvertCallSite
 */
public class ConvertUtils {
    /**
     * @param toType must be an array type
     * @param lengthVar length of array to create
     * @param elementConverter given an array index, return a converted element
     * @return toType result array
     */
    public static Variable convertArray(MethodMaker mm, Class toType, Variable lengthVar,
                                        Function<Variable, Variable> elementConverter)
    {
        var toArrayVar = mm.new_(toType, lengthVar);
        var ixVar = mm.var(int.class).set(0);
        Label start = mm.label().here();
        Label end = mm.label();
        ixVar.ifGe(lengthVar, end);
        toArrayVar.aset(ixVar, elementConverter.apply(ixVar));
        ixVar.inc(1);
        mm.goto_(start);
        end.here();
        return toArrayVar;
    }

    /**
     * Finds a common type which two columns can be converted to without loss or abiguity. The
     * name of the returned ColumnInfo is undefined (it might be null).
     *
     * @param op defined in ColumnFilter; pass -1 if not performing a comparison operation;
     * pass OP_EQ to use a lenient rule which doesn't care if a number converts to a string
     * @return null if a common type cannot be inferred or is ambiguous
     */
    public static ColumnInfo commonType(ColumnInfo aInfo, ColumnInfo bInfo, int op) {
        int aTypeCode = aInfo.unorderedTypeCode();
        int bTypeCode = bInfo.unorderedTypeCode();

        if (aTypeCode == bTypeCode) {
            return aInfo;
        }

        if (isNullable(aTypeCode) || isNullable(bTypeCode)) {
            // Common type shall be nullable.
            aTypeCode |= TYPE_NULLABLE;
            bTypeCode |= TYPE_NULLABLE;
        }

        if (isArray(aTypeCode) || isArray(bTypeCode)) {
            if (isArray(aTypeCode)) {
                aInfo = aInfo.nonArray();
            }
            if (isArray(bTypeCode)) {
                bInfo = bInfo.nonArray();
            }
            ColumnInfo cInfo = commonType(aInfo, bInfo, op);
            if (cInfo != null) {
                cInfo = cInfo.asArray(ColumnInfo.isNullable(aTypeCode));
            }
            return cInfo;
        }

        // Order aTypeCode to be less than bTypeCode to reduce the number of permutations.
        if (bTypeCode < aTypeCode) {
            int tmp = aTypeCode;
            aTypeCode = bTypeCode;
            bTypeCode = tmp;
        }

        int aPlainCode = plainTypeCode(aTypeCode);
        int bPlainCode = plainTypeCode(bTypeCode);

        final int cPlainCode;

        select: if (aPlainCode == bPlainCode) {
            cPlainCode = aPlainCode;
        } else {
            switch (aPlainCode) {
            case TYPE_UBYTE:
                switch (bPlainCode) {
                case TYPE_USHORT: cPlainCode = TYPE_USHORT; break select;
                case TYPE_UINT: cPlainCode = TYPE_UINT; break select;
                case TYPE_ULONG: cPlainCode = TYPE_ULONG; break select;
                case TYPE_BYTE:
                case TYPE_SHORT: cPlainCode = TYPE_SHORT; break select;
                case TYPE_INT: cPlainCode = TYPE_INT; break select;
                case TYPE_LONG: cPlainCode = TYPE_LONG; break select;
                case TYPE_FLOAT: cPlainCode = TYPE_FLOAT; break select;
                case TYPE_DOUBLE: cPlainCode = TYPE_DOUBLE; break select;
                case TYPE_CHAR: cPlainCode = TYPE_USHORT; break select;
                case TYPE_UTF8: cPlainCode = -1; break select;
                case TYPE_BIG_INTEGER: cPlainCode = TYPE_BIG_INTEGER; break select;
                case TYPE_BIG_DECIMAL: cPlainCode = TYPE_BIG_DECIMAL; break select;
                default: return null;
                }

            case TYPE_USHORT:
                switch (bPlainCode) {
                case TYPE_UINT: cPlainCode = TYPE_UINT; break select;
                case TYPE_ULONG: cPlainCode = TYPE_ULONG; break select;
                case TYPE_BYTE:
                case TYPE_SHORT:
                case TYPE_INT: cPlainCode = TYPE_INT; break select;
                case TYPE_LONG: cPlainCode = TYPE_LONG; break select;
                case TYPE_FLOAT: cPlainCode = TYPE_FLOAT; break select;
                case TYPE_DOUBLE: cPlainCode = TYPE_DOUBLE; break select;
                case TYPE_CHAR: cPlainCode = TYPE_USHORT; break select;
                case TYPE_UTF8: cPlainCode = -1; break select;
                case TYPE_BIG_INTEGER: cPlainCode = TYPE_BIG_INTEGER; break select;
                case TYPE_BIG_DECIMAL: cPlainCode = TYPE_BIG_DECIMAL; break select;
                default: return null;
                }

            case TYPE_UINT:
                switch (bPlainCode) {
                case TYPE_ULONG: cPlainCode = TYPE_ULONG; break select;
                case TYPE_BYTE:
                case TYPE_SHORT:
                case TYPE_INT:
                case TYPE_LONG: cPlainCode = TYPE_LONG; break select;
                case TYPE_FLOAT:
                case TYPE_DOUBLE: cPlainCode = TYPE_DOUBLE; break select;
                case TYPE_CHAR: cPlainCode = TYPE_UINT; break select;
                case TYPE_UTF8: cPlainCode = -1; break select;
                case TYPE_BIG_INTEGER: cPlainCode = TYPE_BIG_INTEGER; break select;
                case TYPE_BIG_DECIMAL: cPlainCode = TYPE_BIG_DECIMAL; break select;
                default: return null;
                }

            case TYPE_ULONG:
                switch (bPlainCode) {
                case TYPE_BYTE:
                case TYPE_SHORT:
                case TYPE_INT:
                case TYPE_LONG: cPlainCode = TYPE_BIG_INTEGER; break select;
                case TYPE_FLOAT:
                case TYPE_DOUBLE: cPlainCode = TYPE_BIG_DECIMAL; break select;
                case TYPE_CHAR: cPlainCode = TYPE_ULONG; break select;
                case TYPE_UTF8: cPlainCode = -1; break select;
                case TYPE_BIG_INTEGER: cPlainCode = TYPE_BIG_INTEGER; break select;
                case TYPE_BIG_DECIMAL: cPlainCode = TYPE_BIG_DECIMAL; break select;
                default: return null;
                }

            case TYPE_BYTE:
                switch (bPlainCode) {
                case TYPE_SHORT: cPlainCode = TYPE_SHORT; break select;
                case TYPE_INT: cPlainCode = TYPE_INT; break select;
                case TYPE_LONG: cPlainCode = TYPE_LONG; break select;
                case TYPE_FLOAT: cPlainCode = TYPE_FLOAT; break select;
                case TYPE_DOUBLE: cPlainCode = TYPE_DOUBLE; break select;
                case TYPE_CHAR: cPlainCode = TYPE_INT; break select;
                case TYPE_UTF8: cPlainCode = -1; break select;
                case TYPE_BIG_INTEGER: cPlainCode = TYPE_BIG_INTEGER; break select;
                case TYPE_BIG_DECIMAL: cPlainCode = TYPE_BIG_DECIMAL; break select;
                default: return null;
                }

            case TYPE_SHORT:
                switch (bPlainCode) {
                case TYPE_INT: cPlainCode = TYPE_INT; break select;
                case TYPE_LONG: cPlainCode = TYPE_LONG; break select;
                case TYPE_FLOAT: cPlainCode = TYPE_FLOAT; break select;
                case TYPE_DOUBLE: cPlainCode = TYPE_DOUBLE; break select;
                case TYPE_CHAR: cPlainCode = TYPE_INT; break select;
                case TYPE_UTF8: cPlainCode = -1; break select;
                case TYPE_BIG_INTEGER: cPlainCode = TYPE_BIG_INTEGER; break select;
                case TYPE_BIG_DECIMAL: cPlainCode = TYPE_BIG_DECIMAL; break select;
                default: return null;
                }

            case TYPE_INT:
                switch (bPlainCode) {
                case TYPE_LONG: cPlainCode = TYPE_LONG; break select;
                case TYPE_FLOAT:
                case TYPE_DOUBLE: cPlainCode = TYPE_DOUBLE; break select;
                case TYPE_CHAR: cPlainCode = TYPE_INT; break select;
                case TYPE_UTF8: cPlainCode = -1; break select;
                case TYPE_BIG_INTEGER: cPlainCode = TYPE_BIG_INTEGER; break select;
                case TYPE_BIG_DECIMAL: cPlainCode = TYPE_BIG_DECIMAL; break select;
                default: return null;
                }

            case TYPE_LONG:
                switch (bPlainCode) {
                case TYPE_FLOAT:
                case TYPE_DOUBLE: cPlainCode = TYPE_BIG_DECIMAL; break select;
                case TYPE_CHAR: cPlainCode = TYPE_LONG; break select;
                case TYPE_UTF8: cPlainCode = -1; break select;
                case TYPE_BIG_INTEGER: cPlainCode = TYPE_BIG_INTEGER; break select;
                case TYPE_BIG_DECIMAL: cPlainCode = TYPE_BIG_DECIMAL; break select;
                default: return null;
                }

            case TYPE_FLOAT:
                switch (bPlainCode) {
                case TYPE_DOUBLE: cPlainCode = TYPE_DOUBLE; break select;
                case TYPE_CHAR: cPlainCode = TYPE_FLOAT; break select;
                case TYPE_UTF8: cPlainCode = -1; break select;
                case TYPE_BIG_INTEGER:
                case TYPE_BIG_DECIMAL: cPlainCode = TYPE_BIG_DECIMAL; break select;
                default: return null;
                }

            case TYPE_DOUBLE:
                switch (bPlainCode) {
                case TYPE_CHAR: cPlainCode = TYPE_DOUBLE; break select;
                case TYPE_UTF8: cPlainCode = -1; break select;
                case TYPE_BIG_INTEGER:
                case TYPE_BIG_DECIMAL: cPlainCode = TYPE_BIG_DECIMAL; break select;
                default: return null;
                }

            case TYPE_CHAR:
                switch (bPlainCode) {
                case TYPE_UTF8: cPlainCode = TYPE_UTF8; break select;
                case TYPE_BIG_INTEGER: cPlainCode = TYPE_BIG_INTEGER; break select;
                case TYPE_BIG_DECIMAL: cPlainCode = TYPE_BIG_DECIMAL; break select;
                default: return null;
                }

            case TYPE_UTF8:
                switch (bPlainCode) {
                case TYPE_BIG_INTEGER:
                case TYPE_BIG_DECIMAL: cPlainCode = -1; break select;
                default: return null;
                }

            case TYPE_BIG_INTEGER:
                switch (bPlainCode) {
                case TYPE_BIG_DECIMAL: cPlainCode = TYPE_BIG_DECIMAL; break select;
                default: return null;
                }

            case TYPE_BIG_DECIMAL:
                switch (bPlainCode) {
                default: return null;
                }

            default: return null;
            }
        }

        int cTypeCode = cPlainCode;

        if (cTypeCode == -1) {
            // Mixed numerical and string comparison is ambiguous if not an exact comparison.
            // What does 5 < "a" mean? Is this a numerical or lexicographical comparison?
            if (!ColumnFilter.isExact(op)) {
                return null;
            }
            cTypeCode = TYPE_UTF8;
        }

        cTypeCode |= (aTypeCode & TYPE_NULLABLE);

        var cInfo = new ColumnInfo();

        cInfo.typeCode = cTypeCode;
        cInfo.assignType();

        return cInfo;
    }

    // Called by generated code.
    public static boolean stringToBooleanExact(String str) {
        if (str.equalsIgnoreCase("false")) {
            return false;
        }
        if (str.equalsIgnoreCase("true")) {
            return true;
        }
        throw new IllegalArgumentException("Cannot convert to Boolean: " + str);
    }

    // Called by generated code.
    public static char stringToCharExact(String str) {
        if (str.length() == 1) {
            return str.charAt(0);
        }
        throw new IllegalArgumentException("Cannot convert to Character: " + str);
    }

    // Called by generated code.
    public static int doubleToIntExact(double d) {
        int i = (int) d;
        if ((double) i != d) {
            throw loss(Integer.class, d);
        }
        return i;
    }

    // Called by generated code.
    public static int doubleToUnsignedIntExact(double d) {
        if (d < 0 || d >= 0x1p32 || Math.floor(d) != d) {
            throw lossUnsigned(Integer.class, d);
        }
        return (int) ((long) d);
    }

    // Called by generated code.
    public static long doubleToLongExact(double d) {
        long i = (long) d;
        if ((double) i != d) {
            throw loss(Long.class, d);
        }
        return i;
    }

    // Called by generated code.
    public static long doubleToUnsignedLongExact(double d) {
        if (d < 0 || d >= 0x1p64 || Math.floor(d) != d) {
            throw lossUnsigned(Long.class, d);
        }
        return d <= 0x1p63 ? ((long) d) : ((((long) (d / 2)) << 1) + ((long) d % 2));
    }

    // Called by generated code.
    public static float doubleToFloatExact(double d) {
        float f = (float) d;
        if ((double) f != d && !Double.isNaN(d)) {
            throw loss(Float.class, d);
        }
        return f;
    }

    // Called by generated code.
    public static byte doubleToByteExact(double d) {
        byte b = (byte) d;
        if ((double) b != d) {
            throw loss(Byte.class, d);
        }
        return b;
    }

    // Called by generated code.
    public static byte doubleToUnsignedByteExact(double d) {
        int i = (int) d;
        if ((double) i != d || Integer.compareUnsigned(i, 256) >= 0) {
            throw lossUnsigned(Byte.class, d);
        }
        return (byte) i;
    }

    // Called by generated code.
    public static short doubleToShortExact(double d) {
        short s = (short) d;
        if ((double) s != d) {
            throw loss(Short.class, d);
        }
        return s;
    }

    // Called by generated code.
    public static short doubleToUnsignedShortExact(double d) {
        int i = (int) d;
        if ((double) i != d || Integer.compareUnsigned(i, 65536) >= 0) {
            throw lossUnsigned(Short.class, d);
        }
        return (short) i;
    }

    // Called by generated code.
    public static int floatToIntExact(float f) {
        int i = (int) f;
        if ((float) i != f) {
            throw loss(Integer.class, f);
        }
        return i;
    }

    // Called by generated code.
    public static int floatToUnsignedIntExact(float f) {
        if (f < 0 || f >= 0x1p32 || Math.floor(f) != f) {
            throw lossUnsigned(Integer.class, f);
        }
        return (int) ((long) f);
    }

    // Called by generated code.
    public static long floatToLongExact(float f) {
        long i = (long) f;
        if ((float) i != f) {
            throw loss(Long.class, f);
        }
        return i;
    }

    // Called by generated code.
    public static long floatToUnsignedLongExact(float f) {
        if (f < 0 || f >= 0x1p64 || Math.floor(f) != f) {
            throw lossUnsigned(Long.class, f);
        }
        return f <= 0x1p63 ? ((long) f) : ((((long) (f / 2)) << 1) + ((long) f % 2));
    }

    // Called by generated code.
    public static byte floatToByteExact(float f) {
        byte b = (byte) f;
        if ((float) b != f) {
            throw loss(Byte.class, f);
        }
        return b;
    }

    // Called by generated code.
    public static byte floatToUnsignedByteExact(float f) {
        int i = (int) f;
        if ((float) i != f || Integer.compareUnsigned(i, 256) >= 0) {
            throw lossUnsigned(Byte.class, f);
        }
        return (byte) i;
    }

    // Called by generated code.
    public static short floatToShortExact(float f) {
        short s = (short) f;
        if ((float) s != f) {
            throw loss(Short.class, f);
        }
        return s;
    }

    // Called by generated code.
    public static short floatToUnsignedShortExact(float f) {
        int i = (int) f;
        if ((float) i != f || Integer.compareUnsigned(i, 65536) >= 0) {
            throw lossUnsigned(Short.class, f);
        }
        return (short) i;
    }

    // Called by generated code.
    public static byte shortToByteExact(short i) {
        byte b = (byte) i;
        if ((short) b != i) {
            throw loss(Byte.class, i);
        }
        return b;
    }

    // Called by generated code.
    public static byte intToByteExact(int i) {
        byte b = (byte) i;
        if ((int) b != i) {
            throw loss(Byte.class, i);
        }
        return b;
    }

    // Called by generated code.
    public static byte intToUnsignedByteExact(int i) {
        if (Integer.compareUnsigned(i, 256) >= 0) {
            throw lossUnsigned(Byte.class, i);
        }
        return (byte) i;
    }

    // Called by generated code.
    public static short intToShortExact(int i) {
        short s = (short) i;
        if ((int) s != i) {
            throw loss(Short.class, i);
        }
        return s;
    }

    // Called by generated code.
    public static short intToUnsignedShortExact(int i) {
        if (Integer.compareUnsigned(i, 65536) >= 0) {
            throw lossUnsigned(Short.class, i);
        }
        return (short) i;
    }

    // Called by generated code.
    public static int intToUnsignedIntExact(int i) {
        if (i < 0) {
            throw lossUnsigned(Integer.class, i);
        }
        return i;
    }

    // Called by generated code.
    public static long intToUnsignedLongExact(int i) {
        if (i < 0) {
            throw lossUnsigned(Long.class, i);
        }
        return i;
    }

    // Called by generated code.
    public static byte unsignedIntToByteExact(int i) {
        if (Integer.compareUnsigned(i, 128) >= 0) {
            throw loss(Byte.class, Integer.toUnsignedString(i));
        }
        return (byte) i;
    }

    // Called by generated code.
    public static byte unsignedIntToUnsignedByteExact(int i) {
        if (Integer.compareUnsigned(i, 256) >= 0) {
            throw lossUnsigned(Byte.class, Integer.toUnsignedString(i));
        }
        return (byte) i;
    }

    // Called by generated code.
    public static short unsignedIntToShortExact(int i) {
        if (Integer.compareUnsigned(i, 32768) >= 0) {
            throw loss(Short.class, Integer.toUnsignedString(i));
        }
        return (short) i;
    }

    // Called by generated code.
    public static short unsignedIntToUnsignedShortExact(int i) {
        if (Integer.compareUnsigned(i, 65536) >= 0) {
            throw lossUnsigned(Short.class, Integer.toUnsignedString(i));
        }
        return (short) i;
    }

    // Called by generated code.
    public static int unsignedIntToIntExact(int i) {
        if (i < 0) {
            throw loss(Integer.class, Integer.toUnsignedString(i));
        }
        return i;
    }

    // Called by generated code.
    public static float intToFloatExact(int i) {
        float f = (float) i;
        if ((int) f != i) {
            throw loss(Float.class, i);
        }
        return f;
    }

    // Called by generated code.
    public static byte longToByteExact(long i) {
        byte b = (byte) i;
        if ((long) b != i) {
            throw loss(Byte.class, i);
        }
        return b;
    }

    // Called by generated code.
    public static byte longToUnsignedByteExact(long i) {
        if (Long.compareUnsigned(i, 256) >= 0) {
            throw lossUnsigned(Byte.class, i);
        }
        return (byte) i;
    }

    // Called by generated code.
    public static short longToShortExact(long i) {
        short s = (short) i;
        if ((long) s != i) {
            throw loss(Short.class, i);
        }
        return s;
    }

    // Called by generated code.
    public static short longToUnsignedShortExact(long i) {
        if (Long.compareUnsigned(i, 65536) >= 0) {
            throw lossUnsigned(Short.class, i);
        }
        return (short) i;
    }

    // Called by generated code.
    public static int longToUnsignedIntExact(long i) {
        if (Long.compareUnsigned(i, 0x1_0000_0000L) >= 0) {
            throw lossUnsigned(Integer.class, i);
        }
        return (int) i;
    }

    // Called by generated code.
    public static long longToUnsignedLongExact(long i) {
        if (i < 0) {
            throw lossUnsigned(Long.class, i);
        }
        return i;
    }

    // Called by generated code.
    public static byte unsignedLongToByteExact(long i) {
        if (Long.compareUnsigned(i, 128) >= 0) {
            throw loss(Byte.class, Long.toUnsignedString(i));
        }
        return (byte) i;
    }

    // Called by generated code.
    public static byte unsignedLongToUnsignedByteExact(long i) {
        if (Long.compareUnsigned(i, 256) >= 0) {
            throw lossUnsigned(Byte.class, Long.toUnsignedString(i));
        }
        return (byte) i;
    }

    // Called by generated code.
    public static short unsignedLongToShortExact(long i) {
        if (Long.compareUnsigned(i, 32768) >= 0) {
            throw loss(Short.class, Long.toUnsignedString(i));
        }
        return (short) i;
    }

    // Called by generated code.
    public static short unsignedLongToUnsignedShortExact(long i) {
        if (Long.compareUnsigned(i, 65536) >= 0) {
            throw lossUnsigned(Short.class, Long.toUnsignedString(i));
        }
        return (short) i;
    }

    // Called by generated code.
    public static int unsignedLongToIntExact(long i) {
        if (Long.compareUnsigned(i, 0x8000_0000L) >= 0) {
            throw loss(Integer.class, Long.toUnsignedString(i));
        }
        return (int) i;
    }

    // Called by generated code.
    public static int unsignedLongToUnsignedIntExact(long i) {
        if (Long.compareUnsigned(i, 0x1_0000_0000L) >= 0) {
            throw lossUnsigned(Integer.class, Long.toUnsignedString(i));
        }
        return (int) i;
    }

    // Called by generated code.
    public static long unsignedLongToLongExact(long i) {
        if (i < 0) {
            throw loss(Long.class, Long.toUnsignedString(i));
        }
        return i;
    }

    // Called by generated code.
    public static float unsignedLongToFloatExact(long i) {
        float f = unsignedLongToBigDecimalExact(i).floatValue();
        if ((long) f != i) {
            throw loss(Float.class, Long.toUnsignedString(i));
        }
        return f;
    }

    // Called by generated code.
    public static double unsignedLongToDoubleExact(long i) {
        double d = unsignedLongToBigDecimalExact(i).doubleValue();
        if ((long) d != i) {
            throw loss(Double.class, Long.toUnsignedString(i));
        }
        return d;
    }

    public static BigInteger unsignedLongToBigIntegerExact(long i) {
        if (i >= 0) {
            return BigInteger.valueOf(i);
        } else {
            var magnitude = new byte[8];
            RowUtils.encodeLongBE(magnitude, 0, i);
            return new BigInteger(1, magnitude);
        }
    }

    public static BigDecimal unsignedLongToBigDecimalExact(long i) {
        if (i >= 0) {
            return BigDecimal.valueOf(i);
        } else {
            var magnitude = new byte[8];
            RowUtils.encodeLongBE(magnitude, 0, i);
            return new BigDecimal(new BigInteger(1, magnitude));
        }
    }

    // Called by generated code.
    public static float longToFloatExact(long i) {
        float f = (float) i;
        if ((long) f != i) {
            throw loss(Float.class, i);
        }
        return f;
    }

    // Called by generated code.
    public static double longToDoubleExact(long i) {
        double d = (double) i;
        if ((long) d != i) {
            throw loss(Double.class, i);
        }
        return d;
    }

    // Called by generated code.
    public static byte biToUnsignedByteExact(BigInteger bi) {
        try {
            int i = bi.intValueExact();
            if (Integer.compareUnsigned(i, 256) < 0) {
                return (byte) i;
            }
        } catch (ArithmeticException e) {
        }
        throw lossUnsigned(Byte.class, bi);
    }

    // Called by generated code.
    public static short biToUnsignedShortExact(BigInteger bi) {
        try {
            int i = bi.intValueExact();
            if (Integer.compareUnsigned(i, 65536) < 0) {
                return (short) i;
            }
        } catch (ArithmeticException e) {
        }
        throw lossUnsigned(Short.class, bi);
    }

    // Called by generated code.
    public static int biToUnsignedIntExact(BigInteger bi) {
        try {
            long i = bi.longValueExact();
            if (Long.compareUnsigned(i, 0x1_0000_0000L) < 0) {
                return (int) i;
            }
        } catch (ArithmeticException e) {
        }
        throw lossUnsigned(Integer.class, bi);
    }

    // Called by generated code.
    public static long biToUnsignedLongExact(BigInteger bi) {
        if (bi.signum() < 0 || bi.bitLength() > 64) {
            throw lossUnsigned(Long.class, bi);
        }
        return bi.longValue();
    }

    // Called by generated code.
    public static float biToFloatExact(BigInteger bi) {
        return bdToFloatExact(new BigDecimal(bi));
    }

    // Called by generated code.
    public static double biToDoubleExact(BigInteger bi) {
        return bdToDoubleExact(new BigDecimal(bi));
    }

    public static byte bdToUnsignedByteExact(BigDecimal bd) {
        try {
            int i = bd.intValueExact();
            if (Integer.compareUnsigned(i, 256) < 0) {
                return (byte) i;
            }
        } catch (ArithmeticException e) {
        }
        throw lossUnsigned(Byte.class, bd);
    }

    public static short bdToUnsignedShortExact(BigDecimal bd) {
        try {
            int i = bd.intValueExact();
            if (Integer.compareUnsigned(i, 65536) < 0) {
                return (short) i;
            }
        } catch (ArithmeticException e) {
        }
        throw lossUnsigned(Short.class, bd);
    }

    public static int bdToUnsignedIntExact(BigDecimal bd) {
        try {
            long i = bd.longValueExact();
            if (Long.compareUnsigned(i, 0x1_0000_0000L) < 0) {
                return (int) i;
            }
        } catch (ArithmeticException e) {
        }
        throw lossUnsigned(Integer.class, bd);
    }

    public static long bdToUnsignedLongExact(BigDecimal bd) {
        try {
            BigInteger bi = bd.toBigIntegerExact();
            if (bi.signum() >= 0 && bi.bitLength() <= 64) {
                return bi.longValue();
            }
        } catch (ArithmeticException e) {
        }
        throw lossUnsigned(Long.class, bd);
    }

    // Called by generated code.
    public static float bdToFloatExact(BigDecimal bd) {
        float f = bd.floatValue();
        if (BigDecimalUtils.valueOf(f).compareTo(bd) != 0) {
            throw loss(Float.class, bd);
        }
        return f;
    }

    // Called by generated code.
    public static double bdToDoubleExact(BigDecimal bd) {
        double d = bd.doubleValue();
        if (BigDecimal.valueOf(d).compareTo(bd) != 0) {
            throw loss(Double.class, bd);
        }
        return d;
    }

    private static ArithmeticException loss(Class to, Object value) {
        return loss("", to, value);
    }

    private static ArithmeticException lossUnsigned(Class to, Object value) {
        return loss("unsigned ", to, value);
    }

    private static ArithmeticException loss(String prefix, Class to, Object value) {
        return new ArithmeticException("Cannot exactly convert to " + prefix + to.getSimpleName()
                                       + ": " + value);
    }
}
