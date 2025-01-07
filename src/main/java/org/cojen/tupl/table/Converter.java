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

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Type;
import org.cojen.maker.Variable;

import org.cojen.tupl.ConversionException;

import org.cojen.tupl.table.codec.ColumnCodec;

import static org.cojen.tupl.table.ColumnInfo.*;

/**
 * @author Brian S O'Neill
 * @see ConvertCallSite
 */
public class Converter {
    /**
     * Generates code which decodes a column and stores it in dstVar, applying a lossy
     * conversion if necessary.
     *
     * @param srcVar source byte array
     * @param offsetVar int type; is incremented as a side-effect
     * @param endVar end offset, which when null implies the end of the array
     * @see #decodeExact
     */
    static void decodeLossy(final MethodMaker mm,
                            final Variable srcVar, final Variable offsetVar, final Variable endVar,
                            final ColumnCodec srcCodec,
                            final ColumnInfo dstInfo, final Variable dstVar)
    {
        if (dstInfo.type.isAssignableFrom(srcCodec.info.type)
            && (!srcCodec.info.isNullable() || dstInfo.isNullable()))
        {
            srcCodec.decode(dstVar, srcVar, offsetVar, endVar);
        } else {
            // Decode into a temp variable and then perform a best-effort conversion.
            var tempVar = mm.var(srcCodec.info.type);
            srcCodec.decode(tempVar, srcVar, offsetVar, endVar);
            convertLossy(mm, srcCodec.info, tempVar, dstInfo, dstVar);
        }
    }

    /**
     * Generates code which converts a source variable into something that the destination
     * variable can accept, applying a lossy conversion if necessary.
     *
     * The conversion never results in an exception, but data loss is possible. Numerical
     * conversions are clamped to fit within a target range, for example. If a conversion is
     * completely impossible, then a suitable default value is chosen. See setDefault.
     *
     * @see #convertExact
     */
    public static void convertLossy(final MethodMaker mm,
                                    final ColumnInfo srcInfo, final Variable srcVar,
                                    final ColumnInfo dstInfo, final Variable dstVar)
    {
        if (srcInfo.isCompatibleWith(dstInfo)) {
            dstVar.set(srcVar);
            return;
        }

        Label end = mm.label();

        if (srcInfo.isNullable()) {
            Label notNull = mm.label();
            srcVar.ifNe(null, notNull);
            if (dstInfo.isNullable()) {
                dstVar.set(null);
            } else {
                setDefault(mm, dstInfo, dstVar);
            }
            mm.goto_(end);
            notNull.here();
        }

        // Note: At this point, srcVar isn't null.

        if (dstInfo.isArray()) {
            ColumnInfo dstElementInfo = dstInfo.nonArray();

            if (srcInfo.isArray()) {
                // Array to array conversion.

                ColumnInfo srcElementInfo = srcInfo.nonArray();

                dstVar.set(ConvertUtils.convertArray
                           (mm, dstVar.classType(), srcVar.alength(), ixVar -> {
                               Variable dstElementVar = mm.var(dstElementInfo.type);
                               convertLossy(mm, srcElementInfo, srcVar.aget(ixVar),
                                            dstElementInfo, dstElementVar);
                               return dstElementVar;
                           }));

            } else {
                // Non-array to array conversion.

                if (srcInfo.plainTypeCode() == TYPE_UTF8 &&
                    dstElementInfo.plainTypeCode() == TYPE_CHAR)
                {
                    // Special case for String to char[]. Extract all the characters.
                    var lengthVar = srcVar.invoke("length");
                    dstVar.set(mm.new_(dstVar, lengthVar));
                    srcVar.invoke("getChars", 0, lengthVar, dstVar, 0);
                } else {
                    var dstElementVar = mm.var(dstElementInfo.type);
                    convertLossy(mm, srcInfo, srcVar, dstElementInfo, dstElementVar);
                    dstVar.set(mm.new_(dstVar, 1));
                    dstVar.aset(0, dstElementVar);
                }
            }

            end.here();
            return;
        }

        if (srcInfo.isArray()) {
            // Array to non-array conversion.

            ColumnInfo srcElementInfo = srcInfo.nonArray();

            if (srcElementInfo.plainTypeCode() == TYPE_CHAR &&
                dstInfo.plainTypeCode() == TYPE_UTF8)
            {
                // Special case for char[] to String. Copy all the characters.
                dstVar.set(mm.var(String.class).invoke("valueOf", srcVar));
            } else {
                Label notEmpty = mm.label();
                srcVar.alength().ifNe(0, notEmpty);
                setDefault(mm, dstInfo, dstVar);
                mm.goto_(end);
                notEmpty.here();
                convertLossy(mm, srcElementInfo, srcVar.aget(0), dstInfo, dstVar);
            }

            end.here();
            return;
        }

        int srcPlainTypeCode = srcInfo.plainTypeCode();

        if (dstInfo.isAssignableFrom(srcPlainTypeCode)) {
            dstVar.set(srcVar);
            end.here();
            return;
        }

        int dstPlainTypeCode = dstInfo.plainTypeCode();

        boolean handled = true;

        switch (dstPlainTypeCode) {
        case TYPE_BOOLEAN:
            // Note: For numbers, boolean is treated as an int clamped to the range [0, 1].
            switch (srcPlainTypeCode) {
                case TYPE_BYTE, TYPE_SHORT, TYPE_INT, TYPE_LONG -> dstVar.set(srcVar.gt(0));
                case TYPE_UBYTE, TYPE_USHORT, TYPE_UINT, TYPE_ULONG -> dstVar.set(srcVar.ne(0));
                case TYPE_FLOAT -> dstVar.set(srcVar.ge(1.0f));
                case TYPE_DOUBLE -> dstVar.set(srcVar.ge(1.0d));
                case TYPE_BIG_INTEGER ->
                    dstVar.set(srcVar.invoke("compareTo",
                                             mm.var(BigInteger.class).field("ZERO")).gt(0));
                case TYPE_BIG_DECIMAL ->
                    dstVar.set(srcVar.invoke("compareTo",
                                             mm.var(BigDecimal.class).field("ONE")).ge(0));
                case TYPE_CHAR -> {
                    var utils = mm.var(Converter.class);
                    if (dstInfo.type.isPrimitive()) {
                        dstVar.set(utils.invoke("charToBoolean", srcVar));
                    } else {
                        Boolean default_ = dstInfo.isNullable() ? null : false;
                        dstVar.set(utils.invoke("charToBoolean", srcVar, default_));
                    }
                }
                case TYPE_UTF8 -> {
                    var utils = mm.var(Converter.class);
                    if (dstInfo.type.isPrimitive()) {
                        dstVar.set(utils.invoke("stringToBoolean", srcVar));
                    } else {
                        Boolean default_ = dstInfo.isNullable() ? null : false;
                        dstVar.set(utils.invoke("stringToBoolean", srcVar, default_));
                    }
                }
                default -> handled = false;
            }
            break;

        case TYPE_BYTE:
            switch (srcPlainTypeCode) {
                case TYPE_BOOLEAN -> boolToNum(mm, srcVar, dstVar);
                case TYPE_SHORT, TYPE_INT, TYPE_LONG -> clampSS(mm, srcVar, -128, 127, dstVar);
                case TYPE_UBYTE -> clampUS(mm, srcVar, Byte.MAX_VALUE, dstVar);
                case TYPE_USHORT, TYPE_UINT, TYPE_ULONG ->
                    clampUS_narrow(mm, srcVar, Byte.MAX_VALUE, dstVar);
                case TYPE_CHAR ->
                    clampUS_narrow(mm, srcVar.cast(int.class), Byte.MAX_VALUE, dstVar);
                case TYPE_FLOAT, TYPE_DOUBLE ->
                    clampSS(mm, srcVar.cast(int.class), -128, 127, dstVar);
                case TYPE_BIG_INTEGER ->
                    clampBigInteger_narrow(mm, srcVar, Byte.MIN_VALUE, Byte.MAX_VALUE,
                                           dstInfo, dstVar, "byteValue");
                case TYPE_BIG_DECIMAL ->
                    clampBigDecimal_narrow(mm, srcVar, Byte.MIN_VALUE, Byte.MAX_VALUE,
                                           dstInfo, dstVar, "byteValue");
                case TYPE_UTF8 -> {
                    var bd = parseBigDecimal(mm, srcVar, dstInfo, dstVar, end);
                    clampBigDecimal_narrow(mm, bd, Byte.MIN_VALUE, Byte.MAX_VALUE,
                                           dstInfo, dstVar, "byteValue");
                }
                default -> handled = false;
            }
            break;

        case TYPE_SHORT:
            switch (srcPlainTypeCode) {
                case TYPE_BOOLEAN -> boolToNum(mm, srcVar, dstVar);
                case TYPE_INT, TYPE_LONG -> clampSS(mm, srcVar, -32768, 32767, dstVar);
                case TYPE_UBYTE -> dstVar.set(srcVar.cast(int.class).and(0xff).cast(short.class));
                case TYPE_USHORT -> clampUS(mm, srcVar, Short.MAX_VALUE, dstVar);
                case TYPE_UINT, TYPE_ULONG -> clampUS_narrow(mm, srcVar, Short.MAX_VALUE, dstVar);
                case TYPE_CHAR ->
                    clampUS_narrow(mm, srcVar.cast(int.class), Short.MAX_VALUE, dstVar);
                case TYPE_FLOAT, TYPE_DOUBLE ->
                    clampSS(mm, srcVar.cast(int.class), -32768, 32767, dstVar);
                case TYPE_BIG_INTEGER ->
                    clampBigInteger_narrow(mm, srcVar, Short.MIN_VALUE, Short.MAX_VALUE,
                                           dstInfo, dstVar, "shortValue");
                case TYPE_BIG_DECIMAL ->
                    clampBigDecimal_narrow(mm, srcVar, Short.MIN_VALUE, Short.MAX_VALUE,
                                           dstInfo, dstVar, "shortValue");
                case TYPE_UTF8 -> {
                    var bd = parseBigDecimal(mm, srcVar, dstInfo, dstVar, end);
                    clampBigDecimal_narrow(mm, bd, Short.MIN_VALUE, Short.MAX_VALUE,
                                           dstInfo, dstVar, "shortValue");
                }
                default -> handled = false;
            }
            break;

        case TYPE_INT:
            switch (srcPlainTypeCode) {
                case TYPE_BOOLEAN -> boolToNum(mm, srcVar, dstVar);
                case TYPE_LONG -> clampSS(mm, srcVar, Integer.MIN_VALUE, Integer.MAX_VALUE, dstVar);
                case TYPE_UBYTE -> dstVar.set(srcVar.cast(int.class).and(0xff));
                case TYPE_USHORT, TYPE_CHAR -> dstVar.set(srcVar.cast(int.class).and(0xffff));
                case TYPE_UINT -> clampUS(mm, srcVar, Integer.MAX_VALUE, dstVar);
                case TYPE_ULONG -> clampUS_narrow(mm, srcVar, Integer.MAX_VALUE, dstVar);
                case TYPE_FLOAT, TYPE_DOUBLE -> dstVar.set(srcVar.cast(int.class));
                case TYPE_BIG_INTEGER ->
                    clampBigInteger_narrow(mm, srcVar, Integer.MIN_VALUE, Integer.MAX_VALUE,
                                           dstInfo, dstVar, "intValue");
                case TYPE_BIG_DECIMAL ->
                    clampBigDecimal_narrow(mm, srcVar, Integer.MIN_VALUE, Integer.MAX_VALUE,
                                           dstInfo, dstVar, "intValue");
                case TYPE_UTF8 -> {
                    var bd = parseBigDecimal(mm, srcVar, dstInfo, dstVar, end);
                    clampBigDecimal_narrow(mm, bd, Integer.MIN_VALUE, Integer.MAX_VALUE,
                                           dstInfo, dstVar, "intValue");
                }
                default -> handled = false;
            }
            break;

        case TYPE_LONG:
            switch (srcPlainTypeCode) {
                case TYPE_BOOLEAN -> boolToNum(mm, srcVar, dstVar);
                case TYPE_UBYTE -> dstVar.set(srcVar.cast(long.class).and(0xffL));
                case TYPE_USHORT, TYPE_CHAR -> dstVar.set(srcVar.cast(long.class).and(0xffffL));
                case TYPE_UINT -> dstVar.set(srcVar.cast(long.class).and(0xffff_ffffL));
                case TYPE_ULONG -> clampUS(mm, srcVar, Long.MAX_VALUE, dstVar);
                case TYPE_FLOAT, TYPE_DOUBLE -> dstVar.set(srcVar.cast(long.class));
                case TYPE_BIG_INTEGER ->
                    clampBigInteger_narrow(mm, srcVar, Long.MIN_VALUE, Long.MAX_VALUE,
                                           dstInfo, dstVar, "longValue");
                case TYPE_BIG_DECIMAL ->
                    clampBigDecimal_narrow(mm, srcVar, Long.MIN_VALUE, Long.MAX_VALUE,
                                           dstInfo, dstVar, "longValue");
                case TYPE_UTF8 -> {
                    var bd = parseBigDecimal(mm, srcVar, dstInfo, dstVar, end);
                    clampBigDecimal_narrow(mm, bd, Long.MIN_VALUE, Long.MAX_VALUE,
                                           dstInfo, dstVar, "longValue");
                }
                default -> handled = false;
            }
            break;

        case TYPE_UBYTE:
            switch (srcPlainTypeCode) {
                case TYPE_BOOLEAN -> boolToNum(mm, srcVar, dstVar);
                case TYPE_BYTE -> clampSU(mm, srcVar, dstVar);
                case TYPE_SHORT, TYPE_INT, TYPE_LONG -> clampSU_narrow(mm, srcVar, 0xff, dstVar);
                case TYPE_USHORT, TYPE_UINT -> clampUU_narrow(mm, srcVar, 0xff, dstVar);
                case TYPE_CHAR -> clampUU_narrow(mm, srcVar.cast(int.class), 0xff, dstVar);
                case TYPE_ULONG -> clampUU_narrow(mm, srcVar, 0xffL, dstVar);
                case TYPE_FLOAT -> clampSU_narrow(mm, srcVar.cast(int.class), 0xff, dstVar);
                case TYPE_DOUBLE -> clampSU_narrow(mm, srcVar.cast(long.class), 0xffL, dstVar);
                case TYPE_BIG_INTEGER ->
                    clampBigInteger_narrow(mm, srcVar, 0, 0xff, dstInfo, dstVar, "byteValue");
                case TYPE_BIG_DECIMAL ->
                    clampBigDecimal_narrow(mm, srcVar, 0, 0xff, dstInfo, dstVar, "byteValue");
                case TYPE_UTF8 -> {
                    var bd = parseBigDecimal(mm, srcVar, dstInfo, dstVar, end);
                    clampBigDecimal_narrow(mm, bd, 0, 0xff, dstInfo, dstVar, "byteValue");
                }
                default -> handled = false;
            }
            break;

        case TYPE_USHORT:
            switch (srcPlainTypeCode) {
                case TYPE_BOOLEAN -> boolToNum(mm, srcVar, dstVar);
                case TYPE_BYTE, TYPE_SHORT -> clampSU(mm, srcVar, dstVar);
                case TYPE_INT, TYPE_LONG -> clampSU_narrow(mm, srcVar, 0xffff, dstVar);
                case TYPE_UBYTE -> dstVar.set(srcVar.cast(int.class).and(0xff).cast(short.class));
                case TYPE_UINT -> clampUU_narrow(mm, srcVar, 0xffff, dstVar);
                case TYPE_ULONG -> clampUU_narrow(mm, srcVar, 0xffffL, dstVar);
                case TYPE_FLOAT -> clampSU_narrow(mm, srcVar.cast(int.class), 0xffff, dstVar);
                case TYPE_DOUBLE -> clampSU_narrow(mm, srcVar.cast(long.class), 0xffffL, dstVar);
                case TYPE_CHAR -> dstVar.set(srcVar.cast(short.class));
                case TYPE_BIG_INTEGER ->
                    clampBigInteger_narrow(mm, srcVar, 0, 0xffff, dstInfo, dstVar, "shortValue");
                case TYPE_BIG_DECIMAL ->
                    clampBigDecimal_narrow(mm, srcVar, 0, 0xffff, dstInfo, dstVar, "shortValue");
                case TYPE_UTF8 -> {
                    var bd = parseBigDecimal(mm, srcVar, dstInfo, dstVar, end);
                    clampBigDecimal_narrow(mm, bd, 0, 0xffff, dstInfo, dstVar, "shortValue");
                }
                default -> handled = false;
            }
            break;

        case TYPE_UINT:
            switch (srcPlainTypeCode) {
                case TYPE_BOOLEAN -> boolToNum(mm, srcVar, dstVar);
                case TYPE_BYTE, TYPE_SHORT, TYPE_INT -> clampSU(mm, srcVar, dstVar);
                case TYPE_LONG -> clampSU_narrow(mm, srcVar, 0xffff_ffffL, dstVar);
                case TYPE_UBYTE -> dstVar.set(srcVar.cast(int.class).and(0xff));
                case TYPE_USHORT, TYPE_CHAR -> dstVar.set(srcVar.cast(int.class).and(0xffff));
                case TYPE_ULONG -> clampUU_narrow(mm, srcVar, 0xffff_ffffL, dstVar);
                case TYPE_FLOAT, TYPE_DOUBLE ->
                    clampSU_narrow(mm, srcVar.cast(long.class), 0xffff_ffffL, dstVar);
                case TYPE_BIG_INTEGER ->
                    clampBigInteger_narrow(mm, srcVar, 0, 0xffff_ffffL,
                                           dstInfo, dstVar, "intValue");
                case TYPE_BIG_DECIMAL ->
                    clampBigDecimal_narrow(mm, srcVar, 0, 0xffff_ffffL,
                                           dstInfo, dstVar, "intValue");
                case TYPE_UTF8 -> {
                    var bd = parseBigDecimal(mm, srcVar, dstInfo, dstVar, end);
                    clampBigDecimal_narrow(mm, bd, 0, 0xffff_ffffL, dstInfo, dstVar, "intValue");
                }
                default -> handled = false;
            }
            break; 

        case TYPE_ULONG:
            switch (srcPlainTypeCode) {
                case TYPE_BOOLEAN -> boolToNum(mm, srcVar, dstVar);
                case TYPE_BYTE, TYPE_SHORT, TYPE_INT, TYPE_LONG -> clampSU(mm, srcVar, dstVar);
                case TYPE_UBYTE -> dstVar.set(srcVar.cast(long.class).and(0xff));
                case TYPE_USHORT, TYPE_CHAR -> dstVar.set(srcVar.cast(long.class).and(0xffff));
                case TYPE_UINT -> dstVar.set(srcVar.cast(long.class).and(0xffff_ffffL));
                case TYPE_FLOAT, TYPE_DOUBLE ->
                    dstVar.set(mm.var(Converter.class).invoke("doubleToUnsignedLong", srcVar));
                case TYPE_BIG_INTEGER -> clampBigIntegerU_narrow(mm, srcVar, dstInfo, dstVar);
                case TYPE_BIG_DECIMAL -> clampBigDecimalU_narrow(mm, srcVar, dstInfo, dstVar);
                case TYPE_UTF8 -> {
                    var bd = parseBigDecimal(mm, srcVar, dstInfo, dstVar, end);
                    clampBigDecimalU_narrow(mm, bd, dstInfo, dstVar);
                }
                default -> handled = false;
            }
            break; 

        case TYPE_FLOAT:
            switch (srcPlainTypeCode) {
                case TYPE_BOOLEAN -> boolToNum(mm, srcVar, dstVar);
                case TYPE_INT, TYPE_LONG, TYPE_DOUBLE -> dstVar.set(unbox(srcVar).cast(dstVar));
                case TYPE_UBYTE -> dstVar.set(srcVar.cast(int.class).and(0xff).cast(dstVar));
                case TYPE_USHORT, TYPE_CHAR ->
                    dstVar.set(srcVar.cast(int.class).and(0xffff).cast(dstVar));
                case TYPE_UINT ->
                    dstVar.set(srcVar.cast(long.class).and(0xffff_ffffL).cast(dstVar));
                case TYPE_ULONG -> {
                    var bd = mm.var(BigDecimal.class);
                    convert("unsignedLongToBigDecimalExact", srcVar, bd);
                    dstVar.set(bd.invoke("floatValue"));
                }
                case TYPE_BIG_INTEGER, TYPE_BIG_DECIMAL -> dstVar.set(srcVar.invoke("floatValue"));
                case TYPE_UTF8 -> parseNumber(mm, "parseFloat", srcVar, dstInfo, dstVar);
                default -> handled = false;
            }
            break;

        case TYPE_DOUBLE:
            switch (srcPlainTypeCode) {
                case TYPE_BOOLEAN -> boolToNum(mm, srcVar, dstVar);
                case TYPE_LONG -> dstVar.set(unbox(srcVar).cast(dstVar));
                case TYPE_UBYTE -> dstVar.set(srcVar.cast(int.class).and(0xff).cast(dstVar));
                case TYPE_USHORT, TYPE_CHAR ->
                    dstVar.set(srcVar.cast(int.class).and(0xffff).cast(dstVar));
                case TYPE_UINT ->
                    dstVar.set(srcVar.cast(long.class).and(0xffff_ffffL).cast(dstVar));
                case TYPE_ULONG -> {
                    var bd = mm.var(BigDecimal.class);
                    convert("unsignedLongToBigDecimalExact", srcVar, bd);
                    dstVar.set(bd.invoke("doubleValue"));
                }
                case TYPE_BIG_INTEGER, TYPE_BIG_DECIMAL -> dstVar.set(srcVar.invoke("doubleValue"));
                case TYPE_UTF8 -> parseNumber(mm, "parseDouble", srcVar, dstInfo, dstVar);
                default -> handled = false;
            }
            break;

        case TYPE_CHAR:
            switch (srcPlainTypeCode) {
                case TYPE_BOOLEAN -> {
                    Label L1 = mm.label();
                    srcVar.ifTrue(L1);
                    Label cont = mm.label();
                    dstVar.set('f');
                    mm.goto_(cont);
                    L1.here();
                    dstVar.set('t');
                    cont.here();
                }
                case TYPE_BYTE, TYPE_SHORT -> clampSU(mm, srcVar, dstVar);
                case TYPE_INT, TYPE_LONG -> clampSU_narrow(mm, srcVar, 0xffff, '\uffff', dstVar);
                case TYPE_UBYTE -> dstVar.set(srcVar.cast(int.class).and(0xff).cast(char.class));
                case TYPE_USHORT -> dstVar.set(srcVar.cast(char.class));
                case TYPE_UINT -> clampUU_narrow(mm, srcVar, 0xffff, '\uffff', dstVar);
                case TYPE_ULONG -> clampUU_narrow(mm, srcVar, 0xffffL, '\uffff', dstVar);
                case TYPE_FLOAT ->
                    clampSU_narrow(mm, srcVar.cast(int.class), 0xffff, '\uffff', dstVar);
                case TYPE_DOUBLE ->
                    clampSU_narrow(mm, srcVar.cast(long.class), 0xffffL, '\uffff', dstVar);
                case TYPE_BIG_INTEGER ->
                    clampBigInteger_narrow(mm, srcVar, 0, 0xffff, dstInfo, dstVar, "intValue");
                case TYPE_BIG_DECIMAL ->
                    clampBigDecimal_narrow(mm, srcVar, 0, 0xffff, dstInfo, dstVar, "intValue");
                case TYPE_UTF8 -> {
                    Label L1 = mm.label();
                    srcVar.invoke("isEmpty").ifFalse(L1);
                    setDefault(mm, dstInfo, dstVar);
                    Label cont = mm.label().goto_();
                    L1.here();
                    dstVar.set(srcVar.invoke("charAt", 0));
                    cont.here();
                }
                default -> handled = false;
            }
            break;

        case TYPE_UTF8:
            switch (srcPlainTypeCode) {
                case TYPE_UBYTE ->
                    dstVar.set(mm.var(Integer.class).invoke
                               ("toUnsignedString", srcVar.cast(int.class).and(0xff)));
                case TYPE_USHORT ->
                    dstVar.set(mm.var(Integer.class).invoke
                               ("toUnsignedString", srcVar.cast(int.class).and(0xffff)));
                case TYPE_UINT, TYPE_ULONG -> dstVar.set(srcVar.invoke("toUnsignedString", srcVar));
                default -> dstVar.set(mm.var(String.class).invoke("valueOf", srcVar));
            }
            break;

        case TYPE_BIG_INTEGER:
            var bi = mm.var(BigInteger.class);
            switch (srcPlainTypeCode) {
                case TYPE_BOOLEAN -> {
                    Label isFalse = mm.label();
                    srcVar.ifFalse(isFalse);
                    dstVar.set(bi.field("ONE"));
                    mm.goto_(end);
                    isFalse.here();
                    dstVar.set(bi.field("ZERO"));
                }
                case TYPE_BYTE, TYPE_SHORT, TYPE_INT, TYPE_LONG ->
                    dstVar.set(bi.invoke("valueOf", srcVar));
                case TYPE_UBYTE ->
                    dstVar.set(bi.invoke("valueOf", srcVar.cast(long.class).and(0xffL)));
                case TYPE_USHORT, TYPE_CHAR ->
                    dstVar.set(bi.invoke("valueOf", srcVar.cast(long.class).and(0xffffL)));
                case TYPE_UINT ->
                    dstVar.set(bi.invoke("valueOf", srcVar.cast(long.class).and(0xffff_ffffL)));
                case TYPE_ULONG -> convert("unsignedLongToBigIntegerExact", srcVar, dstVar);
                case TYPE_BIG_DECIMAL -> dstVar.set(srcVar.invoke("toBigInteger"));
                case TYPE_FLOAT, TYPE_DOUBLE -> {
                    Label tryStart = mm.label().here();
                    var clazz = srcPlainTypeCode == TYPE_FLOAT
                        ? BigDecimalUtils.class : BigDecimal.class;
                    dstVar.set(mm.var(clazz).invoke("valueOf", srcVar)
                               .invoke("toBigInteger"));
                    mm.catch_(tryStart, NumberFormatException.class, exVar -> {
                        setDefault(mm, dstInfo, dstVar);
                        mm.goto_(end);
                    });
                }
                case TYPE_UTF8 -> {
                    var bd = parseBigDecimal(mm, srcVar, dstInfo, dstVar, end);
                    dstVar.set(bd.invoke("toBigInteger"));
                }
                default -> handled = false;
            }
            break;

        case TYPE_BIG_DECIMAL:
            var bd = mm.var(BigDecimal.class);
            switch (srcPlainTypeCode) {
                case TYPE_BOOLEAN -> {
                    Label isFalse = mm.label();
                    srcVar.ifFalse(isFalse);
                    dstVar.set(bd.field("ONE"));
                    mm.goto_(end);
                    isFalse.here();
                    dstVar.set(bd.field("ZERO"));
                }
                case TYPE_BYTE, TYPE_SHORT, TYPE_INT, TYPE_LONG ->
                    dstVar.set(bd.invoke("valueOf", srcVar));
                case TYPE_FLOAT, TYPE_DOUBLE -> {
                    Label tryStart = mm.label().here();
                    dstVar.set(mm.var(BigDecimalUtils.class).invoke("toBigDecimal", srcVar));
                    mm.catch_(tryStart, NumberFormatException.class, exVar -> {
                        setDefault(mm, dstInfo, dstVar);
                        mm.goto_(end);
                    });
                }
                case TYPE_UBYTE ->
                    dstVar.set(bd.invoke("valueOf", srcVar.cast(long.class).and(0xffL)));
                case TYPE_USHORT, TYPE_CHAR ->
                    dstVar.set(bd.invoke("valueOf", srcVar.cast(long.class).and(0xffffL)));
                case TYPE_UINT ->
                    dstVar.set(bd.invoke("valueOf", srcVar.cast(long.class).and(0xffff_ffffL)));
                case TYPE_ULONG -> convert("unsignedLongToBigDecimalExact", srcVar, dstVar);
                case TYPE_BIG_INTEGER -> dstVar.set(mm.new_(bd, srcVar));
                case TYPE_UTF8 -> parseBigDecimal(mm, srcVar, dstInfo, dstVar, end);
                default -> handled = false;
            }
            break;

        default:
            handled = false;
        }

        if (!handled) {
            setDefault(mm, dstInfo, dstVar);
        }

        end.here();
    }

    /**
     * Generates code which decodes a column and stores it in dstVar, throwing a
     * ConversionException if the conversion would be lossy.
     *
     * @param columnName if provided, include the column name in the conversion exception message
     * @param srcVar source byte array
     * @param offsetVar int type; is incremented as a side-effect
     * @param endVar end offset, which when null implies the end of the array
     * @see #decodeLossy
     */
    static void decodeExact(final MethodMaker mm, final String columnName,
                            final Variable srcVar, final Variable offsetVar, final Variable endVar,
                            final ColumnCodec srcCodec,
                            final ColumnInfo dstInfo, final Variable dstVar)
    {
        if (dstInfo.type.isAssignableFrom(srcCodec.info.type)
            && (!srcCodec.info.isNullable() || dstInfo.isNullable()))
        {
            srcCodec.decode(dstVar, srcVar, offsetVar, endVar);
        } else {
            // Decode into a temp variable and then perform an exact conversion.
            var tempVar = mm.var(srcCodec.info.type);
            srcCodec.decode(tempVar, srcVar, offsetVar, endVar);
            convertExact(mm, columnName, srcCodec.info, tempVar, dstInfo, dstVar);
        }
    }

    /**
     * Generates code which converts a source variable into something that the destination
     * variable can accept, throwing a ConversionException if the conversion would be lossy.
     *
     * @param columnName if provided, include the column name in the conversion exception message
     * @see #convertLossy
     */
    public static void convertExact(final MethodMaker mm, final String columnName,
                                    final ColumnInfo srcInfo, final Variable srcVar,
                                    final ColumnInfo dstInfo, final Variable dstVar)
    {
        if (srcInfo.isCompatibleWith(dstInfo)) {
            dstVar.set(srcVar);
            return;
        }

        Label tryStart = mm.label().here();
        boolean canThrow = doConvertExact(mm, columnName, srcInfo, srcVar, dstInfo, dstVar);

        if (canThrow) {
            Label tryEnd = mm.label().here();
            Label done = mm.label().goto_();
            var exVar = mm.catch_(tryStart, tryEnd, RuntimeException.class);
            mm.var(Converter.class).invoke("failed", columnName, exVar).throw_();
            done.here();
        }
    }

    /**
     * @return true if a runtime exception is possible which isn't already a
     * ConversionException
     */
    private static boolean doConvertExact(final MethodMaker mm, final String columnName,
                                          final ColumnInfo srcInfo, final Variable srcVar,
                                          final ColumnInfo dstInfo, final Variable dstVar)
    {
        Label end = mm.label();

        if (srcInfo.isNullable()) {
            Label notNull = mm.label();
            srcVar.ifNe(null, notNull);
            if (dstInfo.isNullable()) {
                dstVar.set(null);
            } else {
                mm.new_(ConversionException.class,
                        "Cannot assign null to non-nullable type", columnName).throw_();
            }
            mm.goto_(end);
            notNull.here();
        }

        // Note: At this point, srcVar isn't null.

        if (dstInfo.isArray()) {
            ColumnInfo dstElementInfo = dstInfo.nonArray();

            if (srcInfo.isArray()) {
                // Array to array conversion.

                ColumnInfo srcElementInfo = srcInfo.nonArray();

                dstVar.set(ConvertUtils.convertArray
                           (mm, dstVar.classType(), srcVar.alength(), ixVar -> {
                               Variable dstElementVar = mm.var(dstElementInfo.type);
                               convertExact(mm, columnName, srcElementInfo, srcVar.aget(ixVar),
                                            dstElementInfo, dstElementVar);
                               return dstElementVar;
                           }));

                end.here();
                return true; // could be smarter, but this is safe
            } else {
                // Non-array to array conversion.
                throwConvertFailException(mm, columnName, srcInfo, dstInfo);
                end.here();
                return false;
            }
        }

        if (srcInfo.isArray()) {
            // Array to non-array conversion.
            throwConvertFailException(mm, columnName, srcInfo, dstInfo);
            end.here();
            return false;
        }

        int srcPlainTypeCode = srcInfo.plainTypeCode();

        if (dstInfo.isAssignableFrom(srcPlainTypeCode)) {
            dstVar.set(srcVar);
            end.here();
            return false;
        }

        int dstPlainTypeCode = dstInfo.plainTypeCode();

        boolean canThrow = true;
        boolean handled = true;

        switch (dstPlainTypeCode) {
        case TYPE_BOOLEAN:
            switch (srcPlainTypeCode) {
                case TYPE_UTF8 -> convert("stringToBooleanExact", srcVar, dstVar);
                default -> handled = false;
            }
            break;

        case TYPE_BYTE:
            switch (srcPlainTypeCode) {
                case TYPE_SHORT -> convert("shortToByteExact", srcVar, dstVar);
                case TYPE_INT -> convert("intToByteExact", srcVar, dstVar);
                case TYPE_LONG -> convert("longToByteExact", srcVar, dstVar);
                case TYPE_UBYTE ->
                    convert("unsignedIntToByteExact", srcVar.cast(int.class).and(0xff), dstVar);
                case TYPE_USHORT ->
                    convert("unsignedIntToByteExact", srcVar.cast(int.class).and(0xffff), dstVar);
                case TYPE_UINT -> convert("unsignedIntToByteExact", srcVar, dstVar);
                case TYPE_ULONG -> convert("unsignedLongToByteExact", srcVar, dstVar);
                case TYPE_FLOAT -> convert("floatToByteExact", srcVar, dstVar);
                case TYPE_DOUBLE -> convert("doubleToByteExact", srcVar, dstVar);
                case TYPE_BIG_INTEGER, TYPE_BIG_DECIMAL ->
                    dstVar.set(srcVar.invoke("byteValueExact"));
                case TYPE_UTF8 -> dstVar.set(mm.var(Byte.class).invoke("parseByte", srcVar));
                default -> handled = false;
            }
            break;

        case TYPE_SHORT:
            switch (srcPlainTypeCode) {
                case TYPE_INT -> convert("intToShortExact", srcVar, dstVar);
                case TYPE_LONG -> convert("longToShortExact", srcVar, dstVar);
                case TYPE_UBYTE -> {
                    canThrow = false;
                    dstVar.set(srcVar.cast(int.class).and(0xff).cast(short.class));
                }
                case TYPE_USHORT ->
                    convert("unsignedIntToShortExact", srcVar.cast(int.class).and(0xffff), dstVar);
                case TYPE_UINT -> convert("unsignedIntToShortExact", srcVar, dstVar);
                case TYPE_ULONG -> convert("unsignedLongToShortExact", srcVar, dstVar);
                case TYPE_FLOAT -> convert("floatToShortExact", srcVar, dstVar);
                case TYPE_DOUBLE -> convert("doubleToShortExact", srcVar, dstVar);
                case TYPE_BIG_INTEGER, TYPE_BIG_DECIMAL ->
                    dstVar.set(srcVar.invoke("shortValueExact"));
                case TYPE_UTF8 -> dstVar.set(mm.var(Short.class).invoke("parseShort", srcVar));
                default -> handled = false;
            }
            break;

        case TYPE_INT:
            switch (srcPlainTypeCode) {
                case TYPE_LONG -> dstVar.set(mm.var(Math.class).invoke("toIntExact", srcVar));
                case TYPE_UBYTE -> {
                    canThrow = false;
                    dstVar.set(srcVar.cast(int.class).and(0xff));
                }
                case TYPE_USHORT -> {
                    canThrow = false;
                    dstVar.set(srcVar.cast(int.class).and(0xffff));
                }
                case TYPE_UINT -> convert("unsignedIntToIntExact", srcVar, dstVar);
                case TYPE_ULONG -> convert("unsignedLongToIntExact", srcVar, dstVar);
                case TYPE_FLOAT -> convert("floatToIntExact", srcVar, dstVar);
                case TYPE_DOUBLE -> convert("doubleToIntExact", srcVar, dstVar);
                case TYPE_BIG_INTEGER, TYPE_BIG_DECIMAL ->
                    dstVar.set(srcVar.invoke("intValueExact"));
                case TYPE_UTF8 -> dstVar.set(mm.var(Integer.class).invoke("parseInt", srcVar));
                default -> handled = false;
            }
            break;

        case TYPE_LONG:
            switch (srcPlainTypeCode) {
                case TYPE_UBYTE -> {
                    canThrow = false;
                    dstVar.set(srcVar.cast(long.class).and(0xffL));
                }
                case TYPE_USHORT-> {
                    canThrow = false;
                    dstVar.set(srcVar.cast(long.class).and(0xffffL));
                }
                case TYPE_UINT -> {
                    canThrow = false;
                    dstVar.set(srcVar.cast(long.class).and(0xffff_ffffL));
                }
                case TYPE_ULONG -> convert("unsignedLongToLongExact", srcVar, dstVar);
                case TYPE_FLOAT -> convert("floatToLongExact", srcVar, dstVar);
                case TYPE_DOUBLE -> convert("doubleToLongExact", srcVar, dstVar);
                case TYPE_BIG_INTEGER, TYPE_BIG_DECIMAL ->
                    dstVar.set(srcVar.invoke("longValueExact"));
                case TYPE_UTF8 -> dstVar.set(mm.var(Long.class).invoke("parseLong", srcVar));
                default -> handled = false;
            }
            break;

        case TYPE_UBYTE:
            switch (srcPlainTypeCode) {
                case TYPE_BYTE, TYPE_SHORT, TYPE_INT ->
                    convert("intToUnsignedByteExact", srcVar, dstVar);
                case TYPE_LONG -> convert("longToUnsignedByteExact", srcVar, dstVar);
                case TYPE_USHORT ->
                    convert("unsignedIntToUnsignedByteExact",
                            srcVar.cast(int.class).and(0xffff), dstVar);
                case TYPE_UINT -> convert("unsignedIntToUnsignedByteExact", srcVar, dstVar);
                case TYPE_ULONG -> convert("unsignedLongToUnsignedByteExact", srcVar, dstVar);
                case TYPE_FLOAT -> convert("floatToUnsignedByteExact", srcVar, dstVar);
                case TYPE_DOUBLE -> convert("doubleToUnsignedByteExact", srcVar, dstVar);
                case TYPE_BIG_INTEGER -> convert("biToUnsignedByteExact", srcVar, dstVar);
                case TYPE_BIG_DECIMAL -> convert("bdToUnsignedByteExact", srcVar, dstVar);
                case TYPE_UTF8 -> {
                    var intVar = mm.var(Integer.class).invoke("parseUnsignedInt", srcVar, 10);
                    convert("intToUnsignedByteExact", intVar, dstVar);
                }
                default -> handled = false;
            }
            break;

        case TYPE_USHORT:
            switch (srcPlainTypeCode) {
                case TYPE_BYTE, TYPE_SHORT, TYPE_INT ->
                    convert("intToUnsignedShortExact", srcVar, dstVar);
                case TYPE_LONG -> convert("longToUnsignedShortExact", srcVar, dstVar);
                case TYPE_UBYTE -> {
                    canThrow = false;
                    dstVar.set(srcVar.cast(int.class).and(0xff).cast(short.class));
                }
                case TYPE_UINT -> convert("unsignedIntToUnsignedShortExact", srcVar, dstVar);
                case TYPE_ULONG -> convert("unsignedLongToUnsignedShortExact", srcVar, dstVar);
                case TYPE_FLOAT -> convert("floatToUnsignedShortExact", srcVar, dstVar);
                case TYPE_DOUBLE -> convert("doubleToUnsignedShortExact", srcVar, dstVar);
                case TYPE_BIG_INTEGER -> convert("biToUnsignedShortExact", srcVar, dstVar);
                case TYPE_BIG_DECIMAL -> convert("bdToUnsignedShortExact", srcVar, dstVar);
                case TYPE_UTF8 -> {
                    var intVar = mm.var(Integer.class).invoke("parseUnsignedInt", srcVar, 10);
                    convert("intToUnsignedShortExact", intVar, dstVar);
                }
                default -> handled = false;
            }
            break;

        case TYPE_UINT:
            switch (srcPlainTypeCode) {
                case TYPE_BYTE, TYPE_SHORT, TYPE_INT ->
                    convert("intToUnsignedIntExact", srcVar, dstVar);
                case TYPE_LONG -> convert("longToUnsignedIntExact", srcVar, dstVar);
                case TYPE_UBYTE -> {
                    canThrow = false;
                    dstVar.set(srcVar.cast(int.class).and(0xff));
                }
                case TYPE_USHORT -> {
                    canThrow = false;
                    dstVar.set(srcVar.cast(int.class).and(0xffff));
                }
                case TYPE_ULONG -> convert("unsignedLongToUnsignedIntExact", srcVar, dstVar);
                case TYPE_FLOAT -> convert("floatToUnsignedIntExact", srcVar, dstVar);
                case TYPE_DOUBLE -> convert("doubleToUnsignedIntExact", srcVar, dstVar);
                case TYPE_BIG_INTEGER -> convert("biToUnsignedIntExact", srcVar, dstVar);
                case TYPE_BIG_DECIMAL -> convert("bdToUnsignedIntExact", srcVar, dstVar);
                case TYPE_UTF8 -> dstVar.set
                    (mm.var(Integer.class).invoke("parseUnsignedInt", srcVar, 10));
                default -> handled = false;
            }
            break; 

        case TYPE_ULONG:
            switch (srcPlainTypeCode) {
                case TYPE_BYTE, TYPE_SHORT, TYPE_INT ->
                    convert("intToUnsignedLongExact", srcVar, dstVar);
                case TYPE_LONG -> convert("longToUnsignedLongExact", srcVar, dstVar);
                case TYPE_UBYTE -> {
                    canThrow = false;
                    dstVar.set(srcVar.cast(long.class).and(0xff));
                }
                case TYPE_USHORT-> {
                    canThrow = false;
                    dstVar.set(srcVar.cast(long.class).and(0xffff));
                }
                case TYPE_UINT -> {
                    canThrow = false;
                    dstVar.set(srcVar.cast(long.class).and(0xffff_ffffL));
                }
                case TYPE_FLOAT -> convert("floatToUnsignedLongExact", srcVar, dstVar);
                case TYPE_DOUBLE -> convert("doubleToUnsignedLongExact", srcVar, dstVar);
                case TYPE_BIG_INTEGER -> convert("biToUnsignedLongExact", srcVar, dstVar);
                case TYPE_BIG_DECIMAL -> convert("bdToUnsignedLongExact", srcVar, dstVar);
                case TYPE_UTF8 -> dstVar.set
                    (mm.var(Long.class).invoke("parseUnsignedLong", srcVar, 10));
                default -> handled = false;
            }
            break; 

        case TYPE_FLOAT:
            switch (srcPlainTypeCode) {
                case TYPE_INT -> convert("intToFloatExact", srcVar, dstVar);
                case TYPE_LONG -> convert("longToFloatExact", srcVar, dstVar);
                case TYPE_DOUBLE -> convert("doubleToFloatExact", srcVar, dstVar);
                case TYPE_UBYTE -> dstVar.set(srcVar.cast(int.class).and(0xff).cast(dstVar));
                case TYPE_USHORT -> {
                    canThrow = false;
                    dstVar.set(srcVar.cast(int.class).and(0xffff).cast(dstVar));
                }
                case TYPE_UINT ->
                    convert("longToFloatExact", srcVar.cast(long.class).and(0xffff_ffffL), dstVar);
                case TYPE_ULONG -> convert("unsignedLongToFloatExact", srcVar, dstVar);
                case TYPE_BIG_INTEGER -> convert("biToFloatExact", srcVar, dstVar);
                case TYPE_BIG_DECIMAL -> convert("bdToFloatExact", srcVar, dstVar);
                case TYPE_UTF8 -> dstVar.set(mm.var(Float.class).invoke("parseFloat", srcVar));
                default -> handled = false;
            }
            break;

        case TYPE_DOUBLE:
            switch (srcPlainTypeCode) {
                case TYPE_LONG -> convert("longToDoubleExact", srcVar, dstVar);
                case TYPE_UBYTE -> {
                    canThrow = false;
                    dstVar.set(srcVar.cast(int.class).and(0xff).cast(dstVar));
                }
                case TYPE_USHORT -> {
                    canThrow = false;
                    dstVar.set(srcVar.cast(int.class).and(0xffff).cast(dstVar));
                }
                case TYPE_UINT -> {
                    canThrow = false;
                    dstVar.set(srcVar.cast(long.class).and(0xffff_ffffL).cast(dstVar));
                }
                case TYPE_ULONG -> convert("unsignedLongToDoubleExact", srcVar, dstVar);
                case TYPE_BIG_INTEGER -> convert("biToDoubleExact", srcVar, dstVar);
                case TYPE_BIG_DECIMAL -> convert("bdToDoubleExact", srcVar, dstVar);
                case TYPE_UTF8 -> dstVar.set(mm.var(Double.class).invoke("parseDouble", srcVar));
                default -> handled = false;
            }
            break;

        case TYPE_CHAR:
            switch (srcPlainTypeCode) {
                case TYPE_UTF8 -> convert("stringToCharExact", srcVar, dstVar);
                default -> handled = false;
            }
            break;

        case TYPE_UTF8:
            canThrow = false;
            switch (srcPlainTypeCode) {
                case TYPE_UBYTE ->
                    dstVar.set(mm.var(Integer.class).invoke
                               ("toUnsignedString", srcVar.cast(int.class).and(0xff)));
                case TYPE_USHORT ->
                    dstVar.set(mm.var(Integer.class).invoke
                               ("toUnsignedString", srcVar.cast(int.class).and(0xffff)));
                case TYPE_UINT, TYPE_ULONG -> dstVar.set(srcVar.invoke("toUnsignedString", srcVar));
                default -> dstVar.set(mm.var(String.class).invoke("valueOf", srcVar));
            }
            break;

        case TYPE_BIG_INTEGER:
            var bi = mm.var(BigInteger.class);
            switch (srcPlainTypeCode) {
                case TYPE_BYTE, TYPE_SHORT, TYPE_INT, TYPE_LONG -> {
                    canThrow = false;
                    dstVar.set(bi.invoke("valueOf", srcVar));
                }
                case TYPE_UBYTE -> {
                    canThrow = false;
                    dstVar.set(bi.invoke("valueOf", srcVar.cast(long.class).and(0xffL)));
                }
                case TYPE_USHORT -> {
                    canThrow = false;
                    dstVar.set(bi.invoke("valueOf", srcVar.cast(long.class).and(0xffffL)));
                }
                case TYPE_UINT -> {
                    canThrow = false;
                    dstVar.set(bi.invoke("valueOf", srcVar.cast(long.class).and(0xffff_ffffL)));
                }
                case TYPE_ULONG -> {
                    canThrow = false;
                    convert("unsignedLongToBigIntegerExact", srcVar, dstVar);
                }
                case TYPE_BIG_DECIMAL -> dstVar.set(srcVar.invoke("toBigIntegerExact"));
                case TYPE_FLOAT ->
                    dstVar.set(mm.var(BigDecimalUtils.class).invoke("valueOf", srcVar)
                               .invoke("toBigIntegerExact"));
                case TYPE_DOUBLE ->
                    dstVar.set(mm.var(BigDecimal.class).invoke("valueOf", srcVar)
                               .invoke("toBigIntegerExact"));
                case TYPE_UTF8 -> dstVar.set(mm.new_(bi, srcVar));
                default -> handled = false;
            }
            break;

        case TYPE_BIG_DECIMAL:
            canThrow = false;
            var bd = mm.var(BigDecimal.class);
            switch (srcPlainTypeCode) {
                case TYPE_BYTE, TYPE_SHORT, TYPE_INT, TYPE_LONG ->
                    dstVar.set(bd.invoke("valueOf", srcVar));
                case TYPE_FLOAT, TYPE_DOUBLE ->
                    dstVar.set(mm.var(BigDecimalUtils.class).invoke("toBigDecimal", srcVar));
                case TYPE_UBYTE ->
                    dstVar.set(bd.invoke("valueOf", srcVar.cast(long.class).and(0xffL)));
                case TYPE_USHORT ->
                    dstVar.set(bd.invoke("valueOf", srcVar.cast(long.class).and(0xffffL)));
                case TYPE_UINT ->
                    dstVar.set(bd.invoke("valueOf", srcVar.cast(long.class).and(0xffff_ffffL)));
                case TYPE_ULONG -> convert("unsignedLongToBigDecimalExact", srcVar, dstVar);
                case TYPE_BIG_INTEGER -> dstVar.set(mm.new_(bd, srcVar));
                case TYPE_UTF8 -> {
                    canThrow = true;
                    dstVar.set(mm.new_(bd, srcVar));
                }
                default -> handled = false;
            }
            break;

        case TYPE_REFERENCE:
            if (dstVar.classType().isAssignableFrom(srcVar.classType())) {
                dstVar.set(srcVar);
                break;
            }

        default:
            handled = false;
        }

        if (!handled) {
            throwConvertFailException(mm, columnName, srcInfo, dstInfo);
        }

        end.here();

        return canThrow;
    }

    /**
     * Assigns a default value to the variable: null, 0, false, etc.
     */
    public static void setDefault(MethodMaker mm, ColumnInfo dstInfo, Variable dstVar) {
        if (dstInfo.isNullable()) {
            dstVar.set(null);
        } else if (dstInfo.isArray()) {
            dstVar.set(mm.new_(dstVar.classType(), 0));
        } else {
            switch (dstInfo.plainTypeCode()) {
                case TYPE_BOOLEAN -> dstVar.set(false);
                case TYPE_UTF8 -> dstVar.set("");
                case TYPE_BIG_INTEGER, TYPE_BIG_DECIMAL -> dstVar.set(dstVar.field("ZERO"));
                default -> dstVar.set(0);
            }
        }
    }

    /**
     * Performs a check to see if the given variable is a default value. Returns a boolean
     * Variable with the result.
     *
     * @param nullable pass true to indicate that default is null (unless the value is primitive)
     */
    public static Variable isDefault(final Variable v, final boolean nullable) {
        final Type type = v.type();

        if (type.isPrimitive()) {
            return v.eq(type.classType() == boolean.class ? false : 0);
        }

        if (nullable) {
            return v.eq(null);
        }

        final MethodMaker mm = v.methodMaker();
        final Label done = mm.label();
        final var resultVar = mm.var(boolean.class);

        Label notNull = mm.label();
        v.ifNe(null, notNull);
        resultVar.set(true);
        done.goto_();

        notNull.here();

        final Type unboxed = type.unbox();

        if (unboxed != null) {
            resultVar.set(v.unbox().eq(unboxed.classType() == boolean.class ? false : 0));
        } else if (type.isArray()) {
            resultVar.set(v.alength().eq(0));
        } else {
            Class<?> clazz = type.classType();
            if (CharSequence.class.isAssignableFrom(clazz)) {
                resultVar.set(v.invoke("isEmpty"));
            } else if (BigInteger.class.isAssignableFrom(clazz)) {
                resultVar.set(v.field("ZERO").invoke("equals", v));
            } else if (BigDecimal.class.isAssignableFrom(clazz)) {
                resultVar.set(v.field("ZERO").invoke("compareTo", v).eq(0));
            } else {
                resultVar.set(false);
            }
        }

        done.here();
        return resultVar;
    }

    private static void throwConvertFailException(MethodMaker mm, String columnName,
                                                  ColumnInfo srcInfo, ColumnInfo dstInfo)
    {
        var messageVar = mm.var(String.class);
        messageVar.set("Cannot convert " + srcInfo.typeName() + " to " + dstInfo.typeName());
        mm.new_(ConversionException.class, messageVar, columnName).throw_();
    }

    private static void convert(String methodName, Variable srcVar, Variable dstVar) {
        MethodMaker mm = srcVar.methodMaker();
        dstVar.set(mm.var(ConvertUtils.class).invoke(methodName, srcVar));
    }

    /**
     * Stores 0 or 1 into a primitive numerical variable.
     */
    private static void boolToNum(MethodMaker mm, Variable srcVar, Variable dstVar) {
        Label isFalse = mm.label();
        srcVar.ifFalse(isFalse);
        dstVar.set(1);
        Label cont = mm.label().goto_();
        isFalse.here();
        dstVar.set(0);
        cont.here();
    }

    private static void parseNumber(MethodMaker mm, String method,
                                    Variable srcVar, ColumnInfo dstInfo, Variable dstVar)
    {
        Label tryStart = mm.label().here();
        dstVar.set(dstVar.invoke(method, srcVar));
        mm.catch_(tryStart, NumberFormatException.class, ex -> setDefault(mm, dstInfo, dstVar));
    }

    /**
     * @param srcVar short, int, or long (signed)
     * @param dstVar byte, short, or int (signed)
     */
    private static void clampSS(MethodMaker mm, Variable srcVar, int min, int max,
                                Variable dstVar)
    {
        srcVar = unbox(srcVar);
        Label cont = mm.label();
        Label L1 = mm.label();
        srcVar.ifGt(min, L1);
        dstVar.set(min);
        mm.goto_(cont);
        L1.here();
        Label L2 = mm.label();
        srcVar.ifLe(max, L2);
        dstVar.set(max);
        mm.goto_(cont);
        L2.here();
        dstVar.set(srcVar.cast(dstVar));
        cont.here();
    }

    /**
     * The src and dst types must be the same size.
     *
     * @param srcVar byte, short, int, or long (unsigned)
     * @param dstVar byte, short, int, or long (signed)
     */
    private static void clampUS(MethodMaker mm, Variable srcVar, long max, Variable dstVar) {
        srcVar = unbox(srcVar);
        Label cont = mm.label();
        Label L1 = mm.label();
        srcVar.ifGe(0, L1);
        dstVar.set(max);
        mm.goto_(cont);
        L1.here();
        dstVar.set(srcVar.cast(dstVar));
        cont.here();
    }

    /**
     * The dst type is expected to be smaller than the src type.
     *
     * @param srcVar short, int, or long (unsigned)
     * @param dstVar byte, short, or int (signed)
     */
    private static void clampUS_narrow(MethodMaker mm, Variable srcVar, int max, Variable dstVar) {
        srcVar = unbox(srcVar);
        Label cont = mm.label();
        Label L1 = mm.label();
        srcVar.and(~max).ifEq(0, L1);
        dstVar.set(max);
        mm.goto_(cont);
        L1.here();
        dstVar.set(srcVar.cast(dstVar));
        cont.here();
    }

    /**
     * The dst type is expected to be smaller than the src type.
     *
     * @param srcVar short or int (unsigned)
     * @param dstVar byte or short (unsigned)
     */
    private static void clampUU_narrow(MethodMaker mm, Variable srcVar, int max, Variable dstVar) {
        clampUU_narrow(mm, srcVar, max, -1, dstVar);
    }

    private static void clampUU_narrow(MethodMaker mm, Variable srcVar, int max, Object clampMax,
                                       Variable dstVar)
    {
        srcVar = unbox(srcVar);
        Label cont = mm.label();
        Label L1 = mm.label();
        srcVar.and(~max).ifEq(0, L1);
        dstVar.set(clampMax);
        mm.goto_(cont);
        L1.here();
        dstVar.set(srcVar.and(max).cast(dstVar));
        cont.here();
    }

    /**
     * The dst type is expected to be smaller than the src type.
     *
     * @param srcVar long (unsigned)
     * @param dstVar byte, short, or int (unsigned)
     */
    private static void clampUU_narrow(MethodMaker mm, Variable srcVar, long max, Variable dstVar) {
        clampUU_narrow(mm, srcVar, max, -1L, dstVar);
    }

    /**
     * The dst type is expected to be smaller than the src type.
     *
     * @param srcVar long (unsigned)
     * @param dstVar byte, short, or int (unsigned)
     */
    private static void clampUU_narrow(MethodMaker mm, Variable srcVar, long max, Object clampMax,
                                       Variable dstVar)
    {
        srcVar = unbox(srcVar);
        Label cont = mm.label();
        Label L1 = mm.label();
        srcVar.and(~max).ifEq(0, L1);
        dstVar.set(clampMax);
        mm.goto_(cont);
        L1.here();
        dstVar.set(srcVar.and(max).cast(dstVar));
        cont.here();
    }

    /**
     * The dst type must not be smaller than the src type.
     *
     * @param srcVar byte, short, int, or long (signed)
     * @param dstVar byte, short, int, or long (unsigned)
     */
    private static void clampSU(MethodMaker mm, Variable srcVar, Variable dstVar) {
        srcVar = unbox(srcVar);
        Label cont = mm.label();
        Label L1 = mm.label();
        srcVar.ifGe(0, L1);
        dstVar.set(0);
        mm.goto_(cont);
        L1.here();
        dstVar.set(srcVar.cast(dstVar));
        cont.here();
    }

    /**
     * The dst type is expected to be smaller than the src type.
     *
     * @param srcVar short or int (signed)
     * @param dstVar byte, short, or int (unsigned)
     */
    private static void clampSU_narrow(MethodMaker mm, Variable srcVar, int max, Variable dstVar) {
        clampSU_narrow(mm, srcVar, max, -1, dstVar);
    }

    private static void clampSU_narrow(MethodMaker mm, Variable srcVar, int max, Object clampMax,
                                       Variable dstVar)
    {
        srcVar = unbox(srcVar);
        Label cont = mm.label();
        Label L1 = mm.label();
        srcVar.ifGt(0, L1);
        dstVar.set(0);
        mm.goto_(cont);
        L1.here();
        Label L2 = mm.label();
        srcVar.ifLe(max, L2);
        dstVar.set(clampMax);
        mm.goto_(cont);
        L2.here();
        dstVar.set(srcVar.cast(dstVar));
        cont.here();
    }

    /**
     * The dst type is expected to be smaller than the src type.
     *
     * @param srcVar long (signed)
     * @param dstVar byte, short, or int (unsigned)
     */
    private static void clampSU_narrow(MethodMaker mm, Variable srcVar, long max, Variable dstVar) {
        clampSU_narrow(mm, srcVar, max, -1L, dstVar);
    }

    private static void clampSU_narrow(MethodMaker mm, Variable srcVar, long max, Object clampMax,
                                       Variable dstVar)
    {
        srcVar = unbox(srcVar);
        Label cont = mm.label();
        Label L1 = mm.label();
        srcVar.ifGt(0, L1);
        dstVar.set(0);
        mm.goto_(cont);
        L1.here();
        Label L2 = mm.label();
        srcVar.ifLe(max, L2);
        dstVar.set(clampMax);
        mm.goto_(cont);
        L2.here();
        dstVar.set(srcVar.cast(dstVar));
        cont.here();
    }

    /**
     * @param srcVar BigInteger
     * @param dstVar byte, short, int, char, or long (signed)
     */
    private static void clampBigInteger_narrow(MethodMaker mm, Variable srcVar,
                                               long min, long max,
                                               ColumnInfo dstInfo, Variable dstVar, String method)
    {
        clampBig_narrow(mm, srcVar,
                        min, BigInteger.valueOf(min), max, BigInteger.valueOf(max),
                        dstInfo, dstVar, method);
    }

    /**
     * @param srcVar BigDecimal
     * @param dstVar byte, short, int, or long (signed)
     */
    private static void clampBigDecimal_narrow(MethodMaker mm, Variable srcVar,
                                               long min, long max,
                                               ColumnInfo dstInfo, Variable dstVar, String method)
    {
        clampBig_narrow(mm, srcVar,
                        min, BigDecimal.valueOf(min), max, BigDecimal.valueOf(max),
                        dstInfo, dstVar, method);
    }

    /**
     * @param srcVar BigInteger
     * @param dstVar long (unsigned)
     */
    private static void clampBigIntegerU_narrow(MethodMaker mm, Variable srcVar,
                                                ColumnInfo dstInfo, Variable dstVar)
    {
        clampBig_narrow(mm, srcVar,
                        0, BigInteger.ZERO, -1, new BigInteger(Long.toUnsignedString(-1)),
                        dstInfo, dstVar, "longValue");
    }

    /**
     * @param srcVar BigDecimal
     * @param dstVar long (unsigned)
     */
    private static void clampBigDecimalU_narrow(MethodMaker mm, Variable srcVar,
                                                ColumnInfo dstInfo, Variable dstVar)
    {
        clampBig_narrow(mm, srcVar,
                        0, BigDecimal.ZERO, -1, new BigDecimal(Long.toUnsignedString(-1)),
                        dstInfo, dstVar, "longValue");
    }

    /**
     * @param srcVar BigInteger or BigDecimal
     * @param dstVar byte, short, int, char, or long (signed)
     */
    private static void clampBig_narrow(MethodMaker mm, Variable srcVar,
                                        long min, Number minObj, long max, Number maxObj,
                                        ColumnInfo dstInfo, Variable dstVar, String method)
    {
        Label cont = mm.label();
        Label L1 = mm.label();
        srcVar.invoke("compareTo", mm.var(srcVar).setExact(minObj)).ifGt(0, L1);
        dstVar.set(min);
        mm.goto_(cont);
        L1.here();
        Label L2 = mm.label();
        srcVar.invoke("compareTo", mm.var(srcVar).setExact(maxObj)).ifLe(0, L2);

        switch (dstInfo.plainTypeCode()) {
            case TYPE_UBYTE, TYPE_BYTE -> dstVar.set((byte) max);
            case TYPE_USHORT, TYPE_SHORT -> dstVar.set((short) max);
            case TYPE_UINT, TYPE_INT -> dstVar.set((int) max);
            default -> dstVar.set(max);
        }

        mm.goto_(cont);
        L2.here();

        var v = srcVar.invoke(method);
        if (dstInfo.plainTypeCode() == TYPE_CHAR) {
            v = v.cast(char.class);
        }
        dstVar.set(v);

        cont.here();
    }

    /**
     * If parse fails, sets a default and jumps to the end.
     *
     * @param srcVar String
     * @return BigDecimal variable, or null if dstInfo is BigDecimal and was set here
     */
    private static Variable parseBigDecimal(MethodMaker mm, Variable srcVar,
                                            ColumnInfo dstInfo, Variable dstVar, Label end)
    {
        Label tryStart = mm.label().here();
        var bd = mm.new_(BigDecimal.class, srcVar);
        if (dstInfo.type == BigDecimal.class) {
            dstVar.set(bd);
            bd = null;
        }
        mm.catch_(tryStart, NumberFormatException.class, exVar -> {
            setDefault(mm, dstInfo, dstVar);
            mm.goto_(end);
        });
        return bd;
    }

    private static Variable unbox(Variable v) {
        return v.classType().isPrimitive() ? v : v.unbox();
    }

    // Called by generated code.
    public static boolean charToBoolean(char c) {
        // Note: For numbers, boolean is treated as an int clamped to the range [0, 1].
        return switch (c) {
            case 0, '0', 'f', 'F' -> false;
            case 1, '1', 't', 'T' -> true;
            default -> c <= 9 || ('1' < c && c <= '9');
        };
    }

    // Called by generated code.
    public static Boolean charToBoolean(char c, Boolean default_) {
        // Note: For numbers, boolean is treated as an int clamped to the range [0, 1].
        switch (c) {
        case 0: case '0': case 'f': case 'F':
            return false;
        case 1: case '1': case 't': case 'T':
            return true;
        }
        if (c <= 9 || ('1' < c && c <= '9')) {
            return true;
        }
        return default_;
    }

    // Called by generated code.
    public static boolean stringToBoolean(String str) {
        // Note: For numbers, boolean is treated as an int clamped to the range [0, 1].
        if (str.equalsIgnoreCase("false")) {
            return false;
        }
        if (str.equalsIgnoreCase("true")) {
            return true;
        }
        try {
            return new BigDecimal(str).compareTo(BigDecimal.ONE) >= 0;
        } catch (NumberFormatException e) {
        }
        return false;
    }

    // Called by generated code.
    public static Boolean stringToBoolean(String str, Boolean default_) {
        // Note: For numbers, boolean is treated as an int clamped to the range [0, 1].
        if (str.equalsIgnoreCase("false")) {
            return false;
        }
        if (str.equalsIgnoreCase("true")) {
            return true;
        }
        try {
            return new BigDecimal(str).compareTo(BigDecimal.ONE) >= 0;
        } catch (NumberFormatException e) {
        }
        return default_;
    }

    // Called by generated code.
    public static long doubleToUnsignedLong(double d) {
        if (d <= 0) {
            return 0; // min unsigned long
        }
        long result = (long) d;
        if (result < Long.MAX_VALUE) {
            return result;
        }
        if (d >= 18446744073709551615.0) {
            return -1; // max unsigned long
        }
        return BigDecimal.valueOf(d).longValue();
    }

    // Called by generated code.
    public static ConversionException failed(String columnName, RuntimeException re) {
        if (re instanceof ConversionException ce) {
            return ce;
        }
        String message = re.getMessage();
        if (message == null || message.isEmpty()) {
            message = re.toString();
        }
        return new ConversionException(message, columnName);
    }
}
