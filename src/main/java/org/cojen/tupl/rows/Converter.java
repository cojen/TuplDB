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

import java.math.BigDecimal;
import java.math.BigInteger;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import static org.cojen.tupl.rows.ColumnInfo.*;

/**
 * @author Brian S O'Neill
 */
class Converter {
    /**
     * Generates code which converts a source variable into something that the destination
     * variable can accept. The given variable types must not already match.
     */
    static void convert(final MethodMaker mm,
                        final ColumnInfo srcInfo, final Variable srcVar,
                        final ColumnInfo dstInfo, final Variable dstVar)
    {
        if (dstInfo.isArray() || srcInfo.isArray()) {
            // TODO: Need this when arrays are supported.
            throw new UnsupportedOperationException();
        }

        Label end = mm.label();

        if (srcInfo.isNullable()) {
            Label notNull = mm.label();
            srcVar.ifNe(null, notNull);
            if (dstInfo.isNullable()) {
                dstVar.set(null);
            } else {
                setDefault(dstInfo, dstVar);
            }
            mm.goto_(end);
            notNull.here();
        }

        // Note: At this point, srcVar isn't null.

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
            case TYPE_BYTE: case TYPE_SHORT: case TYPE_INT: case TYPE_LONG:
                dstVar.set(srcVar.gt(0));
                break;
            case TYPE_UBYTE: case TYPE_USHORT: case TYPE_UINT: case TYPE_ULONG:
                dstVar.set(srcVar.ne(0));
                break;
            case TYPE_FLOAT:
                dstVar.set(srcVar.ge(1.0f));
                break;
            case TYPE_DOUBLE:
                dstVar.set(srcVar.ge(1.0d));
                break;
            case TYPE_BIG_INTEGER:
                dstVar.set(srcVar.invoke("compareTo",
                                         mm.var(BigInteger.class).field("ZERO")).gt(0));
                break;
            case TYPE_BIG_DECIMAL:
                dstVar.set(srcVar.invoke("compareTo",
                                         mm.var(BigDecimal.class).field("ONE")).ge(0));
                break;
            case TYPE_CHAR: {
                var utils = mm.var(Converter.class);
                if (dstInfo.type.isPrimitive()) {
                    dstVar.set(utils.invoke("charToBoolean", srcVar));
                } else {
                    Boolean default_ = dstInfo.isNullable() ? null : false;
                    dstVar.set(utils.invoke("charToBoolean", srcVar, default_));
                }
                break;
            }
            case TYPE_UTF8: {
                var utils = mm.var(Converter.class);
                if (dstInfo.type.isPrimitive()) {
                    dstVar.set(utils.invoke("stringToBoolean", srcVar));
                } else {
                    Boolean default_ = dstInfo.isNullable() ? null : false;
                    dstVar.set(utils.invoke("stringToBoolean", srcVar, default_));
                }
                break;
            }
            default:
                handled = false;
            }
            break;

        case TYPE_BYTE:
            switch (srcPlainTypeCode) {
            case TYPE_BOOLEAN:
                boolToNum(mm, srcVar, dstVar);
                break;
            case TYPE_SHORT: case TYPE_INT: case TYPE_LONG:
                clampSS(mm, srcVar, -128, 127, dstVar);
                break;
            case TYPE_UBYTE:
                clampUS(mm, srcVar, Byte.MAX_VALUE, dstVar);
                break;
            case TYPE_USHORT: case TYPE_UINT: case TYPE_ULONG:
                clampUS_narrow(mm, srcVar, Byte.MAX_VALUE, dstVar);
                break;
            case TYPE_CHAR:
                clampUS_narrow(mm, srcVar.cast(int.class), Byte.MAX_VALUE, dstVar);
                break;
            case TYPE_FLOAT: case TYPE_DOUBLE:
                clampSS(mm, srcVar.cast(int.class), -128, 127, dstVar);
                break;
            case TYPE_BIG_INTEGER:
                clampBigInteger_narrow(mm, srcVar, Byte.MIN_VALUE, Byte.MAX_VALUE,
                                       dstInfo, dstVar, "byteValue");
                break;
            case TYPE_BIG_DECIMAL:
                clampBigDecimal_narrow(mm, srcVar, Byte.MIN_VALUE, Byte.MAX_VALUE,
                                       dstInfo, dstVar, "byteValue");
                break;
            case TYPE_UTF8:
                var bd = parseBigDecimal(mm, srcVar, dstInfo, dstVar, end);
                clampBigDecimal_narrow(mm, bd, Byte.MIN_VALUE, Byte.MAX_VALUE,
                                       dstInfo, dstVar, "byteValue");
                break;
            default:
                handled = false;
            }
            break;

        case TYPE_SHORT:
            switch (srcPlainTypeCode) {
            case TYPE_BOOLEAN:
                boolToNum(mm, srcVar, dstVar);
                break;
            case TYPE_INT: case TYPE_LONG:
                clampSS(mm, srcVar, -32768, 32767, dstVar);
                break;
            case TYPE_UBYTE:
                dstVar.set(srcVar.cast(int.class).and(0xff).cast(short.class));
                break;
            case TYPE_USHORT:
                clampUS(mm, srcVar, Short.MAX_VALUE, dstVar);
                break;
            case TYPE_UINT: case TYPE_ULONG:
                clampUS_narrow(mm, srcVar, Short.MAX_VALUE, dstVar);
                break;
            case TYPE_CHAR:
                clampUS_narrow(mm, srcVar.cast(int.class), Short.MAX_VALUE, dstVar);
                break;
            case TYPE_FLOAT: case TYPE_DOUBLE:
                clampSS(mm, srcVar.cast(int.class), -32768, 32767, dstVar);
                break;
            case TYPE_BIG_INTEGER:
                clampBigInteger_narrow(mm, srcVar, Short.MIN_VALUE, Short.MAX_VALUE,
                                       dstInfo, dstVar, "shortValue");
                break;
            case TYPE_BIG_DECIMAL:
                clampBigDecimal_narrow(mm, srcVar, Short.MIN_VALUE, Short.MAX_VALUE,
                                       dstInfo, dstVar, "shortValue");
                break;
            case TYPE_UTF8:
                var bd = parseBigDecimal(mm, srcVar, dstInfo, dstVar, end);
                clampBigDecimal_narrow(mm, bd, Short.MIN_VALUE, Short.MAX_VALUE,
                                       dstInfo, dstVar, "shortValue");
                break;
            default:
                handled = false;
            }
            break;

        case TYPE_INT:
            switch (srcPlainTypeCode) {
            case TYPE_BOOLEAN:
                boolToNum(mm, srcVar, dstVar);
                break;
            case TYPE_LONG:
                clampSS(mm, srcVar, Integer.MIN_VALUE, Integer.MAX_VALUE, dstVar);
                break;
            case TYPE_UBYTE:
                dstVar.set(srcVar.cast(int.class).and(0xff));
                break;
            case TYPE_USHORT: case TYPE_CHAR:
                dstVar.set(srcVar.cast(int.class).and(0xffff));
                break;
            case TYPE_UINT:
                clampUS(mm, srcVar, Integer.MAX_VALUE, dstVar);
                break;
            case TYPE_ULONG:
                clampUS_narrow(mm, srcVar, Integer.MAX_VALUE, dstVar);
                break;
            case TYPE_FLOAT: case TYPE_DOUBLE:
                dstVar.set(srcVar.cast(int.class));
                break;
            case TYPE_BIG_INTEGER:
                clampBigInteger_narrow(mm, srcVar, Integer.MIN_VALUE, Integer.MAX_VALUE,
                                       dstInfo, dstVar, "intValue");
                break;
            case TYPE_BIG_DECIMAL:
                clampBigDecimal_narrow(mm, srcVar, Integer.MIN_VALUE, Integer.MAX_VALUE,
                                       dstInfo, dstVar, "intValue");
                break;
            case TYPE_UTF8:
                var bd = parseBigDecimal(mm, srcVar, dstInfo, dstVar, end);
                clampBigDecimal_narrow(mm, bd, Integer.MIN_VALUE, Integer.MAX_VALUE,
                                       dstInfo, dstVar, "intValue");
                break;
            default:
                handled = false;
            }
            break;

       case TYPE_LONG:
            switch (srcPlainTypeCode) {
            case TYPE_BOOLEAN:
                boolToNum(mm, srcVar, dstVar);
                break;
            case TYPE_UBYTE:
                dstVar.set(srcVar.cast(long.class).and(0xffL));
                break;
            case TYPE_USHORT: case TYPE_CHAR:
                dstVar.set(srcVar.cast(long.class).and(0xffffL));
                break;
            case TYPE_UINT:
                dstVar.set(srcVar.cast(long.class).and(0xffff_ffffL));
                break;
            case TYPE_ULONG:
                clampUS(mm, srcVar, Long.MAX_VALUE, dstVar);
                break;
            case TYPE_FLOAT: case TYPE_DOUBLE:
                dstVar.set(srcVar.cast(long.class));
                break;
            case TYPE_BIG_INTEGER:
                clampBigInteger_narrow(mm, srcVar, Long.MIN_VALUE, Long.MAX_VALUE,
                                       dstInfo, dstVar, "longValue");
                break;
            case TYPE_BIG_DECIMAL:
                clampBigDecimal_narrow(mm, srcVar, Long.MIN_VALUE, Long.MAX_VALUE,
                                       dstInfo, dstVar, "longValue");
                break;
            case TYPE_UTF8:
                var bd = parseBigDecimal(mm, srcVar, dstInfo, dstVar, end);
                clampBigDecimal_narrow(mm, bd, Long.MIN_VALUE, Long.MAX_VALUE,
                                       dstInfo, dstVar, "longValue");
                break;
            default:
                handled = false;
            }
            break;

        case TYPE_UBYTE:
            switch (srcPlainTypeCode) {
            case TYPE_BOOLEAN:
                boolToNum(mm, srcVar, dstVar);
                break;
            case TYPE_BYTE:
                clampSU(mm, srcVar, dstVar);
                break;
            case TYPE_SHORT: case TYPE_INT: case TYPE_LONG:
                clampSU_narrow(mm, srcVar, 0xff, dstVar);
                break;
            case TYPE_USHORT: case TYPE_UINT:
                clampUU_narrow(mm, srcVar, 0xff, dstVar);
                break;
            case TYPE_CHAR:
                clampUU_narrow(mm, srcVar.cast(int.class), 0xff, dstVar);
                break;
            case TYPE_ULONG:
                clampUU_narrow(mm, srcVar, 0xffL, dstVar);
                break;
            case TYPE_FLOAT:
                clampSU_narrow(mm, srcVar.cast(int.class), 0xff, dstVar);
                break;
            case TYPE_DOUBLE:
                clampSU_narrow(mm, srcVar.cast(long.class), 0xffL, dstVar);
                break;
            case TYPE_BIG_INTEGER:
                clampBigInteger_narrow(mm, srcVar, 0, 0xff, dstInfo, dstVar, "byteValue");
                break;
            case TYPE_BIG_DECIMAL:
                clampBigDecimal_narrow(mm, srcVar, 0, 0xff, dstInfo, dstVar, "byteValue");
                break;
            case TYPE_UTF8:
                var bd = parseBigDecimal(mm, srcVar, dstInfo, dstVar, end);
                clampBigDecimal_narrow(mm, bd, 0, 0xff, dstInfo, dstVar, "byteValue");
                break;
            default:
                handled = false;
            }
            break;

        case TYPE_USHORT:
            switch (srcPlainTypeCode) {
            case TYPE_BOOLEAN:
                boolToNum(mm, srcVar, dstVar);
                break;
            case TYPE_BYTE: case TYPE_SHORT:
                clampSU(mm, srcVar, dstVar);
                break;
            case TYPE_INT: case TYPE_LONG:
                clampSU_narrow(mm, srcVar, 0xffff, dstVar);
                break;
            case TYPE_UBYTE:
                dstVar.set(srcVar.cast(int.class).and(0xff).cast(short.class));
                break;
            case TYPE_UINT:
                clampUU_narrow(mm, srcVar, 0xffff, dstVar);
                break;
            case TYPE_ULONG:
                clampUU_narrow(mm, srcVar, 0xffffL, dstVar);
                break;
            case TYPE_FLOAT:
                clampSU_narrow(mm, srcVar.cast(int.class), 0xffff, dstVar);
                break;
            case TYPE_DOUBLE:
                clampSU_narrow(mm, srcVar.cast(long.class), 0xffffL, dstVar);
                break;
            case TYPE_CHAR:
                dstVar.set(srcVar.cast(short.class));
                break;
            case TYPE_BIG_INTEGER:
                clampBigInteger_narrow(mm, srcVar, 0, 0xffff, dstInfo, dstVar, "shortValue");
                break;
            case TYPE_BIG_DECIMAL:
                clampBigDecimal_narrow(mm, srcVar, 0, 0xffff, dstInfo, dstVar, "shortValue");
                break;
            case TYPE_UTF8:
                var bd = parseBigDecimal(mm, srcVar, dstInfo, dstVar, end);
                clampBigDecimal_narrow(mm, bd, 0, 0xffff, dstInfo, dstVar, "shortValue");
                break;
            default:
                handled = false;
            }
            break;

        case TYPE_UINT:
            switch (srcPlainTypeCode) {
            case TYPE_BOOLEAN:
                boolToNum(mm, srcVar, dstVar);
                break;
            case TYPE_BYTE: case TYPE_SHORT: case TYPE_INT:
                clampSU(mm, srcVar, dstVar);
                break;
            case TYPE_LONG:
                clampSU_narrow(mm, srcVar, 0xffff_ffffL, dstVar);
                break;
            case TYPE_UBYTE:
                dstVar.set(srcVar.cast(int.class).and(0xff));
                break;
            case TYPE_USHORT: case TYPE_CHAR:
                dstVar.set(srcVar.cast(int.class).and(0xffff));
                break;
            case TYPE_ULONG:
                clampUU_narrow(mm, srcVar, 0xffff_ffffL, dstVar);
                break;
            case TYPE_FLOAT: case TYPE_DOUBLE:
                clampSU_narrow(mm, srcVar.cast(long.class), 0xffff_ffffL, dstVar);
                break;
            case TYPE_BIG_INTEGER:
                clampBigInteger_narrow(mm, srcVar, 0, 0xffff_ffffL, dstInfo, dstVar, "intValue");
                break;
            case TYPE_BIG_DECIMAL:
                clampBigDecimal_narrow(mm, srcVar, 0, 0xffff_ffffL, dstInfo, dstVar, "intValue");
                break;
            case TYPE_UTF8:
                var bd = parseBigDecimal(mm, srcVar, dstInfo, dstVar, end);
                clampBigDecimal_narrow(mm, bd, 0, 0xffff_ffffL, dstInfo, dstVar, "intValue");
                break;
            default:
                handled = false;
            }
            break; 

        case TYPE_ULONG:
            switch (srcPlainTypeCode) {
            case TYPE_BOOLEAN:
                boolToNum(mm, srcVar, dstVar);
                break;
            case TYPE_BYTE: case TYPE_SHORT: case TYPE_INT: case TYPE_LONG:
                clampSU(mm, srcVar, dstVar);
                break;
            case TYPE_UBYTE:
                dstVar.set(srcVar.cast(long.class).and(0xff));
                break;
            case TYPE_USHORT: case TYPE_CHAR:
                dstVar.set(srcVar.cast(long.class).and(0xffff));
                break;
            case TYPE_UINT:
                dstVar.set(srcVar.cast(long.class).and(0xffff_ffffL));
                break;
            case TYPE_FLOAT: case TYPE_DOUBLE:
                clampBigDecimalU_narrow(mm, mm.var(BigDecimal.class).invoke("valueOf", srcVar),
                                        dstInfo, dstVar);
                break;
            case TYPE_BIG_INTEGER:
                clampBigIntegerU_narrow(mm, srcVar, dstInfo, dstVar);
                break;
            case TYPE_BIG_DECIMAL:
                clampBigDecimalU_narrow(mm, srcVar, dstInfo, dstVar);
                break;
            case TYPE_UTF8:
                var bd = parseBigDecimal(mm, srcVar, dstInfo, dstVar, end);
                clampBigDecimalU_narrow(mm, bd, dstInfo, dstVar);
                break;
            default:
                handled = false;
            }
            break; 

        case TYPE_FLOAT:
            switch (srcPlainTypeCode) {
            case TYPE_BOOLEAN:
                boolToNum(mm, srcVar, dstVar);
                break;
            case TYPE_INT: case TYPE_LONG: case TYPE_DOUBLE:
                dstVar.set(unbox(srcVar).cast(dstVar));
                break;
            case TYPE_UBYTE:
                dstVar.set(srcVar.cast(int.class).and(0xff).cast(dstVar));
                break;
            case TYPE_USHORT: case TYPE_CHAR:
                dstVar.set(srcVar.cast(int.class).and(0xffff).cast(dstVar));
                break;
            case TYPE_UINT:
                dstVar.set(srcVar.cast(long.class).and(0xffff_ffffL).cast(dstVar));
                break;
            case TYPE_ULONG:
                var bd = mm.var(BigDecimal.class);
                toBigDecimalU(mm, srcVar, bd);
                dstVar.set(bd.invoke("floatValue"));
                break;
            case TYPE_BIG_INTEGER: case TYPE_BIG_DECIMAL:
                dstVar.set(srcVar.invoke("floatValue"));
                break;
            case TYPE_UTF8:
                parseNumber(mm, "parseFloat", srcVar, dstInfo, dstVar);
                break;
            default:
                handled = false;
            }
            break;

        case TYPE_DOUBLE:
            switch (srcPlainTypeCode) {
            case TYPE_BOOLEAN:
                boolToNum(mm, srcVar, dstVar);
                break;
            case TYPE_LONG:
                dstVar.set(unbox(srcVar).cast(dstVar));
                break;
            case TYPE_UBYTE:
                dstVar.set(srcVar.cast(int.class).and(0xff).cast(dstVar));
                break;
            case TYPE_USHORT: case TYPE_CHAR:
                dstVar.set(srcVar.cast(int.class).and(0xffff).cast(dstVar));
                break;
            case TYPE_UINT:
                dstVar.set(srcVar.cast(long.class).and(0xffff_ffffL).cast(dstVar));
                break;
            case TYPE_ULONG:
                var bd = mm.var(BigDecimal.class);
                toBigDecimalU(mm, srcVar, bd);
                dstVar.set(bd.invoke("doubleValue"));
                break;
            case TYPE_BIG_INTEGER: case TYPE_BIG_DECIMAL:
                dstVar.set(srcVar.invoke("doubleValue"));
                break;
            case TYPE_UTF8:
                parseNumber(mm, "parseDouble", srcVar, dstInfo, dstVar);
                break;
            default:
                handled = false;
            }
            break;

        case TYPE_CHAR:
            switch (srcPlainTypeCode) {
            case TYPE_BOOLEAN: {
                Label L1 = mm.label();
                srcVar.ifTrue(L1);
                Label cont = mm.label();
                dstVar.set('f');
                mm.goto_(cont);
                L1.here();
                dstVar.set('t');
                cont.here();
                break;
            }
            case TYPE_BYTE: case TYPE_SHORT:
                clampSU(mm, srcVar, dstVar);
                break;
            case TYPE_INT: case TYPE_LONG:
                clampSU_narrow(mm, srcVar, 0xffff, '\uffff', dstVar);
                break;
            case TYPE_UBYTE:
                dstVar.set(srcVar.cast(int.class).and(0xff).cast(char.class));
                break;
            case TYPE_USHORT:
                dstVar.set(srcVar.cast(char.class));
                break;
            case TYPE_UINT:
                clampUU_narrow(mm, srcVar, 0xffff, '\uffff', dstVar);
                break;
            case TYPE_ULONG:
                clampUU_narrow(mm, srcVar, 0xffffL, '\uffff', dstVar);
                break;
            case TYPE_FLOAT:
                clampSU_narrow(mm, srcVar.cast(int.class), 0xffff, '\uffff', dstVar);
                break;
            case TYPE_DOUBLE:
                clampSU_narrow(mm, srcVar.cast(long.class), 0xffffL, '\uffff', dstVar);
                break;
            case TYPE_BIG_INTEGER:
                clampBigInteger_narrow(mm, srcVar, 0, 0xffff, dstInfo, dstVar, "intValue");
                break;
            case TYPE_BIG_DECIMAL:
                clampBigDecimal_narrow(mm, srcVar, 0, 0xffff, dstInfo, dstVar, "intValue");
                break;
            case TYPE_UTF8: {
                Label L1 = mm.label();
                srcVar.invoke("isEmpty").ifFalse(L1);
                setDefault(dstInfo, dstVar);
                Label cont = mm.label();
                mm.goto_(cont);
                L1.here();
                dstVar.set(srcVar.invoke("charAt", 0));
                cont.here();
                break;
            }
            default:
                handled = false;
            }
            break;

        case TYPE_UTF8:
            switch (srcPlainTypeCode) {
            case TYPE_UBYTE:
                dstVar.set(mm.var(Integer.class).invoke
                           ("toUnsignedString", srcVar.cast(int.class).and(0xff)));
                break;
            case TYPE_USHORT:
                dstVar.set(mm.var(Integer.class).invoke
                           ("toUnsignedString", srcVar.cast(int.class).and(0xffff)));
                break;
            case TYPE_UINT: case TYPE_ULONG:
                dstVar.set(srcVar.invoke("toUnsignedString", srcVar));
                break;
            default:
                dstVar.set(mm.var(String.class).invoke("valueOf", srcVar));
                break;
            }
            break;

        case TYPE_BIG_INTEGER:
            var bi = mm.var(BigInteger.class);
            switch (srcPlainTypeCode) {
            case TYPE_BOOLEAN:
                Label isFalse = mm.label();
                srcVar.ifFalse(isFalse);
                dstVar.set(bi.field("ONE"));
                mm.goto_(end);
                isFalse.here();
                dstVar.set(bi.field("ZERO"));
                break;
            case TYPE_BYTE: case TYPE_SHORT: case TYPE_INT: case TYPE_LONG:
                dstVar.set(bi.invoke("valueOf", srcVar));
                break;
            case TYPE_UBYTE:
                dstVar.set(bi.invoke("valueOf", srcVar.cast(long.class).and(0xffL)));
                break;
            case TYPE_USHORT: case TYPE_CHAR:
                dstVar.set(bi.invoke("valueOf", srcVar.cast(long.class).and(0xffffL)));
                break;
            case TYPE_UINT:
                dstVar.set(bi.invoke("valueOf", srcVar.cast(long.class).and(0xffff_ffffL)));
                break;
            case TYPE_ULONG:
                toBigIntegerU(mm, srcVar, dstVar);
                break;
            case TYPE_BIG_DECIMAL:
                dstVar.set(srcVar.invoke("toBigInteger"));
                break;
            case TYPE_FLOAT: case TYPE_DOUBLE:
                dstVar.set(mm.var(BigDecimal.class).invoke("valueOf", srcVar)
                           .invoke("toBigInteger"));
                break;
            case TYPE_UTF8:
                var bd = parseBigDecimal(mm, srcVar, dstInfo, dstVar, end);
                dstVar.set(bd.invoke("toBigInteger"));
                break;
            default:
                handled = false;
            }
            break;

        case TYPE_BIG_DECIMAL:
            var bd = mm.var(BigDecimal.class);
            switch (srcPlainTypeCode) {
            case TYPE_BOOLEAN:
                Label isFalse = mm.label();
                srcVar.ifFalse(isFalse);
                dstVar.set(bd.field("ONE"));
                mm.goto_(end);
                isFalse.here();
                dstVar.set(bd.field("ZERO"));
                break;
            case TYPE_BYTE: case TYPE_SHORT: case TYPE_INT: case TYPE_LONG:
            case TYPE_FLOAT: case TYPE_DOUBLE:
                dstVar.set(bd.invoke("valueOf", srcVar));
                break;
            case TYPE_UBYTE:
                dstVar.set(bd.invoke("valueOf", srcVar.cast(long.class).and(0xffL)));
                break;
            case TYPE_USHORT: case TYPE_CHAR:
                dstVar.set(bd.invoke("valueOf", srcVar.cast(long.class).and(0xffffL)));
                break;
            case TYPE_UINT:
                dstVar.set(bd.invoke("valueOf", srcVar.cast(long.class).and(0xffff_ffffL)));
                break;
            case TYPE_ULONG:
                toBigDecimalU(mm, srcVar, dstVar);
                break;
            case TYPE_BIG_INTEGER:
                dstVar.set(mm.new_(bd, srcVar));
                break;
            case TYPE_UTF8:
                parseBigDecimal(mm, srcVar, dstInfo, dstVar, end);
                break;
            default:
                handled = false;
            }
            break;

        default:
            handled = false;
        }

        if (!handled) {
            setDefault(dstInfo, dstVar);
        }

        end.here();
    }

    /**
     * Assigns a default value to the variable: null, 0, false, etc.
     */
    static void setDefault(ColumnInfo dstInfo, Variable dstVar) {
        if (dstInfo.isNullable()) {
            dstVar.set(null);
        } else {
            switch (dstInfo.plainTypeCode()) {
            case TYPE_BOOLEAN:
                dstVar.set(false);
                break;
            case TYPE_UTF8:
                dstVar.set("");
                break;
            case TYPE_BIG_INTEGER: case TYPE_BIG_DECIMAL:
                dstVar.set(dstVar.field("ZERO"));
                break;
            default:
                dstVar.set(0);
                break;
            }
        }
    }

    /**
     * Stores 0 or 1 into a primitive numerical variable.
     */
    private static void boolToNum(MethodMaker mm, Variable srcVar, Variable dstVar) {
        Label isFalse = mm.label();
        srcVar.ifFalse(isFalse);
        dstVar.set(1);
        Label cont = mm.label();
        mm.goto_(cont);
        isFalse.here();
        dstVar.set(0);
        cont.here();
    }

    private static void parseNumber(MethodMaker mm, String method,
                                    Variable srcVar, ColumnInfo dstInfo, Variable dstVar)
    {
        Label tryStart = mm.label().here();
        dstVar.set(dstVar.invoke(method, srcVar));
        Label cont = mm.label();
        mm.goto_(cont);
        Label tryEnd = mm.label().here();
        mm.catch_(tryStart, tryEnd, NumberFormatException.class);
        setDefault(dstInfo, dstVar);
        cont.here();
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
        case TYPE_UBYTE: case TYPE_BYTE:
            dstVar.set((byte) max);
            break;
        case TYPE_USHORT: case TYPE_SHORT:
            dstVar.set((short) max);
            break;
        case TYPE_UINT: case TYPE_INT:
            dstVar.set((int) max);
            break;
        default:
            dstVar.set(max);
            break;
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
     * @param srcVar long (unsigned)
     * @param dstVar BigInteger
     */
    private static void toBigIntegerU(MethodMaker mm, Variable srcVar, Variable dstVar) {
        srcVar = unbox(srcVar);
        var bi = mm.var(BigInteger.class);
        Label L1 = mm.label();
        srcVar.ifLt(0L, L1);
        Label cont = mm.label();
        dstVar.set(bi.invoke("valueOf", srcVar));
        mm.goto_(cont);
        L1.here();
        var magnitude = mm.new_(byte[].class, 8);
        mm.var(RowUtils.class).invoke("encodeLongBE", magnitude, 0, srcVar);
        dstVar.set(mm.new_(bi, 1, magnitude));
        cont.here();
    }

    /**
     * @param srcVar long (unsigned)
     * @param dstVar BigDecimal
     */
    private static void toBigDecimalU(MethodMaker mm, Variable srcVar, Variable dstVar) {
        srcVar = unbox(srcVar);
        Label L1 = mm.label();
        srcVar.ifLt(0L, L1);
        Label cont = mm.label();
        dstVar.set(mm.var(BigDecimal.class).invoke("valueOf", srcVar));
        mm.goto_(cont);
        L1.here();
        var magnitude = mm.new_(byte[].class, 8);
        mm.var(RowUtils.class).invoke("encodeLongBE", magnitude, 0, srcVar);
        dstVar.set(mm.new_(BigDecimal.class, mm.new_(BigInteger.class, 1, magnitude)));
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
        Label cont = mm.label();
        mm.goto_(cont);
        Label tryEnd = mm.label().here();
        mm.catch_(tryStart, tryEnd, NumberFormatException.class);
        setDefault(dstInfo, dstVar);
        mm.goto_(end);
        cont.here();
        return bd;
    }

    private static Variable unbox(Variable v) {
        return v.classType().isPrimitive() ? v : v.unbox();
    }

    // Called by generated code.
    static boolean charToBoolean(char c) {
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
        return false;
    }

    // Called by generated code.
    static Boolean charToBoolean(char c, Boolean default_) {
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
    static boolean stringToBoolean(String str) {
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
    static Boolean stringToBoolean(String str, Boolean default_) {
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
}
