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

import org.cojen.maker.Field;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.filter.ColumnFilter;

import static org.cojen.tupl.rows.ColumnInfo.*;
import static org.cojen.tupl.rows.RowUtils.*;

/**
 * Makes code for encoding and decoding primitive type columns.
 *
 * @author Brian S O'Neill
 */
class PrimitiveColumnCodec extends ColumnCodec {
    private final boolean mForKey;
    private final int mSize;

    /**
     * @param info non-null
     * @param mm is null for stateless instance
     * @param forKey true to use key encoding (lexicographical order)
     * @param size byte count
     */
    PrimitiveColumnCodec(ColumnInfo info, MethodMaker mm, boolean forKey, int size) {
        super(info, mm);
        mForKey = forKey;
        mSize = size;
    }

    @Override
    ColumnCodec bind(MethodMaker mm) {
        return new PrimitiveColumnCodec(mInfo, mm, mForKey, mSize);
    }

    @Override
    int minSize() {
        // Return just the header size if nullable.
        return mInfo.isNullable() ? 1 : mSize;
    }

    @Override
    boolean isLast() {
        return false;
    }

    @Override
    void encodePrepare() {
    }

    @Override
    void encodeSkip() {
    }

    @Override
    Variable encodeSize(Variable srcVar, Variable totalVar) {
        if (mInfo.isNullable() && mInfo.plainTypeCode() != TYPE_BOOLEAN) {
            if (totalVar == null) {
                totalVar = mMaker.var(int.class).set(0);
            }
            Label isNull = mMaker.label();
            srcVar.ifEq(null, isNull);
            totalVar.inc(mSize);
            isNull.here();
        }

        return totalVar;
    }

    @Override
    void encode(Variable srcVar, Variable dstVar, Variable offsetVar) {
        Label end = null;

        int plain = mInfo.plainTypeCode();

        if (mInfo.isNullable() && plain != TYPE_BOOLEAN) {
            end = mMaker.label();
            encodeNullHeader(end, srcVar, dstVar, offsetVar);
        }

        doEncode: {
            String methodType;

            switch (plain) {
            case TYPE_BOOLEAN: case TYPE_BYTE: case TYPE_UBYTE: {
                if (plain == TYPE_BYTE) {
                    if (mForKey) {
                        int mask = mInfo.isDescending() ? 0x7f : 0x80;
                        srcVar = srcVar.xor(mask).cast(byte.class);
                    }
                } else if (plain == TYPE_UBYTE) {
                    // FIXME
                    throw null;
                } else {
                    byte f, t, n;
                    n = NULL_BYTE_HIGH;
                    if (!mForKey) {
                        f = 0;
                        t = 1;
                    } else {
                        f = (byte) 0x80;
                        t = (byte) 0x81;
                        if (mInfo.isDescending()) {
                            f = (byte) ~f;
                            t = (byte) ~t;
                            n = (byte) ~n;
                        }
                    }

                    var byteVar = mMaker.var(byte.class);
                    Label cont = mMaker.label();

                    if (mInfo.isNullable()) {
                        Label notNull = mMaker.label();
                        srcVar.ifNe(null, notNull);
                        byteVar.set(n);
                        mMaker.goto_(cont);
                        notNull.here();
                    }

                    Label trueCase = mMaker.label();
                    srcVar.ifTrue(trueCase);
                    byteVar.set(f);
                    mMaker.goto_(cont);
                    trueCase.here();
                    byteVar.set(t);
                    srcVar = byteVar;

                    cont.here();
                }

                dstVar.aset(offsetVar, srcVar);
                offsetVar.inc(1);

                break doEncode;
            }

            case TYPE_SHORT: case TYPE_USHORT: case TYPE_CHAR:
                methodType = "Short";
                break;

            case TYPE_FLOAT:
                srcVar = mMaker.var(Float.class).invoke("floatToRawIntBits", srcVar);
            case TYPE_INT: case TYPE_UINT:
                methodType = "Int";
                break;

            case TYPE_DOUBLE:
                srcVar = mMaker.var(Double.class).invoke("doubleToRawLongBits", srcVar);
            case TYPE_LONG: case TYPE_ULONG:
                methodType = "Long";
                break;

            default:
                throw new AssertionError();
            }

            String format;
            if (!mForKey) {
                format = "LE";
            } else {
                format = "BE";
                if (!mInfo.isUnsigned()) {
                    srcVar = srcVar.xor(signMask());
                }
            }

            String methodName = "encode" + methodType + format;
            mMaker.var(RowUtils.class).invoke(methodName, dstVar, offsetVar, srcVar);
            offsetVar.inc(mSize);
        }

        if (end != null) {
            end.here();
        }
    }

    @Override
    void decode(Variable dstVar, Variable srcVar, Variable offsetVar, Variable endVar) {
        decode(dstVar, srcVar, offsetVar, false);
    }

    /**
     * @param raw true to leave floating point values in their raw integer form
     */
    private void decode(Variable dstVar, Variable srcVar, Variable offsetVar, boolean raw) {
        decode(dstVar, srcVar, offsetVar, raw, mInfo.isNullable());
    }

    private void decode(Variable dstVar, Variable srcVar, Variable offsetVar,
                        boolean raw, boolean isNullable)
    {
        Label end = null;

        int plain = mInfo.plainTypeCode();

        if (isNullable && plain != TYPE_BOOLEAN) {
            end = mMaker.label();
            decodeNullHeader(end, dstVar, srcVar, offsetVar);
        }

        Variable valueVar;

        doDecode: {
            String methodType;

            switch (plain) {
            case TYPE_BOOLEAN: case TYPE_BYTE: case TYPE_UBYTE: {
                var byteVar = srcVar.aget(offsetVar);
                offsetVar.inc(1);

                if (plain == TYPE_BYTE) {
                    if (mForKey) {
                        int mask = mInfo.isDescending() ? 0x7f : 0x80;
                        byteVar = byteVar.xor(mask).cast(byte.class);
                    }
                    valueVar = byteVar;
                } else if (plain == TYPE_UBYTE) {
                    // FIXME
                    throw null;
                } else {
                    Label cont = null;

                    if (!isNullable) {
                        valueVar = mMaker.var(boolean.class);
                    } else {
                        byte n = NULL_BYTE_HIGH;
                        if (mInfo.isDescending()) {
                            n = (byte) ~n;
                        }
                        valueVar = mMaker.var(Boolean.class);
                        Label notNull = mMaker.label();
                        byteVar.ifNe(n, notNull);
                        valueVar.set(null);
                        cont = mMaker.label();
                        mMaker.goto_(cont);
                        notNull.here();
                    }

                    if (mInfo.isDescending()) {
                        byteVar = byteVar.com();
                    }

                    valueVar.set(byteVar.cast(boolean.class));

                    if (cont != null) {
                        cont.here();
                    }
                }

                break doDecode;
            }

            case TYPE_SHORT: case TYPE_USHORT: case TYPE_CHAR:
                methodType = "UnsignedShort";
                break;

            case TYPE_INT: case TYPE_FLOAT: case TYPE_UINT:
                methodType = "Int";
                break;

            case TYPE_LONG: case TYPE_DOUBLE: case TYPE_ULONG:
                methodType = "Long";
                break;

            default:
                throw new AssertionError();
            }

            String methodName = "decode" + methodType + (mForKey ? "BE" : "LE");
            valueVar = mMaker.var(RowUtils.class).invoke(methodName, srcVar, offsetVar);
            offsetVar.inc(mSize);

            if (mForKey && !mInfo.isUnsigned()) {
                valueVar = valueVar.xor(signMask());
            }

            switch (plain) {
            case TYPE_SHORT: case TYPE_USHORT:
                valueVar = valueVar.cast(short.class);
                break;
            case TYPE_CHAR:
                valueVar = valueVar.cast(char.class);
                break;
            case TYPE_FLOAT:
                if (!raw) {
                    valueVar = mMaker.var(Float.class).invoke("intBitsToFloat", valueVar);
                }
                break;
            case TYPE_DOUBLE:
                if (!raw) {
                    valueVar = mMaker.var(Double.class).invoke("longBitsToDouble", valueVar);
                }
                break;
            }
        }

        dstVar.set(valueVar);

        if (end != null) {
            end.here();
        }
    }

    @Override
    void decodeSkip(Variable srcVar, Variable offsetVar, Variable endVar) {
        Label end = null;

        if (mInfo.isNullable() && mInfo.plainTypeCode() != TYPE_BOOLEAN) {
            end = mMaker.label();
            decodeNullHeader(end, null, srcVar, offsetVar);
        }

        if (offsetVar != null) {
            offsetVar.inc(mSize);
        }

        if (end != null) {
            end.here();
        }
    }

    /**
     * Defines a single field if the column isn't nullable, or if the column is boolean. If
     * nullable, defines two fields: the usual one and the "is null" boolean field.
     */
    @Override
    void filterPrepare(int op, Variable argVar, int argNum) {
        // FIXME: If boolean and op !isExact, blow up here with an exception.

        Label cont = null;

        if (mInfo.isNullable()) {
            if (mInfo.plainTypeCode() == TYPE_BOOLEAN) {
                defineArgField(argVar, argFieldName(argNum)).set(argVar);
                return;
            }
            Field isNullField = defineArgField(boolean.class, argFieldName(argNum, "isNull"));
            Label notNull = mMaker.label();
            argVar.ifNe(null, notNull);
            isNullField.set(true);
            cont = mMaker.label();
            mMaker.goto_(cont);
            notNull.here();
            isNullField.set(false);
        }

        Variable fieldValue = argVar.cast(mInfo.boxedType()).unbox();

        // For floating point, compare against raw bits to support NaN comparison.
        if (ColumnFilter.isExact(op)) {
            if (mInfo.type == float.class) {
                fieldValue = fieldValue.invoke("floatToRawIntBits", fieldValue);
            } else if (mInfo.type == double.class) {
                fieldValue = fieldValue.invoke("doubleToRawLongBits", fieldValue);
            }
        }

        defineArgField(fieldValue, argFieldName(argNum)).set(fieldValue);

        if (cont != null) {
            cont.here();
        }
    }

    @Override
    Object filterDecode(ColumnInfo dstInfo, Variable srcVar, Variable offsetVar, Variable endVar,
                        int op)
    {
        if (dstInfo.typeCode != mInfo.typeCode) {
            // FIXME: Convert it... (float/double must be raw when finished, if op isExact)
            // FIXME: Need to create a general purpose compare maker, for any Java type. The
            //        optimizations here avoid boxing, that's all. If the conversion yields
            //        a primitive type, possibily nullable, use the optimizations.
            throw null;
        }

        final boolean rawColumn;
        final Variable columnVar;
        defineColumnVar: {
            if (ColumnFilter.isExact(op)) {
                if (dstInfo.type == float.class) {
                    rawColumn = true;
                    columnVar = mMaker.var(int.class);
                    break defineColumnVar;
                } else if (dstInfo.type == double.class) {
                    rawColumn = true;
                    columnVar = mMaker.var(long.class);
                    break defineColumnVar;
                }
            }
            rawColumn = false;
            columnVar = mMaker.var(dstInfo.type);
        }

        Variable isNullVar = null;
        Label end = null;

        if (dstInfo.isNullable() && dstInfo.plainTypeCode() != TYPE_BOOLEAN) {
            columnVar.set(0);
            isNullVar = mMaker.var(boolean.class);
            end = mMaker.label();
            decodeNullHeader(end, isNullVar, srcVar, offsetVar);
        }

        decode(columnVar, srcVar, offsetVar, rawColumn, false);

        if (end != null) {
            end.here();
        }

        if (isNullVar == null) {
            return columnVar;
        } else {
            return new Variable[] {columnVar, isNullVar};
        }
    }

    @Override
    void filterCompare(ColumnInfo dstInfo, Object decoded, int op, Variable argObjVar, int argNum,
                       Label pass, Label fail)
    {
        Variable columnVar, isNullVar;
        if (decoded instanceof Variable) {
            columnVar = (Variable) decoded;
            isNullVar = null;
        } else {
            var pair = (Variable[]) decoded;
            columnVar = pair[0];
            isNullVar = pair[1];
        }

        if (isNullVar != null) {
            var isNullField = argObjVar.field(argFieldName(argNum, "isNull"));
            compareNullHeader(isNullVar, null, isNullField, op, pass, fail);
        }

        var argField = argObjVar.field(argFieldName(argNum));

        // FIXME: This switch can be in the general purpose maker. Both vars should be same
        // type, but they can be widened if necessary.
        // FIXME: Consider unsigned types!
        switch (op) {
        case ColumnFilter.OP_EQ: columnVar.ifEq(argField, pass); break;
        case ColumnFilter.OP_NE: columnVar.ifNe(argField, pass); break;
        case ColumnFilter.OP_LT: columnVar.ifLt(argField, pass); break;
        case ColumnFilter.OP_GE: columnVar.ifGe(argField, pass); break;
        case ColumnFilter.OP_GT: columnVar.ifGt(argField, pass); break;
        case ColumnFilter.OP_LE: columnVar.ifLe(argField, pass); break;
        default: throw new AssertionError();
        }

        mMaker.goto_(fail);
    }

    /**
     * Must only be called for size 2, 4, or 8.
     */
    private Object signMask() {
        if (mSize == 8) {
            long lmask = 1L << 63;
            if (mInfo.isDescending()) {
                lmask = ~lmask;
            }
            return lmask;
        } else {
            int imask = mSize == 4 ? (1 << 31) : (1 << 15);
            if (mInfo.isDescending()) {
                imask = ~imask;
            }
            return imask;
        }
    }
}
