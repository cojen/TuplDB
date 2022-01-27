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

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import static org.cojen.tupl.rows.ColumnInfo.*;
import static org.cojen.tupl.rows.RowUtils.*;

/**
 * Makes code for encoding and decoding primitive type columns.
 *
 * @author Brian S O'Neill
 */
final class PrimitiveColumnCodec extends ColumnCodec {
    private final boolean mLex;
    private final int mSize;

    /**
     * @param info non-null
     * @param mm is null for stateless instance
     * @param lex true to use lexicographical encoding
     * @param size byte count
     */
    PrimitiveColumnCodec(ColumnInfo info, MethodMaker mm, boolean lex, int size) {
        super(info, mm);
        mLex = lex;
        mSize = size;
    }

    @Override
    ColumnCodec bind(MethodMaker mm) {
        return new PrimitiveColumnCodec(mInfo, mm, mLex, mSize);
    }

    @Override
    protected final boolean doEquals(Object obj) {
        var other = (PrimitiveColumnCodec) obj;
        if (mLex != other.mLex || mSize != other.mSize) {
            return false;
        }
        int typeCode = mInfo.typeCode;
        int otherTypeCode = other.mInfo.typeCode;
        if (!mLex) {
            typeCode &= ~TYPE_DESCENDING;
            otherTypeCode &= ~TYPE_DESCENDING;
        }
        return typeCode == otherTypeCode;
    }

    @Override
    public final int doHashCode() {
        return mInfo.typeCode & ~TYPE_DESCENDING;
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
                if (plain == TYPE_BOOLEAN) {
                    byte f, t, n;
                    n = NULL_BYTE_HIGH;
                    if (!mLex) {
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
                } else if (mLex) {
                    if (plain == TYPE_BYTE) {
                        byte mask = (byte) (mInfo.isDescending() ? 0x7f : 0x80);
                        srcVar = srcVar.unbox().xor(mask);
                    } else if (mInfo.isDescending()) {
                        srcVar = srcVar.unbox().xor(-1);
                    }
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

            var rowUtils = mMaker.var(RowUtils.class);

            String format;
            if (!mLex) {
                format = "LE";
            } else {
                format = "BE";
                if (ColumnInfo.isFloat(plain)) {
                    String method = "encodeFloatSign";
                    if (mInfo.isDescending()) {
                        method += "Desc";
                    }
                    srcVar = rowUtils.invoke(method, srcVar);
                } else if (!mInfo.isUnsigned()) {
                    srcVar = srcVar.unbox().xor(signMask());
                } else if (mInfo.isDescending()) {
                    srcVar = srcVar.unbox().xor(-1);
                }
            }

            String methodName = "encode" + methodType + format;
            rowUtils.invoke(methodName, dstVar, offsetVar, srcVar);
            offsetVar.inc(mSize);
        }

        if (end != null) {
            end.here();
        }
    }

    @Override
    void decode(Variable dstVar, Variable srcVar, Variable offsetVar, Variable endVar) {
        decode(dstVar, srcVar, offsetVar, mInfo.isNullable());
    }

    private void decode(Variable dstVar, Variable srcVar, Variable offsetVar, boolean isNullable) {
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
                case TYPE_BOOLEAN, TYPE_BYTE, TYPE_UBYTE -> {
                    var byteVar = srcVar.aget(offsetVar);
                    offsetVar.inc(1);

                    if (plain == TYPE_BOOLEAN) {
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
                            cont = mMaker.label().goto_();
                            notNull.here();
                        }

                        if (mInfo.isDescending()) {
                            byteVar = byteVar.com();
                        }

                        valueVar.set(byteVar.cast(boolean.class));

                        if (cont != null) {
                            cont.here();
                        }
                    } else {
                        if (mLex) {
                            if (plain == TYPE_BYTE) {
                                byte mask = (byte) (mInfo.isDescending() ? 0x7f : 0x80);
                                byteVar = byteVar.xor(mask);
                            } else if (mInfo.isDescending()) {
                                byteVar = byteVar.xor((byte) 0xff);
                            }
                        }
                        valueVar = byteVar;
                    }

                    break doDecode;
                }
                case TYPE_SHORT, TYPE_USHORT, TYPE_CHAR -> methodType = "UnsignedShort";
                case TYPE_INT, TYPE_FLOAT, TYPE_UINT -> methodType = "Int";
                case TYPE_LONG, TYPE_DOUBLE, TYPE_ULONG -> methodType = "Long";
                default -> throw new AssertionError();
            }

            var rowUtils = mMaker.var(RowUtils.class);

            String methodName = "decode" + methodType + (mLex ? "BE" : "LE");
            valueVar = rowUtils.invoke(methodName, srcVar, offsetVar);
            offsetVar.inc(mSize);

            if (mLex) {
                if (isFloat(plain)) {
                    String method = "decodeFloatSign";
                    if (mInfo.isDescending()) {
                        method += "Desc";
                    }
                    valueVar = rowUtils.invoke(method, valueVar);
                } else if (!mInfo.isUnsigned()) {
                    valueVar = valueVar.xor(signMask());
                } else if (mInfo.isDescending()) {
                    valueVar = valueVar.xor(-1);
                }
            }

            valueVar = switch (plain) {
                case TYPE_SHORT, TYPE_USHORT -> valueVar.cast(short.class);
                case TYPE_CHAR -> valueVar.cast(char.class);
                case TYPE_FLOAT -> mMaker.var(Float.class).invoke("intBitsToFloat", valueVar);
                case TYPE_DOUBLE -> mMaker.var(Double.class).invoke("longBitsToDouble", valueVar);
                default -> valueVar;
            };
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

        offsetVar.inc(mSize);

        if (end != null) {
            end.here();
        }
    }

    @Override
    Variable[] decodeDiff(Variable src1Var, Variable offset1Var, Variable end1Var,
                          Variable src2Var, Variable offset2Var, Variable end2Var,
                          ColumnCodec codec2, Label same)
    {
        if (mInfo.isNullable() && mInfo.plainTypeCode() != TYPE_BOOLEAN) {
            return super.decodeDiff
                (src1Var, offset1Var, end1Var, src2Var, offset2Var, end2Var, codec2, same);
        } else {
            var dst1Var = mMaker.var(mInfo.type);
            decode(dst1Var, src1Var, offset1Var, end1Var);
            var dst2Var = mMaker.var(dst1Var);
            codec2.decode(dst2Var, src2Var, offset2Var, end2Var);
            dst1Var.ifEq(dst2Var, same);
            return new Variable[] {dst1Var, dst2Var};
        }
    }

    @Override
    boolean canFilterQuick(ColumnInfo dstInfo) {
        // The quick variant skips conversions and boxing. If no boxing is necessary, or boxing
        // is cheap (Boolean), then use the regular full decode.
        return dstInfo.typeCode == mInfo.typeCode
            && !dstInfo.type.isPrimitive() && dstInfo.plainTypeCode() != TYPE_BOOLEAN;
    }

    @Override
    Object filterQuickDecode(ColumnInfo dstInfo,
                             Variable srcVar, Variable offsetVar, Variable endVar)
    {
        var columnVar = mMaker.var(dstInfo.unboxedType());

        if (!dstInfo.isNullable()) {
            decode(columnVar, srcVar, offsetVar, false);
            return columnVar;
        }

        columnVar.set(0);
        Variable isNullVar = mMaker.var(boolean.class);
        decodeNullHeader(null, isNullVar, srcVar, offsetVar);
        Label isNull = mMaker.label();
        isNullVar.ifTrue(isNull);
        decode(columnVar, srcVar, offsetVar, false);
        isNull.here();

        return new Variable[] {columnVar, isNullVar};
    }

    @Override
    void filterQuickCompare(ColumnInfo dstInfo, Variable srcVar, Variable offsetVar,
                            int op, Object decoded, Variable argObjVar, int argNum,
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

        var argField = argObjVar.field(argFieldName(argNum));

        if (isNullVar != null) {
            compareNullHeader(isNullVar, null, argField, op, pass, fail);
        } else if (mInfo.isNullable()) {
            CompareUtils.compare(mMaker, dstInfo, columnVar, dstInfo, argField, op, pass, fail);
            return;
        }

        CompareUtils.comparePrimitives(mMaker, dstInfo, columnVar,
                                       dstInfo, argField, op, pass, fail);
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
        } else if (mSize == 4) {
            int imask = 1 << 31;
            if (mInfo.isDescending()) {
                imask = ~imask;
            }
            return imask;
        } else {
            int imask = 1 << 15;
            if (mInfo.isDescending()) {
                imask = ~imask;
            }
            return mInfo.plainTypeCode() == TYPE_CHAR ? (char) imask : (short) imask;
        }
    }
}
