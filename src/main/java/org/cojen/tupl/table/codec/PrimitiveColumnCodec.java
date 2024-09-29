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

package org.cojen.tupl.table.codec;

import java.util.function.Function;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.CompareUtils;
import org.cojen.tupl.table.RowUtils;

import static org.cojen.tupl.table.ColumnInfo.*;

/**
 * Makes code for encoding and decoding primitive type columns.
 *
 * @author Brian S O'Neill
 */
final class PrimitiveColumnCodec extends ColumnCodec {
    private final int mFlags;
    private final int mSize;

    /**
     * @param info non-null
     * @param mm is null for stateless instance
     * @param size byte count
     */
    PrimitiveColumnCodec(ColumnInfo info, MethodMaker mm, int flags, int size) {
        super(info, mm);
        mFlags = flags & ~F_LAST;
        mSize = size;
    }

    @Override
    public ColumnCodec bind(MethodMaker mm) {
        return new PrimitiveColumnCodec(info, mm, mFlags, mSize);
    }

    @Override
    protected boolean doEquals(Object obj) {
        var other = (PrimitiveColumnCodec) obj;
        if (mFlags != other.mFlags || mSize != other.mSize) {
            return false;
        }
        int typeCode = info.typeCode;
        int otherTypeCode = other.info.typeCode;
        if (!isLex()) {
            typeCode = ColumnInfo.unorderedTypeCode(typeCode);
            otherTypeCode = ColumnInfo.unorderedTypeCode(otherTypeCode);
        }
        return typeCode == otherTypeCode;
    }

    @Override
    public int doHashCode() {
        return info.unorderedTypeCode();
    }

    @Override
    public int codecFlags() {
        return mFlags;
    }

    @Override
    public int minSize() {
        // Return just the header size if nullable.
        return info.isNullable() ? 1 : mSize;
    }

    @Override
    public boolean encodePrepare() {
        return false;
    }

    @Override
    public void encodeTransfer(ColumnCodec codec, Function<Variable, Variable> transfer) {
    }

    @Override
    public void encodeSkip() {
    }

    @Override
    public Variable encodeSize(Variable srcVar, Variable totalVar) {
        if (info.isNullable() && info.plainTypeCode() != TYPE_BOOLEAN) {
            if (totalVar == null) {
                totalVar = maker.var(int.class).set(0);
            }
            Label isNull = maker.label();
            srcVar.ifEq(null, isNull);
            totalVar.inc(mSize);
            isNull.here();
        }

        return totalVar;
    }

    @Override
    public void encode(Variable srcVar, Variable dstVar, Variable offsetVar) {
        Label end = null;

        int plain = info.plainTypeCode();

        if (info.isNullable() && plain != TYPE_BOOLEAN) {
            end = maker.label();
            encodeNullHeader(end, srcVar, dstVar, offsetVar);
        }

        doEncode: {
            String methodType;

            switch (plain) {
            case TYPE_BOOLEAN: case TYPE_BYTE: case TYPE_UBYTE: {
                if (plain == TYPE_BOOLEAN) {
                    byte f, t, n;
                    n = nullByte();
                    if (!isLex()) {
                        f = 0;
                        t = 1;
                    } else {
                        f = (byte) 0x80;
                        t = (byte) 0x81;
                        if (info.isDescending()) {
                            f = (byte) ~f;
                            t = (byte) ~t;
                        }
                    }

                    var byteVar = maker.var(byte.class);
                    Label cont = maker.label();

                    if (info.isNullable()) {
                        Label notNull = maker.label();
                        srcVar.ifNe(null, notNull);
                        byteVar.set(n);
                        maker.goto_(cont);
                        notNull.here();
                    }

                    Label trueCase = maker.label();
                    srcVar.ifTrue(trueCase);
                    byteVar.set(f);
                    maker.goto_(cont);
                    trueCase.here();
                    byteVar.set(t);
                    srcVar = byteVar;

                    cont.here();
                } else if (isLex()) {
                    if (plain == TYPE_BYTE) {
                        byte mask = (byte) (info.isDescending() ? 0x7f : 0x80);
                        srcVar = srcVar.unbox().xor(mask);
                    } else if (info.isDescending()) {
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
                srcVar = maker.var(Float.class).invoke("floatToRawIntBits", srcVar);
            case TYPE_INT: case TYPE_UINT:
                methodType = "Int";
                break;

            case TYPE_DOUBLE:
                srcVar = maker.var(Double.class).invoke("doubleToRawLongBits", srcVar);
            case TYPE_LONG: case TYPE_ULONG:
                methodType = "Long";
                break;

            default:
                throw new AssertionError();
            }

            var rowUtils = maker.var(RowUtils.class);

            String format;
            if (!isLex()) {
                format = "LE";
            } else {
                format = "BE";
                if (ColumnInfo.isFloat(plain)) {
                    String method = "encodeFloatSign";
                    if (info.isDescending()) {
                        method += "Desc";
                    }
                    srcVar = rowUtils.invoke(method, srcVar);
                } else if (!info.isUnsigned()) {
                    srcVar = srcVar.unbox().xor(signMask());
                } else if (info.isDescending()) {
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
    public void decode(Variable dstVar, Variable srcVar, Variable offsetVar, Variable endVar) {
        decode(dstVar, srcVar, offsetVar, info.isNullable());
    }

    private void decode(Variable dstVar, Variable srcVar, Variable offsetVar, boolean isNullable) {
        Label end = null;

        int plain = info.plainTypeCode();

        if (isNullable && plain != TYPE_BOOLEAN) {
            end = maker.label();
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
                            valueVar = maker.var(boolean.class);
                        } else {
                            byte n = nullByte();
                            valueVar = maker.var(Boolean.class);
                            Label notNull = maker.label();
                            byteVar.ifNe(n, notNull);
                            valueVar.set(null);
                            cont = maker.label().goto_();
                            notNull.here();
                        }

                        if (info.isDescending()) {
                            byteVar = byteVar.com();
                        }

                        valueVar.set(byteVar.cast(boolean.class));

                        if (cont != null) {
                            cont.here();
                        }
                    } else {
                        if (isLex()) {
                            if (plain == TYPE_BYTE) {
                                byte mask = (byte) (info.isDescending() ? 0x7f : 0x80);
                                byteVar = byteVar.xor(mask);
                            } else if (info.isDescending()) {
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

            var rowUtils = maker.var(RowUtils.class);

            String methodName = "decode" + methodType + (isLex() ? "BE" : "LE");
            valueVar = rowUtils.invoke(methodName, srcVar, offsetVar);
            offsetVar.inc(mSize);

            if (isLex()) {
                if (isFloat(plain)) {
                    String method = "decodeFloatSign";
                    if (info.isDescending()) {
                        method += "Desc";
                    }
                    valueVar = rowUtils.invoke(method, valueVar);
                } else if (!info.isUnsigned()) {
                    valueVar = valueVar.xor(signMask());
                } else if (info.isDescending()) {
                    valueVar = valueVar.xor(-1);
                }
            }

            valueVar = switch (plain) {
                case TYPE_SHORT, TYPE_USHORT -> valueVar.cast(short.class);
                case TYPE_CHAR -> valueVar.cast(char.class);
                case TYPE_FLOAT -> maker.var(Float.class).invoke("intBitsToFloat", valueVar);
                case TYPE_DOUBLE -> maker.var(Double.class).invoke("longBitsToDouble", valueVar);
                default -> valueVar;
            };
        }

        dstVar.set(valueVar);

        if (end != null) {
            end.here();
        }
    }

    @Override
    public void decodeSkip(Variable srcVar, Variable offsetVar, Variable endVar) {
        Label end = null;

        if (info.isNullable() && info.plainTypeCode() != TYPE_BOOLEAN) {
            end = maker.label();
            decodeNullHeader(end, null, srcVar, offsetVar);
        }

        offsetVar.inc(mSize);

        if (end != null) {
            end.here();
        }
    }

    @Override
    public boolean canFilterQuick(ColumnInfo dstInfo) {
        // The quick variant skips conversions and boxing. If no boxing is necessary, or boxing
        // is cheap (Boolean), then use the regular full decode.
        return dstInfo.typeCode == info.typeCode
            && !dstInfo.type.isPrimitive() && dstInfo.plainTypeCode() != TYPE_BOOLEAN;
    }

    @Override
    public Object filterQuickDecode(ColumnInfo dstInfo,
                                    Variable srcVar, Variable offsetVar, Variable endVar)
    {
        var columnVar = maker.var(dstInfo.unboxedType());

        if (!dstInfo.isNullable()) {
            decode(columnVar, srcVar, offsetVar, false);
            return columnVar;
        }

        columnVar.set(0);
        Variable isNullVar = maker.var(boolean.class);
        decodeNullHeader(null, isNullVar, srcVar, offsetVar);
        Label isNull = maker.label();
        isNullVar.ifTrue(isNull);
        decode(columnVar, srcVar, offsetVar, false);
        isNull.here();

        return new Variable[] {columnVar, isNullVar};
    }

    @Override
    public void filterQuickCompare(ColumnInfo dstInfo, Variable srcVar, Variable offsetVar,
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
        } else if (info.isNullable()) {
            CompareUtils.compare(maker, dstInfo, columnVar, dstInfo, argField, op, pass, fail);
            return;
        }

        CompareUtils.comparePrimitives(maker, dstInfo, columnVar,
                                       dstInfo, argField, op, pass, fail);
    }

    /**
     * Must only be called for size 2, 4, or 8.
     */
    private Object signMask() {
        if (mSize == 8) {
            long lmask = 1L << 63;
            if (info.isDescending()) {
                lmask = ~lmask;
            }
            return lmask;
        } else if (mSize == 4) {
            int imask = 1 << 31;
            if (info.isDescending()) {
                imask = ~imask;
            }
            return imask;
        } else {
            int imask = 1 << 15;
            if (info.isDescending()) {
                imask = ~imask;
            }
            return info.plainTypeCode() == TYPE_CHAR ? (char) imask : (short) imask;
        }
    }
}
