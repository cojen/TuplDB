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

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import static org.cojen.tupl.rows.ColumnInfo.*;

/**
 * Abstract class for encoding and decoding primitive array columns.
 *
 * @author Brian S O'Neill
 */
abstract class PrimitiveArrayColumnCodec extends BytesColumnCodec {
    protected final boolean mLex;

    // Power-of-2 number of bits per element.
    protected final int mBitPow;

    /**
     * @param info non-null
     * @param mm is null for stateless instance
     */
    PrimitiveArrayColumnCodec(ColumnInfo info, MethodMaker mm, boolean lex) {
        super(info, mm);

        mLex = lex;

        int typeCode = info.plainTypeCode();

        mBitPow = switch (typeCode) {
            default -> typeCode & 0b111;
            case ColumnInfo.TYPE_FLOAT -> 5;
            case ColumnInfo.TYPE_DOUBLE -> 6;
        };
    }

    @Override
    protected final boolean doEquals(Object obj) {
        var other = (PrimitiveArrayColumnCodec) obj;
        if (mLex != other.mLex || mBitPow != other.mBitPow) {
            return false;
        }
        int typeCode = mInfo.typeCode;
        int otherTypeCode = other.mInfo.typeCode;
        if (!mLex) {
            typeCode = ColumnInfo.unorderedTypeCode(typeCode);
            otherTypeCode = ColumnInfo.unorderedTypeCode(otherTypeCode);
        }
        return typeCode == otherTypeCode;
    }

    @Override
    public final int doHashCode() {
        return mInfo.unorderedTypeCode();
    }

    @Override
    protected Variable filterPrepareBytes(Variable argVar) {
        final Variable bytesVar = mMaker.var(byte[].class);
        Label cont = null;

        if (mInfo.isNullable()) {
            Label notNull = mMaker.label();
            argVar.ifNe(null, notNull);
            bytesVar.set(null);
            cont = mMaker.label().goto_();
            notNull.here();
        }

        bytesVar.set(mMaker.new_(byte[].class, byteArrayLength(argVar)));
        encodeByteArray(argVar, bytesVar, 0);

        if (cont != null) {
            cont.here();
        }

        return bytesVar;
    }

    @Override
    protected boolean compareBytesUnsigned() {
        return mLex || mInfo.isUnsigned() || ColumnInfo.isFloat(mInfo.plainTypeCode());
    }

    /**
     * Returns the number of bytes needed to encode the array.
     */
    protected Variable byteArrayLength(Variable srcVar) {
        var lengthVar = srcVar.alength();
        int shift = mBitPow - 3;
        if (shift > 0) {
            lengthVar = lengthVar.shl(shift);
        }
        return lengthVar;
    }

    /**
     * @param srcVar primitive array
     * @param dstVar byte array
     * @param offset variable or constant
     */
    protected void encodeByteArray(Variable srcVar, Variable dstVar, Object offset) {
        var utilsVar = mMaker.var(PrimitiveArrayUtils.class);
        if (mBitPow == 3) {
            var lengthVar = srcVar.alength();
            mMaker.var(System.class).invoke("arraycopy", srcVar, 0, dstVar, offset, lengthVar);
            if (mLex && !mInfo.isUnsigned()) {
                utilsVar.invoke("signFlip", dstVar, offset, lengthVar);
            }
        } else {
            utilsVar.invoke("encodeArray" + methodSuffix(), dstVar, offset, srcVar);
        }
    }

    /**
     * @param dstVar primitive array
     * @param srcVar byte array
     * @param offset variable or constant
     * @param length variable or constant
     */
    protected void decodeByteArray(Variable dstVar, Variable srcVar, Object offset, Object length) {
        var utilsVar = mMaker.var(PrimitiveArrayUtils.class);

        Variable valueVar;
        if (mBitPow == 3) {
            valueVar = mMaker.new_(byte[].class, length);
            mMaker.var(System.class).invoke("arraycopy", srcVar, offset, valueVar, 0, length);
            if (mLex && !mInfo.isUnsigned()) {
                utilsVar.invoke("signFlip", valueVar, 0, length);
            }
        } else {
            String method = switch (mInfo.plainTypeCode()) {
                case TYPE_BOOLEAN -> "Boolean";
                case TYPE_USHORT  -> "Short";
                case TYPE_UINT    -> "Int";
                case TYPE_ULONG   -> "Long";
                case TYPE_SHORT   -> "Short";
                case TYPE_INT     -> "Int";
                case TYPE_LONG    -> "Long";
                case TYPE_FLOAT   -> "Float";
                case TYPE_DOUBLE  -> "Double";
                case TYPE_CHAR    -> "Char";
                default -> throw new AssertionError();
            };
            method = "decode" + method + "Array" + methodSuffix();
            valueVar = utilsVar.invoke(method, srcVar, offset, length);
        }

        dstVar.set(valueVar);
    }

    protected String methodSuffix() {
        // Note that descending order isn't supported, so fewer key formats to implement. Only
        // LexPrimitiveArrayColumnCodec supports descending order.
        return mBitPow == 0 ? "" : ((mLex && !mInfo.isUnsigned()) ? "Lex" : "BE");
    }
}
