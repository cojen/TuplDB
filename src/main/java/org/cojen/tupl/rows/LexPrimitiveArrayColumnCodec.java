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

/**
 * Encoding suitable for lexicographically ordered columns which supports nulls.
 *
 * @author Brian S O'Neill
 */
final class LexPrimitiveArrayColumnCodec extends PrimitiveArrayColumnCodec {
    LexPrimitiveArrayColumnCodec(ColumnInfo info, MethodMaker mm) {
        super(info, mm, true);
    }

    @Override
    ColumnCodec bind(MethodMaker mm) {
        return new LexPrimitiveArrayColumnCodec(mInfo, mm);
    }

    @Override
    int codecFlags() {
        return lexCodecFlags();
    }

    @Override
    void encodePrepare() {
    }

    @Override
    void encodeSkip() {
    }

    @Override
    Variable encodeSize(Variable srcVar, Variable totalVar) {
        return accum(totalVar, calcLength(mMaker.var(PrimitiveArrayUtils.class), srcVar));
    }

    private Variable calcLength(Variable rowUtils, Variable srcVar) {
        Variable lengthVar = mMaker.var(int.class);

        Label cont = null;

        if (mInfo.isNullable()) {
            Label notNull = mMaker.label();
            srcVar.ifNe(null, notNull);
            lengthVar.set(1);
            cont = mMaker.label().goto_();
            notNull.here();
        }

        lengthVar.set(rowUtils.invoke("lengthBytes32K", byteArrayLength(srcVar)));

        if (cont != null) {
            cont.here();
        }

        return lengthVar;
    }

    @Override
    void encode(Variable srcVar, Variable dstVar, Variable offsetVar) {
        doEncode(mMaker.var(PrimitiveArrayUtils.class), srcVar, dstVar, offsetVar);
    }

    /**
     * @param offsetVar can be null to use an offset of zero
     */
    private void doEncode(Variable rowUtils, Variable srcVar, Variable dstVar, Variable offsetVar) {
        if (mInfo.plainTypeCode() != ColumnInfo.TYPE_UBYTE) {
            // Convert to a byte array, or flip the sign bits if already a byte array.
            srcVar = super.filterPrepareBytes(srcVar);
        }

        Label end = null;
        if (mInfo.isNullable()) {
            end = mMaker.label();
            encodeNullHeaderIfNull(end, srcVar, dstVar, offsetVar);
        }

        String methodName = "encodeBytes32K";
        if (mInfo.isDescending()) {
            methodName += "Desc";
        }

        if (offsetVar == null) {
            rowUtils.invoke(methodName, dstVar, 0, srcVar);
        } else {
            offsetVar.set(rowUtils.invoke(methodName, dstVar, offsetVar, srcVar));
        }

        if (end != null) {
            end.here();
        }
    }

    @Override
    void decode(Variable dstVar, Variable srcVar, Variable offsetVar, Variable endVar) {
        String methodName = "decodeBytes32K";
        if (mInfo.isDescending()) {
            methodName += "Desc";
        }
        var rowUtils = mMaker.var(PrimitiveArrayUtils.class);
        var lengthVar = rowUtils.invoke("lengthBytes32K", srcVar, offsetVar);
        var arrayVar = rowUtils.invoke(methodName, srcVar, offsetVar, lengthVar);
        offsetVar.inc(lengthVar);

        int plain = mInfo.plainTypeCode();
        if (plain == ColumnInfo.TYPE_BYTE) {
            // No need to make an extra copy since the new instance isn't shared by anything.
            if (!mInfo.isNullable()) {
                rowUtils.invoke("signFlip", arrayVar, 0, arrayVar.alength());
            } else {
                Label isNull = mMaker.label();
                arrayVar.ifEq(null, isNull);
                rowUtils.invoke("signFlip", arrayVar, 0, arrayVar.alength());
                isNull.here();
            }
        } else if (plain != ColumnInfo.TYPE_UBYTE) {
            var newArrayVar = mMaker.var(mInfo.type);
            if (!mInfo.isNullable()) {
                decodeByteArray(newArrayVar, arrayVar, 0, arrayVar.alength());
            } else {
                Label notNull = mMaker.label();
                arrayVar.ifNe(null, notNull);
                Label cont = mMaker.label();
                newArrayVar.set(null);
                mMaker.goto_(cont);
                notNull.here();
                decodeByteArray(newArrayVar, arrayVar, 0, arrayVar.alength());
                cont.here();
            }
            arrayVar = newArrayVar;
        }

        dstVar.set(arrayVar);
    }

    @Override
    void decodeSkip(Variable srcVar, Variable offsetVar, Variable endVar) {
        var utilsVar = mMaker.var(PrimitiveArrayUtils.class);
        offsetVar.inc(utilsVar.invoke("lengthBytes32K", srcVar, offsetVar));
    }

    @Override
    protected Variable filterPrepareBytes(Variable argVar) {
        var rowUtils = mMaker.var(PrimitiveArrayUtils.class);
        var bytesVar = mMaker.new_(byte[].class, calcLength(rowUtils, argVar));
        doEncode(rowUtils, argVar, bytesVar, null);
        return bytesVar;
    }

    @Override
    protected boolean compareBytesUnsigned() {
        return true;
    }

    @Override
    boolean canFilterQuick(ColumnInfo dstInfo) {
        return dstInfo.typeCode == mInfo.typeCode;
    }

    @Override
    Object filterQuickDecode(ColumnInfo dstInfo,
                             Variable srcVar, Variable offsetVar, Variable endVar)
    {
        decodeSkip(srcVar, offsetVar, endVar);
        // Return a stable copy to the end offset.
        return offsetVar.get();
    }

    /**
     * @param decoded the string end offset
     */
    @Override
    void filterQuickCompare(ColumnInfo dstInfo, Variable srcVar, Variable offsetVar,
                            int op, Object decoded, Variable argObjVar, int argNum,
                            Label pass, Label fail)
    {
        filterQuickCompareLex(dstInfo, srcVar, offsetVar, (Variable) decoded,
                              op, argObjVar, argNum, pass, fail);
    }

    @Override
    protected void decodeHeader(Variable srcVar, Variable offsetVar, Variable endVar,
                                Variable lengthVar, Variable isNullVar)
    {
        throw new UnsupportedOperationException();
    }
}
