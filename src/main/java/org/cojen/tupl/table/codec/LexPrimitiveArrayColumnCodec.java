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

package org.cojen.tupl.table.codec;

import java.util.function.Function;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.PrimitiveArrayUtils;

/**
 * Encoding suitable for lexicographically ordered columns which supports nulls.
 *
 * @author Brian S O'Neill
 */
final class LexPrimitiveArrayColumnCodec extends PrimitiveArrayColumnCodec {
    LexPrimitiveArrayColumnCodec(ColumnInfo info, MethodMaker mm) {
        super(info, mm, F_LEX);
    }

    @Override
    public ColumnCodec bind(MethodMaker mm) {
        return new LexPrimitiveArrayColumnCodec(info, mm);
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
        return accum(totalVar, calcLength(maker.var(PrimitiveArrayUtils.class), srcVar));
    }

    private Variable calcLength(Variable rowUtils, Variable srcVar) {
        Variable lengthVar = maker.var(int.class);

        Label cont = null;

        if (info.isNullable()) {
            Label notNull = maker.label();
            srcVar.ifNe(null, notNull);
            lengthVar.set(1);
            cont = maker.label().goto_();
            notNull.here();
        }

        lengthVar.set(rowUtils.invoke("lengthBytes32K", byteArrayLength(srcVar)));

        if (cont != null) {
            cont.here();
        }

        return lengthVar;
    }

    @Override
    public void encode(Variable srcVar, Variable dstVar, Variable offsetVar) {
        doEncode(maker.var(PrimitiveArrayUtils.class), srcVar, dstVar, offsetVar);
    }

    /**
     * @param offsetVar can be null to use an offset of zero
     */
    private void doEncode(Variable rowUtils, Variable srcVar, Variable dstVar, Variable offsetVar) {
        if (info.plainTypeCode() != ColumnInfo.TYPE_UBYTE) {
            // Convert to a byte array, or flip the sign bits if already a byte array.
            srcVar = super.filterPrepareBytes(srcVar);
        }

        Label end = null;
        if (info.isNullable()) {
            end = maker.label();
            encodeNullHeaderIfNull(end, srcVar, dstVar, offsetVar);
        }

        String methodName = "encodeBytes32K";
        if (info.isDescending()) {
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
    public void decode(Variable dstVar, Variable srcVar, Variable offsetVar, Variable endVar) {
        String methodName = "decodeBytes32K";
        if (info.isDescending()) {
            methodName += "Desc";
        }
        var rowUtils = maker.var(PrimitiveArrayUtils.class);
        var lengthVar = rowUtils.invoke("lengthBytes32K", srcVar, offsetVar);
        var arrayVar = rowUtils.invoke(methodName, srcVar, offsetVar, lengthVar);
        offsetVar.inc(lengthVar);

        int plain = info.plainTypeCode();
        if (plain == ColumnInfo.TYPE_BYTE) {
            // No need to make an extra copy since the new instance isn't shared by anything.
            if (!info.isNullable()) {
                rowUtils.invoke("signFlip", arrayVar, 0, arrayVar.alength());
            } else {
                Label isNull = maker.label();
                arrayVar.ifEq(null, isNull);
                rowUtils.invoke("signFlip", arrayVar, 0, arrayVar.alength());
                isNull.here();
            }
        } else if (plain != ColumnInfo.TYPE_UBYTE) {
            var newArrayVar = maker.var(info.type);
            if (!info.isNullable()) {
                decodeByteArray(newArrayVar, arrayVar, 0, arrayVar.alength());
            } else {
                Label notNull = maker.label();
                arrayVar.ifNe(null, notNull);
                Label cont = maker.label();
                newArrayVar.set(null);
                maker.goto_(cont);
                notNull.here();
                decodeByteArray(newArrayVar, arrayVar, 0, arrayVar.alength());
                cont.here();
            }
            arrayVar = newArrayVar;
        }

        dstVar.set(arrayVar);
    }

    @Override
    public void decodeSkip(Variable srcVar, Variable offsetVar, Variable endVar) {
        var utilsVar = maker.var(PrimitiveArrayUtils.class);
        offsetVar.inc(utilsVar.invoke("lengthBytes32K", srcVar, offsetVar));
    }

    @Override
    protected Variable filterPrepareBytes(Variable argVar) {
        var rowUtils = maker.var(PrimitiveArrayUtils.class);
        var bytesVar = maker.new_(byte[].class, calcLength(rowUtils, argVar));
        doEncode(rowUtils, argVar, bytesVar, null);
        return bytesVar;
    }

    @Override
    protected boolean compareBytesUnsigned() {
        return true;
    }

    @Override
    public boolean canFilterQuick(ColumnInfo dstInfo) {
        return dstInfo.typeCode == info.typeCode;
    }

    @Override
    public Object filterQuickDecode(ColumnInfo dstInfo,
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
    public void filterQuickCompare(ColumnInfo dstInfo, Variable srcVar, Variable offsetVar,
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
