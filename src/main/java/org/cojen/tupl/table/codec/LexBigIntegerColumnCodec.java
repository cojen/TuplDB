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
import org.cojen.tupl.table.RowUtils;

/**
 * Encoding suitable for lexicographically ordered columns which supports nulls.
 *
 * @see RowUtils#encodeBigIntegerLex
 * @author Brian S O'Neill
 */
final class LexBigIntegerColumnCodec extends BigIntegerColumnCodec {
    private Variable mBytesVar;

    LexBigIntegerColumnCodec(ColumnInfo info, MethodMaker mm) {
        super(info, mm);
    }

    @Override
    public ColumnCodec bind(MethodMaker mm) {
        return new LexBigIntegerColumnCodec(info, mm);
    }

    @Override
    protected boolean doEquals(Object obj) {
        return equalOrdering(obj);
    }

    @Override
    public int codecFlags() {
        return F_LEX;
    }

    @Override
    public int minSize() {
        return 1;
    }

    @Override
    public boolean encodePrepare() {
        mBytesVar = maker.var(byte[].class);
        return true;
    }

    @Override
    public void encodeTransfer(ColumnCodec codec, Function<Variable, Variable> transfer) {
        var dst = (LexBigIntegerColumnCodec) codec;
        dst.mBytesVar = transfer.apply(mBytesVar);
    }

    @Override
    public void encodeSkip() {
        mBytesVar.set(null);
    }

    @Override
    public Variable encodeSize(Variable srcVar, Variable totalVar) {
        return encodeSize(srcVar, totalVar, mBytesVar);
    }

    private Variable encodeSize(Variable srcVar, Variable totalVar, Variable bytesVar) {
        if (totalVar == null) {
            totalVar = maker.var(int.class).set(0);
        }
        Label notNull = maker.label();
        srcVar.ifNe(null, notNull);
        bytesVar.set(null);
        Label cont = maker.label().goto_();
        notNull.here();
        bytesVar.set(srcVar.invoke("toByteArray"));
        var lengthVar = bytesVar.alength();
        totalVar.inc(lengthVar);
        lengthVar.ifLt(0x7f, cont);
        totalVar.inc(4);
        cont.here();
        return totalVar;
    }

    @Override
    public void encode(Variable srcVar, Variable dstVar, Variable offsetVar) {
        doEncode(mBytesVar, dstVar, offsetVar);
    }

    /**
     * @param offsetVar can be null to use an offset of zero
     */
    private void doEncode(Variable srcVar, Variable dstVar, Variable offsetVar) {
        Label end = null;
        if (info.isNullable()) {
            end = maker.label();
            encodeNullHeaderIfNull(end, srcVar, dstVar, offsetVar);
        }

        String methodName = "encodeBigIntegerLex";
        if (info.isDescending()) {
            methodName += "Desc";
        }

        var rowUtils = maker.var(RowUtils.class);

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
        var rowUtils = maker.var(RowUtils.class);
        var resultVar = rowUtils.invoke("decodeBigIntegerLexHeader", srcVar, offsetVar);
        offsetVar.set(resultVar.cast(int.class));

        var lengthVar = resultVar.shr(32).cast(int.class);

        if (dstVar == null) {
            offsetVar.inc(lengthVar);
        } else {
            Label notNull = maker.label();
            lengthVar.ifNe(0, notNull);
            dstVar.set(null);
            Label cont = maker.label().goto_();
            notNull.here();
            if (info.isDescending()) {
                rowUtils.invoke("flip", srcVar, offsetVar, lengthVar);
                Label tryStart = maker.label().here();
                dstVar.set(maker.new_(dstVar, srcVar, offsetVar, lengthVar));
                maker.finally_(tryStart, () -> {
                    rowUtils.invoke("flip", srcVar, offsetVar, lengthVar);
                });
            } else {
                dstVar.set(maker.new_(dstVar, srcVar, offsetVar, lengthVar));
            }
            offsetVar.inc(lengthVar);
            cont.here();
        }
    }

    @Override
    public void decodeSkip(Variable srcVar, Variable offsetVar, Variable endVar) {
        decode(null, srcVar, offsetVar, endVar);
    }

    @Override
    protected Variable filterPrepareBytes(Variable argVar) {
        var bytesVar = maker.var(byte[].class);
        var lengthVar = encodeSize(argVar, maker.var(int.class).set(minSize()), bytesVar);
        var encodedBytesVar = maker.new_(byte[].class, lengthVar);
        doEncode(bytesVar, encodedBytesVar, null);
        return encodedBytesVar;
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
     * @param decoded the string end offset, unless a String compare should be performed
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
