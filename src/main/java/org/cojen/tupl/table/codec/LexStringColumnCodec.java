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
 * @see RowUtils#encodeStringLex
 * @author Brian S O'Neill
 */
final class LexStringColumnCodec extends StringColumnCodec {
    LexStringColumnCodec(ColumnInfo info, MethodMaker mm) {
        super(info, mm);
    }

    @Override
    public ColumnCodec bind(MethodMaker mm) {
        return new LexStringColumnCodec(info, mm);
    }

    @Override
    public int codecFlags() {
        return F_LEX;
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
        var rowUtils = maker.var(RowUtils.class);
        return accum(totalVar, rowUtils.invoke("lengthStringLex", srcVar));
    }

    @Override
    public void encode(Variable srcVar, Variable dstVar, Variable offsetVar) {
        doEncode(maker.var(RowUtils.class), srcVar, dstVar, offsetVar);
    }

    /**
     * @param offsetVar can be null to use an offset of zero
     */
    private void doEncode(Variable rowUtils, Variable srcVar, Variable dstVar, Variable offsetVar) {
        Label end = null;
        if (info.isNullable()) {
            end = maker.label();
            encodeNullHeaderIfNull(end, srcVar, dstVar, offsetVar);
        }

        String methodName = "encodeStringLex";
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
        String methodName = "decodeStringLex";
        if (info.isDescending()) {
            methodName += "Desc";
        }
        var rowUtils = maker.var(RowUtils.class);
        var lengthVar = rowUtils.invoke("lengthStringLex", srcVar, offsetVar);
        var valueVar = rowUtils.invoke(methodName, srcVar, offsetVar, lengthVar);
        offsetVar.inc(lengthVar);
        dstVar.set(valueVar);
    }

    @Override
    public void decodeSkip(Variable srcVar, Variable offsetVar, Variable endVar) {
        offsetVar.inc(maker.var(RowUtils.class).invoke("lengthStringLex", srcVar, offsetVar));
    }

    @Override
    protected Variable filterPrepareBytes(Variable strVar) {
        var rowUtils = maker.var(RowUtils.class);
        var lengthVar = rowUtils.invoke("lengthStringLex", strVar);
        var bytesVar = maker.new_(byte[].class, lengthVar);
        doEncode(rowUtils, strVar, bytesVar, null);
        return bytesVar;
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
