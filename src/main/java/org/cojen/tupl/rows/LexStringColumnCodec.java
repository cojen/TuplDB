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
    ColumnCodec bind(MethodMaker mm) {
        return new LexStringColumnCodec(mInfo, mm);
    }

    @Override
    protected boolean doSimilarTo(ColumnCodec codec) {
        return false;
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
        var rowUtils = mMaker.var(RowUtils.class);
        return accum(totalVar, rowUtils.invoke("lengthStringLex", srcVar));
    }

    @Override
    void encode(Variable srcVar, Variable dstVar, Variable offsetVar) {
        String methodName = "encodeStringLex";
        if (mInfo.isDescending()) {
            methodName += "Desc";
        }
        offsetVar.set(mMaker.var(RowUtils.class).invoke(methodName, dstVar, offsetVar, srcVar));
    }

    @Override
    void decode(Variable dstVar, Variable srcVar, Variable offsetVar, Variable endVar) {
        String methodName = "decodeStringLex";
        if (mInfo.isDescending()) {
            methodName += "Desc";
        }
        var rowUtils = mMaker.var(RowUtils.class);
        var lengthVar = rowUtils.invoke("lengthStringLex", srcVar, offsetVar);
        var valueVar = rowUtils.invoke(methodName, srcVar, offsetVar, lengthVar);
        offsetVar.inc(lengthVar);
        dstVar.set(valueVar);
    }

    @Override
    void decodeSkip(Variable srcVar, Variable offsetVar, Variable endVar) {
        offsetVar.inc(mMaker.var(RowUtils.class).invoke("lengthStringLex", srcVar, offsetVar));
    }

    @Override
    protected Variable filterPrepareBytes(Variable strVar) {
        var rowUtils = mMaker.var(RowUtils.class);
        var lengthVar = rowUtils.invoke("lengthStringLex", strVar);
        var bytesVar = mMaker.new_(byte[].class, lengthVar);
        String methodName = "encodeStringLex";
        if (mInfo.isDescending()) {
            methodName += "Desc";
        }
        rowUtils.invoke(methodName, bytesVar, 0, strVar);
        return bytesVar;
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
