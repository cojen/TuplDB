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

import org.cojen.tupl.filter.ColumnFilter;

/**
 * Encoding suitable for non-last key columns which supports nulls.
 *
 * @see RowUtils#encodeStringKey
 * @author Brian S O'Neill
 */
class KeyStringColumnCodec extends StringColumnCodec {
    KeyStringColumnCodec(ColumnInfo info, MethodMaker mm) {
        super(info, mm);
    }

    @Override
    ColumnCodec bind(MethodMaker mm) {
        return new KeyStringColumnCodec(mInfo, mm);
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
        return accum(totalVar, rowUtils.invoke("lengthStringKey", srcVar));
    }

    @Override
    void encode(Variable srcVar, Variable dstVar, Variable offsetVar) {
        String methodName = "encodeStringKey";
        if (mInfo.isDescending()) {
            methodName += "Desc";
        }
        offsetVar.set(mMaker.var(RowUtils.class).invoke(methodName, dstVar, offsetVar, srcVar));
    }

    @Override
    void decode(Variable dstVar, Variable srcVar, Variable offsetVar, Variable endVar) {
        String methodName = "decodeStringKey";
        if (mInfo.isDescending()) {
            methodName += "Desc";
        }
        var rowUtils = mMaker.var(RowUtils.class);
        var lengthVar = rowUtils.invoke("lengthStringKey", srcVar, offsetVar);
        var valueVar = rowUtils.invoke(methodName, srcVar, offsetVar, lengthVar);
        offsetVar.inc(lengthVar);
        dstVar.set(valueVar);
    }

    @Override
    void decodeSkip(Variable srcVar, Variable offsetVar, Variable endVar) {
        offsetVar.inc(mMaker.var(RowUtils.class).invoke("lengthStringKey", srcVar, offsetVar));
    }

    /**
     * Defines a byte[] arg field encoded using the string key format.
     */
    @Override
    Variable filterPrepare(int op, Variable argVar, int argNum) {
        if (ColumnFilter.isIn(op)) {
            // FIXME: array of byte arrays
            throw null;
        }

        argVar = ConvertCallSite.make(mMaker, String.class, argVar);

        var rowUtils = mMaker.var(RowUtils.class);
        var lengthVar = rowUtils.invoke("lengthStringKey", argVar);
        var bytesVar = mMaker.new_(byte[].class, lengthVar);
        String methodName = "encodeStringKey";
        if (mInfo.isDescending()) {
            methodName += "Desc";
        }
        rowUtils.invoke(methodName, bytesVar, 0, argVar);
        defineArgField(byte[].class, argFieldName(argNum)).set(bytesVar);

        return argVar;
    }

    @Override
    Object filterDecode(ColumnInfo dstInfo, Variable srcVar, Variable offsetVar, Variable endVar,
                        int op)
    {
        decodeSkip(srcVar, offsetVar, endVar);
        // Return a stable copy to the end offset.
        return offsetVar.get();
    }

    /**
     * @param decoded the string end offset
     */
    @Override
    void filterCompare(ColumnInfo dstInfo, Variable srcVar, Variable offsetVar, Variable endVar,
                       int op, Object decoded, Variable argObjVar, int argNum,
                       Label pass, Label fail)
    {
        endVar = (Variable) decoded;
        var argVar = argObjVar.field(argFieldName(argNum)).get();
        CompareUtils.compareArrays(mMaker,
                                   srcVar, offsetVar, endVar,
                                   argVar, 0, argVar.alength(),
                                   op, pass, fail);
    }

    @Override
    protected void decodeHeader(Variable srcVar, Variable offsetVar, Variable endVar,
                                Variable lengthVar, Variable isNullVar){
        throw new UnsupportedOperationException();
    }
}
