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

import org.cojen.tupl.filter.ColumnFilter;

/**
 * Abstract class for encoding and decoding columns that can be encoded as a variable length
 * string of bytes.
 *
 * @author Brian S O'Neill
 */
abstract class BytesColumnCodec extends ColumnCodec {
    /**
     * @param info non-null
     * @param mm is null for stateless instance
     */
    BytesColumnCodec(ColumnInfo info, MethodMaker mm) {
        super(info, mm);
    }

    @Override
    int minSize() {
        return 0;
    }

    /**
     * Defines a byte[] arg field set to null or an encoded string of bytes, and also defines a
     * field with the original argument.
     */
    @Override
    Variable filterPrepare(boolean in, Variable argVar, int argNum) {
        argVar = super.filterPrepare(in, argVar, argNum);

        Variable bytesVar;

        if (in) {
            var lengthVar = argVar.alength();
            final var fargVar = argVar;
            bytesVar = ConvertUtils.convertArray(mMaker, byte[][].class, lengthVar, ixVar -> {
                return filterPrepareBytes(fargVar.aget(ixVar));
            });
        } else {
            bytesVar = filterPrepareBytes(argVar);
        }

        defineArgField(bytesVar, argFieldName(argNum, "bytes")).set(bytesVar);

        return argVar;
    }

    /**
     * Called by filterPrepare.
     *
     * @return byte[] variable
     */
    protected abstract Variable filterPrepareBytes(Variable argVar);

    /**
     * Returns true if the prepared byte array is suitable for unsigned comparison.
     */
    protected abstract boolean compareBytesUnsigned();

    @Override
    boolean canFilterQuick(ColumnInfo dstInfo) {
        return dstInfo.plainTypeCode() == mInfo.plainTypeCode();
    }

    @Override
    Object filterQuickDecode(ColumnInfo dstInfo,
                             Variable srcVar, Variable offsetVar, Variable endVar)
    {
        Variable lengthVar = mMaker.var(int.class);
        Variable isNullVar = mInfo.isNullable() ? mMaker.var(boolean.class) : null;

        decodeHeader(srcVar, offsetVar, endVar, lengthVar, isNullVar);

        Variable dataOffsetVar = offsetVar.get(); // need a stable copy
        offsetVar.inc(lengthVar);

        return new Variable[] {dataOffsetVar, lengthVar, isNullVar};
    }


    /**
     * Called by filterQuickDecode to decode the string header and advance the offset to the
     * start of the string data.
     *
     * @param srcVar source byte array
     * @param offsetVar int type; is incremented as a side-effect
     * @param endVar end offset, which when null implies the end of the array
     * @param lengthVar set to the decoded length; must be definitely assigned
     * @param isNullVar set to true/false if applicable; must be definitely assigned for
     * nullable strings
     */
    protected abstract void decodeHeader(Variable srcVar, Variable offsetVar, Variable endVar,
                                         Variable lengthVar, Variable isNullVar);

    /**
     * Called by implementations of decodeHeader which add one to the length for encoding null.
     *
     * @param lengthVar already decoded but not yet adjusted
     * @param isNullVar is definitely assigned
     */
    protected void decodeNullableLength(Variable lengthVar, Variable isNullVar) {
        // Actual length is encoded plus one, and zero means null.
        Label notNull = mMaker.label();
        lengthVar.ifNe(0, notNull);
        isNullVar.set(true);
        Label cont = mMaker.label();
        mMaker.goto_(cont);
        notNull.here();
        isNullVar.set(false);
        lengthVar.inc(-1);
        cont.here();
    }

    @Override
    void filterQuickCompare(ColumnInfo dstInfo, Variable srcVar, Variable offsetVar,
                            int op, Object decoded, Variable argObjVar, int argNum,
                            Label pass, Label fail)
    {
        var decodedVars = (Variable[]) decoded;

        if (!dstInfo.isNullable() && mInfo.isNullable()) {
            Label cont = mMaker.label();
            Variable isNullVar = decodedVars[2];
            isNullVar.ifFalse(cont);
            var columnVar = mMaker.var(dstInfo.type);
            Converter.setDefault(mMaker, dstInfo, columnVar);
            var argField = argObjVar.field(argFieldName(argNum));
            CompareUtils.compare(mMaker, dstInfo, columnVar, dstInfo, argField, op, pass, fail);
            cont.here();
        }

        var argVar = argObjVar.field(argFieldName(argNum, "bytes")).get();

        if (ColumnFilter.isIn(op)) {
            CompareUtils.compareIn(mMaker, argVar, op, pass, fail, (a, p, f) -> {
                compareQuickElement(srcVar, ColumnFilter.OP_EQ, decodedVars, a, p, f);
            });
        } else {
            compareQuickElement(srcVar, op, decodedVars, argVar, pass, fail);
        }
    }

    /**
     * @param decodedVars dataOffsetVar, lengthVar, and optional isNullVar
     * @param argVar byte array
     */
    private void compareQuickElement(Variable srcVar,
                                     int op, Variable[] decodedVars, Variable argVar,
                                     Label pass, Label fail)
    {
        Variable dataOffsetVar = decodedVars[0];
        Variable lengthVar = decodedVars[1];
        Variable isNullVar = decodedVars[2];

        Label notNull = mMaker.label();
        argVar.ifNe(null, notNull);
        // Argument is null...
        Label mismatch = CompareUtils.selectColumnToNullArg(op, pass, fail);
        if (isNullVar != null) {
            Label match = CompareUtils.selectNullColumnToNullArg(op, pass, fail);
            if (match != mismatch) {
                isNullVar.ifTrue(match);
            }
        }
        mMaker.goto_(mismatch);

        // Argument isn't null...
        notNull.here();
        if (isNullVar != null) {
            isNullVar.ifTrue(CompareUtils.selectNullColumnToArg(op, pass, fail));
        }

        CompareUtils.compareArrays(mMaker, compareBytesUnsigned(),
                                   srcVar, dataOffsetVar, dataOffsetVar.add(lengthVar),
                                   argVar, 0, argVar.alength(),
                                   op, pass, fail);
    }

    /**
     * Implementation of filterQuickCompare method for lexicographically ordered encodings.
     * Doesn't perform any special null handling because that must be encoded directly in the
     * bytes. Supports descending keys.
     *
     * @param dstInfo current definition for column
     * @param srcVar source byte array
     * @param offsetVar int type; must not be modified
     * @param endVar non-null end offset
     * @param op defined in ColumnFilter
     * @param argObjVar object which contains fields prepared earlier
     * @param argNum zero-based filter argument number
     * @param pass branch here when comparison passes
     * @param fail branch here when comparison fails
     */
    protected void filterQuickCompareLex(ColumnInfo dstInfo,
                                         Variable srcVar, Variable offsetVar, Variable endVar,
                                         int op, Variable argObjVar, int argNum,
                                         Label pass, Label fail)
    {
        var argVar = argObjVar.field(argFieldName(argNum, "bytes")).get();

        if (dstInfo.isDescending()) {
            op = ColumnFilter.descendingOperator(op);
        }

        if (ColumnFilter.isIn(op)) {
            CompareUtils.compareIn(mMaker, argVar, op, pass, fail, (a, p, f) -> {
                    CompareUtils.compareArrays(mMaker, true,
                                           srcVar, offsetVar, endVar,
                                           a, 0, a.alength(),
                                           ColumnFilter.OP_EQ, p, f);
            });
        } else {
            CompareUtils.compareArrays(mMaker, true,
                                       srcVar, offsetVar, endVar,
                                       argVar, 0, argVar.alength(),
                                       op, pass, fail);
        }
    }
}
