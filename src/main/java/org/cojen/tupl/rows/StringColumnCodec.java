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

import java.nio.charset.StandardCharsets;

import org.cojen.maker.Field;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

/**
 * Abstract class for encoding and decoding string type columns.
 *
 * @author Brian S O'Neill
 */
abstract class StringColumnCodec extends ColumnCodec {
    /**
     * @param info non-null
     * @param mm is null for stateless instance
     */
    StringColumnCodec(ColumnInfo info, MethodMaker mm) {
        super(info, mm);
    }

    @Override
    int minSize() {
        return 0;
    }

    /**
     * Defines a byte[] arg field set to null or a UTF-8 encoded string, and also defines a
     * String field with the original argument.
     *
     * @param argVar expected to be a String at runtime
     */
    @Override
    void filterPrepare(int op, Variable argVar, int argNum) {
        argVar = argVar.cast(String.class);

        defineArgField(String.class, argFieldName(argNum, "str")).set(argVar);

        Field argField = defineArgField(byte[].class, argFieldName(argNum));
        Label cont = mMaker.label();
        argVar.ifEq(null, cont);
        argField.set(argVar.invoke("getBytes", mMaker.var(StandardCharsets.class).field("UTF_8")));
        cont.here();
    }

    @Override
    Object filterDecode(ColumnInfo dstInfo, Variable srcVar, Variable offsetVar, Variable endVar,
                        int op)
    {
        if (dstInfo.plainTypeCode() != mInfo.plainTypeCode()) {
            // FIXME: Need to convert to String and compare that.
            throw null;
        }

        Variable lengthVar = mMaker.var(int.class);
        Variable isNullVar = mInfo.isNullable() ? mMaker.var(boolean.class) : null;

        decodeHeader(srcVar, offsetVar, endVar, lengthVar, isNullVar);

        Variable dataOffsetVar = offsetVar.get(); // need a stable copy

        return new Variable[] {dataOffsetVar, lengthVar, isNullVar};
    }

    /**
     * @param decoded the string end offset, unless a String compare should be performed
     */
    @Override
    void filterCompare(ColumnInfo dstInfo, Variable srcVar, Variable offsetVar, Variable endVar,
                       int op, Object decoded, Variable argObjVar, int argNum,
                       Label pass, Label fail)
    {
        if (dstInfo.plainTypeCode() != mInfo.plainTypeCode()) {
            // FIXME: Compare to String.
            throw null;
        }

        var decodedVars = (Variable[]) decoded;
        Variable dataOffsetVar = decodedVars[0];
        Variable lengthVar = decodedVars[1];
        Variable isNullVar = decodedVars[2];

        var argVar = argObjVar.field(argFieldName(argNum)).get();

        Label notNull = mMaker.label();
        argVar.ifNe(null, notNull);
        // Argument is null...
        if (isNullVar != null) {
            isNullVar.ifTrue(CompareUtils.selectNullColumnToNullArg(op, pass, fail));
        }
        mMaker.goto_(CompareUtils.selectColumnToNullArg(op, pass, fail));

        // Argument is isn't null...
        notNull.here();
        if (isNullVar != null) {
            isNullVar.ifTrue(CompareUtils.selectNullColumnToArg(op, pass, fail));
        }
        CompareUtils.compareArrays(mMaker,
                                   srcVar, dataOffsetVar, dataOffsetVar.add(lengthVar),
                                   argVar, 0, argVar.alength(),
                                   op, pass, fail);
    }

    /**
     * Decode the string header and advance the offset to the start of the string data.
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
}
