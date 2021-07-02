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

import java.math.BigInteger;

import org.cojen.maker.Field;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

/**
 * Abstract class for encoding and decoding BigInteger type columns.
 *
 * @author Brian S O'Neill
 */
abstract class BigIntegerColumnCodec extends ColumnCodec {
    /**
     * @param info non-null
     * @param mm is null for stateless instance
     */
    BigIntegerColumnCodec(ColumnInfo info, MethodMaker mm) {
        super(info, mm);
    }

    @Override
    int minSize() {
        return 0;
    }

    /**
     * Defines a byte[] arg field set to null or an encoded BigInteger, and also defines a
     * BigInteger field with the original argument.
     */
    @Override
    Variable filterPrepare(boolean in, Variable argVar, int argNum) {
        argVar = super.filterPrepare(in, argVar, argNum);
        Variable bytesVar = filterPrepareBytes(in, argVar, argNum, true);
        defineArgField(bytesVar, argFieldName(argNum, "bytes")).set(bytesVar);
        return argVar;
    }

    @Override
    protected Variable filterEncodeBytes(Variable biVar) {
        return mMaker.var(RowUtils.class).invoke("encodeBigInteger", biVar);
    }

    @Override
    Object filterDecode(ColumnInfo dstInfo, Variable srcVar, Variable offsetVar, Variable endVar,
                        int op)
    {
        if (dstInfo.plainTypeCode() != mInfo.plainTypeCode()) {
            var decodedVar = mMaker.var(mInfo.type);
            decode(decodedVar, srcVar, offsetVar, endVar);
            var columnVar = mMaker.var(dstInfo.type);
            Converter.convertLossy(mMaker, mInfo, decodedVar, dstInfo, columnVar);
            return columnVar;
        }

        Variable lengthVar = mMaker.var(int.class);
        Variable isNullVar = mInfo.isNullable() ? mMaker.var(boolean.class) : null;

        decodeHeader(srcVar, offsetVar, endVar, lengthVar, isNullVar);

        Variable dataOffsetVar = offsetVar.get(); // need a stable copy
        offsetVar.inc(lengthVar);

        return new Variable[] {dataOffsetVar, lengthVar, isNullVar};
    }

    @Override
    void filterCompare(ColumnInfo dstInfo, Variable srcVar, Variable offsetVar, Variable endVar,
                       int op, Object decoded, Variable argObjVar, int argNum,
                       Label pass, Label fail)
    {
        if (dstInfo.plainTypeCode() != mInfo.plainTypeCode()) {
            var columnVar = (Variable) decoded;
            var argField = argObjVar.field(argFieldName(argNum));
            CompareUtils.compare(mMaker, dstInfo, columnVar, dstInfo, argField, op, pass, fail);
            return;
        }

        compareEncoded(dstInfo, srcVar, op, (Variable[]) decoded, argObjVar, argNum, pass, fail);
    }

    /**
     * Decode the BigInteger header and advance the offset to the start of the BigInteger data.
     *
     * @param srcVar source byte array
     * @param offsetVar int type; is incremented as a side-effect
     * @param endVar end offset, which when null implies the end of the array
     * @param lengthVar set to the decoded length; must be definitely assigned
     * @param isNullVar set to true/false if applicable; must be definitely assigned for
     * nullable BigIntegers
     */
    protected abstract void decodeHeader(Variable srcVar, Variable offsetVar, Variable endVar,
                                         Variable lengthVar, Variable isNullVar);
}
