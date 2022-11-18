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

import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

/**
 * Encoding suitable for non-null value columns.
 * 
 * @see RowUtils#encodePrefixPF
 * @see RowUtils#lengthStringUTF
 * @see RowUtils#encodeStringUTF
 * @author Brian S O'Neill
 */
class NonNullStringColumnCodec extends StringColumnCodec {
    protected Variable mLengthVar;

    /**
     * @param info non-null
     * @param mm is null for stateless instance
     */
    NonNullStringColumnCodec(ColumnInfo info, MethodMaker mm) {
        super(info, mm);
    }

    @Override
    ColumnCodec bind(MethodMaker mm) {
        return new NonNullStringColumnCodec(mInfo, mm);
    }

    @Override
    int codecFlags() {
        return 0;
    }

    @Override
    void encodePrepare() {
        mLengthVar = mMaker.var(int.class);
    }

    @Override
    void encodeSkip() {
        mLengthVar.set(0);
    }

    @Override
    Variable encodeSize(Variable srcVar, Variable totalVar) {
        var rowUtils = mMaker.var(RowUtils.class);

        // Length prefix will be needed by the encode method.
        mLengthVar.set(rowUtils.invoke("lengthStringUTF", srcVar));

        // Add the prefix length.
        totalVar = accum(totalVar, rowUtils.invoke("lengthPrefixPF", mLengthVar));

        // Add the string length.
        return accum(totalVar, mLengthVar);
    }

    @Override
    void encode(Variable srcVar, Variable dstVar, Variable offsetVar) {
        var rowUtils = mMaker.var(RowUtils.class);
        offsetVar.set(rowUtils.invoke("encodePrefixPF", dstVar, offsetVar, mLengthVar));
        finishEncode(srcVar, rowUtils, dstVar, offsetVar);
    }

    /**
     * @param dstVar String type
     */
    @Override
    void decode(Variable dstVar, Variable srcVar, Variable offsetVar, Variable endVar) {
        var rowUtils = mMaker.var(RowUtils.class);
        var lengthVar = rowUtils.invoke("decodePrefixPF", srcVar, offsetVar);
        offsetVar.inc(rowUtils.invoke("lengthPrefixPF", lengthVar));
        finishDecode(dstVar, rowUtils, srcVar, offsetVar, lengthVar);
    }

    @Override
    void decodeSkip(Variable srcVar, Variable offsetVar, Variable endVar) {
        offsetVar.set(mMaker.var(RowUtils.class).invoke("skipBytesPF", srcVar, offsetVar));
    }

    @Override
    protected void decodeHeader(Variable srcVar, Variable offsetVar, Variable endVar,
                                Variable lengthVar, Variable isNullVar)
    {
        var rowUtils = mMaker.var(RowUtils.class);
        lengthVar.set(rowUtils.invoke("decodePrefixPF", srcVar, offsetVar));
        offsetVar.inc(rowUtils.invoke("lengthPrefixPF", lengthVar));
    }

    /**
     * @param offsetVar never null
     */
    protected void finishEncode(Variable srcVar, Variable rowUtils,
                                Variable dstVar, Variable offsetVar)
    {
        offsetVar.set(rowUtils.invoke("encodeStringUTF", dstVar, offsetVar, srcVar));
    }

    /**
     * @param dstVar String type
     * @param offsetVar never null
     */
    protected void finishDecode(Variable dstVar, Variable rowUtils,
                                Variable srcVar, Variable offsetVar, Variable lengthVar)
    {
        dstVar.set(rowUtils.invoke("decodeStringUTF", srcVar, offsetVar, lengthVar));
        offsetVar.inc(lengthVar);
    }
}
