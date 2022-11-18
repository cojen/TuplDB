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

import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class NonNullPrimitiveArrayColumnCodec extends PrimitiveArrayColumnCodec {
    protected Variable mLengthVar;

    /**
     * @param info non-null
     * @param mm is null for stateless instance
     */
    NonNullPrimitiveArrayColumnCodec(ColumnInfo info, MethodMaker mm) {
        super(info, mm, false);
    }

    @Override
    ColumnCodec bind(MethodMaker mm) {
        return new NonNullPrimitiveArrayColumnCodec(mInfo, mm);
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
        mLengthVar.set(byteArrayLength(srcVar));

        // Add the prefix length.
        var rowUtils = mMaker.var(RowUtils.class);
        totalVar = accum(totalVar, rowUtils.invoke("lengthPrefixPF", mLengthVar));

        // Add the byte array length.
        return accum(totalVar, mLengthVar);
    }

    @Override
    void encode(Variable srcVar, Variable dstVar, Variable offsetVar) {
        var rowUtils = mMaker.var(RowUtils.class);
        offsetVar.set(rowUtils.invoke("encodePrefixPF", dstVar, offsetVar, mLengthVar));
        finishEncode(srcVar, dstVar, offsetVar);
    }

    /**
     * @param dstVar primitive array type
     */
    @Override
    void decode(Variable dstVar, Variable srcVar, Variable offsetVar, Variable endVar) {
        var rowUtils = mMaker.var(RowUtils.class);
        var lengthVar = rowUtils.invoke("decodePrefixPF", srcVar, offsetVar);
        offsetVar.inc(rowUtils.invoke("lengthPrefixPF", lengthVar));
        finishDecode(dstVar, srcVar, offsetVar, lengthVar);
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
    protected void finishEncode(Variable srcVar, Variable dstVar, Variable offsetVar) {
        encodeByteArray(srcVar, dstVar, offsetVar);
        offsetVar.inc(mLengthVar);
    }

    /**
     * @param dstVar primitive array type
     * @param offsetVar never null
     */
    protected void finishDecode(Variable dstVar,
                                Variable srcVar, Variable offsetVar, Variable lengthVar)
    {
        decodeByteArray(dstVar, srcVar, offsetVar, lengthVar);
        offsetVar.inc(lengthVar);
    }
}
