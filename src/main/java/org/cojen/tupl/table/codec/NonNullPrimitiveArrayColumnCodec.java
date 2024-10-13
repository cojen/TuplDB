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

import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.RowUtils;

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
        super(info, mm, 0);
    }

    @Override
    public ColumnCodec bind(MethodMaker mm) {
        return new NonNullPrimitiveArrayColumnCodec(info, mm);
    }

    @Override
    public boolean encodePrepare() {
        mLengthVar = maker.var(int.class);
        return true;
    }

    @Override
    public void encodeTransfer(ColumnCodec codec, Function<Variable, Variable> transfer) {
        var dst = (NonNullPrimitiveArrayColumnCodec) codec;
        dst.mLengthVar = transfer.apply(mLengthVar);
    }

    @Override
    public void encodeSkip() {
        mLengthVar.set(0);
    }

    @Override
    public Variable encodeSize(Variable srcVar, Variable totalVar) {
        mLengthVar.set(byteArrayLength(srcVar));

        // Add the prefix length.
        var rowUtils = maker.var(RowUtils.class);
        totalVar = accum(totalVar, rowUtils.invoke("lengthPrefixPF", mLengthVar));

        // Add the byte array length.
        return accum(totalVar, mLengthVar);
    }

    @Override
    public void encode(Variable srcVar, Variable dstVar, Variable offsetVar) {
        var rowUtils = maker.var(RowUtils.class);
        offsetVar.set(rowUtils.invoke("encodePrefixPF", dstVar, offsetVar, mLengthVar));
        finishEncode(srcVar, dstVar, offsetVar);
    }

    /**
     * @param dstVar primitive array type
     */
    @Override
    public void decode(Variable dstVar, Variable srcVar, Variable offsetVar, Variable endVar) {
        var rowUtils = maker.var(RowUtils.class);
        var lengthVar = rowUtils.invoke("decodePrefixPF", srcVar, offsetVar);
        offsetVar.inc(rowUtils.invoke("lengthPrefixPF", lengthVar));
        finishDecode(dstVar, srcVar, offsetVar, lengthVar);
    }

    @Override
    public void decodeSkip(Variable srcVar, Variable offsetVar, Variable endVar) {
        offsetVar.set(maker.var(RowUtils.class).invoke("skipBytesPF", srcVar, offsetVar));
    }

    @Override
    protected void decodeHeader(Variable srcVar, Variable offsetVar, Variable endVar,
                                Variable lengthVar, Variable isNullVar)
    {
        var rowUtils = maker.var(RowUtils.class);
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
