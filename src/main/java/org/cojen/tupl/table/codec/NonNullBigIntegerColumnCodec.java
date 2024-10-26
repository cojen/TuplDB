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

import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.RowUtils;

/**
 * Encoding suitable for non-null value columns.
 *
 * @author Brian S O'Neill
 */
class NonNullBigIntegerColumnCodec extends BigIntegerColumnCodec {
    protected Variable mBytesVar;
    protected Variable mLengthVar;

    /**
     * @param info non-null
     * @param mm is null for stateless instance
     */
    NonNullBigIntegerColumnCodec(ColumnInfo info, MethodMaker mm) {
        super(info, mm);
    }

    @Override
    public ColumnCodec bind(MethodMaker mm) {
        return new NonNullBigIntegerColumnCodec(info, mm);
    }

    @Override
    public int codecFlags() {
        return 0;
    }

    @Override
    public boolean encodePrepare() {
        mBytesVar = maker.var(byte[].class);
        mLengthVar = maker.var(int.class);
        return true;
    }

    @Override
    public void encodeTransfer(ColumnCodec codec, Function<Variable, Variable> transfer) {
        var dst = (NonNullBigIntegerColumnCodec) codec;
        dst.mBytesVar = transfer.apply(mBytesVar);
        dst.mLengthVar = transfer.apply(mLengthVar);
    }

    @Override
    public void encodeSkip() {
        mBytesVar.set(null);
        mLengthVar.set(0);
    }

    /**
     * @param srcVar BigInteger type
     */
    @Override
    public Variable encodeSize(Variable srcVar, Variable totalVar) {
        mBytesVar.set(srcVar.invoke("toByteArray"));
        mLengthVar.set(mBytesVar.alength());

        // Add the prefix length.
        var rowUtils = maker.var(RowUtils.class);
        totalVar = accum(totalVar, rowUtils.invoke("lengthPrefixPF", mLengthVar));

        // Add the array length.
        return accum(totalVar, mLengthVar);
    }

    /**
     * @param srcVar BigInteger type
     * @param dstVar byte[] type
     */
    @Override
    public void encode(Variable srcVar, Variable dstVar, Variable offsetVar) {
        var rowUtils = maker.var(RowUtils.class);
        offsetVar.set(rowUtils.invoke("encodePrefixPF", dstVar, offsetVar, mLengthVar));
        finishEncode(dstVar, offsetVar, mLengthVar);
    }

    /**
     * @param dstVar BigInteger type
     * @param srcVar byte[] type
     */
    @Override
    public void decode(Variable dstVar, Variable srcVar, Variable offsetVar, Variable endVar) {
        var rowUtils = maker.var(RowUtils.class);
        var lengthVar = rowUtils.invoke("decodePrefixPF", srcVar, offsetVar);
        offsetVar.inc(rowUtils.invoke("lengthPrefixPF", lengthVar));
        finishDecode(dstVar, srcVar, offsetVar, lengthVar);
    }

    /**
     * @param srcVar byte[] type
     */
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
     * @param dstVar byte[] type
     * @param offsetVar never null
     */
    protected void finishEncode(Variable dstVar, Variable offsetVar, Variable lengthVar) {
        maker.var(System.class).invoke("arraycopy", mBytesVar, 0, dstVar, offsetVar, lengthVar);
        offsetVar.inc(lengthVar);
    }

    /**
     * @param dstVar BigInteger type
     * @param srcVar byte[] type
     * @param offsetVar never null
     */
    protected void finishDecode(Variable dstVar,
                                Variable srcVar, Variable offsetVar, Variable lengthVar)
    {
        dstVar.set(maker.new_(dstVar, srcVar, offsetVar, lengthVar));
        offsetVar.inc(lengthVar);
    }
}
