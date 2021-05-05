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
    ColumnCodec bind(MethodMaker mm) {
        return new NonNullBigIntegerColumnCodec(mInfo, mm);
    }

    @Override
    boolean isLast() {
        return false;
    }

    @Override
    void encodePrepare() {
        mBytesVar = mMaker.var(byte[].class);
        mLengthVar = mMaker.var(int.class);
    }

    @Override
    void encodeSkip() {
        mBytesVar.set(null);
        mLengthVar.set(0);
    }

    /**
     * @param srcVar BigInteger type
     */
    @Override
    Variable encodeSize(Variable srcVar, Variable totalVar) {
        mBytesVar.set(srcVar.invoke("toByteArray"));
        mLengthVar.set(mBytesVar.alength());

        // Add the prefix length.
        var rowUtils = mMaker.var(RowUtils.class);
        totalVar = accum(totalVar, rowUtils.invoke("lengthPrefixPF", mLengthVar));

        // Add the array length.
        return accum(totalVar, mLengthVar);
    }

    /**
     * @param srcVar BigInteger type
     * @param dstVar byte[] type
     */
    @Override
    Variable encode(Variable srcVar, Variable dstVar, Variable offsetVar, int fixedOffset) {
        var rowUtils = mMaker.var(RowUtils.class);

        if (offsetVar == null) {
            offsetVar = rowUtils.invoke("encodePrefixPF", dstVar, fixedOffset, mLengthVar);
        } else {
            offsetVar.set(rowUtils.invoke("encodePrefixPF", dstVar, offsetVar, mLengthVar));
        }

        finishEncode(dstVar, offsetVar, mLengthVar);

        return offsetVar;
    }

    /**
     * @param dstVar BigInteger type
     * @param srcVar byte[] type
     */
    @Override
    Variable decode(Variable dstVar, Variable srcVar, Variable offsetVar, int fixedOffset,
                    Variable endVar)
    {
        var rowUtils = mMaker.var(RowUtils.class);

        Variable lengthVar;
        if (offsetVar == null) {
            lengthVar = rowUtils.invoke("decodePrefixPF", srcVar, fixedOffset);
            offsetVar = rowUtils.invoke("lengthPrefixPF", lengthVar);
            if (fixedOffset != 0) {
                offsetVar.inc(fixedOffset);
            }
        } else {
            lengthVar = rowUtils.invoke("decodePrefixPF", srcVar, offsetVar);
            offsetVar.inc(rowUtils.invoke("lengthPrefixPF", lengthVar));
        }

        finishDecode(dstVar, srcVar, offsetVar, lengthVar);

        return offsetVar;
    }

    /**
     * @param srcVar byte[] type
     */
    @Override
    Variable decodeSkip(Variable srcVar, Variable offsetVar, int fixedOffset, Variable endVar) {
        var rowUtils = mMaker.var(RowUtils.class);
        Variable lengthVar;
        if (offsetVar == null) {
            offsetVar = rowUtils.invoke("skipBytesPF", srcVar, fixedOffset);
        } else {
            offsetVar.set(rowUtils.invoke("skipBytesPF", srcVar, offsetVar));
        }
        return offsetVar;
    }

    /**
     * @param dstVar byte[] type
     * @param offsetVar never null
     */
    protected void finishEncode(Variable dstVar, Variable offsetVar, Variable lengthVar) {
        mMaker.var(System.class).invoke("arraycopy", mBytesVar, 0, dstVar, offsetVar, lengthVar);
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
        dstVar.set(mMaker.new_(dstVar, srcVar, offsetVar, lengthVar));
        offsetVar.inc(lengthVar);
    }
}
