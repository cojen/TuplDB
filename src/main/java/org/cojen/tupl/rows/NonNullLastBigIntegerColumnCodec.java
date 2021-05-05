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
 * Encoding suitable for non-null key or value columns which are the last in the set. No
 * length prefix is encoded.
 *
 * @author Brian S O'Neill
 */
class NonNullLastBigIntegerColumnCodec extends BigIntegerColumnCodec {
    protected Variable mBytesVar;

    /**
     * @param info non-null
     * @param mm is null for stateless instance
     */
    NonNullLastBigIntegerColumnCodec(ColumnInfo info, MethodMaker mm) {
        super(info, mm);
    }

    @Override
    ColumnCodec bind(MethodMaker mm) {
        return new NonNullLastBigIntegerColumnCodec(mInfo, mm);
    }

    @Override
    boolean isLast() {
        return true;
    }

    @Override
    void encodePrepare() {
        mBytesVar = mMaker.var(byte[].class);
    }

    @Override
    void encodeSkip() {
        mBytesVar.set(null);
    }

    /**
     * @param srcVar BigInteger type
     */
    @Override
    Variable encodeSize(Variable srcVar, Variable totalVar) {
        mBytesVar.set(srcVar.invoke("toByteArray"));
        return accum(totalVar, mBytesVar.alength());
    }

    /**
     * @param srcVar BigInteger type
     * @param dstVar byte[] type
     */
    @Override
    Variable encode(Variable srcVar, Variable dstVar, Variable offsetVar, int fixedOffset) {
        var rowUtils = mMaker.var(RowUtils.class);

        // Note: Updating the offset var isn't really necessary for the last column, but
        // it's consistent, and the compiler almost certainly optimizes dead stores.

        var system = mMaker.var(System.class);

        if (offsetVar == null) {
            offsetVar = mBytesVar.alength();
            system.invoke("arraycopy", mBytesVar, 0, dstVar, fixedOffset, offsetVar);
            if (fixedOffset != 0) {
                offsetVar.inc(fixedOffset);
            }
        } else {
            var lengthVar = mBytesVar.alength();
            system.invoke("arraycopy", mBytesVar, 0, dstVar, offsetVar, lengthVar);
            offsetVar.inc(lengthVar);
        }

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
        var alengthVar = endVar == null ? srcVar.alength() : endVar;

        Variable valueVar;
        if (offsetVar == null) {
            var lengthVar = alengthVar;
            if (fixedOffset != 0) {
                lengthVar = lengthVar.sub(fixedOffset);
            }
            valueVar = mMaker.new_(dstVar, srcVar, fixedOffset, lengthVar);
        } else {
            var lengthVar = alengthVar.sub(offsetVar);
            valueVar = mMaker.new_(dstVar, srcVar, offsetVar, lengthVar);
        }

        dstVar.set(valueVar);

        return alengthVar;
    }

    /**
     * @param srcVar byte[] type
     */
    @Override
    Variable decodeSkip(Variable srcVar, Variable offsetVar, int fixedOffset, Variable endVar) {
        endVar = endVar == null ? srcVar.alength() : endVar;
        if (offsetVar == null) {
            offsetVar = endVar;
        } else {
            offsetVar.set(endVar);
        }
        return offsetVar;
    }
}
