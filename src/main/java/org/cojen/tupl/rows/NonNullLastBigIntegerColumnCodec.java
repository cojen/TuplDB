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
    int codecFlags() {
        return F_LAST;
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
    void encode(Variable srcVar, Variable dstVar, Variable offsetVar) {
        var lengthVar = mBytesVar.alength();
        mMaker.var(System.class).invoke("arraycopy", mBytesVar, 0, dstVar, offsetVar, lengthVar);
        offsetVar.inc(lengthVar);
    }

    /**
     * @param dstVar BigInteger type
     * @param srcVar byte[] type
     */
    @Override
    void decode(Variable dstVar, Variable srcVar, Variable offsetVar, Variable endVar) {
        var alengthVar = endVar == null ? srcVar.alength() : endVar;
        var lengthVar = alengthVar.sub(offsetVar);
        var valueVar = mMaker.new_(dstVar, srcVar, offsetVar, lengthVar);
        dstVar.set(valueVar);
        offsetVar.set(alengthVar);
    }

    /**
     * @param srcVar byte[] type
     */
    @Override
    void decodeSkip(Variable srcVar, Variable offsetVar, Variable endVar) {
        offsetVar.set(endVar == null ? srcVar.alength() : endVar);
    }

    @Override
    protected void decodeHeader(Variable srcVar, Variable offsetVar, Variable endVar,
                                Variable lengthVar, Variable isNullVar)
    {
        var alengthVar = endVar == null ? srcVar.alength() : endVar;
        lengthVar.set(alengthVar.sub(offsetVar));
    }
}
