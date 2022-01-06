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

import java.math.BigDecimal;

import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

/**
 * Encoding suitable for lexicographically ordered columns which supports nulls.
 *
 * @author Brian S O'Neill
 */
final class LexBigDecimalColumnCodec extends ColumnCodec {
    private Variable mBytesVar;

    LexBigDecimalColumnCodec(ColumnInfo info, MethodMaker mm) {
        super(info, mm);
    }

    @Override
    ColumnCodec bind(MethodMaker mm) {
        return new LexBigDecimalColumnCodec(mInfo, mm);
    }

    @Override
    protected final boolean doEquals(Object obj) {
        return true;
    }

    @Override
    public final int doHashCode() {
        return 0;
    }

    @Override
    int minSize() {
        return 0;
    }

    @Override
    boolean isLast() {
        return false;
    }

    @Override
    void encodePrepare() {
        mBytesVar = mMaker.var(byte[].class);
    }

    @Override
    void encodeSkip() {
        mBytesVar.set(null);
    }

    @Override
    Variable encodeSize(Variable srcVar, Variable totalVar) {
        String methodName = "encodeBigDecimalLex";
        if (mInfo.isDescending()) {
            methodName += "Desc";
        }
        mBytesVar.set(mMaker.var(BigDecimalUtils.class).invoke(methodName, srcVar));
        return accum(totalVar, mBytesVar.alength());
    }

    @Override
    void encode(Variable srcVar, Variable dstVar, Variable offsetVar) {
        var lengthVar = mBytesVar.alength();
        mMaker.var(System.class).invoke("arraycopy", mBytesVar, 0, dstVar, offsetVar, lengthVar);
        offsetVar.inc(lengthVar);
    }

    @Override
    void decode(Variable dstVar, Variable srcVar, Variable offsetVar, Variable endVar) {
        String methodName = "decodeBigDecimalLex";
        if (mInfo.isDescending()) {
            methodName += "Desc";
        }

        Variable bdRefVar = dstVar == null ? null : mMaker.new_(BigDecimal[].class, 1);

        var rowUtils = mMaker.var(BigDecimalUtils.class);
        offsetVar.set(rowUtils.invoke(methodName, srcVar, offsetVar, bdRefVar));

        if (dstVar != null) {
            dstVar.set(bdRefVar.aget(0));
        }
    }

    @Override
    void decodeSkip(Variable srcVar, Variable offsetVar, Variable endVar) {
        decode(null, srcVar, offsetVar, endVar);
    }
}
