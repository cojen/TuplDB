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
 * Encoding suitable for non-last key columns which supports nulls.
 *
 * @author Brian S O'Neill
 */
class KeyBigDecimalColumnCodec extends ColumnCodec {
    protected Variable mBytesVar;

    KeyBigDecimalColumnCodec(ColumnInfo info, MethodMaker mm) {
        super(info, mm);
    }

    @Override
    ColumnCodec bind(MethodMaker mm) {
        return new KeyBigDecimalColumnCodec(mInfo, mm);
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
        String methodName = "encodeBigDecimalKey";
        if (mInfo.isDescending()) {
            methodName += "Desc";
        }
        mBytesVar.set(mMaker.var(BigDecimalUtils.class).invoke(methodName, srcVar));
        return accum(totalVar, mBytesVar.alength());
    }

    @Override
    Variable encode(Variable srcVar, Variable dstVar, Variable offsetVar, int fixedOffset) {
        var system = mMaker.var(System.class);
        var lengthVar = mBytesVar.alength();
        if (offsetVar == null) {
            system.invoke("arraycopy", mBytesVar, 0, dstVar, fixedOffset, lengthVar);
            lengthVar.inc(fixedOffset);
            offsetVar = lengthVar;
        } else {
            system.invoke("arraycopy", mBytesVar, 0, dstVar, offsetVar, lengthVar);
            offsetVar.inc(lengthVar);
        }
        return offsetVar;
    }

    @Override
    Variable decode(Variable dstVar, Variable srcVar, Variable offsetVar, int fixedOffset,
                    Variable endVar)
    {
        String methodName = "decodeBigDecimalKey";
        if (mInfo.isDescending()) {
            methodName += "Desc";
        }

        Variable bdRefVar = dstVar == null ? null : mMaker.new_(BigDecimal[].class, 1);

        var rowUtils = mMaker.var(BigDecimalUtils.class);

        if (offsetVar == null) {
            offsetVar = rowUtils.invoke(methodName, srcVar, fixedOffset, bdRefVar);
        } else {
            offsetVar.set(rowUtils.invoke(methodName, srcVar, offsetVar, bdRefVar));
        }

        if (dstVar != null) {
            dstVar.set(bdRefVar.aget(0));
        }

        return offsetVar;
    }

    @Override
    Variable decodeSkip(Variable srcVar, Variable offsetVar, int fixedOffset, Variable endVar) {
        return decode(null, srcVar, offsetVar, fixedOffset, endVar);
    }

    @Override
    Object filterArgPrepare(Variable argVar, int argNum, int op) {
        // FIXME
        throw null;
    }
}
