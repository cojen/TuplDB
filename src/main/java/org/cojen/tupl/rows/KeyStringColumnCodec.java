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
 * Encoding suitable for non-last key columns which supports nulls.
 *
 * @see RowUtils#encodeStringKey
 * @author Brian S O'Neill
 */
class KeyStringColumnCodec extends StringColumnCodec {
    KeyStringColumnCodec(ColumnInfo info, MethodMaker mm) {
        super(info, mm);
    }

    @Override
    ColumnCodec bind(MethodMaker mm) {
        return new KeyStringColumnCodec(mInfo, mm);
    }

    @Override
    boolean isLast() {
        return false;
    }

    @Override
    void encodePrepare() {
    }

    @Override
    void encodeSkip() {
    }

    @Override
    Variable encodeSize(Variable srcVar, Variable totalVar) {
        var rowUtils = mMaker.var(RowUtils.class);
        return accum(totalVar, rowUtils.invoke("lengthStringKey", srcVar));
    }

    @Override
    Variable encode(Variable srcVar, Variable dstVar, Variable offsetVar, int fixedOffset) {
        String methodName = "encodeStringKey";
        if (mInfo.isDescending()) {
            methodName += "Desc";
        }

        var rowUtils = mMaker.var(RowUtils.class);

        if (offsetVar == null) {
            offsetVar = rowUtils.invoke(methodName, dstVar, fixedOffset, srcVar);
        } else {
            offsetVar.set(rowUtils.invoke(methodName, dstVar, offsetVar, srcVar));
        }

        return offsetVar;
    }

    @Override
    Variable decode(Variable dstVar, Variable srcVar, Variable offsetVar, int fixedOffset,
                    Variable endVar)
    {
        String methodName = "decodeStringKey";
        if (mInfo.isDescending()) {
            methodName += "Desc";
        }

        var rowUtils = mMaker.var(RowUtils.class);

        Variable valueVar;
        if (offsetVar == null) {
            var lengthVar = rowUtils.invoke("lengthStringKey", srcVar, fixedOffset);
            offsetVar = mMaker.var(int.class).set(lengthVar);
            if (fixedOffset != 0) {
                offsetVar.inc(fixedOffset);
            }
            valueVar = rowUtils.invoke(methodName, srcVar, fixedOffset, lengthVar);
        } else {
            var lengthVar = rowUtils.invoke("lengthStringKey", srcVar, offsetVar);
            valueVar = rowUtils.invoke(methodName, srcVar, offsetVar, lengthVar);
            offsetVar.inc(lengthVar);
        }

        dstVar.set(valueVar);

        return offsetVar;
    }

    @Override
    Variable decodeSkip(Variable srcVar, Variable offsetVar, int fixedOffset, Variable endVar) {
        var rowUtils = mMaker.var(RowUtils.class);

        if (offsetVar == null) {
            offsetVar = rowUtils.invoke("lengthStringKey", srcVar, fixedOffset);
            if (fixedOffset != 0) {
                offsetVar.inc(fixedOffset);
            }
        } else {
            offsetVar.inc(rowUtils.invoke("lengthStringKey", srcVar, offsetVar));
        }

        return offsetVar;
    }
}
