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
 * @see RowUtils#encodeStringUTF
 * @author Brian S O'Neill
 */
class NonNullLastStringColumnCodec extends StringColumnCodec {
    /**
     * @param info non-null
     * @param mm is null for stateless instance
     */
    NonNullLastStringColumnCodec(ColumnInfo info, MethodMaker mm) {
        super(info, mm);
    }

    @Override
    ColumnCodec bind(MethodMaker mm) {
        return new NonNullLastStringColumnCodec(mInfo, mm);
    }

    @Override
    boolean isLast() {
        return true;
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
        return accum(totalVar, rowUtils.invoke("lengthStringUTF", srcVar));
    }

    @Override
    Variable encode(Variable srcVar, Variable dstVar, Variable offsetVar, int fixedOffset) {
        var rowUtils = mMaker.var(RowUtils.class);

        // Note: Updating the offset var isn't really necessary for the last column, but
        // it's consistent, and the compiler almost certainly optimizes dead stores.

        if (offsetVar == null) {
            offsetVar = rowUtils.invoke("encodeStringUTF", dstVar, fixedOffset, srcVar);
        } else {
            offsetVar.set(rowUtils.invoke("encodeStringUTF", dstVar, offsetVar, srcVar));
        }

        return offsetVar;
    }

    @Override
    Variable decode(Variable dstVar, Variable srcVar, Variable offsetVar, int fixedOffset,
                    Variable endVar)
    {
        var rowUtils = mMaker.var(RowUtils.class);
        var alengthVar = endVar == null ? srcVar.alength() : endVar;

        Variable valueVar;
        if (offsetVar == null) {
            var lengthVar = alengthVar;
            if (fixedOffset != 0) {
                lengthVar = lengthVar.sub(fixedOffset);
            }
            valueVar = rowUtils.invoke("decodeStringUTF", srcVar, fixedOffset, lengthVar);
        } else {
            var lengthVar = alengthVar.sub(offsetVar);
            valueVar = rowUtils.invoke("decodeStringUTF", srcVar, offsetVar, lengthVar);
        }

        dstVar.set(valueVar);

        return alengthVar;
    }

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
