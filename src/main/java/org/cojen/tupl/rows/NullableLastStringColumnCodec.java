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

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

/**
 * Encoding suitable for nullable key or value columns which are the last in the set. No length
 * prefix is encoded.
 *
 * @see #encodeNullHeader
 * @see RowUtils#encodeStringUTF
 * @author Brian S O'Neill
 */
class NullableLastStringColumnCodec extends NonNullLastStringColumnCodec {
    /**
     * @param info non-null
     * @param mm is null for stateless instance
     */
    NullableLastStringColumnCodec(ColumnInfo info, MethodMaker mm) {
        super(info, mm);
    }

    @Override
    ColumnCodec bind(MethodMaker mm) {
        return new NullableLastStringColumnCodec(mInfo, mm);
    }

    @Override
    int minSize() {
        // Header byte.
        return 1;
    }

    @Override
    Variable encodeSize(Variable srcVar, Variable totalVar) {
        if (totalVar == null) {
            totalVar = mMaker.var(int.class).set(0);
        }
        var rowUtils = mMaker.var(RowUtils.class);
        Label isNull = mMaker.label();
        srcVar.ifEq(null, isNull);
        totalVar.inc(rowUtils.invoke("lengthStringUTF", srcVar));
        isNull.here();
        return totalVar;
    }

    @Override
    Variable encode(Variable srcVar, Variable dstVar, Variable offsetVar, int fixedOffset) {
        Label end = mMaker.label();
        offsetVar = encodeNullHeader(end, srcVar, dstVar, offsetVar, fixedOffset);
        offsetVar = super.encode(srcVar, dstVar, offsetVar, fixedOffset);
        end.here();
        return offsetVar;
    }

    @Override
    Variable decode(Variable dstVar, Variable srcVar, Variable offsetVar, int fixedOffset,
                    Variable endVar)
    {
        Label end = mMaker.label();
        offsetVar = decodeNullHeader(end, dstVar, srcVar, offsetVar, fixedOffset);
        offsetVar = super.decode(dstVar, srcVar, offsetVar, fixedOffset, endVar);
        end.here();
        return offsetVar;
    }

    @Override
    Variable decodeSkip(Variable srcVar, Variable offsetVar, int fixedOffset, Variable endVar) {
        Label end = mMaker.label();
        offsetVar = decodeNullHeader(end, null, srcVar, offsetVar, fixedOffset);
        offsetVar = super.decodeSkip(srcVar, offsetVar, fixedOffset, endVar);
        end.here();
        return offsetVar;
    }
}
