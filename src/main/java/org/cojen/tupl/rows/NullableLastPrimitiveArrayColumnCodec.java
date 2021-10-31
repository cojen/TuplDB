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

package org.cojen.tupl.rows;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

/**
 * Encoding suitable for nullable key or value columns which are the last in the set. No length
 * prefix is encoded.
 *
 * @author Brian S O'Neill
 */
final class NullableLastPrimitiveArrayColumnCodec extends NonNullLastPrimitiveArrayColumnCodec {
    /**
     * @param info non-null
     * @param mm is null for stateless instance
     */
    NullableLastPrimitiveArrayColumnCodec(ColumnInfo info, MethodMaker mm, boolean lex) {
        super(info, mm, lex);
    }

    @Override
    ColumnCodec bind(MethodMaker mm) {
        return new NullableLastPrimitiveArrayColumnCodec(mInfo, mm, mLex);
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
        Label isNull = mMaker.label();
        srcVar.ifEq(null, isNull);
        totalVar.inc(byteArrayLength(srcVar));
        isNull.here();
        return totalVar;
    }

    @Override
    void encode(Variable srcVar, Variable dstVar, Variable offsetVar) {
        Label end = mMaker.label();
        encodeNullHeader(end, srcVar, dstVar, offsetVar);
        super.encode(srcVar, dstVar, offsetVar);
        end.here();
    }

    @Override
    void decode(Variable dstVar, Variable srcVar, Variable offsetVar, Variable endVar) {
        Label end = mMaker.label();
        decodeNullHeader(end, dstVar, srcVar, offsetVar);
        super.decode(dstVar, srcVar, offsetVar, endVar);
        end.here();
    }

    @Override
    void decodeSkip(Variable srcVar, Variable offsetVar, Variable endVar) {
        Label end = mMaker.label();
        decodeNullHeader(end, null, srcVar, offsetVar);
        super.decodeSkip(srcVar, offsetVar, endVar);
        end.here();
    }

    @Override
    protected void decodeHeader(Variable srcVar, Variable offsetVar, Variable endVar,
                                Variable lengthVar, Variable isNullVar)
    {
        decodeNullHeader(null, isNullVar, srcVar, offsetVar);
        Label notNull = mMaker.label();
        isNullVar.ifFalse(notNull);
        lengthVar.set(0);
        Label cont = mMaker.label();
        mMaker.goto_(cont);
        notNull.here();
        super.decodeHeader(srcVar, offsetVar, endVar, lengthVar, null);
        cont.here();
    }
}