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
 * @author Brian S O'Neill
 */
final class NullableLastBigIntegerColumnCodec extends NonNullLastBigIntegerColumnCodec {
    /**
     * @param info non-null
     * @param mm is null for stateless instance
     */
    NullableLastBigIntegerColumnCodec(ColumnInfo info, MethodMaker mm) {
        super(info, mm);
    }

    @Override
    ColumnCodec bind(MethodMaker mm) {
        return new NullableLastBigIntegerColumnCodec(mInfo, mm);
    }

    @Override
    int codecFlags() {
        return F_NULLS | F_LAST;
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
        Label notNull = mMaker.label();
        srcVar.ifNe(null, notNull);
        mBytesVar.set(null);
        Label cont = mMaker.label().goto_();
        notNull.here();
        mBytesVar.set(srcVar.invoke("toByteArray"));
        totalVar.inc(mBytesVar.alength());
        cont.here();
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
        Label cont = mMaker.label().goto_();
        notNull.here();
        super.decodeHeader(srcVar, offsetVar, endVar, lengthVar, null);
        cont.here();
    }
}
