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

package org.cojen.tupl.table.codec;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.table.ColumnInfo;

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
    public ColumnCodec bind(MethodMaker mm) {
        return new NullableLastBigIntegerColumnCodec(info, mm);
    }

    @Override
    public int codecFlags() {
        return F_LAST;
    }

    @Override
    public int minSize() {
        // Header byte.
        return 1;
    }

    @Override
    public Variable encodeSize(Variable srcVar, Variable totalVar) {
        if (totalVar == null) {
            totalVar = maker.var(int.class).set(0);
        }
        Label notNull = maker.label();
        srcVar.ifNe(null, notNull);
        mBytesVar.set(null);
        Label cont = maker.label().goto_();
        notNull.here();
        mBytesVar.set(srcVar.invoke("toByteArray"));
        totalVar.inc(mBytesVar.alength());
        cont.here();
        return totalVar;
    }

    @Override
    public void encode(Variable srcVar, Variable dstVar, Variable offsetVar) {
        Label end = maker.label();
        encodeNullHeader(end, srcVar, dstVar, offsetVar);
        super.encode(srcVar, dstVar, offsetVar);
        end.here();
    }

    @Override
    public void decode(Variable dstVar, Variable srcVar, Variable offsetVar, Variable endVar) {
        Label end = maker.label();
        decodeNullHeader(end, dstVar, srcVar, offsetVar);
        super.decode(dstVar, srcVar, offsetVar, endVar);
        end.here();
    }

    @Override
    public void decodeSkip(Variable srcVar, Variable offsetVar, Variable endVar) {
        Label end = maker.label();
        decodeNullHeader(end, null, srcVar, offsetVar);
        super.decodeSkip(srcVar, offsetVar, endVar);
        end.here();
    }

    @Override
    protected void decodeHeader(Variable srcVar, Variable offsetVar, Variable endVar,
                                Variable lengthVar, Variable isNullVar)
    {
        decodeNullHeader(null, isNullVar, srcVar, offsetVar);
        Label notNull = maker.label();
        isNullVar.ifFalse(notNull);
        lengthVar.set(0);
        Label cont = maker.label().goto_();
        notNull.here();
        super.decodeHeader(srcVar, offsetVar, endVar, lengthVar, null);
        cont.here();
    }
}
