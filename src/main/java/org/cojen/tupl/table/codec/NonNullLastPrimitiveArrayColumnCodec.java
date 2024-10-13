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

package org.cojen.tupl.table.codec;

import java.util.function.Function;

import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.table.ColumnInfo;

/**
 * Encoding suitable for non-null key or value columns which are the last in the set. No
 * length prefix is encoded.
 *
 * @author Brian S O'Neill
 */
class NonNullLastPrimitiveArrayColumnCodec extends PrimitiveArrayColumnCodec {
    /**
     * @param info non-null
     * @param mm is null for stateless instance
     */
    NonNullLastPrimitiveArrayColumnCodec(ColumnInfo info, MethodMaker mm, int flags) {
        super(info, mm, flags);
        if (info.isDescending()) {
            throw new AssertionError();
        }
    }

    @Override
    public ColumnCodec bind(MethodMaker mm) {
        return new NonNullLastPrimitiveArrayColumnCodec(info, mm, mFlags);
    }

    @Override
    public boolean encodePrepare() {
        return false;
    }

    @Override
    public void encodeTransfer(ColumnCodec codec, Function<Variable, Variable> transfer) {
    }

    @Override
    public void encodeSkip() {
    }

    @Override
    public Variable encodeSize(Variable srcVar, Variable totalVar) {
        return accum(totalVar, byteArrayLength(srcVar));
    }

    @Override
    public void encode(Variable srcVar, Variable dstVar, Variable offsetVar) {
        encodeByteArray(srcVar, dstVar, offsetVar);
        offsetVar.inc(byteArrayLength(srcVar));
    }

    @Override
    public void decode(Variable dstVar, Variable srcVar, Variable offsetVar, Variable endVar) {
        var alengthVar = endVar == null ? srcVar.alength() : endVar;
        decodeByteArray(dstVar, srcVar, offsetVar, alengthVar.sub(offsetVar));
        offsetVar.set(alengthVar);
    }

    @Override
    public void decodeSkip(Variable srcVar, Variable offsetVar, Variable endVar) {
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
