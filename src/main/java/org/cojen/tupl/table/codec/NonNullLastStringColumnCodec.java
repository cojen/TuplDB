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

import java.util.function.Function;

import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.RowUtils;

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
    public ColumnCodec bind(MethodMaker mm) {
        return new NonNullLastStringColumnCodec(info, mm);
    }

    @Override
    public int codecFlags() {
        return F_LAST;
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
        var rowUtils = maker.var(RowUtils.class);
        return accum(totalVar, rowUtils.invoke("lengthStringUTF", srcVar));
    }

    @Override
    public void encode(Variable srcVar, Variable dstVar, Variable offsetVar) {
        var rowUtils = maker.var(RowUtils.class);
        offsetVar.set(rowUtils.invoke("encodeStringUTF", dstVar, offsetVar, srcVar));
    }

    @Override
    public void decode(Variable dstVar, Variable srcVar, Variable offsetVar, Variable endVar) {
        var rowUtils = maker.var(RowUtils.class);
        var alengthVar = endVar == null ? srcVar.alength() : endVar;
        var lengthVar = alengthVar.sub(offsetVar);
        var valueVar = rowUtils.invoke("decodeStringUTF", srcVar, offsetVar, lengthVar);
        offsetVar.set(alengthVar);
        dstVar.set(valueVar);
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
