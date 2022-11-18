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
    int codecFlags() {
        return F_LAST;
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
    void encode(Variable srcVar, Variable dstVar, Variable offsetVar) {
        var rowUtils = mMaker.var(RowUtils.class);
        offsetVar.set(rowUtils.invoke("encodeStringUTF", dstVar, offsetVar, srcVar));
    }

    @Override
    void decode(Variable dstVar, Variable srcVar, Variable offsetVar, Variable endVar) {
        var rowUtils = mMaker.var(RowUtils.class);
        var alengthVar = endVar == null ? srcVar.alength() : endVar;
        var lengthVar = alengthVar.sub(offsetVar);
        var valueVar = rowUtils.invoke("decodeStringUTF", srcVar, offsetVar, lengthVar);
        offsetVar.set(alengthVar);
        dstVar.set(valueVar);
    }

    @Override
    void decodeSkip(Variable srcVar, Variable offsetVar, Variable endVar) {
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
