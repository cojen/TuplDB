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
import org.cojen.tupl.table.RowUtils;

/**
 * Encoding suitable for nullable value columns.
 *
 * @author Brian S O'Neill
 */
final class NullableBigIntegerColumnCodec extends NonNullBigIntegerColumnCodec {
    /**
     * @param info non-null
     * @param mm is null for stateless instance
     */
    NullableBigIntegerColumnCodec(ColumnInfo info, MethodMaker mm) {
        super(info, mm);
    }

    @Override
    public ColumnCodec bind(MethodMaker mm) {
        return new NullableBigIntegerColumnCodec(info, mm);
    }

    @Override
    public int codecFlags() {
        return 0;
    }

    @Override
    public Variable encodeSize(Variable srcVar, Variable totalVar) {
        // See notes in NullableStringColumnCodec regarding length prefix encoding.

        Variable arrayLengthVar = maker.var(int.class);
        Label notNull = maker.label();
        srcVar.ifNe(null, notNull);
        mBytesVar.set(null);
        arrayLengthVar.set(0);
        mLengthVar.set(0); // zero means null
        Label cont = maker.label();
        maker.goto_(cont);
        notNull.here();
        mBytesVar.set(srcVar.invoke("toByteArray"));
        arrayLengthVar.set(mBytesVar.alength());
        mLengthVar.set(arrayLengthVar.add(1)); // add one for non-null array
        cont.here();

        // Add the prefix length.
        var rowUtils = maker.var(RowUtils.class);
        totalVar = accum(totalVar, rowUtils.invoke("lengthPrefixPF", mLengthVar));

        // Add the array length.
        return accum(totalVar, arrayLengthVar);
    }

    @Override
    public void decodeSkip(Variable srcVar, Variable offsetVar, Variable endVar) {
        offsetVar.set(maker.var(RowUtils.class).invoke("skipNullableBytesPF", srcVar, offsetVar));
    }

    @Override
    protected void decodeHeader(Variable srcVar, Variable offsetVar, Variable endVar,
                                Variable lengthVar, Variable isNullVar)
    {
        super.decodeHeader(srcVar, offsetVar, endVar, lengthVar, null);
        decodeNullableLength(lengthVar, isNullVar);
    }

    @Override
    protected void finishEncode(Variable dstVar, Variable offsetVar, Variable lengthVar) {
        Label isNull = maker.label();
        mBytesVar.ifEq(null, isNull);
        super.finishEncode(dstVar, offsetVar, mBytesVar.alength());
        isNull.here();
    }

    @Override
    protected void finishDecode(Variable dstVar,
                                Variable srcVar, Variable offsetVar, Variable lengthVar)
    {
        // Actual length is encoded plus one, and zero means null.
        Label notNull = maker.label();
        lengthVar.ifNe(0, notNull);
        dstVar.set(null);
        Label cont = maker.label();
        maker.goto_(cont);
        notNull.here();
        super.finishDecode(dstVar, srcVar, offsetVar, lengthVar.sub(1));
        cont.here();
    }
}
