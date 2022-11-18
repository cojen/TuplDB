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
 * 
 *
 * @author Brian S O'Neill
 */
final class NullablePrimitiveArrayColumnCodec extends NonNullPrimitiveArrayColumnCodec {
    private Variable mBytesLengthVar;

    /**
     * @param info non-null
     * @param mm is null for stateless instance
     */
    NullablePrimitiveArrayColumnCodec(ColumnInfo info, MethodMaker mm) {
        super(info, mm);
    }

    @Override
    ColumnCodec bind(MethodMaker mm) {
        return new NullablePrimitiveArrayColumnCodec(mInfo, mm);
    }

    @Override
    int codecFlags() {
        return F_NULLS;
    }

    @Override
    void encodePrepare() {
        super.encodePrepare();
        mBytesLengthVar = mMaker.var(int.class);
    }

    @Override
    void encodeSkip() {
        super.encodeSkip();
        mBytesLengthVar.set(0);
    }

    @Override
    Variable encodeSize(Variable srcVar, Variable totalVar) {
        // The length prefix encodes the byte length with one added. This allows zero to be
        // used to indicate null. See comments in NullableStringColumnCodec.encodeSize.

        Label notNull = mMaker.label();
        srcVar.ifNe(null, notNull);
        mBytesLengthVar.set(0);
        mLengthVar.set(0); // zero means null
        Label cont = mMaker.label().goto_();
        notNull.here();
        mBytesLengthVar.set(byteArrayLength(srcVar));
        mLengthVar.set(mBytesLengthVar.add(1)); // add one for non-null array
        cont.here();

        // Add the prefix length.
        var rowUtils = mMaker.var(RowUtils.class);
        totalVar = accum(totalVar, rowUtils.invoke("lengthPrefixPF", mLengthVar));

        // Add the byte array length.
        return accum(totalVar, mBytesLengthVar);
    }

    @Override
    void decodeSkip(Variable srcVar, Variable offsetVar, Variable endVar) {
        offsetVar.set(mMaker.var(RowUtils.class).invoke("skipNullableBytesPF", srcVar, offsetVar));
    }

    @Override
    protected void decodeHeader(Variable srcVar, Variable offsetVar, Variable endVar,
                                Variable lengthVar, Variable isNullVar)
    {
        super.decodeHeader(srcVar, offsetVar, endVar, lengthVar, null);
        decodeNullableLength(lengthVar, isNullVar);
    }

    @Override
    protected void finishEncode(Variable srcVar, Variable dstVar, Variable offsetVar) {
        Label isNull = mMaker.label();
        srcVar.ifEq(null, isNull);
        encodeByteArray(srcVar, dstVar, offsetVar);
        offsetVar.inc(mBytesLengthVar);
        isNull.here();
    }

    @Override
    protected void finishDecode(Variable dstVar,
                                Variable srcVar, Variable offsetVar, Variable lengthVar)
    {
        // Actual length is encoded plus one, and zero means null.
        Label notNull = mMaker.label();
        lengthVar.ifNe(0, notNull);
        dstVar.set(null);
        Label cont = mMaker.label().goto_();
        notNull.here();
        super.finishDecode(dstVar, srcVar, offsetVar, lengthVar.sub(1));
        cont.here();
    }
}
