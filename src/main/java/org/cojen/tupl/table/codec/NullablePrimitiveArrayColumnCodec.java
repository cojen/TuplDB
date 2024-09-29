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

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.RowUtils;

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
    public ColumnCodec bind(MethodMaker mm) {
        return new NullablePrimitiveArrayColumnCodec(info, mm);
    }

    @Override
    public boolean encodePrepare() {
        super.encodePrepare();
        mBytesLengthVar = maker.var(int.class);
        return true;
    }

    @Override
    public void encodeTransfer(ColumnCodec codec, Function<Variable, Variable> transfer) {
        super.encodeTransfer(codec, transfer);
        var dst = (NullablePrimitiveArrayColumnCodec) codec;
        dst.mBytesLengthVar = transfer.apply(mBytesLengthVar);
    }

    @Override
    public void encodeSkip() {
        super.encodeSkip();
        mBytesLengthVar.set(0);
    }

    @Override
    public Variable encodeSize(Variable srcVar, Variable totalVar) {
        // The length prefix encodes the byte length with one added. This allows zero to be
        // used to indicate null. See comments in NullableStringColumnCodec.encodeSize.

        Label notNull = maker.label();
        srcVar.ifNe(null, notNull);
        mBytesLengthVar.set(0);
        mLengthVar.set(0); // zero means null
        Label cont = maker.label().goto_();
        notNull.here();
        mBytesLengthVar.set(byteArrayLength(srcVar));
        mLengthVar.set(mBytesLengthVar.add(1)); // add one for non-null array
        cont.here();

        // Add the prefix length.
        var rowUtils = maker.var(RowUtils.class);
        totalVar = accum(totalVar, rowUtils.invoke("lengthPrefixPF", mLengthVar));

        // Add the byte array length.
        return accum(totalVar, mBytesLengthVar);
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
    protected void finishEncode(Variable srcVar, Variable dstVar, Variable offsetVar) {
        Label isNull = maker.label();
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
        Label notNull = maker.label();
        lengthVar.ifNe(0, notNull);
        dstVar.set(null);
        Label cont = maker.label().goto_();
        notNull.here();
        super.finishDecode(dstVar, srcVar, offsetVar, lengthVar.sub(1));
        cont.here();
    }
}
