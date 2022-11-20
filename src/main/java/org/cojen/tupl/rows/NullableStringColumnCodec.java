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
 * Encoding suitable for nullable value columns.
 *
 * @see RowUtils#encodePrefixPF
 * @see RowUtils#lengthStringUTF
 * @see RowUtils#encodeStringUTF
 * @author Brian S O'Neill
 */
final class NullableStringColumnCodec extends NonNullStringColumnCodec {
    /**
     * @param info non-null
     * @param mm is null for stateless instance
     */
    NullableStringColumnCodec(ColumnInfo info, MethodMaker mm) {
        super(info, mm);
    }

    @Override
    ColumnCodec bind(MethodMaker mm) {
        return new NullableStringColumnCodec(mInfo, mm);
    }

    @Override
    int codecFlags() {
        return 0;
    }

    @Override
    Variable encodeSize(Variable srcVar, Variable totalVar) {
        // The length prefix encodes the byte length with one added. This allows zero to be
        // used to indicate null. As a result, this limits the maximum byte length to be one
        // less is possible, and encoding is corrupt. In practice this doesn't cause any
        // persistent issues because the final row is encoded in a byte array anyhow. It will
        // fail to be encoded as the offset wraps around negative.

        var rowUtils = mMaker.var(RowUtils.class);

        Variable strLengthVar = mMaker.var(int.class);
        Label notNull = mMaker.label();
        srcVar.ifNe(null, notNull);
        strLengthVar.set(0);
        mLengthVar.set(0); // zero means null
        Label cont = mMaker.label().goto_();
        notNull.here();
        strLengthVar.set(rowUtils.invoke("lengthStringUTF", srcVar));
        mLengthVar.set(strLengthVar.add(1)); // add one for non-null string
        cont.here();

        // Add the prefix length.
        totalVar = accum(totalVar, rowUtils.invoke("lengthPrefixPF", mLengthVar));

        // Add the string length.
        return accum(totalVar, strLengthVar);
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
    protected void finishEncode(Variable srcVar, Variable rowUtils,
                                Variable dstVar, Variable offsetVar)
    {
        Label isNull = mMaker.label();
        srcVar.ifEq(null, isNull);
        super.finishEncode(srcVar, rowUtils, dstVar, offsetVar);
        isNull.here();
    }

    @Override
    protected void finishDecode(Variable dstVar, Variable rowUtils,
                                Variable srcVar, Variable offsetVar, Variable lengthVar)
    {
        // Actual length is encoded plus one, and zero means null.
        Label notNull = mMaker.label();
        lengthVar.ifNe(0, notNull);
        dstVar.set(null);
        Label cont = mMaker.label().goto_();
        notNull.here();
        super.finishDecode(dstVar, rowUtils, srcVar, offsetVar, lengthVar.sub(1));
        cont.here();
    }
}
