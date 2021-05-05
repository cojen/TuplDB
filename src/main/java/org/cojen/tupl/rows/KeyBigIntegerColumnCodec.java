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
 * Encoding suitable for non-last key columns which supports nulls.
 *
 * @see RowUtils#encodeBigIntegerKey
 * @author Brian S O'Neill
 */
class KeyBigIntegerColumnCodec extends BigIntegerColumnCodec {
    protected Variable mBytesVar;

    KeyBigIntegerColumnCodec(ColumnInfo info, MethodMaker mm) {
        super(info, mm);
    }

    @Override
    ColumnCodec bind(MethodMaker mm) {
        return new KeyBigIntegerColumnCodec(mInfo, mm);
    }

    @Override
    int minSize() {
        return 1;
    }

    @Override
    boolean isLast() {
        return false;
    }

    @Override
    void encodePrepare() {
        mBytesVar = mMaker.var(byte[].class);
    }

    @Override
    void encodeSkip() {
        mBytesVar.set(null);
    }

    @Override
    Variable encodeSize(Variable srcVar, Variable totalVar) {
        if (totalVar == null) {
            totalVar = mMaker.var(int.class).set(0);
        }
        Label notNull = mMaker.label();
        srcVar.ifNe(null, notNull);
        mBytesVar.set(null);
        Label cont = mMaker.label();
        mMaker.goto_(cont);
        notNull.here();
        mBytesVar.set(srcVar.invoke("toByteArray"));
        var lengthVar = mBytesVar.alength();
        totalVar.inc(lengthVar);
        lengthVar.ifLt(0x7f, cont);
        totalVar.inc(4);
        cont.here();
        return totalVar;
    }

    @Override
    Variable encode(Variable srcVar, Variable dstVar, Variable offsetVar, int fixedOffset) {
        String methodName = "encodeBigIntegerKey";
        if (mInfo.isDescending()) {
            methodName += "Desc";
        }

        var rowUtils = mMaker.var(RowUtils.class);

        if (offsetVar == null) {
            offsetVar = rowUtils.invoke(methodName, dstVar, fixedOffset, mBytesVar);
        } else {
            offsetVar.set(rowUtils.invoke(methodName, dstVar, offsetVar, mBytesVar));
        }

        return offsetVar;
    }

    @Override
    Variable decode(Variable dstVar, Variable srcVar, Variable offsetVar, int fixedOffset,
                    Variable endVar)
    {
        var rowUtils = mMaker.var(RowUtils.class);

        Variable resultVar;
        if (offsetVar == null) {
            resultVar = rowUtils.invoke("decodeBigIntegerKeyHeader", srcVar, fixedOffset);
            offsetVar = mMaker.var(int.class).set(resultVar.cast(int.class));
            if (fixedOffset != 0) {
                offsetVar.inc(fixedOffset);
            }
        } else {
            resultVar = rowUtils.invoke("decodeBigIntegerKeyHeader", srcVar, offsetVar);
            offsetVar.inc(resultVar.cast(int.class));
        }

        var lengthVar = resultVar.shr(32).cast(int.class);

        if (dstVar == null) {
            offsetVar.inc(lengthVar);
        } else {
            Label notNull = mMaker.label();
            lengthVar.ifNe(0, notNull);
            dstVar.set(null);
            Label cont = mMaker.label();
            mMaker.goto_(cont);
            notNull.here();
            if (mInfo.isDescending()) {
                var copyVar = rowUtils.invoke("copyAndFlip", srcVar, offsetVar, lengthVar);
                dstVar.set(mMaker.new_(dstVar, copyVar));
            } else {
                dstVar.set(mMaker.new_(dstVar, srcVar, offsetVar, lengthVar));
            }
            offsetVar.inc(lengthVar);
            cont.here();
        }

        return offsetVar;
    }

    @Override
    Variable decodeSkip(Variable srcVar, Variable offsetVar, int fixedOffset, Variable endVar) {
        return decode(null, srcVar, offsetVar, fixedOffset, endVar);
    }
}
