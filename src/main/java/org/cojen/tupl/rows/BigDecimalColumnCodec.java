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

import java.math.BigInteger;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class BigDecimalColumnCodec extends ColumnCodec {
    private final ColumnCodec mUnscaledCodec;

    protected Variable mScaleVar;
    protected Variable mUnscaledVar;

    /**
     * @param unscaledCodec supports BigInteger
     */
    BigDecimalColumnCodec(ColumnInfo info, ColumnCodec unscaledCodec, MethodMaker mm) {
        super(info, mm);
        mUnscaledCodec = unscaledCodec;
    }

    @Override
    ColumnCodec bind(MethodMaker mm) {
        return new BigDecimalColumnCodec(mInfo, mUnscaledCodec.bind(mm), mm);
    }

    @Override
    int minSize() {
        return 0;
    }

    @Override
    boolean isLast() {
        return mUnscaledCodec.isLast();
    }

    @Override
    void encodePrepare() {
        mScaleVar = mMaker.var(int.class);
        mUnscaledVar = mMaker.var(BigInteger.class);
        mUnscaledCodec.encodePrepare();
    }

    @Override
    void encodeSkip() {
        mScaleVar.set(0);
        mUnscaledVar.set(null);
        mUnscaledCodec.encodeSkip();
    }

    @Override
    Variable encodeSize(Variable srcVar, Variable totalVar) {
        boolean newTotal = false;
        if (totalVar == null) {
            totalVar = mMaker.var(int.class);
            newTotal = true;
        }

        Label end = null;

        if (mInfo.isNullable()) {
            end = mMaker.label();
            Label notNull = mMaker.label();
            srcVar.ifNe(null, notNull);
            encodeSkip();
            if (newTotal) {
                totalVar.set(1);
            } else {
                totalVar.inc(1);
            }
            mMaker.goto_(end);
            notNull.here();
        }

        var rowUtils = mMaker.var(RowUtils.class);

        mScaleVar.set(rowUtils.invoke("convertSignedVarInt", srcVar.invoke("scale")));

        var lengthVar = rowUtils.invoke("calcUnsignedVarIntLength", mScaleVar);

        if (newTotal) {
            totalVar.set(lengthVar);
        } else {
            totalVar.inc(lengthVar);
        }

        mUnscaledVar.set(srcVar.invoke("unscaledValue"));
        totalVar = mUnscaledCodec.encodeSize(mUnscaledVar, totalVar);

        if (end != null) {
            end.here();
        }

        return totalVar;
    }

    @Override
    Variable encode(Variable srcVar, Variable dstVar, Variable offsetVar, int fixedOffset) {
        boolean newOffset = false;
        if (offsetVar == null) {
            offsetVar = mMaker.var(int.class);
            newOffset = true;
        }

        Label end = null;

        if (mInfo.isNullable()) {
            end = mMaker.label();
            Label notNull = mMaker.label();
            srcVar.ifNe(null, notNull);
            if (newOffset) {
                dstVar.aset(fixedOffset, RowUtils.NULL_BYTE_HIGH);
                offsetVar.set(mMaker.var(int.class).set(fixedOffset + 1));
            } else {
                dstVar.aset(offsetVar, RowUtils.NULL_BYTE_HIGH);
                offsetVar.inc(1);
            }
            mMaker.goto_(end);
            notNull.here();
        }

        var rowUtils = mMaker.var(RowUtils.class);

        if (newOffset) {
            offsetVar.set(rowUtils.invoke("encodeUnsignedVarInt", dstVar, fixedOffset, mScaleVar));
        } else {
            offsetVar.set(rowUtils.invoke("encodeUnsignedVarInt", dstVar, offsetVar, mScaleVar));
        }

        offsetVar = mUnscaledCodec.encode(mUnscaledVar, dstVar, offsetVar, 0);

        if (end != null) {
            end.here();
        }

        return offsetVar;
    }

    @Override
    Variable decode(Variable dstVar, Variable srcVar, Variable offsetVar, int fixedOffset,
                    Variable endVar)
    {
        boolean newOffset = false;
        if (offsetVar == null) {
            offsetVar = mMaker.var(int.class);
            newOffset = true;
        }

        Label end = null;

        if (mInfo.isNullable()) {
            end = mMaker.label();
            Variable h = srcVar.aget(newOffset ? fixedOffset : offsetVar).cast(int.class).and(0xff);
            Label notNull = mMaker.label();
            h.ifLt(0xf8, notNull);
            dstVar.set(null);
            if (newOffset) {
                offsetVar.set(fixedOffset + 1);
            } else {
                offsetVar.inc(1);
            }
            mMaker.goto_(end);
            notNull.here();
        }

        var rowUtils = mMaker.var(RowUtils.class);

        Variable decodedVar;
        if (newOffset) {
            decodedVar = rowUtils.invoke("decodeSignedVarInt", srcVar, fixedOffset);
        } else {
            decodedVar = rowUtils.invoke("decodeSignedVarInt", srcVar, offsetVar);
        }

        offsetVar.set(decodedVar.shr(32).cast(int.class));
        var scaleVar = decodedVar.cast(int.class);
        var unscaledVar = mMaker.var(BigInteger.class);
        offsetVar = mUnscaledCodec.decode(unscaledVar, srcVar, offsetVar, 0, endVar);
        dstVar.set(mMaker.new_(dstVar, unscaledVar, scaleVar));

        if (end != null) {
            end.here();
        }

        return offsetVar;
    }

    @Override
    Variable decodeSkip(Variable srcVar, Variable offsetVar, int fixedOffset, Variable endVar) {
        if (isLast()) {
            return mUnscaledCodec.decodeSkip(srcVar, offsetVar, fixedOffset, endVar);
        }

        String method = mInfo.isNullable() ? "skipNullableBigDecimal" : "skipBigDecimal";
        
        var rowUtils = mMaker.var(RowUtils.class);

        Variable lengthVar;
        if (offsetVar == null) {
            offsetVar = rowUtils.invoke(method, srcVar, fixedOffset);
        } else {
            offsetVar.set(rowUtils.invoke(method, srcVar, offsetVar));
        }

        return offsetVar;
    }

    @Override
    Object filterArgPrepare(Variable argVar, int argNum, int op) {
        // FIXME
        throw null;
    }
}
