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

import java.math.BigInteger;

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
final class BigDecimalColumnCodec extends ColumnCodec {
    private final ColumnCodec mUnscaledCodec;

    private Variable mScaleVar;
    private Variable mUnscaledVar;

    /**
     * @param unscaledCodec supports BigInteger
     */
    BigDecimalColumnCodec(ColumnInfo info, ColumnCodec unscaledCodec, MethodMaker mm) {
        super(info, mm);
        mUnscaledCodec = unscaledCodec;
    }

    @Override
    public ColumnCodec bind(MethodMaker mm) {
        return new BigDecimalColumnCodec(info, mUnscaledCodec.bind(mm), mm);
    }

    @Override
    protected boolean doEquals(Object obj) {
        return ((BigDecimalColumnCodec) obj).mUnscaledCodec.equals(mUnscaledCodec);
    }

    @Override
    public int doHashCode() {
        return mUnscaledCodec.doHashCode();
    }

    @Override
    public int codecFlags() {
        return mUnscaledCodec.codecFlags();
    }

    @Override
    public int minSize() {
        return 0;
    }

    @Override
    public boolean encodePrepare() {
        mScaleVar = maker.var(int.class);
        mUnscaledVar = maker.var(BigInteger.class);
        mUnscaledCodec.encodePrepare();
        return true;
    }

    @Override
    public void encodeTransfer(ColumnCodec codec, Function<Variable, Variable> transfer) {
        var dst = (BigDecimalColumnCodec) codec;
        dst.mScaleVar = transfer.apply(mScaleVar);
        dst.mUnscaledVar = transfer.apply(mUnscaledVar);
        mUnscaledCodec.encodeTransfer(dst.mUnscaledCodec, transfer);
    }

    @Override
    public void encodeSkip() {
        mScaleVar.set(0);
        mUnscaledVar.set(null);
        mUnscaledCodec.encodeSkip();
    }

    @Override
    public Variable encodeSize(Variable srcVar, Variable totalVar) {
        boolean newTotal = false;
        if (totalVar == null) {
            totalVar = maker.var(int.class);
            newTotal = true;
        }

        Label end = null;

        if (info.isNullable()) {
            end = maker.label();
            Label notNull = maker.label();
            srcVar.ifNe(null, notNull);
            encodeSkip();
            if (newTotal) {
                totalVar.set(1);
            } else {
                totalVar.inc(1);
            }
            maker.goto_(end);
            notNull.here();
        }

        var rowUtils = maker.var(RowUtils.class);

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
    public void encode(Variable srcVar, Variable dstVar, Variable offsetVar) {
        Label end = null;
        if (info.isNullable()) {
            end = maker.label();
            encodeNullHeaderIfNull(end, srcVar, dstVar, offsetVar);
        }

        var rowUtils = maker.var(RowUtils.class);
        offsetVar.set(rowUtils.invoke("encodeUnsignedVarInt", dstVar, offsetVar, mScaleVar));

        mUnscaledCodec.encode(mUnscaledVar, dstVar, offsetVar);

        if (end != null) {
            end.here();
        }
    }

    @Override
    public void decode(Variable dstVar, Variable srcVar, Variable offsetVar, Variable endVar) {
        Label end = null;

        if (info.isNullable()) {
            end = maker.label();
            Variable h = srcVar.aget(offsetVar).cast(int.class).and(0xff);
            Label notNull = maker.label();
            h.ifLt(0xf8, notNull);
            dstVar.set(null);
            offsetVar.inc(1);
            maker.goto_(end);
            notNull.here();
        }

        var rowUtils = maker.var(RowUtils.class);
        var decodedVar = rowUtils.invoke("decodeSignedVarInt", srcVar, offsetVar);

        offsetVar.set(decodedVar.shr(32).cast(int.class));
        var scaleVar = decodedVar.cast(int.class);
        var unscaledVar = maker.var(BigInteger.class);
        mUnscaledCodec.decode(unscaledVar, srcVar, offsetVar, endVar);
        dstVar.set(maker.new_(dstVar, unscaledVar, scaleVar));

        if (end != null) {
            end.here();
        }
    }

    @Override
    public void decodeSkip(Variable srcVar, Variable offsetVar, Variable endVar) {
        if (isLast()) {
            mUnscaledCodec.decodeSkip(srcVar, offsetVar, endVar);
        } else {
            String method = info.isNullable() ? "skipNullableBigDecimal" : "skipBigDecimal";
            offsetVar.set(maker.var(RowUtils.class).invoke(method, srcVar, offsetVar));
        }
    }
}
