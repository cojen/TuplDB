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

import java.math.BigDecimal;

import java.util.function.Function;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.table.BigDecimalUtils;
import org.cojen.tupl.table.ColumnInfo;

/**
 * Encoding suitable for lexicographically ordered columns which supports nulls.
 *
 * @author Brian S O'Neill
 */
final class LexBigDecimalColumnCodec extends ColumnCodec {
    private Variable mBytesVar;

    LexBigDecimalColumnCodec(ColumnInfo info, MethodMaker mm) {
        super(info, mm);
    }

    @Override
    public ColumnCodec bind(MethodMaker mm) {
        return new LexBigDecimalColumnCodec(info, mm);
    }

    @Override
    protected boolean doEquals(Object obj) {
        return equalOrdering(obj);
    }

    @Override
    public int doHashCode() {
        return 0;
    }

    @Override
    public int codecFlags() {
        return F_LEX;
    }

    @Override
    public int minSize() {
        return 0;
    }

    @Override
    public boolean encodePrepare() {
        mBytesVar = maker.var(byte[].class);
        return true;
    }

    @Override
    public void encodeTransfer(ColumnCodec codec, Function<Variable, Variable> transfer) {
        var dst = (LexBigDecimalColumnCodec) codec;
        dst.mBytesVar = transfer.apply(mBytesVar);
    }

    @Override
    public void encodeSkip() {
        mBytesVar.set(null);
    }

    @Override
    public Variable encodeSize(Variable srcVar, Variable totalVar) {
        String methodName = "encodeBigDecimalLex";
        if (info.isDescending()) {
            methodName += "Desc";
        }

        var utilsVar = maker.var(BigDecimalUtils.class);

        if (!info.isNullable()) {
            mBytesVar.set(utilsVar.invoke(methodName, srcVar));
            return accum(totalVar, mBytesVar.alength());
        }

        boolean unset = false;
        if (totalVar == null) {
            unset = true;
            totalVar = maker.var(int.class);
        }

        Label notNull = maker.label();
        srcVar.ifNe(null, notNull);

        mBytesVar.set(maker.new_(byte[].class, 1));
        mBytesVar.aset(0, nullByte());
        if (unset) {
            totalVar.set(1);
        } else {
            totalVar.inc(1);
        }
        Label end = maker.label().goto_();

        notNull.here();
        mBytesVar.set(utilsVar.invoke(methodName, srcVar));
        var lenVar = mBytesVar.alength();
        if (unset) {
            totalVar.set(lenVar);
        } else {
            totalVar.inc(lenVar);
        }

        end.here();

        return totalVar;
    }

    @Override
    public void encode(Variable srcVar, Variable dstVar, Variable offsetVar) {
        var lengthVar = mBytesVar.alength();
        maker.var(System.class).invoke("arraycopy", mBytesVar, 0, dstVar, offsetVar, lengthVar);
        offsetVar.inc(lengthVar);
    }

    @Override
    public void decode(Variable dstVar, Variable srcVar, Variable offsetVar, Variable endVar) {
        String methodName = "decodeBigDecimalLex";
        if (info.isDescending()) {
            methodName += "Desc";
        }

        Variable bdRefVar = dstVar == null ? null : maker.new_(BigDecimal[].class, 1);

        var rowUtils = maker.var(BigDecimalUtils.class);
        offsetVar.set(rowUtils.invoke(methodName, srcVar, offsetVar, bdRefVar));

        if (dstVar != null) {
            dstVar.set(bdRefVar.aget(0));
        }
    }

    @Override
    public void decodeSkip(Variable srcVar, Variable offsetVar, Variable endVar) {
        decode(null, srcVar, offsetVar, endVar);
    }
}
