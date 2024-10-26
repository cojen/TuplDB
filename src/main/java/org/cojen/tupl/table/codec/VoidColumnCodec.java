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

import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.Converter;

/**
 * Makes code for decoding a column which doesn't exist. The ColumnInfo type determines what
 * the decoded result type will be. If nullable, then the decoded value will be null.
 * Otherwise, a suitable value is chosen by the Converter.setDefault method.
 *
 * @author Brian S O'Neill
 */
public final class VoidColumnCodec extends ColumnCodec {
    /**
     * @param info non-null; type can be anything
     * @param mm is null for stateless instance
     */
    public VoidColumnCodec(ColumnInfo info, MethodMaker mm) {
        super(info, mm);
    }

    @Override
    public ColumnCodec bind(MethodMaker mm) {
        return new VoidColumnCodec(info, mm);
    }

    @Override
    protected boolean doEquals(Object obj) {
        // The given object is always the same type as this.
        return true;
    }

    @Override
    protected int doHashCode() {
        return 0;
    }

    @Override
    public int codecFlags() {
        return 0;
    }

    @Override
    public int minSize() {
        return 0;
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
        return totalVar;
    }

    @Override
    public void encode(Variable srcVar, Variable dstVar, Variable offsetVar) {
    }

    @Override
    public void decode(Variable dstVar, Variable srcVar, Variable offsetVar, Variable endVar) {
        Converter.setDefault(maker, info, dstVar);
    }

    @Override
    public void decodeSkip(Variable srcVar, Variable offsetVar, Variable endVar) {
    }
}
