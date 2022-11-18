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

import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

/**
 * Makes code for decoding a column which doesn't exist. The ColumnInfo type determines what
 * the decoded result type will be. If nullable, then the decoded value will be null.
 * Otherwise, a suitable value is chosen by the Converter.setDefault method.
 *
 * @author Brian S O'Neill
 */
final class VoidColumnCodec extends ColumnCodec {
    /**
     * @param info non-null; type can be anything
     * @param mm is null for stateless instance
     */
    VoidColumnCodec(ColumnInfo info, MethodMaker mm) {
        super(info, mm);
    }

    @Override
    ColumnCodec bind(MethodMaker mm) {
        return new VoidColumnCodec(mInfo, mm);
    }

    @Override
    protected final boolean doEquals(Object obj) {
        // The given object is always the same type as this.
        return true;
    }

    @Override
    protected final int doHashCode() {
        return 0;
    }

    @Override
    int codecFlags() {
        return 0;
    }

    @Override
    int minSize() {
        return 0;
    }

    @Override
    void encodePrepare() {
    }

    @Override
    void encodeSkip() {
    }

    @Override
    Variable encodeSize(Variable srcVar, Variable totalVar) {
        return totalVar;
    }

    @Override
    void encode(Variable srcVar, Variable dstVar, Variable offsetVar) {
    }

    @Override
    void decode(Variable dstVar, Variable srcVar, Variable offsetVar, Variable endVar) {
        Converter.setDefault(mMaker, mInfo, dstVar);
    }

    @Override
    void decodeSkip(Variable srcVar, Variable offsetVar, Variable endVar) {
    }
}
