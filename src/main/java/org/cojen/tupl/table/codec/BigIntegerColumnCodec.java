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

import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.RowUtils;

/**
 * Abstract class for encoding and decoding BigInteger type columns.
 *
 * @author Brian S O'Neill
 */
abstract class BigIntegerColumnCodec extends BytesColumnCodec {
    /**
     * @param info non-null
     * @param mm is null for stateless instance
     */
    BigIntegerColumnCodec(ColumnInfo info, MethodMaker mm) {
        super(info, mm);
    }

    @Override
    protected boolean doEquals(Object obj) {
        return true;
    }

    @Override
    protected final int doHashCode() {
        return 0;
    }

    @Override
    protected Variable filterPrepareBytes(Variable argVar) {
        return maker.var(RowUtils.class).invoke("encodeBigInteger", argVar);
    }

    @Override
    protected boolean compareBytesUnsigned() {
        return false;
    }
}
