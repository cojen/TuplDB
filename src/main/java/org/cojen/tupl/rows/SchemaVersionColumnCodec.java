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

import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

/**
 * Makes code for encoding the schema version pseudo column.
 *
 * @see RowUtils#lengthPrefixPF
 * @author Brian S O'Neill
 */
class SchemaVersionColumnCodec extends ColumnCodec {
    // Version is pre-encoded using the prefix format.
    private final int mVersion;

    SchemaVersionColumnCodec(int version) {
        this(version, null);
    }

    /**
     * Construct a bound instance.
     */
    SchemaVersionColumnCodec(int version, MethodMaker mm) {
        super(null, mm);
        mVersion = version < 128 ? version : (version | (1 << 31));
    }

    private SchemaVersionColumnCodec(MethodMaker mm, int version) {
        super(null, mm);
        mVersion = version;
    }

    @Override
    ColumnCodec bind(MethodMaker mm) {
        return new SchemaVersionColumnCodec(mm, mVersion);
    }

    @Override
    int minSize() {
        return mVersion < 0 ? 4 : 1;
    }

    @Override
    boolean isLast() {
        return false;
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
    Variable encode(Variable srcVar, Variable dstVar, Variable offsetVar, int fixedOffset) {
        if (mVersion >= 0) {
            if (offsetVar == null) {
                dstVar.aset(fixedOffset, (byte) mVersion);
            } else {
                dstVar.aset(offsetVar, (byte) mVersion);
                offsetVar.inc(1);
            }
        } else {
            var utils = mMaker.var(RowUtils.class);
            if (offsetVar == null) {
                utils.invoke("encodeIntBE", dstVar, fixedOffset, mVersion);
            } else {
                utils.invoke("encodeIntBE", dstVar, offsetVar, mVersion);
                offsetVar.inc(4);
            }
        }
        return offsetVar;
    }

    @Override
    Variable decode(Variable dstVar, Variable srcVar, Variable offsetVar, int fixedOffset,
                    Variable endVar)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    Variable decodeSkip(Variable srcVar, Variable offsetVar, int fixedOffset, Variable endVar) {
        throw new UnsupportedOperationException();
    }

    @Override
    Object filterArgPrepare(Variable argVar, int argNum, int op) {
        throw new UnsupportedOperationException();
    }
}
