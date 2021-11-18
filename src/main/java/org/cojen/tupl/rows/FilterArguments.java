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

import java.util.HashMap;
import java.util.HashSet;

import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.filter.ColumnToArgFilter;

/**
 * Manages the argument variables which are passed to the constructor of filtering classes.
 *
 * @author Brian S O'Neill
 * @see FilteredScanMaker
 */
class FilterArguments {
    private final RowGen mRowGen;
    private final MethodMaker mCtorMaker;

    private final ColumnCodec[] mKeyCodecs, mValueCodecs;

    private static record ColumnArg(ColumnInfo column, int argument) { }

    private final HashMap<ColumnArg, Variable> mArgVarMap;
    private final HashSet<ColumnArg> mHasFields;

    /**
     * @param ctor first constructor param must be an Object[] of arguments
     */
    FilterArguments(RowGen rowGen, MethodMaker ctorMaker) {
        mRowGen = rowGen;
        mCtorMaker = ctorMaker;

        mKeyCodecs = mRowGen.keyCodecs();
        mValueCodecs = mRowGen.valueCodecs();

        mArgVarMap = new HashMap<>();
        mHasFields = new HashSet<>();
    }

    /**
     * Returns a reference to the argument in the scope of the given MethodMaker. The argument
     * will have already been converted to the correct type, based on the current codec. An
     * exception is thrown at runtime if conversion isn't supported.
     *
     * As a side-effect of calling this method, fields are defined and code is added to the
     * constructor.
     *
     * @param mm can pass null to force field creation, but no variable is returned
     */
    Variable argVar(MethodMaker mm, ColumnToArgFilter filter) {
        var key = new ColumnArg(filter.column(), filter.argument());

        var argVar = mArgVarMap.get(key);

        if (argVar == null) {
            Class<?> argType = key.column.type;

            if (filter.isIn(filter.operator())) {
                // FIXME: Sort and use binary search if large enough. Be sure to clone array if
                // it wasn't converted.
                // FIXME: Support other types of 'in' arguments: Predicate, IntPredicate, etc.
                argType = argType.arrayType();
            }

            argVar = mCtorMaker.param(0).aget(key.argument);
            argVar = ConvertCallSite.make(mCtorMaker, argType, argVar);

            mArgVarMap.put(key, argVar);
        }

        if (mm == mCtorMaker) {
            return argVar;
        }

        String colName = key.column.name;

        if (!mHasFields.contains(key)) {
            int colNum;
            {
                Integer num = mRowGen.columnNumbers().get(colName);
                if (num == null) {
                    throw new IllegalStateException
                        ("Column is unavailable for filtering: " + colName);
                }
                colNum = num;
            }

            ColumnCodec[] codecs = mKeyCodecs;

            if (colNum >= codecs.length) {
                colNum -= codecs.length;
                codecs = mValueCodecs;
            }

            ColumnCodec codec = codecs[colNum];

            if (codec.mMaker != mCtorMaker) { // check if not bound
                codecs[colNum] = codec = codec.bind(mCtorMaker);
            }

            codec.filterDefineFields(filter.isIn(filter.operator()), argVar, key.argument);

            mHasFields.add(key);
        }

        return mm == null ? null : mm.field(colName);
    }
}

