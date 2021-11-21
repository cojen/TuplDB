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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.util.HashSet;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.core.RowPredicate;

import org.cojen.tupl.filter.ColumnToArgFilter;
import org.cojen.tupl.filter.RowFilter;
import org.cojen.tupl.filter.Visitor;

import org.cojen.tupl.io.Utils;

/**
 * Makes classes that implement RowPredicate and contain the filter arguments needed by
 * FilteredScanMaker.
 *
 * @author Brian S O'Neill
 */
class RowPredicateMaker {
    private static volatile long cPackageNum;
    private static final VarHandle cPackageNumHandle;

    static {
        try {
            cPackageNumHandle = MethodHandles.lookup().findStaticVarHandle
                (RowPredicateMaker.class, "cPackageNum", long.class);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    /**
     * @param baseClass pass null if predicate locking isn't supported
     * @param filter the complete scan filter, not broken down into ranges
     * @param ranges filter broken down into ranges, or null if not applicable. See
     * RowFilter.multiRangeExtract
     */
    static Class<? extends RowPredicate> make(Class<? extends RowPredicate> baseClass,
                                              Class<?> rowType, RowInfo rowInfo,
                                              RowFilter filter, RowFilter[][] ranges)
    {
        return new RowPredicateMaker(baseClass, rowType, rowInfo.rowGen(), filter, ranges).finish();
    }

    private final Class<? extends RowPredicate> mBaseClass;
    private final RowGen mRowGen;
    private final RowFilter mFilter;
    private final RowFilter[][] mRanges;

    private final ClassMaker mClassMaker;
    private final MethodMaker mCtorMaker;

    private final ColumnCodec[] mKeyCodecs, mValueCodecs;

    /**
     * @param baseClass pass null if predicate locking isn't supported
     * @param filter the complete scan filter, not broken down into ranges
     * @param ranges filter broken down into ranges, or null if not applicable. See
     * RowFilter.multiRangeExtract
     */
    RowPredicateMaker(Class<? extends RowPredicate> baseClass,
                      Class<?> rowType, RowGen rowGen, RowFilter filter, RowFilter[][] ranges)
    {
        if (baseClass == null) {
            baseClass = RowPredicate.None.class;
        }

        mBaseClass = baseClass;
        mRowGen = rowGen;
        mFilter = filter;
        mRanges = ranges;

        // Generate a sub-package with an increasing number to facilitate unloading.
        String packageName = "p" + (long) cPackageNumHandle.getAndAdd(1L);

        mClassMaker = rowGen.beginClassMaker(getClass(), rowType, packageName, "Predicate")
            .final_().extend(baseClass).implement(RowPredicate.class);

        mCtorMaker = mClassMaker.addConstructor(Object[].class).varargs();
        mCtorMaker.invokeSuperConstructor();

        mKeyCodecs = rowGen.keyCodecs();
        mValueCodecs = rowGen.valueCodecs();
    }

    /**
     * Returns a class which contains filtering fields and is constructed with an Object[]
     * parameter for filter arguments.
     */
    @SuppressWarnings("unchecked")
    Class<? extends RowPredicate> finish() {
        var defined = new HashSet<String>();

        if (mRanges == null) {
            makeAllFields(defined, mFilter);
        } else {
            for (RowFilter[] range : mRanges) {
                makeAllFields(defined, range[0]);
            }
            // Must make basic fields for those not yet defined, which should all be key columns.
            for (RowFilter[] range : mRanges) {
                makeBasicFields(defined, range[1]);
                makeBasicFields(defined, range[2]);
            }
        }

        if (!RowPredicate.None.class.isAssignableFrom(mBaseClass)) {
            // FIXME: RowPredicate methods are implemented lazily, using indy. Must reference
            // the complete filter string.
        }

        return (Class) mClassMaker.finish();
    }

    /**
     * For all codecs of the given filter, makes and initializes all the necessary fields.
     */
    private void makeAllFields(HashSet<String> defined, RowFilter filter) {
        if (filter != null) {
            filter.accept(new FieldMaker(defined, true));
        }
    }

    /**
     * For all codecs of the given filter, makes and initializes only the converted argument
     * fields.
     */
    private void makeBasicFields(HashSet<String> defined, RowFilter filter) {
        if (filter != null) {
            filter.accept(new FieldMaker(defined, false));
        }
    }

    private ColumnCodec codecFor(String colName) {
        int colNum;
        {
            Integer num = mRowGen.columnNumbers().get(colName);
            if (num == null) {
                throw new IllegalStateException("Column is unavailable for filtering: " + colName);
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

        return codec;
    }

    private class FieldMaker extends Visitor {
        private final HashSet<String> mDefined;
        private final boolean mAll;

        FieldMaker(HashSet<String> defined, boolean all) {
            mDefined = defined;
            mAll = all;
        }

        @Override
        public void visit(ColumnToArgFilter filter) {
            String colName = filter.column().name;
            String argFieldName = ColumnCodec.argFieldName(colName, filter.argument());

            if (mDefined.contains(argFieldName)) {
                return;
            }

            Class<?> argType = filter.column().type;
            boolean in = filter.isIn(filter.operator());

            if (in) {
                // FIXME: Sort and use binary search if large enough. Be sure to clone array if
                // it wasn't converted.
                // FIXME: Support other types of 'in' arguments: Predicate, IntPredicate, etc.
                argType = argType.arrayType();
            }

            Variable argVar = mCtorMaker.param(0).aget(filter.argument());
            argVar = ConvertCallSite.make(mCtorMaker, argType, argVar);

            ColumnCodec codec = codecFor(colName);
            codec.defineArgField(argVar, argFieldName).set(argVar);

            if (mAll) {
                codec.filterDefineExtraFields(in, argVar, argFieldName);
            }

            mDefined.add(argFieldName);
        }
    }
}
