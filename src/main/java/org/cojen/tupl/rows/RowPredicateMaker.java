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

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;

import java.lang.ref.WeakReference;

import java.util.HashSet;
import java.util.Map;

import java.util.function.IntFunction;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.DatabaseException;

import org.cojen.tupl.core.RowPredicate;

import org.cojen.tupl.filter.AndFilter;
import org.cojen.tupl.filter.ColumnToArgFilter;
import org.cojen.tupl.filter.ColumnToColumnFilter;
import org.cojen.tupl.filter.GroupFilter;
import org.cojen.tupl.filter.OrFilter;
import org.cojen.tupl.filter.RowFilter;
import org.cojen.tupl.filter.Visitor;

import org.cojen.tupl.io.Utils;

/**
 * Makes classes that implement RowPredicate and contain the filter arguments needed by
 * FilteredScanMaker.
 *
 * @author Brian S O'Neill
 */
public class RowPredicateMaker {
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

    private final WeakReference<RowStore> mStoreRef;
    private final Class<?> mTableClass;
    private final Class<? extends RowPredicate> mBaseClass;
    private final Class<?> mRowType;
    private final RowGen mRowGen;
    private final long mIndexId;
    private final RowFilter mFilter;
    private final WeakReference<RowFilter> mFilterRef;
    private final String mFilterStr;
    private final RowFilter[][] mRanges;

    private final ClassMaker mClassMaker;
    private final MethodMaker mCtorMaker;

    private final ColumnCodec[] mKeyCodecs, mValueCodecs;

    /**
     * @param storeRef is passed along to the generated code
     * @param baseClass pass null if predicate locking isn't supported
     * @param filter the complete scan filter, not broken down into ranges
     * @param ranges filter broken down into ranges, or null if not applicable. See
     * RowFilter.multiRangeExtract
     */
    RowPredicateMaker(WeakReference<RowStore> storeRef, Class<?> tableClass,
                      Class<? extends RowPredicate> baseClass, Class<?> rowType, RowGen rowGen,
                      long indexId, RowFilter filter, String filterStr, RowFilter[][] ranges)
    {
        mStoreRef = storeRef;
        mTableClass = tableClass;

        if (baseClass == null) {
            baseClass = RowPredicate.None.class;
        }

        mBaseClass = baseClass;
        mRowType = rowType;
        mRowGen = rowGen;
        mIndexId = indexId;
        mFilter = filter;
        mFilterRef = new WeakReference<>(filter);
        mFilterStr = filterStr;
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
            addRowTestMethod();
            addPartialDecodeTestMethod();
            addFullDecodeTestMethod();
            // FIXME
            //addKeyTestMethod(long, byte[].class);
        }

        // Add toString method.
        {
            // TODO: Substitute the argument values for non-hidden columns.
            mClassMaker.addMethod(String.class, "toString").public_().return_(mFilterStr);
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

    private void addRowTestMethod() {
        MethodMaker mm = mClassMaker.addMethod(boolean.class, "test", Object.class).public_();
        var indy = mm.var(RowPredicateMaker.class).indy
            ("indyRowTest", mRowType, mFilterRef, mFilterStr);
        Class<?> rowClass = RowMaker.find(mRowType);
        mm.return_(indy.invoke(boolean.class, "test", null,
                               mm.param(0).cast(rowClass), mm.this_()));
    }

    /**
     * Accepts this MethodType:
     *
     * boolean test(R row, RowPredicate predicate)
     */
    public static CallSite indyRowTest(MethodHandles.Lookup lookup, String name, MethodType mt,
                                       Class<?> rowType,
                                       WeakReference<RowFilter> filterRef, String filterStr)
    {
        RowFilter filter = filterRef.get();
        if (filter == null) {
            filter = AbstractTable.parse(rowType, filterStr);
        }
        MethodMaker mm = MethodMaker.begin(lookup, name, mt);
        var tm = new RowTestMaker(mm, mm.param(0), mm.param(1));
        filter.accept(tm);
        tm.finish();
        return new ConstantCallSite(mm.finish());
    }

    /**
     * Implements a predicate test method for a completely filled in row object.
     */
    private static class RowTestMaker extends Visitor {
        private final MethodMaker mMaker;
        private final Variable mRowVar, mPredicateVar;
        private Label mPass, mFail;

        RowTestMaker(MethodMaker mm, Variable rowVar, Variable predicateVar) {
            mMaker = mm;
            mRowVar = rowVar;
            mPredicateVar = predicateVar;
            mPass = mm.label();
            mFail = mm.label();
        }

        void finish() {
            mFail.here();
            mMaker.return_(false);
            mPass.here();
            mMaker.return_(true);
        }

        @Override
        public void visit(OrFilter filter) {
            final Label originalFail = mFail;
            RowFilter[] subFilters = filter.subFilters();
            for (int i=0; i<subFilters.length; i++) {
                mFail = mMaker.label();
                subFilters[i].accept(this);
                mFail.here();
            }
            mMaker.goto_(originalFail);
            mFail = originalFail;
        }

        @Override
        public void visit(AndFilter filter) {
            final Label originalPass = mPass;
            RowFilter[] subFilters = filter.subFilters();
            for (int i=0; i<subFilters.length; i++) {
                mPass = mMaker.label();
                subFilters[i].accept(this);
                mPass.here();
            }
            mMaker.goto_(originalPass);
            mPass = originalPass;
        }

        @Override
        public void visit(ColumnToArgFilter filter) {
            ColumnInfo ci = filter.column();
            String colName = ci.name;
            // TODO: direct field access
            var colVar = mRowVar.invoke(colName);
            String argFieldName = ColumnCodec.argFieldName(colName, filter.argument());
            var argVar = mPredicateVar.field(argFieldName);
            CompareUtils.compare(mMaker, ci, colVar, ci, argVar, filter.operator(), mPass, mFail);
        }

        @Override
        public void visit(ColumnToColumnFilter filter) {
            ColumnInfo c1 = filter.column();
            // TODO: direct field access
            var c1Var = mRowVar.invoke(c1.name);
            ColumnInfo c2 = filter.otherColumn();
            // TODO: direct field access
            var c2Var = mRowVar.invoke(c2.name);
            CompareUtils.compare(mMaker, c1, c1Var, c2, c2Var, filter.operator(), mPass, mFail);
        }
    }

    private void addPartialDecodeTestMethod() {
        MethodMaker mm = mClassMaker.addMethod
            (boolean.class, "test", Object.class, byte[].class).public_();

        // If the filter only accesses key columns, then call the test(row) method.
        {
            Map<String, ColumnInfo> keyColumns = mRowGen.info.keyColumns;

            var checker = new Visitor() {
                boolean stop;

                @Override
                protected void subVisit(GroupFilter filter) {
                    for (RowFilter sub : filter.subFilters()) {
                        sub.accept(this);
                        if (stop) {
                            return;
                        }
                    }
                }

                @Override
                public void visit(ColumnToArgFilter filter) {
                    check(filter.column());
                }

                @Override
                public void visit(ColumnToColumnFilter filter) {
                    check(filter.column());
                    check(filter.otherColumn());
                }

                private void check(ColumnInfo column) {
                    if (!keyColumns.containsKey(column.name)) {
                        stop = true;
                    }
                }
            };

            mFilter.accept(checker);

            if (!checker.stop) {
                mm.return_(mm.invoke("test", mm.param(0)));
                return;
            }
        }

        var indy = mm.var(RowPredicateMaker.class).indy
            ("indyDecodeTest", mStoreRef, mTableClass, mRowType, mIndexId, mFilterRef, mFilterStr);

        Class<?> rowClass = RowMaker.find(mRowType);
        var rowVar = mm.param(0).cast(rowClass);
        var valueVar = mm.param(1);

        mm.return_(indy.invoke(boolean.class, "test", null,
                               mm.var(RowUtils.class).invoke("decodeSchemaVersion", valueVar),
                               rowVar, valueVar, mm.this_()));
    }

    private void addFullDecodeTestMethod() {
        MethodMaker mm = mClassMaker.addMethod
            (boolean.class, "test", byte[].class, byte[].class).public_();

        var indy = mm.var(RowPredicateMaker.class).indy
            ("indyDecodeTest", mStoreRef, mTableClass, mRowType, mIndexId, mFilterRef, mFilterStr);

        var valueVar = mm.param(1);

        mm.return_(indy.invoke(boolean.class, "test", null,
                               mm.var(RowUtils.class).invoke("decodeSchemaVersion", valueVar),
                               mm.param(0), valueVar, mm.this_()));
    }

    /**
     * Accepts these MethodTypes:
     *
     * boolean test(int schemaVersion, R row, byte[] value, RowPredicate predicate)
     * boolean test(int schemaVersion, byte[] key, byte[] value, RowPredicate predicate)
     */
    public static CallSite indyDecodeTest(MethodHandles.Lookup lookup, String name, MethodType mt,
                                          WeakReference<RowStore> storeRef,
                                          Class<?> tableClass, Class<?> rowType, long indexId,
                                          WeakReference<RowFilter> filterRef, String filterStr)
    {
        var dtm = new DecodeTestMaker
            (lookup, mt, storeRef, tableClass, rowType, indexId, filterRef, filterStr);
        return new SwitchCallSite(lookup, mt, dtm);
    }

    /**
     * Implements a predicate test method when at least some decoding is required.
     */
    private static class DecodeTestMaker implements IntFunction<Object> {
        private final MethodHandles.Lookup mLookup;
        private final MethodType mMethodType;
        private final WeakReference<RowStore> mStoreRef;
        private final Class<?> mTableClass;
        private final Class<?> mRowType;
        private final long mIndexId;
        private final String mFilterStr;

        // This class isn't defined as a lambda function because this field cannot be final.
        private WeakReference<RowFilter> mFilterRef;

        DecodeTestMaker(MethodHandles.Lookup lookup, MethodType mt,
                        WeakReference<RowStore> storeRef, Class<?> tableClass,
                        Class<?> rowType, long indexId,
                        WeakReference<RowFilter> filterRef, String filterStr)
        {
            mLookup = lookup;
            mMethodType = mt.dropParameterTypes(0, 1);
            mStoreRef = storeRef;
            mTableClass = tableClass;
            mRowType = rowType;
            mIndexId = indexId;
            mFilterStr = filterStr;
            mFilterRef = filterRef;
        }

        /**
         * Defined in IntFunction, needed by SwitchCallSite.
         *
         * @return MethodHandle or ExceptionCallSite.Failed
         */
        @Override
        public Object apply(int schemaVersion) {
            MethodMaker mm = MethodMaker.begin(mLookup, "test", mMethodType);

            RowStore store = mStoreRef.get();
            if (store == null) {
                mm.new_(DatabaseException.class, "Closed").throw_();
                return mm.finish();
            }

            RowInfo rowInfo;
            try {
                rowInfo = store.rowInfo(mRowType, mIndexId, schemaVersion);
            } catch (Throwable e) {
                return new ExceptionCallSite.Failed(mMethodType, mm, e);
            }

            RowFilter filter = mFilterRef.get();

            if (filter == null) {
                filter = AbstractTable.parse(mRowType, mFilterStr);
                mFilterRef = new WeakReference<>(filter);
            }

            if (mm.param(0).classType() != byte[].class) {
                // The key columns are found in the row, so check those first to reduce the
                // amount of columns that need to be decoded.
                filter = filter.prioritize(rowInfo.keyColumns);
            }

            int valueOffset = RowUtils.lengthPrefixPF(schemaVersion);
            RowGen rowGen = rowInfo.rowGen();
            var predicateVar = mm.param(2);
            var visitor = new DecodeVisitor
                (mm, valueOffset, mTableClass, rowGen, predicateVar, null, 0);
            filter.accept(visitor);
            visitor.finishPredicate();

            return mm.finish();
        }
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
