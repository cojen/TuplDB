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

package org.cojen.tupl.table;

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.lang.ref.WeakReference;

import java.util.HashMap;

import java.util.function.IntFunction;
import java.util.function.Predicate;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.DatabaseException;
import org.cojen.tupl.Entry;

import org.cojen.tupl.core.RowPredicate;

import org.cojen.tupl.table.codec.ColumnCodec;

import org.cojen.tupl.table.filter.AndFilter;
import org.cojen.tupl.table.filter.ColumnFilter;
import org.cojen.tupl.table.filter.ColumnToArgFilter;
import org.cojen.tupl.table.filter.ColumnToColumnFilter;
import org.cojen.tupl.table.filter.GroupFilter;
import org.cojen.tupl.table.filter.InFilter;
import org.cojen.tupl.table.filter.OrFilter;
import org.cojen.tupl.table.filter.RowFilter;
import org.cojen.tupl.table.filter.TrueFilter;
import org.cojen.tupl.table.filter.Visitor;

import static org.cojen.tupl.table.ColumnInfo.*;

/**
 * Makes classes that implement RowPredicate and contain the filter arguments needed by
 * FilteredScanMaker.
 *
 * @author Brian S O'Neill
 */
public class RowPredicateMaker {
    private final WeakReference<RowStore> mStoreRef;
    private final Class<? extends RowPredicate> mBaseClass;
    private final Class<?> mRowType;
    private final RowGen mRowGen;
    private final RowGen mPrimaryRowGen;
    private final long mPrimaryIndexId;
    private final long mIndexId;
    private final RowFilter mFilter;
    private final WeakReference<RowFilter> mFilterRef;
    private final String mFilterStr;
    private final RowFilter[][] mRanges;

    private final ClassMaker mClassMaker;
    private final MethodMaker mCtorMaker;

    private final ColumnCodec[] mCodecs;
    private ColumnCodec[] mPrimaryCodecs;

    /**
     * Constructor for making a full RowPredicate class.
     *
     * @param storeRef is passed along to the generated code
     * @param baseClass required
     * @param primaryRowGen pass non-null to when joining a secondary to a primary
     * @param filter the complete scan filter, not broken down into ranges
     * @param ranges filter broken down into ranges, or null if not applicable. See
     * RowFilter.multiRangeExtract
     */
    RowPredicateMaker(WeakReference<RowStore> storeRef,
                      Class<? extends RowPredicate> baseClass, Class<?> rowType,
                      RowGen rowGen, RowGen primaryRowGen, long primaryIndexId, long indexId,
                      RowFilter filter, String filterStr, RowFilter[][] ranges)
    {
        mStoreRef = storeRef;

        mBaseClass = baseClass;
        mRowType = rowType;
        mRowGen = rowGen;
        mPrimaryRowGen = primaryRowGen;
        mPrimaryIndexId = primaryIndexId;
        mIndexId = indexId;
        mFilter = filter;
        mFilterRef = new WeakReference<>(filter);
        mFilterStr = filterStr;
        mRanges = ranges;

        // Generate a new sub-package to facilitate unloading.
        String subPackage = RowGen.newSubPackage();

        mClassMaker = rowGen.beginClassMaker(getClass(), rowType, subPackage, "predicate")
            .final_().extend(baseClass).implement(RowPredicate.class);

        mCtorMaker = mClassMaker.addConstructor(Object[].class).varargs();
        mCtorMaker.invokeSuperConstructor();

        mCodecs = rowGen.codecsCopy();
    }

    /**
     * Constructor for making a plain Predicate class.
     */
    RowPredicateMaker(Class<?> rowType, RowGen rowGen, RowFilter filter, String filterStr) {
        mStoreRef = null;

        mBaseClass = null;
        mRowType = rowType;
        mRowGen = rowGen;
        mPrimaryRowGen = null;
        mPrimaryIndexId = 0;
        mIndexId = 0;
        mFilter = filter;
        mFilterRef = new WeakReference<>(filter);
        mFilterStr = filterStr;
        mRanges = null;

        mClassMaker = rowGen.beginClassMaker(getClass(), rowType, null, "predicate")
            .final_().implement(Predicate.class);

        mCtorMaker = mClassMaker.addConstructor(Object[].class).varargs();
        mCtorMaker.invokeSuperConstructor();

        mCodecs = rowGen.codecsCopy();
    }

    /**
     * Returns a class which contains filtering fields and is constructed with an Object[]
     * parameter for filter arguments. Columns are accessed via their fields, and so they're
     * not checked if they're set or not.
     *
     * Note: This RowPredicateMaker instance should have been constructed for making a full
     * RowPredicate class.
     */
    @SuppressWarnings("unchecked")
    Class<? extends RowPredicate> finish() {
        var defined = new HashMap<String, ColumnCodec>();

        if (mRanges == null) {
            makeAllFields(defined, mFilter, false, true);
        } else {
            // Make fields for the remainders. Initialize any extra fields, under the
            // assumption that they'll always be needed.
            for (RowFilter[] range : mRanges) {
                makeAllFields(defined, range[2], false, true);
            }

            // Make fields for the low/high ranges, which should all be key columns. Extra
            // fields are lazily initialized when this predicate is tested with an encoded key.
            for (RowFilter[] range : mRanges) {
                makeAllFields(defined, range[0], false, false);
                makeAllFields(defined, range[1], false, false);
            }

            // Make fields for any remainders that are applied after joining a secondary to a
            // primary. If there are any of these remainders, then mPrimaryRowGen must not be
            // null. Otherwise, the joined columns won't be found.
            for (RowFilter[] range : mRanges) {
                makeAllFields(defined, range[3], true, false);
            }
        }

        if (!RowPredicate.None.class.isAssignableFrom(mBaseClass)) {
            addRowTestMethod();
            addPartialRowTestMethod();
            addFullDecodeTestMethod();
            addKeyTestMethod();
        }

        addToStringMethod();

        return (Class) mClassMaker.finish();
    }

    /**
     * Returns a MethodHandle which constructs a plain Predicate (not a RowPredicate) from an
     * Object[] parameter for the filter arguments. Columns are accessed via their methods, and
     * so they're checked if they're set or not.
     *
     * Note: This RowPredicateMaker instance should have been constructed for making a plain
     * Predicate class.
     */
    MethodHandle finishPlain() {
        makeAllFields(new HashMap<String, ColumnCodec>(), mFilter, false, true);
        addDirectRowTestMethod();

        addToStringMethod();

        MethodHandles.Lookup lookup = mClassMaker.finishHidden();

        try {
            MethodType mt = MethodType.methodType(void.class, Object[].class);
            MethodHandle mh = lookup.findConstructor(lookup.lookupClass(), mt);
            return mh.asType(MethodType.methodType(Predicate.class, Object[].class));
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    /**
     * For all codecs of the given filter, makes all the necessary fields and optionally
     * initializes them.
     *
     * @param primaryOnly only use primary codec; mPrimaryRowGen must not be null
     * @param init true to initialize extra fields in the constructor
     */
    private void makeAllFields(HashMap<String, ColumnCodec> defined,
                               RowFilter filter,  boolean primaryOnly, boolean init)
    {
        if (filter != null) {
            filter.accept(new FieldMaker(defined, primaryOnly, init));
        }
    }

    private class FieldMaker implements Visitor {
        private final HashMap<String, ColumnCodec> mDefined;
        private final boolean mPrimaryOnly, mInit;

        /**
         * @param primaryOnly only use primary codec; mPrimaryRowGen must not be null
         * @param init true to initialize extra fields in the constructor
         */
        FieldMaker(HashMap<String, ColumnCodec> defined, boolean primaryOnly, boolean init) {
            mDefined = defined;
            mPrimaryOnly = primaryOnly;
            mInit = init;
        }

        @Override
        public void visit(ColumnToArgFilter filter) {
            String colName = filter.column().name;
            String argFieldName = ColumnCodec.argFieldName(colName, filter.argument());

            boolean hasField = mDefined.containsKey(argFieldName);

            if (!mPrimaryOnly && hasField) {
                return;
            }

            ColumnCodec codec = codecFor(colName, mPrimaryOnly);

            if (mPrimaryOnly && codec.equals(mDefined.get(argFieldName))) {
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

            Variable argVar = mCtorMaker.param(0).aget(filter.argument() - 1);
            argVar = ConvertCallSite.make(mCtorMaker, argType, argVar);

            if (!hasField) {
                codec.defineArgField(argVar, argFieldName, argVar);
            }

            codec.filterDefineExtraFields(in, mInit ? argVar : null, argFieldName);

            mDefined.put(argFieldName, codec);
        }

        @Override
        public void visit(ColumnToColumnFilter filter) {
            // Ignore.
        }
    }

    /**
     * @param primaryOnly only use primary codec; mPrimaryRowGen must not be null
     */
    private ColumnCodec codecFor(String colName, boolean primaryOnly) {
        ColumnCodec[] codecs = mCodecs;

        Integer num;

        if (primaryOnly || (num = mRowGen.columnNumbers().get(colName)) == null) notFound: {
            if (mPrimaryRowGen != null) {
                num = mPrimaryRowGen.columnNumbers().get(colName);
                if (num != null) {
                    codecs = mPrimaryCodecs;
                    if (codecs == null) {
                        mPrimaryCodecs = codecs = mPrimaryRowGen.codecsCopy();
                    }
                }
                break notFound;
            }

            throw new IllegalStateException("Column is unavailable for filtering: " + colName);
        }

        int colNum = num;
        ColumnCodec codec = codecs[colNum];

        if (codec.maker != mCtorMaker) { // check if not bound
            codecs[colNum] = codec = codec.bind(mCtorMaker);
        }

        return codec;
    }

    private void addToStringMethod() {
        MethodMaker mm = mClassMaker.addMethod(String.class, "toString").public_();
        var indy = mm.var(RowPredicateMaker.class).indy
            ("indyToString", mRowType, mm.class_(), mFilterRef, mFilterStr);
        mm.return_(indy.invoke(String.class, "toString", new Object[]{Object.class}, mm.this_()));
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
     * Variant which doesn't rely on indy. Used by plain predicate.
     */
    private void addDirectRowTestMethod() {
        MethodMaker mm = mClassMaker.addMethod(boolean.class, "test", Object.class).public_();
        Class<?> rowClass = RowMaker.find(mRowType);
        var tm = new RowTestMaker(mm, mm.param(0).cast(rowClass), mm.this_(), true);
        mFilter.accept(tm);
        tm.finish();
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
            filter = StoredTable.parseFilter(rowType, filterStr);
        }
        MethodMaker mm = MethodMaker.begin(lookup, name, mt);
        var tm = new RowTestMaker(mm, mm.param(0), mm.param(1), false);
        filter.accept(tm);
        tm.finish();
        return new ConstantCallSite(mm.finish());
    }

    /**
     * Implements a predicate test method for a completely filled in row object.
     */
    private static class RowTestMaker implements Visitor {
        private final MethodMaker mMaker;
        private final Variable mRowVar, mPredicateVar;
        private final boolean mChecked;
        private Label mPass, mFail;

        /**
         * @param checked is true to use accessor methods, which check if columns are set
         */
        RowTestMaker(MethodMaker mm, Variable rowVar, Variable predicateVar, boolean checked) {
            mMaker = mm;
            mRowVar = rowVar;
            mPredicateVar = predicateVar;
            mChecked = checked;
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
            for (RowFilter subFilter : subFilters) {
                mFail = mMaker.label();
                subFilter.accept(this);
                mFail.here();
            }
            mMaker.goto_(originalFail);
            mFail = originalFail;
        }

        @Override
        public void visit(AndFilter filter) {
            final Label originalPass = mPass;
            RowFilter[] subFilters = filter.subFilters();
            for (RowFilter subFilter : subFilters) {
                mPass = mMaker.label();
                subFilter.accept(this);
                mPass.here();
            }
            mMaker.goto_(originalPass);
            mPass = originalPass;
        }

        @Override
        public void visit(ColumnToArgFilter filter) {
            ColumnInfo ci = filter.column();
            String colName = ci.name;
            var colVar = accessColumn(colName);
            String argFieldName = ColumnCodec.argFieldName(colName, filter.argument());
            var argVar = mPredicateVar.field(argFieldName);
            CompareUtils.compare(mMaker, ci, colVar, ci, argVar, filter.operator(), mPass, mFail);
        }

        @Override
        public void visit(ColumnToColumnFilter filter) {
            ColumnInfo c1 = filter.column();
            Variable c1Var = accessColumn(c1.name);
            ColumnInfo c2 = filter.otherColumn();
            Variable c2Var = accessColumn(c2.name);

            if (c1Var.classType() != c2Var.classType()) {
                ColumnInfo ci = filter.common();

                var c1ConvertedVar = mMaker.var(ci.type);
                Converter.convertExact(mMaker, null, c1, c1Var, ci, c1ConvertedVar);
                c1 = ci;
                c1Var = c1ConvertedVar;

                var c2ConvertedVar = mMaker.var(ci.type);
                Converter.convertExact(mMaker, null, c2, c2Var, ci, c2ConvertedVar);
                c2 = ci;
                c2Var = c2ConvertedVar;
            }

            CompareUtils.compare(mMaker, c1, c1Var, c2, c2Var, filter.operator(), mPass, mFail);
        }

        private Variable accessColumn(String name) {
            if (mChecked) {
                return mRowVar.invoke(name);
            } else {
                return mRowVar.field(name);
            }
        }
    }

    private void addPartialRowTestMethod() {
        /* TODO: Override the default implementation.

           MethodMaker mm = mClassMaker.addMethod
               (boolean.class, "testP", Object.class, byte[].class, byte[].class).public_();
         */
    }

    private void addFullDecodeTestMethod() {
        MethodMaker mm = mClassMaker.addMethod
            (boolean.class, "test", byte[].class, byte[].class).public_();

        var keyVar = mm.param(0);
        var valueVar = mm.param(1);

        if (mPrimaryIndexId == mIndexId && mRowType != Entry.class) {
            var indy = mm.var(RowPredicateMaker.class).indy
                ("indySchemaDecodeTest", mStoreRef, mRowType, mIndexId, mFilterRef, mFilterStr);

            var schemaVersionVar = mm.var(RowUtils.class).invoke("decodeSchemaVersion", valueVar);

            mm.return_(indy.invoke(boolean.class, "test", null,
                                   schemaVersionVar, keyVar, valueVar, mm.this_()));
        } else {
            var indy = mm.var(RowPredicateMaker.class).indy
                ("indyDecodeTest", mStoreRef, mRowType,
                 mPrimaryIndexId, mIndexId, mFilterRef, mFilterStr);

            mm.return_(indy.invoke(boolean.class, "test", null, keyVar, valueVar, mm.this_()));
        }
    }

    /**
     * Makes a decode test method for the primary index only. A schema version must be decoded.
     *
     * Accepts this MethodType:
     *
     * boolean test(int schemaVersion, byte[] key, byte[] value, RowPredicate predicate)
     */
    public static CallSite indySchemaDecodeTest
        (MethodHandles.Lookup lookup, String name, MethodType mt,
         WeakReference<RowStore> storeRef, Class<?> rowType, long primaryIndexId,
         WeakReference<RowFilter> filterRef, String filterStr)
    {
        var dtm = new DecodeTestMaker
            (lookup, mt, storeRef, rowType, primaryIndexId, filterRef, filterStr);
        return new SwitchCallSite(lookup, mt, dtm);
    }

    /**
     * Implements a predicate test method when at least some decoding is required.
     */
    private static class DecodeTestMaker implements IntFunction<Object> {
        private final MethodHandles.Lookup mLookup;
        private final MethodType mMethodType;
        private final WeakReference<RowStore> mStoreRef;
        private final Class<?> mRowType;
        private final long mPrimaryIndexId;
        private final String mFilterStr;

        // This class isn't defined as a lambda function because this field cannot be final.
        private WeakReference<RowFilter> mFilterRef;

        DecodeTestMaker(MethodHandles.Lookup lookup, MethodType mt,
                        WeakReference<RowStore> storeRef, Class<?> rowType, long primaryIndexId,
                        WeakReference<RowFilter> filterRef, String filterStr)
        {
            mLookup = lookup;
            mMethodType = mt.dropParameterTypes(0, 1);
            mStoreRef = storeRef;
            mRowType = rowType;
            mPrimaryIndexId = primaryIndexId;
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
            } else {
                RowInfo rowInfo;
                try {
                    rowInfo = store.rowInfo(mRowType, mPrimaryIndexId, schemaVersion);
                } catch (Throwable e) {
                    return new ExceptionCallSite.Failed(mMethodType, mm, e);
                }

                RowFilter filter = mFilterRef.get();

                if (filter == null) {
                    filter = StoredTable.parseFilter(mRowType, mFilterStr);
                    mFilterRef = new WeakReference<>(filter);
                }

                int valueOffset = RowUtils.lengthPrefixPF(schemaVersion);
                RowGen rowGen = rowInfo.rowGen();
                var predicateVar = mm.param(2);
                var visitor = new DecodeVisitor(mm, valueOffset, rowGen, predicateVar, null, 0);
                visitor.applyFilter(filter);
                visitor.finishPredicate();
            }

            return mm.finish();
        }
    }

    private void addKeyTestMethod() {
        // Can only check the key columns. If undecided, assume that the predicate matches.
        RowFilter filter = mFilter.retain(mRowGen.info.keyColumns::containsKey,
                                          true, TrueFilter.THE);

        if (filter == TrueFilter.THE) {
            // Rely on the default implementation, which always returns true.
            return;
        }

        WeakReference<RowFilter> filterRef = mFilterRef;
        String filterStr = mFilterStr;

        if (!filter.equals(mFilter)) {
            filterRef = new WeakReference<>(filter);
            filterStr = filter.toString();
        }

        MethodMaker mm = mClassMaker.addMethod(boolean.class, "test", byte[].class).public_();

        var indy = mm.var(RowPredicateMaker.class).indy
            ("indyDecodeTest", mStoreRef, mRowType,
             mPrimaryIndexId, mIndexId, filterRef, filterStr);

        mm.return_(indy.invoke(boolean.class, "test", null, mm.param(0), mm.this_()));
    }

    /**
     * Makes a decode test method for secondary indexes, or for unevolvable types, or for just
     * the key of primary indexes. In all cases, there's no schema version to decode.
     *
     * Accepts these MethodTypes:
     *
     * boolean test(byte[] key, RowPredicate predicate)
     * boolean test(byte[] key, byte[] value, RowPredicate predicate)
     */
    public static CallSite indyDecodeTest(MethodHandles.Lookup lookup, String name, MethodType mt,
                                          WeakReference<RowStore> storeRef,
                                          Class<?> rowType, long primaryIndexId, long indexId,
                                          WeakReference<RowFilter> filterRef, String filterStr)
    {
        return ExceptionCallSite.make(() -> {
            MethodMaker mm = MethodMaker.begin(lookup, name, mt);

            RowStore store = storeRef.get();
            if (store == null) {
                mm.new_(DatabaseException.class, "Closed").throw_();
            } else {
                RowInfo rowInfo;
                try {
                    rowInfo = store.currentRowInfo(rowType, primaryIndexId, indexId);
                } catch (Throwable e) {
                    return new ExceptionCallSite.Failed(mt, mm, e);
                }

                RowFilter filter = filterRef.get();
                if (filter == null) {
                    filter = StoredTable.parseFilter(rowType, filterStr);
                }

                // DecodeVisitor assumes that the second parameter is a byte[] value, but if
                // the filter only examines key columns, it won't attempt to access the value.

                var predicateVar = mm.param(mt.parameterCount() - 1);
                var visitor = new DecodeVisitor(mm, 0, rowInfo.rowGen(), predicateVar, null, 0);
                visitor.applyFilter(filter);
                visitor.finishPredicate();
            }

            return mm.finish();
        });
    }

    public static CallSite indyToString(MethodHandles.Lookup lookup, String name, MethodType mt,
                                        Class<?> rowType, Class<?> rowPredicateClass,
                                        WeakReference<RowFilter> filterRef, String filterStr)
    {
        RowFilter filter = filterRef.get();
        if (filter == null) {
            filter = StoredTable.parseFilter(rowType, filterStr);
        }
        MethodMaker mm = MethodMaker.begin(lookup, name, mt);
        // Cannot define the parameter as the rowPredicateClass itself because it might be a
        // hidden class, and so it cannot appear in the method signature. This is because it
        // doesn't have a valid name. Instead, define the param as an object and cast it.
        var sm = new ToStringMaker(mm, mm.param(0).cast(rowPredicateClass));
        filter.accept(sm);
        mm.return_(sm.mBuilderVar.invoke("toString"));
        return new ConstantCallSite(mm.finish());

    }

    private static class ToStringMaker implements Visitor {
        private final MethodMaker mMaker;
        private final Variable mPredicateVar;

        Variable mBuilderVar;

        ToStringMaker(MethodMaker mm, Variable predicateVar) {
            mMaker = mm;
            mPredicateVar = predicateVar;
            mBuilderVar = mm.new_(StringBuilder.class);
        }

        @Override
        public void visit(OrFilter filter) {
            RowFilter[] subFilters = filter.subFilters();
            if (subFilters.length == 0) {
                mBuilderVar = mBuilderVar.invoke("append", "false");
            } else {
                appendGroupFilter(filter, subFilters);
            }
        }

        @Override
        public void visit(AndFilter filter) {
            RowFilter[] subFilters = filter.subFilters();
            if (subFilters.length == 0) {
                mBuilderVar = mBuilderVar.invoke("append", "true");
            } else {
                appendGroupFilter(filter, subFilters);
            }
        }

        @Override
        public void visit(InFilter filter) {
            int op = filter.operator();
            if (op == ColumnFilter.OP_NOT_IN) {
                mBuilderVar = mBuilderVar.invoke("append", '!').invoke("append", '(');
            }
            mBuilderVar = mBuilderVar.invoke("append", filter.column().name).invoke("append", ' ')
                .invoke("append", "in").invoke("append", ' ');
            appendArgument(filter.column(), filter.argument());
            if (op == ColumnFilter.OP_NOT_IN) {
                mBuilderVar = mBuilderVar.invoke("append", ')');
            }
        }

        @Override
        public void visit(ColumnToArgFilter filter) {
            appendColumnAndOp(filter);
            appendArgument(filter.column(), filter.argument());
        }

        @Override
        public void visit(ColumnToColumnFilter filter) {
            appendColumnAndOp(filter);
            mBuilderVar = mBuilderVar.invoke("append", filter.otherColumn().name);
        }

        protected void appendArgument(ColumnInfo column, int argNum) {
            if (column.hidden) {
                mBuilderVar = mBuilderVar.invoke("append", '?').invoke("append", argNum);
                return;
            }

            Variable argValue = mPredicateVar.field(ColumnCodec.argFieldName(column.name, argNum));
            Class<?> argType = argValue.classType();

            if (argType.isArray()) {
                MethodHandle mh = ArrayStringMaker.make(argType, column.isUnsignedInteger());
                mBuilderVar = mMaker.invoke(mh, mBuilderVar, argValue, 16); // limit=16
                return;
            }

            if (!column.isUnsignedInteger()) {
                int code = column.plainTypeCode();
                if (code == TYPE_UTF8 || code == TYPE_CHAR) {
                    mMaker.var(RowUtils.class).invoke("appendQuotedString", mBuilderVar, argValue);
                } else {
                    mBuilderVar = mBuilderVar.invoke("append", argValue);
                }
                return;
            }

            final Variable strValue = mMaker.var(String.class);

            Label cont = null;

            if (column.isNullable()) {
                Label notNull = mMaker.label();
                argValue.ifNe(null, notNull);
                strValue.set("null");
                cont = mMaker.label().goto_();
                notNull.here();
            }

            switch (column.plainTypeCode()) {
            default: throw new AssertionError();
            case TYPE_UBYTE: argValue = argValue.cast(int.class).and(0xff); break;
            case TYPE_USHORT: argValue = argValue.cast(int.class).and(0xffff); break;
            case TYPE_UINT: case TYPE_ULONG: break;
            }

            strValue.set(argValue.invoke("toUnsignedString", argValue));

            if (cont != null) {
                cont.here();
            }

            mBuilderVar = mBuilderVar.invoke("append", strValue);
        }

        private void appendColumnAndOp(ColumnFilter filter) {
            mBuilderVar = mBuilderVar.invoke("append", filter.column().name).invoke("append", ' ')
                .invoke("append", filter.operatorString()).invoke("append", ' ');
        }

        private void appendGroupFilter(GroupFilter filter, RowFilter[] subFilters) {
            char opChar = filter.opChar();
            for (int i=0; i<subFilters.length; i++) {
                if (i != 0) {
                    mBuilderVar = mBuilderVar.invoke("append", ' ')
                        .invoke("append", opChar).invoke("append", opChar).invoke("append", ' ');
                }
                RowFilter sub = subFilters[i];
                if (sub instanceof GroupFilter) {
                    mBuilderVar = mBuilderVar.invoke("append", '(');
                    sub.accept(this);
                    mBuilderVar = mBuilderVar.invoke("append", ')');
                } else {
                    sub.accept(this);
                }
            }
        }
    }
}
