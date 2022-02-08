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
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.lang.ref.WeakReference;

import java.math.BigDecimal;

import java.util.function.IntFunction;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.DatabaseException;
import org.cojen.tupl.Index;
import org.cojen.tupl.LockResult;

import org.cojen.tupl.core.RowPredicate;

import org.cojen.tupl.diag.QueryPlan;

import org.cojen.tupl.filter.ColumnToArgFilter;
import org.cojen.tupl.filter.RowFilter;
import org.cojen.tupl.filter.TrueFilter;
import org.cojen.tupl.filter.Visitor;

import org.cojen.tupl.io.Utils;

/**
 * Makes ScanControllerFactory classes which perform basic filtering.
 *
 * @author Brian S O'Neill
 */
public class FilteredScanMaker<R> {
    private final WeakReference<RowStore> mStoreRef;
    private final byte[] mSecondaryDescriptor;
    private final Class<?> mTableClass;
    private final Class<?> mPrimaryTableClass;
    private final SingleScanController<R> mUnfiltered;
    private final Class<?> mPredicateClass;
    private final Class<R> mRowType;
    private final RowGen mRowGen;
    private final long mIndexId;
    private final RowFilter mLowBound, mHighBound, mFilter, mJoinFilter;
    private final ClassMaker mFilterMaker;
    private final MethodMaker mFilterCtorMaker;

    // Stop the scan when this filter evaluates to false.
    private String mStopColumn;
    private int mStopArgument;

    /**
     * Three RowFilter objects are passed in, which correspond to results of the
     * RowFilter.rangeExtract method. If all filters are null, then nothing is filtered at all,
     * and this class shouldn't be used.
     *
     * <p>The low/high bound filters operate only against the key column codecs, and so they
     * don't need to handle schema versions. The remainder filter must handle schema versions,
     * and it maintains a weak reference to the RowFilter object, as a minor optimization. If
     * the RowFilter goes away, the filterStr is needed to create it again.
     *
     * @param storeRef is passed along to the generated code
     * @param secondaryDescriptor non-null if table refers to a secondary index or alternate key
     * @param primaryTableClass pass non-null to when joining a secondary to a primary
     * @param unfiltered defines the encode methods; the decode method will be overridden
     * @param predClass contains references to the argument fields
     * @param lowBound pass null for open bound
     * @param highBound pass null for open bound
     * @param filter the filter to apply to all rows which are in bounds, or null if none
     * @param joinFilter the filter to apply after joining, or null if none
     */
    public FilteredScanMaker(WeakReference<RowStore> storeRef, byte[] secondaryDescriptor,
                             Class<?> tableClass, Class<?> primaryTableClass,
                             SingleScanController<R> unfiltered,
                             Class<? extends RowPredicate> predClass,
                             Class<R> rowType, RowGen rowGen, long indexId,
                             RowFilter lowBound, RowFilter highBound,
                             RowFilter filter, RowFilter joinFilter)
    {
        mStoreRef = storeRef;
        mSecondaryDescriptor = secondaryDescriptor;
        mTableClass = tableClass;
        mPrimaryTableClass = primaryTableClass;
        mUnfiltered = unfiltered;
        mPredicateClass = predClass;
        mRowType = rowType;

        mRowGen = rowGen;

        mIndexId = indexId;
        mLowBound = lowBound;
        mHighBound = highBound;
        mFilter = filter;
        mJoinFilter = joinFilter;

        // Define in the same package as the predicate class, in order to access it, and to
        // facilitate class unloading.
        mFilterMaker = mRowGen.anotherClassMaker(getClass(), predClass, "Filter")
            .public_().final_()
            .extend(unfiltered.getClass()).implement(ScanControllerFactory.class);

        mFilterMaker.addField(predClass, "predicate").private_().final_();

        // Need a constructor for the factory singleton instance.
        Object[] mainCtorParams;
        if (unfiltered instanceof SingleScanController.Joined) {
            MethodMaker ctor = mFilterMaker.addConstructor(Index.class).public_();
            ctor.invokeSuperConstructor(null, false, null, false, ctor.param(0));
            mainCtorParams = new Class[] {predClass, Index.class};
        } else {
            MethodMaker ctor = mFilterMaker.addConstructor().public_();
            ctor.invokeSuperConstructor(null, false, null, false);
            mainCtorParams = new Class[] {predClass};
        }

        mFilterCtorMaker = mFilterMaker.addConstructor(mainCtorParams).private_();
        mFilterCtorMaker.field("predicate").set(mFilterCtorMaker.param(0));
    }

    public ScanControllerFactory<R> finish() {
        // Finish the filter class...

        Object[] ctorParams;
        if (mUnfiltered instanceof SingleScanController.Joined) {
            ctorParams = new Object[5];
            ctorParams[4] = mFilterCtorMaker.param(1);
        } else {
            ctorParams = new Object[4];
        }

        ctorParams[0] = null;
        ctorParams[1] = false;
        ctorParams[2] = null;
        ctorParams[3] = false;

        if (mLowBound != null || mHighBound != null) {
            // TODO: Optimize if bounds are the same strings (name >= ? && name <= ?) by
            // avoiding duplicate calls to perform lex encoding.
            if (mLowBound != null) {
                encodeBound(ctorParams, mLowBound, true);
            }
            if (mHighBound != null) {
                encodeBound(ctorParams, mHighBound, false);
            }
        }

        mFilterCtorMaker.invokeSuperConstructor(ctorParams);

        addDecodeRowMethod();

        {
            // Override and return the predicate object.
            MethodMaker mm = mFilterMaker.addMethod(RowPredicate.class, "predicate").public_();
            mm.return_(mm.field("predicate"));
        }

        {
            // Override the plan method specified by ScanController.
            MethodMaker mm = mFilterMaker.addMethod(QueryPlan.class, "plan").public_();

            int joinOption = mPrimaryTableClass == null ? 0 : 1;

            var condy = mm.var(FilteredScanMaker.class).condy
                ("condyPlan", mRowType, mSecondaryDescriptor, joinOption,
                 toString(mLowBound), toString(mHighBound),
                 toString(mFilter), toString(mJoinFilter));

            mm.return_(condy.invoke(QueryPlan.class, "plan"));
        }

        {
            // Specified by ScanControllerFactory.
            MethodMaker mm = mFilterMaker.addMethod
                (QueryPlan.class, "plan", Object[].class).public_();
            // FIXME: substitute non-hidden arguments into query plan
            mm.return_(mm.invoke("plan"));
        }

        // Define the factory methods.
        addFactoryMethod(Object[].class);
        addFactoryMethod(RowPredicate.class);

        return newFactory(mUnfiltered, mFilterMaker.finish());
    }

    public static <R> ScanControllerFactory<R> newFactory(SingleScanController<R> unfiltered,
                                                          Class<?> filterClass)
    {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        try {
            if (unfiltered instanceof SingleScanController.Joined joined) {
                return (ScanControllerFactory<R>) lookup.findConstructor
                    (filterClass, MethodType.methodType(void.class, Index.class))
                    .invoke(joined.mPrimaryIndex);
            } else {
                return (ScanControllerFactory<R>) lookup.findConstructor
                    (filterClass, MethodType.methodType(void.class)).invoke();
            }
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    /**
     * Adds code to the constructor.
     */
    private void encodeBound(Object[] ctorParams, RowFilter bound, boolean low) {
        ColumnCodec[] codecs = mRowGen.keyCodecs();
        var argVars = new Variable[codecs.length];

        var visitor = new Visitor() {
            ColumnToArgFilter last;
            int pos;
            boolean ulp;

            @Override
            public void visit(ColumnToArgFilter filter) {
                ColumnInfo column = filter.column();

                if (filter.operator() == ColumnToArgFilter.OP_EQ
                    && column.type == BigDecimal.class)
                {
                    if (!column.isDescending()) {
                        if (low) {
                            filter = filter.withOperator(ColumnToArgFilter.OP_GE);
                        } else {
                            mStopColumn = column.name;
                            mStopArgument = filter.argument();
                            // Assume that this sub-filter is the last, but use the previous
                            // last now. As a stop filter, it closes the cursor instead of
                            // relying on a high bound.
                            return;
                        }
                    } else {
                        if (low) {
                            filter = filter.withOperator(ColumnToArgFilter.OP_GT);
                            ulp = true;
                        } else {
                            filter = filter.withOperator(ColumnToArgFilter.OP_LE);
                        }
                    }
                }

                last = filter;

                String fieldName = ColumnCodec.argFieldName(filter.column(), filter.argument());
                // param(0) is the predicate instance which has the argument fields.
                Variable argVar = mFilterCtorMaker.param(0).field(fieldName);

                if (ulp) {
                    argVar = argVar.get();
                    Label cont = mFilterCtorMaker.label();
                    argVar.ifEq(null, cont);
                    argVar.set(argVar.invoke("add", argVar.invoke("ulp")));
                    cont.here();
                }

                argVars[pos++] = argVar;
            }
        };

        bound.accept(visitor);

        ColumnToArgFilter last = visitor.last;

        if (last == null) {
            return;
        }

        int numArgs = visitor.pos;
        boolean ulp = visitor.ulp;

        boolean inclusive;
        boolean increment = false;

        if (low) {
            switch (last.operator()) {
            case ColumnToArgFilter.OP_GE: case ColumnToArgFilter.OP_EQ:
                inclusive = true;
                break;
            case ColumnToArgFilter.OP_GT:
                if (numArgs == codecs.length && !ulp) {
                    inclusive = false;
                } else {
                    inclusive = true;
                    increment = true;
                }
                break;
            default: throw new AssertionError();
            }
        } else {
            switch (last.operator()) {
            case ColumnToArgFilter.OP_LT:
                inclusive = false;
                break;
            case ColumnToArgFilter.OP_LE: case ColumnToArgFilter.OP_EQ:
                if (numArgs == codecs.length) {
                    inclusive = true;
                } else {
                    inclusive = false;
                    increment = true;
                }
                break;
            default: throw new AssertionError();
            }
        }

        var boundCodecs = new ColumnCodec[numArgs];

        // Determine the minimum byte array size and prepare the encoders.

        int minSize = 0;
        for (int i=0; i<numArgs; i++) {
            ColumnCodec codec = codecs[i].bind(mFilterCtorMaker);
            boundCodecs[i] = codec;
            minSize += codec.minSize();
            codec.encodePrepare();
        }

        codecs = boundCodecs;

        // Generate code which determines the additional runtime length.

        Variable totalVar = null;
        for (int i=0; i<codecs.length; i++) {
            totalVar = codecs[i].encodeSize(argVars[i], totalVar);
        }

        // Generate code which allocates the destination byte array.

        final Variable dstVar;
        if (totalVar == null) {
            dstVar = mFilterCtorMaker.new_(byte[].class, minSize);
        } else {
            if (minSize != 0) {
                totalVar = totalVar.add(minSize);
            }
            dstVar = mFilterCtorMaker.new_(byte[].class, totalVar);
        }

        // Generate code which fills in the byte array.

        var offsetVar = mFilterCtorMaker.var(int.class).set(0);
        for (int i=0; i<codecs.length; i++) {
            codecs[i].encode(argVars[i], dstVar, offsetVar);
        }

        var rowUtilsVar = mFilterCtorMaker.var(RowUtils.class);

        if (increment) {
            Object overflow = null;
            if (low) {
                overflow = mFilterCtorMaker.var(ScanController.class).field("EMPTY");
            }
            Label cont = null;
            if (ulp) {
                cont = mFilterCtorMaker.label();
                argVars[numArgs - 1].ifEq(null, cont);
            }
            dstVar.set(rowUtilsVar.invoke("increment", dstVar, overflow));
            if (cont != null) {
                cont.here();
            }
        }

        int ctorParamOffset = low ? 0 : 2;
        ctorParams[ctorParamOffset++] = dstVar;
        ctorParams[ctorParamOffset] = inclusive;
    }

    /**
     * @param which Object[] or RowPredicate class
     */
    private void addFactoryMethod(Class which) {
        MethodMaker mm = mFilterMaker.addMethod
            (ScanController.class, "newScanController", which).public_();

        Variable predicateVar;
        if (which == Object[].class) {
            mm.varargs();
            predicateVar = mm.new_(mPredicateClass, mm.param(0));
        } else {
            predicateVar = mm.param(0).cast(mPredicateClass);
        }

        Object[] params;
        if (mUnfiltered instanceof SingleScanController.Joined) {
            params = new Object[2];
            params[1] = mm.field("mPrimaryIndex");
        } else {
            params = new Object[1];
        }

        params[0] = predicateVar;

        mm.return_(mm.new_(mFilterMaker, params));
    }

    private void addDecodeRowMethod() {
        if (mFilter == null || (mSecondaryDescriptor == null && mFilter == TrueFilter.THE)) {
            // No remainder filter, so rely on inherited method.
            return;
        }

        // Implement/override method as specified by RowDecoderEncoder.

        MethodMaker mm = mFilterMaker.addMethod
            (Object.class, "decodeRow", Cursor.class, LockResult.class, Object.class).public_();

        var cursorVar = mm.param(0);
        var resultVar = mm.param(1);
        var rowVar = mm.param(2);
        var predicateVar = mm.field("predicate");

        if (mSecondaryDescriptor == null) {
            // The decode method is implemented using indy, to support multiple schema versions.

            var indy = mm.var(FilteredScanMaker.class).indy
                ("indyFilter", mStoreRef, mTableClass, mRowType, mIndexId,
                 new WeakReference<>(mFilter), mFilter.toString(), mStopColumn, mStopArgument);

            var valueVar = cursorVar.invoke("value");

            var schemaVersion = mm.var(RowUtils.class).invoke("decodeSchemaVersion", valueVar);

            mm.return_(indy.invoke(Object.class, "decodeRow", null, schemaVersion,
                                   cursorVar, rowVar, predicateVar));
            return;
        }

        // Decoding a secondary index row is simpler because it has no schema version.

        var visitor = new DecodeVisitor(mm, 0, mRowGen, predicateVar, mStopColumn, mStopArgument);

        visitor.applyFilter(mFilter);

        if (mPrimaryTableClass == null) {
            // Not joined to a primary.
            Class<?> rowClass = RowMaker.find(mRowType);
            visitor.finishDecode(null, mTableClass, rowClass, rowVar);
            return;
        }

        Variable[] primaryVars = visitor.joinToPrimary(resultVar);

        var primaryKeyVar = primaryVars[0];
        var primaryValueVar = primaryVars[1];

        // Finish filter and decode using indy, to support multiple schema versions.

        long primaryIndexId = ((SingleScanController.Joined) mUnfiltered).mPrimaryIndex.id();

        WeakReference<RowFilter> filterRef = null;
        String filterStr = null;

        if (mJoinFilter != null && mJoinFilter != TrueFilter.THE) {
            filterRef = new WeakReference<>(mJoinFilter);
            filterStr = mJoinFilter.toString();
        }

        var indy = mm.var(FilteredScanMaker.class).indy
            ("indyFilter", mStoreRef, mPrimaryTableClass, mRowType, primaryIndexId,
             filterRef, filterStr, null, 0);

        var schemaVersion = mm.var(RowUtils.class).invoke("decodeSchemaVersion", primaryValueVar);

        mm.return_(indy.invoke(Object.class, "decodeRow", null, schemaVersion,
                               primaryKeyVar, primaryValueVar, rowVar, predicateVar));
    }

    public static CallSite indyFilter(MethodHandles.Lookup lookup, String name, MethodType mt,
                                      WeakReference<RowStore> storeRef,
                                      Class<?> tableClass, Class<?> rowType, long indexId,
                                      WeakReference<RowFilter> filterRef, String filterStr,
                                      String stopColumn, int stopArgument)
    {
        var dm = new FilterMaker
            (lookup, mt, storeRef, tableClass, rowType, indexId,
             filterRef, filterStr, stopColumn, stopArgument);
        return new SwitchCallSite(lookup, mt, dm);
    }

    private static class FilterMaker implements IntFunction<Object> {
        private final MethodHandles.Lookup mLookup;
        private final MethodType mMethodType;
        private final WeakReference<RowStore> mStoreRef;
        private final Class<?> mTableClass;
        private final Class<?> mRowType;
        private final long mIndexId;
        private final String mFilterStr;
        private final String mStopColumn;
        private final int mStopArgument;

        // This class isn't defined as a lambda function because this field cannot be final.
        private WeakReference<RowFilter> mFilterRef;

        FilterMaker(MethodHandles.Lookup lookup, MethodType mt,
                    WeakReference<RowStore> storeRef, Class<?> tableClass,
                    Class<?> rowType, long indexId,
                    WeakReference<RowFilter> filterRef, String filterStr,
                    String stopColumn, int stopArgument)
        {
            mLookup = lookup;
            mMethodType = mt.dropParameterTypes(0, 1);
            mStoreRef = storeRef;
            mTableClass = tableClass;
            mRowType = rowType;
            mIndexId = indexId;
            mFilterStr = filterStr;
            mFilterRef = filterRef;
            mStopColumn = stopColumn;
            mStopArgument = stopArgument;
        }

        /**
         * Defined in IntFunction, needed by SwitchCallSite.
         *
         * @return MethodHandle or ExceptionCallSite.Failed
         */
        @Override
        public Object apply(int schemaVersion) {
            MethodMaker mm = MethodMaker.begin(mLookup, "case", mMethodType);

            RowFilter filter;
            if (mFilterRef == null) {
                filter = TrueFilter.THE;
            } else {
                filter = mFilterRef.get();
                if (filter == null) {
                    filter = AbstractTable.parse(mRowType, mFilterStr);
                    mFilterRef = new WeakReference<>(filter);
                }
            }

            RowStore store = mStoreRef.get();
            if (store == null) {
                mm.new_(DatabaseException.class, "Closed").throw_();
                return mm.finish();
            }

            RowInfo rowInfo;
            MethodHandle decoder;

            try {
                rowInfo = store.rowInfo(mRowType, mIndexId, schemaVersion);

                // Obtain the MethodHandle which fully decodes the value columns.
                decoder = (MethodHandle) mLookup.findStatic
                    (mLookup.lookupClass(), "decodeValueHandle",
                     MethodType.methodType(MethodHandle.class, int.class))
                    .invokeExact(schemaVersion);
            } catch (Throwable e) {
                return new ExceptionCallSite.Failed(mMethodType, mm, e);
            }

            RowGen rowGen = rowInfo.rowGen();
            int valueOffset = RowUtils.lengthPrefixPF(schemaVersion);
            var predicateVar = mm.param(mMethodType.parameterCount() - 1);

            var visitor = new DecodeVisitor
                (mm, valueOffset, rowGen, predicateVar, mStopColumn, mStopArgument);

            visitor.applyFilter(filter);

            Class<?> rowClass = RowMaker.find(mRowType);
            Variable rowVar = mm.param(mMethodType.parameterCount() - 2);
            visitor.finishDecode(decoder, mTableClass, rowClass, rowVar);

            return mm.finish();
        }
    }

    private static String toString(RowFilter filter) {
        return (filter == null || filter == TrueFilter.THE) ? null : filter.toString();
    }

    public static QueryPlan condyPlan(MethodHandles.Lookup lookup, String name, Class type,
                                      Class rowType, byte[] secondaryDesc, int joinOption,
                                      String lowBoundStr, String highBoundStr,
                                      String filterStr, String joinFilterStr)
    {
        RowInfo primaryRowInfo = RowInfo.find(rowType);

        RowInfo rowInfo;
        String which;

        if (secondaryDesc == null) {
            rowInfo = primaryRowInfo;
            which = "primary key";
        } else {
            rowInfo = RowStore.indexRowInfo(primaryRowInfo, secondaryDesc);
            which = rowInfo.isAltKey() ? "alternate key" : "secondary index";
        }

        QueryPlan plan;

        if (lowBoundStr == null && highBoundStr == null) {
            plan = new QueryPlan.FullScan(rowInfo.name, which, rowInfo.keySpec(), false);
        } else {
            plan = new QueryPlan.RangeScan(rowInfo.name, which, rowInfo.keySpec(), false,
                                           lowBoundStr, highBoundStr);
        }
    
        if (filterStr != null) {
            plan = new QueryPlan.Filter(filterStr, plan);
        }

        if (joinOption != 0) {
            rowInfo = primaryRowInfo;
            plan = new QueryPlan.NaturalJoin(rowInfo.name, "primary key", rowInfo.keySpec(), plan);

            if (joinFilterStr != null) {
                plan = new QueryPlan.Filter(joinFilterStr, plan);
            }
        }

        return plan;
    }
}
