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
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.lang.ref.WeakReference;

import java.math.BigDecimal;

import java.util.Map;

import java.util.function.IntFunction;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.DatabaseException;
import org.cojen.tupl.Entry;
import org.cojen.tupl.Index;
import org.cojen.tupl.LockResult;

import org.cojen.tupl.core.RowPredicate;

import org.cojen.tupl.diag.QueryPlan;

import org.cojen.tupl.table.codec.ColumnCodec;

import org.cojen.tupl.table.filter.ColumnToArgFilter;
import org.cojen.tupl.table.filter.ColumnToColumnFilter;
import org.cojen.tupl.table.filter.RowFilter;
import org.cojen.tupl.table.filter.TrueFilter;
import org.cojen.tupl.table.filter.Visitor;

import org.cojen.tupl.io.Utils;

import static java.util.Spliterator.*;

/**
 * Makes ScanControllerFactory classes which perform basic filtering.
 *
 * @author Brian S O'Neill
 */
public class FilteredScanMaker<R> {
    private final WeakReference<RowStore> mStoreRef;
    private final byte[] mSecondaryDescriptor;
    private final StoredTable<R> mTable;
    private final Class<?> mPrimaryTableClass;
    private final SingleScanController<R> mUnfiltered;
    private final Class<?> mPredicateClass;
    private final Class<R> mRowType;
    private final RowGen mRowGen;
    private final long mIndexId;
    private final RowFilter mLowBound, mHighBound, mFilter, mJoinFilter;
    private final byte[] mProjectionSpec, mJoinProjectionSpec;
    private final boolean mAlwaysJoin;
    private final boolean mDistinct;
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
     * @param table primary or secondary table
     * @param rowGen primary or secondary rowGen
     * @param unfiltered defines the encode methods; the decode method will be overridden
     * @param predClass contains references to the argument fields
     * @param lowBound pass null for open bound
     * @param highBound pass null for open bound
     * @param filter the filter to apply to all rows which are in bounds, or null if none
     * @param joinFilter the filter to apply after joining, or null if none
     * @param projection null if all columns are to be decoded
     */
    public FilteredScanMaker(WeakReference<RowStore> storeRef, StoredTable<R> table, RowGen rowGen,
                             SingleScanController<R> unfiltered,
                             Class<? extends RowPredicate> predClass,
                             RowFilter lowBound, RowFilter highBound,
                             RowFilter filter, RowFilter joinFilter,
                             Map<String, ColumnInfo> projection)
    {
        Class<R> rowType = table.rowType();
        byte[] secondaryDescriptor = table.secondaryDescriptor();
        Class<?> primaryTableClass = table.joinedPrimaryTableClass();

        RowGen primaryRowGen;

        if (primaryTableClass == null) {
            // Not joining to the primary table.
            mJoinProjectionSpec = null;
            mAlwaysJoin = false;
            if (secondaryDescriptor == null) {
                primaryRowGen = rowGen;
                mProjectionSpec = DecodePartialMaker.makeFullSpec(rowGen, null, projection);
            } else {
                primaryRowGen = RowInfo.find(rowType).rowGen();
                mProjectionSpec = DecodePartialMaker.makeFullSpec
                    (rowGen, primaryRowGen, projection);
            }
        } else {
            primaryRowGen = RowInfo.find(rowType).rowGen();
            mJoinProjectionSpec = DecodePartialMaker.makeFullSpec(primaryRowGen, null, projection);
            if (isCovering(rowGen, primaryRowGen, joinFilter, projection)) {
                // No need to join to the primary table when using a Scanner. An Updater
                // performs a join step to position the cursor over the primary table.
                mAlwaysJoin = false;
                mProjectionSpec = DecodePartialMaker.makeFullSpec
                    (rowGen, primaryRowGen, projection);
            } else {
                // Will always join to the primary and decode afterwards.
                mAlwaysJoin = true;
                mProjectionSpec = mJoinProjectionSpec;
            }
        }

        if (projection == null) {
            mDistinct = true;
        } else {
            mDistinct = primaryRowGen.info.isDistinct(projection.keySet());
        }

        mStoreRef = storeRef;
        mSecondaryDescriptor = secondaryDescriptor;
        mTable = table;
        mPrimaryTableClass = primaryTableClass;
        mUnfiltered = unfiltered;
        mPredicateClass = predClass;
        mRowType = rowType;
        mRowGen = rowGen;
        mIndexId = table.mSource.id();
        mLowBound = lowBound;
        mHighBound = highBound;
        mFilter = filter;
        mJoinFilter = joinFilter;

        // Define in the same package as the predicate class, in order to access it, and to
        // facilitate class unloading.
        mFilterMaker = mRowGen.anotherClassMaker(getClass(), predClass, "filter")
            .public_().final_()
            .extend(unfiltered.getClass()).implement(ScanControllerFactory.class);

        mFilterMaker.addField(predClass, "predicate").private_().final_();

        // Need a constructor for the factory singleton instance.
        Object[] mainCtorParams;
        if (unfiltered instanceof JoinedScanController) {
            MethodMaker ctor = mFilterMaker.addConstructor(Index.class).public_();
            ctor.invokeSuperConstructor(null, false, null, false, false, ctor.param(0));
            mainCtorParams = new Class[] {boolean.class, predClass, Index.class};
        } else {
            MethodMaker ctor = mFilterMaker.addConstructor().public_();
            ctor.invokeSuperConstructor(null, false, null, false, false);
            mainCtorParams = new Class[] {boolean.class, predClass};
        }

        // Begin defining the normal constructor, which will be finished later.
        mFilterCtorMaker = mFilterMaker.addConstructor(mainCtorParams).private_();
        mFilterCtorMaker.field("predicate").set(mFilterCtorMaker.param(1));

        // Define a reverse scan copy constructor.
        MethodMaker ctor = mFilterMaker.addConstructor(mFilterMaker).private_();
        var from = ctor.param(0);
        ctor.invokeSuperConstructor(from);
        ctor.field("predicate").set(from.field("predicate"));
    }

    /**
     * Returns true if a secondary index contains all the projected columns and thus no join to
     * the primary table is required.
     */
    private static boolean isCovering(RowGen rowGen, RowGen primaryRowGen,
                                      RowFilter joinFilter, Map<String, ColumnInfo> projection)
    {
        if (doesFilter(joinFilter)) {
            return false;
        }
        if (projection == null) {
            projection = primaryRowGen.info.allColumns;
        }
        return rowGen.info.allColumns.keySet().containsAll(projection.keySet());
    }

    private static boolean doesFilter(RowFilter filter) {
        return filter != null && filter != TrueFilter.THE;
    }

    public ScanControllerFactory<R> finish() {
        // Finish the filter class...

        Variable reverseVar = mFilterCtorMaker.param(0);

        Object[] ctorParams;
        if (mUnfiltered instanceof JoinedScanController) {
            ctorParams = new Object[6];
            ctorParams[5] = mFilterCtorMaker.param(2);
        } else {
            ctorParams = new Object[5];
        }

        ctorParams[0] = null;
        ctorParams[1] = false;
        ctorParams[2] = null;
        ctorParams[3] = false;
        ctorParams[4] = reverseVar;

        if (mLowBound != null) {
            encodeBound(ctorParams, mLowBound, true);
        }

        boolean loadsOne = false;

        high: if (mHighBound != null) {
            matchCheck: if (mLowBound != null) {
                var keyColumns = mRowGen.info.keyColumns.values().toArray(ColumnInfo[]::new);
                if (!mLowBound.matchesOne(mHighBound, keyColumns)) {
                    break matchCheck;
                }
                // The low and high bounds are exactly the same, and so at most one row
                // will be matched.
                loadsOne = true;
                if (ctorParams[1] != Boolean.TRUE) {
                    // Bounds are expected to be inclusive.
                    throw new AssertionError();
                }
                ctorParams[2] = ctorParams[0];
                ctorParams[3] = true;
                break high;
            }

            encodeBound(ctorParams, mHighBound, false);
        }

        mFilterCtorMaker.invokeSuperConstructor(ctorParams);

        addEvalRowMethod();

        addWriteRowMethod();

        {
            // Override and return the predicate object.
            MethodMaker mm = mFilterMaker.addMethod(RowPredicate.class, "predicate").public_();
            mm.return_(mm.field("predicate"));
        }

        {
            // Specified by ScanControllerFactory.
            MethodMaker mm = mFilterMaker.addMethod
                (RowPredicate.class, "predicate", Object[].class).public_();
            mm.return_(mm.new_(mPredicateClass, mm.param(0)));
        }

        // Define two query plan methods, backed by a dynamic constant.
        for (int i = 0b000; i <= 0b001; i += 0b001) {
            String name = i == 0b000 ? "plan" : "planReverse";
            MethodMaker mm = mFilterMaker.addMethod(QueryPlan.class, name).private_().static_();

            int options = i + (mAlwaysJoin ? 0b010 : 0b000);

            var condy = mm.var(FilteredScanMaker.class).condy
                ("condyPlan", mRowType, mSecondaryDescriptor, options,
                 toString(mLowBound), toString(mHighBound),
                 toString(mFilter), toString(mJoinFilter));

            mm.return_(condy.invoke(QueryPlan.class, name));
        }

        {
            // Specified by ScanControllerFactory.
            MethodMaker mm = mFilterMaker.addMethod
                (QueryPlan.class, "plan", Object[].class).public_().varargs();

            Variable planVar = mm.var(QueryPlan.class);
            Label isReverse = mm.label();
            mm.invoke("isReverse").ifTrue(isReverse);
            planVar.set(mm.invoke("plan"));
            Label cont = mm.label().goto_();
            isReverse.here();
            planVar.set(mm.invoke("planReverse"));
            cont.here();
            
            // FIXME: Substitute non-hidden arguments into the query plan.
            mm.return_(planVar);
        }

        {
            // Specified by ScanControllerFactory.
            MethodMaker mm = mFilterMaker.addMethod
                (ScanControllerFactory.class, "reverse").public_();
            // Invoke the reverse scan copy constructor.
            mm.return_(mm.new_(mFilterMaker, mm.this_()));
        }

        {
            // Specified by ScanControllerFactory.
            MethodMaker mm = mFilterMaker.addMethod(int.class, "argumentCount").public_();

            int max = mLowBound == null ? 0 : mLowBound.maxArgument();
            if (mHighBound != null) {
                max = Math.max(max, mHighBound.maxArgument());
            }
            if (mFilter != null) {
                max = Math.max(max, mFilter.maxArgument());
            }
            if (mJoinFilter != null) {
                max = Math.max(max, mJoinFilter.maxArgument());
            }

            mm.return_(max);
        }

        if (loadsOne) {
            // Override the methods specified by ScanController and implemented by
            // SingleScanController. Return the same value plus SIZED.
            mFilterMaker.addMethod(long.class, "estimateSize").public_().return_(1L);
            mFilterMaker.addMethod(int.class, "characteristics").public_()
                .return_(NONNULL | ORDERED | CONCURRENT | DISTINCT | SIZED);

            // Specified by ScanControllerFactory.
            mFilterMaker.addMethod(boolean.class, "loadsOne").public_().return_(true);

            // Specified by ScanControllerFactory.
            MethodMaker mm = mFilterMaker.addMethod
                (QueryPlan.class, "loadOnePlan", Object[].class).public_().varargs();
            int options = 0b100 + (mAlwaysJoin ? 0b010 : 0b000);
            String bound = toString(mLowBound).replace(">=", "==");
            var condy = mm.var(FilteredScanMaker.class).condy
                ("condyPlan", mRowType, mSecondaryDescriptor, options,
                 bound, null, toString(mFilter), toString(mJoinFilter));
            mm.return_(condy.invoke(QueryPlan.class, "loadOnePlan"));

            // Specified by ScanController.
            mm = mFilterMaker.addMethod(byte[].class, "oneKey").public_();
            mm.return_(mm.invoke("lowBound"));
        } else if (!mDistinct) {
            // Override the method specified by ScanController and implemented by
            // SingleScanController. Return the same value minus DISTINCT.
            mFilterMaker.addMethod(int.class, "characteristics").public_()
                .return_(NONNULL | ORDERED | CONCURRENT);
        }

        // Define the factory methods.
        addFactoryMethod(Object[].class);
        addFactoryMethod(RowPredicate.class);

        return newFactory(mUnfiltered, mFilterMaker.finish());
    }

    @SuppressWarnings("unchecked")
    public static <R> ScanControllerFactory<R> newFactory(SingleScanController<R> unfiltered,
                                                          Class<?> filterClass)
    {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        try {
            if (unfiltered instanceof JoinedScanController joined) {
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
     * Adds code to the constructor by filling in the low or high constructor parameters.
     *
     * @param ctorParams references to the parameters to be passed to the super constructor
     * (byte[] lowBound, boolean lowInclusive, byte[] highBound, boolean highInclusive)
     * @param bound low or high bound
     * @param low indicates which bound is to be encoded
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
                // param(1) is the predicate instance which has the argument fields.
                Variable argVar = mFilterCtorMaker.param(1).field(fieldName);

                if (ulp) {
                    argVar = argVar.get();
                    Label cont = mFilterCtorMaker.label();
                    argVar.ifEq(null, cont);
                    argVar.set(argVar.invoke("add", argVar.invoke("ulp")));
                    cont.here();
                }

                argVars[pos++] = argVar;
            }

            @Override
            public void visit(ColumnToColumnFilter filter) {
                // Ignore.
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
            (ScanController.class, "scanController", which).public_();

        Variable predicateVar;
        if (which == Object[].class) {
            mm.varargs();
            predicateVar = mm.new_(mPredicateClass, mm.param(0));
        } else {
            predicateVar = mm.param(0).cast(mPredicateClass);
        }

        Object[] params;
        if (mUnfiltered instanceof JoinedScanController) {
            params = new Object[3];
            params[2] = mm.field("mPrimaryIndex");
        } else {
            params = new Object[2];
        }

        params[0] = mm.invoke("isReverse");
        params[1] = predicateVar;

        mm.return_(mm.new_(mFilterMaker, params));
    }

    private void addEvalRowMethod() {
        // Check if projecting all columns and there's no remainder filter. If so, the original
        // inherited method works just fine.
        if (mProjectionSpec == null && !doesFilter(mFilter) && !doesFilter(mJoinFilter)) {
            return;
        }

        // Implement/override method as specified by RowEvaluator.

        MethodMaker mm = mFilterMaker.addMethod
            (Object.class, "evalRow", Cursor.class, LockResult.class, Object.class).public_();

        var cursorVar = mm.param(0);
        var resultVar = mm.param(1);
        Variable rowVar = mm.param(2);
        var predicateVar = mm.field("predicate");

        if (mSecondaryDescriptor == null && mTable.rowType() != Entry.class) {
            // The eval method is implemented using indy, to support multiple schema versions.

            WeakReference<RowFilter> filterRef;
            String filterStr;
            if (mFilter == null) {
                filterRef = null;
                filterStr = null;
            } else {
                filterRef = new WeakReference<>(mFilter);
                filterStr = mFilter.toString();
            }

            var indy = mm.var(FilteredScanMaker.class).indy
                ("indyFilter", mStoreRef, mTable.getClass(), mRowType, mIndexId,
                 filterRef, filterStr, mProjectionSpec,
                 mStopColumn, mStopArgument);

            var valueVar = cursorVar.invoke("value");

            var schemaVersion = mm.var(RowUtils.class).invoke("decodeSchemaVersion", valueVar);

            mm.return_(indy.invoke(Object.class, "evalRow", null, schemaVersion,
                                   cursorVar, rowVar, predicateVar));

            if (mProjectionSpec != null) {
                overrideDecodeRow();
            }

            return;
        }

        // Decoding an unevolvable row is simpler because it has no schema version.

        var visitor = new DecodeVisitor(mm, 0, mRowGen, predicateVar, mStopColumn, mStopArgument);

        if (mFilter != null) {
            visitor.applyFilter(mFilter);
        }

        if (mPrimaryTableClass != null) {
            // Need to define additional methods for supporting joins to the primary
            // table. These are strictly required by Updater, which always must position a
            // cursor over the primary table.
            addJoinedEval();
            addEvalRowWithPrimaryCursorMethod();
        }

        if (!mAlwaysJoin) {
            // Either not joined to a primary, or this is a covering index, and so Scanner
            // doesn't need to join.

            if (mUnfiltered.isJoined()) {
                mFilterMaker.addMethod(boolean.class, "isJoined").public_().return_(false);
            }

            MethodHandle decoder = null;
            if (mProjectionSpec != null) {
                // Obtain the MethodHandle which decodes the key and value columns.
                decoder = mTable.decodePartialHandle(mProjectionSpec, 0 /*ignored*/);
            }

            Class<?> rowClass = RowMaker.find(mRowType);
            visitor.finishDecode(decoder, null, mTable.getClass(), rowClass, rowVar);

            // Override the inherited method to return the secondary descriptor.
            mm = mFilterMaker.addMethod(byte[].class, "secondaryDescriptor").public_();
            mm.return_(mm.var(byte[].class).setExact(mSecondaryDescriptor));

            if (decoder != null) {
                // Override inherited method, specified by RowEvaluator.
                mm = mFilterMaker.addMethod
                    (Object.class, "decodeRow", Object.class, byte[].class, byte[].class)
                    .public_().override();
                rowVar = CodeUtils.castOrNew(mm.param(0), rowClass);
                mm.invoke(decoder, rowVar, mm.param(1), mm.param(2));
                mm.return_(rowVar);
            }

            return;
        }

        // Note: If no primary key is found, then the generated method returns null a this point.
        Variable[] primaryVars = visitor.joinToPrimary(resultVar, null);

        // These runtime contents of these variables are guaranteed to not be null.
        var primaryKeyVar = primaryVars[0];
        var primaryValueVar = primaryVars[1];

        // Finish filter and decode using a shared private method.
        mm.return_(mm.invoke("joinedEval", primaryKeyVar, primaryValueVar, rowVar));

        // Override the inherited decodeRow method for decoding primary rows.
        overrideDecodeRowForJoin();
    }

    /**
     * Given a non-null primary key and value, fully or partially decodes the a row. The
     * generated method never returns null.
     *
     * This method should only needs to be called when a projection is defined.
     *
     * public R decodeRow(R row, byte[] primaryKey, byte[] primaryValue)
     */
    private void overrideDecodeRow() {
        var mm = mFilterMaker.addMethod
            (Object.class, "decodeRow", Object.class, byte[].class, byte[].class)
            .public_().override();

        var indy = mm.var(FilteredScanMaker.class).indy
            ("indyFilter", mStoreRef, mTable.getClass(), mRowType, mIndexId,
             null, null, mProjectionSpec, mStopColumn, mStopArgument);

        var rowVar = mm.param(0);
        var keyVar = mm.param(1);
        var valueVar = mm.param(2);
        var predicateVar = mm.field("predicate");

        var schemaVersion = mm.var(RowUtils.class).invoke("decodeSchemaVersion", valueVar);

        mm.return_(indy.invoke(Object.class, "decodeRow", null, schemaVersion,
                               keyVar, valueVar, rowVar, predicateVar));
    }

    /**
     * Given a non-null primary key and value, fully or partially decodes a row. The
     * generated method never returns null.
     *
     * public R decodeRow(R row, byte[] primaryKey, byte[] primaryValue)
     */
    private void overrideDecodeRowForJoin() {
        MethodMaker mm = mFilterMaker.addMethod
            (Object.class, "decodeRow", Object.class, byte[].class, byte[].class).public_();

        var rowVar = mm.param(0);
        var primaryKeyVar = mm.param(1);
        var primaryValueVar = mm.param(2);

        if (!doesFilter(mJoinFilter)) {
            // Can call the eval method because no redundant filtering will be applied.
            mm.return_(mm.invoke("joinedEval", primaryKeyVar, primaryValueVar, rowVar));
            return;
        }

        var predicateVar = mm.field("predicate");

        // Use indy, to support multiple schema versions.

        long primaryIndexId = ((JoinedScanController) mUnfiltered).mPrimaryIndex.id();

        var indy = mm.var(FilteredScanMaker.class).indy
            ("indyFilter", mStoreRef, mPrimaryTableClass, mRowType, primaryIndexId,
             null, null, mJoinProjectionSpec, null, 0);

        var schemaVersion = mm.var(RowUtils.class).invoke("decodeSchemaVersion", primaryValueVar);

        mm.return_(indy.invoke(Object.class, "decodeRow", null, schemaVersion,
                               primaryKeyVar, primaryValueVar, rowVar, predicateVar));
    }

    /**
     * Given a non-null primary key and value, applies a filtering step (optional) and fully or
     * partially decodes the a row. The generated method returns null when rows are filtered out.
     *
     * private R joinedEval(byte[] primaryKey, byte[] primaryValue, R row)
     */
    private void addJoinedEval() {
        MethodMaker mm = mFilterMaker.addMethod
            (Object.class, "joinedEval", byte[].class, byte[].class, Object.class).private_();

        var primaryKeyVar = mm.param(0);
        var primaryValueVar = mm.param(1);
        var rowVar = mm.param(2);
        var predicateVar = mm.field("predicate");

        // Use indy, to support multiple schema versions.

        long primaryIndexId = ((JoinedScanController) mUnfiltered).mPrimaryIndex.id();

        WeakReference<RowFilter> filterRef = null;
        String filterStr = null;

        if (doesFilter(mJoinFilter)) {
            filterRef = new WeakReference<>(mJoinFilter);
            filterStr = mJoinFilter.toString();
        }

        var indy = mm.var(FilteredScanMaker.class).indy
            ("indyFilter", mStoreRef, mPrimaryTableClass, mRowType, primaryIndexId,
             filterRef, filterStr, mJoinProjectionSpec, null, 0);

        var schemaVersion = mm.var(RowUtils.class).invoke("decodeSchemaVersion", primaryValueVar);

        mm.return_(indy.invoke(Object.class, "evalRow", null, schemaVersion,
                               primaryKeyVar, primaryValueVar, rowVar, predicateVar));
    }

    /**
     * Defines the other evalRow method which takes a primary cursor, used by Updater.
     */
    private void addEvalRowWithPrimaryCursorMethod() {
        // Implement/override method as specified by RowEvaluator.

        MethodMaker mm = mFilterMaker.addMethod
            (Object.class, "evalRow", Cursor.class, LockResult.class, Object.class,
             Cursor.class).public_();

        //var cursorVar = mm.param(0);
        var resultVar = mm.param(1);
        var rowVar = mm.param(2);
        var primaryCursorVar = mm.param(3);
        var predicateVar = mm.field("predicate");

        var visitor = new DecodeVisitor(mm, 0, mRowGen, predicateVar, mStopColumn, mStopArgument);

        if (mFilter != null) {
            visitor.applyFilter(mFilter);
        }

        // Note: If no primary key is found, then the generated method returns null a this point.
        Variable[] primaryVars = visitor.joinToPrimary(resultVar, primaryCursorVar);

        // These runtime contents of these variables are guaranteed to not be null.
        var primaryKeyVar = primaryVars[0];
        var primaryValueVar = primaryVars[1];

        mm.return_(mm.invoke("joinedEval", primaryKeyVar, primaryValueVar, rowVar));
    }

    public static CallSite indyFilter(MethodHandles.Lookup lookup, String name, MethodType mt,
                                      WeakReference<RowStore> storeRef,
                                      Class<?> tableClass, Class<?> rowType, long indexId,
                                      WeakReference<RowFilter> filterRef, String filterStr,
                                      byte[] projectionSpec, String stopColumn, int stopArgument)
    {
        var dm = new FilterMaker
            (lookup, mt, storeRef, tableClass, rowType, indexId,
             filterRef, filterStr, projectionSpec, stopColumn, stopArgument);
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
        private final byte[] mProjectionSpec;
        private final String mStopColumn;
        private final int mStopArgument;

        // This class isn't defined as a lambda function because this field cannot be final.
        private WeakReference<RowFilter> mFilterRef;

        FilterMaker(MethodHandles.Lookup lookup, MethodType mt,
                    WeakReference<RowStore> storeRef, Class<?> tableClass,
                    Class<?> rowType, long indexId,
                    WeakReference<RowFilter> filterRef, String filterStr,
                    byte[] projectionSpec, String stopColumn, int stopArgument)
        {
            mLookup = lookup;
            mMethodType = mt.dropParameterTypes(0, 1);
            mStoreRef = storeRef;
            mTableClass = tableClass;
            mRowType = rowType;
            mIndexId = indexId;
            mFilterStr = filterStr;
            mFilterRef = filterRef;
            mProjectionSpec = projectionSpec;
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
            MethodMaker mm = MethodMaker.begin(mLookup, "evalRow", mMethodType);

            RowFilter filter;
            if (mFilterRef == null) {
                filter = TrueFilter.THE;
            } else {
                filter = mFilterRef.get();
                if (filter == null) {
                    filter = StoredTable.parseFilter(mRowType, mFilterStr);
                    mFilterRef = new WeakReference<>(filter);
                }
            }

            RowStore store = mStoreRef.get();
            if (store == null) {
                mm.new_(DatabaseException.class, "Closed").throw_();
                return mm.finish();
            }

            RowInfo rowInfo;
            MethodHandle decoder, valueDecoder;

            try {
                rowInfo = store.rowInfo(mRowType, mIndexId, schemaVersion);

                if (mProjectionSpec == null) {
                    // Obtain the MethodHandle which fully decodes the value columns.
                    decoder = null;
                    valueDecoder = (MethodHandle) mLookup.findStatic
                        (mLookup.lookupClass(), "decodeValueHandle",
                         MethodType.methodType(MethodHandle.class, int.class))
                        .invokeExact(schemaVersion);
                } else {
                    // Obtain the MethodHandle which decodes the key and value columns.
                    StoredTable<?> table = store.findTable(mIndexId, mRowType);
                    decoder = table.decodePartialHandle(mProjectionSpec, schemaVersion);
                    valueDecoder = null;
                }
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
            visitor.finishDecode(decoder, valueDecoder, mTableClass, rowClass, rowVar);

            return mm.finish();
        }
    }

    private static String toString(RowFilter filter) {
        return doesFilter(filter) ? filter.toString() : null;
    }

    /**
     * @param options bit 1: reverse, bit 2: joined, bit 3: load one
     */
    public static QueryPlan condyPlan(MethodHandles.Lookup lookup, String name, Class type,
                                      Class rowType, byte[] secondaryDesc, int options,
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
            rowInfo = RowStore.secondaryRowInfo(primaryRowInfo, secondaryDesc);
            which = rowInfo.isAltKey() ? "alternate key" : "secondary index";
        }

        QueryPlan plan;
        boolean reverse = (options & 0b001) != 0;

        if ((options & 0b100) != 0) {
            plan = new QueryPlan.LoadOne(rowInfo.name, which, rowInfo.keySpec(), lowBoundStr);
        } else if (lowBoundStr == null && highBoundStr == null) {
            plan = new QueryPlan.FullScan(rowInfo.name, which, rowInfo.keySpec(), reverse);
        } else {
            plan = new QueryPlan.RangeScan(rowInfo.name, which, rowInfo.keySpec(), reverse,
                                           lowBoundStr, highBoundStr);
        }
    
        if (filterStr != null) {
            plan = new QueryPlan.Filter(filterStr, plan);
        }

        if ((options & 0b010) != 0) {
            rowInfo = primaryRowInfo;
            plan = new QueryPlan.PrimaryJoin(rowInfo.name, rowInfo.keySpec(), plan);

            if (joinFilterStr != null) {
                plan = new QueryPlan.Filter(joinFilterStr, plan);
            }
        }

        return plan;
    }

    /**
     * Override the inherited writeRow method, but only if given a projection.
     *
     * @see RowEvaluator#writeRow
     */
    private void addWriteRowMethod() {
        if (mProjectionSpec == null) {
            return;
        }

        // TODO: Try to use condy and define this code lazily.

        MethodMaker mm = mFilterMaker.addMethod
            (null, "writeRow", RowWriter.class, byte[].class, byte[].class)
            .public_().override();

        var writerVar = mm.param(0);
        var keyVar = mm.param(1);
        var valueVar = mm.param(2);

        StoredTable<R> table = mTable;

        if (mAlwaysJoin) {
            table = table.joinedPrimaryTable();
        }

        var mh = table.writeRowHandle(mProjectionSpec);

        if (mh.type().parameterType(0) == int.class) {
            var schemaVersion = mm.var(RowUtils.class).invoke("decodeSchemaVersion", valueVar);
            mm.invoke(mh, schemaVersion, writerVar, keyVar, valueVar);
        } else {
            mm.invoke(mh, writerVar, keyVar, valueVar);
        }
    }
}
