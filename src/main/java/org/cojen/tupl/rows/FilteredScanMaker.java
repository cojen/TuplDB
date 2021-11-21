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

import java.util.Collections;
import java.util.TreeMap;

import java.util.function.IntFunction;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Field;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.DatabaseException;

import org.cojen.tupl.core.RowPredicate;

import org.cojen.tupl.filter.AndFilter;
import org.cojen.tupl.filter.ColumnToArgFilter;
import org.cojen.tupl.filter.ColumnToColumnFilter;
import org.cojen.tupl.filter.OrFilter;
import org.cojen.tupl.filter.RowFilter;
import org.cojen.tupl.filter.Visitor;

import org.cojen.tupl.io.Utils;

/**
 * Makes ScanControllerFactory classes which perform basic filtering.
 *
 * @author Brian S O'Neill
 */
public class FilteredScanMaker<R> {
    private final WeakReference<RowStore> mStoreRef;
    private final Class<?> mTableClass;
    private final Class<?> mPredicateClass;
    private final Class<R> mRowType;
    private final RowGen mRowGen;
    private final boolean mIsPrimaryTable;
    private final long mIndexId;
    private final RowFilter mFilter, mLowBound, mHighBound;
    private final String mFilterStr;
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
     * @param unfiltered defines the encode methods; the decode method will be overridden
     * @param predClass contains references to the argument fields
     * @param filter the filter to apply to all rows which are in bounds, or null if none
     * @param filterStr the canonical string for the filter param, or null if none
     * @param lowBound pass null for open bound
     * @param highBound pass null for open bound
     */
    public FilteredScanMaker(WeakReference<RowStore> storeRef, Class<?> tableClass,
                             Class<? extends SingleScanController<R>> unfiltered,
                             Class<? extends RowPredicate> predClass,
                             Class<R> rowType, RowInfo rowInfo, long indexId,
                             RowFilter filter, String filterStr,
                             RowFilter lowBound, RowFilter highBound)
    {
        mStoreRef = storeRef;
        mTableClass = tableClass;
        mPredicateClass = predClass;
        mRowType = rowType;

        mRowGen = rowInfo.rowGen();
        mIsPrimaryTable = RowInfo.find(rowType) == rowInfo;

        mIndexId = indexId;
        mFilter = filter;
        mLowBound = lowBound;
        mHighBound = highBound;
        mFilterStr = filterStr;

        // Define in the same package as the predicate class, in order to access it, and to
        // facilitate class unloading.
        mFilterMaker = mRowGen.anotherClassMaker(getClass(), predClass, "Filter")
            .final_().extend(unfiltered).implement(ScanControllerFactory.class);

        mFilterMaker.addField(predClass, "predicate").private_().final_();

        mFilterCtorMaker = mFilterMaker.addConstructor(predClass).private_();
        mFilterCtorMaker.field("predicate").set(mFilterCtorMaker.param(0));

        // Need a constructor for the factory singleton instance.
        mFilterMaker.addConstructor().private_().invokeSuperConstructor(null, false, null, false);
    }

    public ScanControllerFactory<R> finish() {
        // Finish the filter class...

        var ctorParams = new Object[] {null, false, null, false};

        if (mLowBound != null || mHighBound != null) {
            if (mLowBound != null) {
                encodeBound(ctorParams, mLowBound, true);
            }
            if (mHighBound != null) {
                encodeBound(ctorParams, mHighBound, false);
            }
        }

        mFilterCtorMaker.invokeSuperConstructor(ctorParams);

        addDecodeRowMethod();

        // Override and return the predicate object.
        {
            MethodMaker mm = mFilterMaker.addMethod(RowPredicate.class, "predicate").public_();
            mm.return_(mm.field("predicate"));
        }

        // Provide access to the inherited markAllClean method.
        {
            Class<?> rowClass = RowMaker.find(mRowType);
            MethodMaker mm = mFilterMaker.addMethod(null, "markAllClean", rowClass).static_();
            mm.super_().invoke("markAllClean", mm.param(0));
        }

        // Define a singleton instance which serves as the factory.
        {
            mFilterMaker.addField(mFilterMaker, "factory").private_().static_().final_();
            MethodMaker mm = mFilterMaker.addClinit();
            mm.field("factory").set(mm.new_(mFilterMaker));
        }

        // Define the factory methods.
        {
            MethodMaker mm = mFilterMaker.addMethod
                (ScanController.class, "newScanController", Object[].class).public_().varargs();
            mm.return_(mm.new_(mFilterMaker, mm.new_(mPredicateClass, mm.param(0))));

            mm = mFilterMaker.addMethod
                (ScanController.class, "newScanController", RowPredicate.class).public_();
            mm.return_(mm.new_(mFilterMaker, mm.param(0).cast(mPredicateClass)));
        }

        MethodHandles.Lookup filterLookup = mFilterMaker.finishLookup();
        Class<?> filterClass = filterLookup.lookupClass();

        try {
            var vh = filterLookup.findStaticVarHandle(filterClass, "factory", filterClass);
            return (ScanControllerFactory<R>) vh.get();
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

    private void addDecodeRowMethod() {
        if (mFilter == null) {
            // No remainder filter, so rely on inherited method.
            return;
        }

        // Implement/override method as specified by RowDecoderEncoder.

        Object[] params;
        if (mStopColumn == null) {
            params = new Object[] {byte[].class, byte[].class, Object.class};
        } else {
            params = new Object[] {byte[].class, Cursor.class, Object.class};
        }

        MethodMaker mm = mFilterMaker.addMethod(Object.class, "decodeRow", params).public_();

        if (mIsPrimaryTable) {
            // The decode method is implemented using indy, to support multiple schema versions.

            var indy = mm.var(FilteredScanMaker.class).indy
                ("indyDecodeRow", mStoreRef, mTableClass, mRowType, mIndexId,
                 new WeakReference<>(mFilter), mFilterStr, mStopColumn, mStopArgument);

            var valueVar = mm.param(1);

            if (valueVar.classType() == Cursor.class) {
                valueVar = valueVar.invoke("value");
            }

            var schemaVersion = TableMaker.decodeSchemaVersion(mm, valueVar);

            mm.return_(indy.invoke(Object.class, "decodeRow", null, schemaVersion,
                                   mm.param(0), mm.param(1), mm.param(2), mm.this_()));
        } else {
            // Decoding a secondary index row is simpler because it has no schema version.
            Class<?> rowClass = RowMaker.find(mRowType);
            var visitor = new DecodeVisitor
                (mm, 0, mTableClass, rowClass, mRowGen, null, mm.this_(), // decoderVar
                 mStopColumn, mStopArgument);
            mFilter.accept(visitor);
            visitor.done();
        }
    }

    public static CallSite indyDecodeRow(MethodHandles.Lookup lookup, String name, MethodType mt,
                                         WeakReference<RowStore> storeRef,
                                         Class<?> tableClass, Class<?> rowType, long indexId,
                                         WeakReference<RowFilter> filterRef, String filterStr,
                                         String stopColumn, int stopArgument)
    {
        var dm = new DecodeMaker
            (lookup, mt, storeRef, tableClass, rowType, indexId,
             filterRef, filterStr, stopColumn, stopArgument);
        return new SwitchCallSite(lookup, mt, dm);
    }

    private static class DecodeMaker implements IntFunction<Object> {
        private final MethodHandles.Lookup mLookup;
        private final MethodType mMethodType;
        private final WeakReference<RowStore> mStoreRef;
        private final Class<?> mTableClass;
        private final Class<?> mRowType;
        private final long mIndexId;
        private final String mFilterStr;
        private final String mStopColumn;
        private final int mStopArgument;

        // The DecodeMaker isn't defined as a lambda function because this field cannot be final.
        private WeakReference<RowFilter> mFilterRef;

        DecodeMaker(MethodHandles.Lookup lookup, MethodType mt,
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

            RowFilter filter = mFilterRef.get();
            if (filter == null) {
                filter = AbstractTable.parse(mRowType, mFilterStr);
                mFilterRef = new WeakReference<>(filter);
            }

            RowStore store = mStoreRef.get();
            if (store == null) {
                mm.new_(DatabaseException.class, "Closed").throw_();
                return mm.finish();
            }

            RowInfo rowInfo;
            MethodHandle decoder;

            try {
                if (schemaVersion != 0) {
                    rowInfo = store.rowInfo(mRowType, mIndexId, schemaVersion);
                } else {
                    // No value columns to decode, and the primary key cannot change.
                    RowInfo dstRowInfo = RowInfo.find(mRowType);
                    rowInfo = new RowInfo(dstRowInfo.name);
                    rowInfo.keyColumns = dstRowInfo.keyColumns;
                    rowInfo.valueColumns = Collections.emptyNavigableMap();
                    rowInfo.allColumns = new TreeMap<>(rowInfo.keyColumns);
                }

                // Obtain the MethodHandle which fully decodes the value columns.
                decoder = (MethodHandle) mLookup.findStatic
                    (mLookup.lookupClass(), "decodeValueHandle",
                     MethodType.methodType(MethodHandle.class, int.class))
                    .invokeExact(schemaVersion);
            } catch (Throwable e) {
                return new ExceptionCallSite.Failed(mMethodType, mm, e);
            }

            Class<?> rowClass = RowMaker.find(mRowType);
            RowGen rowGen = rowInfo.rowGen();

            int valueOffset = RowUtils.lengthPrefixPF(schemaVersion);

            var visitor = new DecodeVisitor
                (mm, valueOffset, mTableClass, rowClass, rowGen, decoder, null,
                 mStopColumn, mStopArgument);
            filter.accept(visitor);
            visitor.done();

            return mm.finish();
        }
    }

    /**
     * Generates code to filter and decode rows for a specific schema version.
     */
    private static class DecodeVisitor extends Visitor {
        private final MethodMaker mMaker;
        private final Variable mValueVar;
        private final int mValueOffset;
        private final Class<?> mTableClass;
        private final Class<?> mRowClass;
        private final RowGen mRowGen;
        private final MethodHandle mDecoder;
        private final Variable mDecoderVar;
        private final String mStopColumn;
        private final int mStopArgument;

        private final ColumnCodec[] mKeyCodecs, mValueCodecs;

        private Label mPass, mFail;

        private LocatedColumn[] mLocatedKeys;
        private int mHighestLocatedKey;

        private LocatedColumn[] mLocatedValues;
        private int mHighestLocatedValue;

        /**
         * Supports four forms of methods:
         *
         *     R decodeRow(byte[] key, byte[] value, R row, decoder/filter)
         *     R decodeRow(byte[] key, Cursor c, R row, decoder/filter)
         *     R decodeRow(byte[] key, byte[] value, R row)
         *     R decodeRow(byte[] key, Cursor c, R row)
         *
         * When using the first two forms, a decoder MethodHandle must be provided. When using
         * the last two forms, a decoderVar must be provided.
         *
         * If a stopColumn and stopArgument are provided, then the cursor method form is
         * required in order for the stop to actually work.
         *
         * @param mm signature: R decodeRow(byte[] key, byte[] value, R row [, decoder/filter])
         * @param valueOffset offset to skip past the schema version
         * @param tableClass current table implementation class
         * @param rowClass current row implementation
         * @param rowGen actual row definition to be decoded
         * @param decoder performs full decoding of the value columns
         * @param decoderVar the actual decoder/filter instance
         */
        DecodeVisitor(MethodMaker mm, int valueOffset,
                      Class<?> tableClass, Class<?> rowClass, RowGen rowGen,
                      MethodHandle decoder, Variable decoderVar,
                      String stopColumn, int stopArgument)
        {
            mMaker = mm;
            mValueOffset = valueOffset;
            mTableClass = tableClass;
            mRowClass = rowClass;
            mRowGen = rowGen;
            mDecoder = decoder;
            mStopColumn = stopColumn;
            mStopArgument = stopArgument;

            var valueVar = mm.param(1);
            if (valueVar.classType() == Cursor.class) {
                valueVar = valueVar.invoke("value");
            }
            mValueVar = valueVar;

            if (decoderVar == null) {
                if (decoder == null) {
                    throw new IllegalArgumentException();
                }
                mDecoderVar = mm.param(3);
            } else {
                if (decoder != null) {
                    throw new IllegalArgumentException();
                }
                mDecoderVar = decoderVar;
            }

            mKeyCodecs = ColumnCodec.bind(rowGen.keyCodecs(), mm);
            mValueCodecs = ColumnCodec.bind(rowGen.valueCodecs(), mm);

            mPass = mm.label();
            mFail = mm.label();
        }

        void done() {
            mFail.here();
            mMaker.return_(null);

            mPass.here();

            // FIXME: Some columns may have already been decoded, so don't double decode them.

            var tableVar = mMaker.var(mTableClass);
            var rowVar = mMaker.param(2).cast(mRowClass);
            Label hasRow = mMaker.label();
            rowVar.ifNe(null, hasRow);
            rowVar.set(mMaker.new_(mRowClass));
            hasRow.here();
            tableVar.invoke("decodePrimaryKey", rowVar, mMaker.param(0));

            // Invoke the schema-specific decoder directly, instead of calling the decodeValue
            // method which redundantly examines the schema version and switches on it.
            if (mDecoder != null) {
                mMaker.invoke(mDecoder, rowVar, mValueVar);
            } else {
                mMaker.var(mTableClass).invoke("decodeValue", rowVar, mValueVar);
            }

            // Call the generated filter class, which has access to the inherited markAllClean
            // method.
            mDecoderVar.invoke("markAllClean", rowVar);

            mMaker.return_(rowVar);
        }

        @Override
        public void visit(OrFilter filter) {
            final Label originalFail = mFail;

            RowFilter[] subFilters = filter.subFilters();

            if (subFilters.length == 0) {
                mMaker.goto_(originalFail);
                return;
            }

            mFail = mMaker.label();
            subFilters[0].accept(this);
            mFail.here();

            // Only the state observed on the left tree path can be preserved, because it's
            // guaranteed to have executed.
            final int hk = mHighestLocatedKey;
            final int hv = mHighestLocatedValue;

            for (int i=1; i<subFilters.length; i++) {
                mFail = mMaker.label();
                subFilters[i].accept(this);
                mFail.here();
            }

            resetHighestLocatedKey(hk);
            resetHighestLocatedValue(hv);

            mMaker.goto_(originalFail);
            mFail = originalFail;
        }

        @Override
        public void visit(AndFilter filter) {
            final Label originalPass = mPass;

            RowFilter[] subFilters = filter.subFilters();

            if (subFilters.length == 0) {
                mMaker.goto_(originalPass);
                return;
            }

            mPass = mMaker.label();
            subFilters[0].accept(this);
            mPass.here();

            // Only the state observed on the left tree path can be preserved, because it's
            // guaranteed to have executed.
            final int hk = mHighestLocatedKey;
            final int hv = mHighestLocatedValue;

            for (int i=1; i<subFilters.length; i++) {
                mPass = mMaker.label();
                subFilters[i].accept(this);
                mPass.here();
            }

            resetHighestLocatedKey(hk);
            resetHighestLocatedValue(hv);

            mMaker.goto_(originalPass);
            mPass = originalPass;
        }

        @Override
        public void visit(ColumnToArgFilter filter) {
            final Label originalFail = mFail;

            ColumnInfo colInfo = filter.column();
            int op = filter.operator();
            Variable predVar = mDecoderVar.field("predicate");
            int argNum = filter.argument();

            Integer colNum = columnNumberFor(colInfo.name);

            Label stop = null;
            if (argNum == mStopArgument && colInfo.name.equals(mStopColumn)
                && mMaker.param(1).classType() == Cursor.class)
            {
                stop = mMaker.label();
                mFail = stop;
            }

            if (colNum != null) {
                ColumnCodec codec = codecFor(colNum);
                LocatedColumn located = decodeColumn(colNum, colInfo, true);
                Object decoded = located.mDecodedQuick;
                if (decoded != null) {
                    codec.filterQuickCompare(colInfo, located.mSrcVar, located.mOffsetVar, op,
                                             decoded, predVar, argNum, mPass, mFail);
                } else {
                    var argField = predVar.field(ColumnCodec.argFieldName(colInfo, argNum));
                    CompareUtils.compare(mMaker, colInfo, located.mDecodedVar,
                                         colInfo, argField, op, mPass, mFail);
                }
            } else {
                // Column doesn't exist in the row, so compare against a default. This code
                // assumes that value codecs always define an arg field which preserves the
                // original argument, possibly converted to the correct type.
                var argField = predVar.field(ColumnCodec.argFieldName(colInfo, argNum));
                var columnVar = mMaker.var(colInfo.type);
                Converter.setDefault(mMaker, colInfo, columnVar);
                CompareUtils.compare(mMaker, colInfo, columnVar,
                                     colInfo, argField, op, mPass, mFail);
            }

            if (stop != null) {
                stop.here();
                // Reset the cursor to stop scanning.
                mMaker.param(1).invoke("reset");
                mMaker.var(StoppedCursorException.class).field("THE").throw_();
                //mMaker.goto_(originalFail);
                mFail = originalFail;
            }
        }

        @Override
        public void visit(ColumnToColumnFilter filter) {
            ColumnInfo aColInfo = filter.column();
            int op = filter.operator();
            ColumnInfo bColInfo = filter.otherColumn();

            Integer aColNum = columnNumberFor(aColInfo.name);
            Integer bColNum = columnNumberFor(bColInfo.name);

            if (aColNum == null && bColNum == null) {
                // Comparing two columns that don't exist. If this filter is part of a chain,
                // the rest will be dead code.
                mMaker.goto_(CompareUtils.selectNullColumnToNullArg(op, mPass, mFail));
                return;
            }

            Variable aVar = decodeColumnOrDefault(aColNum, aColInfo);
            Variable bVar = decodeColumnOrDefault(bColNum, bColInfo);

            if (aVar.classType() != bVar.classType()) {
                ColumnInfo cColInfo = filter.common();

                var aConvertedVar = mMaker.var(cColInfo.type);
                Converter.convertLossy(mMaker, aColInfo, aVar, cColInfo, aConvertedVar);
                aColInfo = cColInfo;
                aVar = aConvertedVar;

                var bConvertedVar = mMaker.var(cColInfo.type);
                Converter.convertLossy(mMaker, bColInfo, bVar, cColInfo, bConvertedVar);
                bColInfo = cColInfo;
                bVar = bConvertedVar;
            }

            CompareUtils.compare(mMaker, aColInfo, aVar, bColInfo, bVar, op, mPass, mFail);
        }

        private Variable decodeColumnOrDefault(Integer colNum, ColumnInfo colInfo) {
            if (colNum != null) {
                return decodeColumn(colNum, colInfo, false).mDecodedVar;
            } else {
                var colVar = mMaker.var(colInfo.type);
                Converter.setDefault(mMaker, colInfo, colVar);
                return colVar;
            }
        }

        private Integer columnNumberFor(String colName) {
            return mRowGen.columnNumbers().get(colName);
        }

        private ColumnCodec codecFor(int colNum) {
            ColumnCodec[] codecs = mKeyCodecs;
            return colNum < codecs.length ? codecs[colNum] : mValueCodecs[colNum - codecs.length];
        }

        /**
         * Decodes a column and remembers it if requested again later.
         *
         * @param colInfo current definition for column
         * @param quick allow quick decode
         */
        private LocatedColumn decodeColumn(int colNum, ColumnInfo colInfo, boolean quick) {
            Variable srcVar;
            LocatedColumn[] located;
            ColumnCodec[] codecs = mKeyCodecs;
            int highestNum;

            init: {
                int startOffset;
                if (colNum < codecs.length) {
                    // Key column.
                    highestNum = mHighestLocatedKey;
                    srcVar = mMaker.param(0);
                    if ((located = mLocatedKeys) != null) {
                        break init;
                    }
                    mLocatedKeys = located = new LocatedColumn[mRowGen.info.keyColumns.size()];
                    startOffset = 0;
                } else {
                    // Value column.
                    colNum -= codecs.length;
                    highestNum = mHighestLocatedValue;
                    srcVar = mValueVar;
                    codecs = mValueCodecs;
                    if ((located = mLocatedValues) != null) {
                        break init;
                    }
                    mLocatedValues = located = new LocatedColumn[mRowGen.info.valueColumns.size()];
                    startOffset = mValueOffset;
                }
                located[0] = new LocatedColumn();
                located[0].located(srcVar, mMaker.var(int.class).set(startOffset));
            }

            if (colNum < highestNum) {
                LocatedColumn col = located[colNum];
                if (col.isDecoded(quick)) {
                    return col;
                }
                // Regress the highest to force the column to be decoded. The highest field
                // won't regress, since the field assignment (at the end) checks this.
                highestNum = colNum;
            }

            if (!located[highestNum].isLocated()) {
                throw new AssertionError();
            }

            for (; highestNum <= colNum; highestNum++) {
                // Offset will be mutated, and so a copy must be made before calling decode.
                Variable offsetVar = located[highestNum].mOffsetVar;

                LocatedColumn next;
                copyOffsetVar: {
                    if (highestNum + 1 >= located.length) {
                        next = null;
                    } else {
                        next = located[highestNum + 1];
                        if (next == null) {
                            next = new LocatedColumn();
                            located[highestNum + 1] = next;
                        } else if (!next.isLocated()) {
                            // Can recycle the offset variable because it's not used.
                            Variable freeVar = next.mOffsetVar;
                            if (freeVar != null) {
                                freeVar.set(offsetVar);
                                offsetVar = freeVar;
                                break copyOffsetVar;
                            }
                        }
                    }
                    offsetVar = offsetVar.get();
                }

                ColumnCodec codec = codecs[highestNum];
                Variable endVar = null;
                if (highestNum < colNum) {
                    codec.decodeSkip(srcVar, offsetVar, endVar);
                } else if (quick && codec.canFilterQuick(colInfo)) {
                    Object decoded = codec.filterQuickDecode(colInfo, srcVar, offsetVar, endVar);
                    located[highestNum].decodedQuick(decoded);
                } else {
                    Variable dstVar = mMaker.var(colInfo.type);
                    Converter.decode(mMaker, srcVar, offsetVar, endVar, codec, colInfo, dstVar);
                    located[highestNum].decodedVar(dstVar);
                }

                if (next != null && !next.isLocated()) {
                    // The decode call incremented offsetVar as a side-effect. Note that if the
                    // column is already located, then newly discovered offset will match. It
                    // can simply be replaced, but by discarding it, the compiler can discard
                    // some of the redundant steps which computed the offset again.
                    next.located(srcVar, offsetVar);
                }
            }

            highestNum = Math.min(highestNum, located.length - 1);

            if (located == mLocatedKeys) {
                if (highestNum > mHighestLocatedKey) {
                    mHighestLocatedKey = highestNum;
                }
            } else {
                if (highestNum > mHighestLocatedValue) {
                    mHighestLocatedValue = highestNum;
                }
            }

            return located[colNum];
        }

        /**
         * Reset the highest located key column. The trailing LocatedColumn instances can be
         * re-used, reducing the number of Variables created.
         */
        private void resetHighestLocatedKey(int colNum) {
            if (colNum < mHighestLocatedKey) {
                mHighestLocatedKey = colNum;
                finishReset(mLocatedKeys, colNum);
            }
        }

        /**
         * Reset the highest located value column. The trailing LocatedColumn instances can be
         * re-used, reducing the number of Variables created.
         *
         * @param colNum column number among all value columns
         */
        private void resetHighestLocatedValue(int colNum) {
            if (colNum < mHighestLocatedValue) {
                mHighestLocatedValue = colNum;
                finishReset(mLocatedValues, colNum);
            }
        }

        private static void finishReset(LocatedColumn[] columns, int colNum) {
            while (++colNum < columns.length) {
                var col = columns[colNum];
                if (col == null) {
                    break;
                }
                col.unlocated();
            }
        }
    }

    private static class LocatedColumn {
        // Used by mState field.
        private static final int UNLOCATED = 0, LOCATED = 1, DECODED = 2;

        private int mState;

        // Source byte array. Is valid when mState is LOCATED or DECODED.
        Variable mSrcVar;

        // Offset into the byte array. Is valid when mState is LOCATED or DECODED.
        Variable mOffsetVar;

        // Is only valid when mState is DECODED and canFilterQuick returned true.
        Object mDecodedQuick;

        // Is only valid when mState is DECODED.
        Variable mDecodedVar;

        LocatedColumn() {
        }

        boolean isLocated() {
            return mState >= LOCATED;
        }

        /**
         * @param quick when true, accepts quick or fully decoded forms; when false, only
         * accepts the fully decoded form
         */
        boolean isDecoded(boolean quick) {
            return mState == DECODED && (quick || mDecodedVar != null);
        }

        /**
         * @param srcVar source byte array
         * @param offsetVar start offset into the byte array
         */
        void located(Variable srcVar, Variable offsetVar) {
            mSrcVar = srcVar;
            mOffsetVar = offsetVar;
            mState = LOCATED;
        }

        /**
         * @param decoded object returned from ColumnCodec.filterQuickDecode
         */
        void decodedQuick(Object decoded) {
            if (mState == UNLOCATED) {
                throw new IllegalStateException();
            }
            mDecodedQuick = decoded;
            mState = DECODED;
        }

        void decodedVar(Variable decodedVar) {
            if (mState == UNLOCATED) {
                throw new IllegalStateException();
            }
            mDecodedVar = decodedVar;
            mState = DECODED;
        }

        void unlocated() {
            mDecodedQuick = null;
            mDecodedVar = null;
            mState = UNLOCATED;
        }
    }
}
