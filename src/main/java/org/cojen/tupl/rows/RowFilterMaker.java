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
import java.lang.invoke.VarHandle;

import java.lang.ref.WeakReference;

import java.util.Collections;
import java.util.HashSet;
import java.util.TreeMap;

import java.util.function.IntFunction;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.DatabaseException;

import org.cojen.tupl.filter.AndFilter;
import org.cojen.tupl.filter.ColumnToArgFilter;
import org.cojen.tupl.filter.ColumnToColumnFilter;
import org.cojen.tupl.filter.OrFilter;
import org.cojen.tupl.filter.RowFilter;
import org.cojen.tupl.filter.Visitor;

import org.cojen.tupl.io.Utils;

/**
 * Makes RowDecoderEncoder classes which are instantiated by a factory.
 *
 * @author Brian S O'Neill
 */
public class RowFilterMaker<R> {
    private static long cFilterNum;
    private static final VarHandle cFilterNumHandle;

    static {
        try {
            cFilterNumHandle =
                MethodHandles.lookup().findStaticVarHandle
                (RowFilterMaker.class, "cFilterNum", long.class);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    private final WeakReference<RowStore> mStoreRef;
    private final Class<?> mTableClass;
    private final Class<R> mRowType;
    private final RowGen mRowGen;
    private final boolean mIsPrimaryTable;
    private final long mIndexId;
    private final String mFilterStr;
    private final RowFilter mFilter;
    private final ClassMaker mFilterMaker;
    private final MethodMaker mFilterCtorMaker;

    // Bound to mFilterCtorMaker.
    private final ColumnCodec[] mKeyCodecs, mValueCodecs;

    /**
     * @param storeRef is passed along to the generated code
     * @param base defines the encode methods; the decode method will be overridden
     * @param secondaryDesc pass null for primary table
     */
    public RowFilterMaker(WeakReference<RowStore> storeRef, Class<?> tableClass,
                          Class<? extends RowDecoderEncoder<R>> base,
                          Class<R> rowType, byte[] secondaryDesc,
                          long indexId, String filterStr, RowFilter filter)
    {
        mStoreRef = storeRef;
        mTableClass = tableClass;
        mRowType = rowType;

        RowInfo rowInfo = RowInfo.find(rowType);
        if (secondaryDesc == null) {
            mRowGen = rowInfo.rowGen();
            mIsPrimaryTable = true;
        } else {
            mRowGen = RowStore.indexRowInfo(rowInfo, secondaryDesc).rowGen();
            mIsPrimaryTable = false;
        }

        mIndexId = indexId;
        mFilterStr = filterStr;
        mFilter = filter;

        // Generate a sub-package with an increasing number to facilitate unloading.
        long filterNum = (long) cFilterNumHandle.getAndAdd(1L);
        mFilterMaker = mRowGen.beginClassMaker(getClass(), rowType, "f" + filterNum, "Filter")
            .final_().extend(base).implement(RowDecoderEncoder.class);

        mFilterCtorMaker = mFilterMaker.addConstructor(Object[].class).varargs().private_();
        mFilterCtorMaker.invokeSuperConstructor();

        mKeyCodecs = ColumnCodec.bind(mRowGen.keyCodecs(), mFilterCtorMaker);
        mValueCodecs = ColumnCodec.bind(mRowGen.valueCodecs(), mFilterCtorMaker);
    }

    /**
     * Returns a factory method.
     */
    public MethodHandle finish() {
        // Finish the filter class...

        // Define the fields to hold the filter arguments.
        mFilter.accept(new Visitor() {
            private final HashSet<Integer> mAdded = new HashSet<>();

            @Override
            public void visit(ColumnToArgFilter filter) {
                int argNum = filter.argument();
                if (mAdded.add(argNum)) {
                    int colNum = columnNumberFor(filter.column().name);
                    boolean in = filter.isIn(filter.operator());
                    Variable argVar = mFilterCtorMaker.param(0).aget(argNum);
                    codecFor(colNum).filterPrepare(in, argVar, argNum);
                }
            }
        });

        addDecodeRowMethod();

        // Provide access to the inherited markAllClean method.
        {
            Class<?> rowClass = RowMaker.find(mRowType);
            MethodMaker mm = mFilterMaker.addMethod(null, "markAllClean", rowClass).static_();
            mm.super_().invoke("markAllClean", mm.param(0));
        }

        // Factory instances are weakly cached by AbstractTable, and so this can cause the
        // generated classes to get lost. They will eventually be GC'd, but this takes longer.
        // To prevent a pile up of duplicate classes, maintain a strong reference to the
        // factory. As long as the filter class exists, the factory exists, and so the cache
        // entry exists.
        mFilterMaker.addField(Object.class, "factory").private_().static_();

        MethodHandles.Lookup filterLookup = mFilterMaker.finishLookup();
        Class<?> filterClass = filterLookup.lookupClass();

        MethodHandle factory;
        try {
            var mt = MethodType.methodType(RowDecoderEncoder.class, Object[].class);
            factory = filterLookup.findConstructor(filterClass, mt.changeReturnType(void.class));
            factory = factory.asType(mt);

            filterLookup.findStaticVarHandle(filterClass, "factory", Object.class).set(factory);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }

        return factory;
    }

    private Integer columnNumberFor(String colName) {
        return mRowGen.columnNumbers().get(colName);
    }

    private ColumnCodec codecFor(int colNum) {
        ColumnCodec[] codecs = mKeyCodecs;
        return colNum < codecs.length ? codecs[colNum] : mValueCodecs[colNum - codecs.length];
    }

    private void addDecodeRowMethod() {
        // Specified by RowDecoderEncoder.
        MethodMaker mm = mFilterMaker.addMethod
            (Object.class, "decodeRow", byte[].class, byte[].class, Object.class).public_();

        if (mIsPrimaryTable) {
            // The decode method is implemented using indy, to support multiple schema versions.

            var indy = mm.var(RowFilterMaker.class).indy
                ("indyDecodeRow", mStoreRef, mTableClass, mRowType, mIndexId, mFilterStr, mFilter);

            var valueVar = mm.param(1);
            var schemaVersion = TableMaker.decodeSchemaVersion(mm, valueVar);

            mm.return_(indy.invoke(Object.class, "decodeRow", null,
                                   schemaVersion, mm.param(0), valueVar, mm.param(2), mm.this_()));
        } else {
            // Decoding secondary index row is simpler because it has no schema version.
            Class<?> rowClass = RowMaker.find(mRowType);
            var visitor = new DecodeVisitor
                (mm, 0, mTableClass, rowClass, mRowGen, null, mm.this_());
            mFilter.accept(visitor);
            visitor.done();
        }
    }

    public static CallSite indyDecodeRow(MethodHandles.Lookup lookup, String name, MethodType mt,
                                         WeakReference<RowStore> storeRef,
                                         Class<?> tableClass, Class<?> rowType, long indexId,
                                         String filterStr, RowFilter filter)
    {
        var dm = new DecodeMaker
            (lookup, mt, storeRef, tableClass, rowType, indexId, filterStr, filter);
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

        // The DecodeMaker isn't defined as a lambda function because this field cannot be final.
        private WeakReference<RowFilter> mFilterRef;

        DecodeMaker(MethodHandles.Lookup lookup, MethodType mt,
                    WeakReference<RowStore> storeRef, Class<?> tableClass,
                    Class<?> rowType, long indexId,
                    String filterStr, RowFilter filter)
        {
            mLookup = lookup;
            mMethodType = mt.dropParameterTypes(0, 1);
            mStoreRef = storeRef;
            mTableClass = tableClass;
            mRowType = rowType;
            mIndexId = indexId;
            mFilterStr = filterStr;
            mFilterRef = new WeakReference<>(filter);
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
                (mm, valueOffset, mTableClass, rowClass, rowGen, decoder, null);
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
        private final int mValueOffset;
        private final Class<?> mTableClass;
        private final Class<?> mRowClass;
        private final RowGen mRowGen;
        private final MethodHandle mDecoder;
        private final Variable mDecoderVar;

        private final ColumnCodec[] mKeyCodecs, mValueCodecs;

        private Label mPass, mFail;

        private LocatedColumn[] mLocatedKeys;
        private int mHighestLocatedKey;

        private LocatedColumn[] mLocatedValues;
        private int mHighestLocatedValue;

        /**
         * Supports two forms of methods:
         *
         *     R decodeRow(byte[] key, byte[] value, R row, decoder/filter)
         *     R decodeRow(byte[] key, byte[] value, R row)
         *
         * When using the first form, a decoder MethodHandle must be provided. When using the
         * second form, a decoderVar msut be provided.

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
                      MethodHandle decoder, Variable decoderVar)
        {
            mMaker = mm;
            mValueOffset = valueOffset;
            mTableClass = tableClass;
            mRowClass = rowClass;
            mRowGen = rowGen;
            mDecoder = decoder;

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
            var valueVar = mMaker.param(1); // param(1) is the value byte array
            if (mDecoder != null) {
                mMaker.invoke(mDecoder, rowVar, valueVar);
            } else {
                mMaker.var(mTableClass).invoke("decodeValue", rowVar, valueVar);
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
            ColumnInfo colInfo = filter.column();
            int op = filter.operator();
            Variable argObjVar = mDecoderVar; // contains the arg fields prepared earlier
            int argNum = filter.argument();

            Integer colNum = columnNumberFor(colInfo.name);

            if (colNum != null) {
                ColumnCodec codec = codecFor(colNum);
                LocatedColumn located = decodeColumn(colNum, colInfo, true);
                Object decoded = located.mDecodedQuick;
                if (decoded != null) {
                    codec.filterQuickCompare(colInfo, located.mSrcVar, located.mOffsetVar, op,
                                             decoded, argObjVar, argNum, mPass, mFail);
                } else {
                    var argField = argObjVar.field(ColumnCodec.argFieldName(colInfo, argNum));
                    CompareUtils.compare(mMaker, colInfo, located.mDecodedVar,
                                         colInfo, argField, op, mPass, mFail);
                }
            } else {
                // Column doesn't exist in the row, so compare against a default. This code
                // assumes that value codecs always define an arg field which preserves the
                // original argument, possibly converted to the correct type.
                var argField = argObjVar.field(ColumnCodec.argFieldName(colInfo, argNum));
                var columnVar = mMaker.var(colInfo.type);
                Converter.setDefault(mMaker, colInfo, columnVar);
                CompareUtils.compare(mMaker, colInfo, columnVar,
                                     colInfo, argField, op, mPass, mFail);
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
                    srcVar = mMaker.param(1);
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

            if (colNum <= highestNum) {
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
