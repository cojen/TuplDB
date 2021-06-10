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

import java.util.Arrays;
import java.util.HashSet;

import java.util.function.IntFunction;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Field;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.CorruptDatabaseException;
import org.cojen.tupl.Cursor;
import org.cojen.tupl.DatabaseException;

import org.cojen.tupl.filter.AndFilter;
import org.cojen.tupl.filter.ColumnToArgFilter;
import org.cojen.tupl.filter.ColumnToColumnFilter;
import org.cojen.tupl.filter.InFilter;
import org.cojen.tupl.filter.OrFilter;
import org.cojen.tupl.filter.Parser;
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
    private final Class<?> mViewClass;
    private final Class<R> mRowType;
    private final RowGen mRowGen;
    private final String mFilterStr;
    private final RowFilter mFilter;
    private final ClassMaker mFilterMaker;
    private final MethodMaker mFilterCtorMaker;

    // Bound to mFilterCtorMaker.
    private final ColumnCodec[] mKeyCodecs, mValueCodecs;

    /**
     * @param storeRef is passed along to the generated code
     * @param base defines the encode methods; the decode method will be overridden
     */
    public RowFilterMaker(WeakReference<RowStore> storeRef, Class<?> viewClass,
                          Class<? extends RowDecoderEncoder<R>> base,
                          Class<R> rowType, String filterStr, RowFilter filter)
    {
        mStoreRef = storeRef;
        mViewClass = viewClass;
        mRowType = rowType;
        mRowGen = RowInfo.find(rowType).rowGen();
        mFilterStr = filterStr;
        mFilter = filter;

        // Generate a sub-package with an increasing number to facilitate unloading.
        long filterNum = (long) cFilterNumHandle.getAndAdd(1L);
        mFilterMaker = mRowGen.beginClassMaker(rowType, "f" + filterNum, "Filter")
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
            private HashSet<Integer> mAdded = new HashSet<>();

            @Override
            public void visit(InFilter filter) {
                visit((ColumnToArgFilter) filter);
            }

            @Override
            public void visit(ColumnToArgFilter filter) {
                int argNum = filter.argument();
                if (mAdded.add(argNum)) {
                    int colNum = columnNumberFor(filter.column().name());
                    codecFor(colNum).filterPrepare
                        (filter.operator(), mFilterCtorMaker.param(0).aget(argNum), argNum);
                }
            }
        });

        // The decode method is implemented using indy, to support multiple schema versions.
        {
            // Defined by RowDecoderEncoder.
            MethodMaker mm = mFilterMaker.addMethod
                (Object.class, "decodeRow", byte[].class, byte[].class, Object.class).public_();

            var indy = mm.var(RowFilterMaker.class).indy
                ("indyDecodeRow", mStoreRef, mViewClass, mRowType, mFilterStr, mFilter);

            var valueVar = mm.param(1);
            var schemaVersion = RowViewMaker.decodeSchemaVersion(mm, valueVar);

            mm.return_(indy.invoke(Object.class, "decodeRow", null,
                                   schemaVersion, mm.this_(), mm.param(0), valueVar, mm.param(2)));
        }

        // Provide access to the inherited markAllClean method.
        {
            Class<?> rowClass = RowMaker.find(mRowType);
            MethodMaker mm = mFilterMaker.addMethod(null, "markAllClean", rowClass).static_();
            mm.super_().invoke("markAllClean", mm.param(0));
        }

        // Factory instances are weakly cached by AbstractRowView, and so this can cause the
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

    private int columnNumberFor(String colName) {
        return mRowGen.columnNumbers().get(colName);
    }

    private ColumnCodec codecFor(int colNum) {
        ColumnCodec[] codecs = mKeyCodecs;
        return colNum < codecs.length ? codecs[colNum] : mValueCodecs[colNum - codecs.length];
    }

    public static CallSite indyDecodeRow(MethodHandles.Lookup lookup, String name, MethodType mt,
                                         WeakReference<RowStore> storeRef,
                                         Class<?> viewClass, Class<?> rowType,
                                         String filterStr, RowFilter filter)
    {
        var dm = new DecodeMaker(lookup, mt, storeRef, viewClass, rowType, filterStr, filter);
        return new SwitchCallSite(lookup, mt, dm);
    }

    private static class DecodeMaker implements IntFunction<Object> {
        private final MethodHandles.Lookup mLookup;
        private final MethodType mMethodType;
        private final WeakReference<RowStore> mStoreRef;
        private final Class<?> mViewClass;
        private final Class<?> mRowType;
        private final String mFilterStr;

        // The DecodeMaker isn't defined as a lambda function because this field cannot be final.
        private WeakReference<RowFilter> mFilterRef;

        DecodeMaker(MethodHandles.Lookup lookup, MethodType mt,
                    WeakReference<RowStore> storeRef, Class<?> viewClass, Class<?> rowType,
                    String filterStr, RowFilter filter)
        {
            mLookup = lookup;
            mMethodType = mt.dropParameterTypes(0, 1);
            mStoreRef = storeRef;
            mViewClass = viewClass;
            mRowType = rowType;
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
                filter = AbstractRowView.parse(mRowType, mFilterStr);
                mFilterRef = new WeakReference<>(filter);
            }

            RowStore store = mStoreRef.get();
            if (store == null) {
                mm.new_(DatabaseException.class, "Closed").throw_();
                return mm.finish();
            }

            RowInfo rowInfo, dstRowInfo;
            MethodHandle decoder;

            if (schemaVersion == 0) {
                // No value columns to decode, and the primary key cannot change.
                rowInfo = RowInfo.find(mRowType);
                dstRowInfo = rowInfo;
                decoder = null;
            } else {
                try {
                    rowInfo = store.rowInfo(mRowType, schemaVersion);
                    if (rowInfo == null) {
                        throw new CorruptDatabaseException
                            ("Schema version not found: " + schemaVersion);
                    }

                    dstRowInfo = RowInfo.find(mRowType);

                    // Obtain the MethodHandle which fully decodes the value columns.
                    decoder = (MethodHandle) mLookup.findStatic
                        (mLookup.lookupClass(), "decodeValueHandle",
                         MethodType.methodType(MethodHandle.class, int.class))
                        .invokeExact(schemaVersion);
                } catch (Throwable e) {
                    return new ExceptionCallSite.Failed(mMethodType, mm, e);
                }
            }

            Class<?> rowClass = RowMaker.find(mRowType);
            RowGen rowGen = rowInfo.rowGen();

            var visitor = new DecodeVisitor
                (mm, schemaVersion, mViewClass, dstRowInfo, rowClass, rowGen, decoder);
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
        private final int mSchemaVersion;
        private final Class<?> mViewClass;
        private final RowInfo mDstRowInfo;
        private final Class<?> mRowClass;
        private final RowGen mRowGen;
        private final MethodHandle mDecoder;

        private final ColumnCodec[] mKeyCodecs, mValueCodecs;

        private Label mPass, mFail;

        private LocatedColumn[] mLocatedKeys, mLocatedValues;

        private LocatedColumn mHighestLocated;

        /**
         * @param mm signature: R decodeRow(Decoder/filter, byte[] key, byte[] value, R row)
         * @param viewClass current row view implementation class
         * @param dstRowInfo current row defintion
         * @param rowClass current row implementation
         * @param rowGen actual row defintion to be decoded (can differ from dstRowInfo)
         * @param decoder performs full decoding of the value columns; can be null if none
         */
        DecodeVisitor(MethodMaker mm, int schemaVersion,
                      Class<?> viewClass, RowInfo dstRowInfo, Class<?> rowClass, RowGen rowGen,
                      MethodHandle decoder)
        {
            mMaker = mm;
            mSchemaVersion = schemaVersion;
            mViewClass = viewClass;
            mDstRowInfo = dstRowInfo;
            mRowClass = rowClass;
            mRowGen = rowGen;
            mDecoder = decoder;

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

            var viewVar = mMaker.var(mViewClass);
            var rowVar = mMaker.param(3).cast(mRowClass);
            Label hasRow = mMaker.label();
            rowVar.ifNe(null, hasRow);
            rowVar.set(mMaker.new_(mRowClass));
            hasRow.here();
            viewVar.invoke("decodePrimaryKey", rowVar, mMaker.param(1));

            // Invoke the schema-specific decoder directly, instead of calling the decodeValue
            // method which redundantly examines the schema version and switches on it. The
            // decoder can be null if no value columns exist.
            if (mDecoder != null) {
                mMaker.invoke(mDecoder, rowVar, mMaker.param(2)); // param(2) is the byte array
            }

            // Param(0) is the generated filter class, which has access to the inherited
            // markAllClean method.
            mMaker.param(0).invoke("markAllClean", rowVar);

            mMaker.return_(rowVar);
        }

        @Override
        public void visit(OrFilter filter) {
            final Label originalFail = mFail;

            RowFilter[] subFilters = filter.subFilters();

            mFail = mMaker.label();
            subFilters[0].accept(this);
            mFail.here();

            // Only the state observed on the left tree path can be preserved, because it's
            // guaranteed to have executed.
            final LocatedColumn highestLocated = mHighestLocated;

            for (int i=1; i<subFilters.length; i++) {
                mFail = mMaker.label();
                subFilters[i].accept(this);
                mFail.here();
            }

            highestLocated.resetTail();

            mMaker.goto_(originalFail);
            mFail = originalFail;
        }

        @Override
        public void visit(AndFilter filter) {
            final Label originalPass = mPass;

            RowFilter[] subFilters = filter.subFilters();

            mPass = mMaker.label();
            subFilters[0].accept(this);
            mPass.here();

            // Only the state observed on the left tree path can be preserved, because it's
            // guaranteed to have executed.
            final LocatedColumn highestLocated = mHighestLocated;

            for (int i=1; i<subFilters.length; i++) {
                mPass = mMaker.label();
                subFilters[i].accept(this);
                mPass.here();
            }

            highestLocated.resetTail();

            mMaker.goto_(originalPass);
            mPass = originalPass;
        }

        @Override
        public void visit(InFilter filter) {
            // FIXME: visit(InFilter)
            throw null;
        }

        @Override
        public void visit(ColumnToArgFilter filter) {
            String name = filter.column().name();
            ColumnInfo dstInfo = mDstRowInfo.allColumns.get(name);
            int argNum = filter.argument();

            int colNum;
            {
                Integer colNumObj = mRowGen.columnNumbers().get(name);
                if (colNumObj == null) {
                    // FIXME: Generate default and compare against that.
                    //        See Converter.setDefault and CompareUtils.
                    throw null;
                }
                colNum = colNumObj;
            }

            int op = filter.operator();
            LocatedColumn located = locateColumn(colNum, dstInfo, op);
            Variable argObjVar = mMaker.param(0); // contains the arg fields prepared earlier
            ColumnCodec codec = codecFor(colNum);
            codec.filterCompare(dstInfo, located.mSrcVar, located.mOffsetVar, null,
                                op, located.mDecoded, argObjVar, argNum, mPass, mFail);

            // Parent node (AndFilter/OrFilter) needs this to be updated.
            mHighestLocated = located;
        }

        @Override
        public void visit(ColumnToColumnFilter filter) {
            // FIXME: visit(ColumnToColumnFilter)
            throw null;
        }

        private int columnNumberFor(String colName) {
            return mRowGen.columnNumbers().get(colName);
        }

        private ColumnCodec codecFor(int colNum) {
            ColumnCodec[] codecs = mKeyCodecs;
            return colNum < codecs.length ? codecs[colNum] : mValueCodecs[colNum - codecs.length];
        }

        /**
         * @param dstInfo current definition for column
         * @param op defined in ColumnFilter
         */
        private LocatedColumn locateColumn(int colNum, ColumnInfo dstInfo, int op) {
            Variable srcVar;
            LocatedColumn[] located;
            ColumnCodec[] codecs = mKeyCodecs;

            init: {
                int startOffset;
                if (colNum < codecs.length) {
                    // Key column.
                    srcVar = mMaker.param(1);
                    if ((located = mLocatedKeys) != null) {
                        break init;
                    }
                    mLocatedKeys = located = new LocatedColumn[mRowGen.info.keyColumns.size()];
                    startOffset = 0;
                } else {
                    // Value column.
                    srcVar = mMaker.param(2);
                    colNum -= codecs.length;
                    codecs = mValueCodecs;
                    if ((located = mLocatedValues) != null) {
                        break init;
                    }
                    mLocatedValues = located = new LocatedColumn[mRowGen.info.valueColumns.size()];
                    startOffset = RowUtils.lengthPrefixPF(mSchemaVersion);
                }
                located[0] = new LocatedColumn(null);
                located[0].located(srcVar, mMaker.var(int.class).set(startOffset));
            }

            // Scan backwards for filling in the gaps.

            int readyNum = colNum;
            while (true) {
                LocatedColumn ready = located[readyNum];
                if (ready != null && ready.mState >= LocatedColumn.LOCATED) {
                    break;
                }
                readyNum--;
            }

            for (; readyNum <= colNum; readyNum++) {
                // Offset will be mutated, and so a copy must be made before calling decode.
                Variable offsetVar = located[readyNum].mOffsetVar;

                LocatedColumn next;
                copyOffsetVar: {
                    if (readyNum >= colNum) {
                        next = null;
                    } else {
                        next = located[readyNum + 1];
                        if (next == null) {
                            next = new LocatedColumn(located[readyNum]);
                            located[readyNum + 1] = next;
                        } else {
                            // Next offset variable is free because state is UNLOCATED.
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

                ColumnCodec codec = codecs[readyNum];
                Variable endVar = null;
                if (readyNum < colNum) {
                    codec.decodeSkip(srcVar, offsetVar, endVar);
                } else {
                    Object decoded = codec.filterDecode(dstInfo, srcVar, offsetVar, endVar, op);
                    located[readyNum].decoded(decoded);
                }

                if (next != null) {
                    // The decode call incremented offsetVar as a side-effect.
                    next.located(srcVar, offsetVar);
                }
            }

            return located[colNum];
        }
    }

    private static class LocatedColumn {
        // Used by mState field.
        private static final int UNLOCATED = 0, LOCATED = 1, DECODED = 2;

        int mState;

        // Source byte array. Is valid when mState is LOCATED or DECODED.
        Variable mSrcVar;

        // Offset into the byte array. Is valid when mState is LOCATED or DECODED.
        Variable mOffsetVar;

        // Optional Object from ColumnCodec.filterDecode. Is valid when mState is DECODED.
        Object mDecoded;

        // Link order matches runtime decode order.
        LocatedColumn mNext;

        LocatedColumn(LocatedColumn prev) {
            if (prev != null) {
                prev.mNext = this;
            }
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
         * @param decoded object returned from ColumnCodec.filterDecode
         */
        void decoded(Object decoded) {
            if (mState == UNLOCATED) {
                throw new IllegalStateException();
            }
            mDecoded = decoded;
            mState = DECODED;
        }

        /**
         * Reset all next DecodedColumns such that they become unlocated. The tail instances
         * can be re-used, reducing the number of Variables created.
         */
        void resetTail() {
            for (LocatedColumn next = mNext; next != null; next = next.mNext) {
                next.mDecoded = null;
                next.mState = UNLOCATED;
            }
        }
    }
}
