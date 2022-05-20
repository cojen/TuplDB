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

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.lang.ref.WeakReference;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Field;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.DatabaseException;
import org.cojen.tupl.Index;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.RowUpdater;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UnmodifiableViewException;

import org.cojen.tupl.core.RowPredicateLock;

import org.cojen.tupl.diag.QueryPlan;

import org.cojen.tupl.filter.ColumnFilter;

import org.cojen.tupl.views.ViewUtils;

/**
 * Makes Table classes that extend BaseTable.
 *
 * @author Brian S O'Neill
 */
public class TableMaker {
    private final RowStore mStore;
    private final Class<?> mRowType;
    private final RowGen mRowGen;
    private final RowInfo mRowInfo;
    private final RowGen mCodecGen;
    private final Class<?> mRowClass;
    private final byte[] mSecondaryDescriptor;
    private final long mIndexId;
    private final boolean mSupportsIndexLock;
    private final ColumnInfo mAutoColumn;

    private ClassMaker mClassMaker;

    /**
     * Constructor for primary table.
     *
     * @param store generated class is pinned to this specific instance
     * @param rowGen describes row encoding
     */
    TableMaker(RowStore store, Class<?> type, RowGen rowGen,
               long indexId, boolean supportsIndexLock)
    {
        this(store, type, rowGen, rowGen, null, indexId, supportsIndexLock);
    }

    /**
     * Constructor for secondary index view table.
     *
     * @param store generated class is pinned to this specific instance
     * @param rowGen describes row encoding
     * @param codecGen describes key and value codecs (different than rowGen)
     * @param secondaryDesc secondary index descriptor
     */
    TableMaker(RowStore store, Class<?> type, RowGen rowGen, RowGen codecGen,
               byte[] secondaryDesc, long indexId, boolean supportsIndexLock)
    {
        mStore = store;
        mRowType = type;
        mRowGen = rowGen;
        mRowInfo = rowGen.info;
        mCodecGen = codecGen;
        mRowClass = RowMaker.find(type);
        mSecondaryDescriptor = secondaryDesc;
        mIndexId = indexId;
        mSupportsIndexLock = supportsIndexLock;

        ColumnInfo auto = null;
        if (isPrimaryTable()) {
            for (ColumnInfo column : codecGen.info.keyColumns.values()) {
                if (column.isAutomatic()) {
                    auto = column;
                    break;
                }
            }
        }

        mAutoColumn = auto;
    }

    /**
     * Return a constructor which accepts a (TableManager, Index, RowPredicateLock) and returns
     * a BaseTable or BaseTableIndex implementation.
     */
    MethodHandle finish() {
        {
            String suffix;
            Class baseClass;

            if (isPrimaryTable()) {
                suffix = "Table";
                baseClass = BaseTable.class;
            } else {
                suffix = "Unjoined";
                baseClass = BaseTableIndex.class;
            }

            mClassMaker = mCodecGen.beginClassMaker(getClass(), mRowType, suffix).public_()
                .extend(baseClass).implement(TableBasicsMaker.find(mRowType));
        }

        MethodType mt = MethodType.methodType
            (void.class, TableManager.class, Index.class, RowPredicateLock.class);

        MethodMaker ctor = mClassMaker.addConstructor(mt);
        ctor.invokeSuperConstructor(ctor.param(0), ctor.param(1), ctor.param(2));

        // Add private methods which check that required columns are set.
        {
            addCheckSet("checkPrimaryKeySet", mCodecGen.info.keyColumns);

            //addCheckSet("checkValue", mCodecGen.info.valueColumns);

            if (isPrimaryTable()) {
                addCheckSet("checkAllSet", mCodecGen.info.allColumns);
                addRequireSet("requireAllSet", mCodecGen.info.allColumns);
            }

            int i = 0;
            for (ColumnSet altKey : mCodecGen.info.alternateKeys) {
                addCheckSet("checkAltKeySet$" + i, altKey.keyColumns);
                i++;
            }

            if (isPrimaryTable() && !mCodecGen.info.valueColumns.isEmpty()) {
                addCheckAllDirty("checkValueAllDirty", mCodecGen.info.valueColumns);
            }

            addCheckAnyDirty("checkPrimaryKeyAnyDirty", mCodecGen.info.keyColumns);
            addCheckAllDirty("checkPrimaryKeyAllDirty", mCodecGen.info.keyColumns);
        }

        // Add encode/decode methods.
        {
            ColumnCodec[] keyCodecs = mCodecGen.keyCodecs();
            addEncodeColumnsMethod("encodePrimaryKey", keyCodecs);
            addDecodeColumnsMethod("decodePrimaryKey", keyCodecs);
            addUpdatePrimaryKeyMethod(keyCodecs);

            if (isPrimaryTable()) {
                addDynamicEncodeValueColumns();
                addDynamicDecodeValueColumns();
                addDynamicUpdateValueColumns();
            } else {
                // The encodeValue and updateValue methods are only used for storing rows into
                // the table. By making them always fail, there's no backdoor to permit
                // modifications.
                mClassMaker.addMethod(byte[].class, "encodeValue", mRowClass)
                    .static_().new_(UnmodifiableViewException.class).throw_();
                mClassMaker.addMethod(byte[].class, "updateValue", mRowClass, byte[].class)
                    .static_().new_(UnmodifiableViewException.class).throw_();

                addDecodeColumnsMethod("decodeValue", mCodecGen.valueCodecs());
            }

            addDecodePartialHandle();
        }

        // Add code to support an automatic column (if defined).
        if (mAutoColumn != null) {
            Class autoGenClass, autoGenApplierClass;
            Object minVal, maxVal;
            if (mAutoColumn.type == int.class) {
                if (mAutoColumn.isUnsigned()) {
                    autoGenClass = AutomaticKeyGenerator.OfUInt.class;
                } else {
                    autoGenClass = AutomaticKeyGenerator.OfInt.class;
                }
                autoGenApplierClass = AutomaticKeyGenerator.OfInt.Applier.class;
                minVal = (int) Math.max(mAutoColumn.autoMin, Integer.MIN_VALUE);
                maxVal = (int) Math.min(mAutoColumn.autoMax, Integer.MAX_VALUE);
            } else {
                if (mAutoColumn.isUnsigned()) {
                    autoGenClass = AutomaticKeyGenerator.OfULong.class;
                } else {
                    autoGenClass = AutomaticKeyGenerator.OfLong.class;
                }
                autoGenApplierClass = AutomaticKeyGenerator.OfLong.Applier.class;
                minVal = mAutoColumn.autoMin;
                maxVal = mAutoColumn.autoMax;
            }

            mClassMaker.implement(autoGenApplierClass);

            mClassMaker.addField(autoGenClass, "autogen").private_().final_();

            ctor.field("autogen").set
                (ctor.new_(autoGenClass, ctor.param(1), minVal, maxVal, ctor.this_()));

            MethodMaker mm = mClassMaker.addMethod
                (RowPredicateLock.Closer.class, "applyToRow",
                 Transaction.class, Object.class, mAutoColumn.type);
            mm.public_();
            var rowVar = mm.param(1).cast(mRowClass);
            rowVar.field(mAutoColumn.name).set(mm.param(2));

            if (!mSupportsIndexLock) {
                mm.return_(mm.var(RowPredicateLock.NonCloser.class).field("THE"));
            } else {
                mm.return_(mm.field("mIndexLock").invoke("tryOpenAcquire", mm.param(0), rowVar));
            }

            var allButAuto = new TreeMap<>(mCodecGen.info.allColumns);
            allButAuto.remove(mAutoColumn.name);
            addCheckSet("checkAllButAutoSet", allButAuto);

            addStoreAutoMethod();
        }

        // Add the public load/store methods, etc.

        addByKeyMethod("load");
        addByKeyMethod("exists");

        if (isPrimaryTable()) {
            addByKeyMethod("delete");

            addStoreMethod("store", null);
            addStoreMethod("exchange", mRowType);
            addStoreMethod("insert", boolean.class);
            addStoreMethod("replace", boolean.class);

            addDoUpdateMethod();
            addUpdateMethod("update", false);
            addUpdateMethod("merge", true);
        }

        addMarkAllCleanMethod();
        addToRowMethod();
        addRowStoreRefMethod();

        addUnfilteredMethods();

        addPlanMethod(0b00);
        addPlanMethod(0b01); // reverse option

        if (!isPrimaryTable()) {
            addSecondaryDescriptorMethod();
        }

        return doFinish(mt);
    }

    /**
     * Return a constructor which accepts a (Index, RowPredicateLock, TableImpl primary,
     * TableImpl unjoined) and returns a BaseTableIndex implementation.
     *
     * @param primaryTableClass the primary table implementation class
     * @param unjoinedClass the table implementation which is passed as the last constructor
     * parameter
     */
    MethodHandle finishJoined(Class<?> primaryTableClass, Class<?> unjoinedClass) {
        Objects.requireNonNull(primaryTableClass);

        mClassMaker = mCodecGen.beginClassMaker(getClass(), mRowType, "Joined").public_()
            .extend(unjoinedClass);

        {
            MethodMaker mm = mClassMaker.addMethod
                (Class.class, "joinedPrimaryTableClass").protected_();
            mm.return_(primaryTableClass);
        }

        MethodType mt = MethodType.methodType
            (void.class, Index.class, RowPredicateLock.class, primaryTableClass, unjoinedClass);

        MethodMaker ctor = mClassMaker.addConstructor(mt);

        var indexVar = ctor.param(0);
        var lockVar = ctor.param(1);
        var primaryVar = ctor.param(2);
        var unjoinedVar = ctor.param(3);
        var managerVar = primaryVar.invoke("tableManager");

        ctor.invokeSuperConstructor(managerVar, indexVar, lockVar);

        mClassMaker.addField(primaryTableClass, "primaryTable").private_().final_();
        ctor.field("primaryTable").set(primaryVar);

        mClassMaker.addField(Index.class, "primaryIndex").private_().final_();
        ctor.field("primaryIndex").set(managerVar.invoke("primaryIndex"));

        mClassMaker.addField(unjoinedClass, "unjoined").private_().final_();
        ctor.field("unjoined").set(unjoinedVar);

        {
            MethodMaker mm = mClassMaker.addMethod(BaseTable.class, "viewUnjoined").public_();
            mm.return_(mm.field("unjoined"));
        }

        {
            // Create three toPrimaryKey methods. A value param is always needed for alternate
            // keys but never for ordinary secondary indexes.
            int mode = mCodecGen.info.isAltKey() ? 0b011 : 0b010;
            while (mode <= 0b111) {
                addToPrimaryKeyMethod(mClassMaker, mode);
                mode += 0b010;
            }
        }

        addJoinedLoadMethod(primaryTableClass);

        // Override the methods inherited from the unjoined class, defined in BaseTable.
        addJoinedUnfilteredMethods(primaryTableClass);

        // Override the methods inherited from the unjoined class, defined in ScanControllerFactory.
        addPlanMethod(0b10);
        addPlanMethod(0b11); // reverse option

        // Override the method inherited from BaseTableIndex.
        MethodMaker mm = mClassMaker.addMethod
            (RowUpdater.class, "newRowUpdater", Transaction.class, ScanController.class);
        mm.protected_();
        mm.return_(mm.invoke("newJoinedRowUpdater", mm.param(0), mm.param(1),
                             mm.field("primaryTable")));

        return doFinish(mt);
    }

    private void addJoinedLoadMethod(Class<?> primaryTableClass) {
        MethodMaker mm = mClassMaker.addMethod
            (boolean.class, "load", Transaction.class, Object.class).public_();

        Variable txnVar = mm.param(0);
        Variable rowVar = mm.param(1).cast(mRowClass);

        {
            Label ready = mm.label();
            mm.invoke("checkPrimaryKeySet", rowVar).ifTrue(ready);
            mm.new_(IllegalStateException.class, "Primary key isn't fully specified").throw_();
            ready.here();
        }

        Variable valueVar = null;
        Variable repeatableVar = null;
        final Variable joinedPkVar;

        Label notFound = mm.label();

        if (mCodecGen.info.isAltKey()) {
            var keyVar = mm.invoke("encodePrimaryKey", rowVar);
            valueVar = mm.field("mSource").invoke("load", txnVar, keyVar);
            valueVar.ifEq(null, notFound);
            joinedPkVar = mm.invoke("toPrimaryKey", rowVar, keyVar, valueVar);
        } else {
            joinedPkVar = mm.var(byte[].class);
            Label cont = mm.label();
            Label repeatable = mm.label();
            mm.var(RowUtils.class).invoke("isRepeatable", txnVar).ifTrue(repeatable);
            joinedPkVar.set(mm.invoke("toPrimaryKey", rowVar));
            mm.goto_(cont);
            repeatable.here();
            var keyVar = mm.invoke("encodePrimaryKey", rowVar);
            // Calling exists is necessary for proper lock acquisition order. A return value of
            // false isn't expected, but handle it just in case.
            mm.field("mSource").invoke("exists", txnVar, keyVar).ifFalse(notFound);
            joinedPkVar.set(mm.invoke("toPrimaryKey", rowVar, keyVar));
            cont.here();
        }

        var joinedValueVar = mm.field("primaryIndex").invoke("load", txnVar, joinedPkVar);

        Label notNull = mm.label();
        joinedValueVar.ifNe(null, notNull);

        notFound.here();
        markValuesUnset(rowVar);
        mm.return_(false);

        notNull.here();

        if (repeatableVar == null) {
            repeatableVar = mm.var(RowUtils.class).invoke("isRepeatable", txnVar);
        }

        Label checked = mm.label();
        repeatableVar.ifFalse(checked);

        if (valueVar != null) {
            // Decode the primary key columns (required by alt key only).
            mm.invoke("decodeValue", rowVar, valueVar);
        }

        mm.var(primaryTableClass).invoke("decodeValue", rowVar, joinedValueVar);

        Label success = mm.label().here();
        markAllClean(rowVar, mRowGen, mRowGen);
        mm.return_(true);

        // This point is reached for double checking that the joined row matches to the
        // secondary row, which is required when a lock isn't held.
        checked.here();

        // Copy of all the columns which will be modified by decodeValue.
        Map<String, ColumnInfo> copiedColumns;
        if (valueVar != null) {
            // For alt key, the primary key columns will be modified too.
            copiedColumns = mRowInfo.allColumns;
        } else {
            copiedColumns = mRowInfo.valueColumns;
        }
        Map<String, Variable> copiedVars = new LinkedHashMap<>(copiedColumns.size());
        for (String name : copiedColumns.keySet()) {
            copiedVars.put(name, rowVar.field(name).get());
        }

        if (valueVar != null) {
            // For alt key, decode the primary key columns too.
            mm.invoke("decodeValue", rowVar, valueVar);
        }

        mm.var(primaryTableClass).invoke("decodeValue", rowVar, joinedValueVar);

        // Check all the secondary columns, except those that refer to the primary key, which
        // won't have changed.
        Label fail = mm.label();
        Map<String, ColumnInfo> pkColumns = mRowInfo.keyColumns;
        for (ColumnInfo column : mCodecGen.info.allColumns.values()) {
            String name = column.name;
            if (pkColumns.containsKey(name)) {
                continue;
            }
            Label pass = mm.label();
            // Note that the secondary columns are passed as the compare arguments, because
            // that's what they effectively are -- a type of filter expression. This is
            // important because the comparison isn't necessarily symmetrical. See
            // BigDecimalUtils.matches.
            CompareUtils.compare(mm, column, rowVar.field(name),
                                 copiedColumns.get(name), copiedVars.get(name),
                                 ColumnFilter.OP_EQ, pass, fail);
            pass.here();
        }

        mm.goto_(success);

        fail.here();

        // Restore all the columns back to their original values, preventing any side-effects.
        // When the load method returns false, it's not supposed to modify any columns,
        // regardless of their state.
        for (Map.Entry<String, Variable> e : copiedVars.entrySet()) {
            rowVar.field(e.getKey()).set(e.getValue());
        }

        mm.goto_(notFound);
    }

    private MethodHandle doFinish(MethodType mt) {
        try {
            var lookup = mClassMaker.finishLookup();
            return lookup.findConstructor(lookup.lookupClass(), mt);
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    private boolean isPrimaryTable() {
        return mRowGen == mCodecGen;
    }

    private boolean supportsTriggers() {
        return isPrimaryTable();
    }

    /**
     * Defines a static method which accepts a row and returns boolean. When it returns true,
     * all of the given columns are set.
     *
     * @param name method name
     */
    private void addCheckSet(String name, Map<String, ColumnInfo> columns) {
        MethodMaker mm = mClassMaker.addMethod(boolean.class, name, mRowClass).static_();

        if (columns.isEmpty()) {
            mm.return_(true);
            return;
        }

        if (columns.size() == 1) {
            int num = mRowGen.columnNumbers().get(columns.values().iterator().next().name);
            Label cont = mm.label();
            stateField(mm.param(0), num).and(RowGen.stateFieldMask(num)).ifNe(0, cont);
            mm.return_(false);
            cont.here();
            mm.return_(true);
            return;
        }

        int num = 0, mask = 0;

        for (int step = 0; step < 2; step++) {
            // Key columns are numbered before value columns. Add checks in two steps.
            // Note that the codecs are accessed, to match encoding order.
            var baseCodecs = step == 0 ? mRowGen.keyCodecs() : mRowGen.valueCodecs();

            for (ColumnCodec codec : baseCodecs) {
                ColumnInfo info = codec.mInfo;

                if (columns.containsKey(info.name)) {
                    mask |= RowGen.stateFieldMask(num);
                }

                if (isMaskReady(++num, mask)) {
                    // Convert all states of value 0b01 (clean) into value 0b11 (dirty). All
                    // other states stay the same.
                    var state = stateField(mm.param(0), num - 1).get();
                    state = state.or(state.and(0x5555_5555).shl(1));

                    // Flip all column state bits. If final result is non-zero, then some
                    // columns were unset.
                    state = state.xor(mask);
                    mask = maskRemainder(num, mask);
                    if (mask != 0xffff_ffff) {
                        state = state.and(mask);
                    }

                    Label cont = mm.label();
                    state.ifEq(0, cont);
                    mm.return_(false);
                    cont.here();
                    mask = 0;
                }
            }
        }

        mm.return_(true);
    }

    /**
     * Defines a static method which accepts a row and returns boolean. When it returns true,
     * all of the given columns are dirty.
     *
     * @param name method name
     */
    private void addCheckAllDirty(String name, Map<String, ColumnInfo> columns) {
        MethodMaker mm = mClassMaker.addMethod(boolean.class, name, mRowClass).static_();

        int num = 0, mask = 0;

        for (int step = 0; step < 2; step++) {
            // Key columns are numbered before value columns. Add checks in two steps.
            // Note that the codecs are accessed, to match encoding order.
            var baseCodecs = step == 0 ? mRowGen.keyCodecs() : mRowGen.valueCodecs();

            for (ColumnCodec codec : baseCodecs) {
                ColumnInfo info = codec.mInfo;

                if (columns.containsKey(info.name)) {
                    mask |= RowGen.stateFieldMask(num);
                }

                if (isMaskReady(++num, mask)) {
                    Label cont = mm.label();
                    stateField(mm.param(0), num - 1).and(mask).ifEq(mask, cont);
                    mm.return_(false);
                    cont.here();
                    mask = 0;
                }
            }
        }

        mm.return_(true);
    }

    /**
     * Defines a static method which accepts a row and returns boolean. When it returns true,
     * at least one of the given columns are dirty.
     *
     * @param name method name
     */
    private void addCheckAnyDirty(String name, Map<String, ColumnInfo> columns) {
        MethodMaker mm = mClassMaker.addMethod(boolean.class, name, mRowClass).static_();

        int num = 0, mask = 0;

        for (int step = 0; step < 2; step++) {
            // Key columns are numbered before value columns. Add checks in two steps.
            // Note that the codecs are accessed, to match encoding order.
            var baseCodecs = step == 0 ? mRowGen.keyCodecs() : mRowGen.valueCodecs();

            for (ColumnCodec codec : baseCodecs) {
                ColumnInfo info = codec.mInfo;

                if (columns.containsKey(info.name)) {
                    mask |= RowGen.stateFieldMask(num, 0b10);
                }

                if (isMaskReady(++num, mask)) {
                    Label cont = mm.label();
                    stateField(mm.param(0), num - 1).and(mask).ifEq(0, cont);
                    mm.return_(true);
                    cont.here();
                    mask = 0;
                }
            }
        }

        mm.return_(false);
    }

    /**
     * Called when building state field masks for columns, when iterating them in order.
     *
     * @param num column number pre-incremented to the next one
     * @param mask current group; must be non-zero to have any effect
     */
    private boolean isMaskReady(int num, int mask) {
        return mask != 0 && ((num & 0b1111) == 0 || num >= mRowInfo.allColumns.size());
    }

    /**
     * When building a mask for the highest state field, sets the high unused bits on the
     * mask. This can eliminate an unnecessary 'and' operation.
     *
     * @param num column number pre-incremented to the next one
     * @param mask current group
     * @return updated mask
     */
    private int maskRemainder(int num, int mask) {
        if (num >= mRowInfo.allColumns.size()) {
            int shift = (num & 0b1111) << 1;
            if (shift != 0) {
                mask |= 0xffff_ffff << shift;
            }
        }
        return mask;
    }

    /**
     * Defines a static method which accepts a row and always throws a detailed exception
     * describing the required columns which aren't set. A check method should have been
     * invoked first.
     *
     * @param name method name
     */
    private void addRequireSet(String name, Map<String, ColumnInfo> columns) {
        MethodMaker mm = mClassMaker.addMethod(null, name, mRowClass).static_();

        String initMessage = "Some required columns are unset";

        if (columns.isEmpty()) {
            mm.new_(IllegalStateException.class, initMessage).throw_();
            return;
        }

        int initLength = initMessage.length() + 2;
        var bob = mm.new_(StringBuilder.class, initLength << 1)
            .invoke("append", initMessage).invoke("append", ": ");

        boolean first = true;
        for (ColumnInfo info : columns.values()) {
            int num = mRowGen.columnNumbers().get(info.name);
            Label isSet = mm.label();
            stateField(mm.param(0), num).and(RowGen.stateFieldMask(num)).ifNe(0, isSet);
            if (!first) {
                Label sep = mm.label();
                bob.invoke("length").ifEq(initLength, sep);
                bob.invoke("append", ", ");
                sep.here();
            }
            bob.invoke("append", info.name);
            isSet.here();
            first = false;
        }

        mm.new_(IllegalStateException.class, bob.invoke("toString")).throw_();
    }

    /**
     * @return null if no field is defined for the column (probably SchemaVersionColumnCodec)
     */
    private static Field findField(Variable row, ColumnCodec codec) {
        ColumnInfo info = codec.mInfo;
        return info == null ? null : row.field(info.name);
    }

    /**
     * Defines a static method which returns a new composite byte[] key or value. Caller must
     * check that the columns are set.
     *
     * @param name method name
     */
    private void addEncodeColumnsMethod(String name, ColumnCodec[] codecs) {
        MethodMaker mm = mClassMaker.addMethod(byte[].class, name, mRowClass).static_();
        addEncodeColumns(mm, ColumnCodec.bind(codecs, mm));
    }

    /**
     * @param mm param(0): Row object, return: byte[]
     * @param codecs must be bound to the MethodMaker
     */
    private static void addEncodeColumns(MethodMaker mm, ColumnCodec[] codecs) {
        if (codecs.length == 0) {
            mm.return_(mm.var(RowUtils.class).field("EMPTY_BYTES"));
            return;
        }

        // Determine the minimum byte array size and prepare the encoders.
        int minSize = 0;
        for (ColumnCodec codec : codecs) {
            minSize += codec.minSize();
            codec.encodePrepare();
        }

        // Generate code which determines the additional runtime length.
        Variable totalVar = null;
        for (ColumnCodec codec : codecs) {
            Field srcVar = findField(mm.param(0), codec);
            totalVar = codec.encodeSize(srcVar, totalVar);
        }

        // Generate code which allocates the destination byte array.
        Variable dstVar;
        if (totalVar == null) {
            dstVar = mm.new_(byte[].class, minSize);
        } else {
            if (minSize != 0) {
                totalVar = totalVar.add(minSize);
            }
            dstVar = mm.new_(byte[].class, totalVar);
        }

        // Generate code which fills in the byte array.
        var offsetVar = mm.var(int.class).set(0);
        for (ColumnCodec codec : codecs) {
            codec.encode(findField(mm.param(0), codec), dstVar, offsetVar);
        }

        mm.return_(dstVar);
    }

    /**
     * Defines a static method which encodes a new key by comparing dirty row columns to the
     * original key.
     *
     * @param name method name
     */
    private void addUpdatePrimaryKeyMethod(ColumnCodec[] codecs) {
        MethodMaker mm = mClassMaker
            .addMethod(byte[].class, "updatePrimaryKey", mRowClass, byte[].class).static_();

        Variable rowVar = mm.param(0);

        Label partiallyDirty = mm.label();
        mm.invoke("checkPrimaryKeyAllDirty", rowVar).ifFalse(partiallyDirty);
        mm.return_(mm.invoke("encodePrimaryKey", rowVar));
        partiallyDirty.here();

        var tableVar = mm.class_();
        var ue = encodeUpdateEntry(mm, mRowInfo, 0, tableVar, rowVar, mm.param(1));

        mm.return_(ue.newEntryVar);
    }

    /**
     * Implements: static byte[] encodeValue(RowClass row)
     *
     * Method isn't implemented until needed, delaying acquisition/creation of the current
     * schema version. This allows replicas to decode existing rows even when the class
     * definition has changed, but encoding will still fail.
     */
    private void addDynamicEncodeValueColumns() {
        MethodMaker mm = mClassMaker.addMethod(byte[].class, "encodeValue", mRowClass).static_();
        var indy = mm.var(TableMaker.class).indy
            ("indyEncodeValueColumns", mStore.ref(), mRowType, mIndexId);
        mm.return_(indy.invoke(byte[].class, "encodeValue", null, mm.param(0)));
    }

    public static CallSite indyEncodeValueColumns
        (MethodHandles.Lookup lookup, String name, MethodType mt,
         WeakReference<RowStore> storeRef, Class<?> rowType, long indexId)
    {
        return doIndyEncode
            (lookup, name, mt, storeRef, rowType, indexId, (mm, info, schemaVersion) -> {
                ColumnCodec[] codecs = info.rowGen().valueCodecs();
                addEncodeColumns(mm, ColumnCodec.bind(schemaVersion, codecs, mm));
            });
    }

    /**
     * Implements: static byte[] updateValue(RowClass row, byte[] original)
     *
     * Method isn't fully implemented until needed, delaying acquisition/creation of the
     * current schema version. This allows replicas to decode existing rows even when the class
     * definition has changed, but encoding will still fail.
     */
    private void addDynamicUpdateValueColumns() {
        MethodMaker mm = mClassMaker
            .addMethod(byte[].class, "updateValue", mRowClass, byte[].class).static_();

        if (mCodecGen.info.valueColumns.isEmpty()) {
            // If the checkValueAllDirty method was defined, it would always return true.
            mm.return_(mm.var(RowUtils.class).field("EMPTY_BYTES"));
            return;
        }

        Variable rowVar = mm.param(0);

        Label partiallyDirty = mm.label();
        mm.invoke("checkValueAllDirty", rowVar).ifFalse(partiallyDirty);
        mm.return_(mm.invoke("encodeValue", rowVar));
        partiallyDirty.here();

        var indy = mm.var(TableMaker.class).indy
            ("indyUpdateValueColumns", mStore.ref(), mRowType, mIndexId);
        mm.return_(indy.invoke(byte[].class, "updateValue", null, rowVar, mm.param(1)));
    }

    public static CallSite indyUpdateValueColumns
        (MethodHandles.Lookup lookup, String name, MethodType mt,
         WeakReference<RowStore> storeRef, Class<?> rowType, long indexId)
    {
        return doIndyEncode
            (lookup, name, mt, storeRef, rowType, indexId, (mm, info, schemaVersion) -> {
                if (schemaVersion == 0 || info.valueColumns.isEmpty()) {
                    // Not expected, but handle it nonetheless.
                    mm.return_(mm.var(RowUtils.class).field("EMPTY_BYTES"));
                    return;
                }

                // These variables were provided by the indy call in addDynamicUpdateValueColumns.
                Variable rowVar = mm.param(0);
                Variable originalVar = mm.param(1); // byte[]

                var tableVar = mm.var(lookup.lookupClass());
                var ue = encodeUpdateEntry(mm, info, schemaVersion, tableVar, rowVar, originalVar);

                mm.return_(ue.newEntryVar);
            });
    }

    @FunctionalInterface
    static interface EncodeFinisher {
        void finish(MethodMaker mm, RowInfo info, int schemaVersion);
    }

    /**
     * Does the work to obtain the current schema version, handling any exceptions. The given
     * finisher completes the definition of the encode method when no exception was thrown when
     * trying to obtain the schema version. If an exception was thrown, the finisher might be
     * called at a later time.
     */
    private static CallSite doIndyEncode(MethodHandles.Lookup lookup, String name, MethodType mt,
                                         WeakReference<RowStore> storeRef,
                                         Class<?> rowType, long indexId,
                                         EncodeFinisher finisher)
    {
        return ExceptionCallSite.make(() -> {
            MethodMaker mm = MethodMaker.begin(lookup, name, mt);
            RowStore store = storeRef.get();
            if (store == null) {
                mm.new_(DatabaseException.class, "Closed").throw_();
            } else {
                RowInfo info = RowInfo.find(rowType);
                int schemaVersion;
                try {
                    schemaVersion = store.schemaVersion(info, false, indexId, true);
                } catch (Exception e) {
                    return new ExceptionCallSite.Failed(mt, mm, e);
                }
                finisher.finish(mm, info, schemaVersion);
            }
            return mm.finish();
        });
    }

    /**
     * Defines a static method which decodes columns from a composite byte[] parameter.
     *
     * @param name method name
     */
    private void addDecodeColumnsMethod(String name, ColumnCodec[] codecs) {
        MethodMaker mm = mClassMaker.addMethod(null, name, mRowClass, byte[].class)
            .static_().public_();
        addDecodeColumns(mm, mRowInfo, codecs, 0);
    }

    /**
     * @param mm param(0): Row object, param(1): byte[], return: void
     * @param fixedOffset must be after the schema version (when applicable)
     */
    private static void addDecodeColumns(MethodMaker mm, RowInfo dstRowInfo,
                                         ColumnCodec[] srcCodecs, int fixedOffset)
    {
        srcCodecs = ColumnCodec.bind(srcCodecs, mm);

        Variable srcVar = mm.param(1);
        Variable offsetVar = mm.var(int.class).set(fixedOffset);

        for (ColumnCodec srcCodec : srcCodecs) {
            String name = srcCodec.mInfo.name;
            ColumnInfo dstInfo = dstRowInfo.allColumns.get(name);

            if (dstInfo == null) {
                srcCodec.decodeSkip(srcVar, offsetVar, null);
            } else {
                var rowVar = mm.param(0);
                Field dstVar = rowVar.field(name);
                Converter.decode(mm, srcVar, offsetVar, null, srcCodec, dstInfo, dstVar);
            }
        }
    }

    private void addDynamicDecodeValueColumns() {
        // First define a method which generates the SwitchCallSite.
        {
            MethodMaker mm = mClassMaker.addMethod
                (SwitchCallSite.class, "decodeValueSwitchCallSite").static_();
            var condy = mm.var(TableMaker.class).condy
                ("condyDecodeValueColumns",  mStore.ref(), mRowType, mRowClass, mIndexId);
            mm.return_(condy.invoke(SwitchCallSite.class, "_"));
        }

        // Also define a method to obtain a MethodHandle which decodes for a given schema
        // version. This must be defined here to ensure that the correct lookup is used. It
        // must always refer to this table class.
        {
            MethodMaker mm = mClassMaker.addMethod
                (MethodHandle.class, "decodeValueHandle", int.class).static_();
            var lookup = mm.var(MethodHandles.class).invoke("lookup");
            var mh = mm.invoke("decodeValueSwitchCallSite").invoke("getCase", lookup, mm.param(0));
            mm.return_(mh);
        }

        MethodMaker mm = mClassMaker.addMethod
            (null, "decodeValue", mRowClass, byte[].class).static_().public_();

        var data = mm.param(1);
        var schemaVersion = mm.var(RowUtils.class).invoke("decodeSchemaVersion", data);

        var indy = mm.var(TableMaker.class).indy("indyDecodeValueColumns");
        indy.invoke(null, "decodeValue", null, schemaVersion, mm.param(0), data);
    }

    /**
     * Returns a SwitchCallSite instance suitable for decoding all value columns. By defining
     * it via a "condy" method, the SwitchCallSite instance can be shared by other methods. In
     * particular, filter subclasses are generated against specific schema versions, and so
     * they need direct access to just one of the cases. This avoids a redundant version check.
     *
     * MethodType is: void (int schemaVersion, RowClass row, byte[] data)
     */
    public static SwitchCallSite condyDecodeValueColumns
        (MethodHandles.Lookup lookup, String name, Class<?> type,
         WeakReference<RowStore> storeRef, Class<?> rowType, Class<?> rowClass, long indexId)
    {
        MethodType mt = MethodType.methodType(void.class, int.class, rowClass, byte[].class);

        return new SwitchCallSite(lookup, mt, schemaVersion -> {
            MethodMaker mm = MethodMaker.begin(lookup, null, "case", rowClass, byte[].class);

            RowStore store = storeRef.get();
            if (store == null) {
                mm.new_(DatabaseException.class, "Closed").throw_();
            } else {
                RowInfo dstRowInfo = RowInfo.find(rowType);

                if (schemaVersion == 0) {
                    // No columns to decode, so assign defaults.
                    for (Map.Entry<String, ColumnInfo> e : dstRowInfo.valueColumns.entrySet()) {
                        Converter.setDefault(mm, e.getValue(), mm.param(0).field(e.getKey()));
                    }
                } else {
                    RowInfo srcRowInfo;
                    try {
                        srcRowInfo = store.rowInfo(rowType, indexId, schemaVersion);
                    } catch (Exception e) {
                        return new ExceptionCallSite.Failed
                            (MethodType.methodType(void.class, rowClass, byte[].class), mm, e);
                    }

                    ColumnCodec[] srcCodecs = srcRowInfo.rowGen().valueCodecs();
                    int fixedOffset = schemaVersion < 128 ? 1 : 4;

                    addDecodeColumns(mm, dstRowInfo, srcCodecs, fixedOffset);

                    if (dstRowInfo != srcRowInfo) {
                        // Assign defaults for any missing columns.
                        for (Map.Entry<String, ColumnInfo> e : dstRowInfo.valueColumns.entrySet()) {
                            String fieldName = e.getKey();
                            if (!srcRowInfo.valueColumns.containsKey(fieldName)) {
                                Converter.setDefault
                                    (mm, e.getValue(), mm.param(0).field(fieldName));
                            }
                        }
                    }
                }
            }

            return mm.finish();
        });
    }

    /**
     * This just returns the SwitchCallSite generated by condyDecodeValueColumns.
     */
    public static SwitchCallSite indyDecodeValueColumns(MethodHandles.Lookup lookup,
                                                        String name, MethodType mt)
        throws Throwable
    {
        MethodHandle mh = lookup.findStatic(lookup.lookupClass(), "decodeValueSwitchCallSite",
                                            MethodType.methodType(SwitchCallSite.class));
        return (SwitchCallSite) mh.invokeExact();
    }

    private void addDecodePartialHandle() {
        MethodMaker mm = mClassMaker.addMethod
            (MethodHandle.class, "makeDecodePartialHandle", byte[].class, int.class).protected_();

        var spec = mm.param(0);
        var lookup = mm.var(MethodHandles.class).invoke("lookup");

        Variable decoder;
        if (isPrimaryTable()) {
            var schemaVersion = mm.param(1);
            var storeRef = mm.invoke("rowStoreRef");
            decoder = mm.var(DecodePartialMaker.class).invoke
                ("makeDecoder", lookup, storeRef, mRowType, mRowClass, mm.class_(),
                 mIndexId, spec, schemaVersion);
        } else {
            var secondaryDescVar = mm.var(byte[].class).setExact(mSecondaryDescriptor);
            decoder = mm.var(DecodePartialMaker.class).invoke
                ("makeDecoder", lookup, mRowType, mRowClass, mm.class_(), secondaryDescVar, spec);
        }

        mm.return_(decoder);
    }

    /**
     * @param variant "load", "exists", or "delete"
     */
    private void addByKeyMethod(String variant) {
        MethodMaker mm = mClassMaker.addMethod
            (boolean.class, variant, Transaction.class, Object.class).public_();

        Variable txnVar = mm.param(0);
        Variable rowVar = mm.param(1).cast(mRowClass);

        Label ready = mm.label();
        mm.invoke("checkPrimaryKeySet", rowVar).ifTrue(ready);
        mm.new_(IllegalStateException.class, "Primary key isn't fully specified").throw_();

        ready.here();

        var keyVar = mm.invoke("encodePrimaryKey", rowVar);

        final var source = mm.field("mSource");

        final Variable valueVar;

        if (variant != "delete" || !supportsTriggers()) {
            valueVar = source.invoke(variant, txnVar, keyVar);
        } else {
            var triggerVar = mm.var(Trigger.class);
            Label skipLabel = mm.label();
            prepareForTrigger(mm, mm.this_(), triggerVar, skipLabel);
            Label triggerStart = mm.label().here();

            // Trigger requires a non-null transaction.
            txnVar.set(mm.var(ViewUtils.class).invoke("enterScope", source, txnVar));
            Label txnStart = mm.label().here();

            var cursorVar = source.invoke("newCursor", txnVar);
            Label cursorStart = mm.label().here();

            cursorVar.invoke("find", keyVar);
            var oldValueVar = cursorVar.invoke("value");
            Label commit = mm.label();
            oldValueVar.ifEq(null, commit);
            triggerVar.invoke("delete", txnVar, rowVar, keyVar, oldValueVar);
            commit.here();
            cursorVar.invoke("commit", (Object) null);
            mm.return_(oldValueVar.ne(null));

            mm.finally_(cursorStart, () -> cursorVar.invoke("reset"));
            mm.finally_(txnStart, () -> txnVar.invoke("exit"));

            skipLabel.here();

            assert variant == "delete";
            valueVar = source.invoke(variant, txnVar, keyVar);

            mm.finally_(triggerStart, () -> triggerVar.invoke("releaseShared"));
        }

        if (variant != "load") {
            mm.return_(valueVar);
        } else {
            Label notNull = mm.label();
            valueVar.ifNe(null, notNull);
            markValuesUnset(rowVar);
            mm.return_(false);
            notNull.here();
            mm.invoke("decodeValue", rowVar, valueVar);
            markAllClean(rowVar);
            mm.return_(true);
        }
    }

    /**
     * @param variant "store", "exchange", "insert", or "replace"
     */
    private void addStoreMethod(String variant, Class returnType) {
        MethodMaker mm = mClassMaker.addMethod
            (returnType, variant, Transaction.class, Object.class).public_();

        Variable txnVar = mm.param(0);
        Variable rowVar = mm.param(1).cast(mRowClass);

        Label ready = mm.label();
        mm.invoke("checkAllSet", rowVar).ifTrue(ready);

        if (variant != "replace" && mAutoColumn != null) {
            Label notReady = mm.label();
            mm.invoke("checkAllButAutoSet", rowVar).ifFalse(notReady);
            mm.invoke("storeAuto", txnVar, rowVar);
            if (variant == "exchange") {
                mm.return_(null);
            } else if (variant == "insert") {
                mm.return_(true);
            } else {
                mm.return_();
            }
            notReady.here();
        }

        mm.invoke("requireAllSet", rowVar);

        ready.here();

        var keyVar = mm.invoke("encodePrimaryKey", rowVar);
        var valueVar = mm.invoke("encodeValue", rowVar);

        Variable resultVar = null;

        if (!supportsTriggers()) {
            resultVar = storeNoTrigger(mm, variant, txnVar, rowVar, keyVar, valueVar);
        } else {
            Label cont = mm.label();

            var triggerVar = mm.var(Trigger.class);
            Label skipLabel = mm.label();
            prepareForTrigger(mm, mm.this_(), triggerVar, skipLabel);
            Label triggerStart = mm.label().here();

            final var source = mm.field("mSource").get();

            // Trigger requires a non-null transaction.
            txnVar.set(mm.var(ViewUtils.class).invoke("enterScope", source, txnVar));
            Label txnStart = mm.label().here();

            mm.invoke("redoPredicateMode", txnVar);

            // Always use a cursor to acquire the upgradable row lock before updating
            // secondaries. This prevents deadlocks with a concurrent index scan which joins
            // against the row. The row lock is acquired exclusively after all secondaries have
            // been updated. At that point, shared lock acquisition against the row is blocked.
            var cursorVar = source.invoke("newCursor", txnVar);
            Label cursorStart = mm.label().here();

            if (variant == "replace") {
                cursorVar.invoke("find", keyVar);
                var oldValueVar = cursorVar.invoke("value");
                Label passed = mm.label();
                oldValueVar.ifNe(null, passed);
                mm.return_(false);
                passed.here();
                triggerVar.invoke("store", txnVar, rowVar, keyVar, oldValueVar, valueVar);
                cursorVar.invoke("commit", valueVar);
                markAllClean(rowVar);
                mm.return_(true);
            } else {
                Variable closerVar;
                Label opStart;
                if (!mSupportsIndexLock) {
                    closerVar = null;
                    opStart = null;
                } else {
                    closerVar = mm.field("mIndexLock").invoke("openAcquire", txnVar, rowVar);
                    opStart = mm.label().here();
                }

                if (variant == "insert") {
                    cursorVar.invoke("autoload", false);
                    cursorVar.invoke("find", keyVar);
                    if (closerVar != null) {
                        mm.finally_(opStart, () -> closerVar.invoke("close"));
                    }
                    Label passed = mm.label();
                    cursorVar.invoke("value").ifEq(null, passed);
                    mm.return_(false);
                    passed.here();
                    triggerVar.invoke("insert", txnVar, rowVar, keyVar, valueVar);
                    cursorVar.invoke("commit", valueVar);
                    markAllClean(rowVar);
                    mm.return_(true);
                } else {
                    cursorVar.invoke("find", keyVar);
                    if (closerVar != null) {
                        mm.finally_(opStart, () -> closerVar.invoke("close"));
                    }
                    var oldValueVar = cursorVar.invoke("value");
                    Label wasNull = mm.label();
                    oldValueVar.ifEq(null, wasNull);
                    triggerVar.invoke("store", txnVar, rowVar, keyVar, oldValueVar, valueVar);
                    Label commit = mm.label().goto_();
                    wasNull.here();
                    triggerVar.invoke("insert", txnVar, rowVar, keyVar, valueVar);
                    commit.here();
                    cursorVar.invoke("commit", valueVar);
                    if (variant == "store") {
                        markAllClean(rowVar);
                        mm.return_();
                    } else {
                        resultVar = oldValueVar;
                        mm.goto_(cont);
                    }
                }
            }

            mm.finally_(cursorStart, () -> cursorVar.invoke("reset"));
            mm.finally_(txnStart, () -> txnVar.invoke("exit"));

            skipLabel.here();

            Variable storeResultVar = storeNoTrigger(mm, variant, txnVar, rowVar, keyVar, valueVar);
            
            if (resultVar == null) {
                resultVar = storeResultVar;
            } else {
                resultVar.set(storeResultVar);
            }

            mm.finally_(triggerStart, () -> triggerVar.invoke("releaseShared"));

            cont.here();
        }

        if (returnType == null) {
            // This case is expected only for the "store" variant.
            markAllClean(rowVar);
            return;
        }

        if (variant != "exchange") {
            // This case is expected for the "insert" and "replace" variants.
            Label failed = mm.label();
            resultVar.ifFalse(failed);
            markAllClean(rowVar);
            failed.here();
            mm.return_(resultVar);
            return;
        }

        // The rest is for implementing the "exchange" variant.

        markAllClean(rowVar);
        Label found = mm.label();
        resultVar.ifNe(null, found);
        mm.return_(null);
        found.here();

        var copyVar = mm.new_(mRowClass);
        copyFields(mm, rowVar, copyVar, mCodecGen.info.keyColumns.values());
        mm.invoke("decodeValue", copyVar, resultVar);
        markAllClean(copyVar);
        mm.return_(copyVar);

        // Now implement the exchange bridge method.
        mm = mClassMaker.addMethod
            (Object.class, variant, Transaction.class, Object.class).public_().bridge();
        mm.return_(mm.this_().invoke(returnType, variant, null, mm.param(0), mm.param(1)));
    }

    /**
     * @param variant "store", "exchange", "insert", or "replace"
     */
    private Variable storeNoTrigger(MethodMaker mm, String variant,
                                    Variable txnVar, Variable rowVar,
                                    Variable keyVar, Variable valueVar)
    {
        if (variant == "replace" || !mSupportsIndexLock) {
            return mm.field("mSource").invoke(variant, txnVar, keyVar, valueVar);
        } else {
            // Call protected method inherited from BaseTable.
            return mm.invoke(variant, txnVar, rowVar, keyVar, valueVar);
        }
    }

    private void addStoreAutoMethod() {
        MethodMaker mm = mClassMaker.addMethod(null, "storeAuto", Transaction.class, mRowClass);

        Variable txnVar = mm.param(0);
        Variable rowVar = mm.param(1);

        var keyVar = mm.invoke("encodePrimaryKey", rowVar);
        var valueVar = mm.invoke("encodeValue", rowVar);

        // Call enterScopex because bogus transaction doesn't work with AutomaticKeyGenerator.
        txnVar.set(mm.var(ViewUtils.class).invoke("enterScopex", mm.field("mSource"), txnVar));
        Label txnStart = mm.label().here();

        mm.invoke("redoPredicateMode", txnVar);

        if (!supportsTriggers()) {
            mm.field("autogen").invoke("store", txnVar, rowVar, keyVar, valueVar);
            txnVar.invoke("commit");
            markAllClean(rowVar);
            mm.return_();
        } else {
            var triggerVar = mm.var(Trigger.class);
            Label skipLabel = mm.label();
            prepareForTrigger(mm, mm.this_(), triggerVar, skipLabel);
            Label triggerStart = mm.label().here();
            mm.field("autogen").invoke("store", txnVar, rowVar, keyVar, valueVar);
            triggerVar.invoke("insert", txnVar, rowVar, keyVar, valueVar);
            Label commitLabel = mm.label().goto_();
            skipLabel.here();
            mm.field("autogen").invoke("store", txnVar, rowVar, keyVar, valueVar);
            commitLabel.here();
            txnVar.invoke("commit");
            markAllClean(rowVar);
            mm.return_();
            mm.finally_(triggerStart, () -> triggerVar.invoke("releaseShared"));
        }

        mm.finally_(txnStart, () -> txnVar.invoke("exit"));
    }

    private static void copyFields(MethodMaker mm, Variable src, Variable dst,
                                   Collection<ColumnInfo> infos)
    {
        for (ColumnInfo info : infos) {
            Variable srcField = src.field(info.name);

            if (info.isArray()) {
                srcField = srcField.get();
                Label isNull = null;
                if (info.isNullable()) {
                    isNull = mm.label();
                    srcField.ifEq(null, isNull);
                }
                srcField.set(srcField.invoke("clone").cast(info.type));
                if (isNull != null) {
                    isNull.here();
                }
            }

            dst.field(info.name).set(srcField);
        }
    }

    /**
     * Adds a method which does most of the work for the update and merge methods. The
     * transaction parameter must not be null, which is committed when changes are made.
     *
     *     boolean doUpdate(Transaction txn, ActualRow row, boolean merge);
     */
    private void addDoUpdateMethod() {
        MethodMaker mm = mClassMaker.addMethod
            (boolean.class, "doUpdate", Transaction.class, mRowClass, boolean.class);

        Variable txnVar = mm.param(0);
        Variable rowVar = mm.param(1);
        Variable mergeVar = mm.param(2);

        Label ready = mm.label();
        mm.invoke("checkPrimaryKeySet", rowVar).ifTrue(ready);
        mm.new_(IllegalStateException.class, "Primary key isn't fully specified").throw_();

        ready.here();

        final var keyVar = mm.invoke("encodePrimaryKey", rowVar);
        final var source = mm.field("mSource");
        final var cursorVar = source.invoke("newCursor", txnVar);

        Label cursorStart = mm.label().here();

        // If all value columns are dirty, replace the whole row and commit.
        {
            Label cont;
            if (mCodecGen.info.valueColumns.isEmpty()) {
                // If the checkValueAllDirty method was defined, it would always return true.
                cont = null;
            } else {
                cont = mm.label();
                mm.invoke("checkValueAllDirty", rowVar).ifFalse(cont);
            }

            final Variable triggerVar;
            final Label triggerStart;

            if (!supportsTriggers()) {
                triggerVar = null;
                triggerStart = null;
            } else {
                triggerVar = mm.var(Trigger.class);
                Label skipLabel = mm.label();
                prepareForTrigger(mm, mm.this_(), triggerVar, skipLabel);
                triggerStart = mm.label().here();

                cursorVar.invoke("find", keyVar);
                var oldValueVar = cursorVar.invoke("value");
                Label replace = mm.label();
                oldValueVar.ifNe(null, replace);
                mm.return_(false);
                replace.here();
                var valueVar = mm.invoke("encodeValue", rowVar);
                triggerVar.invoke("store", txnVar, rowVar, keyVar, oldValueVar, valueVar);
                cursorVar.invoke("commit", valueVar);
                markAllClean(rowVar);
                mm.return_(true);

                skipLabel.here();
            }

            cursorVar.invoke("autoload", false);
            cursorVar.invoke("find", keyVar);
            Label replace = mm.label();
            cursorVar.invoke("value").ifNe(null, replace);
            mm.return_(false);
            replace.here();
            cursorVar.invoke("commit", mm.invoke("encodeValue", rowVar));

            if (triggerStart != null) {
                mm.finally_(triggerStart, () -> triggerVar.invoke("releaseShared"));
            }

            markAllClean(rowVar);
            mm.return_(true);

            if (cont == null) {
                return;
            }

            cont.here();
        }

        cursorVar.invoke("find", keyVar);

        Label hasValue = mm.label();
        cursorVar.invoke("value").ifNe(null, hasValue);
        mm.return_(false);
        hasValue.here();

        // The bulk of the method isn't implemented until needed, delaying acquisition/creation
        // of the current schema version.

        var indy = mm.var(TableMaker.class).indy
            ("indyDoUpdate", mStore.ref(), mRowType, mIndexId, supportsTriggers() ? 1 : 0);
        indy.invoke(null, "doUpdate", null, mm.this_(), rowVar, mergeVar, cursorVar);
        mm.return_(true);

        mm.finally_(cursorStart, () -> cursorVar.invoke("reset"));
    }

    /**
     * @param triggers 0 for false, 1 for true
     */
    public static CallSite indyDoUpdate(MethodHandles.Lookup lookup, String name, MethodType mt,
                                        WeakReference<RowStore> storeRef,
                                        Class<?> rowType, long indexId, int triggers)
    {
        return doIndyEncode
            (lookup, name, mt, storeRef, rowType, indexId, (mm, info, schemaVersion) -> {
                finishIndyDoUpdate(mm, info, schemaVersion, triggers);
            });
    }

    /**
     * @param triggers 0 for false, 1 for true
     */
    private static void finishIndyDoUpdate(MethodMaker mm, RowInfo rowInfo, int schemaVersion,
                                           int triggers)
    {
        // All these variables were provided by the indy call in addDoUpdateMethod.
        Variable tableVar = mm.param(0);
        Variable rowVar = mm.param(1);
        Variable mergeVar = mm.param(2);
        Variable cursorVar = mm.param(3);

        Variable valueVar = cursorVar.invoke("value");

        var ue = encodeUpdateEntry(mm, rowInfo, schemaVersion, tableVar, rowVar, valueVar);
        Variable newValueVar = ue.newEntryVar;
        Variable[] offsetVars = ue.offsetVars;

        if (triggers == 0) {
            cursorVar.invoke("commit", newValueVar);
        }

        Label doMerge = mm.label();
        mergeVar.ifTrue(doMerge);

        if (triggers != 0) {
            var triggerVar = mm.var(Trigger.class);
            Label skipLabel = mm.label();
            prepareForTrigger(mm, tableVar, triggerVar, skipLabel);
            Label triggerStart = mm.label().here();

            var txnVar = cursorVar.invoke("link");
            var keyVar = cursorVar.invoke("key");
            triggerVar.invoke("storeP", txnVar, rowVar, keyVar, valueVar, newValueVar);
            cursorVar.invoke("commit", newValueVar);
            Label cont = mm.label().goto_();

            skipLabel.here();

            cursorVar.invoke("commit", newValueVar);

            mm.finally_(triggerStart, () -> triggerVar.invoke("releaseShared"));

            cont.here();
        }

        markAllUndirty(rowVar, rowInfo);
        mm.return_();

        doMerge.here();

        // Decode all the original column values that weren't updated into the row.

        RowGen rowGen = rowInfo.rowGen();
        Map<String, Integer> columnNumbers = rowGen.columnNumbers();
        ColumnCodec[] codecs = ColumnCodec.bind(rowGen.valueCodecs(), mm);

        String stateFieldName = null;
        Variable stateField = null;

        for (int i=0; i<codecs.length; i++) {
            ColumnCodec codec = codecs[i];
            ColumnInfo info = codec.mInfo;
            int num = columnNumbers.get(info.name);

            String sfName = rowGen.stateField(num);
            if (!sfName.equals(stateFieldName)) {
                stateFieldName = sfName;
                stateField = rowVar.field(stateFieldName).get();
            }

            int sfMask = RowGen.stateFieldMask(num);
            Label cont = mm.label();
            stateField.and(sfMask).ifEq(sfMask, cont);

            codec.decode(rowVar.field(info.name), valueVar, offsetVars[i], null);

            cont.here();
        }

        if (triggers != 0) {
            var triggerVar = mm.var(Trigger.class);
            Label skipLabel = mm.label();
            prepareForTrigger(mm, tableVar, triggerVar, skipLabel);
            Label triggerStart = mm.label().here();

            var txnVar = cursorVar.invoke("link");
            var keyVar = cursorVar.invoke("key");
            triggerVar.invoke("store", txnVar, rowVar, keyVar, valueVar, newValueVar);
            cursorVar.invoke("commit", newValueVar);
            Label cont = mm.label().goto_();

            skipLabel.here();

            cursorVar.invoke("commit", newValueVar);

            mm.finally_(triggerStart, () -> triggerVar.invoke("releaseShared"));

            cont.here();
        }

        markAllClean(rowVar, rowGen, rowGen);
    }

    private static class UpdateEntry {
        Variable newEntryVar;
        Variable[] offsetVars;
    }

    /**
     * Makes code which encodes a new entry (a key or value) by comparing dirty row columns to
     * the original entry. Returns the new entry and the column offsets in the original entry.
     *
     * @param schemaVersion pass 0 if entry is a key instead of a value; implies that caller
     * must handle the case where the value must be empty
     * @param rowVar non-null
     * @param tableVar doesn't need to be initialized (is used to invoke a static method)
     * @param originalVar original non-null encoded key or value
     */
    private static UpdateEntry encodeUpdateEntry
        (MethodMaker mm, RowInfo rowInfo, int schemaVersion,
         Variable tableVar, Variable rowVar, Variable originalVar)
    {
        RowGen rowGen = rowInfo.rowGen();
        ColumnCodec[] codecs;
        int fixedOffset;

        if (schemaVersion == 0) {
            codecs = rowGen.keyCodecs();
            fixedOffset = 0;
        } else {
            codecs = rowGen.valueCodecs();

            Variable decodeVersion = mm.var(RowUtils.class)
                .invoke("decodeSchemaVersion", originalVar);
            Label sameVersion = mm.label();
            decodeVersion.ifEq(schemaVersion, sameVersion);

            // If different schema versions, decode and re-encode a new entry, and then go to
            // the next step. The simplest way to perform this conversion is to create a new
            // temp row object, decode the entry into it, and then create a new entry from it.
            var tempRowVar = mm.new_(rowVar);
            tableVar.invoke("decodeValue", tempRowVar, originalVar);
            originalVar.set(tableVar.invoke("encodeValue", tempRowVar));

            sameVersion.here();

            fixedOffset = schemaVersion < 128 ? 1 : 4;
        }

        // Identify the offsets to all the columns in the original entry, and calculate the
        // size of the new entry.

        Map<String, Integer> columnNumbers = rowGen.columnNumbers();
        codecs = ColumnCodec.bind(codecs, mm);

        Variable[] offsetVars = new Variable[codecs.length];

        var offsetVar = mm.var(int.class).set(fixedOffset);
        var newSizeVar = mm.var(int.class).set(fixedOffset); // need room for schemaVersion

        String stateFieldName = null;
        Variable stateField = null;

        for (int i=0; i<codecs.length; i++) {
            ColumnCodec codec = codecs[i];
            codec.encodePrepare();

            offsetVars[i] = offsetVar.get();
            codec.decodeSkip(originalVar, offsetVar, null);

            ColumnInfo info = codec.mInfo;
            int num = columnNumbers.get(info.name);

            String sfName = rowGen.stateField(num);
            if (!sfName.equals(stateFieldName)) {
                stateFieldName = sfName;
                stateField = rowVar.field(stateFieldName).get();
            }

            int sfMask = RowGen.stateFieldMask(num);
            Label isDirty = mm.label();
            stateField.and(sfMask).ifEq(sfMask, isDirty);

            // Add in the size of original column, which won't be updated.
            codec.encodeSkip();
            newSizeVar.inc(offsetVar.sub(offsetVars[i]));
            Label cont = mm.label().goto_();

            // Add in the size of the dirty column, which needs to be encoded.
            isDirty.here();
            newSizeVar.inc(codec.minSize());
            codec.encodeSize(rowVar.field(info.name), newSizeVar);

            cont.here();
        }

        // Encode the new byte[] entry...

        var newEntryVar = mm.new_(byte[].class, newSizeVar);

        var srcOffsetVar = mm.var(int.class).set(0);
        var dstOffsetVar = mm.var(int.class).set(0);
        var spanLengthVar = mm.var(int.class).set(fixedOffset);
        var sysVar = mm.var(System.class);

        for (int i=0; i<codecs.length; i++) {
            ColumnCodec codec = codecs[i];
            ColumnInfo info = codec.mInfo;
            int num = columnNumbers.get(info.name);

            Variable columnLenVar;
            {
                Variable endVar;
                if (i + 1 < codecs.length) {
                    endVar = offsetVars[i + 1];
                } else {
                    endVar = originalVar.alength();
                }
                columnLenVar = endVar.sub(offsetVars[i]);
            }

            int sfMask = RowGen.stateFieldMask(num);
            Label isDirty = mm.label();
            stateField.and(sfMask).ifEq(sfMask, isDirty);

            // Increase the copy span length.
            Label cont = mm.label();
            spanLengthVar.inc(columnLenVar);
            mm.goto_(cont);

            isDirty.here();

            // Copy the current span and prepare for the next span.
            {
                Label noSpan = mm.label();
                spanLengthVar.ifEq(0, noSpan);
                sysVar.invoke("arraycopy", originalVar, srcOffsetVar,
                              newEntryVar, dstOffsetVar, spanLengthVar);
                srcOffsetVar.inc(spanLengthVar);
                dstOffsetVar.inc(spanLengthVar);
                spanLengthVar.set(0);
                noSpan.here();
            }

            // Encode the dirty column, and skip over the original column value.
            codec.encode(rowVar.field(info.name), newEntryVar, dstOffsetVar);
            srcOffsetVar.inc(columnLenVar);

            cont.here();
        }

        // Copy any remaining span.
        {
            Label noSpan = mm.label();
            spanLengthVar.ifEq(0, noSpan);
            sysVar.invoke("arraycopy", originalVar, srcOffsetVar,
                          newEntryVar, dstOffsetVar, spanLengthVar);
            noSpan.here();
        }

        var ue = new UpdateEntry();
        ue.newEntryVar = newEntryVar;
        ue.offsetVars = offsetVars;
        return ue;
    }

    /**
     * Delegates to the doUpdate method.
     */
    private void addUpdateMethod(String variant, boolean merge) {
        MethodMaker mm = mClassMaker.addMethod
            (boolean.class, variant, Transaction.class, Object.class).public_();
        Variable txnVar = mm.param(0);
        Variable rowVar = mm.param(1).cast(mRowClass);
        Variable source = mm.field("mSource");
        txnVar.set(mm.var(ViewUtils.class).invoke("enterScope", source, txnVar));
        Label tryStart = mm.label().here();
        mm.invoke("redoPredicateMode", txnVar);
        mm.return_(mm.invoke("doUpdate", txnVar, rowVar, merge));
        mm.finally_(tryStart, () -> txnVar.invoke("exit"));
    }

    /**
     * Makes code which obtains the current trigger and acquires the lock which must be held
     * for the duration of the operation. The lock must be held even if no trigger must be run.
     *
     * @param triggerVar type is Trigger and is assigned by the generated code
     * @param skipLabel label to branch when trigger shouldn't run
     */
    private static void prepareForTrigger(MethodMaker mm, Variable tableVar,
                                          Variable triggerVar, Label skipLabel)
    {
        Label acquireTriggerLabel = mm.label().here();
        triggerVar.set(tableVar.invoke("trigger"));
        triggerVar.invoke("acquireShared");
        var modeVar = triggerVar.invoke("mode");
        modeVar.ifEq(Trigger.SKIP, skipLabel);
        Label activeLabel = mm.label();
        modeVar.ifNe(Trigger.DISABLED, activeLabel);
        triggerVar.invoke("releaseShared");
        mm.goto_(acquireTriggerLabel);
        activeLabel.here();
    }

    private void markAllClean(Variable rowVar) {
        markAllClean(rowVar, mRowGen, mCodecGen);
    }

    private static void markAllClean(Variable rowVar, RowGen rowGen, RowGen codecGen) {
        if (rowGen == codecGen) { // isPrimaryTable, so truly mark all clean
            int mask = 0x5555_5555;
            int i = 0;
            String[] stateFields = rowGen.stateFields();
            for (; i < stateFields.length - 1; i++) {
                rowVar.field(stateFields[i]).set(mask);
            }
            mask >>>= (32 - ((rowGen.info.allColumns.size() & 0b1111) << 1));
            rowVar.field(stateFields[i]).set(mask);
        } else {
            // Only mark columns clean that are defined by codecGen. All others are unset.
            markClean(rowVar, rowGen, codecGen.info.allColumns);
        }
    }

    /**
     * Mark only the given columns as CLEAN. All others are UNSET.
     */
    private static void markClean(final Variable rowVar, final RowGen rowGen,
                                  final Map<String, ColumnInfo> columns)
    {
        final int maxNum = rowGen.info.allColumns.size();

        int num = 0, mask = 0;

        for (int step = 0; step < 2; step++) {
            // Key columns are numbered before value columns. Add checks in two steps.
            // Note that the codecs are accessed, to match encoding order.
            var baseCodecs = step == 0 ? rowGen.keyCodecs() : rowGen.valueCodecs();

            for (ColumnCodec codec : baseCodecs) {
                if (columns.containsKey(codec.mInfo.name)) {
                    mask |= RowGen.stateFieldMask(num, 0b01); // clean state
                }
                if ((++num & 0b1111) == 0 || num >= maxNum) {
                    rowVar.field(rowGen.stateField(num - 1)).set(mask);
                    mask = 0;
                }
            }
        }
    }

    private void addMarkAllCleanMethod() {
        // Used by filter implementations, and it must be public because filters are defined in
        // a different package.
        MethodMaker mm = mClassMaker.addMethod(null, "markAllClean", mRowClass).public_().static_();
        markAllClean(mm.param(0));
    }

    /**
     * Remaining states are UNSET or CLEAN.
     */
    private static void markAllUndirty(Variable rowVar, RowInfo info) {
        int mask = 0x5555_5555;
        int i = 0;
        String[] stateFields = info.rowGen().stateFields();
        for (; i < stateFields.length - 1; i++) {
            var field = rowVar.field(stateFields[i]);
            field.set(field.and(mask));
        }
        mask >>>= (32 - ((info.allColumns.size() & 0b1111) << 1));
        var field = rowVar.field(stateFields[i]);
        field.set(field.and(mask));
    }

    /**
     * Mark all the value columns as UNSET without modifying the key column states.
     */
    private void markValuesUnset(Variable rowVar) {
        if (isPrimaryTable()) {
            // Clear the value column state fields. Skip the key columns, which are numbered
            // first. Note that the codecs are accessed, to match encoding order.
            int num = mRowInfo.keyColumns.size();
            int mask = 0;
            for (ColumnCodec codec : mRowGen.valueCodecs()) {
                mask |= RowGen.stateFieldMask(num);
                if (isMaskReady(++num, mask)) {
                    mask = maskRemainder(num, mask);
                    Field field = stateField(rowVar, num - 1);
                    mask = ~mask;
                    if (mask == 0) {
                        field.set(mask);
                    } else {
                        field.set(field.and(mask));
                        mask = 0;
                    }
                }
            }
            return;
        }

        final Map<String, ColumnInfo> keyColumns = mCodecGen.info.keyColumns;
        final int maxNum = mRowInfo.allColumns.size();

        int num = 0, mask = 0;

        for (int step = 0; step < 2; step++) {
            // Key columns are numbered before value columns. Add checks in two steps.
            // Note that the codecs are accessed, to match encoding order.
            var baseCodecs = step == 0 ? mRowGen.keyCodecs() : mRowGen.valueCodecs();

            for (ColumnCodec codec : baseCodecs) {
                if (!keyColumns.containsKey(codec.mInfo.name)) {
                    mask |= RowGen.stateFieldMask(num);
                }
                if ((++num & 0b1111) == 0 || num >= maxNum) {
                    Field field = rowVar.field(mRowGen.stateField(num - 1));
                    mask = ~mask;
                    if (mask == 0) {
                        field.set(mask);
                    } else {
                        field.set(field.and(mask));
                        mask = 0;
                    }
                }
            }
        }
    }

    private Field stateField(Variable rowVar, int columnNum) {
        return rowVar.field(mRowGen.stateField(columnNum));
    }

    private void addToRowMethod() {
        MethodMaker mm = mClassMaker.addMethod(mRowType, "toRow", byte[].class).protected_();
        var rowVar = mm.new_(mRowClass);
        mm.invoke("decodePrimaryKey", rowVar, mm.param(0));
        markClean(rowVar, mRowGen, mCodecGen.info.keyColumns);
        mm.return_(rowVar);

        mm = mClassMaker.addMethod(Object.class, "toRow", byte[].class).protected_().bridge();
        mm.return_(mm.this_().invoke(mRowType, "toRow", null, mm.param(0)));
    }

    private void addRowStoreRefMethod() {
        MethodMaker mm = mClassMaker.addMethod(WeakReference.class, "rowStoreRef").protected_();
        mm.return_(mm.var(WeakReference.class).setExact(mStore.ref()));
    }

    private void addSecondaryDescriptorMethod() {
        MethodMaker mm = mClassMaker.addMethod(byte[].class, "secondaryDescriptor").protected_();
        mm.return_(mm.var(byte[].class).setExact(mSecondaryDescriptor));
    }

    /**
     * Defines methods which return a SingleScanController instance.
     */
    private void addUnfilteredMethods() {
        MethodMaker mm = mClassMaker.addMethod
            (SingleScanController.class, "unfiltered").protected_();
        var condyClass = mm.var(TableMaker.class).condy
            ("condyDefineUnfiltered", mRowType, mRowClass, mSecondaryDescriptor);
        var scanControllerCtor = condyClass.invoke(MethodHandle.class, "unfiltered");
        Class<?>[] paramTypes = {
            byte[].class, boolean.class, byte[].class, boolean.class, boolean.class
        };
        mm.return_(scanControllerCtor.invoke
                   (SingleScanController.class, "invokeExact", paramTypes,
                    null, false, null, false, false));

        mm = mClassMaker.addMethod(SingleScanController.class, "unfilteredReverse").protected_();
        // Use the same scanControllerCtor constant, but must set against the correct maker.
        scanControllerCtor = mm.var(MethodHandle.class).set(scanControllerCtor);
        mm.return_(scanControllerCtor.invoke
                   (SingleScanController.class, "invokeExact", paramTypes,
                    null, false, null, false, true));
    }

    /**
     * Makes a subclass of SingleScanController with matching constructors.
     *
     * @param secondaryDesc pass null for primary table
     * @return the basic constructor handle
     */
    public static MethodHandle condyDefineUnfiltered
        (MethodHandles.Lookup lookup, String name, Class type,
         Class rowType, Class rowClass, byte[] secondaryDesc)
        throws Throwable
    {
        RowInfo rowInfo = RowInfo.find(rowType);
        RowGen rowGen = rowInfo.rowGen();
        RowGen codecGen = rowGen;

        if (secondaryDesc != null) {
            codecGen = RowStore.indexRowInfo(rowInfo, secondaryDesc).rowGen();
        }

        ClassMaker cm = RowGen.beginClassMaker(TableMaker.class, rowType, rowInfo, null, name)
            .extend(SingleScanController.class).public_();

        // Constructors are protected, for use by filter implementation subclasses.
        final MethodType ctorType;
        {
            ctorType = MethodType.methodType(void.class, byte[].class, boolean.class,
                                             byte[].class, boolean.class, boolean.class);
            MethodMaker mm = cm.addConstructor(ctorType).protected_();
            mm.invokeSuperConstructor
                (mm.param(0), mm.param(1), mm.param(2), mm.param(3), mm.param(4));

            // Define a reverse scan copy constructor.
            mm = cm.addConstructor(SingleScanController.class).protected_();
            mm.invokeSuperConstructor(mm.param(0));
        }

        {
            // Specified by RowEvaluator.
            MethodMaker mm = cm.addMethod
                (Object.class, "decodeRow", Cursor.class, LockResult.class, Object.class).public_();
            var tableVar = mm.var(lookup.lookupClass());
            var rowVar = mm.param(2).cast(rowClass);
            Label hasRow = mm.label();
            rowVar.ifNe(null, hasRow);
            rowVar.set(mm.new_(rowClass));
            hasRow.here();
            var cursorVar = mm.param(0);
            tableVar.invoke("decodePrimaryKey", rowVar, cursorVar.invoke("key"));
            tableVar.invoke("decodeValue", rowVar, cursorVar.invoke("value"));
            markAllClean(rowVar, rowGen, codecGen);
            mm.return_(rowVar);
        }

        {
            // Specified by RowEvaluator.
            MethodMaker mm = cm.addMethod
                (byte[].class, "updateKey", Object.class, byte[].class).public_();
            var rowVar = mm.param(0).cast(rowClass);
            var tableVar = mm.var(lookup.lookupClass());
            Label unchanged = mm.label();
            tableVar.invoke("checkPrimaryKeyAnyDirty", rowVar).ifFalse(unchanged);
            mm.return_(tableVar.invoke("updatePrimaryKey", rowVar, mm.param(1)));
            unchanged.here();
            mm.return_(null);
        }

        {
            // Specified by RowEvaluator.
            MethodMaker mm = cm.addMethod
                (byte[].class, "updateValue", Object.class, byte[].class).public_();
            var rowVar = mm.param(0).cast(rowClass);
            var tableVar = mm.var(lookup.lookupClass());
            mm.return_(tableVar.invoke("updateValue", rowVar, mm.param(1)));
        }

        if (rowGen == codecGen) { // isPrimaryTable, so a schema must be decoded
            // Used by filter subclasses. The int param is the schema version.
            MethodMaker mm = cm.addMethod
                (MethodHandle.class, "decodeValueHandle", int.class).protected_().static_();
            var tableVar = mm.var(lookup.lookupClass());
            mm.return_(tableVar.invoke("decodeValueHandle", mm.param(0)));
        }

        lookup = cm.finishLookup();

        MethodHandle mh = lookup.findConstructor(lookup.lookupClass(), ctorType);

        return mh.asType(mh.type().changeReturnType(SingleScanController.class));
    }

    /**
     * @param option bit 1: reverse, bit 2: joined
     */
    private void addPlanMethod(int option) {
        String name = "plan";
        if ((option & 0b01) != 0) {
            name += "Reverse";
        }
        MethodMaker mm = mClassMaker.addMethod
            (QueryPlan.class, name, Object[].class).varargs().public_();
        var condy = mm.var(TableMaker.class).condy
            ("condyPlan", mRowType, mSecondaryDescriptor, option);
        mm.return_(condy.invoke(QueryPlan.class, "plan"));
    }

    /**
     * @param option bit 1: reverse, bit 2: joined
     */
    public static QueryPlan condyPlan(MethodHandles.Lookup lookup, String name, Class type,
                                      Class rowType, byte[] secondaryDesc, int option)
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

        boolean reverse = (option & 0b01) != 0;
        QueryPlan plan = new QueryPlan.FullScan(rowInfo.name, which, rowInfo.keySpec(), reverse);
    
        if ((option & 0b10) != 0) {
            rowInfo = primaryRowInfo;
            plan = new QueryPlan.NaturalJoin(rowInfo.name, "primary key", rowInfo.keySpec(), plan);
        }

        return plan;
    }

    /**
     * Define a static method which encodes a primary key when given an encoded secondary key.
     * When a row parameter is defined, all key columns must be set.
     *
     *  000:  byte[] toPrimaryKey()               not used / illegal
     *  001:  byte[] toPrimaryKey(byte[] value)   not used
     *  010:  byte[] toPrimaryKey(byte[] key)
     *  011:  byte[] toPrimaryKey(byte[] key, byte[] value)
     *  100:  byte[] toPrimaryKey(Row row)
     *  101:  byte[] toPrimaryKey(Row row, byte[] value)
     *  110:  byte[] toPrimaryKey(Row row, byte[] key)
     *  111:  byte[] toPrimaryKey(Row row, byte[] key, byte[] value)
     *
     * @param options bit 2: pass a row with a fully specified secondary key, bit 1: pass an
     * encoded key, bit 0: pass an encoded value
     */
    private void addToPrimaryKeyMethod(ClassMaker cm, int options) {
        var paramList = new ArrayList<Object>(3);
        if ((options & 0b100) != 0) {
            paramList.add(mRowClass); // row
        }
        if ((options & 0b010) != 0) {
            paramList.add(byte[].class); // key
        }
        if ((options & 0b001) != 0) {
            paramList.add(byte[].class); // value
        }

        Object[] params = paramList.toArray();

        MethodMaker mm = cm.addMethod(byte[].class, "toPrimaryKey", params).static_();

        var paramVars = new Object[params.length];
        for (int i=0; i<params.length; i++) {
            paramVars[i] = mm.param(i);
        }

        // Use indy to create on demand, reducing code bloat.
        var indy = mm.var(TableMaker.class).indy
            ("toPrimaryKey", mRowType, mSecondaryDescriptor, options);
        mm.return_(indy.invoke(byte[].class, "toPrimaryKey", null, paramVars));
    }

    public static CallSite toPrimaryKey(MethodHandles.Lookup lookup, String name, MethodType mt,
                                        Class<?> rowType, byte[] secondaryDesc, int options)
    {
        RowInfo primaryInfo = RowInfo.find(rowType);
        RowInfo secondaryInfo = RowStore.indexRowInfo(primaryInfo, secondaryDesc);

        MethodMaker mm = MethodMaker.begin(lookup, name, mt);

        Variable rowVar = null, keyVar = null, valueVar = null;
        int offset = 0;
        if ((options & 0b100) != 0) {
            rowVar = mm.param(offset++);
        }
        if ((options & 0b010) != 0) {
            keyVar = mm.param(offset++);
        }
        if ((options & 0b001) != 0) {
            valueVar = mm.param(offset++);
        }

        Map<String, TransformMaker.Availability> available = null;

        if (rowVar != null) {
            available = new HashMap<>();
            for (String colName : secondaryInfo.keyColumns.keySet()) {
                available.put(colName, TransformMaker.Availability.ALWAYS);
            }
        }

        var tm = new TransformMaker<>(rowType, secondaryInfo, available);
        tm.addKeyTarget(primaryInfo, 0, true);
        tm.begin(mm, rowVar, keyVar, valueVar, 0);
        mm.return_(tm.encode(0));

        return new ConstantCallSite(mm.finish());
    }

    /**
     * Defines methods which return a JoinedScanController instance.
     */
    private void addJoinedUnfilteredMethods(Class<?> primaryTableClass) {
        MethodMaker mm = mClassMaker.addMethod
            (SingleScanController.class, "unfiltered").protected_();
        var condyClass = mm.var(TableMaker.class).condy
            ("condyDefineJoinedUnfiltered", mRowType, mRowClass,
             mSecondaryDescriptor, mm.class_(), primaryTableClass);
        var scanControllerCtor = condyClass.invoke(MethodHandle.class, "unfiltered");
        Class<?>[] paramTypes = {
            byte[].class, boolean.class, byte[].class, boolean.class, boolean.class, Index.class
        };
        mm.return_(scanControllerCtor.invoke
                   (JoinedScanController.class, "invokeExact", paramTypes,
                    null, false, null, false, false, mm.field("primaryIndex")));

        mm = mClassMaker.addMethod(SingleScanController.class, "unfilteredReverse").protected_();
        // Use the same scanControllerCtor constant, but must set against the correct maker.
        scanControllerCtor = mm.var(MethodHandle.class).set(scanControllerCtor);
        mm.return_(scanControllerCtor.invoke
                   (JoinedScanController.class, "invokeExact", paramTypes,
                    null, false, null, false, true, mm.field("primaryIndex")));
    }

    /**
     * Makes a subclass of JoinedScanController with matching constructors.
     *
     * @return the basic constructor handle
     */
    public static MethodHandle condyDefineJoinedUnfiltered
        (MethodHandles.Lookup lookup, String name, Class type,
         Class rowType, Class rowClass, byte[] secondaryDesc,
         Class<?> tableClass, Class<?> primaryTableClass)
        throws Throwable
    {
        RowInfo rowInfo = RowInfo.find(rowType);
        RowGen rowGen = rowInfo.rowGen();
        RowGen codecGen = RowStore.indexRowInfo(rowInfo, secondaryDesc).rowGen();

        ClassMaker cm = RowGen.beginClassMaker
            (TableMaker.class, rowType, rowInfo, null, name)
            .extend(JoinedScanController.class).public_();

        // Constructors are protected, for use by filter implementation subclasses.
        final MethodType ctorType;
        {
            ctorType = MethodType.methodType
                (void.class, byte[].class, boolean.class, byte[].class, boolean.class,
                 boolean.class, Index.class);
            MethodMaker mm = cm.addConstructor(ctorType).protected_();
            mm.invokeSuperConstructor
                (mm.param(0), mm.param(1), mm.param(2), mm.param(3), mm.param(4), mm.param(5));

            // Define a reverse scan copy constructor.
            mm = cm.addConstructor(JoinedScanController.class).protected_();
            mm.invokeSuperConstructor(mm.param(0));
        }

        // Provide access to the toPrimaryKey method to be accessible by filter implementation
        // subclasses, which are defined in a different package.
        if (codecGen.info.isAltKey()) {
            MethodMaker mm = cm.addMethod(byte[].class, "toPrimaryKey", byte[].class, byte[].class);
            mm.static_().protected_();
            mm.return_(mm.var(tableClass).invoke("toPrimaryKey", mm.param(0), mm.param(1)));
        } else {
            MethodMaker mm = cm.addMethod(byte[].class, "toPrimaryKey", byte[].class);
            mm.static_().protected_();
            mm.return_(mm.var(tableClass).invoke("toPrimaryKey", mm.param(0)));
        }

        // Note regarding the RowEvaluator methods: The decode methods fully resolve rows
        // by joining to the primary table, and the encode methods return bytes for storing
        // into the primary table.

        // Specified by RowEvaluator.
        addJoinedDecodeRow(cm, codecGen, rowClass, tableClass, primaryTableClass, false);
        addJoinedDecodeRow(cm, codecGen, rowClass, tableClass, primaryTableClass, true);

        {
            // Specified by RowEvaluator.
            MethodMaker mm = cm.addMethod
                (byte[].class, "updateKey", Object.class, byte[].class).public_();
            var rowVar = mm.param(0).cast(rowClass);
            var tableVar = mm.var(primaryTableClass);
            mm.return_(tableVar.invoke("updatePrimaryKey", rowVar, mm.param(1)));
        }

        {
            // Specified by RowEvaluator.
            MethodMaker mm = cm.addMethod
                (byte[].class, "updateValue", Object.class, byte[].class).public_();
            var rowVar = mm.param(0).cast(rowClass);
            var tableVar = mm.var(primaryTableClass);
            mm.return_(tableVar.invoke("updateValue", rowVar, mm.param(1)));
        }

        {
            // Used by filter subclasses when joining to the primary. The int param is the
            // schema version.
            MethodMaker mm = cm.addMethod
                (MethodHandle.class, "decodeValueHandle", int.class).protected_().static_();
            var tableVar = mm.var(primaryTableClass);
            mm.return_(tableVar.invoke("decodeValueHandle", mm.param(0)));
        }

        lookup = cm.finishLookup();

        MethodHandle mh = lookup.findConstructor(lookup.lookupClass(), ctorType);

        return mh.asType(mh.type().changeReturnType(JoinedScanController.class));
    }

    private static void addJoinedDecodeRow
        (ClassMaker cm, RowGen codecGen, Class rowClass, 
         Class<?> tableClass, Class<?> primaryTableClass, boolean withPrimaryCursor)
    {
        Object[] params;
        if (!withPrimaryCursor) {
            params = new Object[] {Cursor.class, LockResult.class, Object.class};
        } else {
            params = new Object[] {Cursor.class, LockResult.class, Object.class, Cursor.class};
        }

        MethodMaker mm = cm.addMethod(Object.class, "decodeRow", params).public_();

        var cursorVar = mm.param(0);
        var resultVar = mm.param(1);
        var keyVar = cursorVar.invoke("key");

        Variable primaryKeyVar;
        {
            var tableVar = mm.var(tableClass);
            if (codecGen.info.isAltKey()) {
                var valueVar = cursorVar.invoke("value");
                primaryKeyVar = tableVar.invoke("toPrimaryKey", keyVar, valueVar);
            } else {
                primaryKeyVar = tableVar.invoke("toPrimaryKey", keyVar);
            }
        }

        if (!withPrimaryCursor) {
            params = new Object[] {cursorVar, resultVar, primaryKeyVar};
        } else {
            params = new Object[] {cursorVar, resultVar, primaryKeyVar, mm.param(3)};
        }

        var primaryValueVar = mm.invoke("join", params);

        Label hasValue = mm.label();
        primaryValueVar.ifNe(null, hasValue);
        mm.return_(null);
        hasValue.here();

        var rowVar = mm.param(2).cast(rowClass);
        Label hasRow = mm.label();
        rowVar.ifNe(null, hasRow);
        rowVar.set(mm.new_(rowClass));
        hasRow.here();

        var tableVar = mm.var(primaryTableClass);
        tableVar.invoke("decodePrimaryKey", rowVar, primaryKeyVar);
        tableVar.invoke("decodeValue", rowVar, primaryValueVar);
        tableVar.invoke("markAllClean", rowVar);
        mm.return_(rowVar);
    }
}
