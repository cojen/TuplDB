/*
 *  Copyright (C) 2022 Cojen.org
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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

import java.util.Map;
import java.util.TreeMap;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.Index;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UnmodifiableViewException;

import org.cojen.tupl.core.RowPredicateLock;
import org.cojen.tupl.core.TupleKey;

import org.cojen.tupl.table.codec.ColumnCodec;

import org.cojen.tupl.views.ViewUtils;

/**
 * Makes Table classes that extend StoredTable for primary tables and for unjoined secondary
 * index tables. The classes are "static" in that they have no dynamic code generation which
 * depends on RowStore and tableId constants. Without dynamic code generation, schema evolution
 * isn't supported.
 *
 * @author Brian S O'Neill
 */
class StaticTableMaker extends TableMaker {
    private static final WeakCache<Object, Class<?>, RowGen> cCache;

    static {
        cCache = new WeakCache<>() {
            @Override
            protected Class<?> newValue(Object key, RowGen rowGen) {
                if (key instanceof Class type) {
                    return new StaticTableMaker(type, rowGen).finish();
                } else {
                    var tuple = (TupleKey) key;
                    var type = (Class) tuple.get(0);
                    var secondaryDesc = (byte[]) tuple.get(1);

                    RowInfo indexRowInfo = RowStore.secondaryRowInfo(rowGen.info, secondaryDesc);

                    // Indexes don't have indexes.
                    indexRowInfo.alternateKeys = EmptyNavigableSet.the();
                    indexRowInfo.secondaryIndexes = EmptyNavigableSet.the();
        
                    RowGen codecGen = indexRowInfo.rowGen();

                    return new StaticTableMaker(type, rowGen, codecGen, secondaryDesc).finish();
                }
            }
        };
    }

    /**
     * Obtains an abstract primary table class, which extends StoredTable, and has one constructor:
     *
     *    (TableManager, Index, RowPredicateLock)
     *
     * @param rowGen describes row encoding
     */
    static Class<?> obtain(Class<?> type, RowGen rowGen) {
        return cCache.obtain(type, rowGen);
    }

    /**
     * Obtains an abstract unjoined secondary index table class, which extends StoredTableIndex,
     * and has one constructor:
     *
     *    (TableManager, Index, RowPredicateLock)
     *
     * @param rowGen describes row encoding
     * @param secondaryDesc secondary index descriptor
     */
    static Class<?> obtain(Class<?> type, RowGen rowGen, byte[] secondaryDesc) {
        return cCache.obtain(TupleKey.make.with(type, secondaryDesc), rowGen);
    }

    private final ColumnInfo mAutoColumn;

    /**
     * Constructor for making a primary table.
     *
     * @param rowGen describes row encoding
     */
    private StaticTableMaker(Class<?> type, RowGen rowGen) {
        this(type, rowGen, rowGen, null);
    }

    /**
     * Constructor for making an unjoined secondary index table.
     *
     * @param rowGen describes row encoding
     * @param codecGen describes key and value codecs (different than rowGen)
     * @param secondaryDesc secondary index descriptor
     */
    private StaticTableMaker(Class<?> type, RowGen rowGen, RowGen codecGen, byte[] secondaryDesc) {
        super(type, rowGen, codecGen, secondaryDesc);

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
     * The generated class is a StoredTable or StoredTableIndex, and the constructor params are:
     *
     *    (TableManager, Index, RowPredicateLock)
     */
    Class<?> finish() {
        {
            String suffix;
            Class baseClass;

            if (isPrimaryTable()) {
                suffix = "table";
                baseClass = StoredTable.class;
            } else {
                suffix = "unjoined";
                baseClass = StoredTableIndex.class;
            }

            mClassMaker = mCodecGen.beginClassMaker(getClass(), mRowType, suffix)
                .public_().extend(baseClass).implement(TableBasicsMaker.find(mRowType));

            if (isEvolvable()) {
                mClassMaker.abstract_();
            }
        }

        MethodMaker ctor = mClassMaker.addConstructor
            (TableManager.class, Index.class, RowPredicateLock.class);
        ctor.invokeSuperConstructor(ctor.param(0), ctor.param(1), ctor.param(2));

        if (!isEvolvable()) {
            // Needs to be public to be constructed directly by RowStore.makeTable.
            ctor.public_();
        }

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
        encodeDecode: {
            ColumnCodec[] keyCodecs = mCodecGen.keyCodecs();
            addEncodeColumnsMethod("encodePrimaryKey", keyCodecs);
            addDecodeColumnsMethod("decodePrimaryKey", keyCodecs);

            addUpdatePrimaryKeyMethod();

            addDecodePartialHandle();

            // Define encode/decode delegation methods.
            MethodMaker doEncode = mClassMaker.addMethod
                (byte[].class, "doEncodeValue", mRowClass).protected_();
            MethodMaker doDecode = mClassMaker.addMethod
                (null, "doDecodeValue", mRowClass, byte[].class).protected_();

            if (isEvolvable()) {
                // Subclasses will need to implement these, since they might depend on dynamic
                // code generation.
                doEncode.abstract_();
                doDecode.abstract_();
                break encodeDecode;
            }

            // Unevolvable, so implement concrete methods.

            if (isPrimaryTable()) {
                addEncodeColumnsMethod("encodeValue", mCodecGen.valueCodecs());
                addUpdateValueMethod(0); // no schema version
            } else {
                // The encodeValue and updateValue methods are only used for storing rows into
                // the table. By making them always fail, there's no backdoor to permit
                // modifications.
                mClassMaker.addMethod(byte[].class, "encodeValue", mRowClass)
                    .static_().new_(UnmodifiableViewException.class).throw_();
                mClassMaker.addMethod(byte[].class, "updateValue", mRowClass, byte[].class)
                    .static_().new_(UnmodifiableViewException.class).throw_();
            }

            addDecodeColumnsMethod("decodeValue", mCodecGen.valueCodecs());

            doEncode.return_(doEncode.invoke("encodeValue", doEncode.param(0)));
            doDecode.invoke("decodeValue", doDecode.param(0), doDecode.param(1));
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

            mClassMaker.addField(autoGenClass, "autogen").final_();

            ctor.field("autogen").set
                (ctor.new_(autoGenClass, ctor.param(1), minVal, maxVal, ctor.this_()));

            {
                MethodMaker mm = mClassMaker.addMethod
                    (RowPredicateLock.Closer.class, "applyToRow",
                     Transaction.class, Object.class, mAutoColumn.type);
                mm.public_();
                var rowVar = mm.param(1).cast(mRowClass);
                rowVar.field(mAutoColumn.name).set(mm.param(2));
                mm.return_(mm.field("mIndexLock").invoke
                           ("tryOpenAcquire", mm.param(0), rowVar, null, null));
            }

            {
                MethodMaker mm = mClassMaker.addMethod
                    (RowPredicateLock.Closer.class, "tryOpenAcquire",
                     Transaction.class, byte[].class, byte[].class);
                mm.public_();
                mm.return_(mm.field("mIndexLock").invoke
                           ("tryOpenAcquire", mm.param(0), null, mm.param(1), mm.param(2)));
            }

            var allButAuto = new TreeMap<>(mCodecGen.info.allColumns);
            allButAuto.remove(mAutoColumn.name);
            addCheckSet("checkAllButAutoSet", allButAuto);

            addStoreAutoMethod();
        }

        // Add the public load/store methods, etc.

        addByKeyMethod("tryLoad");
        addByKeyMethod("exists");

        if (isPrimaryTable()) {
            addByKeyMethod("tryDelete");

            addStoreMethod("store", null);
            addStoreMethod("exchange", mRowType);
            addStoreMethod("tryInsert", boolean.class);
            addStoreMethod("tryReplace", boolean.class);

            // Add a method called by the update methods. The implementation might depend on
            // dynamic code generation.
            MethodMaker mm = mClassMaker.addMethod
                (boolean.class, "doUpdate", Transaction.class, mRowClass, boolean.class)
                .protected_();

            if (isEvolvable()) {
                mm.abstract_();
            } else {
                addDoUpdateMethod(mm);
            }

            addUpdateMethod("tryUpdate", false);
            addUpdateMethod("tryMerge", true);
        }

        addMarkAllCleanMethod();
        addToRowMethod();

        addPlanMethod(0b00);
        addPlanMethod(0b01); // reverse option

        if (!isPrimaryTable()) {
            addSecondaryDescriptorMethod();
        }
        if (!isEvolvable()) {
            addUnfilteredMethods(0);
        }

        if (!isEvolvable()) {
            // Add a method for remotely serializing rows. Dynamic table needs to implement
            // this differently because it has to examine a schema version.
            MethodMaker mm = mClassMaker.addMethod
                (null, "writeRow", RowWriter.class, byte[].class, byte[].class).static_();
            WriteRowMaker.makeWriteRow(mm, mRowInfo, 0, null);
        }

        return mClassMaker.finish();
    }

    /**
     * Defines a static method which accepts a row and returns boolean. When it returns true,
     * all of the given columns are set.
     *
     * @param name method name
     */
    private void addCheckSet(String name, Map<String, ColumnInfo> columns) {
        MethodMaker mm = mClassMaker.addMethod(boolean.class, name, mRowClass).static_().public_();
        mRowGen.checkSet(mm, columns, mm.param(0));
    }

    /**
     * Defines a static method which accepts a row and returns boolean. When it returns true,
     * all of the given columns are dirty.
     *
     * @param name method name
     */
    private void addCheckAllDirty(String name, Map<String, ColumnInfo> columns) {
        MethodMaker mm = mClassMaker.addMethod(boolean.class, name, mRowClass).static_();
        mRowGen.checkDirty(mm, columns, mm.param(0));
    }

    /**
     * Defines a static method which accepts a row and returns boolean. When it returns true,
     * at least one of the given columns are dirty.
     *
     * @param name method name
     */
    private void addCheckAnyDirty(String name, Map<String, ColumnInfo> columns) {
        MethodMaker mm = mClassMaker.addMethod(boolean.class, name, mRowClass).static_();
        mRowGen.checkAnyDirty(mm, columns, mm.param(0));
    }

    /**
     * Defines a static method which accepts a row and always throws a detailed exception
     * describing the required columns which aren't set. A check method should have been
     * invoked first.
     *
     * @param name method name
     */
    private void addRequireSet(String name, Map<String, ColumnInfo> columns) {
        MethodMaker mm = mClassMaker.addMethod(null, name, mRowClass).static_().public_();
        mRowGen.requireSet(mm, columns, mm.param(0));
    }

    /**
     * Defines a static method which encodes a new key by comparing dirty row columns to the
     * original key.
     */
    private void addUpdatePrimaryKeyMethod() {
        MethodMaker mm = mClassMaker
            .addMethod(byte[].class, "updatePrimaryKey", mRowClass, byte[].class).static_();

        Variable rowVar = mm.param(0);

        Label partiallyDirty = mm.label();
        mm.invoke("checkPrimaryKeyAllDirty", rowVar).ifFalse(partiallyDirty);
        mm.return_(mm.invoke("encodePrimaryKey", rowVar));
        partiallyDirty.here();

        var tableVar = mm.class_();
        var ue = encodeUpdateKey(mm, mRowInfo, tableVar, rowVar, mm.param(1));

        mm.return_(ue.newEntryVar);
    }

    /**
     * Defines a static method which encodes a new value by comparing dirty row columns to the
     * original value.
     */
    private void addUpdateValueMethod(int schemaVersion) {
        MethodMaker mm = mClassMaker
            .addMethod(byte[].class, "updateValue", mRowClass, byte[].class).static_();

        Variable rowVar = mm.param(0);

        Label partiallyDirty = mm.label();
        mm.invoke("checkValueAllDirty", rowVar).ifFalse(partiallyDirty);
        mm.return_(mm.invoke("encodeValue", rowVar));
        partiallyDirty.here();

        var tableVar = mm.class_();
        var ue = encodeUpdateValue(mm, mRowInfo, schemaVersion, tableVar, rowVar, mm.param(1));

        mm.return_(ue.newEntryVar);
    }

    /**
     * Override in order for addDoUpdateMethod to work.
     */
    @Override
    protected void finishDoUpdate(MethodMaker mm, Variable rowVar, Variable mergeVar,
                                  Variable keyVar, Variable cursorVar)
    {
        finishDoUpdate(mm, mRowInfo, 0, // no schema version
                       supportsTriggers() ? 1 : 0, mm.this_(), rowVar, mergeVar, keyVar, cursorVar);
    }

    private void addDecodePartialHandle() {
        // Specified by StoredTable.
        MethodMaker mm = mClassMaker.addMethod
            (MethodHandle.class, "makeDecodePartialHandle", byte[].class, int.class).protected_();

        var spec = mm.param(0);
        var lookup = mm.var(MethodHandles.class).invoke("lookup");

        Variable decoder;
        if (isEvolvable()) {
            var schemaVersion = mm.param(1);
            var storeRefVar = mm.invoke("rowStoreRef");
            var thisClassVar = mm.invoke("getClass");
            var tableIdVar = mm.field("mSource").invoke("id");
            decoder = mm.var(DecodePartialMaker.class).invoke
                ("makeDecoder", lookup, storeRefVar, mRowType, mRowClass, thisClassVar,
                 tableIdVar, spec, schemaVersion);
        } else {
            var thisClassVar = mm.invoke("getClass");
            var secondaryDescVar = mm.var(byte[].class).setExact(mSecondaryDescriptor);
            decoder = mm.var(DecodePartialMaker.class).invoke
                ("makeDecoder", lookup, mRowType, mRowClass, thisClassVar, secondaryDescVar, spec);
        }

        mm.return_(decoder);
    }

    /**
     * @param variant "tryLoad", "exists", or "tryDelete"
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

        if (variant != "tryDelete" || !supportsTriggers()) {
            String sourceMethod;
            if (variant == "tryLoad") {
                sourceMethod = "load";
            } else if (variant == "tryDelete") {
                sourceMethod = "delete";
            } else {
                sourceMethod = variant;
            }
            valueVar = source.invoke(sourceMethod, txnVar, keyVar);
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

            assert variant == "tryDelete";
            valueVar = source.invoke("delete", txnVar, keyVar);

            mm.finally_(triggerStart, () -> triggerVar.invoke("releaseShared"));
        }

        if (variant != "tryLoad") {
            mm.return_(valueVar);
        } else {
            Label notNull = mm.label();
            valueVar.ifNe(null, notNull);
            markValuesUnset(rowVar);
            mm.return_(false);
            notNull.here();
            mm.invoke("doDecodeValue", rowVar, valueVar);
            markAllClean(rowVar);
            mm.return_(true);
        }
    }

    /**
     * @param variant "store", "exchange", "tryInsert", or "tryReplace"
     */
    private void addStoreMethod(String variant, Class returnType) {
        MethodMaker mm = mClassMaker.addMethod
            (returnType, variant, Transaction.class, Object.class).public_();

        Variable txnVar = mm.param(0);
        Variable rowVar = mm.param(1).cast(mRowClass);

        Label ready = mm.label();
        mm.invoke("checkAllSet", rowVar).ifTrue(ready);

        if (variant != "tryReplace" && mAutoColumn != null) {
            Label notReady = mm.label();
            mm.invoke("checkAllButAutoSet", rowVar).ifFalse(notReady);
            mm.invoke("storeAuto", txnVar, rowVar);
            if (variant == "exchange") {
                mm.return_(null);
            } else if (variant == "tryInsert") {
                mm.return_(true);
            } else {
                mm.return_();
            }
            notReady.here();
        }

        mm.invoke("requireAllSet", rowVar);

        ready.here();

        var keyVar = mm.invoke("encodePrimaryKey", rowVar);
        var valueVar = mm.invoke("doEncodeValue", rowVar);

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

            // Must enable redoPredicateMode before openAcquire to have any effect.
            mm.invoke("redoPredicateMode", txnVar);

            // Always use a cursor to acquire the upgradable row lock before updating
            // secondaries. This prevents deadlocks with a concurrent index scan which joins
            // against the row. The row lock is acquired exclusively after all secondaries have
            // been updated. At that point, shared lock acquisition against the row is blocked.
            var cursorVar = source.invoke("newCursor", txnVar);
            Label cursorStart = mm.label().here();

            Variable closerVar = mm.field("mIndexLock").invoke("openAcquire", txnVar, rowVar);
            Label opStart = mm.label().here();

            if (variant == "tryReplace") {
                // Note that although that calling replace doesn't insert a new physical row,
                // it can insert a logical row, depending on which query predicates now match.
                // For this reason, it needs to always call openAcquire.
                cursorVar.invoke("find", keyVar);
                mm.finally_(opStart, () -> closerVar.invoke("close"));
                var oldValueVar = cursorVar.invoke("value");
                Label passed = mm.label();
                oldValueVar.ifNe(null, passed);
                mm.return_(false);
                passed.here();
                cursorVar.invoke("store", valueVar);
                triggerVar.invoke("store", txnVar, rowVar, keyVar, oldValueVar, valueVar);
                txnVar.invoke("commit");
                markAllClean(rowVar);
                mm.return_(true);
            } else if (variant == "tryInsert") {
                cursorVar.invoke("autoload", false);
                cursorVar.invoke("find", keyVar);
                mm.finally_(opStart, () -> closerVar.invoke("close"));
                Label passed = mm.label();
                cursorVar.invoke("value").ifEq(null, passed);
                mm.return_(false);
                passed.here();
                triggerVar.invoke("insert", txnVar, rowVar, keyVar, valueVar);
                cursorVar.invoke("commit", valueVar);
                markAllClean(rowVar);
                mm.return_(true);
            } else { // "store" or "exchange"
                cursorVar.invoke("find", keyVar);
                mm.finally_(opStart, () -> closerVar.invoke("close"));
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
                } else { // "exchange"
                    resultVar = oldValueVar;
                    mm.goto_(cont);
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
            // This case is expected for the "tryInsert" and "tryReplace" variants.
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
        copyFields(rowVar, copyVar, mCodecGen.info.keyColumns.values());
        mm.invoke("doDecodeValue", copyVar, resultVar);
        markAllClean(copyVar);
        mm.return_(copyVar);

        // Now implement the exchange bridge method.
        mm = mClassMaker.addMethod
            (Object.class, variant, Transaction.class, Object.class).public_().bridge();
        mm.return_(mm.this_().invoke(returnType, variant, null, mm.param(0), mm.param(1)));
    }

    /**
     * @param variant "store", "exchange", "tryInsert", or "tryReplace"
     */
    private Variable storeNoTrigger(MethodMaker mm, String variant,
                                    Variable txnVar, Variable rowVar,
                                    Variable keyVar, Variable valueVar)
    {
        // Call protected method inherited from StoredTable.
        return mm.invoke(variant + "NoTrigger", txnVar, rowVar, keyVar, valueVar);
    }

    private void addStoreAutoMethod() {
        MethodMaker mm = mClassMaker.addMethod(null, "storeAuto", Transaction.class, mRowClass);

        Variable txnVar = mm.param(0);
        Variable rowVar = mm.param(1);

        var keyVar = mm.invoke("encodePrimaryKey", rowVar);
        var valueVar = mm.invoke("doEncodeValue", rowVar);

        // Call enterScopex because bogus transaction doesn't work with AutomaticKeyGenerator.
        txnVar.set(mm.var(ViewUtils.class).invoke("enterScopex", mm.field("mSource"), txnVar));
        Label txnStart = mm.label().here();

        // Enable redoPredicateMode now because the call to the AutomaticKeyGenerator will
        // acquire a predicate lock.
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
        mm.return_(mm.invoke("doUpdate", txnVar, rowVar, merge));
        mm.finally_(tryStart, () -> txnVar.invoke("exit"));
    }

    private void addMarkAllCleanMethod() {
        // Used by filter implementations, and it must be public because filters are defined in
        // a different package.
        MethodMaker mm = mClassMaker.addMethod(null, "markAllClean", mRowClass).public_().static_();
        markAllClean(mm.param(0));
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

    private void addSecondaryDescriptorMethod() {
        MethodMaker mm = mClassMaker.addMethod(byte[].class, "secondaryDescriptor").protected_();
        mm.return_(mm.var(byte[].class).setExact(mSecondaryDescriptor));
    }
}
