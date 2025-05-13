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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.lang.ref.WeakReference;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.cojen.tupl.DatabaseException;
import org.cojen.tupl.DeletedIndexException;
import org.cojen.tupl.Index;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UniqueConstraintException;

import org.cojen.tupl.core.RowPredicateLock;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Field;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import static org.cojen.tupl.table.RowUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 * @see TableManager
 */
public class IndexTriggerMaker<R> {
    private final Class<R> mRowType;
    private final Class<? extends R> mRowClass;
    private final RowGen mPrimaryGen;

    // To be filled in by caller (TableManager).
    final byte[][] mSecondaryDescriptors;
    final RowInfo[] mSecondaryInfos;
    final Index[] mSecondaryIndexes;
    final RowPredicateLock<R>[] mSecondaryLocks;
    final IndexBackfill<R>[] mBackfills;

    private ClassMaker mClassMaker;

    /**
     * @param rowType can pass null if only makeBackfill is to be called
     */
    @SuppressWarnings("unchecked")
    IndexTriggerMaker(Class<R> rowType, RowInfo primaryInfo, int numIndexes) {
        mRowType = rowType;
        mRowClass = rowType == null ? null : RowMaker.find(rowType);
        mPrimaryGen = primaryInfo.rowGen();

        mSecondaryDescriptors = new byte[numIndexes][];
        mSecondaryInfos = new RowInfo[numIndexes];
        mSecondaryIndexes = new Index[numIndexes];
        mSecondaryLocks = new RowPredicateLock[numIndexes];
        mBackfills = new IndexBackfill[numIndexes];
    }

    /**
     * @param primaryIndexId primary index id
     * @param which which secondary index to make a backfill for 
     */
    @SuppressWarnings("unchecked")
    IndexBackfill<R> makeBackfill(RowStore rs, long primaryIndexId,
                                  TableManager<R> manager, int which)
    {
        ClassMaker cm = mPrimaryGen.beginClassMaker(IndexTriggerMaker.class, mRowType, "backfill");
        cm.extend(IndexBackfill.class).final_();

        MethodMaker mm = cm.addMethod
            (null, "encode", byte[].class, byte[].class, byte[][].class, int.class);
        mm.protected_();

        var primaryKeyVar = mm.param(0);
        var primaryValueVar = mm.param(1);
        var secondaryEntryVar = mm.param(2);
        var offsetVar = mm.param(3);

        byte[] secondaryDesc = mSecondaryDescriptors[which];

        var indy = mm.var(IndexTriggerMaker.class).indy
            ("indyBackfillEncode", rs.ref(), mRowType, primaryIndexId, secondaryDesc);

        var schemaVersion = mm.var(RowUtils.class).invoke("decodeSchemaVersion", primaryValueVar);

        indy.invoke(null, "encode", null, schemaVersion,
                    primaryKeyVar, primaryValueVar, secondaryEntryVar, offsetVar);

        // Now define the constructor.

        MethodType ctorMethodType = MethodType.methodType
            (void.class, RowStore.class, TableManager.class,
             Index.class, byte[].class, String.class);

        mm = cm.addConstructor(ctorMethodType);
        mm.invokeSuperConstructor(mm.param(0), mm.param(1), true, // autoload
                                  mm.param(2), mm.param(3), mm.param(4));

        var lookup = cm.finishHidden();

        Index secondaryIndex = mSecondaryIndexes[which];
        String secondaryStr = mSecondaryInfos[which].eventString();

        try {
            var ctor = lookup.findConstructor(lookup.lookupClass(), ctorMethodType);
            return (IndexBackfill<R>) ctor.invoke
                (rs, manager, secondaryIndex, secondaryDesc, secondaryStr);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    /**
     * Bootstrap method for the backfill encode method.
     *
     * MethodType is:
     *
     *     void (int schemaVersion,
     *           byte[] primaryKey, byte[] primaryValue, byte[][] secondaryEntry, int offset)
     */
    public static SwitchCallSite indyBackfillEncode
        (MethodHandles.Lookup lookup, String name, MethodType mt,
         WeakReference<RowStore> storeRef, Class<?> rowType, long primaryIndexId,
         byte[] secondaryDesc)
    {
        return new SwitchCallSite(lookup, mt, schemaVersion -> {
            // Drop the schemaVersion parameter.
            var mtx = mt.dropParameterTypes(0, 1);

            RowStore store = storeRef.get();
            if (store == null) {
                var mm = MethodMaker.begin(lookup, "encode", mtx);
                mm.new_(DatabaseException.class, "Closed").throw_();
                return mm.finish();
            }

            RowInfo primaryInfo;
            try {
                primaryInfo = store.rowInfo(rowType, primaryIndexId, schemaVersion);
            } catch (Exception e) {
                var mm = MethodMaker.begin(lookup, "encode", mtx);
                return new ExceptionCallSite.Failed(mtx, mm, e);
            }

            MethodMaker mm = MethodMaker.begin(lookup, "encode", mtx);

            var primaryKeyVar = mm.param(0);
            var primaryValueVar = mm.param(1);
            var secondaryEntryVar = mm.param(2);
            var offsetVar = mm.param(3);

            var tm = new TransformMaker<>(null, primaryInfo, null);

            SecondaryInfo secondaryInfo = RowStore.secondaryRowInfo(primaryInfo, secondaryDesc);
            tm.addKeyTarget(secondaryInfo, 0, true);
            tm.addValueTarget(secondaryInfo, 0, true);

            int valueOffset = RowUtils.lengthPrefixPF(schemaVersion);
            tm.begin(mm, null, primaryKeyVar, primaryValueVar, valueOffset);

            secondaryEntryVar.aset(offsetVar, tm.encode(0));
            secondaryEntryVar.aset(offsetVar.add(1), tm.encode(1));

            return mm.finish();
        });
    }

    /**
     * @param primaryIndexId primary index id
     */
    @SuppressWarnings("unchecked")
    Trigger<R> makeTrigger(RowStore rs, long primaryIndexId, StoredTable<R> table) {
        mClassMaker = mPrimaryGen.beginClassMaker(IndexTriggerMaker.class, mRowType, "trigger");
        mClassMaker.extend(Trigger.class).final_();

        boolean hasBackfills = false;

        for (int i=0; i<mSecondaryIndexes.length; i++) {
            mClassMaker.addField(Index.class, "ix" + i).private_().final_();

            if (mSecondaryLocks[i] != null) {
                mClassMaker.addField(RowPredicateLock.class, "lock" + i).private_().final_();
            }

            IndexBackfill backfill = mBackfills[i];
            if (backfill != null) {
                hasBackfills = true;
                mClassMaker.addField(IndexBackfill.class, "backfill" + i).private_().final_();
            }
        }


        MethodType ctorMethodType;
        if (!hasBackfills) {
            ctorMethodType = MethodType.methodType
                (void.class, Index[].class, RowPredicateLock[].class);
        } else {
            ctorMethodType = MethodType.methodType
                (void.class, Index[].class, RowPredicateLock[].class, IndexBackfill[].class);
        }

        {
            MethodMaker mm = mClassMaker.addConstructor(ctorMethodType);
            mm.invokeSuperConstructor();

            for (int i=0; i<mSecondaryIndexes.length; i++) {
                mm.field("ix" + i).set(mm.param(0).aget(i));
            }

            for (int i=0; i<mSecondaryLocks.length; i++) {
                if (mSecondaryLocks[i] != null) {
                    mm.field("lock" + i).set(mm.param(1).aget(i));
                }
            }

            if (hasBackfills) {
                for (int i=0; i<mBackfills.length; i++) {
                    if (mBackfills[i] != null) {
                        mm.field("backfill" + i).set(mm.param(2).aget(i));
                    }
                }
            }
        }

        {
            boolean requiresRow = addInsertMethod("insert", true);
            addInsertMethod("insertP", requiresRow);
        }

        {
            addDeleteMethod(rs, primaryIndexId, true, hasBackfills);
            addDeleteMethod(rs, primaryIndexId, false, hasBackfills);
        }

        {
            boolean requiresRow = addStoreMethod("store", true, table);
            addStoreMethod("storeP", requiresRow, table);
        }

        if (hasBackfills) {
            MethodMaker mm = mClassMaker.addMethod(null, "notifyDisabled").protected_();
            for (int i=0; i<mBackfills.length; i++) {
                if (mBackfills[i] != null) {
                    mm.field("backfill" + i).invoke("unused", mm.this_());
                }
            }
        }

        var lookup = mClassMaker.finishHidden();

        Trigger<R> trigger;

        try {
            var ctor = lookup.findConstructor(lookup.lookupClass(), ctorMethodType);
            if (!hasBackfills) {
                trigger = (Trigger<R>) ctor.invoke(mSecondaryIndexes, mSecondaryLocks);
            } else {
                trigger = (Trigger<R>) ctor.invoke(mSecondaryIndexes, mSecondaryLocks, mBackfills);
            }
        } catch (Throwable e) {
            throw rethrow(e);
        }

        if (hasBackfills) {
            for (IndexBackfill<R> b : mBackfills) {
                if (b != null) {
                    b.used(trigger);
                }
            }
        }

        return trigger;
    }

    /**
     * @param variant "insert" or "insertP"
     * @param define delegate to the non-partial variant when false
     * @return true if always requires a row instance
     */
    private boolean addInsertMethod(String variant, boolean define) {
        MethodMaker mm = mClassMaker.addMethod
            (null, variant, Transaction.class, Object.class, byte[].class, byte[].class).public_();

        boolean isPartial = variant == "insertP";

        var txnVar = mm.param(0);
        Variable rowVar = mm.param(1);
        var keyVar = mm.param(2);
        var newValueVar = mm.param(3);

        if (isPartial && !define) {
            mm.invoke("insert", txnVar, rowVar, keyVar, newValueVar);
            return false;
        }

        rowVar = rowVar.cast(mRowClass);

        Map<String, TransformMaker.Availability> available = new HashMap<>();
        var avail = isPartial ? TransformMaker.Availability.CONDITIONAL
            : TransformMaker.Availability.ALWAYS;
        for (String colName : mPrimaryGen.info.allColumns.keySet()) {
            available.put(colName, avail);
        }

        var tm = new TransformMaker<R>(mRowType, mPrimaryGen.info, available);

        for (RowInfo secondaryInfo : mSecondaryInfos) {
            tm.addKeyTarget(secondaryInfo, 0, true);
            tm.addValueTarget(secondaryInfo, 0, true);
        }

        tm.begin(mm, rowVar, keyVar, newValueVar, -1);

        for (int i=0; i<mSecondaryInfos.length; i++) {
            var secondaryKeyVar = tm.encode(i << 1);
            var secondaryValueVar = tm.encode((i << 1) + 1);

            Variable closerVar = acquirePredicateLock
                (mm, i, isPartial, txnVar, rowVar, secondaryKeyVar, secondaryValueVar);

            Label opStart = mm.label().here();
            var ixField = mm.field("ix" + i);

            if (!mSecondaryInfos[i].isAltKey()) {
                ixField.invoke("store", txnVar, secondaryKeyVar, secondaryValueVar);
                if (closerVar != null) {
                    mm.finally_(opStart, () -> closerVar.invoke("close"));
                }
            } else {
                var result = ixField.invoke("insert", txnVar, secondaryKeyVar, secondaryValueVar);
                if (closerVar != null) {
                    mm.finally_(opStart, () -> closerVar.invoke("close"));
                }
                Label pass = mm.label();
                result.ifTrue(pass);
                mm.new_(UniqueConstraintException.class, "Alternate key").throw_();
                pass.here();
            }

            if (isPartial) {
                var attachment = txnVar.invoke("attachment");
                Label noAttachment = mm.label();
                attachment.instanceOf(TriggerIndexAccessor.class).ifFalse(noAttachment);
                attachment.cast(TriggerIndexAccessor.class)
                    .invoke("stored", ixField, secondaryKeyVar, secondaryValueVar);
                noAttachment.here();
            }

            if (mBackfills[i] != null) {
                mm.field("backfill" + i).invoke
                    ("inserted", txnVar, secondaryKeyVar, secondaryValueVar);
            }

            mm.catch_(opStart, DeletedIndexException.class, exVar -> {
                // Index was dropped. Assume that this trigger will soon be replaced.
            });
        }

        return tm.requiresRow();
    }

    @SuppressWarnings("unchecked")
    private void addDeleteMethod(RowStore rs, long primaryIndexId,
                                 boolean hasRow, boolean hasBackfills)
    {
        Object[] params;
        if (hasRow) {
            params = new Object[] {Transaction.class, Object.class, byte[].class, byte[].class};
        } else {
            params = new Object[] {Transaction.class, byte[].class, byte[].class};
        }

        MethodMaker mm = mClassMaker.addMethod(null, "delete", params).public_();

        // The deletion of secondary indexes typically requires that the old value be
        // decoded. Given that the schema version can vary, don't fully implement this method
        // until needed. Create a new delegate for each schema version encountered.

        Variable txnVar = mm.param(0), rowVar, keyVar, oldValueVar;

        if (hasRow) {
            rowVar = mm.param(1).cast(mRowClass);
            keyVar = mm.param(2);
            oldValueVar = mm.param(3);
        } else {
            rowVar = null;
            keyVar = mm.param(1);
            oldValueVar = mm.param(2);
        }

        var schemaVersion = mm.var(RowUtils.class).invoke("decodeSchemaVersion", oldValueVar);

        var secondaryIndexIds = new long[mSecondaryIndexes.length];
        for (int i=0; i<secondaryIndexIds.length; i++) {
            secondaryIndexIds[i] = mSecondaryIndexes[i].id();
        }

        WeakReference<IndexBackfill>[] backfillRefs = null;
        if (hasBackfills) {
            backfillRefs = new WeakReference[mBackfills.length];
            for (int i=0; i<mBackfills.length; i++) {
                IndexBackfill backfill = mBackfills[i];
                if (backfill != null) {
                    backfillRefs[i] = new WeakReference<>(backfill);
                }
            }
        }

        var indy = mm.var(IndexTriggerMaker.class).indy
            ("indyDelete", rs.ref(), mRowType, primaryIndexId,
             mSecondaryDescriptors, secondaryIndexIds, backfillRefs);

        if (hasRow) {
            indy.invoke(null, "delete", null, schemaVersion, txnVar, rowVar, keyVar, oldValueVar);
        } else {
            indy.invoke(null, "delete", null, schemaVersion, txnVar, keyVar, oldValueVar);
        }
    }

    /**
     * Bootstrap method for the trigger delete method.
     *
     * MethodType is either of these:
     *
     *     void (int schemaVersion, Transaction txn, Row row, byte[] key, byte[] oldValueVar)
     *     void (int schemaVersion, Transaction txn, byte[] key, byte[] oldValueVar)
     */
    public static SwitchCallSite indyDelete(MethodHandles.Lookup lookup, String name,
                                            MethodType mt, WeakReference<RowStore> storeRef,
                                            Class<?> rowType, long indexId,
                                            byte[][] secondaryDescs, long[] secondaryIndexIds,
                                            WeakReference<IndexBackfill>[] backfillRefs)
    {
        Class<?> rowClass;
        if (mt.parameterCount() == 5) {
            rowClass = mt.parameterType(2);
        } else {
            rowClass = null;
        }

        return new SwitchCallSite(lookup, mt, schemaVersion -> {
            // Drop the schemaVersion parameter.
            var mtx = mt.dropParameterTypes(0, 1);

            RowStore store = storeRef.get();
            if (store == null) {
                var mm = MethodMaker.begin(lookup, "delete", mtx);
                mm.new_(DatabaseException.class, "Closed").throw_();
                return mm.finish();
            }

            RowInfo primaryInfo;
            Index[] secondaryIndexes;
            try {
                primaryInfo = store.rowInfo(rowType, indexId, schemaVersion);
                secondaryIndexes = new Index[secondaryIndexIds.length];
                for (int i=0; i<secondaryIndexIds.length; i++) {
                    secondaryIndexes[i] = store.mDatabase.indexById(secondaryIndexIds[i]);
                }
            } catch (Exception e) {
                var mm = MethodMaker.begin(lookup, "delete", mtx);
                return new ExceptionCallSite.Failed(mtx, mm, e);
            }

            SecondaryInfo[] secondaryInfos =
                RowStore.secondaryRowInfos(primaryInfo, secondaryDescs);

            IndexBackfill[] backfills = null;
            if (backfillRefs != null) {
                backfills = new IndexBackfill[backfillRefs.length];
                for (int i=0; i<backfills.length; i++) {
                    var backfillRef = backfillRefs[i];
                    if (backfillRef != null) {
                        backfills[i] = backfillRef.get();
                    }
                }
            }

            return makeDeleteMethod(mtx, schemaVersion, rowType, rowClass, primaryInfo,
                                    secondaryInfos, secondaryIndexes, backfills);
        });
    }

    private static MethodHandle makeDeleteMethod
        (MethodType mt, int schemaVersion,
         Class<?> rowType, Class rowClass, RowInfo primaryInfo,
         RowInfo[] secondaryInfos, Index[] secondaryIndexes,
         IndexBackfill[] backfills)
    {
        ClassMaker cm = primaryInfo.rowGen().beginClassMaker
            (IndexTriggerMaker.class, rowType, "trigger-delete").final_();

        MethodType ctorMethodType;
        if (backfills == null) {
            ctorMethodType = MethodType.methodType(void.class, Index[].class);
        } else {
            ctorMethodType = MethodType.methodType
                (void.class, Index[].class, IndexBackfill.class);
        }

        MethodMaker ctorMaker = cm.addConstructor(ctorMethodType);
        ctorMaker.invokeSuperConstructor();

        MethodMaker mm = cm.addMethod("delete", mt);

        Variable txnVar = mm.param(0), rowVar, keyVar, oldValueVar;
        Map<String, TransformMaker.Availability> available;

        if (rowClass != null) {
            rowVar = mm.param(1);
            keyVar = mm.param(2);
            oldValueVar = mm.param(3);
            available = new HashMap<>();
            for (String colName : primaryInfo.keyColumns.keySet()) {
                available.put(colName, TransformMaker.Availability.ALWAYS);
            }
        } else {
            rowVar = null;
            keyVar = mm.param(1);
            oldValueVar = mm.param(2);
            available = null;
        }

        var tm = new TransformMaker<>(rowType, primaryInfo, available);

        for (int i=0; i<secondaryInfos.length; i++) {
            if (secondaryIndexes[i] == null) {
                // Index was dropped, so no need to delete anything from it either.
            } else {
                tm.addKeyTarget(secondaryInfos[i], 0, true);
            }
        }

        int valueOffset = schemaVersion == 0 ? 0 : (schemaVersion < 128 ? 1 : 4);
        tm.begin(mm, rowVar, keyVar, oldValueVar, valueOffset);

        for (int i=0; i<secondaryInfos.length; i++) {
            if (secondaryIndexes[i] == null) {
                // Index was dropped, so no need to delete anything from it either.
                continue;
            }

            var secondaryKeyVar = tm.encode(i);

            String ixFieldName = "ix" + i;
            cm.addField(Index.class, ixFieldName).private_().final_();

            ctorMaker.field(ixFieldName).set(ctorMaker.param(0).aget(i));

            String backfillFieldName = null;
            if (backfills != null && backfills[i] != null) {
                backfillFieldName = "backfill" + i;
                cm.addField(IndexBackfill.class, backfillFieldName)
                    .private_().final_();
                ctorMaker.field(backfillFieldName).set(ctorMaker.param(1).aget(i));
            }

            Label opStart = mm.label().here();

            var ixField = mm.field(ixFieldName);
            if (rowVar != null) {
                ixField.invoke("store", txnVar, secondaryKeyVar, null);
            } else {
                var attachment = txnVar.invoke("attachment");
                Label noAttachment = mm.label();
                attachment.instanceOf(TriggerIndexAccessor.class).ifFalse(noAttachment);
                Label cont = mm.label();
                attachment.cast(TriggerIndexAccessor.class)
                    .invoke("delete", ixField, secondaryKeyVar).ifTrue(cont);
                noAttachment.here();
                ixField.invoke("store", txnVar, secondaryKeyVar, null);
                cont.here();
            }

            if (backfillFieldName != null) {
                mm.field(backfillFieldName).invoke("deleted", txnVar, secondaryKeyVar);
            }

            mm.catch_(opStart, DeletedIndexException.class, exVar -> {
                // Index was dropped. Assume that this trigger will soon be replaced.
            });
        }

        var lookup = cm.finishHidden();
        var clazz = lookup.lookupClass();

        try {
            var ctor = lookup.findConstructor(clazz, ctorMethodType);
            Object deleter;
            if (backfills == null) {
                deleter = ctor.invoke(secondaryIndexes);
            } else {
                deleter = ctor.invoke(secondaryIndexes, backfills);
            }
            return lookup.findVirtual(clazz, "delete", mt).bindTo(deleter);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    /**
     * @param variant "store" or "storeP"
     * @param define delegate to the non-partial variant when false
     * @return true if always requires a row instance
     */
    private boolean addStoreMethod(String variant, boolean define, StoredTable<R> table) {
        MethodMaker mm = mClassMaker.addMethod
            (null, variant, Transaction.class, Object.class,
             byte[].class, byte[].class, byte[].class).public_();

        boolean isPartial = variant == "storeP";

        var txnVar = mm.param(0);
        Variable rowVar = mm.param(1);
        var keyVar = mm.param(2);
        var oldValueVar = mm.param(3);
        var newValueVar = mm.param(4);

        if (isPartial && !define) {
            mm.invoke("store", txnVar, rowVar, keyVar, oldValueVar, newValueVar);
            return false;
        }

        Map<String, TransformMaker.Availability> available = new HashMap<>();
        var avail = isPartial ? TransformMaker.Availability.CONDITIONAL
            : TransformMaker.Availability.ALWAYS;
        for (String colName : mPrimaryGen.info.allColumns.keySet()) {
            available.put(colName, avail);
        }

        var tm = new TransformMaker<R>(mRowType, mPrimaryGen.info, available);

        for (RowInfo secondaryInfo : mSecondaryInfos) {
            tm.addKeyTarget(secondaryInfo, 0, false);
            tm.addValueTarget(secondaryInfo, 0, false);
        }

        if (tm.onlyNeedsKeys()) {
            // If the targets only depend on the primary keys, then any differences in the
            // values are irrelevant.
            mm.return_();
            return false;
        }

        rowVar = rowVar.cast(mRowClass);

        var newVersion = mm.var(RowUtils.class).invoke("decodeSchemaVersion", newValueVar);
        var tableVar = mm.var(table.getClass());
        TableMaker.convertValueIfNecessary(tableVar, rowVar, newVersion, oldValueVar);

        TransformMaker otm = tm.beginValueDiff(mm, rowVar, keyVar, newValueVar, -1, oldValueVar);

        for (int i=0; i<mSecondaryInfos.length; i++) {
            Label modified = mm.label();

            Label cont = mm.label();
            if (!otm.diffValueCheck(cont, i << 1, (i << 1) + 1)) {
                continue;
            }

            // Index needs to be updated. Insert the new entry and delete the old one.

            var secondaryKeyVar = tm.encode(i << 1);
            var secondaryValueVar = tm.encode((i << 1) + 1);

            Variable closerVar = acquirePredicateLock
                (mm, i, isPartial, txnVar, rowVar, secondaryKeyVar, secondaryValueVar);

            Label opStart = mm.label().here();
            Field ixField = mm.field("ix" + i);
            RowInfo secondaryInfo = mSecondaryInfos[i];
            RowGen secondaryGen = secondaryInfo.rowGen();

            if (!secondaryInfo.isAltKey()) {
                ixField.invoke("store", txnVar, secondaryKeyVar, secondaryValueVar);
                if (closerVar != null) {
                    mm.finally_(opStart, () -> closerVar.invoke("close"));
                }
            } else {
                var result = ixField.invoke("insert", txnVar, secondaryKeyVar, secondaryValueVar);
                if (closerVar != null) {
                    mm.finally_(opStart, () -> closerVar.invoke("close"));
                }
                Label pass = mm.label();
                result.ifTrue(pass);
                mm.new_(UniqueConstraintException.class, "Alternate key").throw_();
                pass.here();
            }

            if (isPartial) {
                var attachment = txnVar.invoke("attachment");
                Label noAttachment = mm.label();
                attachment.instanceOf(TriggerIndexAccessor.class).ifFalse(noAttachment);
                attachment.cast(TriggerIndexAccessor.class)
                    .invoke("stored", ixField, secondaryKeyVar, secondaryValueVar);
                noAttachment.here();
            }

            if (mBackfills[i] != null) {
                mm.field("backfill" + i).invoke
                    ("inserted", txnVar, secondaryKeyVar, secondaryValueVar);
            }

            // Now delete the old entry.

            var deleteKeyVar = otm.encode(i << 1);

            if (!secondaryInfo.isAltKey() && !secondaryInfo.valueColumns.isEmpty()) {
                // If this is a covering index, then the key might be the same. Don't delete it.
                mm.var(Arrays.class).invoke("equals", secondaryKeyVar, deleteKeyVar).ifTrue(cont);
            }

            ixField.invoke("store", txnVar, deleteKeyVar, null);

            if (mBackfills[i] != null) {
                mm.field("backfill" + i).invoke("deleted", txnVar, deleteKeyVar);
            }

            mm.catch_(opStart, DeletedIndexException.class, exVar -> {
                // Index was dropped. Assume that this trigger will soon be replaced.
            });

            cont.here();
        }

        return tm.requiresRow();
    }

    /**
     * @param i secondaryInfo index
     * @return the closerVar or null if no lock was acquired
     */
    private Variable acquirePredicateLock(MethodMaker mm, int i, boolean isPartial,
                                          Variable txnVar, Variable rowVar,
                                          Variable secondaryKeyVar, Variable secondaryValueVar)
    {
        if (mSecondaryLocks[i] == null) {
            return null;
        }

        var lockVar = mm.field("lock" + i);
        if (!isPartial) {
            return lockVar.invoke("openAcquire", txnVar, rowVar);
        }

        // Row might be partially specified, so use the variant that can examine the fully
        // encoded binary form.
        return lockVar.invoke("openAcquireP", txnVar, rowVar, secondaryKeyVar, secondaryValueVar);
    }
}
