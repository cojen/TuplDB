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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.lang.ref.WeakReference;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.cojen.tupl.DatabaseException;
import org.cojen.tupl.Index;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UniqueConstraintException;

import org.cojen.tupl.util.Clutch;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Field;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import static org.cojen.tupl.rows.RowUtils.*;

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
    final SecondaryInfo[] mSecondaryInfos;
    final Index[] mSecondaryIndexes;
    final IndexBackfill<R>[] mBackfills;

    // Map for all the columns needed by all of the secondary indexes.
    private Map<String, ColumnSource> mColumnSources;

    private ClassMaker mClassMaker;

    // Values for rowMode param.
    //  ROW_NONE     - no row exists
    //  ROW_KEY_ONLY - only key fields of the row can be used
    //  ROW_FULL     - the full row can be used
    private static final int ROW_NONE = 0, ROW_KEY_ONLY = 1, ROW_FULL = 2;

    /**
     * @param rowType can pass null if only makeBackfill is to be called
     */
    @SuppressWarnings("unchecked")
    IndexTriggerMaker(Class<R> rowType, RowInfo primaryInfo, int numIndexes) {
        mRowType = rowType;
        mRowClass = rowType == null ? null : RowMaker.find(rowType);
        mPrimaryGen = primaryInfo.rowGen();

        mSecondaryDescriptors = new byte[numIndexes][];
        mSecondaryInfos = new SecondaryInfo[numIndexes];
        mSecondaryIndexes = new Index[numIndexes];
        mBackfills = new IndexBackfill[numIndexes];
    }

    /**
     * Used by indy methods.
     *
     * @param secondaryIndexes note: elements are null for dropped indexes
     */
    private IndexTriggerMaker(Class<R> rowType, RowInfo primaryInfo,
                              SecondaryInfo[] secondaryInfos, Index[] secondaryIndexes)
    {
        mRowType = rowType;
        mRowClass = RowMaker.find(rowType);
        mPrimaryGen = primaryInfo.rowGen();

        mSecondaryDescriptors = null;
        mSecondaryInfos = secondaryInfos;
        mSecondaryIndexes = secondaryIndexes;
        mBackfills = null;
    }

    /**
     * @param which which secondary index to make a backfill for 
     */
    IndexBackfill<R> makeBackfill(RowStore rs, TableManager<R> manager, int which) {
        SecondaryInfo secondaryInfo = mSecondaryInfos[which];
        Index secondaryIndex = mSecondaryIndexes[which];

        ClassMaker cm = mPrimaryGen.beginClassMaker(IndexTriggerMaker.class, mRowType, "Backfill");
        cm.extend(IndexBackfill.class).final_();

        mColumnSources = buildColumnSources(secondaryInfo);

        MethodMaker mm = cm.addMethod
            (null, "encode", byte[].class, byte[].class, byte[][].class, int.class);
        mm.protected_();

        var primaryKeyVar = mm.param(0);
        var primaryValueVar = mm.param(1);
        var secondaryEntryVar = mm.param(2);
        var offsetVar = mm.param(3);

        int numValues = findColumns(mm, primaryKeyVar, primaryValueVar, -1, ROW_NONE);

        RowGen secondaryGen = secondaryInfo.rowGen();

        ColumnCodec[] secondaryKeyCodecs = ColumnCodec.bind(secondaryGen.keyCodecs(), mm);
        ColumnCodec[] secondaryValueCodecs = ColumnCodec.bind(secondaryGen.valueCodecs(), mm);

        var secondaryKeyVar = encodeColumns(mm, mColumnSources, null, ROW_NONE,
                                            primaryKeyVar, primaryValueVar, secondaryKeyCodecs);

        secondaryEntryVar.aset(offsetVar, secondaryKeyVar);

        var secondaryValueVar = encodeColumns(mm, mColumnSources, null, ROW_NONE,
                                              primaryKeyVar, primaryValueVar, secondaryValueCodecs);

        secondaryEntryVar.aset(offsetVar.add(1), secondaryValueVar);
        
        // Now define the constructor.

        MethodType ctorMethodType = MethodType.methodType
            (void.class, RowStore.class, TableManager.class,
             Index.class, byte[].class, String.class);

        mm = cm.addConstructor(ctorMethodType);
        boolean autoload = numValues != 0;
        mm.invokeSuperConstructor(mm.param(0), mm.param(1), autoload,
                                  mm.param(2), mm.param(3), mm.param(4));

        var lookup = cm.finishHidden();

        byte[] secondaryDesc = mSecondaryDescriptors[which];
        String secondaryStr = secondaryInfo.eventString();

        try {
            var ctor = lookup.findConstructor(lookup.lookupClass(), ctorMethodType);
            return (IndexBackfill<R>) ctor.invoke
                (rs, manager, secondaryIndex, secondaryDesc, secondaryStr);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    /**
     * @param primaryIndexId primary index id
     */
    Trigger<R> makeTrigger(RowStore rs, long primaryIndexId, Clutch.Pack pack) {
        mClassMaker = mPrimaryGen.beginClassMaker(IndexTriggerMaker.class, mRowType, "Trigger");
        mClassMaker.extend(Trigger.class).final_();

        boolean hasBackfills = false;

        for (int i=0; i<mSecondaryIndexes.length; i++) {
            mClassMaker.addField(Index.class, "ix" + i).private_().final_();

            IndexBackfill backfill = mBackfills[i];
            if (backfill != null) {
                hasBackfills = true;
                mClassMaker.addField(IndexBackfill.class, "backfill" + i).private_().final_();
            }
        }


        MethodType ctorMethodType;
        if (!hasBackfills) {
            ctorMethodType = MethodType.methodType(void.class, Clutch.Pack.class, Index[].class);
        } else {
            ctorMethodType = MethodType.methodType
                (void.class, Clutch.Pack.class, Index[].class, IndexBackfill[].class);
        }

        {
            MethodMaker mm = mClassMaker.addConstructor(ctorMethodType);
            mm.invokeSuperConstructor(mm.param(0));

            for (int i=0; i<mSecondaryIndexes.length; i++) {
                mm.field("ix" + i).set(mm.param(1).aget(i));
            }

            if (hasBackfills) {
                for (int i=0; i<mBackfills.length; i++) {
                    if (mBackfills[i] != null) {
                        mm.field("backfill" + i).set(mm.param(2).aget(i));
                    }
                }
            }
        }

        mColumnSources = buildColumnSources();

        addInsertMethod();

        addDeleteMethod(rs, primaryIndexId, hasBackfills);

        addStoreMethod("store");

        addStoreMethod("update");

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
                trigger = (Trigger<R>) ctor.invoke(pack, mSecondaryIndexes);
            } else {
                trigger = (Trigger<R>) ctor.invoke(pack, mSecondaryIndexes, mBackfills);
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

    private Map<String, ColumnSource> buildColumnSources() {
        return buildColumnSources(mSecondaryInfos);
    }

    private Map<String, ColumnSource> buildColumnSources(SecondaryInfo... secondaryInfos) {
        Map<String, ColumnCodec> keyCodecMap = mPrimaryGen.keyCodecMap();
        Map<String, ColumnCodec> valueCodecMap = mPrimaryGen.valueCodecMap();

        var sources = new HashMap<String, ColumnSource>();

        for (RowInfo secondaryInfo : secondaryInfos) {
            RowGen secondaryGen = secondaryInfo.rowGen();
            ColumnCodec[] secondaryKeyCodecs = secondaryGen.keyCodecs();
            ColumnCodec[] secondaryValueCodecs = secondaryGen.valueCodecs();

            buildColumnSources(sources, keyCodecMap, true, secondaryKeyCodecs);
            buildColumnSources(sources, keyCodecMap, true, secondaryValueCodecs);

            buildColumnSources(sources, valueCodecMap, false, secondaryKeyCodecs);
            buildColumnSources(sources, valueCodecMap, false, secondaryValueCodecs);
        }

        return sources;
    }

    /**
     * @param sources results stored here
     * @param primaryCodecMap
     * @param secondaryCodecs secondary index columns which need to be found
     */
    private void buildColumnSources(Map<String, ColumnSource> sources,
                                    Map<String, ColumnCodec> primaryCodecMap, boolean fromKey,
                                    ColumnCodec[] secondaryCodecs)
    {
        for (ColumnCodec codec : secondaryCodecs) {
            String name = codec.mInfo.name;
            ColumnSource source = sources.get(name);
            if (source == null) {
                ColumnCodec primaryCodec = primaryCodecMap.get(name);
                if (primaryCodec == null) {
                    continue;
                }
                source = new ColumnSource(sources.size(), primaryCodec, fromKey);
                sources.put(name, source);
            }
            if (source.mCodec.equals(codec)) {
                source.mMatches++;
            } else {
                source.mMismatches++;
            }
        }
    }

    /**
     * Returns a copy that excludes the ColumnSource Variable fields.
     */
    private Map<String, ColumnSource> cloneColumnSources() {
        var columnSources = new HashMap<String, ColumnSource>(mColumnSources.size());
        for (Map.Entry<String, ColumnSource> e : mColumnSources.entrySet()) {
            columnSources.put(e.getKey(), new ColumnSource(e.getValue()));
        }
        return columnSources;
    }

    /**
     * Generates code which finds column offsets in the encoded primary row. As a side-effect,
     * the Variable fields of all ColumnSources are updated.
     *
     * @param keyVar primary key byte array
     * @param valueVar primary value byte array
     * @param valueOffset initial offset into byte array, or -1 to auto skip schema version
     * @param rowMode see ROW_* fields
     * @return the number of columns accessed from the encoded value
     */
    private int findColumns(MethodMaker mm,
                            Variable keyVar, Variable valueVar, int valueOffset, int rowMode)
    {
        // Determine how many columns must be accessed from the encoded form.

        int numKeys = 0, numValues = 0;

        for (ColumnSource source : mColumnSources.values()) {
            source.clearVars();
            if (source.mustFind(rowMode)) {
                if (source.mFromKey) {
                    numKeys++;
                } else {
                    numValues++;
                }
            }
        }

        findColumns(mm, keyVar, 0, true, rowMode, numKeys);

        findColumns(mm, valueVar, valueOffset, false, rowMode, numValues);

        return numValues;
    }

    /**
     * Generates code which finds column offsets in the encoded primary row. As a side-effect,
     * the Variable fields of all ColumnSources are updated.
     *
     * @param srcVar byte array
     * @param offset initial offset into byte array, or -1 to auto skip schema version
     * @param rowMode see ROW_* fields
     * @param num number of columns to decode/skip from the byte array
     */
    private void findColumns(MethodMaker mm,
                             Variable srcVar, int offset,
                             boolean forKey, int rowMode, int num)
    {
        if (num == 0) {
            return;
        }

        ColumnCodec[] codecs;
        Variable offsetVar = mm.var(int.class);

        if (forKey) {
            codecs = mPrimaryGen.keyCodecs();
            offsetVar.set(0);
        } else {
            codecs = mPrimaryGen.valueCodecs();
            if (offset >= 0) {
                offsetVar.set(offset);
            } else {
                offsetVar.set(mm.var(RowUtils.class).invoke("skipSchemaVersion", srcVar));
            }
        }

        codecs = ColumnCodec.bind(codecs, mm);
        Variable endVar = null;

        for (ColumnCodec codec : codecs) {
            ColumnSource source = mColumnSources.get(codec.mInfo.name);

            findCheck: {
                if (source != null) {
                    if (source.mFromKey != forKey) {
                        throw new AssertionError();
                    }
                    if (source.mustFind(rowMode)) {
                        break findCheck;
                    }
                }
                // Can't re-use end offset for next start offset when a gap exists.
                endVar = null;
                codec.decodeSkip(srcVar, offsetVar, null);
                continue;
            }

            Variable startVar = endVar;
            if (startVar == null) {
                // Need a stable copy.
                startVar = mm.var(int.class).set(offsetVar);
            }

            if (source.mMismatches == 0 ||
                rowMode == ROW_FULL || (rowMode == ROW_KEY_ONLY && source.mFromKey))
            {
                // Can skip decoding if the column doesn't need to be transformed or if it can
                // be found in the row.
                codec.decodeSkip(srcVar, offsetVar, null);
            } else {
                var dstVar = mm.var(codec.mInfo.type);
                codec.decode(dstVar, srcVar, offsetVar, null);
                source.setDecodedVar(dstVar);
            }

            // Need a stable copy.
            endVar = mm.var(int.class).set(offsetVar);

            if (source.mMatches != 0) {
                source.mSrcVar = srcVar;
                source.mStartVar = startVar;
                source.mEndVar = endVar;
            }

            if (--num <= 0) {
                break;
            }
        }
    }

    /**
     * Generates code which finds columns in two encoded rows and determines if they differ.
     * Both rows must have the same schema version. As a side-effect, the Variable fields of
     * all ColumnSources are updated.
     *
     * The returned variables represent a bit map, whose bits correspond to a ColumnSource
     * slot. Some variables in the array will be null, which implies that the bit map is all
     * zero for that particular range.
     *
     * @param bitMap set of long varibles representing a bit map
     * @param oldSrcVar primary value byte array
     * @param newSrcVar primary value byte array
     */
    private Variable[] diffColumns(MethodMaker mm, Variable[] bitMap,
                                   Map<String, ColumnSource> oldColumnSources,
                                   Map<String, ColumnSource> newColumnSources,
                                   Variable oldSrcVar, Variable newSrcVar)
    {
        if (oldColumnSources.size() != newColumnSources.size()) {
            throw new AssertionError();
        }

        // Determine how many columns must be accessed from the encoded form.

        int numValues = 0;

        for (ColumnSource source : oldColumnSources.values()) {
            source.clearVars();
            if (!source.mFromKey) {
                numValues++;
            }
        }

        for (ColumnSource source : newColumnSources.values()) {
            source.clearVars();
            // ColumnSources are expected to be equivalent, so don't double count the
            // values. Two source instances are used to track separate sets of Variables.
        }

        diffColumns(mm, oldColumnSources, newColumnSources,
                    oldSrcVar, newSrcVar, bitMap, false, numValues);

        return bitMap;
    }

    private void diffColumns(MethodMaker mm,
                             Map<String, ColumnSource> oldColumnSources,
                             Map<String, ColumnSource> newColumnSources,
                             Variable oldSrcVar, Variable newSrcVar,
                             Variable[] bitMap, boolean forKey, int num)
    {
        if (num == 0) {
            return;
        }

        ColumnCodec[] oldCodecs;
        Variable oldOffsetVar = mm.var(int.class);
        Variable newOffsetVar = mm.var(int.class);

        if (forKey) {
            oldCodecs = mPrimaryGen.keyCodecs();
            oldOffsetVar.set(0);
            newOffsetVar.set(0);
        } else {
            oldCodecs = mPrimaryGen.valueCodecs();
            oldOffsetVar.set(mm.var(RowUtils.class).invoke("skipSchemaVersion", oldSrcVar));
            newOffsetVar.set(oldOffsetVar); // same schema version
        }

        oldCodecs = ColumnCodec.bind(oldCodecs, mm);
        ColumnCodec[] newCodecs = ColumnCodec.bind(oldCodecs, mm);
        Variable oldEndVar = null, newEndVar = null;

        for (int i=0; i<oldCodecs.length; i++) {
            ColumnCodec oldCodec = oldCodecs[i];
            ColumnCodec newCodec = newCodecs[i];

            ColumnSource oldSource = oldColumnSources.get(oldCodec.mInfo.name);

            if (oldSource == null) {
                // Can't re-use end offset for next start offset when a gap exists.
                oldEndVar = null;
                newEndVar = null;
                oldCodec.decodeSkip(oldSrcVar, oldOffsetVar, null);
                newCodec.decodeSkip(newSrcVar, newOffsetVar, null);
                continue;
            }

            int slot = oldSource.mSlot;

            Variable bitsVar = bitMap[bitMapWord(slot)];
            if (bitsVar == null) {
                bitsVar = mm.var(long.class).set(0);
                bitMap[bitMapWord(slot)] = bitsVar;
            }

            Variable oldStartVar = oldEndVar;
            Variable newStartVar = newEndVar;

            if (oldStartVar == null) {
                // Need a stable copy.
                oldStartVar = mm.var(int.class).set(oldOffsetVar);
                newStartVar = mm.var(int.class).set(newOffsetVar);
            }

            Label same = mm.label();

            Variable[] decoded = oldCodec.decodeDiff
                (oldSrcVar, oldOffsetVar, null, newSrcVar, newOffsetVar, null,
                 newCodec, same);

            bitsVar.set(bitsVar.or(bitMapWordMask(slot)));
            same.here();

            // Need a stable copy.
            oldEndVar = mm.var(int.class).set(oldOffsetVar);
            newEndVar = mm.var(int.class).set(newOffsetVar);

            if (decoded == null || decoded[0] == null) {
                oldSource.mSrcVar = oldSrcVar;
                oldSource.mStartVar = oldStartVar;
                oldSource.mEndVar = oldEndVar;
            } else {
                oldSource.setDecodedVar(decoded[0]);
            }

            ColumnSource newSource = newColumnSources.get(newCodec.mInfo.name);

            if (decoded == null || decoded[1] == null) {
                newSource.mSrcVar = newSrcVar;
                newSource.mStartVar = newStartVar;
                newSource.mEndVar = newEndVar;
            } else {
                newSource.setDecodedVar(decoded[1]);
            }

            if (--num <= 0) {
                break;
            }
        }
    }

    /**
     * Each "word" is a long and holds 64 bits.
     */
    private static int numBitMapWords(Map<String, ColumnSource> sources) {
        return (sources.size() + 63) >> 6;
    }

    /**
     * Returns the zero-based bit map word ordinal.
     */
    private static int bitMapWord(int slot) {
        return slot >> 6;
    }

    /**
     * Returns a single bit shifted into the correct bit map word position.
     */
    private static long bitMapWordMask(int slot) {
        return 1L << (slot & 0x3f);
    }

    /**
     * Compute the bit mask to use in conjunction with diffColumns to determine which indexes
     * need to be updated.
     *
     * @param masks to fill in
     * @return false if index consists only of primary key columns
     */
    private static boolean bitMask(Map<String, ColumnSource> sources, RowInfo info, long[] masks) {
        Arrays.fill(masks, 0);

        boolean any = false;

        for (String name : info.allColumns.keySet()) {
            ColumnSource source = sources.get(name);
            if (!source.mFromKey) {
                any = true;
                int slot = source.mSlot;
                masks[bitMapWord(slot)] |= bitMapWordMask(slot);
            }
        }

        return any;
    }

    private void addInsertMethod() {
        MethodMaker mm = mClassMaker.addMethod
            (null, "insert", Transaction.class, Object.class, byte[].class, byte[].class).public_();

        var txnVar = mm.param(0);
        var rowVar = mm.param(1).cast(mRowClass);
        var keyVar = mm.param(2);
        var newValueVar = mm.param(3);

        findColumns(mm, keyVar, newValueVar, -1, ROW_FULL);

        // FIXME: As an optimization, when encoding complex columns (non-primitive), check if
        // prior secondary indexes have a matching codec and copy from them.

        for (int i=0; i<mSecondaryInfos.length; i++) {
            SecondaryInfo secondaryInfo = mSecondaryInfos[i];
            RowGen secondaryGen = secondaryInfo.rowGen();

            ColumnCodec[] secondaryKeyCodecs = ColumnCodec.bind(secondaryGen.keyCodecs(), mm);
            ColumnCodec[] secondaryValueCodecs = ColumnCodec.bind(secondaryGen.valueCodecs(), mm);

            var secondaryKeyVar = encodeColumns
                (mm, mColumnSources, rowVar, ROW_FULL, keyVar, newValueVar, secondaryKeyCodecs);

            var secondaryValueVar = encodeColumns
                (mm, mColumnSources, rowVar, ROW_FULL, keyVar, newValueVar, secondaryValueCodecs);

            var ixField = mm.field("ix" + i);

            if (!secondaryInfo.isAltKey) {
                ixField.invoke("store", txnVar, secondaryKeyVar, secondaryValueVar);
            } else {
                var result = ixField.invoke("insert", txnVar, secondaryKeyVar, secondaryValueVar);
                Label pass = mm.label();
                result.ifTrue(pass);
                mm.new_(UniqueConstraintException.class, "Alternate key").throw_();
                pass.here();
            }

            if (mBackfills[i] != null) {
                mm.field("backfill" + i).invoke
                    ("inserted", txnVar, secondaryKeyVar, secondaryValueVar);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void addDeleteMethod(RowStore rs, long primaryIndexId, boolean hasBackfills) {
        MethodMaker mm = mClassMaker.addMethod
            (null, "delete", Transaction.class, Object.class, byte[].class, byte[].class).public_();

        // The deletion of secondary indexes typically requires that the old value be
        // decoded. Given that the schema version can vary, don't fully implement this method
        // until needed. Create a new delegate for each schema version encountered.

        var txnVar = mm.param(0);
        var rowVar = mm.param(1).cast(mRowClass);
        var keyVar = mm.param(2);
        var oldValueVar = mm.param(3);

        var schemaVersion = TableMaker.decodeSchemaVersion(mm, oldValueVar);

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

        indy.invoke(null, "delete", null, schemaVersion, txnVar, rowVar, keyVar, oldValueVar);
    }

    /**
     * Bootstrap method for the trigger delete method.
     *
     * MethodType is:
     *
     *     void (int schemaVersion, Transaction txn, Row row, byte[] key, byte[] oldValueVar)
     */
    public static SwitchCallSite indyDelete(MethodHandles.Lookup lookup, String name,
                                            MethodType mt, WeakReference<RowStore> storeRef,
                                            Class<?> rowType, long indexId,
                                            byte[][] secondaryDescs, long[] secondaryIndexIds,
                                            WeakReference<IndexBackfill>[] backfillRefs)
    {
        Class<?> rowClass = mt.parameterType(2);

        return new SwitchCallSite(lookup, mt, schemaVersion -> {
            // Drop the schemaVersion parameter.
            var mtx = MethodType.methodType
                (void.class, Transaction.class, rowClass, byte[].class, byte[].class);

            RowStore store = storeRef.get();
            if (store == null) {
                var mm = MethodMaker.begin(lookup, "delete", mtx);
                mm.new_(DatabaseException.class, "Closed").throw_();
                return mm.finish();
            }

            RowInfo primaryInfo;
            Index[] secondaryIndexes;
            try {
                if (schemaVersion == 0) {
                    // When the schema version is zero, the primary value is empty. Since the
                    // primary key never changes, can lookup the canonical RowInfo instance.
                    primaryInfo = RowInfo.find(rowType);
                } else {
                    primaryInfo = store.rowInfo(rowType, indexId, schemaVersion);
                }
                secondaryIndexes = new Index[secondaryIndexIds.length];
                for (int i=0; i<secondaryIndexIds.length; i++) {
                    secondaryIndexes[i] = store.mDatabase.indexById(secondaryIndexIds[i]);
                }
            } catch (Exception e) {
                var mm = MethodMaker.begin(lookup, "delete", mtx);
                return new ExceptionCallSite.Failed(mtx, mm, e);
            }

            SecondaryInfo[] secondaryInfos = RowStore.indexRowInfos(primaryInfo, secondaryDescs);

            var maker = new IndexTriggerMaker<>
                (rowType, primaryInfo, secondaryInfos, secondaryIndexes);

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

            return maker.makeDeleteMethod(mtx, schemaVersion, backfills);
        });
    }

    private MethodHandle makeDeleteMethod(MethodType mt, int schemaVersion,
                                          IndexBackfill[] backfills)
    {
        mClassMaker = mPrimaryGen.beginClassMaker(IndexTriggerMaker.class, mRowType, "TriggerDel");
        mClassMaker.final_();

        MethodType ctorMethodType;
        if (backfills == null) {
            ctorMethodType = MethodType.methodType(void.class, Index[].class);
        } else {
            ctorMethodType = MethodType.methodType
                (void.class, Index[].class, IndexBackfill.class);
        }

        MethodMaker ctorMaker = mClassMaker.addConstructor(ctorMethodType);
        ctorMaker.invokeSuperConstructor();

        MethodMaker mm = mClassMaker.addMethod("delete", mt);

        mColumnSources = buildColumnSources();

        var txnVar = mm.param(0);
        var rowVar = mm.param(1);
        var keyVar = mm.param(2);
        var oldValueVar = mm.param(3);

        int valueOffset = schemaVersion == 0 ? 0 : (schemaVersion < 128 ? 1 : 4);
        findColumns(mm, keyVar, oldValueVar, valueOffset, ROW_KEY_ONLY);

        // FIXME: As an optimization, when encoding complex columns (non-primitive), check if
        // prior secondary indexes have a matching codec and copy from them.

        for (int i=0; i<mSecondaryInfos.length; i++) {
            Index ix = mSecondaryIndexes[i];
            if (ix == null) {
                // Index was dropped, so no need to delete anything from it either.
                continue;
            }

            String ixFieldName = "ix" + i;
            mClassMaker.addField(Index.class, ixFieldName).private_().final_();

            ctorMaker.field(ixFieldName).set(ctorMaker.param(0).aget(i));

            String backfillFieldName = null;
            if (backfills != null && backfills[i] != null) {
                backfillFieldName = "backfill" + i;
                mClassMaker.addField(IndexBackfill.class, backfillFieldName)
                    .private_().final_();
                ctorMaker.field(backfillFieldName).set(ctorMaker.param(1).aget(i));
            }

            RowGen secondaryGen = mSecondaryInfos[i].rowGen();

            ColumnCodec[] secondaryKeyCodecs = ColumnCodec.bind(secondaryGen.keyCodecs(), mm);

            var secondaryKeyVar = encodeColumns
                (mm, mColumnSources, rowVar, ROW_KEY_ONLY, keyVar, oldValueVar, secondaryKeyCodecs);

            mm.field(ixFieldName).invoke("store", txnVar, secondaryKeyVar, null);

            if (backfillFieldName != null) {
                mm.field(backfillFieldName).invoke("deleted", txnVar, secondaryKeyVar);
            }
        }

        var lookup = mClassMaker.finishHidden();
        var clazz = lookup.lookupClass();

        try {
            var ctor = lookup.findConstructor(clazz, ctorMethodType);
            Object deleter;
            if (backfills == null) {
                deleter = ctor.invoke(mSecondaryIndexes);
            } else {
                deleter = ctor.invoke(mSecondaryIndexes, backfills);
            }
            return lookup.findVirtual(clazz, "delete", mt).bindTo(deleter);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    /**
     * @param variant "store" or "update"
     */
    private void addStoreMethod(String variant) {
        MethodMaker mm = mClassMaker.addMethod
            (null, variant, Transaction.class, Object.class,
             byte[].class, byte[].class, byte[].class).public_();

        Map<String, ColumnSource> oldColumnSources = cloneColumnSources();
        Map<String, ColumnSource> newColumnSources = cloneColumnSources();

        var bitMap = new Variable[numBitMapWords(oldColumnSources)];
        var allMasks = new long[mSecondaryIndexes.length][];
        var skipIndex = new boolean[mSecondaryIndexes.length];

        boolean isUpdate = variant == "update";

        // Determine which indexes should be skipped, and if the "update" variant, determine if
        // it does the exact same thing as the "store" variant.
        boolean sameAsStore = true;
        for (int i=0; i<mSecondaryInfos.length; i++) {
            RowInfo secondaryInfo = mSecondaryInfos[i];

            var masks = new long[bitMap.length];
            if (!bitMask(oldColumnSources, secondaryInfo, masks)) {
                skipIndex[i] = true;
                continue;
            }

            allMasks[i] = masks;

            if (!isUpdate) {
                continue;
            }

            RowGen secondaryGen = secondaryInfo.rowGen();

            // See comments in requiresColumnChecks regarding side-effects. This also requires that
            // newColumnSources be a clone of mColumnSources, which is later discarded.
            if (requiresColumnChecks(mm, newColumnSources, secondaryGen.keyCodecs()) ||
                requiresColumnChecks(mm, newColumnSources, secondaryGen.valueCodecs()))
            {
                sameAsStore = false;
            }

            // Called for the side-effects, and so oldColumnSources must also be a clone.
            requiresColumnChecks(mm, oldColumnSources, secondaryGen.keyCodecs());
        }

        if (isUpdate && sameAsStore) {
            // No need to create duplicate methods.
            mm.invoke("store", mm.param(0), mm.param(1), mm.param(2), mm.param(3), mm.param(4));
            return;
        }

        var txnVar = mm.param(0);
        var rowVar = mm.param(1).cast(mRowClass);
        var keyVar = mm.param(2);
        var oldValueVar = mm.param(3);
        var newValueVar = mm.param(4);

        diffColumns(mm, bitMap, oldColumnSources, newColumnSources, oldValueVar, newValueVar);

        // Need to prepare columns needed by performJitDecode.
        for (int i=0; i<mSecondaryInfos.length; i++) {
            if (skipIndex[i]) {
                continue;
            }
            RowGen secondaryGen = mSecondaryInfos[i].rowGen();
            for (Map.Entry<String, ColumnCodec> e : secondaryGen.keyCodecMap().entrySet()) {
                oldColumnSources.get(e.getKey()).prepareJitDecode(mm, ROW_KEY_ONLY, e.getValue());
            }
        }

        // FIXME: As an optimization, when encoding complex columns (non-primitive), check if
        // prior secondary indexes have a matching codec and copy from them.

        for (int i=0; i<mSecondaryInfos.length; i++) {
            if (skipIndex[i]) {
                continue;
            }

            Label modified = mm.label();

            long[] masks = allMasks[i];
            for (int j=0; j<bitMap.length; j++) {
                bitMap[j].and(masks[j]).ifNe(0L, modified);
            }

            Label cont = mm.label();
            mm.goto_(cont);

            // Index needs to be updated. Delete the old entry and insert a new one.
            modified.here();

            SecondaryInfo secondaryInfo = mSecondaryInfos[i];
            RowGen secondaryGen = secondaryInfo.rowGen();
            Field ixField = mm.field("ix" + i);

            {
                // Perform "just-in-time" decoding for some or all of the columns that make up
                // the key to delete.
                for (Map.Entry<String, ColumnCodec> e : secondaryGen.keyCodecMap().entrySet()) {
                    oldColumnSources.get(e.getKey()).performJitDecode(mm);
                }

                ColumnCodec[] secondaryKeyCodecs = ColumnCodec.bind(secondaryGen.keyCodecs(), mm);

                var secondaryKeyVar = encodeColumns
                    (mm, oldColumnSources, rowVar, ROW_KEY_ONLY,
                     keyVar, oldValueVar, secondaryKeyCodecs);

                ixField.invoke("store", txnVar, secondaryKeyVar, null);

                if (mBackfills[i] != null) {
                    mm.field("backfill" + i).invoke("deleted", txnVar, secondaryKeyVar);
                }
            }

            // Now insert a new entry.

            ColumnCodec[] secondaryKeyCodecs = ColumnCodec.bind(secondaryGen.keyCodecs(), mm);
            ColumnCodec[] secondaryValueCodecs = ColumnCodec.bind(secondaryGen.valueCodecs(), mm);

            var secondaryKeyVar = encodeColumns
                (mm, newColumnSources, rowVar, ROW_FULL, keyVar, newValueVar, secondaryKeyCodecs);

            var secondaryValueVar = encodeColumns
                (mm, newColumnSources, rowVar, ROW_FULL, keyVar, newValueVar, secondaryValueCodecs);

            if (!secondaryInfo.isAltKey) {
                ixField.invoke("store", txnVar, secondaryKeyVar, secondaryValueVar);
            } else {
                var result = ixField.invoke("insert", txnVar, secondaryKeyVar, secondaryValueVar);
                Label pass = mm.label();
                result.ifTrue(pass);
                mm.new_(UniqueConstraintException.class, "Alternate key").throw_();
                pass.here();
            }

            if (mBackfills[i] != null) {
                mm.field("backfill" + i).invoke
                    ("inserted", txnVar, secondaryKeyVar, secondaryValueVar);
            }

            cont.here();
        }
    }

    /**
     * Calls requiresColumnCheck for all sources used by secondary indexes. This method is only
     * expected to be called for the "update" trigger variant.
     *
     * @param codecs secondary key or value codecs
     */
    private boolean requiresColumnChecks(MethodMaker mm, Map<String, ColumnSource> columnSources,
                                         ColumnCodec[] codecs)
    {
        boolean result = false;
        for (ColumnCodec codec : codecs) {
            // Because the requiresColumnCheck method can have a side-effect (creating new
            // variables), cannot short-circuit when true is returned. All sources must be
            // called to ensure that the side-effects are applied.
            result |= columnSources.get(codec.mInfo.name).requiresColumnCheck(mm, codec);
        }
        return result;
    }

    /**
     * Generate a key or value for a secondary index.
     *
     * @param rowVar primary row object, filled in according to rowMode
     * @param rowMode see ROW_* fields
     * @param keyVar primary key byte[], fully specified
     * @param valueVar primary value byte[], fully specified
     * @param codecs secondary key or value codecs, bound to MethodMaker
     * @return a filled-in byte[] variable
     */
    private Variable encodeColumns(MethodMaker mm, Map<String, ColumnSource> columnSources,
                                   Variable rowVar, int rowMode,
                                   Variable keyVar, Variable valueVar, ColumnCodec[] codecs)
    {
        assert rowVar != null || rowMode == ROW_NONE;

        if (codecs.length == 0) {
            return mm.var(RowUtils.class).field("EMPTY_BYTES");
        }

        // Determine the minimum byte array size and prepare the encoders.

        int minSize = 0;
        for (ColumnCodec codec : codecs) {
            ColumnSource source = columnSources.get(codec.mInfo.name);
            if (!source.shouldCopyBytes(codec)) {
                minSize += codec.minSize();
                codec.encodePrepare();
                // Note that prepareColumn only does anything for the "update" variant.
                source.prepareColumn(mm, mPrimaryGen, rowVar, rowMode);
            }
        }

        // Generate code which determines the additional runtime length.

        Variable totalVar = null;
        for (ColumnCodec codec : codecs) {
            ColumnSource source = columnSources.get(codec.mInfo.name);
            if (!source.shouldCopyBytes(codec)) {
                totalVar = codec.encodeSize(source.accessColumn(rowVar), totalVar);
            } else {
                // FIXME: Detect spans and reduce additions.
                var lengthVar = source.mEndVar.sub(source.mStartVar);
                if (totalVar == null) {
                    totalVar = lengthVar;
                } else {
                    totalVar = totalVar.add(lengthVar);
                }
            }
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
        for (int i=0; i<codecs.length; i++) {
            ColumnCodec codec = codecs[i];
            ColumnSource source = columnSources.get(codec.mInfo.name);
            if (!source.shouldCopyBytes(codec)) {
                codec.encode(source.accessColumn(rowVar), dstVar, offsetVar);
            } else {
                // FIXME: Detect spans and reduce copies.
                Variable bytesVar = source.mFromKey ? keyVar : valueVar;
                var lengthVar = source.mEndVar.sub(source.mStartVar);
                mm.var(System.class).invoke
                    ("arraycopy", bytesVar, source.mStartVar, dstVar, offsetVar, lengthVar);
                if (i < codecs.length - 1) {
                    offsetVar.inc(lengthVar);
                }
            }
        }

        return dstVar;
    }

    private static class ColumnSource {
        final int mSlot;

        // Describes the encoding of the column within the primary key or primary value.
        final ColumnCodec mCodec;

        // When true, the column is in the primary key, else in the primary value.
        final boolean mFromKey;

        // Number of secondary indexes that can use the codec directly; encoding is the same.
        int mMatches;

        // Number of secondary indexes that need transformed columns.
        int mMismatches;

        // The remaining fields are used during code generation passes.

        // Source byte array and offsets. These are set when the column can be copied directly
        // without any transformation.
        Variable mSrcVar, mStartVar, mEndVar;

        // Fully decoded column. Is set when the column cannot be directly copied from the
        // source, and it needed a transformation step. Or when decoding is cheap.
        Variable mDecodedVar;

        // Variable which indicates that mDecodedVar is assigned. Isn't used when mDecodedVar
        // was assigned before the first call to accessColumn. If is the same is mDecodedVar,
        // then a null check at runtime is used to detect if the column has been decoded yet.
        // Otherwise, mJitDecodedVar is a boolean. (jit == just-in-time)
        Variable mJitDecodedVar;

        // Set as a side-effect of calling requiresColumnCheck.
        Checked mChecked;

        ColumnSource(int slot, ColumnCodec codec, boolean fromKey) {
            mSlot = slot;
            mCodec = codec;
            mFromKey = fromKey;
        }

        /**
         * Copy constructor.
         */
        ColumnSource(ColumnSource source) {
            mSlot = source.mSlot;
            mCodec = source.mCodec;
            mFromKey = source.mFromKey;
            mMatches = source.mMatches;
            mMismatches = source.mMismatches;
        }

        boolean isPrimitive() {
            return mCodec instanceof PrimitiveColumnCodec;
        }

        /**
         * Returns true if the column must be found in the encoded byte array.
         *
         * @param rowMode see ROW_* fields
         */
        boolean mustFind(int rowMode) {
            if (rowMode == ROW_FULL || (rowMode == ROW_KEY_ONLY && mFromKey)) {
                if (isPrimitive()) {
                    // Primitive columns are cheap to encode, so no need to copy the byte form.
                    return false;
                } else {
                    // Return true if at least one secondary index can copy the byte form,
                    // avoiding the cost of encoding the column from the row object.
                    return mMatches != 0;
                }
            } else {
                // Without a valid column in the row, must find it in the byte form.
                return true;
            }
        }

        /**
         * Returns true if the location of the encoded column is known and the codec matches
         * the destination codec.
         *
         * @param dstCodec codec of secondary index
         */
        boolean shouldCopyBytes(ColumnCodec dstCodec) {
            return mSrcVar != null && mCodec.equals(dstCodec);
        }

        /**
         * Determines if a row column might need to be checked at runtime if valid or not.
         *
         * - If the column value can be copied in binary form, then the row object isn't
         *   examined and this method returns false.
         *
         * - If the column is part of the primary key, then it's always assumed to be valid,
         *   and so false is returned.
         *
         * This method is only expected to be called for the "update" trigger variant, which
         * cannot always trust that row fields are usable. When they're marked dirty, they can
         * be trusted because they were encoded into the new binary value.
         *
         * When true is returned, additional variables are created for tracking runtime state.
         *
         * @param dstCodec codec of secondary index
         */
        boolean requiresColumnCheck(MethodMaker mm, ColumnCodec dstCodec) {
            if (shouldCopyBytes(dstCodec) || mFromKey) {
                return false;
            }

            Checked checked = mChecked;

            if (checked == null) {
                mChecked = checked = new Checked();
                checked.mColumnVar = mm.var(mCodec.mInfo.type);
                if (mCodec.mInfo.isPrimitive() || mCodec.mInfo.isNullable()) {
                    // Cannot use null to detect if column hasn't been prepared yet.
                    checked.mCheckVar = mm.var(boolean.class);
                }
            } else if (checked.mUsageCount == 1) {
                // Need to initialize these variables to ensure definite assignment.
                if (checked.mCheckVar != null) {
                    checked.mCheckVar.set(false);
                } else {
                    checked.mColumnVar.set(null);
                }
            }

            checked.mUsageCount++;

            return true;
        }

        void setDecodedVar(Variable var) {
            mDecodedVar = var;
            // Won't need column check when it got decoded anyhow. If it initialized some extra
            // variables, they'll be dead stores, so assume that the compiler eliminates them.
            mChecked = null;
        }

        /**
         * If requiresColumnCheck was called earlier and returned true, this method ensures
         * that the column is decoded or copied from the row. Must be called before accessColumn.
         *
         * @param rowVar primary row; pass null to never access the row field
         * @throws IllegalStateException if column check is required but location of encoded
         * column is unknown
         */
        void prepareColumn(MethodMaker mm, RowGen primaryGen, Variable rowVar, int rowMode) {
            Checked checked = mChecked;

            if (checked == null) {
                return;
            }

            if (mSrcVar == null) {
                // Cannot decode if location is unknown.
                throw new IllegalStateException("" + mDecodedVar);
            }

            checked.mPrepareCount++;

            Label cont = mm.label();

            if (checked.mPrepareCount > 1) {
                if (checked.mPrepareCount > checked.mUsageCount) {
                    throw new AssertionError();
                }
                // Might already have been prepared.
                if (checked.mCheckVar != null) {
                    checked.mCheckVar.ifTrue(cont);
                } else {
                    checked.mColumnVar.ifNe(null, cont);
                }
            }

            if (checked.mCheckVar != null) {
                checked.mCheckVar.set(true);
            }

            if (rowVar != null && rowMode == ROW_FULL) {
                String columnName = mCodec.mInfo.name;
                int columnNum = primaryGen.columnNumbers().get(columnName);
                String stateField = primaryGen.stateField(columnNum);
                int stateFieldMask = primaryGen.stateFieldMask(columnNum);
                Label mustDecode = mm.label();
                rowVar.field(stateField).and(stateFieldMask).ifNe(stateFieldMask, mustDecode);
                checked.mColumnVar.set(rowVar.field(columnName));
                mm.goto_(cont);
                mustDecode.here();
            }

            // The decode method modifies the offset, so operate against a copy.
            var offsetVar = mStartVar.get();
            mCodec.bind(mm).decode(checked.mColumnVar, mSrcVar, offsetVar, null);
            
            cont.here();
        }

        /**
         * Prepare a column for "just-in-time" decoding, which decodes from the binary form to
         * the object form when first required by an index. This defines the necessary extra
         * variables and initializes them to be definitely assigned.
         *
         * In general, this method is used for columns which:
         *
         *  - are a component of a secondary index key to be deleted
         *  - aren't in the primary row object
         *  - have a different encoding format than the primary row
         *  - aren't primitive types (primitive types are eagerly decoded)
         */
        void prepareJitDecode(MethodMaker mm, int rowMode, ColumnCodec dstCodec) {
            if (mDecodedVar == null && !shouldCopyBytes(dstCodec) &&
                (rowMode == ROW_NONE || (rowMode == ROW_KEY_ONLY && !mFromKey)))
            {
                mDecodedVar = mm.var(mCodec.mInfo.type).clear();

                if (mCodec.mInfo.isPrimitive() || mCodec.mInfo.isNullable()) {
                    mJitDecodedVar = mm.var(boolean.class).set(false);
                } else {
                    // Can use null to indicate that column hasn't been decoded yet.
                    mJitDecodedVar = mDecodedVar;
                }
            }
        }

        /**
         * Use in conjunction with prepareJitDecode to decode a column when first needed at
         * runtime. Subsequent requests for the same column don't need to decode it again.
         */
        void performJitDecode(MethodMaker mm) {
            if (mJitDecodedVar == null) {
                return;
            }

            Label cont = mm.label();

            if (mJitDecodedVar != mDecodedVar) {
                mJitDecodedVar.ifTrue(cont);
            } else {
                mDecodedVar.ifNe(null, cont);
            }

            // The decode method modifies the offset, so operate against a copy.
            var offsetVar = mStartVar.get();
            mCodec.bind(mm).decode(mDecodedVar, mSrcVar, offsetVar, mEndVar);

            if (mJitDecodedVar != mDecodedVar) {
                mJitDecodedVar.set(true);
            }

            cont.here();
        }

        /**
         * @param rowVar primary row
         * @return a decoded variable or a field from the row
         */
        Variable accessColumn(Variable rowVar) {
            Variable colVar = mDecodedVar;
            if (colVar == null) {
                Checked checked = mChecked;
                if (checked != null) {
                    colVar = checked.mColumnVar;
                } else {
                    colVar = rowVar.field(mCodec.mInfo.name);
                }
            }
            return colVar;
        }

        void clearVars() {
            mSrcVar = null;
            mStartVar = null;
            mEndVar = null;
            mDecodedVar = null;
            mJitDecodedVar = null;
        }

        /**
         * State to check if a row field is valid or not at runtime.
         */
        static class Checked {
            // Count of indexes that depend on this.
            int mUsageCount;

            // Variable which holds the column value, either from the row, or decoded from the
            // binary representation.
            Variable mColumnVar;

            // Optional boolean variable for checking if mColumnVar has been prepared yet. When
            // mCheckVar is null, then a runtime comparison of mColumnVar to null is performed.
            Variable mCheckVar;

            // Count of times that prepareColumn has been called against this.
            int mPrepareCount;
        }
    }
}
