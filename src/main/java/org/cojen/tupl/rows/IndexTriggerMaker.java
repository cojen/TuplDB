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

import java.util.HashMap;
import java.util.Map;

import org.cojen.tupl.DatabaseException;
import org.cojen.tupl.Index;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UniqueConstraintException;

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
 * @see IndexManager
 */
public class IndexTriggerMaker<R> {
    private final Class<R> mRowType;
    private final Class<? extends R> mRowClass;
    private final RowGen mPrimaryGen;

    // To be filled in by caller (IndexManager).
    final byte[][] mSecondaryDescriptors;
    final SecondaryInfo[] mSecondaryInfos;
    final Index[] mSecondaryIndexes;
    final byte[] mSecondaryStates; // FIXME: remember to build indexes

    // Map for all the columns needed by all of the secondary indexes.
    private Map<String, ColumnSource> mColumnSources;

    private ClassMaker mClassMaker;

    IndexTriggerMaker(Class<R> rowType, RowInfo primaryInfo, int numIndexes) {
        mRowType = rowType;
        mRowClass = RowMaker.find(rowType);
        mPrimaryGen = primaryInfo.rowGen();

        mSecondaryDescriptors = new byte[numIndexes][];
        mSecondaryInfos = new SecondaryInfo[numIndexes];
        mSecondaryIndexes = new Index[numIndexes];
        mSecondaryStates = new byte[numIndexes];
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
        mSecondaryStates = null;
    }

    /**
     * @param primaryIndexId primary index id
     */
    Trigger<R> make(RowStore rs, long primaryIndexId) {
        mClassMaker = mPrimaryGen.beginClassMaker(IndexTriggerMaker.class, mRowType, "Trigger");
        mClassMaker.extend(Trigger.class).final_();

        for (int i=0; i<mSecondaryIndexes.length; i++) {
            mClassMaker.addField(Index.class, "ix" + i).private_().final_();
        }

        {
            MethodMaker mm = mClassMaker.addConstructor(Index[].class);
            mm.invokeSuperConstructor();
            for (int i=0; i<mSecondaryIndexes.length; i++) {
                mm.field("ix" + i).set(mm.param(0).aget(i));
            }
        }

        mColumnSources = buildColumnSources();

        addInsertMethod();

        addDeleteMethod(rs, primaryIndexId);

        addStoreMethod("store");

        addStoreMethod("update");

        var lookup = mClassMaker.finishHidden();

        try {
            var mt = MethodType.methodType(void.class, Index[].class);
            var ctor = lookup.findConstructor(lookup.lookupClass(), mt);
            return (Trigger<R>) ctor.invoke(mSecondaryIndexes);
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    private Map<String, ColumnSource> buildColumnSources() {
        Map<String, ColumnCodec> keyCodecMap = mPrimaryGen.keyCodecMap();
        Map<String, ColumnCodec> valueCodecMap = mPrimaryGen.valueCodecMap();

        var sources = new HashMap<String, ColumnSource>();

        for (RowInfo secondaryInfo : mSecondaryInfos) {
            RowGen secondaryGen = secondaryInfo.rowGen();
            ColumnCodec[] secondaryKeyCodecs = secondaryGen.keyCodecs();
            ColumnCodec[] secondaryValueCodecs = secondaryGen.valueCodecs();

            buildColumnSources(keyCodecMap, true, sources, secondaryKeyCodecs);
            buildColumnSources(keyCodecMap, true, sources, secondaryValueCodecs);

            buildColumnSources(valueCodecMap, false, sources, secondaryKeyCodecs);
            buildColumnSources(valueCodecMap, false, sources, secondaryValueCodecs);
        }

        return sources;
    }

    /**
     * @param primaryCodecMap
     * @param sources results stored here
     * @param secondaryCodecs secondary index columns which need to be found
     */
    private void buildColumnSources(Map<String, ColumnCodec> primaryCodecMap, boolean fromKey,
                                    Map<String, ColumnSource> sources,
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
     * @param fullRow when false, only key fields of the row can be used
     */
    private void findColumns(MethodMaker mm, Map<String, ColumnSource> columnSources,
                             Variable keyVar, Variable valueVar, int valueOffset, boolean fullRow)
    {
        // Determine how many columns must be accessed from the encoded form.

        int remainingKeys = 0, remainingValues = 0;
        for (ColumnSource source : columnSources.values()) {
            source.clearVars();
            if (source.mustFind(fullRow)) {
                if (source.mFromKey) {
                    remainingKeys++;
                } else {
                    remainingValues++;
                }
            }
        }

        findColumns(mm, columnSources, keyVar, 0, true, fullRow, remainingKeys);

        findColumns(mm, columnSources, valueVar, valueOffset, false, fullRow, remainingValues);
    }

    /**
     * Generates code which finds column offsets in the encoded primary row. As a side-effect,
     * the Variable fields of all ColumnSources are updated.
     *
     * @param srcVar byte array
     * @param offset initial offset into byte array, or -1 to auto skip schema version
     * @param forKey true if the byte array is an encoded key
     * @param fullRow when false, only key fields of the row can be used
     * @param remaining number of columns to decode/skip from the byte array
     */
    private void findColumns(MethodMaker mm, Map<String, ColumnSource> columnSources,
                             Variable srcVar, int offset,
                             boolean forKey, boolean fullRow, int remaining)
    {
        if (remaining == 0) {
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
            ColumnSource source = columnSources.get(codec.mInfo.name);

            findCheck: {
                if (source != null) {
                    if (source.mFromKey != forKey) {
                        throw new AssertionError();
                    }
                    if (source.mustFind(fullRow)) {
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

            if (fullRow || source.mFromKey || source.mMismatches == 0) {
                // Can skip decoding if the column can be found in the row or if it doesn't
                // need to be transformed.
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

            if (--remaining <= 0) {
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

        int remainingValues = 0;
        for (ColumnSource source : oldColumnSources.values()) {
            source.clearVars();
            if (!source.mFromKey) {
                remainingValues++;
            }
        }

        for (ColumnSource source : newColumnSources.values()) {
            source.clearVars();
            // ColumnSources are expected to be equivalent, so don't double count remaining
            // values. Two source instances are used to track separate sets of Variables.
        }

        diffColumns(mm, oldColumnSources, newColumnSources,
                    oldSrcVar, newSrcVar, bitMap, false, remainingValues);

        return bitMap;
    }

    private void diffColumns(MethodMaker mm,
                             Map<String, ColumnSource> oldColumnSources,
                             Map<String, ColumnSource> newColumnSources,
                             Variable oldSrcVar, Variable newSrcVar,
                             Variable[] bitMap, boolean forKey, int remaining)
    {
        if (remaining == 0) {
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

            if (--remaining <= 0) {
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
        for (int i=0; i<masks.length; i++) {
            masks[i] = 0;
        }

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

        findColumns(mm, mColumnSources, keyVar, newValueVar, -1, true);

        // FIXME: As an optimization, when encoding complex columns (non-primitive), check if
        // prior secondary indexes have a matching codec and copy from them.

        for (int i=0; i<mSecondaryInfos.length; i++) {
            SecondaryInfo secondaryInfo = mSecondaryInfos[i];
            RowGen secondaryGen = secondaryInfo.rowGen();

            ColumnCodec[] secondaryKeyCodecs = ColumnCodec.bind(secondaryGen.keyCodecs(), mm);
            ColumnCodec[] secondaryValueCodecs = ColumnCodec.bind(secondaryGen.valueCodecs(), mm);

            var secondaryKeyVar = encodeColumns
                (mm, mColumnSources, rowVar, true, keyVar, newValueVar, secondaryKeyCodecs);

            var secondaryValueVar = encodeColumns
                (mm, mColumnSources, rowVar, true, keyVar, newValueVar, secondaryValueCodecs);

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
        }
    }

    private void addDeleteMethod(RowStore rs, long primaryIndexId) {
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

        var indy = mm.var(IndexTriggerMaker.class).indy
            ("indyDelete", rs.ref(), mRowType, primaryIndexId,
             mSecondaryDescriptors, secondaryIndexIds);
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
                                            byte[][] secondaryDescs, long[] secondaryIndexIds)
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

            if (schemaVersion == 0) {
                // When the schema version is zero, the primary value is empty.
                throw new RuntimeException("FIXME: handle schemaVersion 0");
            }

            RowInfo primaryInfo;
            var secondaryIndexes = new Index[secondaryIndexIds.length];
            try {
                primaryInfo = store.rowInfo(rowType, indexId, schemaVersion);
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

            return maker.makeDeleteMethod(mtx, schemaVersion);
        });
    }

    private MethodHandle makeDeleteMethod(MethodType mt, int schemaVersion) {
        mClassMaker = mPrimaryGen.beginClassMaker(IndexTriggerMaker.class, mRowType, "Trigger");
        mClassMaker.final_();

        MethodMaker ctorMaker = mClassMaker.addConstructor(Index[].class);
        ctorMaker.invokeSuperConstructor();

        MethodMaker mm = mClassMaker.addMethod("delete", mt);

        mColumnSources = buildColumnSources();

        var txnVar = mm.param(0);
        var rowVar = mm.param(1);
        var keyVar = mm.param(2);
        var oldValueVar = mm.param(3);

        findColumns(mm, mColumnSources, keyVar, oldValueVar, schemaVersion < 128 ? 1 : 4, false);

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

            RowGen secondaryGen = mSecondaryInfos[i].rowGen();

            ColumnCodec[] secondaryKeyCodecs = ColumnCodec.bind(secondaryGen.keyCodecs(), mm);

            var secondaryKeyVar = encodeColumns
                (mm, mColumnSources, rowVar, false, keyVar, oldValueVar, secondaryKeyCodecs);

            mm.field(ixFieldName).invoke("store", txnVar, secondaryKeyVar, null);
        }

        var lookup = mClassMaker.finishHidden();
        var clazz = lookup.lookupClass();

        try {
            Object deleter = lookup.findConstructor
                (clazz, MethodType.methodType(void.class, Index[].class))
                .invoke(mSecondaryIndexes);

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
        var masks = new long[bitMap.length];
        var skipIndex = new boolean[mSecondaryIndexes.length];

        boolean isUpdate = variant == "update";

        // Determine which indexes should be skipped, and if the "update" variant, determine if
        // it does the exact same thing as the "store" variant.
        boolean sameAsStore = true;
        for (int i=0; i<mSecondaryInfos.length; i++) {
            RowInfo secondaryInfo = mSecondaryInfos[i];

            if (!bitMask(oldColumnSources, secondaryInfo, masks)) {
                skipIndex[i] = true;
                continue;
            }

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

        // FIXME: As an optimization, when encoding complex columns (non-primitive), check if
        // prior secondary indexes have a matching codec and copy from them.

        for (int i=0; i<mSecondaryInfos.length; i++) {
            if (skipIndex[i]) {
                continue;
            }

            Label modified = mm.label();

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
                ColumnCodec[] secondaryKeyCodecs = ColumnCodec.bind(secondaryGen.keyCodecs(), mm);

                var secondaryKeyVar = encodeColumns
                    (mm, oldColumnSources, rowVar, false, keyVar, oldValueVar, secondaryKeyCodecs);

                ixField.invoke("store", txnVar, secondaryKeyVar, null);
            }

            // Now insert a new entry.

            ColumnCodec[] secondaryKeyCodecs = ColumnCodec.bind(secondaryGen.keyCodecs(), mm);
            ColumnCodec[] secondaryValueCodecs = ColumnCodec.bind(secondaryGen.valueCodecs(), mm);

            var secondaryKeyVar = encodeColumns
                (mm, newColumnSources, rowVar, true, keyVar, newValueVar, secondaryKeyCodecs);

            var secondaryValueVar = encodeColumns
                (mm, newColumnSources, rowVar, true, keyVar, newValueVar, secondaryValueCodecs);

            if (!secondaryInfo.isAltKey) {
                ixField.invoke("store", txnVar, secondaryKeyVar, secondaryValueVar);
            } else {
                var result = ixField.invoke("insert", txnVar, secondaryKeyVar, secondaryValueVar);
                Label pass = mm.label();
                result.ifTrue(pass);
                mm.new_(UniqueConstraintException.class, "Alternate key").throw_();
                pass.here();
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
     * @param rowVar primary row object, fully specified
     * @param fullRow when false, only key fields of the row can be used
     * @param keyVar primary key byte[], fully specified
     * @param valueVar primary value byte[], fully specified
     * @param codecs secondary key or value codecs, bound to MethodMaker
     * @return a filled-in byte[] variable
     */
    private Variable encodeColumns(MethodMaker mm, Map<String, ColumnSource> columnSources,
                                   Variable rowVar, boolean fullRow,
                                   Variable keyVar, Variable valueVar, ColumnCodec[] codecs)
    {
        if (codecs.length == 0) {
            return mm.var(RowUtils.class).field("EMPTY_BYTES");
        }

        // Determine the minimum byte array size and prepare the encoders.

        int minSize = 0;
        for (int i=0; i<codecs.length; i++) {
            ColumnCodec codec = codecs[i];
            ColumnSource source = columnSources.get(codec.mInfo.name);
            if (!source.shouldCopy(codec)) {
                minSize += codec.minSize();
                codec.encodePrepare();
                source.prepareColumn(mm, mPrimaryGen, fullRow ? rowVar : null);
            }
        }

        // Generate code which determines the additional runtime length.

        Variable totalVar = null;
        for (int i=0; i<codecs.length; i++) {
            ColumnCodec codec = codecs[i];
            ColumnSource source = columnSources.get(codec.mInfo.name);
            if (!source.shouldCopy(codec)) {
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
            if (!source.shouldCopy(codec)) {
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
         */
        boolean mustFind(boolean fullRow) {
            if (fullRow || mFromKey) {
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
        boolean shouldCopy(ColumnCodec dstCodec) {
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
            if (shouldCopy(dstCodec) || mFromKey) {
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
            // varaibles, they'll be dead stores, so assume that the compiler eliminates them.
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
        void prepareColumn(MethodMaker mm, RowGen primaryGen, Variable rowVar) {
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

            if (rowVar != null) {
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
