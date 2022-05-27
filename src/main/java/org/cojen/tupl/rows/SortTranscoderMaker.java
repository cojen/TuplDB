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

package org.cojen.tupl.rows;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.lang.ref.WeakReference;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.cojen.tupl.DatabaseException;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

/**
 * Makes totally ordered Transcoders suitable for feeding entries into a Sorter.
 *
 * @author Brian S O'Neill
 */
public class SortTranscoderMaker<R> {
    private final RowStore mRowStore;
    private final Class<?> mRowType;
    private final Class<?> mRowClass;
    private final RowInfo mInfo;
    private final long mIndexId;
    private final Set<String> mProjection;
    private final RowInfo mTargetInfo;

    /**
     * @param rowClass current row implementation
     * @param info source info; can be a primary RowInfo or a SecondaryInfo
     * @param indexId source index; only used for primary RowInfo
     * @param available columns available in the source rows; can pass null if all are available
     * @param orderBy ordering specification
     * @param projection columns to keep; can pass null to project all available columns
     * @throws IllegalArgumentException if any orderBy or projected columns aren't available
     */
    SortTranscoderMaker(RowStore rs,
                        Class<?> rowType, Class<?> rowClass, RowInfo info, long indexId,
                        Set<String> available, OrderBy orderBy, Set<String> projection)
    {
        if (available == null) {
            available = info.allColumns.keySet();
        }
        if (projection == null) {
            projection = available;
        }

        mRowStore = rs;
        mRowType = rowType;
        mRowClass = rowClass;
        mInfo = info;
        mIndexId = indexId;
        mProjection = projection;

        var targetInfo = new RowInfo(info.name);
        targetInfo.keyColumns = new LinkedHashMap<>();

        for (Map.Entry<String, OrderBy.Rule> e : orderBy.entrySet()) {
            ColumnInfo orderColumn = e.getValue().asColumn();
            if (!available.contains(orderColumn.name)) {
                throw new IllegalArgumentException();
            }
            targetInfo.keyColumns.put(orderColumn.name, orderColumn);
        }

        boolean hasDuplicates = false;

        for (ColumnInfo keyColumn : info.keyColumns.values()) {
            if (!available.contains(keyColumn.name)) {
                hasDuplicates = true;
            } else if (!targetInfo.keyColumns.containsKey(keyColumn.name)) {
                targetInfo.keyColumns.put(keyColumn.name, keyColumn);
            }
        }

        if (!hasDuplicates) {
            // All of the primary key columns are part of the target key, and so no duplicates
            // can exist. Define the remaining projected columns in the target value.

            for (String colName : projection) {
                if (!available.contains(colName)) {
                    throw new IllegalArgumentException();
                }
                if (!targetInfo.keyColumns.containsKey(colName)) {
                    if (targetInfo.valueColumns == null) {
                        targetInfo.valueColumns = new TreeMap<>();
                    }
                    targetInfo.valueColumns.put(colName, info.allColumns.get(colName));
                }
            }
        } else {
            // Because duplicate keys can exist, define all available columns in the target
            // key. This doesn't fully prevent duplicates, and so any extra rows will be
            // eliminated by the sorter. For supporting proper "select distinct" behavior, the
            // given available set must be the same as the projected set.

            for (String colName : available) {
                if (!targetInfo.keyColumns.containsKey(colName)) {
                    targetInfo.keyColumns.put(colName, info.allColumns.get(colName));
                }
            }
        }

        if (targetInfo.valueColumns == null) {
            targetInfo.valueColumns = Collections.emptyNavigableMap();
        }

        targetInfo.allColumns = new TreeMap<>();
        targetInfo.allColumns.putAll(targetInfo.keyColumns);
        targetInfo.allColumns.putAll(targetInfo.valueColumns);

        mTargetInfo = targetInfo;
    }

    Transcoder<R> finish() {
        ClassMaker cm = RowGen.beginClassMaker
            (SortTranscoderMaker.class, null, mTargetInfo, null, null)
            .implement(Transcoder.class).public_();

        cm.addConstructor().private_();

        addTranscodeMethod(cm);
        addDecodeRowMethod(cm);

        MethodHandles.Lookup lookup = cm.finishHidden();

        try {
            MethodHandle mh = lookup.findConstructor
                (lookup.lookupClass(), MethodType.methodType(void.class));
            return (Transcoder<R>) mh.invoke();
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    private void addTranscodeMethod(ClassMaker cm) {
        MethodMaker mm = cm.addMethod
            (null, "transcode", byte[].class, byte[].class, byte[][].class, int.class);
        mm.public_();

        if (mInfo instanceof SecondaryInfo) {
            // Secondary values don't support multiple schemas, so no need to use indy.
            transcode(mm, mInfo, mTargetInfo, 0);
            return;
        }

        var keyVar = mm.param(0);
        var valueVar = mm.param(1);
        var kvPairsVar = mm.param(2);
        var offsetVar = mm.param(3);

        // A secondary index descriptor is suitable for reconstructing the target info.
        byte[] targetDesc = RowStore.indexDescriptor(mTargetInfo, false);

        var indy = mm.var(SortTranscoderMaker.class).indy
            ("indyTranscode", mRowStore.ref(), mRowType, mIndexId, targetDesc);

        var schemaVersion = mm.var(RowUtils.class).invoke("decodeSchemaVersion", valueVar);

        indy.invoke(null, "transcode", null, schemaVersion,
                    keyVar, valueVar, kvPairsVar, offsetVar);
    }

    /**
     * @param sourceOffset start offset of the source binary value; pass -1 to auto skip schema
     * version
     */
    private static <R> void transcode(MethodMaker mm,
                                      RowInfo info, RowInfo targetInfo, int sourceOffset)
    {
        var tm = new TransformMaker<R>(null, info, null);

        tm.addKeyTarget(targetInfo, 0, true);

        if (!targetInfo.valueColumns.isEmpty()) {
            tm.addValueTarget(targetInfo, 0, true);
        }

        var keyVar = mm.param(0);
        var valueVar = mm.param(1);
        var kvPairsVar = mm.param(2);
        var offsetVar = mm.param(3);

        tm.begin(mm, null, keyVar, valueVar, sourceOffset);

        kvPairsVar.aset(offsetVar, tm.encode(0));
        offsetVar.inc(1);

        if (!targetInfo.valueColumns.isEmpty()) {
            kvPairsVar.aset(offsetVar, tm.encode(1));
        } else {
            kvPairsVar.aset(offsetVar, mm.var(RowUtils.class).field("EMPTY_BYTES"));
        }
    }

    /**
     * Bootstrap method for the transcode method when a schema version must be decoded.
     *
     * MethodType is:
     *
     *     void (int schemaVersion,
     *           byte[] srcKey, byte[] srcValue, byte[][] kvPairsVar, int offset)
     */
    public static SwitchCallSite indyTranscode
        (MethodHandles.Lookup lookup, String name, MethodType mt,
         WeakReference<RowStore> storeRef, Class<?> rowType, long indexId, byte[] targetDesc)
    {
        return new SwitchCallSite(lookup, mt, schemaVersion -> {
            // Drop the schemaVersion parameter.
            var mtx = mt.dropParameterTypes(0, 1);

            RowStore store = storeRef.get();
            if (store == null) {
                var mm = MethodMaker.begin(lookup, "transcode", mtx);
                mm.new_(DatabaseException.class, "Closed").throw_();
                return mm.finish();
            }

            RowInfo sourceInfo;
            try {
                sourceInfo = store.rowInfo(rowType, indexId, schemaVersion);
            } catch (Exception e) {
                var mm = MethodMaker.begin(lookup, "transcode", mtx);
                return new ExceptionCallSite.Failed(mtx, mm, e);
            }

            MethodMaker mm = MethodMaker.begin(lookup, "transcode", mtx);

            RowInfo targetInfo = RowStore.indexRowInfo(sourceInfo, targetDesc);
            int sourceOffset = RowUtils.lengthPrefixPF(schemaVersion);

            transcode(mm, sourceInfo, targetInfo, sourceOffset);

            return mm.finish();
        });
    }

    private void addDecodeRowMethod(ClassMaker cm) {
        MethodMaker mm = cm.addMethod(null, "decodeRow", Object.class, byte[].class, byte[].class);
        mm.public_();

        var rowVar = mm.param(0).cast(mRowClass);

        RowGen targetRowGen = mTargetInfo.rowGen();

        final ColumnCodec[] keyCodecs = targetRowGen.keyCodecs();
        decodeColumns(mm, rowVar, mm.param(1), keyCodecs);

        final ColumnCodec[] valueCodecs = targetRowGen.valueCodecs();
        decodeColumns(mm, rowVar, mm.param(2), valueCodecs);

        // Mark projected columns as clean, all others are unset.

        final int maxNum = mTargetInfo.allColumns.size();
        int mask = 0;

        for (int num = 0; num < maxNum; ) {
            ColumnCodec codec;
            if (num < keyCodecs.length) {
                codec = keyCodecs[num];
            } else {
                codec = valueCodecs[num - keyCodecs.length];
            }

            if (mProjection.contains(codec.mInfo.name)) {
                mask |= RowGen.stateFieldMask(num, 0b01); // clean state
            }

            if ((++num & 0b1111) == 0 || num >= maxNum) {
                rowVar.field(targetRowGen.stateField(num - 1)).set(mask);
                mask = 0;
            }
        }
    }

    private void decodeColumns(MethodMaker mm, Variable rowVar, Variable srcVar,
                               ColumnCodec[] codecs)
    {
        if (codecs.length != 0) {
            codecs = ColumnCodec.bind(codecs, mm);
            var offsetVar = mm.var(int.class).set(0);
            for (int i=0; i<codecs.length; i++) {
                ColumnCodec codec = codecs[i];
                String name = codec.mInfo.name;
                if (mProjection.contains(name)) {
                    codec.decode(rowVar.field(name), srcVar, offsetVar, null);
                } else if (i < codecs.length - 1) {
                    codec.decodeSkip(srcVar, offsetVar, null);
                }
            }
        }
    }
}
