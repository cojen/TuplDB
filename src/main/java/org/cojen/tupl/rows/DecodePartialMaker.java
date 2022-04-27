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

import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import org.cojen.maker.Field;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.DatabaseException;

/**
 * Makes a call site which decodes rows partially.
 *
 * @author Brian S O'Neill
 * @see BaseTable#decodePartialHandle
 */
public class DecodePartialMaker {
    private DecodePartialMaker() {
    }

    /**
     * Makes a specification in which the columns to decode and the columns to mark clean are
     * the same.
     *
     * @param projection if null, then null is returned (implies all columns)
     */
    public static byte[] makeFullSpec(RowGen rowGen, Map<String, ColumnInfo> projection) {
        if (projection == null) {
            return null;
        }

        var toDecode = new BitSet();

        Map<String, Integer> columnNumbers = rowGen.columnNumbers();
        for (String name : projection.keySet()) {
            toDecode.set(columnNumbers.get(name));
        }

        byte[] bytes = toDecode.toByteArray();
        return makeSpec(bytes, bytes);
    }

    /**
     * Makes a specification in which only the value columns are decoded, but all projected
     * columns are marked clean. This implies that any key columns are already decoded.
     *
     * @param projection if null, then null is returned (implies all columns)
     */
    public static byte[] makeValueSpec(RowGen rowGen, Map<String, ColumnInfo> projection) {
        if (projection == null) {
            return null;
        }

        var toDecode = new BitSet();
        var toMarkClean = new BitSet();

        Map<String, Integer> columnNumbers = rowGen.columnNumbers();
        int numKeys = rowGen.info.keyColumns.size();
        for (String name : projection.keySet()) {
            int num = columnNumbers.get(name);
            if (num >= numKeys) {
                toDecode.set(num);
            }
            toMarkClean.set(num);
        }

        return makeSpec(toDecode, toMarkClean);
    }

    private static byte[] makeSpec(BitSet toDecode, BitSet toMarkClean) {
        return makeSpec(toDecode.toByteArray(), toMarkClean.toByteArray());
    }

    private static byte[] makeSpec(byte[] toDecode, byte[] toMarkClean) {
        byte[] spec = new byte[Math.max(toDecode.length, toMarkClean.length) << 1];
        System.arraycopy(toDecode, 0, spec, 0, toDecode.length);
        System.arraycopy(toMarkClean, 0, spec, spec.length >> 1, toMarkClean.length);
        return spec;
    }

    /**
     * Make a decoder for a specific schema version.
     *
     * MethodType is: void (RowClass row, byte[] key, byte[] value)
     *
     * @param spec obtained from makeFullSpec, et al
     */
    public static MethodHandle makeDecoder(MethodHandles.Lookup lookup,
                                           WeakReference<RowStore> storeRef,
                                           Class<?> rowType, Class<?> rowClass, Class<?> tableClass,
                                           long indexId, byte[] spec, int schemaVersion)
    {
        return ExceptionCallSite.make(() -> {
            MethodMaker mm = MethodMaker.begin
                (lookup, null, "decodeRow", rowClass, byte[].class, byte[].class);

            RowStore store = storeRef.get();
            if (store == null) {
                mm.new_(DatabaseException.class, "Closed").throw_();
                return mm.finish();
            }

            var rowVar = mm.param(0);
            var keyVar = mm.param(1);
            var valueVar = mm.param(2);

            RowInfo dstRowInfo = RowInfo.find(rowType);
            RowGen dstRowGen = dstRowInfo.rowGen();

            int specLen = spec.length;
            BitSet toDecode = BitSet.valueOf(Arrays.copyOfRange(spec, 0, specLen >> 1));
            BitSet toMarkClean = BitSet.valueOf(Arrays.copyOfRange(spec, specLen >> 1, specLen));

            // Decode the key columns.
            ColumnCodec[] keyCodecs = dstRowGen.keyCodecs();
            if (allRequested(toDecode, 0, keyCodecs.length)) {
                // All key columns are requested.
                mm.var(tableClass).invoke("decodePrimaryKey", rowVar, keyVar);
            } else {
                addDecodeColumns(mm, toDecode, rowVar, dstRowInfo, keyVar, keyCodecs, 0);
            }

            // Decode the value columns.
            if (allRequested(toDecode, keyCodecs.length, dstRowInfo.allColumns.size())) {
                // All value columns are requested. Use decodeValueHandle instead of
                // decodeValue because the schema version is already known.
                try {
                    MethodHandle mh = lookup.findStatic
                        (tableClass, "decodeValueHandle",
                         MethodType.methodType(MethodHandle.class, int.class));
                    mh = (MethodHandle) mh.invokeExact(schemaVersion);
                    mm.invoke(mh, rowVar, valueVar);
                } catch (Throwable e) {
                    throw RowUtils.rethrow(e);
                }
            } else if (schemaVersion == 0) {
                // No value columns to decode, so assign defaults.
                addDefaultColumns(mm, toDecode, rowVar, dstRowGen,
                                  dstRowInfo.valueColumns, null);
            } else {
                RowInfo srcRowInfo;
                try {
                    srcRowInfo = store.rowInfo(rowType, indexId, schemaVersion);
                } catch (Exception e) {
                    return new ExceptionCallSite.Failed
                        (MethodType.methodType(void.class, rowClass, byte[].class, byte[].class),
                         mm, e);
                }

                ColumnCodec[] srcCodecs = srcRowInfo.rowGen().valueCodecs();
                int fixedOffset = schemaVersion < 128 ? 1 : 4;

                addDecodeColumns(mm, toDecode,  rowVar, dstRowInfo,
                                 valueVar, srcCodecs, fixedOffset);

                if (dstRowInfo != srcRowInfo) {
                    // Assign defaults for any missing columns.
                    addDefaultColumns(mm, toDecode, rowVar, dstRowGen,
                                      dstRowInfo.valueColumns, srcRowInfo.valueColumns);
                }
            }

            // Mark requested columns as clean, all others are unset.
            {
                final int maxNum = dstRowInfo.allColumns.size();
                int mask = 0;

                for (int num = 0; num < maxNum; ) {
                    if (toMarkClean.get(num)) {
                        mask |= RowGen.stateFieldMask(num, 0b01); // clean state
                    }
                    if ((++num & 0b1111) == 0 || num >= maxNum) {
                        rowVar.field(dstRowGen.stateField(num - 1)).set(mask);
                        mask = 0;
                    }
                }
            }

            return mm.finish();
        }).dynamicInvoker();
    }

    /**
     * Make a decoder for a secondary index.
     *
     * MethodType is: void (RowClass row, byte[] key, byte[] value)
     *
     * @param spec obtained from makeFullSpec, et al
     */
    public static MethodHandle makeDecoder(MethodHandles.Lookup lookup,
                                           Class<?> rowType, Class<?> rowClass, Class<?> tableClass,
                                           byte[] secondaryDesc, byte[] spec)
    {
        MethodMaker mm = MethodMaker.begin
            (lookup, null, "decodeRow", rowClass, byte[].class, byte[].class);

        var rowVar = mm.param(0);
        var keyVar = mm.param(1);
        var valueVar = mm.param(2);

        RowInfo primaryRowInfo = RowInfo.find(rowType);
        RowInfo rowInfo = RowStore.indexRowInfo(primaryRowInfo, secondaryDesc);
        RowGen rowGen = rowInfo.rowGen();

        int specLen = spec.length;
        BitSet toDecode = BitSet.valueOf(Arrays.copyOfRange(spec, 0, specLen >> 1));
        BitSet toMarkClean = BitSet.valueOf(Arrays.copyOfRange(spec, specLen >> 1, specLen));

        // Decode the key columns.
        ColumnCodec[] keyCodecs = rowGen.keyCodecs();
        if (allRequested(toDecode, 0, keyCodecs.length)) {
            // All key columns are requested.
            mm.var(tableClass).invoke("decodePrimaryKey", rowVar, keyVar);
        } else {
            addDecodeColumns(mm, toDecode, rowVar, rowInfo, keyVar, keyCodecs, 0);
        }

        // Decode the value columns.
        if (allRequested(toDecode, keyCodecs.length, rowInfo.allColumns.size())) {
            // All value columns are requested.
            mm.var(tableClass).invoke("decodeValue", rowVar, valueVar);
        } else {
            ColumnCodec[] valueCodecs = rowGen.valueCodecs();
            addDecodeColumns(mm, toDecode, rowVar, rowInfo, valueVar, valueCodecs, 0);
        }

        // Mark requested columns as clean, all others are unset.
        {
            Map<Integer, String> numToName = flip(rowGen.columnNumbers());

            RowGen primaryRowGen = primaryRowInfo.rowGen();
            Map<String, Integer> primaryColumnNumbers = primaryRowGen.columnNumbers();

            final int maxNum = primaryColumnNumbers.size();
            int mask = 0;

            for (int num = 0; num < maxNum; ) {
                if (toMarkClean.get(num)) {
                    String name = numToName.get(num);
                    int primaryNum = primaryColumnNumbers.get(name);
                    mask |= RowGen.stateFieldMask(primaryNum, 0b01); // clean state
                }
                if ((++num & 0b1111) == 0 || num >= maxNum) {
                    rowVar.field(primaryRowGen.stateField(num - 1)).set(mask);
                    mask = 0;
                }
            }
        }

        return mm.finish();
    }

    /**
     * Returns true if all key columns or all value columns are to be decoded.
     */
    private static boolean allRequested(BitSet toDecode, int from, int to) {
        return toDecode.get(from, to).cardinality() == (to - from);
    }

    /**
     * @param rowVar a row instance
     * @param srcVar a byte array
     * @param fixedOffset must be after the schema version (when applicable)
     */
    private static void addDecodeColumns(MethodMaker mm, BitSet toDecode,
                                         Variable rowVar, RowInfo dstRowInfo,
                                         Variable srcVar, ColumnCodec[] srcCodecs, int fixedOffset)
    {
        Map<String, Integer> columnNumbers = dstRowInfo.rowGen().columnNumbers();

        srcCodecs = ColumnCodec.bind(srcCodecs, mm);
        Variable offsetVar = mm.var(int.class).set(fixedOffset);

        int remaining = 0;
        for (ColumnCodec srcCodec : srcCodecs) {
            if (toDecode.get(columnNumbers.get(srcCodec.mInfo.name))) {
                remaining++;
            }
        }

        if (remaining == 0) {
            return;
        }

        for (ColumnCodec srcCodec : srcCodecs) {
            String name = srcCodec.mInfo.name;

            if (toDecode.get(columnNumbers.get(name))) {
                ColumnInfo dstInfo = dstRowInfo.allColumns.get(name);
                if (dstInfo != null) {
                    Field dstVar = rowVar.field(name);
                    Converter.decode(mm, srcVar, offsetVar, null, srcCodec, dstInfo, dstVar);
                    if (--remaining <= 0) {
                        break;
                    }
                    continue;
                }
            }

            srcCodec.decodeSkip(srcVar, offsetVar, null);
        }
    }

    /**
     * @param srcColumns can be null if no source columns exist
     */
    private static void addDefaultColumns(MethodMaker mm, BitSet toDecode,
                                          Variable rowVar, RowGen dstRowGen,
                                          Map<String, ColumnInfo> dstColumns,
                                          Map<String, ColumnInfo> srcColumns)
    {
        for (Map.Entry<String, ColumnInfo> e : dstColumns.entrySet()) {
            String name = e.getKey();
            if (srcColumns == null || !srcColumns.containsKey(name)) {
                if (toDecode.get(dstRowGen.columnNumbers().get(name))) {
                    Converter.setDefault(mm, e.getValue(), rowVar.field(name));
                }
            }
        }
    }

    private static <K, V> Map<V, K> flip(Map<K, V> map) {
        var flipped = new HashMap<V, K>(map.size());
        for (Map.Entry<K, V> e : map.entrySet()) {
            flipped.put(e.getValue(), e.getKey());
        }
        return flipped;
    }
}
