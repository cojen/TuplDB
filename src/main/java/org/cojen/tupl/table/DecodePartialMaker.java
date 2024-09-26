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
import java.lang.invoke.MethodType;

import java.lang.ref.WeakReference;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Map;
import java.util.Set;

import org.cojen.maker.Field;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.DatabaseException;

import org.cojen.tupl.table.codec.ColumnCodec;

/**
 * Makes a call site which decodes rows partially.
 *
 * @author Brian S O'Neill
 * @see StoredTable#decodePartialHandle
 */
public class DecodePartialMaker {
    private DecodePartialMaker() {
    }

    /**
     * Makes a specification in which the columns to decode and the columns to mark clean are
     * the same.
     *
     * @param primaryRowGen non-null if rowGen refers to a secondary
     * @param projection if null, then null is returned (implies all columns)
     */
    public static byte[] makeFullSpec(RowGen rowGen, RowGen primaryRowGen,
                                      Map<String, ColumnInfo> projection)
    {
        return projection == null ? null : makeFullSpec(rowGen, primaryRowGen, projection.keySet());
    }

    /**
     * Makes a specification in which the columns to decode and the columns to mark clean are
     * the same.
     *
     * @param primaryRowGen non-null if rowGen refers to a secondary
     * @param projection column names to project; if null, then null is returned (implies all
     * columns)
     */
    public static byte[] makeFullSpec(RowGen rowGen, RowGen primaryRowGen, Set<String> projection) {
        if (projection == null) {
            return null;
        }

        BitSet toDecodeBits = new BitSet();
        {
            Map<String, Integer> columnNumbers = rowGen.columnNumbers();
            for (String name : projection) {
                toDecodeBits.set(columnNumbers.get(name));
            }
        }

        BitSet toMarkCleanBits;
        if (primaryRowGen == null) {
            toMarkCleanBits = null;
        } else {
            toMarkCleanBits = new BitSet();
            Map<String, Integer> columnNumbers = primaryRowGen.columnNumbers();
            for (String name : projection) {
                toMarkCleanBits.set(columnNumbers.get(name));
            }
        }

        return makeSpec(toDecodeBits, toMarkCleanBits);
    }

    /**
     * @param toDecodeBits bits correspond to column numbers of the decoded row, which can be a
     * primary row or a secondary row
     * @param toMarkCleanBits bits correspond to column numbers of the primary row; if null,
     * then is same as toDecodeBits, which implies that the decoded row is a primary row
     */
    private static byte[] makeSpec(BitSet toDecodeBits, BitSet toMarkCleanBits) {
        byte[] toDecode = toDecodeBits.toByteArray();
        byte[] toMarkClean = toMarkCleanBits == null ? null : toMarkCleanBits.toByteArray();

        int capacity = 1 + toDecode.length;

        Encoder encoder;
        if (toMarkClean == null) {
            encoder = new Encoder(capacity);
            encoder.writeBytes(toDecode);
        } else {
            encoder = new Encoder(capacity + 1 + toMarkClean.length);
            encoder.writeBytes(toDecode);
            encoder.writeBytes(toMarkClean);
        }

        return encoder.toByteArray();
    }

    /**
     * @return toDecode and toMarkClean
     */
    static BitSet[] decodeSpec(byte[] spec) {
        int len = RowUtils.decodePrefixPF(spec, 0);
        int offset = RowUtils.lengthPrefixPF(len);
        BitSet toDecode = BitSet.valueOf(Arrays.copyOfRange(spec, offset, offset += len));

        BitSet toMarkClean;
        if (offset >= spec.length) {
            toMarkClean = toDecode;
        } else {
            len = RowUtils.decodePrefixPF(spec, offset);
            offset += RowUtils.lengthPrefixPF(len);
            toMarkClean = BitSet.valueOf(Arrays.copyOfRange(spec, offset, offset + len));
        }
        
        return new BitSet[] {toDecode, toMarkClean};
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

            BitSet[] sets = decodeSpec(spec);
            BitSet toDecode = sets[0];
            BitSet toMarkClean = sets[1];

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
                addDefaultColumns(mm, toDecode, rowVar, dstRowGen, dstRowInfo.valueColumns, null);
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
     * Make a decoder for a secondary index or an unevolvable type.
     *
     * MethodType is: void (RowClass row, byte[] key, byte[] value)
     *
     * @param secondaryDesc pass null if not a secondary index
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

        RowInfo rowInfo;
        if (secondaryDesc == null) {
            rowInfo = primaryRowInfo;
        } else {
            rowInfo = RowStore.secondaryRowInfo(primaryRowInfo, secondaryDesc);
        }

        RowGen rowGen = rowInfo.rowGen();

        BitSet[] sets = decodeSpec(spec);
        BitSet toDecode = sets[0];
        BitSet toMarkClean = sets[1];

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
            RowGen primaryRowGen = primaryRowInfo.rowGen();
            Map<String, Integer> primaryColumnNumbers = primaryRowGen.columnNumbers();

            final int maxNum = primaryColumnNumbers.size();
            int mask = 0;

            for (int num = 0; num < maxNum; ) {
                if (toMarkClean.get(num)) {
                    mask |= RowGen.stateFieldMask(num, 0b01); // clean state
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
    static boolean allRequested(BitSet toDecode, int from, int to) {
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
            if (toDecode.get(columnNumbers.get(srcCodec.info.name))) {
                remaining++;
            }
        }

        if (remaining == 0) {
            return;
        }

        for (ColumnCodec srcCodec : srcCodecs) {
            String name = srcCodec.info.name;

            if (toDecode.get(columnNumbers.get(name))) {
                ColumnInfo dstInfo = dstRowInfo.allColumns.get(name);
                if (dstInfo != null) {
                    Field dstVar = rowVar.field(name);
                    Converter.decodeLossy(mm, srcVar, offsetVar, null, srcCodec, dstInfo, dstVar);
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
}
