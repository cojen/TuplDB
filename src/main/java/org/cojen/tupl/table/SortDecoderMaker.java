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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.table.codec.ColumnCodec;

/**
 * 
 *
 * @author Brian S O'Neill
 * @see RowSorter
 * @see SortTranscoderMaker
 */
class SortDecoderMaker {
    /**
     * Find or instantiate an info object which describes the encoding of the keys and values.
     *
     * @param orderBySpec ordering specification
     * @param projection columns to decode; can pass null to decode all available columns
     * @param allowDuplicates pass false for "select distinct" behavior
     */
    static SecondaryInfo findSortedInfo(Class<?> rowType, String orderBySpec,
                                        Set<String> projection, boolean allowDuplicates)
    {
        var key = new InfoKey(rowType, orderBySpec, projection, allowDuplicates);
        return cSortedInfos.obtain(key, null);
    }

    /**
     * Find or generate a decoder instance.
     *
     * @param sortedInfo describes the encoding of the keys and values
     * @param projection columns to decode; can pass null to decode all available columns
     */
    @SuppressWarnings("unchecked")
    static <R> RowDecoder<R> findDecoder(Class<?> rowType,
                                         SecondaryInfo sortedInfo, Set<String> projection)
    {
        var key = new DecoderKey(rowType, sortedInfo.indexSpec(), projection);
        return cDecoders.obtain(key, sortedInfo);
    }

    private record DecoderKey(Class<?> rowType, String sortedInfoSpec, Set<String> projection) {
        DecoderKey withNullProjection() {
            return new DecoderKey(rowType, sortedInfoSpec, null);
        }
    }

    private static final WeakCache<DecoderKey, RowDecoder, SecondaryInfo> cDecoders;

    static {
        cDecoders = new WeakCache<>() {
            @Override
            protected RowDecoder newValue(DecoderKey key, SecondaryInfo sortedInfo) {
                Set<String> projection = key.projection();
                if (projection != null && projection.equals(sortedInfo.allColumns.keySet())) {
                    // Null is the canonical form when projecting all columns.
                    return obtain(key.withNullProjection(), sortedInfo);
                } else {
                    return makeDecoder(key.rowType, sortedInfo, key.projection());
                }
            }
        };
    }

    private record InfoKey(Class<?> rowType, String orderBySpec,
                           Set<String> projection, boolean allowDuplicates)
    {
        InfoKey withNullProjection() {
            return new InfoKey(rowType, orderBySpec, null, allowDuplicates);
        }
    }

    private static final WeakCache<InfoKey, SecondaryInfo, RowInfo> cSortedInfos;

    static {
        cSortedInfos = new WeakCache<>() {
            @Override
            protected SecondaryInfo newValue(InfoKey key, RowInfo rowInfo) {
                Set<String> projection = key.projection();

                if (projection != null) {
                    if (rowInfo == null) {
                        rowInfo = RowInfo.find(key.rowType);
                    }
                    if (projection.equals(rowInfo.allColumns.keySet())) {
                        // Null is the canonical form when projecting all columns.
                        return obtain(key.withNullProjection(), rowInfo);
                    }
                }

                return newSortedInfo(key.rowType, key.orderBySpec, projection, key.allowDuplicates);
            }
        };
    }

    /**
     * @param orderBySpec ordering specification
     * @param projection columns to decode; can pass null to decode all available columns
     * @param allowDuplicates pass false for "select distinct" behavior
     * @throws IllegalArgumentException if any orderBy or projected columns aren't available
     */
    static SecondaryInfo newSortedInfo(Class<?> rowType,
                                       String orderBySpec, Set<String> projection,
                                       boolean allowDuplicates)
    {
        RowInfo rowInfo = RowInfo.find(rowType);

        if (projection == null) {
            projection = rowInfo.allColumns.keySet();
        }

        Set<String> available = allowDuplicates ? rowInfo.allColumns.keySet() : projection;

        var sortedInfo = new SecondaryInfo(rowInfo, false);
        sortedInfo.keyColumns = new LinkedHashMap<>();

        OrderBy orderBy = OrderBy.forSpec(rowInfo, orderBySpec);

        for (Map.Entry<String, OrderBy.Rule> e : orderBy.entrySet()) {
            ColumnInfo orderColumn = e.getValue().asColumn();
            if (!available.contains(orderColumn.name)) {
                throw new IllegalArgumentException();
            }
            sortedInfo.keyColumns.put(orderColumn.name, orderColumn);
        }

        boolean hasDuplicates = false;

        for (ColumnInfo keyColumn : rowInfo.keyColumns.values()) {
            if (!available.contains(keyColumn.name)) {
                hasDuplicates = true;
            } else if (!sortedInfo.keyColumns.containsKey(keyColumn.name)) {
                sortedInfo.keyColumns.put(keyColumn.name, keyColumn);
            }
        }

        if (!hasDuplicates) {
            // All of the primary key columns are part of the target key, and so no duplicates
            // can exist. Define the remaining projected columns in the target value.
            for (String colName : projection) {
                if (!available.contains(colName)) {
                    throw new IllegalArgumentException();
                }
                if (!sortedInfo.keyColumns.containsKey(colName)) {
                    if (sortedInfo.valueColumns == null) {
                        sortedInfo.valueColumns = new TreeMap<>();
                    }
                    sortedInfo.valueColumns.put(colName, rowInfo.allColumns.get(colName));
                }
            }
        } else {
            // Because duplicate keys can exist, define all available columns in the target
            // key. This doesn't fully prevent duplicates, and so any extra rows will be
            // eliminated by the sorter. Proper "select distinct" behavior happens when the
            // available set is the same as the projected set.
            for (String colName : available) {
                if (!sortedInfo.keyColumns.containsKey(colName)) {
                    sortedInfo.keyColumns.put(colName, rowInfo.allColumns.get(colName));
                }
            }
        }

        if (sortedInfo.valueColumns == null) {
            sortedInfo.valueColumns = Collections.emptyNavigableMap();
        }

        sortedInfo.allColumns = new TreeMap<>();
        sortedInfo.allColumns.putAll(sortedInfo.keyColumns);
        sortedInfo.allColumns.putAll(sortedInfo.valueColumns);

        return sortedInfo;
    }

    /**
     * @param sortedInfo describes the encoding of the keys and values
     * @param projection columns to decode; can pass null to decode all available columns
     */
    private static RowDecoder makeDecoder(Class<?> rowType,
                                          SecondaryInfo sortedInfo, Set<String> projection)
    {
        if (projection == null) {
            projection = sortedInfo.allColumns.keySet();
        }

        RowInfo rowInfo = RowInfo.find(rowType);
        RowGen rowGen = rowInfo.rowGen();
        Class rowClass = RowMaker.find(rowType);

        ClassMaker cm = rowGen.anotherClassMaker
            (SortDecoderMaker.class, rowClass, null).implement(RowDecoder.class).final_();

        // Keep a singleton instance, in order for a weakly cached reference to the RowDecoder
        // to stick around until the class is unloaded.
        cm.addField(RowDecoder.class, "_").private_().static_();

        {
            MethodMaker mm = cm.addConstructor().private_();
            mm.invokeSuperConstructor();
            mm.field("_").set(mm.this_());
        }

        MethodMaker mm = cm.addMethod
            (Object.class, "decodeRow", Object.class, byte[].class, byte[].class).public_();

        var rowVar = mm.param(0);
        var keyVar = mm.param(1);
        var valueVar = mm.param(2);

        Label notRow = mm.label();
        var typedRowVar = CodeUtils.castOrNew(rowVar, rowClass, notRow);
        Label hasTypedRow = mm.label();
        typedRowVar.ifNe(null, hasTypedRow);
        typedRowVar.set(mm.new_(rowClass));
        hasTypedRow.here();

        RowGen sortedRowGen = sortedInfo.rowGen();
        decodeColumns(projection, mm, typedRowVar, keyVar, sortedRowGen.keyCodecs());
        decodeColumns(projection, mm, typedRowVar, valueVar, sortedRowGen.valueCodecs());

        // Mark projected columns as clean; all others are unset.

        ColumnCodec[] keyCodecs = rowGen.keyCodecs();
        ColumnCodec[] valueCodecs = rowGen.valueCodecs();

        int maxNum = rowInfo.allColumns.size();
        int mask = 0;

        for (int num = 0; num < maxNum; ) {
            ColumnCodec codec;
            if (num < keyCodecs.length) {
                codec = keyCodecs[num];
            } else {
                codec = valueCodecs[num - keyCodecs.length];
            }

            if (projection.contains(codec.info.name)) {
                mask |= RowGen.stateFieldMask(num, 0b01); // clean state
            }

            if ((++num & 0b1111) == 0 || num >= maxNum) {
                typedRowVar.field(sortedRowGen.stateField(num - 1)).set(mask);
                mask = 0;
            }
        }

        mm.return_(typedRowVar);

        // Assume the passed in row is actually a RowConsumer.
        notRow.here();
        CodeUtils.acceptAsRowConsumerAndReturn(rowVar, rowClass, keyVar, valueVar);

        try {
            MethodHandles.Lookup lookup = cm.finishHidden();
            MethodHandle mh = lookup.findConstructor
                (lookup.lookupClass(), MethodType.methodType(void.class));
            return (RowDecoder) mh.invoke();
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    private static void decodeColumns(Set<String> projection,
                                      MethodMaker mm, Variable rowVar, Variable srcVar,
                                      ColumnCodec[] codecs)
    {
        if (codecs.length != 0) {
            codecs = ColumnCodec.bind(codecs, mm);
            var offsetVar = mm.var(int.class).set(0);
            for (int i=0; i<codecs.length; i++) {
                ColumnCodec codec = codecs[i];
                String name = codec.info.name;
                if (projection.contains(name)) {
                    codec.decode(rowVar.field(name), srcVar, offsetVar, null);
                } else if (i < codecs.length - 1) {
                    codec.decodeSkip(srcVar, offsetVar, null);
                }
            }
        }
    }
}
