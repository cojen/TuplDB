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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.util.HashMap;
import java.util.Map;

import org.cojen.tupl.Index;
import org.cojen.tupl.Transaction;

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
 */
class IndexTriggerMaker<R> {
    private final Class<R> mRowType;
    private final Class<? extends R> mRowClass;
    private final RowGen mPrimaryGen;

    // To be filled in by caller.
    final RowInfo[] mSecondaryInfos;
    final Index[] mSecondaryIndexes;
    final byte[] mSecondaryStates; // FIXME: remember to build indexes

    private Map<ColumnCodec, Pair<Variable, Variable>> mFoundKeyCodecs, mFoundValueCodecs;

    private ClassMaker mClassMaker;

    IndexTriggerMaker(Class<R> rowType, RowInfo primaryInfo, int numIndexes) {
        mRowType = rowType;
        mRowClass = RowMaker.find(rowType);
        mPrimaryGen = primaryInfo.rowGen();

        mSecondaryInfos = new RowInfo[numIndexes];
        mSecondaryIndexes = new Index[numIndexes];
        mSecondaryStates = new byte[numIndexes];
    }

    Trigger<R> make() {
        for (RowInfo secondaryInfo : mSecondaryInfos) {
            mFoundKeyCodecs = findCodecs(mPrimaryGen.keyCodecMap(), secondaryInfo, false);
            mFoundValueCodecs = findCodecs(mPrimaryGen.valueCodecMap(), secondaryInfo, false);
        }

        mClassMaker = mPrimaryGen.beginClassMaker(IndexTriggerMaker.class, mRowType, "Trigger");
        mClassMaker.extend(Trigger.class);
        mClassMaker.addConstructor();

        addInsertMethod();

        // FIXME: delete, store, update

        var lookup = mClassMaker.finishHidden();

        try {
            var ctor = lookup.findConstructor
                (lookup.lookupClass(), MethodType.methodType(void.class));
            return (Trigger<R>) ctor.invoke();
        } catch (Throwable e) {
            throw rethrow(e);
        }
    }

    /**
     * For the given secondary index RowInfo, find all matching primary ColumnCodecs. A
     * matching ColumnCodec has the same name and encoding strategy. The map keys are exact
     * ColumnCodec instances from primaryCodecMap, and the map values are null.
     *
     * @param primitive pass false to ignore primitive columns
     * @return a new map or null if it would be empty
     */
    private static Map<ColumnCodec, Pair<Variable, Variable>> findCodecs
        (Map<ColumnCodec, ColumnCodec> primaryCodecMap, RowInfo secondaryInfo, boolean primitive)
    {
        RowGen secondaryGen = secondaryInfo.rowGen();
        Map<ColumnCodec, Pair<Variable, Variable>> foundMap = null;
        foundMap = findCodecs(primaryCodecMap, secondaryGen.keyCodecs(), primitive, foundMap);
        foundMap = findCodecs(primaryCodecMap, secondaryGen.valueCodecs(), primitive, foundMap);
        return foundMap;
    }

    /**
     * @param foundMap the current map, which can be initially null
     * @return a new or current map (can still be null)
     */
    private static Map<ColumnCodec, Pair<Variable, Variable>> findCodecs
        (Map<ColumnCodec, ColumnCodec> primaryCodecMap, ColumnCodec[] secondaryCodecs,
         boolean primitive, Map<ColumnCodec, Pair<Variable, Variable>> foundMap)
    {
        for (ColumnCodec codec : secondaryCodecs) {
            if (!primitive && codec instanceof PrimitiveColumnCodec) {
                continue;
            }
            ColumnCodec found = primaryCodecMap.get(codec);
            if (found != null) {
                if (foundMap == null) {
                    foundMap = new HashMap<>();
                }
                foundMap.put(found, null);
            }
        }

        return foundMap;
    }


    /**
     * Generates code which finds column offsets in an encoded byte array.
     *
     * @param srcVar src byte array
     * @param hasVersion true if byte array has a schema version field
     * @param codecs must be bound to the MethodMaker, without a schema version column
     * @param foundMap map whose values must be filled in with start/end offsets pairs
     */
    private static void findColumns(MethodMaker mm, Variable srcVar, boolean hasVersion,
                                    ColumnCodec[] codecs,
                                    Map<ColumnCodec, Pair<Variable, Variable>> foundMap)
    {
        int remaining;
        if (foundMap == null || (remaining = foundMap.size()) == 0) {
            return;
        }

        var offsetVar = mm.var(int.class).set(0);

        if (hasVersion) {
            Variable versionVar = srcVar.aget(offsetVar);
            offsetVar.inc(1);
            Label cont = mm.label();
            versionVar.ifGe(0, cont);
            offsetVar.inc(3);
            cont.here();
        }

        Variable endVar = null;

        for (ColumnCodec codec : codecs) {
            if (!foundMap.containsKey(codec)) {
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

            codec.decodeSkip(srcVar, offsetVar, null);

            // Need a stable copy.
            endVar = mm.var(int.class).set(offsetVar);

            foundMap.put(codec, new Pair<>(startVar, endVar));

            if (--remaining <= 0) {
                break;
            }
        }
    }

    private void addInsertMethod() {
        MethodMaker mm = mClassMaker.addMethod
            (null, "insert", Transaction.class, Object.class, byte[].class, byte[].class).public_();

        var txnVar = mm.param(0);
        var rowVar = mm.param(1).cast(mRowClass);
        var keyVar = mm.param(2);
        var newValueVar = mm.param(3);

        ColumnCodec[] keyCodecs = ColumnCodec.bind(mPrimaryGen.keyCodecs(), mm);
        ColumnCodec[] valueCodecs = ColumnCodec.bind(mPrimaryGen.valueCodecs(), mm);

        findColumns(mm, keyVar, false, keyCodecs, mFoundKeyCodecs);
        findColumns(mm, newValueVar, true, valueCodecs, mFoundValueCodecs);

        for (int i=0; i<mSecondaryInfos.length; i++) {
            RowInfo secondaryInfo = mSecondaryInfos[i];
            RowGen secondaryGen = secondaryInfo.rowGen();

            ColumnCodec[] secondaryKeyCodecs = ColumnCodec.bind(secondaryGen.keyCodecs(), mm);
            ColumnCodec[] secondaryValueCodecs = ColumnCodec.bind(secondaryGen.valueCodecs(), mm);

            var secondaryKeyVar = addEncodeColumns
                (mm, rowVar, keyVar, newValueVar, secondaryKeyCodecs);

            var secondaryValueVar = addEncodeColumns
                (mm, rowVar, keyVar, newValueVar, secondaryValueCodecs);

            var ix = mm.var(Index.class).setExact(mSecondaryIndexes[i]);
            ix.invoke("store", txnVar, secondaryKeyVar, secondaryValueVar);
        }
    }

    /**
     * Generate a key or value for a secondary index.
     *
     * @param rowVar primary row object, fully specified
     * @param keyVar primary key byte[], fully specified
     * @param valueVar primary value byte[], fully specified
     * @param codecs secondary key or value codecs, bound to MethodMaker
     * @return a filled-in byte[] variable
     */
    private Variable addEncodeColumns(MethodMaker mm,
                                      Variable rowVar, Variable keyVar, Variable valueVar,
                                      ColumnCodec[] codecs)
    {
        if (codecs.length == 0) {
            return mm.var(RowUtils.class).field("EMPTY_BYTES");
        }

        // Determine the minimum byte array size and prepare the encoders.

        // 0: must encode from row, 1: copy from primary key, 2: copy from primary value
        var wheres = new int[codecs.length];

        int minSize = 0;
        for (int i=0; i<codecs.length; i++) {
            ColumnCodec codec = codecs[i];

            int where;
            if (mFoundKeyCodecs != null && mFoundKeyCodecs.containsKey(codec)) {
                where = 1;
            } else if (mFoundValueCodecs != null && mFoundValueCodecs.containsKey(codec)) {
                where = 2;
            } else {
                where = 0;
                minSize += codec.minSize();
                codec.encodePrepare();
            }

            wheres[i] = where;
        }

        // Generate code which determines the additional runtime length.

        Variable totalVar = null;
        for (int i=0; i<codecs.length; i++) {
            ColumnCodec codec = codecs[i];
            int where = wheres[i];
            if (where == 0) {
                Field srcVar = rowVar.field(codec.mInfo.name);
                totalVar = codec.encodeSize(srcVar, totalVar);
            } else {
                Variable bytesVar;
                Map<ColumnCodec, Pair<Variable, Variable>> foundCodecs;
                if (where == 1) {
                    bytesVar = keyVar;
                    foundCodecs = mFoundKeyCodecs;
                } else {
                    bytesVar = valueVar;
                    foundCodecs = mFoundValueCodecs;
                }
                // FIXME: Detect spans and reduce additions.
                Pair<Variable, Variable> foundOffsets = foundCodecs.get(codec);
                var lengthVar = foundOffsets.b.sub(foundOffsets.a);
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
            int where = wheres[i];
            if (where == 0) {
                Field srcVar = rowVar.field(codec.mInfo.name);
                codec.encode(srcVar, dstVar, offsetVar);
            } else {
                // FIXME: Duplicate code. See above.
                Variable bytesVar;
                Map<ColumnCodec, Pair<Variable, Variable>> foundCodecs;
                if (where == 1) {
                    bytesVar = keyVar;
                    foundCodecs = mFoundKeyCodecs;
                } else {
                    bytesVar = valueVar;
                    foundCodecs = mFoundValueCodecs;
                }
                // FIXME: Detect spans and reduce copies.
                Pair<Variable, Variable> foundOffsets = foundCodecs.get(codec);
                var lengthVar = foundOffsets.b.sub(foundOffsets.a);
                mm.var(System.class).invoke
                    ("arraycopy", bytesVar, foundOffsets.a, dstVar, offsetVar, lengthVar);
                if (i < codecs.length - 1) {
                    offsetVar.inc(lengthVar);
                }
            }
        }

        return dstVar;
    }
}
