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

    // Map for all the columns needed by all of the secondary indexes.
    private Map<String, ColumnSource> mColumnSources;

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
        mClassMaker = mPrimaryGen.beginClassMaker(IndexTriggerMaker.class, mRowType, "Trigger");
        mClassMaker.extend(Trigger.class);
        mClassMaker.addConstructor();

        mColumnSources = buildColumnSources();

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
                source = new ColumnSource(primaryCodec, fromKey);
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
     * Generates code which finds column offsets in the encoded primary row. As a side-effect,
     * the Variable fields of all ColumnSources are updated.
     *
     * @param keyVar primary key byte array
     * @param valueVar primary value byte array
     * @param fullRow true of all columns of the primary row are valid; false if only the
     * primary key columns are valid
     */
    private void findColumns(MethodMaker mm, Variable keyVar, Variable valueVar, boolean fullRow) {
        // Determine how many columns must be accessed from the encoded form.

        int remainingKeys = 0, remainingValues = 0;
        for (ColumnSource source : mColumnSources.values()) {
            source.clearVars();
            if (source.mustFind(fullRow)) {
                if (source.mFromKey) {
                    remainingKeys++;
                } else {
                    remainingValues++;
                }
            }
        }

        // Note that true is passed for fullRow because only the key columns are found.
        findColumns(mm, keyVar, true, true, remainingKeys);

        // Here the value columns are found, and so the fullRow param must be passed as-is.
        findColumns(mm, valueVar, false, fullRow, remainingValues);
    }

    /**
     * Generates code which finds column offsets in the encoded primary row. As a side-effect,
     * the Variable fields of all ColumnSources are updated.
     *
     * @param srcVar byte array
     * @param forKey true if the byte array is an encoded key
     * @param fullRow true of all columns of the primary row are valid; false if only the
     * primary key columns are valid
     * @param remaining number of columns to decode/skip from the byte array
     */
    private void findColumns(MethodMaker mm, Variable srcVar, boolean forKey, boolean fullRow,
                             int remaining)
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
            // Skip the schema version pseudo field.
            Variable versionVar = srcVar.aget(0);
            offsetVar.set(1);
            Label cont = mm.label();
            versionVar.ifGe(0, cont);
            offsetVar.inc(3);
            cont.here();
        }

        codecs = ColumnCodec.bind(codecs, mm);
        Variable endVar = null;

        for (ColumnCodec codec : codecs) {
            ColumnSource source = mColumnSources.get(codec.mInfo.name);

            if (source.mFromKey != forKey) {
                throw new AssertionError();
            }

            if (!source.mustFind(fullRow)) {
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

            if (fullRow || source.mMismatches == 0) {
                codec.decodeSkip(srcVar, offsetVar, null);
            } else {
                var dstVar = mm.var(codec.mInfo.type);
                codec.decode(dstVar, srcVar, offsetVar, null);
                source.mDecodedVar = dstVar;
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

    private void addInsertMethod() {
        MethodMaker mm = mClassMaker.addMethod
            (null, "insert", Transaction.class, Object.class, byte[].class, byte[].class).public_();

        var txnVar = mm.param(0);
        var rowVar = mm.param(1).cast(mRowClass);
        var keyVar = mm.param(2);
        var newValueVar = mm.param(3);

        findColumns(mm, keyVar, newValueVar, true);

        // FIXME: As an optimization, when encoding complex columns (non-primitive), check if
        // prior secondary indexes have a matching codec and copy from them.

        for (int i=0; i<mSecondaryInfos.length; i++) {
            RowInfo secondaryInfo = mSecondaryInfos[i];
            RowGen secondaryGen = secondaryInfo.rowGen();

            ColumnCodec[] secondaryKeyCodecs = ColumnCodec.bind(secondaryGen.keyCodecs(), mm);
            ColumnCodec[] secondaryValueCodecs = ColumnCodec.bind(secondaryGen.valueCodecs(), mm);

            var secondaryKeyVar = encodeColumns
                (mm, rowVar, keyVar, newValueVar, secondaryKeyCodecs);

            var secondaryValueVar = encodeColumns
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
    private Variable encodeColumns(MethodMaker mm,
                                   Variable rowVar, Variable keyVar, Variable valueVar,
                                   ColumnCodec[] codecs)
    {
        if (codecs.length == 0) {
            return mm.var(RowUtils.class).field("EMPTY_BYTES");
        }

        // Determine the minimum byte array size and prepare the encoders.

        int minSize = 0;
        for (int i=0; i<codecs.length; i++) {
            ColumnCodec codec = codecs[i];
            ColumnSource source = mColumnSources.get(codec.mInfo.name);
            if (!source.shouldCopy(codec)) {
                minSize += codec.minSize();
                codec.encodePrepare();
            }
        }

        // Generate code which determines the additional runtime length.

        Variable totalVar = null;
        for (int i=0; i<codecs.length; i++) {
            ColumnCodec codec = codecs[i];
            ColumnSource source = mColumnSources.get(codec.mInfo.name);
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
            ColumnSource source = mColumnSources.get(codec.mInfo.name);
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
        // Describes the encoding of the column within the primary key or primary value.
        final ColumnCodec mCodec;

        // When true, the column is in the primary key, else in the primary value.
        final boolean mFromKey;

        // Number of secondary indexes that can use the codec directly; encoding is the same.
        int mMatches;

        // Number of secondary indexes that need to be transformed.
        int mMismatches;

        // The remaining fields are used during code generation passes.

        // Source byte array and offsets. These are set when the column can be copied directly
        // without any transformation.
        Variable mSrcVar, mStartVar, mEndVar;

        // Fully decoded column. Is set when the column cannot be directly copied from the
        // source, and it needed a transformation step.
        Variable mDecodedVar;

        ColumnSource(ColumnCodec codec, boolean fromKey) {
            mCodec = codec;
            mFromKey = fromKey;
        }

        boolean isPrimitive() {
            return mCodec instanceof PrimitiveColumnCodec;
        }

        /**
         * Returns true if the column must be found in the encoded byte array.
         *
         * @param fullRow true of all columns of the primary row are valid; false if only the
         * primary key columns are valid
         */
        boolean mustFind(boolean fullRow) {
            if (fullRow || mFromKey) {
                if (isPrimitive()) {
                    // Primitive columns are cheap to encode, so no need to copy byte form.
                    return false;
                } else {
                    // Return true if at least one secondary index can use copy the byte form,
                    // avoiding the cost of encoding the column from the row object.
                    return mMatches != 0;
                }
            } else {
                // Without a valid column in the row, must find it in the byte form.
                return true;
            }
        }

        /**
         * @param dstCodec codec of secondary index
         */
        boolean shouldCopy(ColumnCodec dstCodec) {
            return mSrcVar != null && mCodec.equals(dstCodec);
        }

        /**
         * @param rowVar primary row
         * @return a decoded variable or a field from the row
         */
        Variable accessColumn(Variable rowVar) {
            Variable colVar = mDecodedVar;
            if (colVar == null) {
                colVar = rowVar.field(mCodec.mInfo.name);
            }
            return colVar;
        }

        void clearVars() {
            mSrcVar = null;
            mStartVar = null;
            mEndVar = null;
            mDecodedVar = null;
        }
    }
}
