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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

/**
 * Utility for making code which transforms binary encoded rows to one or more other binary
 * encodings. This utility is designed for column placement and transcoding, but it doesn't
 * support type conversions.
 *
 * @author Brian S O'Neill
 */
class TransformMaker<R> {
    private final Class<R> mRowType;
    private final RowInfo mRowInfo;
    private final Map<String, Availability> mAvailable;

    private final List<Target> mKeyTargets, mValueTargets;

    private MethodMaker mMaker;
    private Variable mRowVar, mKeyVar, mValueVar;
    private int mValueOffset;

    private Map<String, ColumnSource> mSources;

    /**
     * @param rowType source row type; can pass null if no row instance is available
     * @param rowInfo source row info; can pass null to obtain from rowType
     * @param available defines which columns from a row instance are available; can pass null
     * if all columns are never available
     */
    TransformMaker(Class<R> rowType, RowInfo rowInfo, Map<String, Availability> available) {
        if (rowInfo == null) {
            if (rowType != null) {
                rowInfo = RowInfo.find(rowType);
            } else {
                Objects.requireNonNull(rowInfo);
            }
        }

        if (available != null) hasAny: {
            for (Availability avail : available.values()) {
                if (avail != Availability.NEVER) {
                    break hasAny;
                }
            }
            available = null;
        }

        if (available == null) {
            // Row instance won't be used, and this makes sure of that.
            rowType = null;
        } else if (rowType == null) {
            // A row instance is needed, and so a row type is needed too.
            throw new IllegalArgumentException();
        }

        mRowType = rowType;
        mRowInfo = rowInfo;
        mAvailable = available;

        mKeyTargets = new ArrayList<>();
        mValueTargets = new ArrayList<>();
    }

    /**
     * Add a transformation target, which will have key encoding applied to it. A target
     * identifier is returned, which always starts at 0 and increments by 1 for subsequent key
     * targets.
     *
     * @param rowInfo target row info
     * @param offset start offset of target value (padding)
     * @param eager true to always encode the key
     */
    int addKeyTarget(RowInfo rowInfo, int offset, boolean eager) {
        if (mMaker != null) {
            throw new IllegalStateException();
        }
        mKeyTargets.add(new Target(true, rowInfo, offset, eager));
        return mKeyTargets.size() - 1;
    }

    /**
     * Add a transformation target, which will have value encoding applied to it. A target
     * identifier is returned, which always starts at 0 and increments by 1 for subsequent
     * value targets.
     *
     * @param rowInfo target row info
     * @param offset start offset of target value (padding)
     * @param eager true to always encode the value
     */
    int addValueTarget(RowInfo rowInfo, int offset, boolean eager) {
        if (mMaker != null) {
            throw new IllegalStateException();
        }
        mValueTargets.add(new Target(false, rowInfo, offset, eager));
        return mValueTargets.size() - 1;
    }

    /**
     * Called after adding targets to begin making transformation code. After calling this
     * method, no more targets can be added.
     *
     * @param rowVar refers to the row instance; can be null if all columns are never available
     * @param keyVar refers to the source binary key instance
     * @param valueVar refers to the source binary value instance
     * @param valueOffset start offset of the source binary value; pass -1 to auto skip schema
     * version
     */
    void begin(MethodMaker mm, Variable rowVar,
               Variable keyVar, Variable valueVar, int valueOffset)
    {
        Objects.requireNonNull(mm);

        if (mMaker != null) {
            throw new IllegalStateException();
        }

        if (mAvailable == null) {
            rowVar = null;
        }

        mMaker = mm;
        mRowVar = rowVar;
        mKeyVar = keyVar;
        mValueVar = valueVar;
        mValueOffset = valueOffset;

        buildColumnSources();

        // Try to ditch conditional availability if possible, reducing runtime checks.
        for (Map.Entry<String, ColumnSource> e : mSources.entrySet()) {
            ColumnSource source = e.getValue();
            if (source.mAvailability == Availability.CONDITIONAL) {
                // Primitive types are cheap to extract from the binary encoding, or if no
                // transcoding is required, then a binary copy is preferred.
                if (source.isPrimitive() || !source.hasMismatches()) {
                    source.mAvailability = Availability.NEVER;
                }
            }
        }

        findColumns();

        encodeEagerTargets(mKeyTargets);
        encodeEagerTargets(mValueTargets);
    }

    void diffValueColumns(Variable oldValueVar) {
        // FIXME: Assume another transformer is involved. Must have the same schema. Can be
        // called once, only after begin.
    }

    /**
     * Makes code to encode a target key, but only after begin was called.
     *
     * @param keyTarget identifier returned by addKeyTarget
     * @return a byte[] variable
     */
    Variable encodeKey(int keyTarget) {
        Target target = mKeyTargets.get(keyTarget);
        encodeColumns(target);
        return target.mEncodedVar;
    }

    /**
     * Makes code to encode a target value, but only after begin was called.
     *
     * @param valueTarget identifier returned by addValueTarget
     * @param skip used only if diffValueColumns was called; if target value is the same as the
     * old value, then no value is encoded and instead control flow branches here
     * @return a byte[] variable
     */
    Variable encodeValue(int valueTarget, Label skip) {
        // FIXME: support skip label
        Target target = mValueTargets.get(valueTarget);
        encodeColumns(target);
        return target.mEncodedVar;
    }

    private void buildColumnSources() {
        RowGen srcGen = mRowInfo.rowGen();
        Map<String, ColumnCodec> keyCodecMap = srcGen.keyCodecMap();
        Map<String, ColumnCodec> valueCodecMap = srcGen.valueCodecMap();

        var sources = new HashMap<String, ColumnSource>();

        for (Target target : mKeyTargets) {
            ColumnCodec[] targetCodecs = target.mRowInfo.rowGen().keyCodecs();
            buildColumnSources(sources, keyCodecMap, true, target, targetCodecs);
            buildColumnSources(sources, valueCodecMap, false, target, targetCodecs);
            buildNullColumnSources(sources, targetCodecs);
        }

        for (Target target : mValueTargets) {
            ColumnCodec[] targetCodecs = target.mRowInfo.rowGen().valueCodecs();
            buildColumnSources(sources, keyCodecMap, true, target, targetCodecs);
            buildColumnSources(sources, valueCodecMap, false, target, targetCodecs);
            buildNullColumnSources(sources, targetCodecs);
        }

        mSources = sources;
    }

    /**
     * @param sources results stored here
     */
    private void buildColumnSources(Map<String, ColumnSource> sources,
                                    Map<String, ColumnCodec> srcCodecMap, boolean srcIsKey,
                                    Target target, ColumnCodec[] targetCodecs)
    {
        for (ColumnCodec targetCodec : targetCodecs) {
            String name = targetCodec.mInfo.name;
            ColumnCodec srcCodec = srcCodecMap.get(name);
            if (srcCodec == null) {
                continue;
            }
            ColumnSource source = sources.get(name);
            if (source == null) {
                Availability availability;
                if (mAvailable == null || (availability = mAvailable.get(name)) == null) {
                    availability = Availability.NEVER;
                }
                source = new ColumnSource(srcIsKey, srcCodec, availability);
                sources.put(name, source);
            }
            source.addTarget(targetCodec, target.mEager);
        }
    }

    /**
     * Builds column sources with NullColumnCodec for those not found in the source row/entry.
     *
     * @param sources results stored here
     */
    private void buildNullColumnSources(Map<String, ColumnSource> sources,
                                        ColumnCodec[] targetCodecs)
    {
        for (ColumnCodec targetCodec : targetCodecs) {
            String name = targetCodec.mInfo.name;
            if (!sources.containsKey(name)) {
                var srcCodec = new NullColumnCodec(targetCodec.mInfo, null);
                sources.put(name, new ColumnSource(false, srcCodec, Availability.NEVER));
            }
        }
    }

    /**
     * Generates code which finds column offsets in an encoded row.
     */
    private void findColumns() {
        // Determine how many columns must be accessed from the encoded form.
        int numKeys = 0, numValues = 0;
        for (ColumnSource source : mSources.values()) {
            if (source.mustFind()) {
                if (source.mIsKey) {
                    numKeys++;
                } else {
                    numValues++;
                }
            }
        }

        findColumns(mKeyVar, 0, true, numKeys);
        findColumns(mValueVar, mValueOffset, false, numValues);
    }

    /**
     * @param srcVar byte array
     */
    private void findColumns(Variable srcVar, int offset, boolean fromKey, int num) {
        if (num == 0) {
            return;
        }

        ColumnCodec[] codecs;
        Variable offsetVar = mMaker.var(int.class);

        if (fromKey) {
            codecs = mRowInfo.rowGen().keyCodecs();
            offsetVar.set(0);
        } else {
            codecs = mRowInfo.rowGen().valueCodecs();
            if (offset >= 0) {
                offsetVar.set(offset);
            } else {
                offsetVar.set(mMaker.var(RowUtils.class).invoke("skipSchemaVersion", srcVar));
            }
        }

        codecs = ColumnCodec.bind(codecs, mMaker);
        Variable endVar = null;

        for (ColumnCodec codec : codecs) {
            ColumnSource source = mSources.get(codec.mInfo.name);

            if (source == null || !source.mustFind()) {
                // Can't re-use end offset for next start offset when a gap exists.
                endVar = null;
                codec.decodeSkip(srcVar, offsetVar, null);
                continue;
            }

            if (source.mIsKey != fromKey) {
                throw new AssertionError();
            }

            Variable startVar = endVar;
            if (startVar == null) {
                // Need a stable copy.
                startVar = mMaker.var(int.class).set(offsetVar);
            }

            if (source.mEager &&
                source.mAvailability == Availability.NEVER && source.hasMismatches())
            {
                // Eager decoding is required when the column is never available in the row or
                // if it needs to be transcoded.
                var dstVar = mMaker.var(codec.mInfo.type);
                codec.decode(dstVar, srcVar, offsetVar, null);
                source.mDecodedVar = dstVar;
            } else {
                // Skip it for now. As side effect, the offsetVar is updated, and so the column
                // can be decoded later if necessary.
                codec.decodeSkip(srcVar, offsetVar, null);
            }

            // Need a stable copy.
            endVar = mMaker.var(int.class).set(offsetVar);

            source.mSrcVar = srcVar;
            source.mStartVar = startVar;
            source.mEndVar = endVar;

            if (--num <= 0) {
                break;
            }
        }
    }

    private void encodeEagerTargets(List<Target> targets) {
        for (Target target : targets) {
            if (target.mEager) {
                encodeColumns(target);
            }
        }
    }

    private void encodeColumns(Target target) {
        if (target.mEncodedVar != null) {
            // Already encoded.
            return;
        }

        RowGen targetGen = target.mRowInfo.rowGen();
        ColumnCodec[] codecs = target.mIsKey ? targetGen.keyCodecs() : targetGen.valueCodecs();

        if (codecs.length == 0) {
            if (target.mOffset == 0) {
                target.mEncodedVar = mMaker.var(RowUtils.class).field("EMPTY_BYTES");
            } else {
                target.mEncodedVar = mMaker.new_(byte[].class, target.mOffset);
            }
            return;
        }

        codecs = ColumnCodec.bind(codecs, mMaker);

        // Determine the minimum byte array size and prepare the encoders.

        int minSize = target.mOffset;
        for (ColumnCodec codec : codecs) {
            ColumnSource source = mSources.get(codec.mInfo.name);
            if (!source.shouldCopyBytes(codec)) {
                if (source.hasStash(codec) == null) {
                    minSize += codec.minSize();
                    codec.encodePrepare();
                    source.prepareColumn(mMaker, mRowVar, mRowInfo);
                }
            }
        }

        // Generate code which determines the additional runtime length.

        Variable totalVar = null;
        for (ColumnCodec codec : codecs) {
            ColumnSource source = mSources.get(codec.mInfo.name);
            if (source.shouldCopyBytes(codec)) {
                totalVar = codec.accum(totalVar, source.mEndVar.sub(source.mStartVar));
            } else {
                ColumnTarget stash = source.hasStash(codec);
                if (stash == null) {
                    totalVar = codec.encodeSize(source.accessColumn(mMaker), totalVar);
                } else {
                    totalVar = codec.accum(totalVar, stash.mLengthVar);
                }
            }
        }

        // Generate code which allocates the target byte array.

        Variable encodedVar;
        if (totalVar == null) {
            encodedVar = mMaker.new_(byte[].class, minSize);
        } else {
            if (minSize != 0) {
                totalVar = totalVar.add(minSize);
            }
            encodedVar = mMaker.new_(byte[].class, totalVar);
        }

        var offsetVar = mMaker.var(int.class).set(target.mOffset);
        for (int i=0; i<codecs.length; i++) {
            ColumnCodec codec = codecs[i];
            String name = codec.mInfo.name;
            ColumnSource source = mSources.get(name);

            if (source.shouldCopyBytes(codec)) {
                var lengthVar = source.mEndVar.sub(source.mStartVar);
                mMaker.var(System.class).invoke("arraycopy", source.mSrcVar, source.mStartVar,
                                                encodedVar, offsetVar, lengthVar);
                if (i < codecs.length - 1) {
                    offsetVar.inc(lengthVar);
                }
            } else {
                ColumnTarget stash = source.hasStash(codec);
                if (stash != null) {
                    // Copy the stashed encoding.
                    mMaker.var(System.class).invoke
                        ("arraycopy", stash.mEncodedVar, stash.mStartVar,
                         encodedVar, offsetVar, stash.mLengthVar);
                    if (i < codecs.length - 1) {
                        offsetVar.inc(stash.mLengthVar);
                    }
                } else if ((stash = source.shouldStash(codec, target)) != null) {
                    // Encode the first time and stash for later.
                    stash.mEncodedVar = encodedVar;
                    stash.mStartVar = offsetVar.get();
                    codec.encode(source.accessColumn(mMaker), encodedVar, offsetVar);
                    stash.mLengthVar = offsetVar.sub(stash.mStartVar);
                } else {
                    // Encode and don't stash for later.
                    codec.encode(source.accessColumn(mMaker), encodedVar, offsetVar);
                }
            }
        }

        target.mEncodedVar = encodedVar;
    }

    static enum Availability {
        /** Column is always available in the row instance. */
        ALWAYS,

        /** Column is never available in the row instance. */
        NEVER,

        /**
         * Column availability must be checked at runtime. Unset columns are ignored and are
         * instead decoded from the binary key or value.
         */
        CONDITIONAL
    }

    /**
     * Tracks a key/value target to be encoded.
     */
    private static class Target {
        final boolean mIsKey;
        final RowInfo mRowInfo;
        final int mOffset;
        final boolean mEager;

        // The fully encoded target row, as a byte array.
        Variable mEncodedVar;

        Target(boolean isKey, RowInfo rowInfo, int offset, boolean eager) {
            mIsKey = isKey;
            mRowInfo = rowInfo;
            mOffset = offset;
            mEager = eager;
        }
    }

    /**
     * State for a column source.
     */
    private static class ColumnSource {
        final boolean mIsKey;
        final ColumnCodec mCodec;
        Availability mAvailability;

        // Maps target codecs to additional state.
        final Map<ColumnCodec, ColumnTarget> mColumnTargets;

        // Is true if at least one target eagerly depends on this source.
        boolean mEager;

        // Is one if a target codec exists which is equal to the source codec.
        int mMatches;

        // Source byte array and decoded offsets. Offset vars are null for skipped columns.
        Variable mSrcVar, mStartVar, mEndVar;

        // Fully decoded column. Is defined when the column cannot be directly copied from the
        // source, and it needed a transformation step. Or when decoding is cheap.
        // FIXME: Can only be trusted when eagerly decoded. Otherwise, add a runtime check.
        Variable mDecodedVar;

        ColumnSource(boolean isKey, ColumnCodec codec, Availability availability) {
            mIsKey = isKey;
            mCodec = codec;
            mAvailability = availability;
            mColumnTargets = new HashMap<>();
        }

        /**
         * Returns true if source type is a boxed or unboxed primitive type.
         */
        boolean isPrimitive() {
            return mCodec instanceof PrimitiveColumnCodec;
        }

        ColumnTarget addTarget(ColumnCodec targetCodec, boolean eager) {
            mEager |= eager;
            ColumnTarget target = mColumnTargets.get(targetCodec);
            if (target == null) {
                if (mCodec.equals(targetCodec)) {
                    mMatches = 1;
                }
                target = new ColumnTarget();
                mColumnTargets.put(targetCodec, target);
            }
            target.mUsageCount++;
            return target;
        }

        /**
         * Returns true if any target codecs need transcoding.
         */
        boolean hasMismatches() {
            return mMatches < mColumnTargets.size();
        }

        /**
         * Returns true if the column must be found in the encoded byte array.
         */
        boolean mustFind() {
            if (mCodec instanceof NullColumnCodec) {
                // Can't find that which doesn't exist.
                return false;
            }

            if (mAvailability == Availability.ALWAYS) {
                if (isPrimitive()) {
                    // Primitive columns are cheap to encode, so no need to copy the byte form.
                    return false;
                } else {
                    // Return true if at least one target can copy the byte form, avoiding the
                    // cost of encoding the column from the row object.
                    return mMatches != 0;
                }
            }

            // No guarantee that the column is available in the row, so find it.
            return true;
        }

        /**
         * Returns true if the target codec matches this source codec and the column must be
         * found in the encoded byte array.
         */
        boolean shouldCopyBytes(ColumnCodec targetCodec) {
            return targetCodec.equals(mCodec) && mustFind();
        }

        /**
         * Returns non-null if an encoded target column has been stashed and should be copied
         * to avoid redundant (and expensive) transformations.
         */
        ColumnTarget hasStash(ColumnCodec targetCodec) {
            ColumnTarget columnTarget = mColumnTargets.get(targetCodec);
            if (columnTarget != null && columnTarget.mEncodedVar != null) {
                return columnTarget;
            }
            return null;
        }

        /**
         * Returns non-null if an encoded target column should be stashed and copied again to
         * avoid redundant (and expensive) transformations.
         */
        ColumnTarget shouldStash(ColumnCodec targetCodec, Target target) {
            ColumnTarget columnTarget;
            if (target.mEager && !isPrimitive() &&
                (columnTarget = mColumnTargets.get(targetCodec)) != null &&
                columnTarget.mUsageCount > 1)
            {
                return columnTarget;
            }
            return null;
        }

        /**
         * Prepares the necessary column checks in order for accessColumn to work.
         *
         * @param rowVar source row
         */
        void prepareColumn(MethodMaker mm, Variable rowVar, RowInfo rowInfo) {
            if (mDecodedVar != null) {
                // Already prepared or was eagerly decoded from the binary form.
                return;
            }

            if (mCodec instanceof NullColumnCodec ncc) {
                mDecodedVar = mm.var(ncc.mInfo.type);
                ncc.decode(mDecodedVar, null, null, null);
                return;
            }

            if (mAvailability == Availability.ALWAYS) {
                mDecodedVar = rowVar.field(mCodec.mInfo.name);
                return;
            }

            if (mAvailability != Availability.CONDITIONAL) {
                return;
            }

            if (rowVar == null || rowInfo == null) {
                throw new AssertionError();
            }

            // FIXME: If not mEager, need to check each time.

            mDecodedVar = mm.var(mCodec.mInfo.type);

            RowGen rowGen = rowInfo.rowGen();
            String columnName = mCodec.mInfo.name;
            int columnNum = rowGen.columnNumbers().get(columnName);
            String stateField = rowGen.stateField(columnNum);
            int stateFieldMask = rowGen.stateFieldMask(columnNum);

            Label mustDecode = mm.label();
            rowVar.field(stateField).and(stateFieldMask).ifEq(0, mustDecode);
            // The column field is clean or dirty, so use it directly.
            mDecodedVar.set(rowVar.field(columnName));
            Label cont = mm.label().goto_();
            mustDecode.here();
            // The decode method modifies the offset, so operate against a copy.
            var offsetVar = mStartVar.get();
            mCodec.bind(mm).decode(mDecodedVar, mSrcVar, offsetVar, null);

            cont.here();
        }

        /**
         * Returns a decoded variable or a field from the row. A call to prepareColumn must
         * have been made earlier.
         */
        // FIXME: remove? will it be different if mEager?
        Variable accessColumn(MethodMaker mm) {
            return mDecodedVar;
        }
    }

    /**
     * State for a column target. The number of actual target instances in use might be less
     * than the number of targets added, if they have a common encoding strategy.
     */
    private static class ColumnTarget {
        // Count of times this target is used.
        int mUsageCount;

        // Eagerly encoded byte[] slice which can be copied to avoid redundant transformations.
        Variable mEncodedVar, mStartVar, mLengthVar;
    }
}
