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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.cojen.maker.Field;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.table.codec.ColumnCodec;
import org.cojen.tupl.table.codec.VoidColumnCodec;

/**
 * Utility for making code which transforms binary encoded rows to one or more other binary
 * encodings. This utility is designed for column placement and transcoding, performing
 * lossy type conversions if necessary.
 *
 * @author Brian S O'Neill
 */
class TransformMaker<R> {
    private final Class<R> mRowType;
    private final RowInfo mRowInfo;
    private final Map<String, Availability> mAvailable;

    private List<Target> mTargets;

    private MethodMaker mMaker;
    private Variable mRowVar, mKeyVar, mValueVar;
    private Object mValueOffset; // Integer, Variable, or null (to auto skip schema version)

    private Map<String, ColumnSource> mColumnSources;

    private Variable[] mDiffBitMap;

    private boolean mRequiresRow;

    /**
     * @param rowType source row type; can pass null if no row instance is available
     * @param rowInfo source row info; can pass null to obtain from rowType
     * @param available defines which columns from a row instance are available; can pass null
     * if all columns are never available
     */
    public TransformMaker(Class<R> rowType, RowInfo rowInfo, Map<String, Availability> available) {
        this(rowType, rowInfo, available, true);
    }

    private TransformMaker(Class<R> rowType, RowInfo rowInfo, Map<String, Availability> available,
                           boolean validate)
    {
        if (validate) {
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
        }

        mRowType = rowType;
        mRowInfo = rowInfo;
        mAvailable = available;

        mTargets = new ArrayList<>();
    }

    /**
     * Add a transformation target, which will have key encoding applied to it. A target
     * identifier is returned, which always starts at 0 and increments by 1 for subsequent
     * targets.
     *
     * @param rowInfo target row info
     * @param offset start offset of target value (padding)
     * @param eager true if the target will always be encoded
     */
    public int addKeyTarget(RowInfo rowInfo, int offset, boolean eager) {
        return addTarget(true, rowInfo, offset, eager);
    }

    /**
     * Add a transformation target, which will have value encoding applied to it. A target
     * identifier is returned, which always starts at 0 and increments by 1 for subsequent
     * targets.
     *
     * @param rowInfo target row info
     * @param offset start offset of target value (padding)
     * @param eager true if the target will always be encoded
     */
    public int addValueTarget(RowInfo rowInfo, int offset, boolean eager) {
        return addTarget(false, rowInfo, offset, eager);
    }

    private int addTarget(boolean isKey, RowInfo rowInfo, int offset, boolean eager) {
        if (mColumnSources != null) {
            throw new IllegalStateException();
        }
        mTargets.add(new Target(isKey, rowInfo, offset, eager));
        return mTargets.size() - 1;
    }

    /**
     * Returns true if the targets only need to access source keys. After calling this method,
     * no more targets can be added.
     */
    public boolean onlyNeedsKeys() {
        buildColumnSources();
        for (ColumnSource source : mColumnSources.values()) {
            if (!source.mIsKey) {
                return false;
            }
        }
        return true;
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
    public void begin(MethodMaker mm, Variable rowVar,
                      Variable keyVar, Variable valueVar, int valueOffset)
    {
        setup(mm, rowVar, keyVar, valueVar, valueOffset);

        findColumns(false);
    }

    /**
     * Begin making code which computes the difference between two encoded values which have
     * the same schema, returning a maker suitable for making the old value. The diffValueCheck
     * method can be called (on the returned maker) to skip over targets that haven't changed.
     */
    public TransformMaker<R> beginValueDiff(MethodMaker mm, Variable rowVar,
                                            Variable keyVar, Variable valueVar, int valueOffset,
                                            Variable oldValueVar)
    {
        setup(mm, rowVar, keyVar, valueVar, valueOffset);

        // Binary comparison requires that all value columns in the encoded value be found.
        for (ColumnSource source : mColumnSources.values()) {
            if (!source.mIsKey) {
                source.mForceFind = true;
            }
        }

        var oldMaker = new TransformMaker<>(mRowType, mRowInfo, mAvailable, false);

        oldMaker.mTargets = cloneTargets(mTargets);

        // Find with keepValueOffset such that oldMaker doesn't need to find it again.
        findColumns(true);

        oldMaker.setup(mm, rowVar, keyVar, oldValueVar, mValueOffset);

        // Never consult the row instance for value columns, ensuring that they must be found
        // in the encoded value. Then find only the value columns for the old maker, and the
        // already found key columns will be copied over.
        {
            int numValues = 0;
            for (ColumnSource source : oldMaker.mColumnSources.values()) {
                if (!source.mIsKey) {
                    source.mAvailability = Availability.NEVER;
                    if (source.mustFind()) {
                        numValues++;
                    }
                }
            }

            oldMaker.findColumns(oldMaker.mValueVar, false, numValues, false);

            // Copy the key columns sources, since they don't need to be found again.
            for (Map.Entry<String, ColumnSource> e : mColumnSources.entrySet()) {
                ColumnSource source = e.getValue();
                if (source.mIsKey) {
                    oldMaker.mColumnSources.put(e.getKey(), source);
                }
            }
        }

        // Use a bit map (of longs) whose bits correspond to a ColumnSource slot. Some
        // variables in the array will remain null, which implies that the bits are all zero
        // for that particular range.
        var bitMap = new Variable[numBitMapWords(mColumnSources)];

        for (ColumnSource oldSource : oldMaker.mColumnSources.values()) {
            ColumnSource source = mColumnSources.get(oldSource.mCodec.info.name);

            if (source.mIsKey || source.mCodec instanceof VoidColumnCodec) {
                continue;
            }

            int slot = source.mSlot;

            Variable bitsVar = bitMap[bitMapWord(slot)];
            if (bitsVar == null) {
                bitsVar = mm.var(long.class).set(0);
                bitMap[bitMapWord(slot)] = bitsVar;
            }

            Label same = mm.label();

            Variable columnVar = source.accessColumn(this);
            if (columnVar != null && source.isPrimitive()) {
                // Quick compare.
                columnVar.ifEq(oldSource.accessColumn(oldMaker), same);
            } else {
                // Compare array ranges in the encoded values.
                mMaker.var(Arrays.class).invoke
                    ("equals", source.mSrcVar, source.mStartVar, source.mEndVar,
                     oldSource.mSrcVar, oldSource.mStartVar, oldSource.mEndVar)
                    .ifTrue(same);
            }

            // Set the bit to indicate that a difference was found.
            bitsVar.set(bitsVar.or(bitMapWordMask(slot)));

            same.here();
        }

        oldMaker.mDiffBitMap = bitMap;

        // Assign the masks needed by the diffValueCheck method.
        for (Target target : oldMaker.mTargets) {
            target.assignValueSourceMasks(oldMaker.mColumnSources);
        }

        for (Target target : mTargets) {
            // Column variables need to be definitely assigned in advance due to conditional
            // branching from all the expected diffValueCheck calls.
            prepareColumns(target);
            oldMaker.prepareColumns(target);
        }

        return oldMaker;
    }

    /**
     * Call after beginValueDiff has been called to make code which skips over targets that
     * haven't changed.
     *
     * @param skip branch to this label when none of the specified targets have changed
     * @param targetIds identifier returned by addKeyTarget or addValueTarget
     * @return false if target is always skipped and no code was generated
     */
    public boolean diffValueCheck(Label skip, int... targetIds) {
        boolean notSkipped = false;
        Label modified = mMaker.label();

        for (int i=0; i<mDiffBitMap.length; i++) {
            Variable word = mDiffBitMap[i];
            if (word != null) {
                long mask = 0;
                for (int targetId : targetIds) {
                    mask |= mTargets.get(targetId).mSourceMasks[i];
                }
                if (mask != 0) {
                    notSkipped = true;
                    word.and(mask).ifNe(0L, modified);
                }
            }
        }

        if (notSkipped) {
            mMaker.goto_(skip);
            modified.here();
        }

        return notSkipped;
    }

    /**
     * Makes code to encode a target key or value, but only after begin was called.
     *
     * @param targetId identifier returned by addKeyTarget or addValueTarget
     * @return a byte[] variable
     */
    public Variable encode(int targetId) {
        Target target = mTargets.get(targetId);
        encodeColumns(target);
        return target.mEncodedVar;
    }

    /**
     * Returns false if a row instance was never accessed by the generated code.
     */
    public boolean requiresRow() {
        return mRequiresRow;
    }

    /**
     * Assigns the maker related variables and builds the map of ColumnSources.
     */
    private void setup(MethodMaker mm, Variable rowVar,
                       Variable keyVar, Variable valueVar, int valueOffset)
    {
        setup(mm, rowVar, keyVar, valueVar, valueOffset < 0 ? null : valueOffset);
    }

    private void setup(MethodMaker mm, Variable rowVar,
                       Variable keyVar, Variable valueVar, Object valueOffset)
    {
        buildColumnSources();

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
    }

    private void buildColumnSources() {
        if (mColumnSources != null) {
            return;
        }

        RowGen srcGen = mRowInfo.rowGen();
        Map<String, ColumnCodec> keyCodecMap = srcGen.keyCodecMap();
        Map<String, ColumnCodec> valueCodecMap = srcGen.valueCodecMap();

        var sources = new HashMap<String, ColumnSource>();

        for (Target target : mTargets) {
            RowGen rowGen = target.mRowInfo.rowGen();
            ColumnCodec[] targetCodecs = target.mIsKey ? rowGen.keyCodecs() : rowGen.valueCodecs();
            buildColumnSources(sources, keyCodecMap, true, target, targetCodecs);
            buildColumnSources(sources, valueCodecMap, false, target, targetCodecs);
            buildVoidColumnSources(sources, targetCodecs);
        }

        // Try to ditch conditional availability if possible, reducing runtime checks.
        for (ColumnSource source : sources.values()) {
            if (source.mAvailability == Availability.CONDITIONAL) {
                // Primitive types are cheap to extract from the binary encoding, or if no
                // transcoding is required, then a binary copy is preferred.
                if (source.isPrimitive() || !source.hasMismatches()) {
                    source.mAvailability = Availability.NEVER;
                }
            }
        }

        mColumnSources = sources;
    }

    /**
     * @param sources results stored here
     */
    private void buildColumnSources(Map<String, ColumnSource> sources,
                                    Map<String, ColumnCodec> srcCodecMap, boolean srcIsKey,
                                    Target target, ColumnCodec[] targetCodecs)
    {
        for (ColumnCodec targetCodec : targetCodecs) {
            String name = targetCodec.info.name;
            ColumnCodec srcCodec = srcCodecMap.get(name);
            if (srcCodec == null) {
                continue;
            }

            ColumnSource source = sources.get(name);
            if (source == null) {
                int slot = sources.size();
                Availability availability;
                if (mAvailable == null || (availability = mAvailable.get(name)) == null) {
                    availability = Availability.NEVER;
                }
                source = new ColumnSource(slot, srcIsKey, srcCodec, availability);
                sources.put(name, source);
            }

            source.addTarget(targetCodec, target.mEager);
        }
    }

    /**
     * Builds column sources with VoidColumnCodec for those not found in the source row/entry.
     *
     * @param sources results stored here
     */
    private void buildVoidColumnSources(Map<String, ColumnSource> sources,
                                        ColumnCodec[] targetCodecs)
    {
        for (ColumnCodec targetCodec : targetCodecs) {
            String name = targetCodec.info.name;
            if (!sources.containsKey(name)) {
                int slot = sources.size();
                var srcCodec = new VoidColumnCodec(targetCodec.info, null);
                sources.put(name, new ColumnSource(slot, false, srcCodec, Availability.NEVER));
            }
        }
    }

    /**
     * Generates code which finds column offsets in an encoded row.
     *
     * @param keepValueOffset when true, assign known mValueOffset when configured to auto skip
     * schema version
     */
    private void findColumns(boolean keepValueOffset) {
        // Determine how many columns must be accessed from the encoded form.
        int numKeys = 0, numValues = 0;
        for (ColumnSource source : mColumnSources.values()) {
            if (source.mustFind()) {
                if (source.mIsKey) {
                    numKeys++;
                } else {
                    numValues++;
                }
            }
        }

        findColumns(mKeyVar, true, numKeys, false);
        findColumns(mValueVar, false, numValues, keepValueOffset);
    }

    /**
     * @param srcVar byte array
     */
    private void findColumns(Variable srcVar, boolean fromKey, int num, boolean keepValueOffset) {
        if (srcVar == null || num == 0) {
            return;
        }

        ColumnCodec[] codecs;
        Variable offsetVar = mMaker.var(int.class);

        if (fromKey) {
            codecs = mRowInfo.rowGen().keyCodecs();
            offsetVar.set(0);
        } else {
            codecs = mRowInfo.rowGen().valueCodecs();
            if (mValueOffset == null) {
                offsetVar.set(mMaker.var(RowUtils.class).invoke("skipSchemaVersion", srcVar));
                if (keepValueOffset) {
                    mValueOffset = offsetVar.get();
                }
            } else {
                offsetVar.set(mValueOffset);
            }
        }

        codecs = ColumnCodec.bind(codecs, mMaker);
        Variable endVar = null;

        for (ColumnCodec codec : codecs) {
            ColumnSource source = mColumnSources.get(codec.info.name);

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

            if (source.mustDecodeEagerly()) {
                var columnVar = mMaker.var(codec.info.type);
                codec.decode(columnVar, srcVar, offsetVar, null);
                source.initColumnVar(columnVar);
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
                return;
            }
        }

        // Reached if num was miscalculated.
        throw new AssertionError();
    }

    /**
     * Each "word" is a long and holds 64 bits.
     */
    private static int numBitMapWords(Map<String, ColumnSource> columnSources) {
        return (columnSources.size() + 63) >> 6;
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
     * Invokes prepareColumn for all sources that the target depends on, just like
     * encodeColumns would do.
     */
    private void prepareColumns(Target target) {
        if (target.mEncodedVar != null) {
            // Already encoded, which implies that all columns were prepared too.
            return;
        }

        RowGen targetGen = target.mRowInfo.rowGen();
        ColumnCodec[] codecs = target.mIsKey ? targetGen.keyCodecs() : targetGen.valueCodecs();

        for (ColumnCodec codec : codecs) {
            ColumnSource source = mColumnSources.get(codec.info.name);
            if (!source.mustCopyBytes(codec)) {
                source.prepareColumn(this);
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
            ColumnSource source = mColumnSources.get(codec.info.name);
            if (!source.mustCopyBytes(codec)) {
                if (source.hasStash(codec) == null) {
                    minSize += codec.minSize();
                    codec.encodePrepare();
                    source.prepareColumn(this);
                }
            }
        }

        // Generate code which determines the additional runtime length.

        Variable totalVar = null;
        for (ColumnCodec codec : codecs) {
            ColumnSource source = mColumnSources.get(codec.info.name);
            if (source.mustCopyBytes(codec)) {
                totalVar = codec.accum(totalVar, source.mEndVar.sub(source.mStartVar));
            } else {
                ColumnTarget stash = source.hasStash(codec);
                if (stash == null) {
                    var columnVar = source.accessColumn(this);
                    totalVar = codec.encodeSize(columnVar, totalVar);
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
            String name = codec.info.name;
            ColumnSource source = mColumnSources.get(name);

            if (source.mustCopyBytes(codec)) {
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
                } else {
                    // Can bypass accessColumn and its mColumnAccessCheck because accessColumn
                    // was called earlier to determine the additional runtime length.
                    var columnVar = source.mColumnVar;
                    if ((stash = source.shouldStash(codec, target)) != null) {
                        // Encode the first time and stash for later.
                        stash.mEncodedVar = encodedVar;
                        stash.mStartVar = offsetVar.get();
                        codec.encode(columnVar, encodedVar, offsetVar);
                        stash.mLengthVar = offsetVar.sub(stash.mStartVar);
                    } else {
                        // Encode and don't stash for later.
                        codec.encode(columnVar, encodedVar, offsetVar);
                    }
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

    private static List<Target> cloneTargets(List<Target> targets) {
        var clone = new ArrayList<Target>(targets.size());
        for (Target t : targets) {
            clone.add(t.clone());
        }
        return clone;
    }

    /**
     * Tracks a key/value target to be encoded.
     */
    private static class Target implements Cloneable {
        final boolean mIsKey;
        final RowInfo mRowInfo;
        final int mOffset;
        final boolean mEager;

        // The fully encoded target row, as a byte array.
        Variable mEncodedVar;

        // Indicates which sources this target depends on, for a value difference operation.
        long[] mSourceMasks;

        Target(boolean isKey, RowInfo rowInfo, int offset, boolean eager) {
            mIsKey = isKey;
            mRowInfo = rowInfo;
            mOffset = offset;
            mEager = eager;
        }

        /**
         * Assign the bit mask needed for a value diff.
         */
        void assignValueSourceMasks(Map<String, ColumnSource> sources) {
            var masks = new long[numBitMapWords(sources)];

            Map<String, ColumnInfo> columns = mIsKey ? mRowInfo.keyColumns : mRowInfo.valueColumns;

            for (String name : columns.keySet()) {
                ColumnSource source = sources.get(name);
                if (!source.mIsKey) {
                    int slot = source.mSlot;
                    masks[bitMapWord(slot)] |= bitMapWordMask(slot);
                }
            }

            mSourceMasks = masks;
        }

        @Override
        public Target clone() {
            try {
                return (Target) super.clone();
            } catch (CloneNotSupportedException e) {
                throw RowUtils.rethrow(e);
            }
        }
    }

    /**
     * State for a column source.
     */
    private static class ColumnSource {
        final int mSlot;
        final boolean mIsKey;
        final ColumnCodec mCodec;
        Availability mAvailability;

        // Maps target codecs to additional state.
        final Map<ColumnCodec, ColumnTarget> mColumnTargets;

        // Common target column type. A conversion is required when different from the source.
        ColumnInfo mTargetInfo;

        // Is true if at least one target eagerly depends on this source.
        boolean mEager;

        // Is one if a target codec exists which is equal to the source codec.
        int mMatches;

        boolean mForceFind;

        // The remaining fields are used during code generation.

        // Source byte array and decoded offsets. Offset vars are null for skipped columns.
        Variable mSrcVar, mStartVar, mEndVar;

        // Fully decoded column. Is defined when the column cannot be directly copied from the
        // source, and it needed a transformation step. Or when decoding is cheap.
        Variable mColumnVar;

        // Optional boolean variable used when column isn't eagerly decoded.
        Variable mColumnSetVar;

        // Is true when column isn't eagerly decoded and must be checked at runtime.
        boolean mColumnAccessCheck;

        ColumnSource(int slot, boolean isKey, ColumnCodec codec, Availability availability) {
            mSlot = slot;
            mIsKey = isKey;
            mCodec = codec;
            mAvailability = availability;
            mColumnTargets = new HashMap<>();
        }

        ColumnTarget addTarget(ColumnCodec targetCodec, boolean eager) {
            if (mTargetInfo == null) {
                mTargetInfo = targetCodec.info;
                if (mTargetInfo.type == null) {
                    mTargetInfo.assignType();
                }
            } else if (!mTargetInfo.isCompatibleWith(targetCodec.info)) {
                // For now, all targets must have the same column type.
                throw new IllegalStateException();
            }

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
         * Returns true if source type is an unboxed primitive type.
         */
        boolean isPrimitive() {
            return mCodec.info.type.isPrimitive();
        }

        /**
         * Returns true if any target codecs need transcoding.
         */
        boolean hasMismatches() {
            return mMatches < mColumnTargets.size();
        }

        /**
         * Returns true if the column must be decoded during the find step.
         */
        boolean mustDecodeEagerly() {
            if (mAvailability != Availability.NEVER) {
                // If the column is always available in the row no need to decode anything. If
                // conditionally available in the row, decode on demand if necessary.
                return false;
            } else if (isPrimitive()) {
                // If never available in the row, but decoding is cheap, go ahead and decode
                // even if the column is never used. This avoids having to create an additional
                // variable and check step to determine if the column has been decoded yet.
                return true;
            } else {
                // If never available in the row, decode if eager and transcoding is required.
                return mEager && hasMismatches();
            }
        }

        /**
         * Returns true if the column must be found in the encoded byte array, if provided.
         */
        boolean mustFind() {
            if (mCodec instanceof VoidColumnCodec) {
                // Can't find that which doesn't exist.
                return false;
            }

            if (!mForceFind && mAvailability == Availability.ALWAYS) {
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
        boolean mustCopyBytes(ColumnCodec targetCodec) {
            return mSrcVar != null && targetCodec.equals(mCodec) && mustFind();
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
         * Prepares the necessary column checks in order for accessColumn to work. Neither
         * method should be called when mustCopyBytes is true.
         */
        void prepareColumn(TransformMaker tm) {
            if (mColumnVar != null) {
                // Already prepared or was eagerly decoded from the binary form.
                return;
            }

            if (mCodec instanceof VoidColumnCodec vcc) {
                mColumnVar = tm.mMaker.var(vcc.info.type);
                vcc.decode(mColumnVar, null, null, null);
                return;
            }

            if (mAvailability == Availability.ALWAYS) {
                mColumnVar = tm.mRowVar.field(mCodec.info.name);
                return;
            }

            mColumnVar = tm.mMaker.var(mTargetInfo.type);

            if (mEager) {
                if (mAvailability == Availability.CONDITIONAL) {
                    decodeColumn(tm);
                } else {
                    // Is never available in the row and it should have already been decoded.
                    throw new AssertionError();
                }
            } else {
                // Need to check if the column is set at runtime.
                mColumnAccessCheck = true;
                mColumnVar.clear(); // needs to be definitely assigned
                if (isPrimitive() || mTargetInfo.isNullable()) {
                    // Need an additional variable for checking if set or not. A null check
                    // works fine for non-nullable object types.
                    mColumnSetVar = tm.mMaker.var(boolean.class).set(false);
                }
            }
        }

        /**
         * @param columnVar must be assigned
         */
        void initColumnVar(Variable columnVar) {
            if (mColumnVar != null) {
                throw new IllegalStateException();
            }
            if (mCodec.info.isCompatibleWith(mTargetInfo)) {
                mColumnVar = columnVar;
            } else {
                mColumnVar = columnVar.methodMaker().var(mTargetInfo.type);
                setToColumnVar(columnVar);
            }
        }

        /**
         * @param columnVar must be assigned
         */
        private void setToColumnVar(Variable columnVar) {
            if (mCodec.info.isCompatibleWith(mTargetInfo)) {
                mColumnVar.set(columnVar);
            } else {
                MethodMaker mm = columnVar.methodMaker();
                Converter.convertLossy(mm, mCodec.info, columnVar, mTargetInfo, mColumnVar);
            }
        }

        /**
         * Returns a decoded variable or a field from the row. A call to prepareColumn must
         * have been made earlier.
         */
        Variable accessColumn(TransformMaker tm) {
            if (mColumnAccessCheck) {
                Label isSet = tm.mMaker.label();
                if (mColumnSetVar == null) {
                    mColumnVar.ifNe(null, isSet);
                    decodeColumn(tm);
                } else {
                    mColumnSetVar.ifTrue(isSet);
                    decodeColumn(tm);
                    mColumnSetVar.set(true);
                }
                isSet.here();
            } else if (mColumnVar instanceof Field) {
                tm.mRequiresRow = true;
            }

            return mColumnVar;
        }

        private void decodeColumn(TransformMaker tm) {
            MethodMaker mm = tm.mMaker;
            Label cont = null;

            if (mAvailability != Availability.NEVER) {
                Variable rowVar = tm.mRowVar;
                RowInfo rowInfo = tm.mRowInfo;

                if (rowVar == null || rowInfo == null) {
                    // Need to access a row instance.
                    throw new AssertionError();
                }

                tm.mRequiresRow = true;

                RowGen rowGen = rowInfo.rowGen();
                String columnName = mCodec.info.name;
                int columnNum = rowGen.columnNumbers().get(columnName);
                String stateField = rowGen.stateField(columnNum);
                int stateFieldMask = RowGen.stateFieldMask(columnNum);

                Label mustDecode = mm.label();
                rowVar.field(stateField).and(stateFieldMask).ifNe(stateFieldMask, mustDecode);
                // The column field is dirty, so use it directly. The update method only
                // examines dirty fields when encoding the key and value, and so clean fields
                // cannot be trusted in case the underlying row changed concurrently.
                setToColumnVar(rowVar.field(columnName));
                cont = mm.label().goto_();
                mustDecode.here();
            }

            // The decode method modifies the offset, so operate against a copy.
            var offsetVar = mStartVar.get();
            mCodec.bind(mm).decode(mColumnVar, mSrcVar, offsetVar, null);

            if (cont != null) {
                cont.here();
            }
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
