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

package org.cojen.tupl.table;

import java.lang.invoke.MethodHandle;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.Cursor;

import org.cojen.tupl.table.codec.ColumnCodec;

import org.cojen.tupl.table.filter.AndFilter;
import org.cojen.tupl.table.filter.ColumnToArgFilter;
import org.cojen.tupl.table.filter.ColumnToColumnFilter;
import org.cojen.tupl.table.filter.OrFilter;
import org.cojen.tupl.table.filter.RowFilter;
import org.cojen.tupl.table.filter.TrueFilter;
import org.cojen.tupl.table.filter.Visitor;

/**
 * Generates code to filter and decode rows for a specific schema version. After passing this
 * visitor to the RowFilter.accept method, call a finish method.
 *
 * @author Brian S O'Neill
 * @see FilteredScanMaker
 */
class DecodeVisitor implements Visitor {
    private final MethodMaker mMaker;
    private Variable mRowVar, mKeyVar, mValueVar, mCursorVar;
    private final int mValueOffset;
    private final RowGen mRowGen;
    private final Variable mPredicateVar;
    private final String mStopColumn;
    private final int mStopArgument;

    private final ColumnCodec[] mKeyCodecs, mValueCodecs;

    private Label mPass, mFail;

    private LocatedColumn[] mLocatedKeys;
    private int mHighestLocatedKey;

    private LocatedColumn[] mLocatedValues;
    private int mHighestLocatedValue;

    /**
     * Supports three forms of decode methods and two forms of predicate methods.
     *
     *     R decodeRow(byte[] key, byte[] value, ...)
     *     R decodeRow(byte[] key, Cursor c, ...)
     *     R decodeRow(Cursor c, ...)
     *
     * If a stopColumn and stopArgument are provided, then a cursor method form is required in
     * order for the stop to actually work.
     *
     *     boolean test(byte[] key, byte[] value, ...)
     *     boolean test(R row, byte[] key, byte[] value, ...)
     *
     * The predicate form which accepts a row only examines dirty columns from it.
     *    NOTE: Row paramater isn't currently supported.
     *
     * @param mm signature: see above
     * @param valueOffset offset to skip past the schema version
     * @param rowGen actual row definition to be decoded
     * @param predicateVar implements RowPredicate
     * @param stopColumn optional
     * @param stopArgument required when stopColumn is provided
     */
    DecodeVisitor(MethodMaker mm, int valueOffset, RowGen rowGen,
                  Variable predicateVar, String stopColumn, int stopArgument)
    {
        mMaker = mm;
        mValueOffset = valueOffset;
        mRowGen = rowGen;
        mPredicateVar = predicateVar;
        mStopColumn = stopColumn;
        mStopArgument = stopArgument;

        mKeyCodecs = ColumnCodec.bind(rowGen.keyCodecs(), mm);
        mValueCodecs = ColumnCodec.bind(rowGen.valueCodecs(), mm);
    }

    /**
     * Initialize the row, key, value, and cursor variables.
     */
    private void initVars(boolean requireValue) {
        if (mKeyVar != null) {
            return;
        }

        int offset = 0;
        while (true) {
            mKeyVar = mMaker.param(offset);
            Class<?> keyType = mKeyVar.classType(); 

            if (keyType == Cursor.class) {
                mCursorVar = mKeyVar;
                mKeyVar = mCursorVar.invoke("key");
                if (requireValue) {
                    mValueVar = mCursorVar.invoke("value");
                }
                break;
            } else if (keyType == byte[].class) {
                mValueVar = mMaker.param(offset + 1);
                if (mValueVar.classType() == Cursor.class) {
                    mCursorVar = mValueVar;
                    if (requireValue) {
                        mValueVar = mCursorVar.invoke("value");
                    } else {
                        mValueVar = null;
                    }
                }
                break;
            } else {
                if (true) {
                    throw new UnsupportedOperationException();
                }
                mRowVar = mKeyVar;
                offset++;
            }
        }
    }

    /**
     * Call to generate filtering code. If called more than once, each additional filter
     * behaves as-if it was combined with the 'and' operator.
     */
    void applyFilter(RowFilter filter) {
        if (filter != TrueFilter.THE) {
            mPass = mMaker.label();
            mFail = mMaker.label();
            initVars(true);
            mHighestLocatedKey = -1;
            mHighestLocatedValue = -1;
            filter.accept(this);
        }
    }

    /**
     * Join a secondary to a primary, returning the primary key and value. If the join returns
     * null, then the generated method returns null. After calling this method, this visitor
     * cannot be used again.
     *
     * @param resultVar LockResult from cursor access
     * @param primaryCursorVar non-null if should be passed to join method which accepts it
     */
    Variable[] joinToPrimary(Variable resultVar, Variable primaryCursorVar) {
        var secInfo = (SecondaryInfo) mRowGen.info; // cast as an assertion
        boolean isAltKey = secInfo.isAltKey();

        // Must call this in case applyFilter wasn't called, or it did nothing.
        initVars(isAltKey);

        if (mCursorVar == null) {
            throw new IllegalStateException();
        }

        passFail(null);

        Variable primaryKeyVar;
        if (isAltKey) {
            primaryKeyVar = mMaker.invoke("toPrimaryKey", mKeyVar, mValueVar);
        } else {
            primaryKeyVar = mMaker.invoke("toPrimaryKey", mKeyVar);
        }

        Variable primaryValueVar;
        if (primaryCursorVar == null) {
            primaryValueVar = mMaker.invoke
                ("join", mCursorVar, resultVar, primaryKeyVar);
        } else {
            primaryValueVar = mMaker.invoke
                ("join", mCursorVar, resultVar, primaryKeyVar, primaryCursorVar);
        }

        Label hasValue = mMaker.label();
        primaryValueVar.ifNe(null, hasValue);
        mMaker.return_(null);
        hasValue.here();

        return new Variable[] {primaryKeyVar, primaryValueVar};
    }

    /**
     * Finishes the method by returning a decoded row. After calling this method, this visitor
     * cannot be used again.
     *
     * @param decoder performs decoding of the key and value columns; pass null to rely on
     * default key decoding and use valueDecoder for the value columns
     * @param valueDecoder performs decoding of the value columns; pass null to invoke the
     * default value decoding method in the generated table class
     * @param tableClass current table implementation class
     * @param rowClass current row implementation
     * @param rowVar refers to the row parameter to allocate or fill in
     */
    void finishDecode(MethodHandle decoder, MethodHandle valueDecoder,
                      Class<?> tableClass, Class<?> rowClass, final Variable rowVar)
    {
        if (decoder != null && valueDecoder != null) {
            throw new IllegalArgumentException();
        }

        passFail(null);

        // Must call this in case applyFilter wasn't called, or it did nothing.
        initVars(true);

        final Label notRow = mMaker.label();
        final var typedRowVar = CodeUtils.castOrNew(rowVar, rowClass, notRow);

        if (decoder != null) {
            mMaker.invoke(decoder, typedRowVar, mKeyVar, mValueVar);
        } else {
            var tableVar = mMaker.var(tableClass); 
            tableVar.invoke("decodePrimaryKey", typedRowVar, mKeyVar);

            // Invoke the schema-specific decoder directly, instead of calling the decodeValue
            // method which redundantly examines the schema version and switches on it.
            if (valueDecoder != null) {
                mMaker.invoke(valueDecoder, typedRowVar, mValueVar);
            } else {
                tableVar.invoke("decodeValue", typedRowVar, mValueVar);
            }

            tableVar.invoke("markAllClean", typedRowVar);
        }

        mMaker.return_(typedRowVar);

        // Assume the passed in row is actually a RowConsumer.
        notRow.here();
        CodeUtils.acceptAsRowConsumerAndReturn(rowVar, rowClass, mKeyVar, mValueVar);
    }

    /**
     * Must be called after visiting when making a predicate. After calling this method, this
     * visitor cannot be used again.
     */
    void finishPredicate() {
        passFail(false);
        mMaker.return_(true);
    }

    private void passFail(Object failResult) {
        if (mFail != null) {
            mFail.here();
            mMaker.return_(failResult);
            mPass.here();
        }
    }

    @Override
    public void visit(OrFilter filter) {
        final Label originalFail = mFail;

        RowFilter[] subFilters = filter.subFilters();

        if (subFilters.length == 0) {
            mMaker.goto_(originalFail);
            return;
        }

        mFail = mMaker.label();
        subFilters[0].accept(this);
        mFail.here();

        // Only the state observed on the left tree path can be preserved, because it's
        // guaranteed to have executed.
        final int[] states = copyLocatedStates();

        for (int i=1; i<subFilters.length; i++) {
            mFail = mMaker.label();
            subFilters[i].accept(this);
            mFail.here();
        }

        resetLocatedStates(states);

        mMaker.goto_(originalFail);
        mFail = originalFail;
    }

    @Override
    public void visit(AndFilter filter) {
        final Label originalPass = mPass;

        RowFilter[] subFilters = filter.subFilters();

        if (subFilters.length == 0) {
            mMaker.goto_(originalPass);
            return;
        }

        mPass = mMaker.label();
        subFilters[0].accept(this);
        mPass.here();

        // Only the state observed on the left tree path can be preserved, because it's
        // guaranteed to have executed.
        final int[] states = copyLocatedStates();

        for (int i=1; i<subFilters.length; i++) {
            mPass = mMaker.label();
            subFilters[i].accept(this);
            mPass.here();
        }

        resetLocatedStates(states);

        mMaker.goto_(originalPass);
        mPass = originalPass;
    }

    @Override
    public void visit(ColumnToArgFilter filter) {
        final Label originalFail = mFail;

        ColumnInfo colInfo = filter.column();
        int op = filter.operator();
        int argNum = filter.argument();

        Integer colNum = columnNumberFor(colInfo.name);

        Label stop = null;
        if (argNum == mStopArgument && colInfo.name.equals(mStopColumn) && mCursorVar != null) {
            stop = mMaker.label();
            mFail = stop;
        }

        doCompare:
        if (colNum != null) {
            ColumnCodec codec = codecFor(colNum);
            LocatedColumn located = decodeColumn(colNum, colInfo, true);
            Object decoded = located.mDecodedQuick;
            if (decoded != null) {
                codec.filterQuickCompare(colInfo, located.mSrcVar, located.mOffsetVar, op,
                                         decoded, mPredicateVar, argNum, mPass, mFail);
                break doCompare;
            }
            var columnVar = located.mDecodedVar;
            var argField = mPredicateVar.field(ColumnCodec.argFieldName(colInfo, argNum));
            CompareUtils.compare(mMaker, colInfo, columnVar, colInfo, argField, op, mPass, mFail);
        } else {
            // Column doesn't exist in the row, so compare against a default. This code
            // assumes that value codecs always define an arg field which preserves the
            // original argument, possibly converted to the correct type.
            var argField = mPredicateVar.field(ColumnCodec.argFieldName(colInfo, argNum));
            var columnVar = mMaker.var(colInfo.type);
            Converter.setDefault(mMaker, colInfo, columnVar);
            CompareUtils.compare(mMaker, colInfo, columnVar, colInfo, argField, op, mPass, mFail);
        }

        if (stop != null) {
            stop.here();
            // Reset the cursor to stop scanning.
            mCursorVar.invoke("reset");
            mMaker.var(StoppedCursorException.class).field("THE").throw_();
            //mMaker.goto_(originalFail);
            mFail = originalFail;
        }
    }

    @Override
    public void visit(ColumnToColumnFilter filter) {
        ColumnInfo aColInfo = filter.column();
        int op = filter.operator();
        ColumnInfo bColInfo = filter.otherColumn();

        Integer aColNum = columnNumberFor(aColInfo.name);
        Integer bColNum = columnNumberFor(bColInfo.name);

        if (aColNum == null && bColNum == null) {
            // Comparing two columns that don't exist. If this filter is part of a chain,
            // the rest will be dead code.
            mMaker.goto_(CompareUtils.selectNullColumnToNullArg(op, mPass, mFail));
            return;
        }

        Variable aVar = decodeColumnOrDefault(aColNum, aColInfo);
        Variable bVar = decodeColumnOrDefault(bColNum, bColInfo);

        if (aVar.classType() != bVar.classType()) {
            ColumnInfo cColInfo = filter.common();

            var aConvertedVar = mMaker.var(cColInfo.type);
            Converter.convertLossy(mMaker, aColInfo, aVar, cColInfo, aConvertedVar);
            aColInfo = cColInfo;
            aVar = aConvertedVar;

            var bConvertedVar = mMaker.var(cColInfo.type);
            Converter.convertLossy(mMaker, bColInfo, bVar, cColInfo, bConvertedVar);
            bColInfo = cColInfo;
            bVar = bConvertedVar;
        }

        CompareUtils.compare(mMaker, aColInfo, aVar, bColInfo, bVar, op, mPass, mFail);
    }

    private Variable decodeColumnOrDefault(Integer colNum, ColumnInfo colInfo) {
        if (colNum != null) {
            return decodeColumn(colNum, colInfo, false).mDecodedVar;
        } else {
            var colVar = mMaker.var(colInfo.type);
            Converter.setDefault(mMaker, colInfo, colVar);
            return colVar;
        }
    }

    private Integer columnNumberFor(String colName) {
        return mRowGen.columnNumbers().get(colName);
    }

    private ColumnCodec codecFor(int colNum) {
        ColumnCodec[] codecs = mKeyCodecs;
        return colNum < codecs.length ? codecs[colNum] : mValueCodecs[colNum - codecs.length];
    }

    /**
     * Decodes a column and remembers it if requested again later.
     *
     * @param colInfo current definition for column
     * @param quick allow quick decode
     */
    private LocatedColumn decodeColumn(int colNum, ColumnInfo colInfo, boolean quick) {
        Variable srcVar;
        LocatedColumn[] located;
        ColumnCodec[] codecs = mKeyCodecs;
        int highestNum;

        init: {
            int startOffset;
            if (colNum < codecs.length) {
                // Key column.
                highestNum = mHighestLocatedKey;
                srcVar = mKeyVar;
                located = mLocatedKeys;
                if (highestNum >= 0) {
                    break init;
                }
                if (located == null) {
                    mLocatedKeys = located = new LocatedColumn[mRowGen.info.keyColumns.size()];
                }
                startOffset = 0;
                mHighestLocatedKey = 0;
            } else {
                // Value column.
                colNum -= codecs.length;
                highestNum = mHighestLocatedValue;
                srcVar = mValueVar;
                codecs = mValueCodecs;
                located = mLocatedValues;
                if (highestNum >= 0) {
                    break init;
                }
                if (located == null) {
                    mLocatedValues = located = new LocatedColumn[mRowGen.info.valueColumns.size()];
                }
                startOffset = mValueOffset;
                mHighestLocatedValue = 0;
            }
            located[0] = new LocatedColumn();
            located[0].located(srcVar, mMaker.var(int.class).set(startOffset));
            highestNum = 0;
        }

        if (colNum <= highestNum) {
            LocatedColumn col = located[colNum];
            if (col.isDecoded(quick)) {
                return col;
            }
            // Regress the highest to force the column to be decoded. The highest field
            // won't regress, since the field assignment (at the end) checks this.
            highestNum = colNum;
        }

        if (!located[highestNum].isLocated()) {
            throw new AssertionError();
        }

        for (; highestNum <= colNum; highestNum++) {
            // Offset will be mutated, and so a copy must be made before calling decode.
            Variable offsetVar = located[highestNum].mOffsetVar;

            LocatedColumn next;
            copyOffsetVar: {
                if (highestNum + 1 >= located.length) {
                    next = null;
                } else {
                    next = located[highestNum + 1];
                    if (next == null) {
                        next = new LocatedColumn();
                        located[highestNum + 1] = next;
                    } else if (!next.isLocated()) {
                        // Can recycle the offset variable because it's not used.
                        Variable freeVar = next.mOffsetVar;
                        if (freeVar != null) {
                            freeVar.set(offsetVar);
                            offsetVar = freeVar;
                            break copyOffsetVar;
                        }
                    }
                }
                offsetVar = offsetVar.get();
            }

            ColumnCodec codec = codecs[highestNum];
            Variable endVar = null;
            if (highestNum < colNum) {
                codec.decodeSkip(srcVar, offsetVar, endVar);
            } else if (quick && codec.canFilterQuick(colInfo)) {
                Object decoded = codec.filterQuickDecode(colInfo, srcVar, offsetVar, endVar);
                located[highestNum].decodedQuick(decoded);
            } else {
                Variable dstVar = mMaker.var(colInfo.type);
                Converter.decodeLossy(mMaker, srcVar, offsetVar, endVar, codec, colInfo, dstVar);
                located[highestNum].decodedVar(dstVar);
            }

            if (next != null && !next.isLocated()) {
                // The decode call incremented offsetVar as a side-effect. Note that if the
                // column is already located, then the newly discovered offset will match. It
                // could simply be replaced, but by discarding it, the compiler can discard
                // some of the redundant steps which had recomputed the offset.
                next.located(srcVar, offsetVar);
            }
        }

        highestNum = Math.min(highestNum, located.length - 1);

        if (located == mLocatedKeys) {
            if (highestNum > mHighestLocatedKey) {
                mHighestLocatedKey = highestNum;
            }
        } else {
            if (highestNum > mHighestLocatedValue) {
                mHighestLocatedValue = highestNum;
            }
        }

        return located[colNum];
    }

    private int[] copyLocatedStates() {
        int numKeys = mKeyCodecs.length;
        int numValues = mValueCodecs.length;
        var states = new int[numKeys + numValues];
        
        if (mLocatedKeys != null) {
            for (int i=0; i<numKeys; i++) {
                LocatedColumn col = mLocatedKeys[i];
                if (col == null) {
                    break;
                }
                states[i] = col.state();
            }
        }

        if (mLocatedValues != null) {
            for (int i=0; i<numValues; i++) {
                LocatedColumn col = mLocatedValues[i];
                if (col == null) {
                    break;
                }
                states[numKeys + i] = col.state();
            }
        }

        return states;
    }

    private void resetLocatedStates(int[] states) {
        int numKeys = mKeyCodecs.length;

        if (mLocatedKeys != null) {
            int i = 0;
            for (; i<numKeys; i++) {
                LocatedColumn col = mLocatedKeys[i];
                if (col == null || col.resetState(states[i])) {
                    break;
                }
            }
            mHighestLocatedKey = i - 1;
            for (; i<numKeys; i++) {
                LocatedColumn col = mLocatedKeys[i];
                if (col == null) {
                    break;
                }
                col.unlocated();
            }
        }

        if (mLocatedValues != null) {
            int numValues = mValueCodecs.length;
            int i = 0;
            for (; i<numValues; i++) {
                LocatedColumn col = mLocatedValues[i];
                if (col == null || col.resetState(states[numKeys + i])) {
                    break;
                }
            }
            mHighestLocatedValue = i - 1;
            for (; i<numValues; i++) {
                LocatedColumn col = mLocatedValues[i];
                if (col == null) {
                    break;
                }
                col.unlocated();
            }
        }
    }

    private static final class LocatedColumn {
        // Used by mState field.
        private static final int UNLOCATED = 0, LOCATED = 1, DECODED = 2;

        private int mState;

        // Source byte array. Is valid when mState is LOCATED or DECODED.
        Variable mSrcVar;

        // Offset into the byte array. Is valid when mState is LOCATED or DECODED.
        Variable mOffsetVar;

        // Is only valid when mState is DECODED and canFilterQuick returned true.
        Object mDecodedQuick;

        // Is only valid when mState is DECODED.
        Variable mDecodedVar;

        LocatedColumn() {
        }

        int state() {
            return mState;
        }

        /**
         * @return true if state is unlocated
         */
        boolean resetState(int state) {
            if (state <= LOCATED) {
                mDecodedQuick = null;
                mDecodedVar = null;
                mState = state;
            }
            return state == UNLOCATED;
        }

        void unlocated() {
            mDecodedQuick = null;
            mDecodedVar = null;
            mState = UNLOCATED;
        }

        boolean isLocated() {
            return mState >= LOCATED;
        }

        /**
         * @param quick when true, accepts quick or fully decoded forms; when false, only
         * accepts the fully decoded form
         */
        boolean isDecoded(boolean quick) {
            return mState == DECODED && (quick || mDecodedVar != null);
        }

        /**
         * @param srcVar source byte array
         * @param offsetVar start offset into the byte array
         */
        void located(Variable srcVar, Variable offsetVar) {
            mSrcVar = srcVar;
            mOffsetVar = offsetVar;
            mState = LOCATED;
        }

        /**
         * @param decoded object returned from ColumnCodec.filterQuickDecode
         */
        void decodedQuick(Object decoded) {
            if (mState == UNLOCATED) {
                throw new IllegalStateException();
            }
            mDecodedQuick = decoded;
            mState = DECODED;
        }

        void decodedVar(Variable decodedVar) {
            if (mState == UNLOCATED) {
                throw new IllegalStateException();
            }
            mDecodedVar = decodedVar;
            mState = DECODED;
        }
    }
}
