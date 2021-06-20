/*
 *  Copyright 2021 Cojen.org
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
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.rows;

import java.math.BigInteger;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.cojen.maker.Field;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.filter.ColumnFilter;

import static org.cojen.tupl.rows.ColumnInfo.*;
import static org.cojen.tupl.rows.RowUtils.*;

/**
 * Makes code for encoding and decoding columns.
 *
 * @author Brian S O'Neill
 */
abstract class ColumnCodec {
    /**
     * Returns an array of new stateless ColumnCodec instances.
     *
     * @param forKey true to use key encoding (lexicographical order)
     */
    static ColumnCodec[] make(Map<String, ColumnInfo> infoMap, boolean forKey) {
        return make(infoMap.values(), forKey);
    }

    /**
     * Returns an array of new stateless ColumnCodec instances.
     *
     * @param forKey true to use key encoding (lexicographical order)
     */
    static ColumnCodec[] make(Collection<ColumnInfo> infos, boolean forKey) {
        var codecs = new ColumnCodec[infos.size()];

        if (codecs.length != 0) {
            int slot = 0;
            Iterator<ColumnInfo> it = infos.iterator();
            ColumnInfo info = it.next();
            while (true) {
                boolean hasNext = it.hasNext();
                codecs[slot++] = make(info, forKey, !hasNext);
                if (!hasNext) {
                    break;
                }
                info = it.next();
            }
        }

        return codecs;
    }

    /**
     * Returns a new stateless ColumnCodec instance.
     *
     * @param forKey true to use key encoding (lexicographical order)
     * @param isLast true if this is the last column in a group
     */
    static ColumnCodec make(ColumnInfo info, boolean forKey, boolean isLast) {
        int typeCode = info.typeCode;

        if (isArray(typeCode)) {
            // FIXME: Remember to consider last slot. Remember to optimize byte[]. If key is
            // just one byte array, return a clone. Note that the optimization isn't possible
            // for nullable primitive arrays. For signed arrays, the highest bit needs to be
            // flipped.
            throw null;
        }

        typeCode = plainTypeCode(typeCode);

        if (typeCode <= 0b1111) {
            int bitSize = 1 << (typeCode & 0b111);
            return new PrimitiveColumnCodec(info, null, forKey, (bitSize + 7) >>> 3);
        }

        switch (typeCode) {
        case TYPE_FLOAT:
            return new PrimitiveColumnCodec(info, null, forKey, 4);
        case TYPE_DOUBLE:
            return new PrimitiveColumnCodec(info, null, forKey, 8);

        case TYPE_CHAR:
            return new PrimitiveColumnCodec(info, null, forKey, 2);

        case TYPE_UTF8:
            if (isLast && !info.isDescending()) {
                // Note that with descending order, key format is still required. Otherwise,
                // two strings which share a common prefix would be ordered wrong, even when
                // all the bits are flipped.
                if (info.isNullable()) {
                    return new NullableLastStringColumnCodec(info, null);
                } else {
                    return new NonNullLastStringColumnCodec(info, null);
                }
            } else if (forKey) {
                return new KeyStringColumnCodec(info, null);
            } else if (info.isNullable()) {
                return new NullableStringColumnCodec(info, null);
            } else {
                return new NonNullStringColumnCodec(info, null);
            }

        case TYPE_BIG_INTEGER:
            if (forKey) {
                // Must always use key format, even when the last because of variable length
                // encoding, and numerical ordering rules are different than for strings.
                return new KeyBigIntegerColumnCodec(info, null);
            } else if (isLast) {
                if (info.isNullable()) {
                    return new NullableLastBigIntegerColumnCodec(info, null);
                } else {
                    return new NonNullLastBigIntegerColumnCodec(info, null);
                }
            } else if (info.isNullable()) {
                return new NullableBigIntegerColumnCodec(info, null);
            } else {
                return new NonNullBigIntegerColumnCodec(info, null);
            }

        case TYPE_BIG_DECIMAL:
            if (forKey) {
                return new KeyBigDecimalColumnCodec(info, null);
            } else {
                ColumnInfo unscaledInfo = info.copy();
                unscaledInfo.type = BigInteger.class;
                unscaledInfo.typeCode = TYPE_BIG_INTEGER;
                ColumnCodec unscaledCodec = make(unscaledInfo, false, isLast);
                return new BigDecimalColumnCodec(info, unscaledCodec, null);
            }

        default:
            throw new AssertionError();
        }
    }

    final ColumnInfo mInfo;
    final MethodMaker mMaker;

    /**
     * @param info non-null (except for SchemaVersionColumnCodec)
     * @param mm is null for stateless instance
     */
    ColumnCodec(ColumnInfo info, MethodMaker mm) {
        mInfo = info;
        mMaker = mm;
    }

    /**
     * Return stateful instances suitable for making code.
     */
    static ColumnCodec[] bind(ColumnCodec[] codecs, MethodMaker mm) {
        if (codecs.length != 0) {
            codecs = codecs.clone();
            for (int i=0; i<codecs.length; i++) {
                codecs[i] = codecs[i].bind(mm);
            }
        }
        return codecs;
    }

    /**
     * Return stateful instances suitable for making code. Unless the given array is empty, the
     * first element in the returned array is a SchemaVersionColumnCodec, which can only be
     * used for encoding.
     */
    static ColumnCodec[] bind(int schemaVersion, ColumnCodec[] codecs, MethodMaker mm) {
        if (codecs.length == 0) {
            return codecs;
        }
        var copy = new ColumnCodec[1 + codecs.length];
        copy[0] = new SchemaVersionColumnCodec(schemaVersion, mm);
        for (int i=0; i<codecs.length; i++) {
            copy[1 + i] = codecs[i].bind(mm);
        }
        return copy;
    }

    /**
     * Returns a stateful instance suitable for making code.
     */
    abstract ColumnCodec bind(MethodMaker mm);

    /**
     * Returns the minimum number of bytes to encode the column.
     */
    abstract int minSize();

    /**
     * Returns true if decoding always reaches the end.
     */
    abstract boolean isLast();

    /**
     * Makes code which declares all necessary variables used for encoding. Must be called
     * before calling any other encode methods.
     */
    abstract void encodePrepare();

    /**
     * Called when the column won't be encoded, but any variables that would be used by the
     * encode method need to be defined and initialized to anything. This is necessary to keep
     * the class code verifier happy. Must be called before calling encode.
     */
    abstract void encodeSkip();

    /**
     * Makes code which adds to the total size variable, excluding the minSize. Must be called
     * before calling encode.
     *
     * @param srcVar variable or field of the correct type
     * @param totalVar int type, which can be null initially
     * @return new or existing total variable (can still be null)
     */
    abstract Variable encodeSize(Variable srcVar, Variable totalVar);

    /**
     * Makes code which encodes the column.
     *
     * @param srcVar variable or field of the correct type
     * @param dstVar destination byte array
     * @param offsetVar int type; is incremented as a side-effect
     */
    abstract void encode(Variable srcVar, Variable dstVar, Variable offsetVar);

    /**
     * Makes code which decodes the column.
     *
     * @param dstVar variable or field of the correct type
     * @param srcVar source byte array
     * @param offsetVar int type; is incremented as a side-effect
     * @param endVar end offset, which when null implies the end of the array
     */
    abstract void decode(Variable dstVar, Variable srcVar, Variable offsetVar, Variable endVar);

    /**
     * Makes code which skips the column instead of decoding it.
     *
     * @param srcVar source byte array
     * @param offsetVar int type; is incremented as a side-effect
     * @param endVar end offset, which when null implies the end of the array
     */
    abstract void decodeSkip(Variable srcVar, Variable offsetVar, Variable endVar);

    /**
     * Makes code which defines and initializes final field(s) for filter arguments. An
     * exception is thrown at runtime if the argVar cannot be cast to the actual column type.
     *
     * This method is only called when the codec has been bound to a constructor, and it's
     * called on the destination codec (the currently defined row version). The implementation
     * should only consider the column type and not the specific encoding format.
     *
     * @param argVar argument value to compare against; variable type is Object
     * @param argNum zero-based filter argument number
     * @param op defined in ColumnFilter
     */
    abstract void filterPrepare(int op, Variable argVar, int argNum);

    /**
     * Makes code which identifies the location of an encoded column and optionally decodes it.
     * When a non-null object is returned, it's treated as cached state that can be re-used for
     * multiple invocations of filterCompare.
     *
     * This method is only called when the codec has been bound to a decode method, and it's
     * called on the source codec (it might be different than the current row version). If a
     * conversion must be applied, any special variables must be known by the returned object.
     *
     * @param dstInfo current definition for column
     * @param srcVar source byte array
     * @param offsetVar int type; is incremented as a side-effect
     * @param endVar end offset, which when null implies the end of the array
     * @param op defined in ColumnFilter
     * @return an object with decoded state
     */
    abstract Object filterDecode(ColumnInfo dstInfo,
                                 Variable srcVar, Variable offsetVar, Variable endVar, int op);

    /**
     * Makes code which compares a column.
     *
     * This method is only called when the codec has been bound to a decode method, and it's
     * called on the source codec.
     *
     * @param dstInfo current definition for column
     * @param srcVar source byte array
     * @param offsetVar int type; must not be modified
     * @param endVar end offset, which when null implies the end of the array
     * @param op defined in ColumnFilter
     * @param decoded object which was returned by the filterDecode method
     * @param argObjVar object which contains fields prepared earlier
     * @param argNum zero-based filter argument number
     * @param pass branch here when comparison passes
     * @param fail branch here when comparison fails
     */
    abstract void filterCompare(ColumnInfo dstInfo,
                                Variable srcVar, Variable offsetVar, Variable endVar,
                                int op, Object decoded, Variable argObjVar, int argNum,
                                Label pass, Label fail);

    // FIXME: When filter passes, take advantage of existing decoded variables and avoid double
    // decode if possible.

    protected String argFieldName(int argNum) {
        return mInfo.name + '$' + argNum;
    }

    protected String argFieldName(int argNum, String suffix) {
        return mInfo.name + '$' + argNum + '$' + suffix;
    }

    protected Field defineArgField(Object type, String name) {
        mMaker.classMaker().addField(type, name).private_().final_();
        return mMaker.field(name);
    }

    /**
     * @return new or existing accumulator variable
     */
    protected Variable accum(Variable accumVar, Object amount) {
        if (accumVar == null) {
            accumVar = mMaker.var(int.class).set(amount);
        } else {
            accumVar.inc(amount);
        }
        return accumVar;
    }

    /**
     * Encodes a null header byte and jumps to the end if the runtime column value is null.
     *
     * @param offsetVar int type; is incremented as a side-effect
     * @param end required
     */
    protected void encodeNullHeader(Label end, Variable srcVar,
                                    Variable dstVar, Variable offsetVar)
    {
        Label notNull = mMaker.label();
        srcVar.ifNe(null, notNull);

        byte nullHeader = NULL_BYTE_HIGH;
        byte notNullHeader = NOT_NULL_BYTE_HIGH;

        if (mInfo.isDescending()) {
            nullHeader = (byte) ~nullHeader;
            notNullHeader = (byte) ~notNullHeader;
        }

        dstVar.aset(offsetVar, nullHeader);
        offsetVar.inc(1);
        mMaker.goto_(end);

        notNull.here();
        dstVar.aset(offsetVar, notNullHeader);
        offsetVar.inc(1);
    }

    /**
     * Decode a null header byte and jumps to the end if the decoded column value is null.
     *
     * @param end required except when dst is boolean
     * @param dst optional; if boolean, assigns true/false as null/not-null
     * @param offsetVar int type; is incremented as a side-effect
     */
    protected void decodeNullHeader(Label end, Variable dstVar,
                                    Variable srcVar, Variable offsetVar)
    {
        var header = srcVar.aget(offsetVar);
        offsetVar.inc(1);

        byte nullHeader = NULL_BYTE_HIGH;
        if (mInfo.isDescending()) {
            nullHeader = (byte) ~nullHeader;
        }

        if (dstVar == null) {
            header.ifEq(nullHeader, end);
        } else if (dstVar.classType() == boolean.class) {
            dstVar.set(header.eq(nullHeader));
        } else {
            Label notNull = mMaker.label();        
            header.ifNe(nullHeader, notNull);
            dstVar.set(null);
            mMaker.goto_(end);
            notNull.here();
        }
    }

    /**
     * Makes code which compares a column and filter argument when one or both of them might be
     * null. If either is null, then code flows to the pass or fail target. Otherwise, the flow
     * continues on.
     *
     * @param srcVar source byte array or boolean (true means the column null)
     * @param offsetVar int type; is incremented as a side-effect (ignored if srcVar is boolean)
     * @param argVar boolean or Object (when boolean, true means the arg is null)
     * @param op defined in ColumnFilter
     * @param pass branch here when comparison passes
     * @param fail branch here when comparison fails
     */
    protected void compareNullHeader(Variable srcVar, Variable offsetVar,
                                     Variable argVar, int op, Label pass, Label fail)
    {
        Label isColumnNull = mMaker.label();
        if (srcVar.classType() == boolean.class) {
            srcVar.ifTrue(isColumnNull);
        } else {
            decodeNullHeader(isColumnNull, null, srcVar, offsetVar);
        }

        // Column isn't null...
        if (argVar.classType() == boolean.class) {
            argVar.ifTrue(CompareUtils.selectColumnToNullArg(op, pass, fail));
        } else {
            argVar.ifEq(null, CompareUtils.selectColumnToNullArg(op, pass, fail));
        }
        Label cont = mMaker.label();
        mMaker.goto_(cont);

        // Column is null...
        isColumnNull.here();
        if (argVar.classType() == boolean.class) {
            argVar.ifTrue(CompareUtils.selectNullColumnToNullArg(op, pass, fail));
        } else {
            argVar.ifEq(null, CompareUtils.selectNullColumnToNullArg(op, pass, fail));
        }
        mMaker.goto_(CompareUtils.selectNullColumnToArg(op, pass, fail));

        // Neither is null...
        cont.here();
    }

    /**
     * Common code used by BigIntegerColumnCodec and StringColumnCodec for comparing non-key
     * values and their binary encoded format.
     *
     * @param decodedVars dataOffsetVar, lengthVar, and optional isNullVar
     */
    protected void compareEncoded(ColumnInfo dstInfo, Variable srcVar,
                                  int op, Variable[] decodedVars, Variable argObjVar, int argNum,
                                  Label pass, Label fail)
    {
        Variable dataOffsetVar = decodedVars[0];
        Variable lengthVar = decodedVars[1];
        Variable isNullVar = decodedVars[2];

        if (!dstInfo.isNullable() && mInfo.isNullable()) {
            Label cont = mMaker.label();
            isNullVar.ifFalse(cont);
            var columnVar = mMaker.var(dstInfo.type);
            Converter.setDefault(dstInfo, columnVar);
            var argField = argObjVar.field(argFieldName(argNum));
            CompareUtils.compare(mMaker, dstInfo, columnVar, dstInfo, argField, op, pass, fail);
            cont.here();
        }

        var argVar = argObjVar.field(argFieldName(argNum, "bytes")).get();

        Label notNull = mMaker.label();
        argVar.ifNe(null, notNull);
        // Argument is null...
        if (isNullVar != null) {
            isNullVar.ifTrue(CompareUtils.selectNullColumnToNullArg(op, pass, fail));
        }
        mMaker.goto_(CompareUtils.selectColumnToNullArg(op, pass, fail));

        // Argument is isn't null...
        notNull.here();
        if (isNullVar != null) {
            isNullVar.ifTrue(CompareUtils.selectNullColumnToArg(op, pass, fail));
        }
        CompareUtils.compareArrays(mMaker,
                                   srcVar, dataOffsetVar, dataOffsetVar.add(lengthVar),
                                   argVar, 0, argVar.alength(),
                                   op, pass, fail);
    }
}
