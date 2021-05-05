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
        ColumnCodec[] codecs = new ColumnCodec[infos.size()];

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
            if (isLast) {
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
            if (isLast) {
                if (info.isNullable()) {
                    return new NullableLastBigIntegerColumnCodec(info, null);
                } else {
                    return new NonNullLastBigIntegerColumnCodec(info, null);
                }
            } else if (forKey) {
                return new KeyBigIntegerColumnCodec(info, null);
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
     * Return stateful instances suitable for making code. The first element in the returned
     * array is a SchemaVersionColumnCodec, which can only be used for encoding.
     */
    static ColumnCodec[] bind(int schemaVersion, ColumnCodec[] codecs, MethodMaker mm) {
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
     * Makes code which encodes the column. If no offset variable was provided, and none is
     * returned, this implies that the amount of bytes encoded is the minSize.
     *
     * @param srcVar variable or field of the correct type
     * @param dstVar destination byte array
     * @param offsetVar int type, which can be null initially
     * @param fixedOffset applicable only when offsetVar is null
     * @return new or existing offset variable (can still be null)
     */
    abstract Variable encode(Variable srcVar, Variable dstVar, Variable offsetVar, int fixedOffset);

    /**
     * Makes code which decodes the column. If no offset variable was provided, and none is
     * returned, this implies that the amount of bytes decoded is the minSize.
     *
     * @param dstVar variable or field of the correct type
     * @param srcVar source byte array
     * @param offsetVar int type, which can be null initially
     * @param fixedOffset applicable only when offsetVar is null
     * @param endVar end offset, which when null implies the end of the array
     * @return new or existing offset variable (can still be null)
     */
    abstract Variable decode(Variable dstVar, Variable srcVar, Variable offsetVar, int fixedOffset,
                             Variable endVar);

    /**
     * Makes code which skips the column instead of decoding it. If no offset variable was
     * provided, and none is returned, this implies that the amount of bytes decoded is the
     * minSize.
     *
     * @param srcVar source byte array
     * @param offsetVar int type, which can be null initially
     * @param fixedOffset applicable only when offsetVar is null
     * @param endVar end offset, which when null implies the end of the array
     * @return new or existing offset variable (can still be null)
     */
    abstract Variable decodeSkip(Variable srcVar, Variable offsetVar, int fixedOffset,
                                 Variable endVar);

    /**
     * Defines and initializes final field(s) for filter arguments. An exception is thrown at
     * runtime if the argVar cannot be cast to the actual column type.
     *
     * Note: This method is only called when the codec has been bound to a constructor.
     *
     * @param argVar argument value to compare against; variable type is Object
     * @param argNum zero-based filter argument number
     * @param op defined in ColumnFilter
     * @return any object, which is passed into the compare code generation method
     */
    // FIXME: Note that for the compare method, the codec is bound to the scanner step method.
    abstract Object filterArgPrepare(Variable argVar, int argNum, int op);

    protected Field defineArgField(Object type, int argNum) {
        return defineArgField(type, mInfo.name + '$' + argNum);
    }

    protected Field defineArgField(Object type, int argNum, String suffix) {
        return defineArgField(type, mInfo.name + '$' + argNum + '$' + suffix);
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
     * @param end required
     * @return new or existing offset variable
     */
    protected Variable encodeNullHeader(Label end, Variable srcVar,
                                        Variable dstVar, Variable offsetVar, int fixedOffset)
    {
        if (offsetVar == null) {
            offsetVar = mMaker.var(int.class).set(fixedOffset);
        }

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

        return offsetVar;
    }

    /**
     * Decode a null header byte and jumps to the end if the decoded column value is null.
     *
     * @param end required
     * @param dst optional
     * @return new or existing offset variable
     */
    protected Variable decodeNullHeader(Label end, Variable dstVar,
                                        Variable srcVar, Variable offsetVar, int fixedOffset)
    {
        Variable header;
        if (offsetVar == null) {
            header = srcVar.aget(fixedOffset);
            offsetVar = mMaker.var(int.class).set(fixedOffset + 1);
        } else {
            header = srcVar.aget(offsetVar);
            offsetVar.inc(1);
        }

        byte nullHeader = NULL_BYTE_HIGH;
        if (mInfo.isDescending()) {
            nullHeader = (byte) ~nullHeader;
        }

        if (dstVar == null) {
            header.ifEq(nullHeader, end);
        } else {
            Label notNull = mMaker.label();        
            header.ifNe(nullHeader, notNull);
            dstVar.set(null);
            mMaker.goto_(end);
            notNull.here();
        }

        return offsetVar;
    }
}
