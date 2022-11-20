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
import java.util.Objects;

import org.cojen.maker.FieldMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.filter.ColumnFilter;

import static org.cojen.tupl.rows.ColumnInfo.*;

/**
 * Makes code for encoding and decoding columns.
 *
 * @author Brian S O'Neill
 */
abstract class ColumnCodec {
    static final int F_LAST = 1; // last column encoding
    static final int F_LEX = 2; // lexicographical order

    /**
     * Returns an array of new stateless ColumnCodec instances.
     *
     * @param flags 0 or else F_LEX to use lexicographical encoding
     */
    static ColumnCodec[] make(Map<String, ColumnInfo> infoMap, int flags) {
        return make(infoMap.values(), flags);
    }

    /**
     * Returns an array of new stateless ColumnCodec instances.
     *
     * @param flags 0 or else F_LEX to use lexicographical encoding
     */
    static ColumnCodec[] make(Collection<ColumnInfo> infos, int flags) {
        var codecs = new ColumnCodec[infos.size()];

        if (codecs.length != 0) {
            int slot = 0;
            Iterator<ColumnInfo> it = infos.iterator();
            ColumnInfo info = it.next();
            while (true) {
                boolean hasNext = it.hasNext();
                codecs[slot++] = make(info, hasNext ? flags : (flags | F_LAST));
                if (!hasNext) {
                    break;
                }
                info = it.next();
            }
        }

        return codecs;
    }

    /**
     * Returns an array of new stateless ColumnCodec instances, favoring non-lexicographical
     * encoding, but adopting the encoding from the given map if preferred.
     */
    static ColumnCodec[] make(Collection<ColumnInfo> infos, Map<String, ColumnCodec> pkCodecs) {
        var codecs = new ColumnCodec[infos.size()];

        if (codecs.length != 0) {
            int slot = 0;
            Iterator<ColumnInfo> it = infos.iterator();
            ColumnInfo info = it.next();
            while (true) {
                boolean hasNext = it.hasNext();
                ColumnCodec codec = make(info, hasNext ? 0 : F_LAST);
                ColumnCodec pkCodec = pkCodecs.get(info.name);
                if (pkCodec != null &&
                    (pkCodec.equals(codec) || !pkCodec.isLast() || codec.isLast()))
                {
                    codec = pkCodec;
                }
                codecs[slot++] = codec;
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
     */
    private static ColumnCodec make(ColumnInfo info, int flags) {
        int typeCode = info.typeCode;

        if (isArray(typeCode)) {
            if (!ColumnInfo.isPrimitive(plainTypeCode(typeCode))) {
                // Should have already been checked in RowInfo.examine.
                throw new AssertionError();
            }
        
            if ((flags & F_LAST) != 0 && !info.isDescending()) {
                // Note that with descending order, lexicographical encoding is still required.
                // Otherwise, two arrays which share a common prefix would be ordered wrong,
                // even when all the bits are flipped.
                if (info.isNullable()) {
                    return new NullableLastPrimitiveArrayColumnCodec(info, null, flags);
                } else {
                    return new NonNullLastPrimitiveArrayColumnCodec(info, null, flags);
                }
            } else if ((flags & F_LEX) != 0) {
                return new LexPrimitiveArrayColumnCodec(info, null);
            } else if (info.isNullable()) {
                return new NullablePrimitiveArrayColumnCodec(info, null);
            } else {
                return new NonNullPrimitiveArrayColumnCodec(info, null);
            }
        }

        typeCode = plainTypeCode(typeCode);

        if (typeCode <= 0b1111) {
            int bitSize = 1 << (typeCode & 0b111);
            return new PrimitiveColumnCodec(info, null, flags, (bitSize + 7) >>> 3);
        }

        switch (typeCode) {
        case TYPE_FLOAT:
            return new PrimitiveColumnCodec(info, null, flags, 4);
        case TYPE_DOUBLE:
            return new PrimitiveColumnCodec(info, null, flags, 8);

        case TYPE_CHAR:
            return new PrimitiveColumnCodec(info, null, flags, 2);

        case TYPE_UTF8:
            if ((flags & F_LAST) != 0 && !info.isDescending()) {
                // Note that with descending order, lexicographical encoding is still required.
                // Otherwise, two strings which share a common prefix would be ordered wrong,
                // even when all the bits are flipped.
                if (info.isNullable()) {
                    return new NullableLastStringColumnCodec(info, null);
                } else {
                    return new NonNullLastStringColumnCodec(info, null);
                }
            } else if ((flags & F_LEX) != 0) {
                return new LexStringColumnCodec(info, null);
            } else if (info.isNullable()) {
                return new NullableStringColumnCodec(info, null);
            } else {
                return new NonNullStringColumnCodec(info, null);
            }

        case TYPE_BIG_INTEGER:
            if ((flags & F_LEX) != 0) {
                // Must always use lexicographical encoding, even when the last because of
                // variable length encoding, and numerical ordering rules are different than
                // for strings.
                return new LexBigIntegerColumnCodec(info, null);
            } else if ((flags & F_LAST) != 0) {
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
            if ((flags & F_LEX) != 0) {
                return new LexBigDecimalColumnCodec(info, null);
            } else {
                ColumnInfo unscaledInfo = info.copy();
                unscaledInfo.type = BigInteger.class;
                unscaledInfo.typeCode = TYPE_BIG_INTEGER;
                ColumnCodec unscaledCodec = make(unscaledInfo, flags);
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

    @Override
    public final boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj != null && obj.getClass() == getClass()) {
            return doEquals(obj);
        }
        return false;
    }

    @Override
    public final int hashCode() {
        int hash = mInfo == null ? 0 : Objects.hashCode(mInfo.name);
        return (doHashCode() * 31 + hash) ^ getClass().hashCode();
    }

    /**
     * Should only consider the encoding strategy.
     *
     * @param obj is an instance of the class being called
     */
    protected abstract boolean doEquals(Object obj);

    protected final boolean equalOrdering(Object obj) {
        // TODO: optimize transform which just alters the null ordering or if bits can be flipped
        var other = (ColumnCodec) obj;
        return mInfo.isDescending() == other.mInfo.isDescending()
            && mInfo.isNullLow() == other.mInfo.isNullLow();
    }

    /**
     * Should only consider the encoding strategy.
     */
    protected abstract int doHashCode();

    /**
     * F_LAST, F_NULLS, F_LEX, etc.
     */
    abstract int codecFlags();

    /**
     * Returns true if decoding always reaches the end.
     */
    final boolean isLast() {
        return (codecFlags() & F_LAST) != 0;
    }

    /**
     * Returns true if column is lexicographically encoded.
     */
    final boolean isLex() {
        return (codecFlags() & F_LEX) != 0;
    }

    /**
     * Returns the minimum number of bytes to encode the column.
     */
    abstract int minSize();

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
     * Makes code which defines and initializes extra final field(s) for filter arguments.
     *
     * This method is only called when the codec has been bound to a constructor, and it's
     * called on the destination codec (the currently defined row version). The implementation
     * should only consider the column type and not the specific encoding format.
     *
     * @param in true if argument is a collection for "in" filtering
     * @param argVar argument value to compare against, converted to the the column type; if null,
     * extra fields aren't final and are lazily initialized
     */
    void filterDefineExtraFields(boolean in, Variable argVar, String argFieldName) {
    }

    /**
     * Returns true if the filterQuick methods can be used instead of performing a full decode.
     * If the quick form is the same as the full decode, despite still being "quick", false
     * should be returned. The quick form is only usable for column-to-argument filters, but
     * the full decode form can be used for column-to-column filters too.
     *
     * This method is only called when the codec has been bound to a decode method, and it's
     * called on the source codec (it might be different than the current row version).
     *
     * @param dstInfo current definition for column
     */
    boolean canFilterQuick(ColumnInfo dstInfo) {
        return false;
    }

    /**
     * Makes code which identifies the location of an encoded column and optionally decodes it.
     * The object is which is returned is treated as cached state that can be re-used for
     * multiple invocations of filterQuickCompare.
     *
     * This method is only called when the codec has been bound to a decode method, and it's
     * called on the source codec (it might be different than the current row version). If a
     * conversion must be applied, any special variables must be known by the returned object.
     *
     * @param dstInfo current definition for column
     * @param srcVar source byte array
     * @param offsetVar int type; is incremented as a side-effect
     * @param endVar end offset, which when null implies the end of the array
     * @return a non-null object with decoded state
     */
    Object filterQuickDecode(ColumnInfo dstInfo,
                             Variable srcVar, Variable offsetVar, Variable endVar)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Makes code which compares a decoded column to an argument.
     *
     * This method is only called when the codec has been bound to a decode method, and it's
     * called on the source codec.
     *
     * @param dstInfo current definition for column
     * @param srcVar source byte array
     * @param offsetVar int type; must not be modified
     * @param op defined in ColumnFilter
     * @param decoded object which was returned by the filterQuickDecode method
     * @param argObjVar object which contains fields prepared earlier
     * @param argNum zero-based filter argument number
     * @param pass branch here when comparison passes
     * @param fail branch here when comparison fails
     */
    void filterQuickCompare(ColumnInfo dstInfo,
                            Variable srcVar, Variable offsetVar,
                            int op, Object decoded, Variable argObjVar, int argNum,
                            Label pass, Label fail)
    {
        throw new UnsupportedOperationException();
    }

    static String argFieldName(String colName, int argNum) {
        return colName + '$' + argNum;
    }

    static String argFieldName(ColumnInfo info, int argNum) {
        return argFieldName(info.name, argNum);
    }

    final String argFieldName(int argNum) {
        return argFieldName(mInfo, argNum);
    }

    final String argFieldName(int argNum, String suffix) {
        return argFieldName(argFieldName(argNum), suffix);
    }

    static String argFieldName(String base, String suffix) {
        return base + '$' + suffix;
    }

    /**
     * Define a final arg field, initialized by the constructor.
     *
     * @param initVar initial and final value to assign
     */
    final void defineArgField(Object type, String name, Variable initVar) {
        defineArgField(type, name).final_();
        mMaker.field(name).set(initVar);
    }

    /**
     * Define a non-final arg field, to be lazily initialzied.
     */
    final FieldMaker defineArgField(Object type, String name) {
        return mMaker.classMaker().addField(type, name);
    }

    /**
     * @return new or existing accumulator variable
     */
    protected final Variable accum(Variable accumVar, Object amount) {
        if (accumVar == null) {
            accumVar = mMaker.var(int.class).set(amount);
        } else {
            accumVar.inc(amount);
        }
        return accumVar;
    }

    protected final byte nullByte() {
        return (mInfo.isDescending() == mInfo.isNullLow())
            ? RowUtils.NULL_BYTE_HIGH : RowUtils.NULL_BYTE_LOW;
    }

    protected final byte notNullByte() {
        return (mInfo.isDescending() == mInfo.isNullLow())
            ? RowUtils.NOT_NULL_BYTE_HIGH : RowUtils.NOT_NULL_BYTE_LOW;
    }

    /**
     * Conditionally encodes a null header byte and jumps to the end if the runtime column
     * value is null.
     *
     * @param end required
     * @param offsetVar int type; can be null to use an offset of zero or else is incremented
     * as a side-effect
     */
    protected final void encodeNullHeaderIfNull(Label end, Variable srcVar,
                                                Variable dstVar, Variable offsetVar)
    {
        Label notNull = mMaker.label();
        srcVar.ifNe(null, notNull);

        byte nb = nullByte();
        if (offsetVar == null) {
            dstVar.aset(0, nb);
        } else {
            dstVar.aset(offsetVar, nb);
            offsetVar.inc(1);
        }
        mMaker.goto_(end);

        notNull.here();
    }

    /**
     * Always encodes a null/not-null header byte and jumps to the end if the runtime column
     * value is null.
     *
     * @param end required
     * @param offsetVar int type; is incremented as a side-effect
     */
    protected final void encodeNullHeader(Label end, Variable srcVar,
                                          Variable dstVar, Variable offsetVar)
    {
        encodeNullHeaderIfNull(end, srcVar, dstVar, offsetVar);
        dstVar.aset(offsetVar, notNullByte());
        offsetVar.inc(1);
    }

    /**
     * Decode a null header byte and jumps to the end if the decoded column value is null.
     *
     * @param end required except when dst is boolean
     * @param dstVar optional; if boolean, assigns true/false as null/not-null
     * @param offsetVar int type; is incremented as a side-effect
     */
    protected final void decodeNullHeader(Label end, Variable dstVar,
                                          Variable srcVar, Variable offsetVar)
    {
        var header = srcVar.aget(offsetVar);
        offsetVar.inc(1);

        byte nullHeader = nullByte();

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
    protected final void compareNullHeader(Variable srcVar, Variable offsetVar,
                                           Variable argVar, int op, Label pass, Label fail)
    {
        Label isColumnNull = mMaker.label();
        if (srcVar.classType() == boolean.class) {
            srcVar.ifTrue(isColumnNull);
        } else {
            decodeNullHeader(isColumnNull, null, srcVar, offsetVar);
        }

        // Column isn't null...

        Label match = CompareUtils.selectColumnToNullArg(op, pass, fail);

        if (argVar.classType() == boolean.class) {
            argVar.ifTrue(match);
        } else {
            argVar.ifEq(null, match);
        }

        Label cont = mMaker.label().goto_();

        // Column is null...

        isColumnNull.here();
        match = CompareUtils.selectNullColumnToNullArg(op, pass, fail);

        if (ColumnFilter.isIn(op)) {
            if (argVar.classType() == boolean.class) {
                argVar.ifTrue(match);
            } else {
                argVar.ifEq(null, match);
            }
            CompareUtils.compareIn(mMaker, argVar, op, pass, fail, (a, p, f) -> a.ifEq(null, p));
        } else {
            Label mismatch = CompareUtils.selectNullColumnToArg(op, pass, fail);
            if (match != mismatch) {
                if (argVar.classType() == boolean.class) {
                    argVar.ifTrue(match);
                } else {
                    argVar.ifEq(null, match);
                }
            }
            mMaker.goto_(mismatch);
        }

        // Neither is null...
        cont.here();
    }
}
