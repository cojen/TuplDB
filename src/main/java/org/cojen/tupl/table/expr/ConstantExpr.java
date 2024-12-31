/*
 *  Copyright (C) 2024 Cojen.org
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

package org.cojen.tupl.table.expr;

import java.lang.invoke.CallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import java.util.function.Consumer;

import org.cojen.tupl.table.ConvertCallSite;
import org.cojen.tupl.table.RowInfo;
import org.cojen.tupl.table.RowUtils;
import org.cojen.tupl.table.SoftCache;

import org.cojen.tupl.table.filter.FalseFilter;
import org.cojen.tupl.table.filter.RowFilter;
import org.cojen.tupl.table.filter.TrueFilter;

import org.cojen.tupl.util.Canonicalizer;

import org.cojen.maker.Label;
import org.cojen.maker.Variable;

import static org.cojen.tupl.table.ColumnInfo.*;

/**
 * Defines a simple expression which just represents a constant value.
 *
 * @author Brian S. O'Neill
 */
public final class ConstantExpr extends Expr {
    public static ConstantExpr makeNull(int startPos, int endPos) {
        return new ConstantExpr(startPos, endPos, NullType.THE, null);
    }

    public static ConstantExpr make(int startPos, int endPos, String value) {
        return new ConstantExpr(startPos, endPos, String.class, TYPE_UTF8, value);
    }

    public static ConstantExpr make(int startPos, int endPos, BigInteger value) {
        value = canonicalize(value);
        return new ConstantExpr(startPos, endPos, BigInteger.class, TYPE_BIG_INTEGER, value);
    }

    public static ConstantExpr make(int startPos, int endPos, BigDecimal value) {
        value = canonicalize(value);
        return new ConstantExpr(startPos, endPos, BigDecimal.class, TYPE_BIG_DECIMAL, value);
    }

    public static ConstantExpr make(int startPos, int endPos, boolean value) {
        return new ConstantExpr(startPos, endPos, boolean.class, TYPE_BOOLEAN, value);
    }

    public static ConstantExpr make(int startPos, int endPos, int value) {
        return new ConstantExpr(startPos, endPos, int.class, TYPE_INT, value);
    }

    public static ConstantExpr make(int startPos, int endPos, long value) {
        return new ConstantExpr(startPos, endPos, long.class, TYPE_LONG, value);
    }

    public static ConstantExpr make(int startPos, int endPos, float value) {
        return new ConstantExpr(startPos, endPos, float.class, TYPE_FLOAT, value);
    }

    public static ConstantExpr make(int startPos, int endPos, double value) {
        return new ConstantExpr(startPos, endPos, double.class, TYPE_DOUBLE, value);
    }

    public static ConstantExpr make(int startPos, int endPos, Type type, Object value) {
        value = canonicalize(value);
        return new ConstantExpr(startPos, endPos, type, value);
    }

    private static final Canonicalizer cCanonicalizer = new Canonicalizer();

    private static <V> V canonicalize(V value) {
        if (value != null && mustSetExact(value.getClass(), value)) {
            value = cCanonicalizer.apply(value);
        }
        return value;
    }

    private static final SoftCache<Class, MethodHandle, Object> cConverterCache;

    static {
        cConverterCache = new SoftCache<>() {
            @Override
            protected MethodHandle newValue(Class toType, Object unused) {
                MethodType mt = MethodType.methodType(toType, Object.class);
                CallSite cs = ConvertCallSite.makeNext(MethodHandles.lookup(), "_", mt);
                return cs.dynamicInvoker();
            }
        };
    }

    private final Type mType;
    private final Object mValue;

    private ConstantExpr(int startPos, int endPos, Class clazz, int typeCode, Object value) {
        this(startPos, endPos, BasicType.make(clazz, typeCode), value);
    }

    private ConstantExpr(int startPos, int endPos, Type type, Object value) {
        super(startPos, endPos);
        mType = type;
        mValue = value;
    }

    @Override
    public Type type() {
        return mType;
    }

    @Override
    public ConstantExpr asType(Type type) {
        if (mType.equals(type)) {
            return this;
        }

        if (mValue == null) {
            return new ConstantExpr(startPos(), endPos(), type.nullable(), null);
        }

        try {
            Object value = cConverterCache.obtain(type.clazz(), null).invoke(mValue);
            value = canonicalize(value);
            return new ConstantExpr(startPos(), endPos(), type, value);
        } catch (IllegalArgumentException | ArithmeticException e) {
            throw queryException("Cannot convert " + mType + " to " + type);
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    @Override
    public Expr negate(int startPos, boolean widen) {
        if (mValue == null) {
            return withStartPos(startPos);
        }

        if (mValue instanceof Number num && !mType.isUnsigned()) isNum: {
            Object newValue;

            switch (num) {
                case Integer n:
                    if (n == Integer.MIN_VALUE) {
                        if (!widen) {
                            break isNum;
                        }
                        newValue = -((long) n);
                    } else {
                        newValue = -n;
                    }
                    break;
                case Double n:
                    newValue = Double.valueOf(-n);
                    break;
                case Long n:
                    if (n == Long.MIN_VALUE) {
                        if (!widen) {
                            break isNum;
                        }
                        newValue = BigInteger.valueOf(n).negate();
                    } else {
                        newValue = Long.valueOf(-n);
                    }
                    break;
                case BigDecimal n:
                    newValue = canonicalize(n.negate());
                    break;
                case BigInteger n:
                    newValue = canonicalize(n.negate());
                    break;
                case Float n:
                    newValue = Float.valueOf(-n);
                    break;
                default:
                    break isNum;
            }

            return new ConstantExpr(startPos, endPos(), mType, newValue);
        }

        return super.negate(startPos, widen);
    }

    @Override
    public Expr not(int startPos) {
        if (mValue == null) {
            return withStartPos(startPos);
        } else if (mValue instanceof Boolean bool) {
            return make(startPos, endPos(), !bool);
        } else {
            return super.not(startPos);
        }
    }

    @Override
    public boolean supportsLogicalNot() {
        return mValue == null || mValue instanceof Boolean;
    }

    private ConstantExpr withStartPos(int startPos) {
        return startPos == startPos() ? this : new ConstantExpr(startPos, endPos(), mType, mValue); 
    }

    @Override
    public int maxArgument() {
        return 0;
    }

    @Override
    public boolean isPureFunction() {
        return true;
    }

    @Override
    public boolean isTrivial() {
        return true;
    }

    @Override
    public RowFilter toRowFilter(RowInfo info, Map<String, ColumnExpr> columns) {
        if (mValue == Boolean.TRUE) {
            return TrueFilter.THE;
        } else if (mValue == Boolean.FALSE) {
            return FalseFilter.THE;
        }
        return super.toRowFilter(info, columns);
    }

    @Override
    public boolean isNullable() {
        return mValue == null;
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    @Override
    public boolean isNull() {
        return mValue == null;
    }

    @Override
    public boolean isZero() {
        Object value = mValue;

        if (value != null) {
            return switch (mType.typeCode()) {
                case TYPE_BIG_INTEGER -> BigInteger.ZERO.equals(value);
                case TYPE_BIG_DECIMAL -> BigDecimal.ZERO.equals(value);
                case TYPE_INT -> 0 == (Integer) value;
                case TYPE_LONG -> 0L == (Long) value;
                case TYPE_FLOAT -> 0.0f == (Float) value;
                case TYPE_DOUBLE -> 0.0d == (Double) value;
                default -> false;
            };
        }

        return false;
    }

    @Override
    public boolean isOne() {
        Object value = mValue;

        if (value != null) {
            return switch (mType.typeCode()) {
                case TYPE_BIG_INTEGER -> BigInteger.ONE.equals(value);
                case TYPE_BIG_DECIMAL -> BigDecimal.ONE.equals(value);
                case TYPE_INT -> 1 == (Integer) value;
                case TYPE_LONG -> 1L == (Long) value;
                case TYPE_FLOAT -> 1.0f == (Float) value;
                case TYPE_DOUBLE -> 1.0d == (Double) value;
                default -> false;
            };
        }

        return false;
    }

    @Override
    public boolean isOrderDependent() {
        return false;
    }

    @Override
    public boolean isGrouping() {
        return false;
    }

    @Override
    public boolean isAccumulating() {
        return false;
    }

    @Override
    public boolean isAggregating() {
        return false;
    }

    @Override
    public ConstantExpr asAggregate(Set<String> group) {
        return this;
    }

    @Override
    public ConstantExpr asWindow(Map<ColumnExpr, AssignExpr> newAssignments) {
        return this;
    }

    @Override
    public boolean isRangeWithCurrent() {
        return mValue instanceof Range r && r.includesZero();
    }

    @Override
    public LazyValue lazyValue(EvalContext context) {
        return new LazyValue(context, this) {
            @Override
            public boolean isConstant() {
                return true;
            }

            @Override
            public Object constantValue() {
                return ConstantExpr.this.value();
            }
        };
    }

    @Override
    public void gatherEvalColumns(Consumer<Column> c) {
    }

    @Override
    protected Variable doMakeEval(EvalContext context, EvalContext.ResultRef resultRef) {
        var valueVar = context.methodMaker().var(mType.clazz());
        setAny(valueVar, mValue);
        return valueVar;
    }

    @Override
    public void makeFilter(EvalContext context, Label pass, Label fail) {
        if (mValue instanceof Boolean b) {
            context.methodMaker().goto_(b ? pass : fail);
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public boolean canThrowRuntimeException() {
        return false;
    }

    public Object value() {
        return mValue;
    }

    private static final byte K_TYPE = KeyEncoder.allocType();

    @Override
    protected void encodeKey(KeyEncoder enc) {
        if (!enc.encode(this, K_TYPE)) {
            return;
        }

        int typeCode = mType.typeCode();
        enc.encodeInt(typeCode);

        Object value = mValue;

        if (value == null) {
            if (Type.isNullable(typeCode)) {
                enc.encodeBoolean(false);
            } else {
                throw new AssertionError();
            }
        } else if (Type.isNullable(typeCode)) {
            enc.encodeBoolean(true);
        }

        switch (typeCode) {
        case TYPE_BOOLEAN -> enc.encodeBoolean((Boolean) value);
        case TYPE_BYTE    -> enc.encodeByte((Byte) value);
        case TYPE_SHORT   -> enc.encodeShort((Short) value);
        case TYPE_CHAR    -> enc.encodeShort((Character) value);
        case TYPE_INT     -> enc.encodeInt((Integer) value);
        case TYPE_LONG    -> enc.encodeLong((Long) value);
        case TYPE_FLOAT   -> enc.encodeInt(Float.floatToIntBits((Float) value));
        case TYPE_DOUBLE  -> enc.encodeLong(Double.doubleToLongBits((Double) value));
        case TYPE_UTF8    -> enc.encodeString((String) value);
        default           -> enc.encodeReference(value);
        }
    }

    @Override
    public int hashCode() {
        return mType.hashCode() * 31 + Objects.hashCode(mValue);
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this ||
            obj instanceof ConstantExpr ce
            && mType.equals(ce.mType) && Objects.equals(mValue, ce.mValue);
    }

    @Override
    public String toString() {
        if (mValue instanceof String s) {
            var b = new StringBuilder();
            RowUtils.appendQuotedString(b, s);
            return b.toString();
        } else {
            return String.valueOf(mValue);
        }
    }

    @Override
    public void appendTo(StringBuilder b) {
        if (mValue instanceof String s) {
            RowUtils.appendQuotedString(b, s);
        } else {
            b.append(mValue);
        }
    }
}
