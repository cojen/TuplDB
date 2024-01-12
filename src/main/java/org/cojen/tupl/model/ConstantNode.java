/*
 *  Copyright (C) 2023 Cojen.org
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

package org.cojen.tupl.model;

import java.lang.invoke.CallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.cojen.maker.Label;
import org.cojen.maker.Variable;

import org.cojen.tupl.rows.ConvertCallSite;
import org.cojen.tupl.rows.RowInfo;
import org.cojen.tupl.rows.RowUtils;
import org.cojen.tupl.rows.SoftCache;

import org.cojen.tupl.rows.filter.FalseFilter;
import org.cojen.tupl.rows.filter.RowFilter;
import org.cojen.tupl.rows.filter.TrueFilter;

import org.cojen.tupl.util.Canonicalizer;

import static org.cojen.tupl.rows.ColumnInfo.*;

/**
 * Defines a simple node which just represents a constant value.
 *
 * @author Brian S. O'Neill
 */
public sealed class ConstantNode extends Node {
    public static final ConstantNode NULL = new ConstantNode(AnyType.THE, null);

    public static ConstantNode make(Object value) {
        return new ConstantNode(Object.class, TYPE_REFERENCE, value);
    }

    public static ConstantNode make(String value) {
        return new ConstantNode(String.class, TYPE_UTF8, value);
    }

    public static ConstantNode make(BigInteger value) {
        return new ConstantNode(BigInteger.class, TYPE_BIG_INTEGER, value);
    }

    public static ConstantNode make(BigDecimal value) {
        return new ConstantNode(BigDecimal.class, TYPE_BIG_DECIMAL, value);
    }

    public static ConstantNode make(boolean value) {
        return new ConstantNode(boolean.class, TYPE_BOOLEAN, value);
    }

    public static ConstantNode make(byte value) {
        return new ConstantNode(byte.class, TYPE_BYTE, value);
    }

    public static ConstantNode make(short value) {
        return new ConstantNode(short.class, TYPE_SHORT, value);
    }

    public static ConstantNode make(char value) {
        return new ConstantNode(char.class, TYPE_CHAR, value);
    }

    public static ConstantNode make(int value) {
        return new ConstantNode(int.class, TYPE_INT, value);
    }

    public static ConstantNode make(long value) {
        return new ConstantNode(long.class, TYPE_LONG, value);
    }

    public static ConstantNode make(float value) {
        return new ConstantNode(float.class, TYPE_FLOAT, value);
    }

    public static ConstantNode make(double value) {
        return new ConstantNode(double.class, TYPE_DOUBLE, value);
    }

    private static final Canonicalizer cCanonicalizer = new Canonicalizer();

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

    private ConstantNode(Class clazz, int typeCode, Object value) {
        this(BasicType.make(clazz, typeCode), value);
    }

    private ConstantNode(Type type, Object value) {
        mType = type;
        mValue = value;
    }

    @Override
    public Type type() {
        return mType;
    }

    @Override
    public ConstantNode asType(Type type) {
        if (mType.equals(type)) {
            return this;
        }

        if (mValue == null) {
            return new ConstantNode(type, null);
        }

        // FIXME: Conversion to RelationType should always be supported (SelectNode).

        try {
            Object value = cConverterCache.obtain(type.clazz(), null).invoke(mValue);
            return new ConstantNode(type, value);
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    @Override
    public Node negate() {
        if (mValue == null) {
            return this;
        }

        if (mValue instanceof Number num) isNum: {
            Object newValue;

            if (num instanceof Integer n) {
                newValue = Integer.valueOf(-n);
            } else if (num instanceof Double n) {
                newValue = Double.valueOf(-n);
            } else if (num instanceof Long n) {
                newValue = Long.valueOf(-n);
            } else if (num instanceof BigDecimal n) {
                newValue = n.negate();
            } else if (num instanceof BigInteger n) {
                newValue = n.negate();
            } else if (num instanceof Float n) {
                newValue = Float.valueOf(-n);
            } else if (num instanceof Short n) {
                newValue = Short.valueOf((short) -n);
            } else if (num instanceof Byte n) {
                newValue = Byte.valueOf((byte) -n);
            } else {
                break isNum;
            }

            return new ConstantNode(mType, newValue);
        }

        return super.negate();
    }

    @Override
    public Node not() {
        if (mValue == null) {
            return this;
        } else if (mValue instanceof Boolean bool) {
            return make(!bool);
        } else {
            return super.not();
        }
    }

    @Override
    public String name() {
        StringBuilder b;

        if (mValue instanceof String s) {
            b = new StringBuilder();
            RowUtils.appendQuotedString(b, s);
        } else if (mValue instanceof Character c) {
            b = new StringBuilder();
            RowUtils.appendQuotedString(b, c);
        } else {
            return String.valueOf(mValue);
        }

        return b.toString();
    }

    @Override
    public ConstantNode withName(String name) {
        return new Named(mType, mValue, name);
    }

    private static final class Named extends ConstantNode {
        private final String mName;

        private Named(Type type, Object value, String name) {
            super(type, value);
            mName = name;
        }

        @Override
        public String name() {
            return mName;
        }

        @Override
        public ConstantNode withName(String name) {
            return name.equals(mName) ? this : super.withName(name);
        }
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
    public RowFilter toRowFilter(RowInfo info, Map<String, ColumnNode> columns) {
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
    public void evalColumns(Set<String> columns) {
    }

    @Override
    public Variable makeEval(EvalContext context) {
        return makeEval(context, mType.clazz(), mValue);
    }

    public static Variable makeEval(EvalContext context, Class type, Object value) {
        var valueVar = context.methodMaker().var(type);

        if (value == null || type.isPrimitive() ||
            String.class.isAssignableFrom(type) ||
            Class.class.isAssignableFrom(type))
        {
            return valueVar.set(value);
        } else {
            return valueVar.setExact(cCanonicalizer.apply(value));
        }
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
    public Variable makeFilterEval(EvalContext context) {
        if (mValue instanceof Boolean b) {
            return context.methodMaker().var(boolean.class).set(b.booleanValue());
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

    @Override
    public int hashCode() {
        return mType.hashCode() * 31 + Objects.hashCode(mValue);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ConstantNode cn
            && mType.equals(cn.mType) && Objects.equals(mValue, cn.mValue);
    }
}
