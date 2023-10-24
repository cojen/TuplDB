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

import java.util.Objects;

import org.cojen.maker.Label;
import org.cojen.maker.Variable;

import org.cojen.tupl.rows.ConvertCallSite;
import org.cojen.tupl.rows.RowInfo;
import org.cojen.tupl.rows.RowUtils;
import org.cojen.tupl.rows.SoftCache;

import org.cojen.tupl.rows.filter.FalseFilter;
import org.cojen.tupl.rows.filter.RowFilter;
import org.cojen.tupl.rows.filter.TrueFilter;

import static org.cojen.tupl.rows.ColumnInfo.*;

/**
 * Defines a simple node which just represents a constant value.
 *
 * @author Brian S. O'Neill
 */
public final class ConstantNode extends Node {
    public static ConstantNode make(Object value) {
        /* FIXME: needs a proper typeCode
        Type type = BasicType.make(value == null ? Object.class : value.getClass());
        return new ConstantNode(type, value);
        */
        throw null;
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
    public String name() {
        return String.valueOf(mValue);
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
    public RowFilter toFilter(RowInfo info) {
        if (mValue == Boolean.TRUE) {
            return TrueFilter.THE;
        } else if (mValue == Boolean.FALSE) {
            return FalseFilter.THE;
        }
        return super.toFilter(info);
    }

    @Override
    public Variable makeEval(EvalContext context) {
        return makeEval(context, mType.clazz(), mValue);
    }

    public static Variable makeEval(EvalContext context, Class type, Object value) {
        var valueVar = context.methodMaker().var(type);
        // FIXME: Must call setExact if necessary. Use Canonicalizer.
        return valueVar.set(value);
    }

    @Override
    public void makeFilter(EvalContext context, Label pass, Label fail) {
        if (mValue instanceof Boolean b) {
            context.methodMaker().goto_(b ? pass : fail);
        } else {
            throw new IllegalStateException();
        }
    }

    public Object value() {
        return mValue;
    }

    /**
     * Try to convert this numerical type to the given (different) numerical type if no
     * information is lost. This only needs to be called when a conversion would create a
     * BigDecimal or BigInteger, and so it doesn't need to cover all cases.
     */
    public ConstantNode tryConvert(Type toType) {
        Class<?> clazz;
        int typeCode;

        switch (toType.typeCode) {
        case TYPE_ULONG:
            switch (mType.typeCode) {
            case TYPE_BYTE: case TYPE_SHORT: case TYPE_INT: case TYPE_LONG:
                if (((Number) mValue).longValue() < 0L) {
                    return null;
                }
                break;

            case TYPE_FLOAT:
                float f = ((Float) mValue).floatValue();
                long i = (long) f;
                if (f < 0 || ((float) i) != f) {
                    return null;
                }
                break;

            case TYPE_DOUBLE:
                double d = ((Double) mValue).doubleValue();
                i = (long) d;
                if (d < 0 || ((double) i) != d) {
                    return null;
                }
                break;

            default:
                return null;
            }

            clazz = long.class;
            typeCode = TYPE_ULONG;
            break;

        case TYPE_FLOAT:
            switch (mType.typeCode) {
            case TYPE_LONG:
                long i = ((Long) mValue).longValue();
                float f = (float) i;
                if (((long) f) != i) {
                    return null;
                }
                break;

            default:
                return null;
            }

            clazz = float.class;
            typeCode = TYPE_FLOAT;
            break;

        case TYPE_DOUBLE:
            switch (mType.typeCode) {
            case TYPE_LONG:
                long i = ((Long) mValue).longValue();
                double d = (double) i;
                if (((long) d) != i) {
                    return null;
                }
                break;

            default:
                return null;
            }

            clazz = double.class;
            typeCode = TYPE_DOUBLE;
            break;

        default:
            return null;
        }

        return asType(BasicType.make(clazz, typeCode));
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
