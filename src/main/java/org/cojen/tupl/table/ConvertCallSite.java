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

import java.lang.invoke.CallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.MutableCallSite;
import java.lang.invoke.VarHandle;

import java.lang.reflect.Modifier;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.RandomAccess;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.io.Utils;

/**
 * Makes code which converts a value declared as Object to a specific type.
 *
 * @author Brian S O'Neill
 * @see Converter
 */
public class ConvertCallSite extends MutableCallSite {
    private static final MethodHandle cImplementHandle;
    private static final VarHandle cImplementedHandle;

    static {
        try {
            var lookup = MethodHandles.lookup();
            cImplementHandle = lookup.findVirtual
                (ConvertCallSite.class, "implement",
                 MethodType.methodType(Object.class, Object.class));
            cImplementedHandle = lookup.findVarHandle
                (ConvertCallSite.class, "mImplemented", int.class);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    // 0: not implemented, 1: being implemented, 2: implemented
    private volatile int mImplemented;

    private ConvertCallSite(MethodType mt) {
        super(mt);
    }

    /**
     * Makes code which converts a value declared as an object type to a specific type. If the
     * conversion cannot be performed, an exception is thrown at runtime.
     *
     * @param toType the specific desired "to" type
     * @param from variable declared as an object type which contains the "from" value
     * @return the "to" value, of type "toType"
     */
    public static Variable make(MethodMaker mm, Class toType, Variable from) {
        return mm.var(ConvertCallSite.class).indy("makeNext").invoke(toType, "_", null, from);
    }

    /**
     * Indy bootstrap method.
     *
     * @param mt accepts one param, an object, and returns a non-void toType
     */
    public static CallSite makeNext(MethodHandles.Lookup lookup, String name, MethodType mt) {
        var cs = new ConvertCallSite(mt);
        cs.setTarget(cImplementHandle.bindTo(cs).asType(mt));
        return cs;
    }

    private Object implement(Object obj) {
        while (true) {
            int state = mImplemented;
            if (state >= 2) {
                break;
            }
            if (state == 0 && cImplementedHandle.compareAndSet(this, 0, 1)) {
                try {
                    setTarget(makeConverter(obj, type()));
                    mImplemented = 2;
                    break;
                } catch (Throwable e) {
                    mImplemented = 0;
                    throw e;
                }
            }
            Thread.yield();
        }

        try {
            return getTarget().invoke(obj);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    /**
     * @param obj defines the "from" type, which can be null
     * @param mt defines the "to" type (the return type)
     */
    private static MethodHandle makeConverter(Object obj, MethodType mt) {
        // It's tempting to want to cache the generated converters, but each conversion chain
        // is unique to the initial call site it's generated for. A converter cannot therefore
        // be identified by just the from/to types and be shared.

        Class<?> fromType = obj == null ? null : obj.getClass();
        Class<?> toType = mt.returnType();

        MethodMaker mm = MethodMaker.begin(MethodHandles.lookup(), "convert", mt);
        Variable from = mm.param(0);

        final Label next = mm.label();
        final Variable result;

        if (fromType == null) {
            if (toType.isPrimitive()) {
                result = null;
            } else {
                from.ifNe(null, next);
                result = mm.var(toType).set(null);
            }
        } else {
            if (fromType.isAssignableFrom(from.classType())) {
                // InstanceOf check will always be true, but must still check for null.
                from.ifEq(null, next);
            } else {
                instanceOf(from, fromType, next);
            }

            if (!toType.isPrimitive() && toType.isAssignableFrom(fromType)) {
                result = from.cast(toType);
            } else if (toType == String.class) {
                result = toString(mm, fromType, from);
            } else if (toType == long.class || toType == Long.class) {
                result = toLong(mm, fromType, from);
            } else if (toType == int.class || toType == Integer.class) {
                result = toInt(mm, fromType, from);
            } else if (toType == double.class || toType == Double.class) {
                result = toDouble(mm, fromType, from);
            } else if (toType == float.class || toType == Float.class) {
                result = toFloat(mm, fromType, from);
            } else if (toType == boolean.class || toType == Boolean.class) {
                result = toBoolean(mm, fromType, from);
            } else if (toType == BigInteger.class) {
                result = toBigInteger(mm, fromType, from);
            } else if (toType == BigDecimal.class) {
                result = toBigDecimal(mm, fromType, from);
            } else if (toType == char.class || toType == Character.class) {
                result = toChar(mm, fromType, from);
            } else if (toType == byte.class || toType == Byte.class) {
                result = toByte(mm, fromType, from);
            } else if (toType == short.class || toType == Short.class) {
                result = toShort(mm, fromType, from);
            } else if (toType.isArray()) {
                result = toArray(mm, toType, fromType, from);
            } else {
                result = null;
            }
        }

        if (result != null) {
            mm.return_(result);
        } else {
            mm.new_(IllegalArgumentException.class,
                    "Cannot convert " + (fromType == null ? null : fromType.getSimpleName()) +
                    " to " + toType.getSimpleName()).throw_();
        }

        next.here();
        var indy = mm.var(ConvertCallSite.class).indy("makeNext");
        mm.return_(indy.invoke(toType, "_", null, from));

        return mm.finish();
    }

    /**
     * @param fail branch here if not instanceOf
     */
    private static void instanceOf(Variable fromVar, Class<?> fromType, Label fail) {
        if (Modifier.isPublic(fromType.getModifiers())) {
            fromVar.instanceOf(fromType).ifFalse(fail);
            return;
        }

        // Cannot generate code against a non-public type. Instead, generate a chain of
        // instanceOf checks against all publicly available super classes and interfaces.

        var types = new HashSet<Class<?>>();
        gatherAccessibleTypes(types, fromType);

        // Remove redundant interfaces.
        Iterator<Class<?>> it = types.iterator();
        outer: while (it.hasNext()) {
            Class<?> type = it.next();
            for (Class<?> other : types) {
                if (other != type && type.isAssignableFrom(other)) {
                    it.remove();
                    continue outer;
                }
            }
        }

        for (Class<?> type : types) {
            fromVar.instanceOf(type).ifFalse(fail);
        }
    }

    private static void gatherAccessibleTypes(HashSet<Class<?>> types, Class<?> type) {
        if (types.contains(type)) {
            return;
        }

        if (Modifier.isPublic(type.getModifiers())) {
            types.add(type);
        }

        Class<?> superType = type.getSuperclass();
        if (superType != null && superType != Object.class) {
            gatherAccessibleTypes(types, superType);
        }

        Class<?>[] ifaces = type.getInterfaces();
        if (ifaces != null) {
            for (var iface : ifaces) {
                if (iface != java.io.Serializable.class) {
                    gatherAccessibleTypes(types, iface);
                }
            }
        }
    }

    private static Variable toBoolean(MethodMaker mm, Class fromType, Variable from) {
        if (fromType == Boolean.class) {
            return from.cast(Boolean.class);
        } else if (fromType == String.class) {
            return mm.var(ConvertUtils.class).invoke
                ("stringToBooleanExact", from.cast(String.class));
        } else {
            return null;
        }
    }

    private static Variable toByte(MethodMaker mm, Class fromType, Variable from) {
        if (fromType == Byte.class) {
            return from.cast(Byte.class);
        } else if (fromType == Integer.class) {
            return mm.var(ConvertUtils.class).invoke("intToByteExact", from.cast(Integer.class));
        } else if (fromType == Long.class) {
            return mm.var(ConvertUtils.class).invoke("longToByteExact", from.cast(Long.class));
        } else if (fromType == String.class) {
            return mm.var(Byte.class).invoke("parseByte", from.cast(String.class));
        } else if (fromType == Double.class) {
            return mm.var(ConvertUtils.class).invoke("doubleToByteExact", from.cast(Double.class));
        } else if (fromType == Float.class) {
            return mm.var(ConvertUtils.class).invoke("floatToByteExact", from.cast(Float.class));
        } else if (fromType == Short.class) {
            return mm.var(ConvertUtils.class).invoke("shortToByteExact", from.cast(Short.class));
        } else if (fromType == BigInteger.class) {
            return from.cast(BigInteger.class).invoke("byteValueExact");
        } else if (fromType == BigDecimal.class) {
            return from.cast(BigDecimal.class).invoke("byteValueExact");
        } else {
            return null;
        }
    }

    private static Variable toShort(MethodMaker mm, Class fromType, Variable from) {
        if (fromType == Short.class) {
            return from.cast(Short.class);
        } else if (fromType == Integer.class) {
            return mm.var(ConvertUtils.class).invoke("intToShortExact", from.cast(Integer.class));
        } else if (fromType == Long.class) {
            return mm.var(ConvertUtils.class).invoke("longToShortExact", from.cast(Long.class));
        } else if (fromType == String.class) {
            return mm.var(Short.class).invoke("parseShort", from.cast(String.class));
        } else if (fromType == Double.class) {
            return mm.var(ConvertUtils.class).invoke("doubleToShortExact", from.cast(Double.class));
        } else if (fromType == Float.class) {
            return mm.var(ConvertUtils.class).invoke("floatToShortExact", from.cast(Float.class));
        } else if (fromType == Byte.class) {
            return from.cast(Byte.class).invoke("shortValue");
        } else if (fromType == BigInteger.class) {
            return from.cast(BigInteger.class).invoke("shortValueExact");
        } else if (fromType == BigDecimal.class) {
            return from.cast(BigDecimal.class).invoke("shortValueExact");
        } else {
            return null;
        }
    }

    private static Variable toInt(MethodMaker mm, Class fromType, Variable from) {
        if (fromType == Integer.class) {
            return from.cast(Integer.class);
        } else if (fromType == Long.class) {
            return mm.var(Math.class).invoke("toIntExact", from.cast(Long.class));
        } else if (fromType == String.class) {
            return mm.var(Integer.class).invoke("parseInt", from.cast(String.class));
        } else if (fromType == Double.class) {
            return mm.var(ConvertUtils.class).invoke("doubleToIntExact", from.cast(Double.class));
        } else if (fromType == Float.class) {
            return mm.var(ConvertUtils.class).invoke("floatToIntExact", from.cast(Float.class));
        } else if (fromType == Byte.class) {
            return from.cast(Byte.class).invoke("intValue");
        } else if (fromType == Short.class) {
            return from.cast(Short.class).invoke("intValue");
        } else if (fromType == BigInteger.class) {
            return from.cast(BigInteger.class).invoke("intValueExact");
        } else if (fromType == BigDecimal.class) {
            return from.cast(BigDecimal.class).invoke("intValueExact");
        } else {
            return null;
        }
    }

    private static Variable toLong(MethodMaker mm, Class fromType, Variable from) {
        if (fromType == Long.class) {
            return from.cast(Long.class);
        } else if (fromType == Integer.class) {
            return from.cast(Integer.class).invoke("longValue");
        } else if (fromType == String.class) {
            return mm.var(Long.class).invoke("parseLong", from.cast(String.class));
        } else if (fromType == Double.class) {
            return mm.var(ConvertUtils.class).invoke("doubleToLongExact", from.cast(Double.class));
        } else if (fromType == Float.class) {
            return mm.var(ConvertUtils.class).invoke("floatToLongExact", from.cast(Float.class));
        } else if (fromType == Byte.class) {
            return from.cast(Byte.class).invoke("longValue");
        } else if (fromType == Short.class) {
            return from.cast(Short.class).invoke("longValue");
        } else if (fromType == BigInteger.class) {
            return from.cast(BigInteger.class).invoke("longValueExact");
        } else if (fromType == BigDecimal.class) {
            return from.cast(BigDecimal.class).invoke("longValueExact");
        } else {
            return null;
        }
    }

    private static Variable toFloat(MethodMaker mm, Class fromType, Variable from) {
        if (fromType == Float.class) {
            return from.cast(Float.class);
        } else if (fromType == String.class) {
            return mm.var(Float.class).invoke("parseFloat", from.cast(String.class));
        } else if (fromType == Integer.class) {
            return mm.var(ConvertUtils.class).invoke("intToFloatExact", from.cast(Integer.class));
        } else if (fromType == Double.class) {
            return mm.var(ConvertUtils.class).invoke("doubleToFloatExact", from.cast(Double.class));
        } else if (fromType == Long.class) {
            return mm.var(ConvertUtils.class).invoke("longToFloatExact", from.cast(Long.class));
        } else if (fromType == Byte.class) {
            return from.cast(Byte.class).invoke("floatValue");
        } else if (fromType == Short.class) {
            return from.cast(Short.class).invoke("floatValue");
        } else if (fromType == BigInteger.class) {
            return mm.var(ConvertUtils.class).invoke("biToFloatExact", from.cast(BigInteger.class));
        } else if (fromType == BigDecimal.class) {
            return mm.var(ConvertUtils.class).invoke("bdToFloatExact", from.cast(BigDecimal.class));
        } else {
            return null;
        }
    }

    private static Variable toDouble(MethodMaker mm, Class fromType, Variable from) {
        if (fromType == Double.class) {
            return from.cast(Double.class);
        } else if (fromType == String.class) {
            return mm.var(Double.class).invoke("parseDouble", from.cast(String.class));
        } else if (fromType == Integer.class) {
            return from.cast(Integer.class);
        } else if (fromType == Long.class) {
            return mm.var(ConvertUtils.class).invoke("longToDoubleExact", from.cast(Long.class));
        } else if (fromType == Float.class) {
            return from.cast(Float.class).invoke("doubleValue");
        } else if (fromType == Byte.class) {
            return from.cast(Byte.class).invoke("doubleValue");
        } else if (fromType == Short.class) {
            return from.cast(Short.class).invoke("doubleValue");
        } else if (fromType == BigInteger.class) {
            return mm.var(ConvertUtils.class).invoke
                ("biToDoubleExact", from.cast(BigInteger.class));
        } else if (fromType == BigDecimal.class) {
            return mm.var(ConvertUtils.class).invoke
                ("bdToDoubleExact", from.cast(BigDecimal.class));
        } else {
            return null;
        }
    }

    private static Variable toChar(MethodMaker mm, Class fromType, Variable from) {
        if (fromType == Character.class) {
            return from.cast(Character.class);
        } else if (fromType == String.class) {
            return mm.var(ConvertUtils.class).invoke("stringToCharExact", from.cast(String.class));
        } else {
            return null;
        }
    }

    private static Variable toString(MethodMaker mm, Class fromType, Variable from) {
        return mm.var(String.class).invoke("valueOf", from);
    }

    private static Variable toBigInteger(MethodMaker mm, Class fromType, Variable from) {
        Variable longVar;

        if (fromType == Integer.class) {
            longVar = from.cast(Integer.class);
        } else if (fromType == Long.class) {
            longVar = from.cast(Long.class);
        } else if (fromType == String.class) {
            return mm.new_(BigInteger.class, from.cast(String.class));
        } else if (fromType == Double.class) {
            longVar = mm.var(ConvertUtils.class).invoke
                ("doubleToLongExact", from.cast(Double.class));
        } else if (fromType == Float.class) {
            longVar = mm.var(ConvertUtils.class).invoke("floatToLongExact", from.cast(Float.class));
        } else if (fromType == Byte.class) {
            longVar = from.cast(Byte.class);
        } else if (fromType == Short.class) {
            longVar = from.cast(Short.class);
        } else if (fromType == BigDecimal.class) {
            return from.cast(BigDecimal.class).invoke("toBigIntegerExact");
        } else {
            return null;
        }

        return mm.var(BigInteger.class).invoke("valueOf", longVar);
    }

    private static Variable toBigDecimal(MethodMaker mm, Class fromType, Variable from) {
        Variable numVar;

        standard: {
            if (fromType == String.class) {
                return mm.new_(BigDecimal.class, from.cast(String.class));
            } else if (fromType == Integer.class) {
                numVar = from.cast(Integer.class);
            } else if (fromType == Long.class) {
                numVar = from.cast(Long.class);
            } else if (fromType == Double.class) {
                numVar = from.cast(Double.class);
                break standard;
            } else if (fromType == Float.class) {
                numVar = from.cast(Float.class);
                break standard;
            } else if (fromType == Byte.class) {
                numVar = from.cast(Byte.class);
            } else if (fromType == Short.class) {
                numVar = from.cast(Short.class);
            } else if (fromType == BigInteger.class) {
                return mm.new_(BigDecimal.class, from.cast(BigInteger.class));
            } else {
                return null;
            }

            return mm.var(BigDecimal.class).invoke("valueOf", numVar);
        }

        return mm.var(BigDecimalUtils.class).invoke("toBigDecimal", numVar);
    }

    private static Variable toArray(MethodMaker mm, Class toType, Class fromType, Variable from) {
        Class toElementType = toType.getComponentType();

        if (fromType.isArray()) {
            var fromArrayVar = from.cast(fromType);
            var lengthVar = fromArrayVar.alength();
            // Copy and conversion loop. Note that the from elements are always boxed for
            // simplicity. The generated code is expected to have inlining and autoboxing
            // optimizations applied anyhow.
            return ConvertUtils.convertArray(mm, toType, lengthVar, ixVar -> {
                return make(mm, toElementType, fromArrayVar.aget(ixVar).box());
            });
        } else if (List.class.isAssignableFrom(fromType) &&
                   RandomAccess.class.isAssignableFrom(fromType))
        {
            var fromListVar = from.cast(List.class);
            var lengthVar = fromListVar.invoke("size");
            return ConvertUtils.convertArray(mm, toType, lengthVar, ixVar -> {
                return make(mm, toElementType, fromListVar.invoke("get", ixVar));
            });
        } else if (Collection.class.isAssignableFrom(fromType)) {
            var fromCollectionVar = from.cast(Collection.class);
            var lengthVar = fromCollectionVar.invoke("size");
            var itVar = fromCollectionVar.invoke("iterator");
            return ConvertUtils.convertArray(mm, toType, lengthVar, ixVar -> {
                return make(mm, toElementType, itVar.invoke("next"));
            });
        } else {
            return null;
        }
    }
}
