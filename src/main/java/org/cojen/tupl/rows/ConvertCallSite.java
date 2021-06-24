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

package org.cojen.tupl.rows;

import java.lang.invoke.CallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.MutableCallSite;
import java.lang.invoke.VarHandle;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.io.Utils;

/**
 * Makes code which converts a value declared as Object to a specific type.
 *
 * @author Brian S O'Neill
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
     * Makes code which converts a value declared as Object to a specific type. If the
     * conversion cannot be performed, an exception is thrown at runtime.
     *
     * @param toType the specific desired "to" type
     * @param from variable declared as Object type which contains the "from" value
     * @return the "to" value, of type "toType"
     */
    static Variable make(MethodMaker mm, Class toType, Variable from) {
        return mm.var(ConvertCallSite.class).indy("makeNext").invoke(toType, "_", null, from);
    }

    /**
     * Indy bootstrap method.
     *
     * @param mt accepts one param, an Object, and returns a non-void toType
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
        Label next = mm.label();
        Variable from = mm.param(0);

        final Variable result;
        if (fromType == null) {
            if (toType.isPrimitive()) {
                result = null;
            } else {
                from.ifNe(null, next);
                result = mm.var(toType).set(null);
            }
        } else {
            from.instanceOf(fromType).ifFalse(next);

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

    private static Variable toBoolean(MethodMaker mm, Class fromType, Variable from) {
        if (fromType == Boolean.class) {
            return from.cast(Boolean.class);
        } else if (fromType == String.class) {
            return mm.var(ConvertCallSite.class).invoke("stringToBoolean", from.cast(String.class));
        } else {
            return null;
        }
    }

    private static Variable toByte(MethodMaker mm, Class fromType, Variable from) {
        if (fromType == Byte.class) {
            return from.cast(Byte.class);
        } else if (fromType == Integer.class) {
            return mm.var(ConvertCallSite.class).invoke("intToByte", from.cast(Integer.class));
        } else if (fromType == Long.class) {
            return mm.var(ConvertCallSite.class).invoke("longToByte", from.cast(Long.class));
        } else if (fromType == String.class) {
            return mm.var(Byte.class).invoke("parseByte", from.cast(String.class));
        } else if (fromType == Double.class) {
            return mm.var(ConvertCallSite.class).invoke("doubleToByte", from.cast(Double.class));
        } else if (fromType == Float.class) {
            return mm.var(ConvertCallSite.class).invoke("floatToByte", from.cast(Float.class));
        } else if (fromType == Short.class) {
            return mm.var(ConvertCallSite.class).invoke("shortToByte", from.cast(Short.class));
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
            return mm.var(ConvertCallSite.class).invoke("intToShort", from.cast(Integer.class));
        } else if (fromType == Long.class) {
            return mm.var(ConvertCallSite.class).invoke("longToShort", from.cast(Long.class));
        } else if (fromType == String.class) {
            return mm.var(Short.class).invoke("parseShort", from.cast(String.class));
        } else if (fromType == Double.class) {
            return mm.var(ConvertCallSite.class).invoke("doubleToShort", from.cast(Double.class));
        } else if (fromType == Float.class) {
            return mm.var(ConvertCallSite.class).invoke("floatToShort", from.cast(Float.class));
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
            return mm.var(ConvertCallSite.class).invoke("doubleToInt", from.cast(Double.class));
        } else if (fromType == Float.class) {
            return mm.var(ConvertCallSite.class).invoke("floatToInt", from.cast(Float.class));
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
            return mm.var(ConvertCallSite.class).invoke("doubleToLong", from.cast(Double.class));
        } else if (fromType == Float.class) {
            return mm.var(ConvertCallSite.class).invoke("floatToLong", from.cast(Float.class));
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
            return mm.var(ConvertCallSite.class).invoke("intToFloat", from.cast(Integer.class));
        } else if (fromType == Double.class) {
            return mm.var(ConvertCallSite.class).invoke("doubleToFloat", from.cast(Double.class));
        } else if (fromType == Long.class) {
            return mm.var(ConvertCallSite.class).invoke("longToFloat", from.cast(Long.class));
        } else if (fromType == Byte.class) {
            return from.cast(Byte.class).invoke("floatValue");
        } else if (fromType == Short.class) {
            return from.cast(Short.class).invoke("floatValue");
        } else if (fromType == BigInteger.class) {
            var intVar = from.cast(BigInteger.class).invoke("intValueExact");
            return mm.var(ConvertCallSite.class).invoke("intToFloat", intVar);
        } else if (fromType == BigDecimal.class) {
            return mm.var(ConvertCallSite.class).invoke("bdToFloat", from.cast(BigDecimal.class));
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
            return mm.var(ConvertCallSite.class).invoke("longToDouble", from.cast(Long.class));
        } else if (fromType == Float.class) {
            return from.cast(Float.class).invoke("doubleValue");
        } else if (fromType == Byte.class) {
            return from.cast(Byte.class).invoke("doubleValue");
        } else if (fromType == Short.class) {
            return from.cast(Short.class).invoke("doubleValue");
        } else if (fromType == BigInteger.class) {
            var longVar = from.cast(BigInteger.class).invoke("longValueExact");
            return mm.var(ConvertCallSite.class).invoke("longToDouble", longVar);
        } else if (fromType == BigDecimal.class) {
            return mm.var(ConvertCallSite.class).invoke("bdToDouble", from.cast(BigDecimal.class));
        } else {
            return null;
        }
    }

    private static Variable toChar(MethodMaker mm, Class fromType, Variable from) {
        if (fromType == Character.class) {
            return from.cast(Character.class);
        } else if (fromType == String.class) {
            return mm.var(ConvertCallSite.class).invoke("stringToChar", from.cast(String.class));
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
            longVar = mm.var(ConvertCallSite.class).invoke("doubleToLong", from.cast(Double.class));
        } else if (fromType == Float.class) {
            longVar = mm.var(ConvertCallSite.class).invoke("floatToLong", from.cast(Float.class));
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

        if (fromType == String.class) {
            return mm.new_(BigDecimal.class, from.cast(String.class));
        } else if (fromType == Integer.class) {
            numVar = from.cast(Integer.class);
        } else if (fromType == Long.class) {
            numVar = from.cast(Long.class);
        } else if (fromType == Double.class) {
            numVar = from.cast(Double.class);
        } else if (fromType == Float.class) {
            numVar = from.cast(Float.class);
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

    // Called by generated code.
    public static boolean stringToBoolean(String str) {
        if (str.equalsIgnoreCase("false")) {
            return false;
        }
        if (str.equalsIgnoreCase("true")) {
            return true;
        }
        throw new IllegalArgumentException("Cannot convert to Boolean: " + str);
    }

    // Called by generated code.
    public static char stringToChar(String str) {
        if (str.length() == 1) {
            return str.charAt(0);
        }
        throw new IllegalArgumentException("Cannot convert to Character: " + str);
    }

    // Called by generated code.
    public static int doubleToInt(double d) {
        int i = (int) d;
        if ((double) i != d) {
            throw loss(Integer.class, d);
        }
        return i;
    }

    // Called by generated code.
    public static long doubleToLong(double d) {
        long i = (int) d;
        if ((double) i != d) {
            throw loss(Long.class, d);
        }
        return i;
    }

    // Called by generated code.
    public static float doubleToFloat(double d) {
        float f = (float) d;
        if ((double) f != d && !Double.isNaN(d)) {
            throw loss(Float.class, d);
        }
        return f;
    }

    // Called by generated code.
    public static byte doubleToByte(double d) {
        byte b = (byte) d;
        if ((double) b != d) {
            throw loss(Byte.class, d);
        }
        return b;
    }

    // Called by generated code.
    public static short doubleToShort(double d) {
        short s = (short) d;
        if ((double) s != d) {
            throw loss(Short.class, d);
        }
        return s;
    }

    // Called by generated code.
    public static int floatToInt(float f) {
        int i = (int) f;
        if ((float) i != f) {
            throw loss(Integer.class, f);
        }
        return i;
    }

    // Called by generated code.
    public static long floatToLong(float f) {
        long i = (int) f;
        if ((float) i != f) {
            throw loss(Long.class, f);
        }
        return i;
    }

    // Called by generated code.
    public static byte floatToByte(float f) {
        byte b = (byte) f;
        if ((float) b != f) {
            throw loss(Byte.class, f);
        }
        return b;
    }

    // Called by generated code.
    public static short floatToShort(float f) {
        short s = (short) f;
        if ((float) s != f) {
            throw loss(Short.class, f);
        }
        return s;
    }

    // Called by generated code.
    public static byte shortToByte(short i) {
        byte b = (byte) i;
        if ((short) b != i) {
            throw loss(Byte.class, i);
        }
        return b;
    }

    // Called by generated code.
    public static byte intToByte(int i) {
        byte b = (byte) i;
        if ((int) b != i) {
            throw loss(Byte.class, i);
        }
        return b;
    }

    // Called by generated code.
    public static short intToShort(int i) {
        short s = (short) i;
        if ((int) s != i) {
            throw loss(Short.class, i);
        }
        return s;
    }

    // Called by generated code.
    public static float intToFloat(int i) {
        float f = (float) i;
        if ((int) f != i) {
            throw loss(Float.class, i);
        }
        return f;
    }

    // Called by generated code.
    public static byte longToByte(long i) {
        byte b = (byte) i;
        if ((long) b != i) {
            throw loss(Byte.class, i);
        }
        return b;
    }

    // Called by generated code.
    public static short longToShort(long i) {
        short s = (short) i;
        if ((long) s != i) {
            throw loss(Short.class, i);
        }
        return s;
    }

    // Called by generated code.
    public static float longToFloat(long i) {
        float f = (float) i;
        if ((long) f != i) {
            throw loss(Float.class, i);
        }
        return f;
    }

    // Called by generated code.
    public static double longToDouble(long i) {
        double d = (double) i;
        if ((long) d != i) {
            throw loss(Double.class, i);
        }
        return d;
    }

    // Called by generated code.
    public static float bdToFloat(BigDecimal bd) {
        float f = bd.floatValue();
        if (BigDecimal.valueOf(f).compareTo(bd) != 0) {
            throw loss(Float.class, bd);
        }
        return f;
    }

    // Called by generated code.
    public static double bdToDouble(BigDecimal bd) {
        double d = bd.doubleValue();
        if (BigDecimal.valueOf(d).compareTo(bd) != 0) {
            throw loss(Double.class, bd);
        }
        return d;
    }

    private static ArithmeticException loss(Class to, Object value) {
        return new ArithmeticException("Cannot convert to " + to.getSimpleName()
                                       + " without loss: " + value);
    }
}
