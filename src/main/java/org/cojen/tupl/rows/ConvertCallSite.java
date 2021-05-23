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
class ConvertCallSite extends MutableCallSite {
    // FIXME: testing
    public static void main(String[] args) throws Throwable {
        MethodMaker mm = MethodMaker.begin(MethodHandles.lookup(), null, "test", Object.class);

        /*
        var result = make(mm, long.class, mm.param(0));
        mm.var(System.class).field("out").invoke("println", result);
        MethodHandle mh = mm.finish();
        mh.invoke(Integer.valueOf(100));
        mh.invoke(Long.valueOf(Long.MAX_VALUE));
        mh.invoke(Long.valueOf(Long.MAX_VALUE));
        mh.invoke("123");
        */

        /*
        var result = make(mm, String.class, mm.param(0));
        mm.var(System.class).field("out").invoke("println", result);
        MethodHandle mh = mm.finish();
        mh.invoke(100);
        mh.invoke("hello");
        mh.invoke(1001L);
        mh.invoke(null);
        mh.invoke(true);
        mh.invoke("true!");
        */

        var result = make(mm, boolean.class, mm.param(0));
        mm.var(System.class).field("out").invoke("println", result);
        MethodHandle mh = mm.finish();
        mh.invoke(true);
        mh.invoke("true");
        mh.invoke(0);
        mh.invoke(1);
        mh.invoke(1L);
        mh.invoke("true!");
    }

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
     * @param mt accepts one param, an Object, and returns non-void toType
     */
    static CallSite makeNext(MethodHandles.Lookup lookup, String name, MethodType mt) {
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
                    doImplement(obj);
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

    private void doImplement(Object obj) {
        MethodMaker mm = MethodMaker.begin(MethodHandles.lookup(), "convert", type());
        Label next = mm.label();
        Variable from = mm.param(0);

        Class fromType = obj == null ? null : obj.getClass();
        Class toType = type().returnType();

        Variable result;
        if (fromType == null) {
            if (toType.isPrimitive()) {
                result = null;
            } else {
                from.ifNe(null, next);
                result = mm.var(toType).set(null);
            }
        } else {
            from.instanceOf(fromType).ifFalse(next);

            if (!toType.isPrimitive()) {
                Label notNull = mm.label();
                from.ifNe(null, notNull);
                mm.return_(null);
                notNull.here();
            }

            if (toType == String.class) {
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
                    "Unable to convert " + (fromType == null ? null : fromType.getSimpleName()) +
                    " to " + toType.getSimpleName()).throw_();
        }

        next.here();
        var indy = mm.var(ConvertCallSite.class).indy("makeNext");
        mm.return_(indy.invoke(toType, "_", null, from));

        MethodHandle mh = mm.finish();
        setTarget(mh);
    }

    private static Variable toBoolean(MethodMaker mm, Class fromType, Variable from) {
        if (fromType == Boolean.class) {
            return from.cast(Boolean.class);
        } else if (fromType == String.class) {
            return mm.var(Boolean.class).invoke("parseBoolean", from.cast(String.class));
        } else {
            return null;
        }
    }

    private static Variable toByte(MethodMaker mm, Class fromType, Variable from) {
        // FIXME
        return null;
    }

    private static Variable toShort(MethodMaker mm, Class fromType, Variable from) {
        // FIXME
        return null;
    }

    private static Variable toInt(MethodMaker mm, Class fromType, Variable from) {
        // FIXME: more types
        if (fromType == Integer.class) {
            return from.cast(Integer.class);
        } else if (fromType == Long.class) {
            return mm.var(Math.class).invoke("toIntExact", from.cast(Long.class));
        } else if (fromType == String.class) {
            return mm.var(Integer.class).invoke("parseInt", from.cast(String.class));
        } else {
            return null;
        }
    }

    private static Variable toLong(MethodMaker mm, Class fromType, Variable from) {
        // FIXME: more types
        if (fromType == Long.class) {
            return from.cast(Long.class);
        } else if (fromType == Integer.class) {
            return from.cast(Integer.class);
        } else if (fromType == String.class) {
            return mm.var(Long.class).invoke("parseLong", from.cast(String.class));
        } else {
            return null;
        }
    }

    private static Variable toFloat(MethodMaker mm, Class fromType, Variable from) {
        // FIXME: more types
        if (fromType == Float.class) {
            return from.cast(Float.class);
        } else if (fromType == String.class) {
            return mm.var(Float.class).invoke("parseFloat", from.cast(String.class));
        } else {
            return null;
        }
    }

    private static Variable toDouble(MethodMaker mm, Class fromType, Variable from) {
        // FIXME: more types
        if (fromType == Double.class) {
            return from.cast(Double.class);
        } else if (fromType == String.class) {
            return mm.var(Double.class).invoke("parseDouble", from.cast(String.class));
        } else {
            return null;
        }
    }

    private static Variable toChar(MethodMaker mm, Class fromType, Variable from) {
        // FIXME
        return null;
    }

    private static Variable toString(MethodMaker mm, Class fromType, Variable from) {
        if (fromType == String.class) {
            return from.cast(String.class);
        } else {
            return mm.var(String.class).invoke("valueOf", from);
        }
    }

    private static Variable toBigInteger(MethodMaker mm, Class fromType, Variable from) {
        // FIXME
        return null;
    }

    private static Variable toBigDecimal(MethodMaker mm, Class fromType, Variable from) {
        // FIXME
        return null;
    }
}
