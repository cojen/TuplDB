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

package org.cojen.tupl.core;

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import java.util.Arrays;
import java.util.Objects;
import java.util.Random;
import java.util.RandomAccess;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

/**
 * Generates various types of tuple objects. Example: TupleKey.make.with("hello", "world")
 *
 * <p>A special feature is that array elements are properly handled when calling the {@code
 * hashCode}, {@code equals}, and {@code toString} methods. The sub elements of the array are
 * examined recursively instead of relying on the default array behavior which just inherits
 * the methods defined in the {@code Object} class.
 *
 * @author Brian S. O'Neill
 */
public abstract class TupleKey implements RandomAccess {
    public static final Maker make = makeMaker();

    public abstract int size();

    /**
     * Returns the declared type of a tuple element.
     *
     * @param ix zero-based index
     * @throws IndexOutOfBoundsException if out of bounds
     */
    public abstract Class<?> type(int ix);

    /**
     * Returns a tuple element.
     *
     * @param ix zero-based index
     * @throws IndexOutOfBoundsException if out of bounds
     */
    public abstract Object get(int ix);

    /**
     * Returns a tuple element.
     *
     * @param ix zero-based index
     * @throws IndexOutOfBoundsException if out of bounds
     * @throws ClassCastException if element isn't a string
     */
    public abstract String getString(int ix);

    /**
     * Returns a tuple element.
     *
     * @param ix zero-based index
     * @throws IndexOutOfBoundsException if out of bounds
     * @throws ClassCastException if element isn't an int or cannot be widened to an int
     */
    public abstract int get_int(int ix);

    /**
     * Returns a tuple element.
     *
     * @param ix zero-based index
     * @throws IndexOutOfBoundsException if out of bounds
     * @throws ClassCastException if element isn't a long or cannot be widened to a long
     */
    public abstract long get_long(int ix);

    public abstract static class Maker {
        // Declaring more specific types of "with" methods allows for more efficient types of
        // tuple implementations to be generated. Note that the method name can be anything.
        // The "with" methods and custom tuple classes are generated on demand, and so there's
        // no harm in declaring more than is actually needed at runtime.

        public abstract TupleKey with(byte[] e0);

        public abstract TupleKey with(byte[] e0, Object e1);

        public abstract TupleKey with(byte[] e0, Object[] e1);

        public abstract TupleKey with(int e0, byte[] e1);

        public abstract TupleKey with(int e0, Object e1);

        public abstract TupleKey with(Object e0, boolean e1);

        public abstract TupleKey with(Object e0, long e1);

        public abstract TupleKey with(Object e0, byte[] e1);

        public abstract TupleKey with(Object e0, Object e1);

        public abstract TupleKey with(Object e0, Object e1, Object e2, Object e3);

        public abstract TupleKey with(Object e0, Object[] e1);

        public abstract TupleKey with(Object e0, Object e1, Object e2);

        public abstract TupleKey with(Object e0, Object e1, String e2);

        public abstract TupleKey with(Object e0, Object e1, Object[] e2);

        public abstract TupleKey with(Object e0, String e1, String e2);

        public abstract TupleKey with(Object e0, boolean e1, Object[] e2);

        public abstract TupleKey with(Object[] e0, Object e1);

        public abstract TupleKey with(Object[] e0);

        static CallSite indyWith(MethodHandles.Lookup lookup, String name, MethodType mt) {
            MethodMaker mm = MethodMaker.begin(lookup, name, mt);
            makeWith(lookup, mm, mt.parameterArray());
            return new ConstantCallSite(mm.finish());
        }
    }

    private static Maker makeMaker() {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        ClassMaker cm = ClassMaker.begin(null, lookup);
        cm.final_().extend(Maker.class);

        cm.addConstructor();

        makeHashCode(cm);
        makeEquals(cm);
        makeToString(cm);

        for (Method m : Maker.class.getMethods()) {
            if (Modifier.isAbstract(m.getModifiers())) {
                String name = m.getName();
                Class<?>[] types = m.getParameterTypes();
                MethodMaker mm = cm.addMethod(TupleKey.class, name, (Object[]) types).public_();
                var params = new Object[types.length];
                for (int i=0; i<types.length; i++) {
                    params[i] = mm.param(i);
                }
                mm.return_(mm.this_().indy("indyWith").invoke(TupleKey.class, name, null, params));
            }
        }

        Class<?> clazz = cm.finish();

        try {
            MethodHandle mh = lookup.findConstructor(clazz, MethodType.methodType(void.class));
            return (Maker) mh.invoke();
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    /**
     * Makes a method which calls obj.hashCode, or Arrays.hashCode, or Arrays.deepHashCode. The
     * parameter must not be null.
     *
     *     static int hashCode(Object obj);
     */
    private static void makeHashCode(ClassMaker cm) {
        makeCases(cm.addMethod(int.class, "hashCode", Object.class).static_(), 0);
    }

    /**
     * Makes a method which calls obj.equals, or Arrays.equals, or Arrays.deepEquals. The
     * first parameter must not be null.
     *
     *     static boolean equals(Object obj1, Object obj2);
     */
    private static void makeEquals(ClassMaker cm) {
        makeCases(cm.addMethod(boolean.class, "equals", Object.class, Object.class).static_(), 1);
    }

    /**
     * Makes a method which calls obj.toString, or Arrays.toString, or Arrays.deepToString. The
     * parameter must not be null.
     *
     *     static String toString(Object obj);
     */
    private static void makeToString(ClassMaker cm) {
        makeCases(cm.addMethod(String.class, "toString", Object.class).static_(), 2);
    }

    /**
     * @param mode 0=hashCode, 1=equals, 2=toString
     */
    private static void makeCases(MethodMaker mm, int mode) {
        Object[] cases = {
            Object[].class,
            boolean[].class, byte[].class, char[].class, short[].class,
            int[].class, long[].class, float[].class, double[].class
        };

        var labels = new Label[cases.length];

        for (int i=0; i<labels.length; i++) {
            labels[i] = mm.label();
        }

        Label defaultLabel = mm.label();

        var obj1Var = mm.param(0);
        obj1Var.invoke("getClass").switch_(defaultLabel, cases, labels);

        Label fail = mode == 1 ? mm.label() : null;

        var arraysVar = mm.var(Arrays.class);

        labels[0].here(); // expected to be the Object[] case

        switch (mode) {
            case 0  -> mm.return_(arraysVar.invoke("deepHashCode", obj1Var.cast(cases[0])));

            case 1  -> {
                var obj2Var = mm.param(1);
                obj2Var.instanceOf(cases[0]).ifFalse(fail);
                mm.return_(arraysVar.invoke
                           ("deepEquals", obj1Var.cast(cases[0]), obj2Var.cast(cases[0])));
            }

            default -> mm.return_(arraysVar.invoke("deepToString", obj1Var.cast(cases[0])));
        }

        for (int i=1; i<labels.length; i++) {
            labels[i].here(); // expected to be a primitive array case

            switch (mode) {
                case 0  -> mm.return_(arraysVar.invoke("hashCode", obj1Var.cast(cases[i])));

                case 1  -> {
                    var obj2Var = mm.param(1);
                    obj2Var.instanceOf(cases[i]).ifFalse(fail);
                    mm.return_(arraysVar.invoke
                               ("equals", obj1Var.cast(cases[i]), obj2Var.cast(cases[i])));
                }

                default -> mm.return_(arraysVar.invoke("toString", obj1Var.cast(cases[i])));
            }
        }

        defaultLabel.here(); // expected to be the generic Object case

        // Might be a subclass, like String[].
        obj1Var.instanceOf(Object[].class).ifTrue(labels[0]);

        switch (mode) {
            case 0  -> mm.return_(obj1Var.invoke("hashCode"));
            case 1  -> mm.return_(obj1Var.invoke("equals", mm.param(1)));
            default -> mm.return_(obj1Var.invoke("toString"));
        }

        if (fail != null) { // only expected for the equals method
            fail.here();
            mm.return_(false);
        }
    }

    private static void makeWith(MethodHandles.Lookup lookup, MethodMaker mm, Class<?>[] types) {
        ClassMaker cm = ClassMaker.begin(null, lookup).final_().extend(TupleKey.class);

        MethodMaker ctor = cm.addConstructor((Object[]) types);
        ctor.invokeSuperConstructor();
        for (int i=0; i<types.length; i++) {
            String fieldName = fieldName(i);
            cm.addField(types[i], fieldName).private_().final_();
            ctor.field(fieldName).set(ctor.param(i));
        }

        Class<?> makerClass = lookup.lookupClass();

        cm.addMethod(int.class, "size").public_().return_(types.length);

        makeGet(cm, types, Class.class, "type");
        makeGet(cm, types, Object.class, "get");
        makeGet(cm, types, String.class, "getString");
        makeGet(cm, types, int.class, "get_int");
        makeGet(cm, types, long.class, "get_long");

        makeHashCode(cm, types, makerClass);
        makeEquals(cm, types, makerClass);
        makeToString(cm, types, makerClass);

        Class<?> tupleClazz = cm.finish();

        var params = new Object[types.length];
        for (int i=0; i<params.length; i++) {
            params[i] = mm.param(i);
        }

        mm.return_(mm.new_(tupleClazz, params));
    }

    private static void makeGet(ClassMaker cm, Class<?>[] types, Class<?> retType, String variant) {
        MethodMaker mm = cm.addMethod(retType, variant, int.class).public_();
        var ixVar = mm.param(0);

        if (types.length == 0) {
            mm.var(Objects.class).invoke("checkIndex", ixVar, 0);
            // Not expected to be reached.
            mm.new_(IndexOutOfBoundsException.class).throw_();
        }

        var cases = new int[types.length];
        var labels = new Label[cases.length];

        for (int i=0; i<cases.length; i++) {
            cases[i] = i;
            labels[i] = mm.label();
        }

        Label defaultLabel = mm.label();

        ixVar.switch_(defaultLabel, cases, labels);

        defaultLabel.here();
        mm.var(Objects.class).invoke("checkIndex", ixVar, cases.length);

        for (int i=0; i<labels.length; i++) {
            labels[i].here();

            if (variant == "type") {
                mm.return_(types[i]);
                continue;
            }

            var fieldVar = mm.field(fieldName(i)).get();

            switch (variant) {
                default -> throw new AssertionError();

                case "get" ->  mm.return_(fieldVar);

                case "getString" -> {
                    Class<?> type = fieldVar.classType();
                    if (type == String.class) {
                        mm.return_(fieldVar);
                    } else if (type.isPrimitive() || type.isArray()) {
                        mm.new_(ClassCastException.class).throw_();
                    } else {
                        mm.return_(fieldVar.cast(String.class));
                    }
                }

                case "get_int" -> {
                    Class<?> type = fieldVar.classType();
                    if (type == int.class || type == byte.class ||
                        type == char.class || type == short.class)
                    {
                        mm.return_(fieldVar);
                    } else {
                        mm.new_(ClassCastException.class).throw_();
                    }
                }

                case "get_long" -> {
                    Class<?> type = fieldVar.classType();
                    if (type == long.class || type == int.class || type == byte.class ||
                        type == char.class || type == short.class)
                    {
                        mm.return_(fieldVar);
                    } else {
                        mm.new_(ClassCastException.class).throw_();
                    }
                }
            }
        }
    }

    private static void makeHashCode(ClassMaker cm, Class<?>[] types, Class<?> makerClass) {
        MethodMaker mm = cm.addMethod(int.class, "hashCode");
        var hashVar = mm.var(int.class).set(new Random().nextInt());
        for (int i=0; i<types.length; i++) {
            if (i != 0) {
                hashVar.set(hashVar.mul(31));
            }
            Label skip = mm.label();
            var subHash = subHashCode(makerClass, mm.field(fieldName(i)).get(), skip);
            hashVar.set(hashVar.add(subHash));
            skip.here();
        }
        mm.return_(hashVar);
    }

    private static Variable subHashCode(Class<?> makerClass, Variable v, Label skip) {
        Class<?> type = v.classType();
        if (type == Object.class) {
            v.ifEq(null, skip);
            return v.methodMaker().var(makerClass).invoke("hashCode", v);
        } else if (type.isArray()) {
            String method = Object[].class.isAssignableFrom(type) ? "deepHashCode" : "hashCode";
            return v.methodMaker().var(Arrays.class).invoke(method, v);
        } else if (type.isPrimitive()) {
            return v.invoke("hashCode", v); // static method
        } else {
            v.ifEq(null, skip);
            return v.invoke("hashCode");
        }
    }

    private static void makeEquals(ClassMaker cm, Class<?>[] types, Class<?> makerClass) {
        MethodMaker mm = cm.addMethod(boolean.class, "equals", Object.class);
        var objVar = mm.param(0);

        {
            Label pass = mm.label();
            objVar.ifNe(mm.this_(), pass);
            mm.return_(true);
            pass.here();
        }

        Variable otherVar;
        {
            Label pass = mm.label();
            objVar.instanceOf(cm).ifTrue(pass);
            mm.return_(false);
            pass.here();
            otherVar = objVar.cast(cm);
        }

        for (int i=0; i<types.length; i++) {
            String fieldName = fieldName(i);
            subEquals(makerClass, mm.field(fieldName).get(), otherVar.field(fieldName).get(),
                      i == (types.length - 1));
        }

        mm.return_(true);
    }

    // Note: Both variables should have the same type.
    private static void subEquals(Class<?> makerClass, Variable v1, Variable v2, boolean last) {
        MethodMaker mm = v1.methodMaker();
        Label pass = mm.label();

        Class<?> type = v1.classType();
        if (type == Object.class) {
            Label v1NotNull = mm.label();
            v1.ifNe(null, v1NotNull);
            v2.ifEq(null, pass);
            mm.return_(false);
            v1NotNull.here();
            var result = mm.var(makerClass).invoke("equals", v1, v2);
            if (last) {
                mm.return_(result);
            } else {
                result.ifTrue(pass);
            }
        } else if (type.isArray()) {
            String method = Object[].class.isAssignableFrom(type) ? "deepEquals" : "equals";
            var result = mm.var(Arrays.class).invoke(method, v1, v2);
            if (last) {
                mm.return_(result);
            } else {
                result.ifTrue(pass);
            }
        } else if (type.isPrimitive()) {
            v1.ifEq(v2, pass);
        } else {
            Label v1NotNull = mm.label();
            v1.ifNe(null, v1NotNull);
            v2.ifEq(null, pass);
            mm.return_(false);
            v1NotNull.here();
            var result = v1.invoke("equals", v2);
            if (last) {
                mm.return_(result);
            } else {
                result.ifTrue(pass);
            }
        }

        mm.return_(false);

        pass.here();
    }

    private static void makeToString(ClassMaker cm, Class<?>[] types, Class<?> makerClass) {
        MethodMaker mm = cm.addMethod(String.class, "toString");

        if (types.length == 0) {
            mm.return_("()");
            return;
        }

        var toConcat = new Object[types.length * 2 + 1];

        toConcat[0] = '(';
        int pos = 1;

        for (int i=0; i<types.length; i++) {
            if (i != 0) {
                toConcat[pos++] = ", ";
            }
            toConcat[pos++] = subToString(makerClass, mm.field(fieldName(i)).get());
        }

        toConcat[pos] = ')';
        assert pos == toConcat.length - 1;

        mm.return_(mm.concat(toConcat));
    }

    private static Variable subToString(Class<?> makerClass, Variable v) {
        Class<?> type = v.classType();
        if (type == Object.class) {
            MethodMaker mm = v.methodMaker();
            var strVar = mm.var(String.class);
            Label notNull = mm.label();
            v.ifNe(null, notNull);
            strVar.set("null");
            Label cont = mm.label().goto_();
            notNull.here();
            strVar.set(mm.var(makerClass).invoke("toString", v));
            cont.here();
            return strVar;
        } else if (type.isArray()) {
            String method = Object[].class.isAssignableFrom(type) ? "deepToString" : "toString";
            return v.methodMaker().var(Arrays.class).invoke(method, v);
        } else if (type.isPrimitive()) {
            return v.invoke("toString", v); // static method
        } else {
            return v.methodMaker().var(String.class).invoke("valueOf", v);
        }
    }

    private static String fieldName(int i) {
        return String.valueOf(i);
    }
}
