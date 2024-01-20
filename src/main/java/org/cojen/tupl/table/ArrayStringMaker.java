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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

import java.util.HashMap;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;

/**
 * Makes methods which serve the same function as Arrays.toString, except with more features.
 *
 * @author Brian S O'Neill
 */
class ArrayStringMaker {
    private static HashMap<Class<?>, MethodHandle> cSignedCache, cUnsignedCache;

    /**
     * Returns a MethodHandle with the signature:
     *
     *     StringBuilder append(StringBuilder builder, arrayType array, int limit)
     *
     * ...which returns a new StringBuilder or the given one, if it wasn't null.
     *
     * @param arrayType must be an array
     * @param unsigned true if primitive array is unsigned
     */
    synchronized static MethodHandle make(Class<?> arrayType, boolean unsigned) {
        Class<?> elementType = arrayType.getComponentType();
        if (!unsigned && !isPrimitiveElement(elementType)) {
            // Use the generic Object[] superclass, reducing the number of generated classes.
            int dims = 1;
            while ((elementType = elementType.getComponentType()) != null) {
                dims++;
            }
            arrayType = Object[].class;
            while (--dims > 0) {
                arrayType = arrayType.arrayType();
            }
        }

        HashMap<Class<?>, MethodHandle> cache;
        if (unsigned) {
            cache = cUnsignedCache;
            if (cache == null) {
                cUnsignedCache = cache = new HashMap<>();
            }
        } else {
            cache = cSignedCache;
            if (cache == null) {
                cSignedCache = cache = new HashMap<>();
            }
        }

        MethodHandle mh = cache.get(arrayType);

        if (mh == null) {
            mh = doMake(arrayType, unsigned);
            cache.put(arrayType, mh);
        }

        return mh;
    }

    private static boolean isPrimitiveElement(Class<?> elementType) {
        do {
            if (elementType.isPrimitive()) {
                return true;
            }
        } while ((elementType = elementType.getComponentType()) != null);
        return false;
    }

    private static MethodHandle doMake(Class<?> arrayType, boolean unsigned) {
        MethodMaker mm = MethodMaker.begin
            (MethodHandles.lookup(), StringBuilder.class, "append",
             StringBuilder.class, arrayType, int.class);

        final var builderVar = mm.param(0);
        final var arrayVar = mm.param(1);
        final var limitVar = mm.param(2);

        {
            Label notNull = mm.label();
            arrayVar.ifNe(null, notNull);
            Label builderNotNull = mm.label();
            builderVar.ifNe(null, builderNotNull);
            builderVar.set(mm.new_(StringBuilder.class, 4));
            builderNotNull.here();
            builderVar.invoke("append", "null");
            mm.return_(builderVar);
            notNull.here();
        }

        Label builderNotNull = mm.label();
        builderVar.ifNe(null, builderNotNull);
        var mathVar = mm.var(Math.class);
        var capacityVar = mathVar.invoke("min", limitVar, arrayVar.alength());
        capacityVar.set(mathVar.invoke("max", 16, capacityVar));
        builderVar.set(mm.new_(StringBuilder.class, capacityVar));
        builderNotNull.here();

        builderVar.invoke("append", '[');

        var ixVar = mm.var(int.class).set(0);
        Label start = mm.label().here();
        Label end = mm.label();
        ixVar.ifGe(arrayVar.alength(), end);
        Label cont = null;

        Label first = mm.label();
        ixVar.ifEq(0, first);
        builderVar.invoke("append", ", ");
        first.here();

        Label notLimit = mm.label();
        ixVar.ifLt(limitVar, notLimit);
        builderVar.invoke("append", "...");
        mm.goto_(end);
        notLimit.here();

        var elementVar = arrayVar.aget(ixVar);
        Class<?> elementType = elementVar.classType();

        if (elementType.isArray()) {
            mm.invoke(make(elementType, unsigned), builderVar, elementVar, limitVar);
        } else if (!unsigned) {
            builderVar.invoke("append", elementVar);
        } else {
            if (!elementType.isPrimitive()) {
                Label elementNotNull = mm.label();
                elementVar.ifNe(null, elementNotNull);
                builderVar.invoke("append", "null");
                cont = mm.label().goto_();
                elementNotNull.here();
                try {
                    elementVar = elementVar.unbox();
                    elementType = elementVar.classType();
                } catch (IllegalStateException e) {
                    // Cannot be unboxed.
                }
            }

            if (elementType == byte.class) {
                builderVar.invoke("append", elementVar.cast(int.class).and(0xff));
            } else if (elementType == short.class) {
                builderVar.invoke("append", elementVar.cast(int.class).and(0xffff));
            } else if (elementType == int.class) {
                builderVar.invoke("append", elementVar.cast(long.class).and(0xffff_ffffL));
            } else if (elementType == long.class) {
                var elementStrVar = mm.var(Long.class).invoke("toUnsignedString", elementVar);
                builderVar.invoke("append", elementStrVar);
            } else {
                builderVar.invoke("append", elementVar);
            }
        }

        if (cont != null) {
            cont.here();
        }

        ixVar.inc(1);
        mm.goto_(start);

        end.here();
        builderVar.invoke("append", ']');

        mm.return_(builderVar);

        return mm.finish();
    }
}
