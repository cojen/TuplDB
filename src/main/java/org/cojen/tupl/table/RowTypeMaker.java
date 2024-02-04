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

package org.cojen.tupl.table;

import java.lang.reflect.Method;

import java.util.Arrays;
import java.util.List;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.MethodMaker;

import org.cojen.tupl.Nullable;
import org.cojen.tupl.PrimaryKey;

/**
 * Makes row type interfaces suitable for use by custom Mappers and Aggregators.
 *
 * @author Brian S. O'Neill
 */
public final class RowTypeMaker {
    /**
     * Returns an object for representing a column described just by a class.
     */
    public static Object columnType(Class clazz) {
        return clazz;
    }

    /**
     * Returns an object for representing a column described by a class and a ColumnInfo type
     * code. Only the type code modifiers are considered.
     */
    public static Object columnType(Class clazz, int typeCode) {
        int modifiers = ColumnInfo.modifiers(typeCode);
        return modifiers == 0 ? clazz : new Type(clazz, modifiers);
    }

    private record Type(Class clazz, int modifiers) {
        boolean isNullable() {
            return ColumnInfo.isNullable(modifiers) && !clazz.isPrimitive();
        }

        void addAnnotations(MethodMaker mm) {
            if (isNullable()) {
                mm.addAnnotation(Nullable.class, true);
            }
        }

        boolean matchesAnnotations(Method method) {
            if (isNullable() != method.isAnnotationPresent(Nullable.class)) {
                return false;
            }
            return true;
        }

        /**
         * @return true if the method has any annotations which would affect the type
         */
        static boolean hasAnnotations(Method method) {
            return method.isAnnotationPresent(Nullable.class);
        }
    }

    /**
     * @param rowType generated row type interface
     * @param keyNames column names associated with the requested key column types
     * @param valueNames column names associated with the requested value column types
     */
    public record Result(Class rowType, String[] keyNames, String[] valueNames) {
        public String[] allNames() {
            if (keyNames.length == 0) {
                return valueNames;
            }
            if (valueNames.length == 0) {
                return keyNames;
            }
            var all = new String[keyNames.length + valueNames.length];
            System.arraycopy(keyNames, 0, all, 0, keyNames.length);
            System.arraycopy(valueNames, 0, all, keyNames.length, valueNames.length);
            return all;
        }
    }

    /**
     * @param keyTypes column types which belong to the primary key; can be null
     * @param valueTypes column types which aren't in the primary key; can be null
     */
    public static Result find(List<Object> keyTypes, List<Object> valueTypes) {
        return find(toArray(keyTypes), toArray(valueTypes));
    }

    private static Object[] toArray(List<Object> list) {
        return list == null || list.isEmpty() ? null : list.toArray(Object[]::new);
    }

    /**
     * Note: The given arrays will be modified.
     *
     * @param keyTypes column types which belong to the primary key; can be null
     * @param valueTypes column types which aren't in the primary key; can be null
     */
    private static Result find(Object[] keyTypes, Object[] valueTypes) {
        String[] keyNames, valueNames;

        Object[] allTypes;
        if (keyTypes == null || keyTypes.length == 0) {
            keyNames = NO_NAMES;
            if (valueTypes == null || valueTypes.length == 0) {
                return new Result(IdentityTable.Row.class, keyNames, NO_NAMES);
            }
            keyTypes = null;
            valueNames = new String[valueTypes.length];
            allTypes = valueTypes.clone();
        } else {
            keyNames = null;
            if (valueTypes == null || valueTypes.length == 0) {
                valueNames = NO_NAMES;
                allTypes = keyTypes.clone();
            } else {
                valueNames = new String[valueTypes.length];
                allTypes = new Class[keyTypes.length + valueTypes.length];
                System.arraycopy(keyTypes, 0, allTypes, 0, keyTypes.length);
                System.arraycopy(valueTypes, 0, allTypes, keyTypes.length, valueTypes.length);
            }
        }

        Arrays.sort(allTypes, (a, b) -> {
            int cmp = typeClass(a).getName().compareTo(typeClass(b).getName());
            if (cmp == 0) {
                cmp = Integer.compare(typeModifiers(a), typeModifiers(b));
            }
            return cmp;
        });

        Object cacheKey;
        if (keyTypes == null) {
            cacheKey = new BasicCacheKey(allTypes);
        } else {
            cacheKey = new FullCacheKey(allTypes, keyTypes);
        }

        Class<?> rowType = cCache.obtain(cacheKey, null);

        if (keyNames == null) {
            keyNames = rowType.getAnnotation(PrimaryKey.class).value();
        }

        if (valueNames.length > 0) {
            Method[] methods = rowType.getMethods();
            Arrays.sort(methods, (a, b) -> a.getName().compareTo(b.getName()));

            for (var method : methods) {
                if (method.getParameterCount() == 0) {
                    assign: for (int i=0; i<valueNames.length; i++) {
                        if (valueNames[i] == null) {
                            if (typeMatches(valueTypes[i], method)) {
                                String name = method.getName();
                                if (keyNames != null) {
                                    for (String kn : keyNames) {
                                        if (name.equals(kn)) {
                                            continue assign;
                                        }
                                    }
                                }
                                valueNames[i] = name;
                                break;
                            }
                        }
                    }
                }
            }
        }

        return new Result(rowType, keyNames, valueNames);
    }

    private static Class typeClass(Object typeObj) {
        if (typeObj instanceof Class clazz) {
            return clazz;
        } else {
            return ((Type) typeObj).clazz;
        }
    }

    private static int typeModifiers(Object typeObj) {
        if (typeObj instanceof Class clazz) {
            return 0;
        } else {
            return ((Type) typeObj).modifiers;
        }
    }

    /**
     * @param method must have a non-void return type
     */
    private static boolean typeMatches(Object typeObj, Method method) {
        Class returnType = method.getReturnType();
        if (typeObj instanceof Class clazz) {
            return clazz == returnType && !Type.hasAnnotations(method);
        } else {
            var type = (Type) typeObj;
            return type.clazz == returnType && type.matchesAnnotations(method);
        }
    }

    public static final String[] NO_NAMES = new String[0];

    private static final class BasicCacheKey {
        final Object[] allTypes;

        BasicCacheKey(Object[] allTypes) {
            this.allTypes = allTypes;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(allTypes);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof BasicCacheKey other
                && Arrays.equals(allTypes, other.allTypes);
        }
    }

    private static final class FullCacheKey {
        final Object[] allTypes, keyTypes;

        /**
         * @param keyTypes must not be null
         */
        FullCacheKey(Object[] allTypes, Object[] keyTypes) {
            this.allTypes = allTypes;
            this.keyTypes = keyTypes;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(allTypes) + Arrays.hashCode(keyTypes);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof FullCacheKey other
                && Arrays.equals(allTypes, other.allTypes)
                && Arrays.equals(keyTypes, other.keyTypes);
        }
    }

    private static final WeakCache<Object, Class, Object> cCache = new WeakCache<>() {
        @Override
        public Class newValue(Object cacheKey, Object unused) {
            return make(cacheKey);
        }
    };

    private static Class make(Object cacheKey) {
        Object[] allTypes, keyTypes;

        if (cacheKey instanceof BasicCacheKey basic) {
            allTypes = basic.allTypes;
            keyTypes = null;
        } else {
            var full = (FullCacheKey) cacheKey;
            allTypes = full.allTypes;
            keyTypes = full.keyTypes;
        }

        // Use sub packages to facilitate class unloading.
        String name = RowTypeMaker.class.getPackageName() + '.' + RowGen.newSubPackage() + ".Type";

        ClassMaker cm = ClassMaker.begin(name, null, RowGen.MAKER_KEY);
        cm.public_().interface_();

        // Make synthetic in order for new classes generated via RowGen.beginClassMaker to use
        // the same ClassLoader as the row type interface.
        cm.synthetic();

        cm.addAnnotation(Unpersisted.class, true);

        for (int i=0; i<allTypes.length; i++) {
            String colName = 'c' + String.valueOf(i);
            Object typeObj = allTypes[i];

            Type type;
            Class clazz;

            if (typeObj instanceof Class c) {
                type = null;
                clazz = c;
            } else {
                type = (Type) typeObj;
                clazz = type.clazz;
            }

            MethodMaker mm = cm.addMethod(clazz, colName).public_().abstract_();

            if (type != null) {
                type.addAnnotations(mm);
            }

            cm.addMethod(null, colName, clazz).public_().abstract_();
        }

        if (keyTypes != null) {
            var keyNames = new String[keyTypes.length];
            allTypes = allTypes.clone();

            outer: for (int i=0; i<keyTypes.length; i++) {
                Object keyType = keyTypes[i];
                for (int j=0; j<allTypes.length; j++) {
                    if (allTypes[j] == keyType) {
                        keyNames[i] = 'c' + String.valueOf(j);
                        allTypes[i] = null; // consumed
                        continue outer;
                    }
                }
                throw new AssertionError();
            }

            cm.addAnnotation(PrimaryKey.class, true).put("value", keyNames);
        }

        return cm.finish();
    }
}
