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

package org.cojen.tupl.rows;

import java.util.Arrays;
import java.util.List;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.MethodMaker;

import org.cojen.tupl.Nullable;
import org.cojen.tupl.PrimaryKey;

/**
 * Makes row type interfaces suitable for use by custom Mappers and Groupers.
 *
 * @author Brian S. O'Neill
 */
public final class RowTypeMaker {
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

    // FIXME: Doesn't support nulls. Should I just use @Nullable for all objects?

    /**
     * @param keyTypes column types which belong to the primary key; can be null
     * @param valueTypes column types which aren't in the primary key; can be null
     */
    public static Result find(List<Class> keyTypes, List<Class> valueTypes) {
        return find(toArray(keyTypes), toArray(valueTypes));
    }

    private static Class[] toArray(List<Class> list) {
        return list == null || list.isEmpty() ? null : list.toArray(Class[]::new);
    }

    /**
     * Note: The given arrays will be modified.
     *
     * @param keyTypes column types which belong to the primary key; can be null
     * @param valueTypes column types which aren't in the primary key; can be null
     */
    private static Result find(Class[] keyTypes, Class[] valueTypes) {
        String[] keyNames, valueNames;

        Class[] allTypes;
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

        Arrays.sort(allTypes, (a, b) -> a.getName().compareTo(b.getName()));

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
            for (var method : rowType.getMethods()) {
                if (method.getParameterCount() == 0) {
                    Class type = method.getReturnType();
                    assign: for (int i=0; i<valueNames.length; i++) {
                        if (valueNames[i] == null) {
                            if (valueTypes[i] == type) {
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

    public static final String[] NO_NAMES = new String[0];

    private static final class BasicCacheKey {
        final Class[] allTypes;

        BasicCacheKey(Class[] allTypes) {
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
        final Class[] allTypes, keyTypes;

        /**
         * @param keyTypes must not be null
         */
        FullCacheKey(Class[] allTypes, Class[] keyTypes) {
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

    private static long cClassNum;

    private static Class make(Object cacheKey) {
        Class[] allTypes, keyTypes;

        if (cacheKey instanceof BasicCacheKey basic) {
            allTypes = basic.allTypes;
            keyTypes = null;
        } else {
            var full = (FullCacheKey) cacheKey;
            allTypes = full.allTypes;
            keyTypes = full.keyTypes;
        }

        long num;
        synchronized (RowTypeMaker.class) {
            num = cClassNum++;
        }

        // Use small sub packages to facilitate class unloading.
        String name = RowTypeMaker.class.getPackageName() + ".p" + (num / 10) + ".R" + (num % 10);

        ClassMaker cm = ClassMaker.beginExplicit(name, null, null).public_().interface_();

        cm.addAnnotation(Unpersisted.class, true);

        for (int i=0; i<allTypes.length; i++) {
            String colName = 'c' + String.valueOf(i);
            Class type = allTypes[i];

            MethodMaker mm = cm.addMethod(type, colName).public_().abstract_();
            if (!type.isPrimitive()) {
                // FIXME: testing
                mm.addAnnotation(Nullable.class, true);
            }

            cm.addMethod(null, colName, type).public_().abstract_();
        }

        if (keyTypes != null) {
            var keyNames = new String[keyTypes.length];
            allTypes = allTypes.clone();

            outer: for (int i=0; i<keyTypes.length; i++) {
                Class keyType = keyTypes[i];
                for (int j=0; j<allTypes.length; j++) {
                    if (allTypes[j] == keyType) {
                        keyNames[i] = String.valueOf(j);
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
