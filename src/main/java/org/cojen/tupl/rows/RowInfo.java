/*
 *  Copyright 2021 Cojen.org
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
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.rows;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.util.Collections;
import java.util.Iterator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.cojen.tupl.AlternateKey;
import org.cojen.tupl.Hidden;
import org.cojen.tupl.Nullable;
import org.cojen.tupl.PrimaryKey;
import org.cojen.tupl.SecondaryIndex;
import org.cojen.tupl.Unsigned;

import static org.cojen.tupl.rows.ColumnInfo.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class RowInfo extends ColumnSet {
    private static final WeakCache<Class<?>, RowInfo> cache = new WeakCache<>();

    /**
     * Returns a new or cached instance.
     *
     * @throws IllegalArgumentException if row type is malformed
     */
    static RowInfo find(Class<?> rowType) {
        RowInfo info = cache.get(rowType);
        if (info == null) {
            synchronized (cache) {
                info = cache.get(rowType);
                if (info == null) {
                    info = examine(rowType);
                    cache.put(rowType, info);
                }
            }
        }
        return info;
    }

    /**
     * @throws IllegalArgumentException if row type is malformed
     */
    private static RowInfo examine(Class<?> rowType) {
        var messages = new LinkedHashSet<String>(1);

        if (!rowType.isInterface()) {
            messages.add("must be an interface");
            errorCheck(rowType, messages);
        }

        var info = new RowInfo(rowType.getName());

        info.examineAllColumns(rowType, messages);
        errorCheck(rowType, messages);

        info.examinePrimaryKey(rowType, messages);
        errorCheck(rowType, messages);

        AlternateKey altKey = rowType.getAnnotation(AlternateKey.class);
        AlternateKey.Set altKeySet = rowType.getAnnotation(AlternateKey.Set.class);
        if (altKey == null && altKeySet == null) {
            info.alternateKeys = Collections.emptyNavigableSet();
        } else {
            info.alternateKeys = new TreeSet<>(ColumnSetComparator.THE);
            if (altKey != null) {
                info.examineIndex(messages, info.alternateKeys, altKey.value(), true);
            }
            if (altKeySet != null) {
                for (AlternateKey alt : altKeySet.value()) {
                    info.examineIndex(messages, info.alternateKeys, alt.value(), true);
                }
            }
        }

        SecondaryIndex index = rowType.getAnnotation(SecondaryIndex.class);
        SecondaryIndex.Set indexSet = rowType.getAnnotation(SecondaryIndex.Set.class);
        if (index == null && indexSet == null) {
            info.secondaryIndexes = Collections.emptyNavigableSet();
        } else {
            info.secondaryIndexes = new TreeSet<>(ColumnSetComparator.THE);
            // Add the primary key initially, which will be removed by reduction.
            info.secondaryIndexes.add(info);
            if (index != null) {
                info.examineIndex(messages, info.secondaryIndexes, index.value(), false);
            }
            if (indexSet != null) {
                for (SecondaryIndex ix : indexSet.value()) {
                    info.examineIndex(messages, info.secondaryIndexes, ix.value(), false);
                }
            }
        }

        errorCheck(rowType, messages);

        info.alternateKeys = info.finishIndexSet(info.alternateKeys);
        info.secondaryIndexes = info.finishIndexSet(info.secondaryIndexes);

        return info;
    }

    // Fully qualified row type name.
    final String name;

    // Sets are reduced and contain all the necessary columns to retrieve by primary key.
    NavigableSet<ColumnSet> alternateKeys;

    // Sets are reduced and contain all the necessary columns to retrieve by primary key.
    NavigableSet<ColumnSet> secondaryIndexes;

    private volatile RowGen mRowGen;

    RowInfo(String name) {
        this.name = name;
    }

    /**
     * Returns a new or cached RowGen instance.
     */
    RowGen rowGen() {
        RowGen gen = mRowGen;
        if (gen == null) {
            synchronized (this) {
                gen = mRowGen;
                if (gen == null) {
                    mRowGen = gen = new RowGen(this);
                }
            }
        }
        return gen;
    }

    boolean alternateKeysMatch(RowInfo other) {
        return matches(alternateKeys, other.alternateKeys);
    }

    boolean secondaryIndexesMatch(RowInfo other) {
        return matches(secondaryIndexes, other.secondaryIndexes);
    }

    static boolean matches(NavigableSet<ColumnSet> a, NavigableSet<ColumnSet> b) {
        var ia = a.iterator();
        var ib = b.iterator();
        while (ia.hasNext()) {
            if (!ib.hasNext() || !ia.next().matches(ib.next())) {
                return false;
            }
        }
        return !ib.hasNext();
    }

    @Override
    public String toString() {
        return "name: " + name + ", " + super.toString() +
            ", alternateKeys: " + alternateKeys + ", secondaryIndexes: " + secondaryIndexes;
    }

    private static void errorCheck(Class<?> rowType, Set<String> messages) {
        if (!messages.isEmpty()) {
            var bob = new StringBuilder();
            bob.append("Row type \"").append(rowType.getSimpleName()).append("\" is malformed: ");
            final int length = bob.length();
            for (String message : messages) {
                if (bob.length() > length) {
                    bob.append("; ");
                }
                bob.append(message);
            }
            throw new IllegalArgumentException(bob.toString());
        }
    }

    private void examineAllColumns(Class<?> rowType, Set<String> messages) {
        allColumns = new TreeMap<>();

        for (Method method : rowType.getMethods()) {
            if (method.isDefault()) {
                continue;
            }
            int modifiers = method.getModifiers();
            if (Modifier.isStatic(modifiers) || !Modifier.isAbstract(modifiers)) {
                continue;
            }

            String name = method.getName();
            Class<?> type = method.getReturnType();
            Class<?>[] params = method.getParameterTypes();

            ColumnInfo info;

            lookup: {
                if (type != void.class) {
                    if (params.length == 0) {
                        if (name.equals("clone") ||
                            name.equals("hashCode") || name.equals("toString"))
                        {
                            // Inherited non-final method declared in Object.
                            continue;
                        }
                        info = addColumn(messages, name, type);
                        if (info == null) {
                            continue;
                        }
                        if (info.accessor != null) {
                            messages.add("duplicate accessor method \"" + name + '"');
                            continue;
                        }
                        info.accessor = method;
                        break lookup;
                    }
                } else {
                    if (params.length == 1) {
                        type = params[0];
                        info = addColumn(messages, name, type);
                        if (info == null) {
                            continue;
                        }
                        if (info.mutator != null) {
                            messages.add("duplicate mutator method \"" + name + '"');
                            continue;
                        }
                        info.mutator = method;
                        break lookup;
                    }
                }

                messages.add("unsupported method \"" + name + '"');
                continue;
            }

            if (method.isAnnotationPresent(Nullable.class)) {
                if (info.type.isPrimitive()) {
                    messages.add("column \"" + info.type.getSimpleName() + ' ' + info.name +
                                 "\" cannot be nullable");
                } else {
                    info.typeCode |= TYPE_NULLABLE;
                }
            }

            if (method.isAnnotationPresent(Unsigned.class)) {
                int typeCode = info.plainTypeCode();
                if (typeCode >= 0b000_10000 || typeCode == TYPE_BOOLEAN) {
                    messages.add("column \"" + info.type.getSimpleName() + ' ' + info.name +
                                 "\" cannot be unsigned");
                } else {
                    info.typeCode &= ~0b000_01000;
                }
            }

            info.hidden |= method.isAnnotationPresent(Hidden.class);
        }

        for (ColumnInfo info : allColumns.values()) {
            if (info.accessor == null) {
                messages.add("no accessor method for column \"" + info.name + '"');
            } else if (info.mutator == null) {
                messages.add("no mutator method for column \"" + info.name + '"');
            }
        }
    }

    /**
     * @return null if illegal
     */
    private ColumnInfo addColumn(Set<String> messages, String name, Class<?> type) {
        int typeCode = selectTypeCode(messages, name, type);

        ColumnInfo info = allColumns.get(name);

        if (info == null) {
            name = name.intern();
            info = new ColumnInfo();
            info.name = name;
            info.type = type;
            info.typeCode = typeCode;
            allColumns.put(name, info);
        } else if (info.type != type) {
            messages.add("column \"" + info.type.getSimpleName() + ' ' + info.name +
                         "\" doesn't match type \"" + type.getSimpleName() + '"');
            info = null;
        }

        return info;
    }

    /**
     * @return -1 if unsupported
     */
    private static int selectTypeCode(Set<String> messages, String name, Class<?> type) {
        if (type.isPrimitive()) {
            if (type == int.class) {
                return TYPE_INT;
            } else if (type == long.class) {
                return TYPE_LONG;
            } else if (type == boolean.class) {
                return TYPE_BOOLEAN;
            } else if (type == double.class) {
                return TYPE_DOUBLE;
            } else if (type == float.class) {
                return TYPE_FLOAT;
            } else if (type == byte.class) {
                return TYPE_BYTE;
            } else if (type == char.class) {
                return TYPE_CHAR;
            } else if (type == short.class) {
                return TYPE_SHORT;
            }
        } else if (type.isArray()) {
            Class<?> subType = type.getComponentType();
            if (!subType.isArray()) {
                int typeCode = selectTypeCode(null, name, subType);
                if (typeCode != -1 && isPrimitive(typeCode)) {
                    return typeCode | TYPE_ARRAY;
                }
            }
        } else if (type == String.class) {
            return TYPE_UTF8;
        } else if (type == BigInteger.class) {
            return TYPE_BIG_INTEGER;
        } else if (type == BigDecimal.class) {
            return TYPE_BIG_DECIMAL;
        } else if (type == Integer.class) {
            return TYPE_INT;
        } else if (type == Long.class) {
            return TYPE_LONG;
        } else if (type == Boolean.class) {
            return TYPE_BOOLEAN;
        } else if (type == Double.class) {
            return TYPE_DOUBLE;
        } else if (type == Float.class) {
            return TYPE_FLOAT;
        } else if (type == Byte.class) {
            return TYPE_BYTE;
        } else if (type == Character.class) {
            return TYPE_CHAR;
        } else if (type == Short.class) {
            return TYPE_SHORT;
        }

        if (messages != null) {
            messages.add("column \"" + type.getSimpleName() + ' ' + name +
                         "\" has an unsupported type");
        }

        return -1;
    }

    private void examinePrimaryKey(Class<?> rowType, Set<String> messages) {
        PrimaryKey pk = rowType.getAnnotation(PrimaryKey.class);

        if (pk == null) {
            messages.add("no PrimaryKey annotation is present");
            return;
        }

        String[] columnNames = pk.value();

        if (columnNames.length == 0) {
            messages.add(noColumns("primary key"));
            return;
        }

        if (columnNames.length > 1) {
            keyColumns = new LinkedHashMap<>(columnNames.length << 1);
        }

        for (String name : columnNames) {
            boolean descending = false;

            if (name.startsWith("+")) {
                name = name.substring(1);
            } else if (name.startsWith("-")) {
                name = name.substring(1);
                descending = true;
            }

            ColumnInfo info = allColumns.get(name);

            if (info == null) {
                messages.add(notExist("primary key", name));
                return;
            }

            // Use intern'd string.
            name = info.name;

            if (descending) {
                info.typeCode |= TYPE_DESCENDING;
            }

            if (keyColumns == null) {
                keyColumns = Map.of(name, info);
            } else if (keyColumns.put(name, info) != null) {
                messages.add(duplicate("primary key", name));
                return;
            }
        }

        valueColumns = new TreeMap<>();

        for (var entry : allColumns.entrySet()) {
            String name = entry.getKey();
            if (!keyColumns.containsKey(name)) {
                valueColumns.put(name, entry.getValue());
            }
        }
    }

    /**
     * Examines an alternate key or secondary index and fills in the key and data columns.
     *
     * @param fullSet result is added here
     */
    private void examineIndex(Set<String> messages, NavigableSet<ColumnSet> fullSet,
                              String[] columnNames, boolean forAltKey)
    {
        if (columnNames.length == 0) {
            messages.add(noColumns(forAltKey ? "alternate key" : "secondary index"));
            return;
        }

        var set = new ColumnSet();

        if (!forAltKey || columnNames.length > 1) {
            set.keyColumns = new LinkedHashMap<>(columnNames.length << 1);
        }

        for (String name : columnNames) {
            Boolean descending = null;

            if (name.startsWith("+")) {
                name = name.substring(1);
                descending = false;
            } else if (name.startsWith("-")) {
                name = name.substring(1);
                descending = true;
            }

            ColumnInfo info = allColumns.get(name);

            if (info == null) {
                messages.add(notExist(forAltKey ? "alternate key" : "secondary index", name));
                return;
            }

            // Use intern'd string.
            name = info.name;

            if (descending == null) {
                // Initially use an unspecified type to aid with index set reduction.
                info = withUnspecifiedType(info);
            } else if (descending) {
                if (!info.isDescending()) {
                    info = info.copy();
                    info.typeCode |= TYPE_DESCENDING;
                }
            } else {
                if (info.isDescending()) {
                    info = info.copy();
                    info.typeCode &= ~TYPE_DESCENDING;
                }
            }

            if (set.keyColumns == null) {
                set.keyColumns = new HashMap<>(1); // might be modified during index set reduction
                set.keyColumns.put(name, info);
            } else if (set.keyColumns.put(name, info) != null) {
                messages.add(duplicate(forAltKey ? "alternate key" : "secondary index", name));
                return;
            }
        }

        boolean hasFullPrimaryKey = set.keyColumns.keySet().containsAll(keyColumns.keySet());

        if (forAltKey) {
            if (hasFullPrimaryKey) {
                // Alternate key is effectively a secondary index and doesn't affect uniqueness.
                messages.add("alternate key contains all columns of the primary key");
                return;
            }

            // Add the remaining primary key columns as data columns.

            set.valueColumns = new TreeMap<>();

            for (ColumnInfo pkColumn : keyColumns.values()) {
                if (!set.keyColumns.containsKey(pkColumn.name)) {
                    // Initially use an unspecified type to aid with index set reduction.
                    set.valueColumns.put(pkColumn.name, withUnspecifiedType(pkColumn));
                }
            }
        } else {
            if (hasFullPrimaryKey) {
                // Move columns after the last primary key column to the data column set. This
                // helps optimize covering indexes because it doesn't use key encoding format
                // for everything.

                int remaining = keyColumns.size();
                Iterator<Map.Entry<String, ColumnInfo>> it = set.keyColumns.entrySet().iterator();
                while (it.hasNext()) {
                    var entry = it.next();
                    if (remaining > 0) {
                        if (keyColumns.containsKey(entry.getKey())) {
                            remaining--;
                        }
                    } else {
                        it.remove();
                        if (set.valueColumns == null) {
                            set.valueColumns = new TreeMap<>();
                        }
                        set.valueColumns.put(entry.getKey(), entry.getValue());
                    }
                }
            } else {
                // Add the remaining primary key columns as index key columns.

                for (ColumnInfo pkColumn : keyColumns.values()) {
                    if (!set.keyColumns.containsKey(pkColumn.name)) {
                        // Initially use an unspecified type to aid with index set reduction.
                        set.keyColumns.put(pkColumn.name, withUnspecifiedType(pkColumn));
                    }
                }
            }

            if (set.valueColumns == null) {
                set.valueColumns = Collections.emptyNavigableMap();
            }
        }

        fullSet.add(set);
    }

    private static String notExist(String prefix, String name) {
        return prefix + " refers to a column that doesn't exist: " + name;
    }

    private static String duplicate(String prefix, String name) {
        return prefix + " refers to a column more than once: " + name;
    }

    private static String noColumns(String prefix) {
        return prefix + " doesn't specify any columns";
    }

    private static ColumnInfo withUnspecifiedType(ColumnInfo info) {
        info = info.copy();
        info.typeCode = -1;
        return info;
    }

    /**
     * Reduces a set of alternate keys or secondary indexes by examining explicit column sort
     * directions. Initially, primary key columns should have been added with an unspecified
     * type and direction. The given set must be ordered with ColumnSetComparator.THE.
     */
    private NavigableSet<ColumnSet> finishIndexSet(NavigableSet<ColumnSet> initialSet) {
        // All unspecified types are currently ordered lower. By iterating in reverse order
        // into a new set which treats unspecified types as equal, unspecified ones are less
        // favored and will be effectively discarded if they're redundant.

        var nextSet = new TreeSet<ColumnSet>(new ColumnSetComparator(true));
        nextSet.addAll(initialSet.descendingSet());

        // Add the results into a final set, with specified orderings.

        var finalSet = new TreeSet<ColumnSet>(ColumnSetComparator.THE);
        Iterator<ColumnSet> it = nextSet.iterator();
        while (it.hasNext()) {
            ColumnSet set = it.next();
            it.remove(); // remove it before fixing the types and messing things up

            fixTypes(set.keyColumns);
            fixTypes(set.valueColumns);

            set.allColumns = new TreeMap<>();
            set.allColumns.putAll(set.keyColumns);
            set.allColumns.putAll(set.valueColumns);

            finalSet.add(set);
        }

        // Remove the primary key, in case it was in the initial set (secondary indexes do this).
        finalSet.remove(this);

        return finalSet;
    }

    private void fixTypes(Map<String, ColumnInfo> columns) {
        for (Map.Entry<String, ColumnInfo> entry : columns.entrySet()) {
            ColumnInfo info = entry.getValue();
            if (info.typeCode == -1) {
                entry.setValue(allColumns.get(info.name));
            }
        }
    }
}
