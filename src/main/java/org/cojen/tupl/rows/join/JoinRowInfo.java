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

package org.cojen.tupl.rows.join;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.cojen.tupl.AlternateKey;
import org.cojen.tupl.Automatic;
import org.cojen.tupl.Hidden;
import org.cojen.tupl.PrimaryKey;
import org.cojen.tupl.SecondaryIndex;
import org.cojen.tupl.Unsigned;

import org.cojen.tupl.rows.ColumnInfo;
import org.cojen.tupl.rows.CompareUtils;
import org.cojen.tupl.rows.RowInfo;
import org.cojen.tupl.rows.WeakClassCache;

import static org.cojen.tupl.rows.ColumnInfo.*;

/**
 * A join row type can only reference other row types, and it doesn't represent something which
 * can be directly persisted.
 *
 * @author Brian S O'Neill
 */
public final class JoinRowInfo {
    private static final WeakClassCache<JoinRowInfo> cache = new WeakClassCache<>();

    private static Set<Class<?>> examining;

    /**
     * Returns a new or cached instance.
     *
     * @throws IllegalArgumentException if row type is malformed
     */
    public static JoinRowInfo find(Class<?> joinType) {
        JoinRowInfo info = cache.get(joinType);

        if (info == null) {
            synchronized (cache) {
                info = cache.get(joinType);
                if (info == null) {
                    if (examining == null) {
                        examining = new HashSet<>();
                    }
                    if (!examining.add(joinType)) {
                        throw new IllegalArgumentException("Recursive join");
                    }
                    try {
                        info = examine(joinType);
                        cache.put(joinType, info);
                    } finally {
                        examining.remove(joinType);
                        if (examining.isEmpty()) {
                            examining = null;
                        }
                    }
                }
            }
        }

        return info;
    }

    /**
     * @throws IllegalArgumentException if row type is malformed
     */
    private static JoinRowInfo examine(Class<?> joinType) {
        var messages = new LinkedHashSet<String>(1);

        if (!joinType.isInterface()) {
            messages.add("must be an interface");
            errorCheck(joinType, messages);
        }

        if (joinType.isAnnotationPresent(PrimaryKey.class) ||
            joinType.isAnnotationPresent(AlternateKey.class) ||
            joinType.isAnnotationPresent(SecondaryIndex.class))
        {
            messages.add("cannot define a key or secondary index");
        }

        var allColumns = new TreeMap<String, JoinColumnInfo>();
        examineAllColumns(allColumns, joinType, messages);
        if (allColumns.isEmpty()) {
            messages.add("no columns are defined");
        }
        errorCheck(joinType, messages);

        if (CompareUtils.needsCompareTo(joinType)) {
            StringBuilder b = null;

            for (ColumnInfo info : allColumns.values()) {
                if (!Comparable.class.isAssignableFrom(info.type)) {
                    if (b == null) {
                        b = new StringBuilder();
                        b.append("extends Comparable but not all columns are Comparable: ");
                        b.append(info.name);
                    } else {
                        b.append(", ").append(info.name);
                    }
                }
            }

            if (b != null) {
                messages.add(b.toString());
                errorCheck(joinType, messages);
            }
        }

        return new JoinRowInfo(joinType.getName(), allColumns);
    }    

    /**
     * @param allColumns results stored here
     */
    private static void examineAllColumns(NavigableMap<String, JoinColumnInfo> allColumns,
                                          Class<?> joinType, Set<String> messages)
    {
        for (Method method : joinType.getMethods()) {
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
                        if (name.equals("hashCode") || name.equals("toString")) {
                            // Inherited non-final method declared in Object.
                            continue;
                        }
                        if (name.equals("clone") && type.isAssignableFrom(joinType)) {
                            // Inherited non-final method declared in Object.
                            continue;
                        }
                        info = addColumn(allColumns, messages, name, type);
                        if (info == null) {
                            continue;
                        }
                        if (info.accessor != null) {
                            messages.add("duplicate accessor method \"" + name + '"');
                            continue;
                        }
                        info.accessor = method;
                        break lookup;
                    } else if (params.length == 1) {
                        if (name.equals("equals") && params[0] == Object.class) {
                            // Inherited non-final method declared in Object.
                            continue;
                        }
                        if (name.equals("compareTo") && type == int.class &&
                            (params[0] == Object.class || params[0] == joinType) &&
                            Comparable.class.isAssignableFrom(joinType))
                        {
                            // Inherited method from Comparable interface.
                            continue;
                        }
                    }
                } else {
                    if (params.length == 1) {
                        type = params[0];
                        info = addColumn(allColumns, messages, name, type);
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

            info.hidden |= method.isAnnotationPresent(Hidden.class);

            if (method.isAnnotationPresent(Unsigned.class)) {
                messages.add("column \"" + info.type.getSimpleName() + ' ' + info.name +
                             "\" cannot be unsigned");
            }

            if (method.isAnnotationPresent(Automatic.class)) {
                messages.add("column \"" + info.type.getSimpleName() + ' ' + info.name +
                             "\" cannot be automatic");
            }
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
    private static ColumnInfo addColumn(NavigableMap<String, JoinColumnInfo> allColumns,
                                        Set<String> messages, String name, Class<?> type)
    {
        if (!typeCheck(messages, name, type)) {
            return null;
        }

        JoinColumnInfo info = allColumns.get(name);

        if (info == null) {
            name = name.intern();
            info = new JoinColumnInfo();
            info.name = name;
            info.type = type;
            info.typeCode = TYPE_OBJECT;
            allColumns.put(name, info);
        } else if (info.type != type) {
            messages.add("column \"" + info.type.getSimpleName() + ' ' + info.name +
                         "\" doesn't match type \"" + type.getSimpleName() + '"');
            info = null;
        }

        return info;
    }

    /**
     * @return false if illegal
     */
    private static boolean typeCheck(Set<String> messages, String name, Class<?> type) {
        String msg;

        if (!type.isInterface()) {
            msg = "must be a row type interface";
        } else {
            try {
                if (type.isAnnotationPresent(PrimaryKey.class)) {
                    RowInfo.find(type);
                } else {
                    JoinRowInfo.find(type);
                }
                return true;
            } catch (IllegalArgumentException e) {
                msg = e.getMessage();
                if (msg == null || msg.isEmpty()) {
                    msg = e.toString();
                }
            }
        }

        msg = "column \"" + type.getSimpleName() + ' ' +
                name + "\" has an unsupported type" + ": " + msg;
        messages.add(msg);

        return false;
    }

    // Fully qualified join row type name.
    final String name;

    // Map is ordered lexicographically by name.
    final NavigableMap<String, JoinColumnInfo> allColumns;

    private JoinRowInfo(String name, NavigableMap<String, JoinColumnInfo> allColumns) {
        this.name = name;
        this.allColumns = allColumns;
    }

    @Override
    public String toString() {
        return "name: " + name + ", allColumns: " + allColumns;
    }

    private static void errorCheck(Class<?> joinType, Set<String> messages) {
        RowInfo.errorCheck("Join row type", joinType, messages);
    }
}
