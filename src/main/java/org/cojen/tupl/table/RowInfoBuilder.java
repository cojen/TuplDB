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

package org.cojen.tupl.table;

import java.lang.invoke.VarHandle;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.cojen.tupl.table.ColumnInfo.*;

/**
 * Allows a RowInfo instance to be made without a corresponding row type class.
 *
 * @author Brian S. O'Neill
 */
public class RowInfoBuilder {
    private final String mName;
    private final ArrayList<ColumnInfo> mColumns;

    private ArrayList<String> mPrimaryKey;

    private final ArrayList<ArrayList<String>> mAlternateKeys;
    private ArrayList<String> mCurrentAlternateKey;

    private final ArrayList<ArrayList<String>> mSecondaryIndexes;
    private ArrayList<String> mCurrentSecondaryIndex;

    /**
     * @param name fully qualified row type name
     */
    public RowInfoBuilder(String name) {
        mName = name.intern();
        mColumns = new ArrayList<>();
        mAlternateKeys = new ArrayList<>();
        mSecondaryIndexes = new ArrayList<>();
    }

    /**
     * Add all the columns, keys, and indexes from an existing RowInfo instance.
     */
    public void addAll(RowInfo info) {
        for (ColumnInfo ci : info.allColumns.values()) {
            if (ci.type == null) {
                ci = ci.copy();
                ci.assignType();
            }
            mColumns.add(ci);
        }

        for (String name : info.keyColumns.keySet()) {
            addToPrimaryKey(name);
        }

        for (ColumnSet set : info.alternateKeys) {
            for (String name : set.keyColumns.keySet()) {
                addToAlternateKey(name);
            }
            for (String name : set.valueColumns.keySet()) {
                addToAlternateKey(name);
            }
            finishAlternateKey();
        }

        for (ColumnSet set : info.secondaryIndexes) {
            for (String name : set.keyColumns.keySet()) {
                addToSecondaryIndex(name);
            }
            for (String name : set.valueColumns.keySet()) {
                addToSecondaryIndex(name);
            }
            finishSecondaryIndex();
        }
    }

    /**
     * @param typeCode see ColumnInfo.TYPE_*
     */
    public void addColumn(String name, int typeCode) {
        addColumn(name, typeCode, false, 0, 0);
    }

    /**
     * @param typeCode see ColumnInfo.TYPE_*
     * @param hidden true if column should be hidden
     */
    public void addColumn(String name, int typeCode, boolean hidden) {
        addColumn(name, typeCode, hidden, 0, 0);
    }

    /**
     * @param typeCode see ColumnInfo.TYPE_*
     * @param hidden true if column should be hidden
     * @param autoMin automatic column minimum value (inclusive)
     * @param autoMax automatic column maximum value (inclusive)
     */
    public void addColumn(String name, int typeCode, boolean hidden, long autoMin, long autoMax) {
        var ci = new ColumnInfo();
        ci.name = name.intern();
        ci.typeCode = typeCode & ~(TYPE_NULL_LOW | TYPE_DESCENDING);
        ci.hidden = hidden;
        ci.autoMin = autoMin;
        ci.autoMax = autoMax;
        ci.assignType();

        mColumns.add(ci);
    }

    /**
     * Adds a column to the primary key definition.
     *
     * @param name column name; can be prefixed with [ + | - ] [ ! ]
     */
    public void addToPrimaryKey(String name) {
        Objects.requireNonNull(name);
        ArrayList<String> list = mPrimaryKey;
        if (list == null) {
            mPrimaryKey = list = new ArrayList<>();
        }
        list.add(name);
    }

    /**
     * Adds a column to the current alternate key definition, creating a new one if necessary.
     *
     * @param name column name; can be prefixed with [ + | - ] [ ! ]
     */
    public void addToAlternateKey(String name) {
        Objects.requireNonNull(name);
        ArrayList<String> list = mCurrentAlternateKey;
        if (list == null) {
            mCurrentAlternateKey = list = new ArrayList<>();
            mAlternateKeys.add(list);
        }
        list.add(name);
    }

    /**
     * Finishes the current alternate key definition and allows a new one to be created.
     */
    public void finishAlternateKey() {
        mCurrentAlternateKey = null;
    }

    /**
     * Adds a column to the current secondary index definition, creating a new one if necessary.
     *
     * @param name column name; can be prefixed with [ + | - ] [ ! ]
     */
    public void addToSecondaryIndex(String name) {
        Objects.requireNonNull(name);
        ArrayList<String> list = mCurrentSecondaryIndex;
        if (list == null) {
            mCurrentSecondaryIndex = list = new ArrayList<>();
            mSecondaryIndexes.add(list);
        }
        list.add(name);
    }

    /**
     * Finishes the current secondary index definition and allows a new one to be created.
     */
    public void finishSecondaryIndex() {
        mCurrentSecondaryIndex = null;
    }

    /**
     * @throws IllegalArgumentException if the row type definition is malformed
     */
    public RowInfo build() {
        var info = new RowInfo(mName);
        var messages = new LinkedHashSet<String>(1);

        info.allColumns = new TreeMap<>();

        ColumnInfo autoColumn = null;

        for (ColumnInfo column : mColumns) {
            if (info.allColumns.putIfAbsent(column.name, column) != null) {
                messages.add("duplicate column \"" + column.name + '"');
            }

            long min = column.autoMin;
            long max = column.autoMax;

            if (min == max) {
                continue;
            }

            if (min > max) {
                messages.add("illegal automatic range [" + min + ", " + max + ']');
            }

            switch (column.unorderedTypeCode()) {
            case TYPE_UINT: case TYPE_ULONG: case TYPE_INT: case TYPE_LONG:
                if (autoColumn == null) {
                    autoColumn = column;
                } else {
                    messages.add("at most one column can be automatic");
                }
                break;
            default:
                messages.add("column \"" + column.type.getSimpleName() + ' ' + column.name +
                             "\" cannot be automatic");
            }
        }

        info.errorCheck(messages);

        info.examinePrimaryKey(mPrimaryKey == null ? (String[]) null
                               : mPrimaryKey.toArray(String[]::new), messages);

        info.errorCheck(messages);

        info.examineAutoColumn(messages, autoColumn);

        if (mAlternateKeys.isEmpty()) {
            info.alternateKeys = EmptyNavigableSet.the();
        } else {
            info.alternateKeys = new TreeSet<>(ColumnSetComparator.THE);
            for (ArrayList<String> names : mAlternateKeys) {
                info.examineIndex(messages, info.alternateKeys,
                                  names.toArray(String[]::new), true);
            }
        }

        if (mSecondaryIndexes.isEmpty()) {
            info.secondaryIndexes = EmptyNavigableSet.the();
        } else {
            info.secondaryIndexes = new TreeSet<>(ColumnSetComparator.THE);
            for (ArrayList<String> names : mSecondaryIndexes) {
                info.examineIndex(messages, info.secondaryIndexes,
                                  names.toArray(String[]::new), false);
            }
        }

        info.errorCheck(messages);

        info.alternateKeys = info.finishIndexSet(info.alternateKeys);
        info.secondaryIndexes = info.finishIndexSet(info.secondaryIndexes);

        // This is necessary because most of the RowInfo fields aren't final, and the RowInfo
        // object might be stored into a cache which doesn't require synchronized access.
        VarHandle.storeStoreFence();

        return info;
    }
}
