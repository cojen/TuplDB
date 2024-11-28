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

import java.util.*;

import java.util.concurrent.ThreadLocalRandom;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import org.cojen.tupl.core.LocalDatabase;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RowStoreTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RowStoreTest.class.getName());
    }

    @Test
    public void randomRowInfos() throws Exception {
        for (int i=0; i<10; i++) {
            long seed = ThreadLocalRandom.current().nextLong();
            try {
                randomRowInfos(seed);
            } catch (Throwable e) {
                throw new AssertionError("seed: " + seed, e);
            }
        }
    }

    private void randomRowInfos(long seed) throws Exception {
        final var rnd = new Random(seed);

        final Database db = Database.open(new DatabaseConfig());
        final Index ix = db.openIndex("test");
        final RowStore rs = new RowStore((LocalDatabase) db, db.openIndex("Schemata"));

        final Map<String, ColumnInfo> keyColumns = randomKeyColumns(0, rnd);
        final boolean withAltKey = rnd.nextBoolean();
        final boolean withIndex = rnd.nextBoolean();

        final Map<Integer, RowInfo> rowInfos = new HashMap<>();

        for (int i=0; i<200; i++) {
            RowInfo rowInfo = randomRowInfo("Test", keyColumns, withAltKey, withIndex, rnd);
            Integer version = rs.schemaVersion(rowInfo, true, ix.id(), false);

            RowInfo existing = rowInfos.get(version);
            if (existing == null) {
                rowInfos.put(version, rowInfo);
            } else {
                assertMatches(rowInfo, existing);
            }
        }

        for (Map.Entry<Integer, RowInfo> e : rowInfos.entrySet()) {
            Integer version = rs.schemaVersion(e.getValue(), true, ix.id(), false);
            assertEquals(version, e.getKey());
        }

        db.close();
    }

    private static void assertMatches(RowInfo a, RowInfo b) {
        assertEquals(a.name, b.name);
        assertEquals(a.allColumns, b.allColumns);
        assertEquals(a.keyColumns, b.keyColumns);
        assertEquals(a.valueColumns, b.valueColumns);
        assertEquals(a.alternateKeys, b.alternateKeys);
        assertEquals(a.secondaryIndexes, b.secondaryIndexes);
    }

    private static RowInfo randomRowInfo(String name, Map<String, ColumnInfo> keyColumns,
                                         boolean withAltKey, boolean withIndex, Random rnd)
    {
        var rowInfo = new RowInfo(name);
        rowInfo.keyColumns = keyColumns;
        rowInfo.valueColumns = randomValueColumns(keyColumns.size(), rnd);
        rowInfo.allColumns = new TreeMap<>();
        rowInfo.allColumns.putAll(rowInfo.keyColumns);
        rowInfo.allColumns.putAll(rowInfo.valueColumns);

        if (!withAltKey) {
            rowInfo.alternateKeys = Collections.emptyNavigableSet();
        } else {
            // Not a proper primary key, but good enough to test schema storage.
            rowInfo.alternateKeys = toIndexes(keyColumns);
        }

        if (!withIndex) {
            rowInfo.secondaryIndexes = Collections.emptyNavigableSet();
        } else {
            // Not a proper secondary index, but good enough to test schema storage.
            rowInfo.secondaryIndexes = toIndexes(keyColumns);
        }

        return rowInfo;
    }

    private static NavigableSet<ColumnSet> toIndexes(Map<String, ColumnInfo> keyColumns) {
        var fullSet = new TreeSet<>(ColumnSetComparator.THE);

        var index = new ColumnSet();
        index.keyColumns = keyColumns;
        index.valueColumns = Collections.emptyNavigableMap();
        index.allColumns = new TreeMap<>(keyColumns);

        fullSet.add(index);

        return fullSet;
    }

    private static Map<String, ColumnInfo> randomKeyColumns(int base, Random rnd) {
        var columns = new LinkedHashMap<String, ColumnInfo>();
        randomColumns(columns, 1 + rnd.nextInt(4), base, rnd);
        return columns;
    }

    private static NavigableMap<String, ColumnInfo> randomValueColumns(int base, Random rnd) {
        var columns = new TreeMap<String, ColumnInfo>();
        randomColumns(columns, rnd.nextInt(10), base, rnd);
        return columns;
    }

    private static void randomColumns(Map<String, ColumnInfo> columns, int num, int base,
                                      Random rnd)
    {
        for (int i=0; i<num; i++) {
            String colName = String.valueOf((char) ('a' + base + i));
            columns.put(colName, randomColumnInfo(colName, rnd));
        }
    }

    private static ColumnInfo randomColumnInfo(String name, Random rnd) {
        var info = new ColumnInfo();
        info.name = name;

        int typeCode = rnd.nextInt(ColumnInfo.TYPE_BIG_DECIMAL + 1);

        if (rnd.nextBoolean()) {
            typeCode |= ColumnInfo.TYPE_NULLABLE;
        }

        if (rnd.nextBoolean()) {
            typeCode |= ColumnInfo.TYPE_DESCENDING;
        }

        if (rnd.nextInt(10) == 0) {
            typeCode |= ColumnInfo.TYPE_ARRAY;
        }

        info.typeCode = typeCode;

        info.assignType();

        return info;
    }
}
