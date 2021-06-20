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

import java.lang.reflect.Method;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.TreeMap;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.Database;
import org.cojen.tupl.DatabaseConfig;
import org.cojen.tupl.RowIndex;
import org.cojen.tupl.RowScanner;

/**
 * Schema evolution tests.
 *
 * @author Brian S O'Neill
 */
@SuppressWarnings("unchecked")
public class EvolutionTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(EvolutionTest.class.getName());
    }

    @Test
    public void defaults() throws Exception {
        // When adding new columns, columns for old rows should be null, 0, "", etc.

        Database db = Database.open(new DatabaseConfig());

        // First insert a row with no value columns.
        {
            Class rowType = RowTestUtils.newRowType("test.evolve.MyStuff", int.class, "+key");
            RowIndex ix = db.openRowIndex(rowType);
            Method setter = rowType.getMethod("key", int.class);
            var row = ix.newRow();
            setter.invoke(row, 0);
            ix.store(null, row);
        }

        // Now update the definition with value columns of all supported types.

        Object[] toAdd = specToAdd();
        Object[] spec = new Object[2 + toAdd.length];
        spec[0] = int.class;
        spec[1] = "+key";
        System.arraycopy(toAdd, 0, spec, 2, toAdd.length);

        Class rowType = RowTestUtils.newRowType("test.evolve.MyStuff", spec);
        RowIndex ix = db.openRowIndex(rowType);
        Method setter = rowType.getMethod("key", int.class);
        var row = ix.newRow();
        setter.invoke(row, 0);
        ix.load(null, row);

        for (int i=0; i<toAdd.length; i+=2) {
            Class type = (Class) toAdd[i];
            String name = (String) toAdd[i + 1];

            boolean nullable = false;
            if (name.endsWith("?")) {
                nullable = true;
                name = name.substring(0, name.length() - 1);
            }

            Object value = rowType.getMethod(name).invoke(row);

            if (nullable) {
                assertNull(value);
            } else if (type == String.class) {
                assertEquals("", value);
            } else if (type == boolean.class || type == Boolean.class) {
                assertEquals(false, value);
            } else if (type == char.class || type == Character.class) {
                assertEquals('\0', value);
            } else {
                assertEquals(0, BigDecimal.ZERO.compareTo(new BigDecimal(String.valueOf(value))));
            }
        }
    }

    private static Object[] specToAdd() {
        return new Object[] {
            boolean.class, "bo",
            byte.class, "by",
            char.class, "ch",
            double.class, "do",
            float.class, "fl",
            int.class, "in",
            long.class, "lo",
            short.class, "sh",

            Boolean.class, "boo",
            Byte.class, "byt",
            Character.class, "cha",
            Double.class, "dou",
            Float.class, "flo",
            Integer.class, "int",
            Long.class, "lon",
            Short.class, "sho",

            Boolean.class, "bool?",
            Byte.class, "byte?",
            Character.class, "char?",
            Double.class, "doub?",
            Float.class, "floa?",
            Integer.class, "inte?",
            Long.class, "long?",
            Short.class, "shor?",

            String.class, "st",
            BigInteger.class, "bi",
            BigDecimal.class, "bd",

            String.class, "str?",
            BigInteger.class, "bint?",
            BigDecimal.class, "bdec?",
        };
    }

    @Test
    public void addColumns() throws Exception {
        // When adding new columns, columns for old rows should be null, 0, "", etc. When
        // loading new rows with old schema, new columns should be dropped.

        Database db = Database.open(new DatabaseConfig());

        Object[] toAdd = specToAdd();

        var specs = new ArrayList<Object[]>();
        var indexes = new ArrayList<RowIndex<?>>();

        for (int i = 0; i <= toAdd.length; i += 2) {
            var spec = new Object[2 + i];
            spec[0] = long.class;
            spec[1] = "+key";

            for (int j=0; j<i; j++) {
                spec[2 + j] = toAdd[j];
            }

            specs.add(spec);

            Class<?> type = RowTestUtils.newRowType("test.evolve.MyStuff", spec);
            RowIndex<?> ix = db.openRowIndex(type);
            indexes.add(ix);

            insertRandom(RowUtils.scramble(8675309 + i), spec, ix, 10);
        }

        TreeMap<Object, List<TreeMap<String, Object>>> extracted = new TreeMap<>();

        for (int i=0; i<specs.size(); i++) {
            Object[] spec = specs.get(i);
            RowIndex<?> ix = indexes.get(i);

            Method[] getters = RowTestUtils.access(spec, ix.rowType())[0];

            RowScanner scanner = ix.newScanner(null);
            for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
                TreeMap<String, Object> columns = extractColumns(getters, row);
                Object key = columns.remove("key");
                List<TreeMap<String, Object>> list = extracted.get(key);
                if (list == null) {
                    list = new ArrayList<>();
                    extracted.put(key, list);
                }
                list.add(columns);
            }
        }

        for (List<TreeMap<String, Object>> list : extracted.values()) {
            for (int i=0; i<list.size(); i++) {
                for (int j=0; j<list.size(); j++) {
                    if (i != j) {
                        compare(list.get(i), list.get(j));
                    }
                }
            }
        }
    }

    private static void compare(TreeMap<String, Object> aCols, TreeMap<String, Object> bCols) {
        for (Map.Entry<String, Object> e : aCols.entrySet()) {
            Object other = bCols.get(e.getKey());
            if (other != null || bCols.containsKey(e.getKey())) {
                assertEquals(e.getValue(), other);
            }
        }

        for (Map.Entry<String, Object> e : bCols.entrySet()) {
            Object other = aCols.get(e.getKey());
            if (other != null || aCols.containsKey(e.getKey())) {
                assertEquals(e.getValue(), other);
            }
        }
    }

    private static TreeMap<String, Object> extractColumns(Method[] getters, Object row)
        throws Exception
    {
        var map = new TreeMap<String, Object>();
        for (Method m : getters) {
            map.put(m.getName(), m.invoke(row));
        }
        return map;
    }

    private static void insertRandom(long seed, Object[] spec, RowIndex ix, int amt)
        throws Exception
    {
        Method[][] access = RowTestUtils.access(spec, ix.rowType());
        Method[] getters = access[0];
        Method[] setters = access[1];

        Random rnd = new Random(seed);

        var inserted = new Object[amt];

        for (int i=0; i<amt; i++) {
            var row = ix.newRow();
            for (int j=0; j<setters.length; j++) {
                setters[j].invoke(row, RowTestUtils.randomValue(rnd, spec, j));
            }
            // Collision is possible here, although unlikely.
            assertTrue(ix.insert(null, row));
            inserted[i] = row;
        }

        // Verify by loading everything back. Assume first column is the primary key.

        rnd = new Random(seed);

        for (int i=0; i<inserted.length; i++) {
            var row = ix.newRow();
            for (int j=0; j<setters.length; j++) {
                Object value = RowTestUtils.randomValue(rnd, spec, j);
                if (j == 0) {
                    setters[j].invoke(row, value);
                }
            }
            ix.load(null, row);
            assertEquals(inserted[i], row);
        }
    }

    @Test
    public void alterColumns() throws Exception {
        // Test a few column type conversions. ConverterTest does a more exhaustive test.

        Object[] initSpec = {
            int.class, "+key",
            double.class, "a",
            Integer.class, "b?",
            String.class, "c",
            BigInteger.class, "d",
            BigDecimal.class, "e?",
        };

        Database db = Database.open(new DatabaseConfig());

        {
            Class rowType = RowTestUtils.newRowType("test.evolve.MyStuff", initSpec);
            RowIndex ix = db.openRowIndex(rowType);
            var row = ix.newRow();

            rowType.getMethod("key", int.class).invoke(row, 0);

            rowType.getMethod("a", double.class).invoke(row, 123.9);
            rowType.getMethod("b", Integer.class).invoke(row, -100);
            rowType.getMethod("c", String.class).invoke(row, "hello");
            rowType.getMethod("d", BigInteger.class).invoke(row, new BigInteger("9999999999999"));
            rowType.getMethod("e", BigDecimal.class).invoke(row, new BigDecimal("123.987"));

            ix.store(null, row);
        }

        Object[] newSpec = {
            int.class, "+key",
            int.class, "a",      // double to int
            Double.class, "b?",  // Integer? to Double?
            String.class, "c?",  // String to String?
            int.class, "d",      // BigInteger to int
            String.class, "e",   // BigDecimal? to String
        };

        Class rowType = RowTestUtils.newRowType("test.evolve.MyStuff", newSpec);
        RowIndex ix = db.openRowIndex(rowType);
        var row = ix.newRow();
        rowType.getMethod("key", int.class).invoke(row, 0);
        ix.load(null, row);

        // Cast double to int.
        assertEquals(123, rowType.getMethod("a").invoke(row));

        // Cast int to double.
        assertEquals(-100.0, rowType.getMethod("b").invoke(row));

        // String is now nullable, but value shouldn't change.
        assertEquals("hello", rowType.getMethod("c").invoke(row));

        // BigInteger was too big to fit into an int and so it got clamped.
        assertEquals(Integer.MAX_VALUE, rowType.getMethod("d").invoke(row));

        // Convert BigDecimal to String.
        assertEquals("123.987", rowType.getMethod("e").invoke(row));
    }

    @Test
    public void toStringCompare() throws Exception {
        // Test converting to string columns and running filters.

        Object[] initSpec = {
            int.class, "+key",
            double.class, "a",
            Integer.class, "b?",
            String.class, "c?",
        };

        Database db = Database.open(new DatabaseConfig());

        {
            Class rowType = RowTestUtils.newRowType("test.evolve.MyStuff", initSpec);
            RowIndex ix = db.openRowIndex(rowType);

            var setKey = rowType.getMethod("key", int.class);
            var setA = rowType.getMethod("a", double.class);
            var setB = rowType.getMethod("b", Integer.class);
            var setC = rowType.getMethod("c", String.class);

            {
                var row = ix.newRow();
                setKey.invoke(row, 1);
                setA.invoke(row, 123.9);
                setB.invoke(row, 1);
                setC.invoke(row, "str1");
                ix.store(null, row);
            }

            {
                var row = ix.newRow();
                setKey.invoke(row, 2);
                setA.invoke(row, 0.0/0.0);
                setB.invoke(row, 2);
                setC.invoke(row, "str2");
                ix.store(null, row);
            }

            {
                var row = ix.newRow();
                setKey.invoke(row, 3);
                setA.invoke(row, 100.0/0.0);
                setB.invoke(row, 11);
                setC.invoke(row, (Object) null);
                ix.store(null, row);
            }

            {
                var row = ix.newRow();
                setKey.invoke(row, 4);
                setA.invoke(row, 0);
                setB.invoke(row, (Object) null);
                setC.invoke(row, "str4");
                ix.store(null, row);
            }
        }

        Object[] newSpec = {
            int.class, "+key",
            String.class, "a",  // double to String
            String.class, "b?", // Integer? to String?
            String.class, "c",  // String? to String
        };

        Class rowType = RowTestUtils.newRowType("test.evolve.MyStuff", newSpec);
        RowIndex ix = db.openRowIndex(rowType);

        var setKey = rowType.getMethod("key", int.class);
        var getA = rowType.getMethod("a");
        var getB = rowType.getMethod("b");
        var getC = rowType.getMethod("c");

        {
            var row = ix.newRow();
            setKey.invoke(row, 1);
            ix.load(null, row);
            assertEquals("123.9", getA.invoke(row));
            assertEquals("1", getB.invoke(row));
            assertEquals("str1", getC.invoke(row));
        }

        {
            var row = ix.newRow();
            setKey.invoke(row, 2);
            ix.load(null, row);
            assertEquals("NaN", getA.invoke(row));
            assertEquals("2", getB.invoke(row));
            assertEquals("str2", getC.invoke(row));
        }

        {
            var row = ix.newRow();
            setKey.invoke(row, 3);
            ix.load(null, row);
            assertEquals("Infinity", getA.invoke(row));
            assertEquals("11", getB.invoke(row));
            assertEquals("", getC.invoke(row)); // Default for non-null String is "".
        }

        {
            var row = ix.newRow();
            setKey.invoke(row, 4);
            ix.load(null, row);
            assertEquals("0.0", getA.invoke(row));
            assertEquals(null, getB.invoke(row));
            assertEquals("str4", getC.invoke(row));
        }

        // Now test some filters.

        /* FIXME
        RowScanner scanner = ix.newScanner(null, "a == ?", "0.0");
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            System.out.println(row);
        }
        */
    }
}
