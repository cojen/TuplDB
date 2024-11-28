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

import java.lang.reflect.Method;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.Database;
import org.cojen.tupl.DatabaseConfig;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Table;

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
            Table table = db.openTable(rowType);
            Method setter = rowType.getMethod("key", int.class);
            var row = table.newRow();
            setter.invoke(row, 0);
            table.store(null, row);
        }

        // Now update the definition with value columns of all supported types.

        Object[] toAdd = specToAdd();
        Object[] spec = new Object[2 + toAdd.length];
        spec[0] = int.class;
        spec[1] = "+key";
        System.arraycopy(toAdd, 0, spec, 2, toAdd.length);

        Class rowType = RowTestUtils.newRowType("test.evolve.MyStuff", spec);
        Table table = db.openTable(rowType);
        Method setter = rowType.getMethod("key", int.class);
        var row = table.newRow();
        setter.invoke(row, 0);
        table.load(null, row);

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

        db.close();
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
        var tables = new ArrayList<Table<?>>();

        for (int i = 0; i <= toAdd.length; i += 2) {
            var spec = new Object[2 + i];
            spec[0] = long.class;
            spec[1] = "+key";

            for (int j=0; j<i; j++) {
                spec[2 + j] = toAdd[j];
            }

            specs.add(spec);

            Class<?> type = RowTestUtils.newRowType("test.evolve.MyStuff", spec);
            Table<?> table = db.openTable(type);
            tables.add(table);

            insertRandom(RowUtils.scramble(8675309 + i), spec, table, 10);
        }

        TreeMap<Object, List<TreeMap<String, Object>>> extracted = new TreeMap<>();

        for (int i=0; i<specs.size(); i++) {
            Object[] spec = specs.get(i);
            Table<?> table = tables.get(i);

            Method[] getters = RowTestUtils.access(spec, table.rowType())[0];

            Scanner scanner = table.newScanner(null);
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

        db.close();
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

    private static void insertRandom(long seed, Object[] spec, Table table, int amt)
        throws Exception
    {
        Method[][] access = RowTestUtils.access(spec, table.rowType());
        Method[] getters = access[0];
        Method[] setters = access[1];

        Random rnd = new Random(seed);

        var inserted = new Object[amt];

        for (int i=0; i<amt; i++) {
            var row = table.newRow();
            for (int j=0; j<setters.length; j++) {
                setters[j].invoke(row, RowTestUtils.randomValue(rnd, spec, j));
            }
            // Collision is possible here, although unlikely.
            table.insert(null, row);
            inserted[i] = row;
        }

        // Verify by loading everything back. Assume first column is the primary key.

        rnd = new Random(seed);

        for (int i=0; i<inserted.length; i++) {
            var row = table.newRow();
            for (int j=0; j<setters.length; j++) {
                Object value = RowTestUtils.randomValue(rnd, spec, j);
                if (j == 0) {
                    setters[j].invoke(row, value);
                }
            }
            table.load(null, row);
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
            Table table = db.openTable(rowType);
            var row = table.newRow();

            rowType.getMethod("key", int.class).invoke(row, 0);

            rowType.getMethod("a", double.class).invoke(row, 123.9);
            rowType.getMethod("b", Integer.class).invoke(row, -100);
            rowType.getMethod("c", String.class).invoke(row, "hello");
            rowType.getMethod("d", BigInteger.class).invoke(row, new BigInteger("9999999999999"));
            rowType.getMethod("e", BigDecimal.class).invoke(row, new BigDecimal("123.987"));

            table.store(null, row);
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
        Table table = db.openTable(rowType);
        var row = table.newRow();
        rowType.getMethod("key", int.class).invoke(row, 0);
        table.load(null, row);

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

        db.close();
    }

    @Test
    public void toStringCompare() throws Exception {
        // Test converting to string columns and running filters.

        Object[] initSpec = {
            int.class, "+key",
            double.class, "a",
            Integer.class, "b?",
            String.class, "c?",
            BigInteger.class, "d",
            BigDecimal.class, "e?",
        };

        Database db = Database.open(new DatabaseConfig());

        {
            Class rowType = RowTestUtils.newRowType("test.evolve.MyStuff", initSpec);
            Table table = db.openTable(rowType);

            var setKey = rowType.getMethod("key", int.class);
            var setA = rowType.getMethod("a", double.class);
            var setB = rowType.getMethod("b", Integer.class);
            var setC = rowType.getMethod("c", String.class);
            var setD = rowType.getMethod("d", BigInteger.class);
            var setE = rowType.getMethod("e", BigDecimal.class);

            {
                var row = table.newRow();
                setKey.invoke(row, 1);
                setA.invoke(row, 123.9);
                setB.invoke(row, 1);
                setC.invoke(row, "str1");
                setD.invoke(row, BigInteger.valueOf(100));
                setE.invoke(row, (Object) null);
                table.store(null, row);
            }

            {
                var row = table.newRow();
                setKey.invoke(row, 2);
                setA.invoke(row, 0.0/0.0);
                setB.invoke(row, 2);
                setC.invoke(row, "str2");
                setD.invoke(row, BigInteger.valueOf(200));
                setE.invoke(row, new BigDecimal("123.456"));
                table.store(null, row);
            }

            {
                var row = table.newRow();
                setKey.invoke(row, 3);
                setA.invoke(row, 100.0/0.0);
                setB.invoke(row, 11);
                setC.invoke(row, (Object) null);
                setD.invoke(row, BigInteger.valueOf(300));
                setE.invoke(row, new BigDecimal("-123.456"));
                table.store(null, row);
            }

            {
                var row = table.newRow();
                setKey.invoke(row, 4);
                setA.invoke(row, 0);
                setB.invoke(row, (Object) null);
                setC.invoke(row, "str4");
                setD.invoke(row, BigInteger.valueOf(-400));
                setE.invoke(row, new BigDecimal("999999999999999.1"));
                table.store(null, row);
            }
        }

        Object[] newSpec = {
            int.class, "+key",
            String.class, "a",  // double to String
            String.class, "b?", // Integer? to String?
            String.class, "c",  // String? to String
            String.class, "d",  // BigInteger to String
            String.class, "e",  // BigDecimal? to String
        };

        Class rowType = RowTestUtils.newRowType("test.evolve.MyStuff", newSpec);
        Table table = db.openTable(rowType);

        var setKey = rowType.getMethod("key", int.class);
        var getKey = rowType.getMethod("key");
        var getA = rowType.getMethod("a");
        var getB = rowType.getMethod("b");
        var getC = rowType.getMethod("c");
        var getD = rowType.getMethod("d");
        var getE = rowType.getMethod("e");

        {
            var row = table.newRow();
            setKey.invoke(row, 1);
            table.load(null, row);
            assertEquals("123.9", getA.invoke(row));
            assertEquals("1", getB.invoke(row));
            assertEquals("str1", getC.invoke(row));
            assertEquals("100", getD.invoke(row));
            assertEquals("", getE.invoke(row)); // Default for non-null String is "".
        }

        {
            var row = table.newRow();
            setKey.invoke(row, 2);
            table.load(null, row);
            assertEquals("NaN", getA.invoke(row));
            assertEquals("2", getB.invoke(row));
            assertEquals("str2", getC.invoke(row));
            assertEquals("200", getD.invoke(row));
            assertEquals("123.456", getE.invoke(row));
        }

        {
            var row = table.newRow();
            setKey.invoke(row, 3);
            table.load(null, row);
            assertEquals("Infinity", getA.invoke(row));
            assertEquals("11", getB.invoke(row));
            assertEquals("", getC.invoke(row)); // Default for non-null String is "".
            assertEquals("300", getD.invoke(row));
            assertEquals("-123.456", getE.invoke(row));
        }

        {
            var row = table.newRow();
            setKey.invoke(row, 4);
            table.load(null, row);
            assertEquals("0.0", getA.invoke(row));
            assertEquals(null, getB.invoke(row));
            assertEquals("str4", getC.invoke(row));
            assertEquals("-400", getD.invoke(row));
            assertEquals("999999999999999.1", getE.invoke(row));
        }

        // Now test some filters.

        Scanner scanner = table.newScanner(null, "a == ?", "0.0");
        int count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            assertEquals(4, getKey.invoke(row));
            assertEquals("0.0", getA.invoke(row));
            assertEquals(null, getB.invoke(row));
            assertEquals("str4", getC.invoke(row));
            assertEquals("-400", getD.invoke(row));
            assertEquals("999999999999999.1", getE.invoke(row));
            count++;
        }

        assertEquals(1, count);

        scanner = table.newScanner(null, "b >= ?", "11");
        count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            switch ((int) getKey.invoke(row)) {
                default -> fail();
                case 2 -> {
                    assertEquals("NaN", getA.invoke(row));
                    assertEquals("2", getB.invoke(row));
                    assertEquals("str2", getC.invoke(row));
                    assertEquals("200", getD.invoke(row));
                    assertEquals("123.456", getE.invoke(row));
                }
                case 3 -> {
                    assertEquals("Infinity", getA.invoke(row));
                    assertEquals("11", getB.invoke(row));
                    assertEquals("", getC.invoke(row));
                    assertEquals("300", getD.invoke(row));
                    assertEquals("-123.456", getE.invoke(row));
                }
                case 4 -> {
                    assertEquals("0.0", getA.invoke(row));
                    assertEquals(null, getB.invoke(row));
                    assertEquals("str4", getC.invoke(row));
                    assertEquals("-400", getD.invoke(row));
                    assertEquals("999999999999999.1", getE.invoke(row));
                }
            }
            count++;
        }

        assertEquals(3, count);

        scanner = table.newScanner(null, "c == ?", "");
        count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            assertEquals(3, getKey.invoke(row));
            assertEquals("Infinity", getA.invoke(row));
            assertEquals("11", getB.invoke(row));
            assertEquals("", getC.invoke(row));
            assertEquals("300", getD.invoke(row));
            assertEquals("-123.456", getE.invoke(row));
            count++;
        }

        assertEquals(1, count);

        scanner = table.newScanner(null, "d >= ?", "300");
        count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            assertEquals(3, getKey.invoke(row));
            assertEquals("Infinity", getA.invoke(row));
            assertEquals("11", getB.invoke(row));
            assertEquals("", getC.invoke(row));
            assertEquals("300", getD.invoke(row));
            assertEquals("-123.456", getE.invoke(row));
            count++;
        }

        assertEquals(1, count);

        scanner = table.newScanner(null, "e > ? || e == ?", "123.456", "");
        count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            switch ((int) getKey.invoke(row)) {
            default: fail(); break;
            case 1:
                assertEquals("123.9", getA.invoke(row));
                assertEquals("1", getB.invoke(row));
                assertEquals("str1", getC.invoke(row));
                assertEquals("100", getD.invoke(row));
                assertEquals("", getE.invoke(row));
                break;
            case 4:
                assertEquals("0.0", getA.invoke(row));
                assertEquals(null, getB.invoke(row));
                assertEquals("str4", getC.invoke(row));
                assertEquals("-400", getD.invoke(row));
                assertEquals("999999999999999.1", getE.invoke(row));
                break;
            }
            count++;
        }

        assertEquals(2, count);

        db.close();
    }

    @Test
    public void toBigIntegerCompare() throws Exception {
        // Test converting to BigInteger columns and running filters.

        Object[] initSpec = {
            int.class, "+key",
            double.class, "a",
            Integer.class, "b?",
            String.class, "c",
            BigInteger.class, "d?",
            BigDecimal.class, "e",
        };

        Database db = Database.open(new DatabaseConfig());

        {
            Class rowType = RowTestUtils.newRowType("test.evolve.MyStuff", initSpec);
            Table table = db.openTable(rowType);

            var setKey = rowType.getMethod("key", int.class);
            var setA = rowType.getMethod("a", double.class);
            var setB = rowType.getMethod("b", Integer.class);
            var setC = rowType.getMethod("c", String.class);
            var setD = rowType.getMethod("d", BigInteger.class);
            var setE = rowType.getMethod("e", BigDecimal.class);

            {
                var row = table.newRow();
                setKey.invoke(row, 1);
                setA.invoke(row, 123.9);
                setB.invoke(row, 1);
                setC.invoke(row, "8888888888888888888");
                setD.invoke(row, BigInteger.valueOf(100));
                setE.invoke(row, BigDecimal.ZERO);
                table.store(null, row);
            }

            {
                var row = table.newRow();
                setKey.invoke(row, 2);
                setA.invoke(row, 0.0/0.0);
                setB.invoke(row, 2);
                setC.invoke(row, "10.9");
                setD.invoke(row, (Object) null);
                setE.invoke(row, new BigDecimal("123.456"));
                table.store(null, row);
            }

            {
                var row = table.newRow();
                setKey.invoke(row, 3);
                setA.invoke(row, 100.0/0.0);
                setB.invoke(row, 11);
                setC.invoke(row, "000");
                setD.invoke(row, BigInteger.valueOf(300));
                setE.invoke(row, new BigDecimal("-123.456"));
                table.store(null, row);
            }

            {
                var row = table.newRow();
                setKey.invoke(row, 4);
                setA.invoke(row, 0);
                setB.invoke(row, (Object) null);
                setC.invoke(row, "-10");
                setD.invoke(row, BigInteger.valueOf(-400));
                setE.invoke(row, new BigDecimal("999999999999999.1"));
                table.store(null, row);
            }
        }

        Object[] newSpec = {
            int.class, "+key",
            BigInteger.class, "a",  // double to BigInteger
            BigInteger.class, "b?", // Integer? to BigInteger?
            BigInteger.class, "c",  // String to BigInteger
            BigInteger.class, "d",  // BigInteger? to BigInteger
            BigInteger.class, "e",  // BigDecimal to BigInteger
        };

        Class rowType = RowTestUtils.newRowType("test.evolve.MyStuff", newSpec);
        Table table = db.openTable(rowType);

        var setKey = rowType.getMethod("key", int.class);
        var getKey = rowType.getMethod("key");
        var getA = rowType.getMethod("a");
        var getB = rowType.getMethod("b");
        var getC = rowType.getMethod("c");
        var getD = rowType.getMethod("d");
        var getE = rowType.getMethod("e");

        {
            var row = table.newRow();
            setKey.invoke(row, 1);
            table.load(null, row);
            assertEquals(new BigInteger("123"), getA.invoke(row));
            assertEquals(BigInteger.ONE, getB.invoke(row));
            assertEquals(new BigInteger("8888888888888888888"), getC.invoke(row));
            assertEquals(new BigInteger("100"), getD.invoke(row));
            assertEquals(BigInteger.ZERO, getE.invoke(row));
        }

        {
            var row = table.newRow();
            setKey.invoke(row, 2);
            table.load(null, row);
            assertEquals(BigInteger.ZERO, getA.invoke(row));
            assertEquals(new BigInteger("2"), getB.invoke(row));
            assertEquals(new BigInteger("10"), getC.invoke(row));
            assertEquals(BigInteger.ZERO, getD.invoke(row));
            assertEquals(new BigInteger("123"), getE.invoke(row));
        }

        {
            var row = table.newRow();
            setKey.invoke(row, 3);
            table.load(null, row);
            assertEquals(BigInteger.ZERO, getA.invoke(row));
            assertEquals(new BigInteger("11"), getB.invoke(row));
            assertEquals(BigInteger.ZERO, getC.invoke(row));
            assertEquals(new BigInteger("300"), getD.invoke(row));
            assertEquals(new BigInteger("-123"), getE.invoke(row));
        }

        {
            var row = table.newRow();
            setKey.invoke(row, 4);
            table.load(null, row);
            assertEquals(BigInteger.ZERO, getA.invoke(row));
            assertEquals(null, getB.invoke(row));
            assertEquals(new BigInteger("-10"), getC.invoke(row));
            assertEquals(new BigInteger("-400"), getD.invoke(row));
            assertEquals(new BigInteger("999999999999999"), getE.invoke(row));
        }

        // Now test some filters.

        Scanner scanner = table.newScanner(null, "a == ?", "0");
        int count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            switch ((int) getKey.invoke(row)) {
                default -> fail();
                case 2 -> {
                    assertEquals(BigInteger.ZERO, getA.invoke(row));
                    assertEquals(new BigInteger("2"), getB.invoke(row));
                    assertEquals(new BigInteger("10"), getC.invoke(row));
                    assertEquals(BigInteger.ZERO, getD.invoke(row));
                    assertEquals(new BigInteger("123"), getE.invoke(row));
                }
                case 3 -> {
                    assertEquals(BigInteger.ZERO, getA.invoke(row));
                    assertEquals(new BigInteger("11"), getB.invoke(row));
                    assertEquals(BigInteger.ZERO, getC.invoke(row));
                    assertEquals(new BigInteger("300"), getD.invoke(row));
                    assertEquals(new BigInteger("-123"), getE.invoke(row));
                }
                case 4 -> {
                    assertEquals(BigInteger.ZERO, getA.invoke(row));
                    assertEquals(null, getB.invoke(row));
                    assertEquals(new BigInteger("-10"), getC.invoke(row));
                    assertEquals(new BigInteger("-400"), getD.invoke(row));
                    assertEquals(new BigInteger("999999999999999"), getE.invoke(row));
                }
            }
            count++;
        }

        assertEquals(3, count);

        scanner = table.newScanner(null, "b > ?", 2);
        count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            switch ((int) getKey.invoke(row)) {
            default: fail(); break;
            case 3:
                assertEquals(BigInteger.ZERO, getA.invoke(row));
                assertEquals(new BigInteger("11"), getB.invoke(row));
                assertEquals(BigInteger.ZERO, getC.invoke(row));
                assertEquals(new BigInteger("300"), getD.invoke(row));
                assertEquals(new BigInteger("-123"), getE.invoke(row));
                break;
            case 4:
                assertEquals(BigInteger.ZERO, getA.invoke(row));
                assertEquals(null, getB.invoke(row));
                assertEquals(new BigInteger("-10"), getC.invoke(row));
                assertEquals(new BigInteger("-400"), getD.invoke(row));
                assertEquals(new BigInteger("999999999999999"), getE.invoke(row));
                break;
            }
            count++;
        }

        assertEquals(2, count);

        scanner = table.newScanner(null, "c == ?", 0);
        count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            assertEquals(3, getKey.invoke(row));
            assertEquals(BigInteger.ZERO, getA.invoke(row));
            assertEquals(new BigInteger("11"), getB.invoke(row));
            assertEquals(BigInteger.ZERO, getC.invoke(row));
            assertEquals(new BigInteger("300"), getD.invoke(row));
            assertEquals(new BigInteger("-123"), getE.invoke(row));
            count++;
        }

        assertEquals(1, count);

        scanner = table.newScanner(null, "d <= ?", 0);
        count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            switch ((int) getKey.invoke(row)) {
            default: fail(); break;
            case 2:
                assertEquals(BigInteger.ZERO, getA.invoke(row));
                assertEquals(new BigInteger("2"), getB.invoke(row));
                assertEquals(new BigInteger("10"), getC.invoke(row));
                assertEquals(BigInteger.ZERO, getD.invoke(row));
                assertEquals(new BigInteger("123"), getE.invoke(row));
                break;
            case 4:
                assertEquals(BigInteger.ZERO, getA.invoke(row));
                assertEquals(null, getB.invoke(row));
                assertEquals(new BigInteger("-10"), getC.invoke(row));
                assertEquals(new BigInteger("-400"), getD.invoke(row));
                assertEquals(new BigInteger("999999999999999"), getE.invoke(row));
                break;
            }
            count++;
        }

        assertEquals(2, count);

        scanner = table.newScanner(null, "e == ?", new BigInteger("999999999999999"));
        count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            assertEquals(4, getKey.invoke(row));
            assertEquals(BigInteger.ZERO, getA.invoke(row));
            assertEquals(null, getB.invoke(row));
            assertEquals(new BigInteger("-10"), getC.invoke(row));
            assertEquals(new BigInteger("-400"), getD.invoke(row));
            assertEquals(new BigInteger("999999999999999"), getE.invoke(row));
            count++;
        }

        assertEquals(1, count);

        db.close();
    }

    @Test
    public void toBigDecimalCompare() throws Exception {
        // Test converting to BigDecimal columns and running filters.

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
            Table table = db.openTable(rowType);

            var setKey = rowType.getMethod("key", int.class);
            var setA = rowType.getMethod("a", double.class);
            var setB = rowType.getMethod("b", Integer.class);
            var setC = rowType.getMethod("c", String.class);
            var setD = rowType.getMethod("d", BigInteger.class);
            var setE = rowType.getMethod("e", BigDecimal.class);

            {
                var row = table.newRow();
                setKey.invoke(row, 1);
                setA.invoke(row, 123.9);
                setB.invoke(row, 1);
                setC.invoke(row, "8888888888888888888");
                setD.invoke(row, BigInteger.valueOf(100));
                setE.invoke(row, new BigDecimal("0.000"));
                table.store(null, row);
            }

            {
                var row = table.newRow();
                setKey.invoke(row, 2);
                setA.invoke(row, 0.0/0.0);
                setB.invoke(row, 2);
                setC.invoke(row, "10.9");
                setD.invoke(row, BigInteger.ZERO);
                setE.invoke(row, (Object) null);
                table.store(null, row);
            }

            {
                var row = table.newRow();
                setKey.invoke(row, 3);
                setA.invoke(row, 100.0/0.0);
                setB.invoke(row, 11);
                setC.invoke(row, "000");
                setD.invoke(row, BigInteger.valueOf(300));
                setE.invoke(row, new BigDecimal("-123.456"));
                table.store(null, row);
            }

            {
                var row = table.newRow();
                setKey.invoke(row, 4);
                setA.invoke(row, 0);
                setB.invoke(row, (Object) null);
                setC.invoke(row, "-10");
                setD.invoke(row, BigInteger.valueOf(-400));
                setE.invoke(row, new BigDecimal("999999999999999.1"));
                table.store(null, row);
            }
        }

        Object[] newSpec = {
            int.class, "+key",
            BigDecimal.class, "a",  // double to BigDecimal
            BigDecimal.class, "b?", // Integer? to BigDecimal?
            BigDecimal.class, "c",  // String to BigDecimal
            BigDecimal.class, "d",  // BigInteger to BigDecimal
            BigDecimal.class, "e",  // BigDecimal? to BigDecimal
        };

        Class rowType = RowTestUtils.newRowType("test.evolve.MyStuff", newSpec);
        Table table = db.openTable(rowType);

        var setKey = rowType.getMethod("key", int.class);
        var getKey = rowType.getMethod("key");
        var getA = rowType.getMethod("a");
        var getB = rowType.getMethod("b");
        var getC = rowType.getMethod("c");
        var getD = rowType.getMethod("d");
        var getE = rowType.getMethod("e");

        {
            var row = table.newRow();
            setKey.invoke(row, 1);
            table.load(null, row);
            assertEquals(new BigDecimal("123.9"), getA.invoke(row));
            assertEquals(BigDecimal.ONE, getB.invoke(row));
            assertEquals(new BigDecimal("8888888888888888888"), getC.invoke(row));
            assertEquals(new BigDecimal("100"), getD.invoke(row));
            assertEquals(new BigDecimal("0.000"), getE.invoke(row));
        }

        {
            var row = table.newRow();
            setKey.invoke(row, 2);
            table.load(null, row);
            assertEquals(BigDecimal.ZERO, getA.invoke(row));
            assertEquals(new BigDecimal("2"), getB.invoke(row));
            assertEquals(new BigDecimal("10.9"), getC.invoke(row));
            assertEquals(BigDecimal.ZERO, getD.invoke(row));
            assertEquals(BigDecimal.ZERO, getE.invoke(row));
        }

        {
            var row = table.newRow();
            setKey.invoke(row, 3);
            table.load(null, row);
            assertEquals(BigDecimal.ZERO, getA.invoke(row));
            assertEquals(new BigDecimal("11"), getB.invoke(row));
            assertEquals(BigDecimal.ZERO, getC.invoke(row));
            assertEquals(new BigDecimal("300"), getD.invoke(row));
            assertEquals(new BigDecimal("-123.456"), getE.invoke(row));
        }

        {
            var row = table.newRow();
            setKey.invoke(row, 4);
            table.load(null, row);
            assertEquals(BigDecimal.ZERO, getA.invoke(row));
            assertEquals(null, getB.invoke(row));
            assertEquals(new BigDecimal("-10"), getC.invoke(row));
            assertEquals(new BigDecimal("-400"), getD.invoke(row));
            assertEquals(new BigDecimal("999999999999999.1"), getE.invoke(row));
        }

        // Now test some filters.

        Scanner scanner = table.newScanner(null, "a == ?", "0");
        int count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            switch ((int) getKey.invoke(row)) {
            default: fail(); break;
            case 2:
                assertEquals(BigDecimal.ZERO, getA.invoke(row));
                assertEquals(new BigDecimal("2"), getB.invoke(row));
                assertEquals(new BigDecimal("10.9"), getC.invoke(row));
                assertEquals(BigDecimal.ZERO, getD.invoke(row));
                assertEquals(BigDecimal.ZERO, getE.invoke(row));
                break;
            case 3:
                assertEquals(BigDecimal.ZERO, getA.invoke(row));
                assertEquals(new BigDecimal("11"), getB.invoke(row));
                assertEquals(BigDecimal.ZERO, getC.invoke(row));
                assertEquals(new BigDecimal("300"), getD.invoke(row));
                assertEquals(new BigDecimal("-123.456"), getE.invoke(row));
                break;
            case 4:
                assertEquals(BigDecimal.ZERO, getA.invoke(row));
                assertEquals(null, getB.invoke(row));
                assertEquals(new BigDecimal("-10"), getC.invoke(row));
                assertEquals(new BigDecimal("-400"), getD.invoke(row));
                assertEquals(new BigDecimal("999999999999999.1"), getE.invoke(row));
                break;
            }
            count++;
        }

        assertEquals(3, count);

        scanner = table.newScanner(null, "b >= ?", 11);
        count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            switch ((int) getKey.invoke(row)) {
                default -> fail();
                case 3 -> {
                    assertEquals(BigDecimal.ZERO, getA.invoke(row));
                    assertEquals(new BigDecimal("11"), getB.invoke(row));
                    assertEquals(BigDecimal.ZERO, getC.invoke(row));
                    assertEquals(new BigDecimal("300"), getD.invoke(row));
                    assertEquals(new BigDecimal("-123.456"), getE.invoke(row));
                }
                case 4 -> {
                    assertEquals(BigDecimal.ZERO, getA.invoke(row));
                    assertEquals(null, getB.invoke(row));
                    assertEquals(new BigDecimal("-10"), getC.invoke(row));
                    assertEquals(new BigDecimal("-400"), getD.invoke(row));
                    assertEquals(new BigDecimal("999999999999999.1"), getE.invoke(row));
                }
            }
            count++;
        }

        assertEquals(2, count);

        scanner = table.newScanner(null, "c == ?", 0);
        count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            assertEquals(3, getKey.invoke(row));
            assertEquals(BigDecimal.ZERO, getA.invoke(row));
            assertEquals(new BigDecimal("11"), getB.invoke(row));
            assertEquals(BigDecimal.ZERO, getC.invoke(row));
            assertEquals(new BigDecimal("300"), getD.invoke(row));
            assertEquals(new BigDecimal("-123.456"), getE.invoke(row));
            count++;
        }

        assertEquals(1, count);

        scanner = table.newScanner(null, "d <= ? && d > ?", 0, -100);
        count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            assertEquals(2, getKey.invoke(row));
            assertEquals(BigDecimal.ZERO, getA.invoke(row));
            assertEquals(new BigDecimal("2"), getB.invoke(row));
            assertEquals(new BigDecimal("10.9"), getC.invoke(row));
            assertEquals(BigDecimal.ZERO, getD.invoke(row));
            assertEquals(BigDecimal.ZERO, getE.invoke(row));
            count++;
        }

        assertEquals(1, count);

        scanner = table.newScanner(null, "e == ? || e == ?", 0, "999999999999999.1");
        count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            switch ((int) getKey.invoke(row)) {
                default -> fail();
                case 1 -> {
                    assertEquals(new BigDecimal("123.9"), getA.invoke(row));
                    assertEquals(BigDecimal.ONE, getB.invoke(row));
                    assertEquals(new BigDecimal("8888888888888888888"), getC.invoke(row));
                    assertEquals(new BigDecimal("100"), getD.invoke(row));
                    assertEquals(new BigDecimal("0.000"), getE.invoke(row));
                }
                case 2 -> {
                    assertEquals(BigDecimal.ZERO, getA.invoke(row));
                    assertEquals(new BigDecimal("2"), getB.invoke(row));
                    assertEquals(new BigDecimal("10.9"), getC.invoke(row));
                    assertEquals(BigDecimal.ZERO, getD.invoke(row));
                    assertEquals(BigDecimal.ZERO, getE.invoke(row));
                }
                case 4 -> {
                    assertEquals(BigDecimal.ZERO, getA.invoke(row));
                    assertEquals(null, getB.invoke(row));
                    assertEquals(new BigDecimal("-10"), getC.invoke(row));
                    assertEquals(new BigDecimal("-400"), getD.invoke(row));
                    assertEquals(new BigDecimal("999999999999999.1"), getE.invoke(row));
                }
            }
            count++;
        }

        assertEquals(3, count);

        db.close();
    }

    @Test
    public void toIntCompare() throws Exception {
        // Test converting to int columns and running filters.

        Object[] initSpec = {
            int.class, "+key",
            Double.class, "a?",
            Integer.class, "b?",
            String.class, "c",
            BigInteger.class, "d?",
            BigDecimal.class, "e",
        };

        Database db = Database.open(new DatabaseConfig());

        {
            Class rowType = RowTestUtils.newRowType("test.evolve.MyStuff", initSpec);
            Table table = db.openTable(rowType);

            var setKey = rowType.getMethod("key", int.class);
            var setA = rowType.getMethod("a", Double.class);
            var setB = rowType.getMethod("b", Integer.class);
            var setC = rowType.getMethod("c", String.class);
            var setD = rowType.getMethod("d", BigInteger.class);
            var setE = rowType.getMethod("e", BigDecimal.class);

            {
                var row = table.newRow();
                setKey.invoke(row, 1);
                setA.invoke(row, 123.9);
                setB.invoke(row, 1);
                setC.invoke(row, "8888888888888888888");
                setD.invoke(row, BigInteger.valueOf(100));
                setE.invoke(row, BigDecimal.ZERO);
                table.store(null, row);
            }

            {
                var row = table.newRow();
                setKey.invoke(row, 2);
                setA.invoke(row, 0.0/0.0);
                setB.invoke(row, 2);
                setC.invoke(row, "10.9");
                setD.invoke(row, (Object) null);
                setE.invoke(row, new BigDecimal("123.456"));
                table.store(null, row);
            }

            {
                var row = table.newRow();
                setKey.invoke(row, 3);
                setA.invoke(row, 100.0/0.0);
                setB.invoke(row, 11);
                setC.invoke(row, "000");
                setD.invoke(row, BigInteger.valueOf(300));
                setE.invoke(row, new BigDecimal("-123.456"));
                table.store(null, row);
            }

            {
                var row = table.newRow();
                setKey.invoke(row, 4);
                setA.invoke(row, (Object) null);
                setB.invoke(row, (Object) null);
                setC.invoke(row, "-10");
                setD.invoke(row, BigInteger.valueOf(-400));
                setE.invoke(row, new BigDecimal("999999999999999.1"));
                table.store(null, row);
            }
        }

        Object[] newSpec = {
            int.class, "+key",
            Integer.class, "a?", // Double? to Integer?
            int.class, "b",      // Integer? to int
            int.class, "c",      // String to int
            Integer.class, "d?", // BigInteger? to Integer?
            int.class, "e",      // BigDecimal to int
        };

        Class rowType = RowTestUtils.newRowType("test.evolve.MyStuff", newSpec);
        Table table = db.openTable(rowType);

        var setKey = rowType.getMethod("key", int.class);
        var getKey = rowType.getMethod("key");
        var getA = rowType.getMethod("a");
        var getB = rowType.getMethod("b");
        var getC = rowType.getMethod("c");
        var getD = rowType.getMethod("d");
        var getE = rowType.getMethod("e");

        {
            var row = table.newRow();
            setKey.invoke(row, 1);
            table.load(null, row);
            assertEquals(123, getA.invoke(row));
            assertEquals(1, getB.invoke(row));
            assertEquals(Integer.MAX_VALUE, getC.invoke(row));
            assertEquals(100, getD.invoke(row));
            assertEquals(0, getE.invoke(row));
        }

        {
            var row = table.newRow();
            setKey.invoke(row, 2);
            table.load(null, row);
            assertEquals(0, getA.invoke(row));
            assertEquals(2, getB.invoke(row));
            assertEquals(10, getC.invoke(row));
            assertEquals(null, getD.invoke(row));
            assertEquals(123, getE.invoke(row));
        }

        {
            var row = table.newRow();
            setKey.invoke(row, 3);
            table.load(null, row);
            assertEquals(Integer.MAX_VALUE, getA.invoke(row));
            assertEquals(11, getB.invoke(row));
            assertEquals(0, getC.invoke(row));
            assertEquals(300, getD.invoke(row));
            assertEquals(-123, getE.invoke(row));
        }

        {
            var row = table.newRow();
            setKey.invoke(row, 4);
            table.load(null, row);
            assertEquals(null, getA.invoke(row));
            assertEquals(0, getB.invoke(row));
            assertEquals(-10, getC.invoke(row));
            assertEquals(-400, getD.invoke(row));
            assertEquals(Integer.MAX_VALUE, getE.invoke(row));
        }

        // Now test some filters.

        Scanner scanner = table.newScanner(null, "a == ?", "0");
        int count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            assertEquals(2, getKey.invoke(row));
            assertEquals(0, getA.invoke(row));
            assertEquals(2, getB.invoke(row));
            assertEquals(10, getC.invoke(row));
            assertEquals(null, getD.invoke(row));
            assertEquals(123, getE.invoke(row));
            count++;
        }

        assertEquals(1, count);

        scanner = table.newScanner(null, "b > ?", 2);
        count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            assertEquals(3, getKey.invoke(row));
            assertEquals(Integer.MAX_VALUE, getA.invoke(row));
            assertEquals(11, getB.invoke(row));
            assertEquals(0, getC.invoke(row));
            assertEquals(300, getD.invoke(row));
            assertEquals(-123, getE.invoke(row));
            count++;
        }

        assertEquals(1, count);

        scanner = table.newScanner(null, "c == ?", 0);
        count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            assertEquals(3, getKey.invoke(row));
            assertEquals(Integer.MAX_VALUE, getA.invoke(row));
            assertEquals(11, getB.invoke(row));
            assertEquals(0, getC.invoke(row));
            assertEquals(300, getD.invoke(row));
            assertEquals(-123, getE.invoke(row));
            count++;
        }

        assertEquals(1, count);

        scanner = table.newScanner(null, "d > ?", 250);
        count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            switch ((int) getKey.invoke(row)) {
                default -> fail();
                case 2 -> {
                    assertEquals(0, getA.invoke(row));
                    assertEquals(2, getB.invoke(row));
                    assertEquals(10, getC.invoke(row));
                    assertEquals(null, getD.invoke(row));
                    assertEquals(123, getE.invoke(row));
                }
                case 3 -> {
                    assertEquals(Integer.MAX_VALUE, getA.invoke(row));
                    assertEquals(11, getB.invoke(row));
                    assertEquals(0, getC.invoke(row));
                    assertEquals(300, getD.invoke(row));
                    assertEquals(-123, getE.invoke(row));
                }
            }
            count++;
        }

        assertEquals(2, count);

        scanner = table.newScanner(null, "e == ?", Integer.MAX_VALUE);
        count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            assertEquals(4, getKey.invoke(row));
            assertEquals(null, getA.invoke(row));
            assertEquals(0, getB.invoke(row));
            assertEquals(-10, getC.invoke(row));
            assertEquals(-400, getD.invoke(row));
            assertEquals(Integer.MAX_VALUE, getE.invoke(row));
            count++;
        }

        assertEquals(1, count);

        db.close();
    }

    @Test
    public void toFloatCompare() throws Exception {
        // Test converting to float columns and running filters.

        Object[] initSpec = {
            int.class, "+key",
            Double.class, "a?",
            Integer.class, "b?",
            String.class, "c",
            BigInteger.class, "d?",
            BigDecimal.class, "e",
        };

        Database db = Database.open(new DatabaseConfig());

        {
            Class rowType = RowTestUtils.newRowType("test.evolve.MyStuff", initSpec);
            Table table = db.openTable(rowType);

            var setKey = rowType.getMethod("key", int.class);
            var setA = rowType.getMethod("a", Double.class);
            var setB = rowType.getMethod("b", Integer.class);
            var setC = rowType.getMethod("c", String.class);
            var setD = rowType.getMethod("d", BigInteger.class);
            var setE = rowType.getMethod("e", BigDecimal.class);

            {
                var row = table.newRow();
                setKey.invoke(row, 1);
                setA.invoke(row, 123.9);
                setB.invoke(row, 1);
                setC.invoke(row, "8888888888888888888");
                setD.invoke(row, BigInteger.valueOf(100));
                setE.invoke(row, BigDecimal.ZERO);
                table.store(null, row);
            }

            {
                var row = table.newRow();
                setKey.invoke(row, 2);
                setA.invoke(row, 0.0/0.0);
                setB.invoke(row, 2);
                setC.invoke(row, "10.9");
                setD.invoke(row, (Object) null);
                setE.invoke(row, new BigDecimal("123.456"));
                table.store(null, row);
            }

            {
                var row = table.newRow();
                setKey.invoke(row, 3);
                setA.invoke(row, 100.0/0.0);
                setB.invoke(row, 11);
                setC.invoke(row, "000");
                setD.invoke(row, BigInteger.valueOf(300));
                setE.invoke(row, new BigDecimal("-123.456"));
                table.store(null, row);
            }

            {
                var row = table.newRow();
                setKey.invoke(row, 4);
                setA.invoke(row, (Object) null);
                setB.invoke(row, (Object) null);
                setC.invoke(row, "-10");
                setD.invoke(row, BigInteger.valueOf(-400));
                setE.invoke(row, new BigDecimal("999999999999999.1"));
                table.store(null, row);
            }
        }

        Object[] newSpec = {
            int.class, "+key",
            Float.class, "a?", // Double? to Float?
            float.class, "b",  // Integer? to float
            float.class, "c",  // String to float
            Float.class, "d?", // BigInteger? to Float?
            float.class, "e",  // BigDecimal to float
        };

        Class rowType = RowTestUtils.newRowType("test.evolve.MyStuff", newSpec);
        Table table = db.openTable(rowType);

        var setKey = rowType.getMethod("key", int.class);
        var getKey = rowType.getMethod("key");
        var getA = rowType.getMethod("a");
        var getB = rowType.getMethod("b");
        var getC = rowType.getMethod("c");
        var getD = rowType.getMethod("d");
        var getE = rowType.getMethod("e");

        {
            var row = table.newRow();
            setKey.invoke(row, 1);
            table.load(null, row);
            assertEquals(123.9f, getA.invoke(row));
            assertEquals(1f, getB.invoke(row));
            assertEquals(8888888888888888888f, getC.invoke(row));
            assertEquals(100f, getD.invoke(row));
            assertEquals(0f, getE.invoke(row));
        }

        {
            var row = table.newRow();
            setKey.invoke(row, 2);
            table.load(null, row);
            assertEquals(0.0f/0.0f, getA.invoke(row));
            assertEquals(2f, getB.invoke(row));
            assertEquals(10.9f, getC.invoke(row));
            assertEquals(null, getD.invoke(row));
            assertEquals(123.456f, getE.invoke(row));
        }

        {
            var row = table.newRow();
            setKey.invoke(row, 3);
            table.load(null, row);
            assertEquals(1.0f/0.0f, getA.invoke(row));
            assertEquals(11f, getB.invoke(row));
            assertEquals(0f, getC.invoke(row));
            assertEquals(300f, getD.invoke(row));
            assertEquals(-123.456f, getE.invoke(row));
        }

        {
            var row = table.newRow();
            setKey.invoke(row, 4);
            table.load(null, row);
            assertEquals(null, getA.invoke(row));
            assertEquals(0f, getB.invoke(row));
            assertEquals(-10f, getC.invoke(row));
            assertEquals(-400f, getD.invoke(row));
            assertEquals(999999999999999.0f, getE.invoke(row));
        }

        // Now test some filters.

        Scanner scanner = table.newScanner(null, "a == ?", "NaN");
        int count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            assertEquals(2, getKey.invoke(row));
            assertEquals(0.0f/0.0f, getA.invoke(row));
            assertEquals(2f, getB.invoke(row));
            assertEquals(10.9f, getC.invoke(row));
            assertEquals(null, getD.invoke(row));
            assertEquals(123.456f, getE.invoke(row));
            count++;
        }

        assertEquals(1, count);

        scanner = table.newScanner(null, "b > ?", 2);
        count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            assertEquals(3, getKey.invoke(row));
            assertEquals(1.0f/0.0f, getA.invoke(row));
            assertEquals(11f, getB.invoke(row));
            assertEquals(0f, getC.invoke(row));
            assertEquals(300f, getD.invoke(row));
            assertEquals(-123.456f, getE.invoke(row));
            count++;
        }

        assertEquals(1, count);

        scanner = table.newScanner(null, "c == ?", 0.0);
        count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            assertEquals(3, getKey.invoke(row));
            assertEquals(1.0f/0.0f, getA.invoke(row));
            assertEquals(11f, getB.invoke(row));
            assertEquals(0f, getC.invoke(row));
            assertEquals(300f, getD.invoke(row));
            assertEquals(-123.456f, getE.invoke(row));
            count++;
        }

        assertEquals(1, count);

        scanner = table.newScanner(null, "d > ?", 250);
        count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            switch ((int) getKey.invoke(row)) {
                default -> fail();
                case 2 -> {
                    assertEquals(0.0f / 0.0f, getA.invoke(row));
                    assertEquals(2f, getB.invoke(row));
                    assertEquals(10.9f, getC.invoke(row));
                    assertEquals(null, getD.invoke(row));
                    assertEquals(123.456f, getE.invoke(row));
                }
                case 3 -> {
                    assertEquals(1.0f / 0.0f, getA.invoke(row));
                    assertEquals(11f, getB.invoke(row));
                    assertEquals(0f, getC.invoke(row));
                    assertEquals(300f, getD.invoke(row));
                    assertEquals(-123.456f, getE.invoke(row));
                }
            }
            count++;
        }

        assertEquals(2, count);

        scanner = table.newScanner(null, "d == ? || d == ?", null, 300);
        count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            switch ((int) getKey.invoke(row)) {
                default -> fail();
                case 2 -> {
                    assertEquals(0.0f / 0.0f, getA.invoke(row));
                    assertEquals(2f, getB.invoke(row));
                    assertEquals(10.9f, getC.invoke(row));
                    assertEquals(null, getD.invoke(row));
                    assertEquals(123.456f, getE.invoke(row));
                }
                case 3 -> {
                    assertEquals(1.0f / 0.0f, getA.invoke(row));
                    assertEquals(11f, getB.invoke(row));
                    assertEquals(0f, getC.invoke(row));
                    assertEquals(300f, getD.invoke(row));
                    assertEquals(-123.456f, getE.invoke(row));
                }
            }
            count++;
        }

        assertEquals(2, count);

        scanner = table.newScanner(null, "e == ?", 999999999999999.0f);
        count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            assertEquals(4, getKey.invoke(row));
            assertEquals(null, getA.invoke(row));
            assertEquals(0f, getB.invoke(row));
            assertEquals(-10f, getC.invoke(row));
            assertEquals(-400f, getD.invoke(row));
            assertEquals(999999999999999.0f, getE.invoke(row));
            count++;
        }

        assertEquals(1, count);

        db.close();
    }

    @Test
    public void toBooleanCompare() throws Exception {
        // Test converting to boolean columns and running filters.

        Object[] initSpec = {
            int.class, "+key",
            Boolean.class, "a?",
            Boolean.class, "b",
            boolean.class, "c",
            String.class, "d?",
            int.class, "e",
        };

        Database db = Database.open(new DatabaseConfig());

        {
            Class rowType = RowTestUtils.newRowType("test.evolve.MyStuff", initSpec);
            Table table = db.openTable(rowType);

            var setKey = rowType.getMethod("key", int.class);
            var setA = rowType.getMethod("a", Boolean.class);
            var setB = rowType.getMethod("b", Boolean.class);
            var setC = rowType.getMethod("c", boolean.class);
            var setD = rowType.getMethod("d", String.class);
            var setE = rowType.getMethod("e", int.class);

            {
                var row = table.newRow();
                setKey.invoke(row, 1);
                setA.invoke(row, true);
                setB.invoke(row, true);
                setC.invoke(row, true);
                setD.invoke(row, "true");
                setE.invoke(row, 1);
                table.store(null, row);
            }

            {
                var row = table.newRow();
                setKey.invoke(row, 2);
                setA.invoke(row, false);
                setB.invoke(row, false);
                setC.invoke(row, false);
                setD.invoke(row, "false");
                setE.invoke(row, 0);
                table.store(null, row);
            }

            {
                var row = table.newRow();
                setKey.invoke(row, 3);
                setA.invoke(row, (Object) null);
                setB.invoke(row, false);
                setC.invoke(row, false);
                setD.invoke(row, (Object) null);
                setE.invoke(row, -1);
                table.store(null, row);
            }

            {
                var row = table.newRow();
                setKey.invoke(row, 4);
                setA.invoke(row, false);
                setB.invoke(row, false);
                setC.invoke(row, false);
                setD.invoke(row, "xxx");
                setE.invoke(row, 10);
                table.store(null, row);
            }
        }

        Object[] newSpec = {
            int.class, "+key",
            boolean.class, "a",  // Boolean? to boolean
            boolean.class, "b",  // Boolean to boolean
            Boolean.class, "c",  // boolean to Boolean
            Boolean.class, "d?", // String? to Boolean?
            Boolean.class, "e?", // int to Boolean?
        };

        Class rowType = RowTestUtils.newRowType("test.evolve.MyStuff", newSpec);
        Table table = db.openTable(rowType);

        var setKey = rowType.getMethod("key", int.class);
        var getKey = rowType.getMethod("key");
        var getA = rowType.getMethod("a");
        var getB = rowType.getMethod("b");
        var getC = rowType.getMethod("c");
        var getD = rowType.getMethod("d");
        var getE = rowType.getMethod("e");

        {
            var row = table.newRow();
            setKey.invoke(row, 1);
            table.load(null, row);
            assertEquals(true, getA.invoke(row));
            assertEquals(true, getB.invoke(row));
            assertEquals(true, getC.invoke(row));
            assertEquals(true, getD.invoke(row));
            assertEquals(true, getE.invoke(row));
        }

        {
            var row = table.newRow();
            setKey.invoke(row, 2);
            table.load(null, row);
            assertEquals(false, getA.invoke(row));
            assertEquals(false, getB.invoke(row));
            assertEquals(false, getC.invoke(row));
            assertEquals(false, getD.invoke(row));
            assertEquals(false, getE.invoke(row));
        }

        {
            var row = table.newRow();
            setKey.invoke(row, 3);
            table.load(null, row);
            assertEquals(false, getA.invoke(row));
            assertEquals(false, getB.invoke(row));
            assertEquals(false, getC.invoke(row));
            assertEquals(null, getD.invoke(row));
            assertEquals(false, getE.invoke(row));
        }

        {
            var row = table.newRow();
            setKey.invoke(row, 4);
            table.load(null, row);
            assertEquals(false, getA.invoke(row));
            assertEquals(false, getB.invoke(row));
            assertEquals(false, getC.invoke(row));
            assertEquals(null, getD.invoke(row));
            assertEquals(true, getE.invoke(row));
        }

        // Now test some filters.

        Scanner scanner = table.newScanner(null, "a == ?", "true");
        int count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            assertEquals(1, getKey.invoke(row));
            assertEquals(true, getA.invoke(row));
            assertEquals(true, getB.invoke(row));
            assertEquals(true, getC.invoke(row));
            assertEquals(true, getD.invoke(row));
            assertEquals(true, getE.invoke(row));
            count++;
        }

        assertEquals(1, count);

        scanner = table.newScanner(null, "b != ?", false);
        count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            assertEquals(1, getKey.invoke(row));
            assertEquals(true, getA.invoke(row));
            assertEquals(true, getB.invoke(row));
            assertEquals(true, getC.invoke(row));
            assertEquals(true, getD.invoke(row));
            assertEquals(true, getE.invoke(row));
            count++;
        }

        assertEquals(1, count);

        scanner = table.newScanner(null, "c == ?", false);
        count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            switch ((int) getKey.invoke(row)) {
                default -> fail();
                case 2 -> {
                    assertEquals(false, getA.invoke(row));
                    assertEquals(false, getB.invoke(row));
                    assertEquals(false, getC.invoke(row));
                    assertEquals(false, getD.invoke(row));
                    assertEquals(false, getE.invoke(row));
                }
                case 3 -> {
                    assertEquals(false, getA.invoke(row));
                    assertEquals(false, getB.invoke(row));
                    assertEquals(false, getC.invoke(row));
                    assertEquals(null, getD.invoke(row));
                    assertEquals(false, getE.invoke(row));
                }
                case 4 -> {
                    assertEquals(false, getA.invoke(row));
                    assertEquals(false, getB.invoke(row));
                    assertEquals(false, getC.invoke(row));
                    assertEquals(null, getD.invoke(row));
                    assertEquals(true, getE.invoke(row));
                }
            }
            count++;
        }

        assertEquals(3, count);

        scanner = table.newScanner(null, "d == ?", (Object) null);
        count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            switch ((int) getKey.invoke(row)) {
                default -> fail();
                case 3 -> {
                    assertEquals(false, getA.invoke(row));
                    assertEquals(false, getB.invoke(row));
                    assertEquals(false, getC.invoke(row));
                    assertEquals(null, getD.invoke(row));
                    assertEquals(false, getE.invoke(row));
                }
                case 4 -> {
                    assertEquals(false, getA.invoke(row));
                    assertEquals(false, getB.invoke(row));
                    assertEquals(false, getC.invoke(row));
                    assertEquals(null, getD.invoke(row));
                    assertEquals(true, getE.invoke(row));
                }
            }
            count++;
        }

        assertEquals(2, count);

        scanner = table.newScanner(null, "e == ?", "TRUe");
        count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            switch ((int) getKey.invoke(row)) {
                default -> fail();
                case 1 -> {
                    assertEquals(true, getA.invoke(row));
                    assertEquals(true, getB.invoke(row));
                    assertEquals(true, getC.invoke(row));
                    assertEquals(true, getD.invoke(row));
                    assertEquals(true, getE.invoke(row));
                }
                case 4 -> {
                    assertEquals(false, getA.invoke(row));
                    assertEquals(false, getB.invoke(row));
                    assertEquals(false, getC.invoke(row));
                    assertEquals(null, getD.invoke(row));
                    assertEquals(true, getE.invoke(row));
                }
            }
            count++;
        }

        assertEquals(2, count);

        db.close();
    }

    @Test
    public void toDefaultCompare() throws Exception {
        // Test filtering against default column values.

        Object[] initSpec = {
            int.class, "+key",
        };

        Database db = Database.open(new DatabaseConfig());

        {
            Class rowType = RowTestUtils.newRowType("test.evolve.MyStuff", initSpec);
            Table table = db.openTable(rowType);

            var setKey = rowType.getMethod("key", int.class);

            var row = table.newRow();
            setKey.invoke(row, 1);
            table.store(null, row);
        }

        {
            Object[] newSpec = {
                int.class, "+key",
                double.class, "a",
                String.class, "b",
                BigInteger.class, "c",
            };

            Class rowType = RowTestUtils.newRowType("test.evolve.MyStuff", newSpec);
            Table table = db.openTable(rowType);

            var setKey = rowType.getMethod("key", int.class);
            var getKey = rowType.getMethod("key");
            var setA = rowType.getMethod("a", double.class);
            var getA = rowType.getMethod("a");
            var setB = rowType.getMethod("b", String.class);
            var getB = rowType.getMethod("b");
            var setC = rowType.getMethod("c", BigInteger.class);
            var getC = rowType.getMethod("c");

            {
                var row = table.newRow();
                setKey.invoke(row, 2);
                setA.invoke(row, 1.1);
                setB.invoke(row, "one");
                setC.invoke(row, BigInteger.ONE);
                table.store(null, row);
            }

            Scanner scanner = table.newScanner(null, "a == ? && b == ? && c == ?",
                                                    0.0, "", BigInteger.ZERO);
            int count = 0;
            for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
                assertEquals(1, getKey.invoke(row));
                assertEquals(0.0, getA.invoke(row));
                assertEquals("", getB.invoke(row));
                assertEquals(BigInteger.ZERO, getC.invoke(row));
                count++;
            }

            assertEquals(1, count);
        }

        // Add some more columns.

        Object[] newSpec = {
            int.class, "+key",
            double.class, "a",
            String.class, "b",
            BigInteger.class, "c",
            BigDecimal.class, "d",
            Integer.class, "e?",
        };

        Class rowType = RowTestUtils.newRowType("test.evolve.MyStuff", newSpec);
        Table table = db.openTable(rowType);

        var setKey = rowType.getMethod("key", int.class);
        var getKey = rowType.getMethod("key");
        var setA = rowType.getMethod("a", double.class);
        var getA = rowType.getMethod("a");
        var setB = rowType.getMethod("b", String.class);
        var getB = rowType.getMethod("b");
        var setC = rowType.getMethod("c", BigInteger.class);
        var getC = rowType.getMethod("c");
        var setD = rowType.getMethod("d", BigDecimal.class);
        var getD = rowType.getMethod("d");
        var setE = rowType.getMethod("e", Integer.class);
        var getE = rowType.getMethod("e");

        {
            var row = table.newRow();
            setKey.invoke(row, 3);
            setA.invoke(row, 1.2);
            setB.invoke(row, "two");
            setC.invoke(row, BigInteger.TWO);
            setD.invoke(row, BigDecimal.valueOf(2));
            setE.invoke(row, 1);
            table.store(null, row);
        }

        Scanner scanner = table.newScanner(null, "d == ? && e == ?", BigDecimal.ZERO, null);
        int count = 0;
        for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
            switch ((int) getKey.invoke(row)) {
                default -> fail();
                case 1 -> {
                    assertEquals(0.0, getA.invoke(row));
                    assertEquals("", getB.invoke(row));
                    assertEquals(BigInteger.ZERO, getC.invoke(row));
                    assertEquals(BigDecimal.ZERO, getD.invoke(row));
                    assertEquals(null, getE.invoke(row));
                }
                case 2 -> {
                    assertEquals(1.1, getA.invoke(row));
                    assertEquals("one", getB.invoke(row));
                    assertEquals(BigInteger.ONE, getC.invoke(row));
                    assertEquals(BigDecimal.ZERO, getD.invoke(row));
                    assertEquals(null, getE.invoke(row));
                }
            }
            count++;
        }

        assertEquals(2, count);

        db.close();
    }
}
