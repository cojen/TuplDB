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

import java.lang.reflect.Array;
import java.lang.reflect.Method;

import java.math.*;

import java.util.*;

import java.util.concurrent.ThreadLocalRandom;

import org.cojen.maker.*;

import org.cojen.tupl.*;
import org.cojen.tupl.Scanner;

import org.junit.*;
import static org.junit.Assert.*;


/**
 * Easy way to test all sorts of permutations and discover bugs against the row format.
 *
 * @author Brian S O'Neill
 */
public class FuzzTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(FuzzTest.class.getName());
    }

    protected Database createTempDb() throws Exception {
        return Database.open(new DatabaseConfig());
    }

    protected void closeTempDb(Database db) throws Exception {
        db.close();
    }

    protected void installRowType(Database db, Class<?> rowType) {
    }

    @Test
    public void fuzz() throws Exception {
        var tasks = new TestUtils.TestTask[4];
        for (int i=0; i<tasks.length; i++) {
            tasks[i] = TestUtils.startTestTask(() -> fuzz(8));
        }
        for (var task : tasks) {
            task.join();
        }
    }

    private void fuzz(int n) {
        for (int i=0; i<n; i++) {
            long seed = ThreadLocalRandom.current().nextLong();
            try {
                fuzz(seed);
            } catch (Throwable e) {
                var err = new AssertionError("seed: " + seed, e);
                err.printStackTrace(System.out);
                throw err;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void fuzz(long seed) throws Exception {
        var rnd = new Random(seed);

        Database db = createTempDb();

        for (int i=0; i<100; i++) {
            Column[] columns = randomColumns(rnd);
            Class<?> rowType = randomRowType(rnd, columns);
            installRowType(db, rowType);
            Index ix = db.openIndex(rowType.getName());
            Table table = ix.asTable(rowType);

            basicTests(rowType, table);

            var cmp = keyComparator(columns, rowType);
            var set = new TreeSet<Object>(cmp);

            for (int j=0; j<10; j++) {
                var row = table.newRow();
                fillInColumns(rnd, columns, row);
                table.store(null, row);

                var clone = row.getClass().getMethod("clone").invoke(row);
                assertEquals(row, clone);
                assertEquals(row.hashCode(), clone.hashCode());
                assertEquals(row.toString(), clone.toString());

                table.load(null, row);
                assertEquals(clone, row);

                if (!set.add(row)) {
                    set.remove(row);
                    set.add(row);
                }
            }

            // Verify correct ordering.

            int count = 0;
            Iterator<Object> it = set.iterator();
            Scanner scanner = table.newScanner(null);
            for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
                count++;
                assertEquals(it.next(), row);
            }

            assertEquals(set.size(), count);

            // Verify filtering which matches the exact row.

            String filter = filterAll(rnd, columns);
            scanner = table.newScanner(null);
            for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
                filterAllMatch(table, filter, filterAllArgs(columns, row), row);
            }

            filter = filterAll2(rnd, columns);
            scanner = table.newScanner(null);
            for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
                filterAllMatch(table, filter, filterAllArgs(columns, row), row);
            }

            filter = filterAll3(rnd, columns);
            scanner = table.newScanner(null);
            for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
                filterAllMatch(table, filter, filterAllArgsIn(columns, row), row);
            }

            truncateAndClose(ix, table);
        }

        closeTempDb(db);
    }

    @SuppressWarnings("unchecked")
    private static void basicTests(Class<?> rowType, Table table) throws Exception {
        // Tests on an empty row instance and index.

        assertTrue(table.isEmpty());

        var row = table.newRow();
        assertTrue(rowType.isInstance(row));
        var clone = row.getClass().getMethod("clone").invoke(row);
        assertNotSame(row, clone);
        assertEquals(row, clone);
        assertEquals(row.hashCode(), clone.hashCode());
        assertEquals(row.toString(), clone.toString());
        assertEquals("{}", row.toString());
        table.unsetRow(clone);
        assertEquals(row, clone);

        try {
            table.load(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("isn't fully specified"));
        }

        try {
            table.exists(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("isn't fully specified"));
        }

        try {
            table.delete(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("isn't fully specified"));
        }

        try {
            table.store(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("columns are unset"));
        }

        try {
            table.exchange(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("columns are unset"));
        }

        try {
            table.insert(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("columns are unset"));
        }

        try {
            table.replace(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("columns are unset"));
        }

        try {
            table.update(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("isn't fully specified"));
        }

        try {
            table.merge(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("isn't fully specified"));
        }

        Scanner scanner = table.newScanner(null);
        assertNull(scanner.row());
        assertNull(scanner.step());
        assertNull(scanner.step(row));
        assertNull(scanner.step(null));
        scanner.close();

        Updater updater = table.newUpdater(null);
        assertNull(updater.row());
        assertNull(updater.step());
        assertNull(updater.step(row));
        try {
            updater.update();
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("current"));
        }
        try {
            updater.update(null);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("current"));
        }
        try {
            updater.delete();
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("current"));
        }
        try {
            updater.delete(null);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("current"));
        }
        updater.close();
    }

    static void truncateAndClose(Index ix, Table table) throws Exception {
        var updater = table.newUpdater(null);
        while (updater.row() != null) {
            updater.delete();
        }
        ix.drop();
    }

    static void fillInColumns(Random rnd, Column[] columns, Object row) throws Exception {
        Class<?> rowClass = row.getClass();
        for (Column c : columns) {
            Method m = rowClass.getMethod(c.name, c.type.clazz);
            m.invoke(row, c.type.randomValue(rnd));
        }
    }

    /**
     * @return names of columns which where altered
     */
    static Set<String> alterValueColumns(Random rnd, Column[] columns, Object oldRow, Object newRow)
        throws Exception
    {
        var altered = new HashSet<String>();

        Class<?> rowClass = oldRow.getClass();
        for (Column c : columns) {
            Method setter = rowClass.getMethod(c.name, c.type.clazz);
            if (c.pk == 0 && rnd.nextInt(2) == 0) {
                altered.add(c.name);
                setter.invoke(newRow, c.type.randomValue(rnd));
            } else {
                Method getter = rowClass.getMethod(c.name);
                setter.invoke(newRow, getter.invoke(oldRow));
            }
        }

        return altered;
    }

    /**
     * Returns a filter over all the columns, combined with the 'and' operator. The order of
     * the column array is shuffled as a side-effect.
     */
    private static String filterAll(Random rnd, Column[] columns) {
        Collections.shuffle(Arrays.asList(columns), rnd);
        var bob = new StringBuilder();
        for (int i=0; i<columns.length; i++) {
            if (bob.length() != 0) {
                bob.append(" && ");
            }
            Column c = columns[i];
            bob.append(c.name).append(" == ?");
        }
        return bob.toString();
    }

    /**
     * Returns a filter over all the columns, combined with the 'and' operator. The order of
     * the column array is shuffled as a side-effect.
     *
     * This variant uses ranges and explicit argument ordinals.
     */
    private static String filterAll2(Random rnd, Column[] columns) {
        Collections.shuffle(Arrays.asList(columns), rnd);
        var bob = new StringBuilder();
        for (int i=0; i<columns.length; i++) {
            if (bob.length() != 0) {
                bob.append(" && ");
            }
            Column c = columns[i];
            bob.append(c.name).append(" >= ?").append(i + 1).append(" && ");
            bob.append(c.name).append(" <= ?").append(i + 1);
        }
        return bob.toString();
    }

    /**
     * Returns a filter over all the columns, combined with the 'and' operator. The order of
     * the column array is shuffled as a side-effect.
     *
     * This variant uses the 'in' operator.
     */
    private static String filterAll3(Random rnd, Column[] columns) {
        Collections.shuffle(Arrays.asList(columns), rnd);
        var bob = new StringBuilder();
        for (int i=0; i<columns.length; i++) {
            if (bob.length() != 0) {
                bob.append(" && ");
            }
            Column c = columns[i];
            bob.append(c.name).append(" in ?");
        }
        return bob.toString();
    }

    /**
     * Fills in the arguments corresponding to the last invocation of filterAll.
     */
    private static Object[] filterAllArgs(Column[] columns, Object row) throws Exception {
        var args = new Object[columns.length];
        Class<?> rowClass = row.getClass();
        for (int i=0; i<args.length; i++) {
            args[i] = rowClass.getMethod(columns[i].name).invoke(row);
        }
        return args;
    }

    /**
     * Fills in the arguments corresponding to the last invocation of filterAll.
     */
    private static Object[] filterAllArgsIn(Column[] columns, Object row) throws Exception {
        var args = new Object[columns.length];
        Class<?> rowClass = row.getClass();
        for (int i=0; i<args.length; i++) {
            Object arg = rowClass.getMethod(columns[i].name).invoke(row);

            if (arg == null) {
                arg = new Object[1];
            } else {
                Object array = Array.newInstance(arg.getClass(), 1);
                Array.set(array, 0, arg);
                arg = array;
            }

            args[i] = arg;
        }
        return args;
    }

    /**
     * Verifies that the given filter matches only the given row, corresponding to the last
     * invocation of filterAll.
     */
    private static void filterAllMatch(Table table, String filter, Object[] args, Object row)
        throws Exception
    {
        Scanner scanner = table.newScanner(null, filter, args);

        Object matchRow = scanner.row();
        assertNotNull(matchRow);

        while (!row.equals(matchRow)) {
            if (!fuzzyEquals(row, matchRow)) {
                assertEquals(row, matchRow);
            }
            matchRow = scanner.step();
            assertNotNull(matchRow);
        }

        Object nextRow = scanner.step();

        if (nextRow != null && !fuzzyEquals(matchRow, nextRow)) {
            assertNull(nextRow);
        }

        scanner.close();
    }

    private static boolean fuzzyEquals(Object r1, Object r2) throws Exception {
        // Searches against BigDecimal columns can yield more than one result if the column
        // values only differ by scale.
        Method[] methods = r1.getClass().getMethods();
        for (Method m : methods) {
            if (m.getName().length() == 1 && m.getParameters().length == 0) {
                Object v1 = m.invoke(r1);
                Object v2 = m.invoke(r2);
                if (m.getReturnType() == BigDecimal.class) {
                    var bd1 = (BigDecimal) v1;
                    var bd2 = (BigDecimal) v2;
                    if (bd1.compareTo(bd2) != 0) {
                        return false;
                    }
                } else if (!Objects.deepEquals(v1, v2)) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * @return an interface
     */
    static Class randomRowType(Random rnd, Column[] columns) {
        ClassMaker cm = RowTestUtils.newRowTypeMaker();

        for (Column c : columns) {
            Type t = c.type;
            MethodMaker mm = cm.addMethod(t.clazz, c.name).public_().abstract_();
            if (t.nullable) {
                mm.addAnnotation(Nullable.class, true);
            }
            if (t.unsigned) {
                mm.addAnnotation(Unsigned.class, true);
            }
            cm.addMethod(void.class, c.name, t.clazz).public_().abstract_();
        }

        // Now add the primary key.

        Collections.shuffle(Arrays.asList(columns), rnd);
        int pkNum = 1 + rnd.nextInt(columns.length);
        var pkNames = new String[pkNum];

        for (int i=0; i<columns.length; i++) {
            Column c = columns[i];
            if (i < pkNames.length) {
                String name = c.name;
                if (c.type.nullable && rnd.nextBoolean()) {
                    // Nulls are low.
                    c.nullLow = true;
                    name = '!' + name;
                }
                if (rnd.nextBoolean()) {
                    // Descending order.
                    name = '-' + name;
                    c.pk = -1;
                } else {
                    // Ascending order.
                    name = '+' + name;
                    c.pk = 1;
                }
                pkNames[i] = name;
            } else {
                c.pk = 0;
            }
        }

        RowTestUtils.addPrimaryKey(cm, pkNames);

        return cm.finish();
    }

    static Column[] randomColumns(Random rnd) {
        var columns = new Column[1 + rnd.nextInt(20)];

        for (int i=0; i<columns.length; i++) {
            columns[i] = new Column(randomType(rnd), String.valueOf((char) ('a' + i)));
        }

        Collections.shuffle(Arrays.asList(columns), rnd);

        return columns;
    }

    /**
     * Returns a comparator against the row's primary key.
     */
    static Comparator<Object> keyComparator(Column[] columns, Class<?> rowType) throws Exception {
        Comparator<Object> cmp = null;

        for (Column c : columns) {
            if (c.pk == 0) {
                continue;
            }

            Method m = rowType.getMethod(c.name);
            boolean flip = c.pk < 0;

            @SuppressWarnings("unchecked")
            Comparator<Object> sub = (a, b) -> {
                try {
                    a = m.invoke(a);
                    b = m.invoke(b);
                } catch (Exception e) {
                    throw RowUtils.rethrow(e);
                }

                Class<?> aClass;

                int result;
                if (a == null) {
                    if (b == null) {
                        result = 0;
                    } else {
                        result = c.nullLow ? -1 : 1;
                    }
                } else if (b == null) {
                    result = c.nullLow ? 1 : -1;
                } else if ((aClass = a.getClass()).isArray()) {
                    if (c.type.unsigned) {
                        if (aClass == byte[].class) {
                            result = Arrays.compareUnsigned((byte[]) a, (byte[]) b);
                        } else if (aClass == short[].class) {
                            result = Arrays.compareUnsigned((short[]) a, (short[]) b);
                        } else if (aClass == int[].class) {
                            result = Arrays.compareUnsigned((int[]) a, (int[]) b);
                        } else {
                            result = Arrays.compareUnsigned((long[]) a, (long[]) b);
                        }
                    } else {
                        if (aClass == byte[].class) {
                            result = Arrays.compare((byte[]) a, (byte[]) b);
                        } else if (aClass == short[].class) {
                            result = Arrays.compare((short[]) a, (short[]) b);
                        } else if (aClass == char[].class) {
                            result = Arrays.compare((char[]) a, (char[]) b);
                        } else if (aClass == int[].class) {
                            result = Arrays.compare((int[]) a, (int[]) b);
                        } else if (aClass == long[].class) {
                            result = Arrays.compare((long[]) a, (long[]) b);
                        } else if (aClass == float[].class) {
                            result = Arrays.compare((float[]) a, (float[]) b);
                        } else if (aClass == double[].class) {
                            result = Arrays.compare((double[]) a, (double[]) b);
                        } else {
                            result = Arrays.compare((boolean[]) a, (boolean[]) b);
                        }
                    }
                } else if (c.type.unsigned) {
                    if (a instanceof Byte) {
                        result = Byte.compareUnsigned((Byte) a, (Byte) b);
                    } else if (a instanceof Short) {
                        result = Short.compareUnsigned((Short) a, (Short) b);
                    } else if (a instanceof Integer) {
                        result = Integer.compareUnsigned((Integer) a, (Integer) b);
                    } else {
                        result = Long.compareUnsigned((Long) a, (Long) b);
                    }
                } else {
                    result = ((Comparable) a).compareTo((Comparable) b);
                    if (result == 0 && a instanceof BigDecimal && !a.equals(b)) {
                        result = Integer.compare(((BigDecimal) a).scale(),
                                                 ((BigDecimal) b).scale());
                        if (((BigDecimal) a).signum() < 0) {
                            result = -result;
                        }
                    }
                }

                if (flip) {
                    result = -result;
                }

                return result;
            };

            cmp = cmp == null ? sub : cmp.thenComparing(sub);
        }

        return cmp;
    }

    static Column[] cloneColumns(Column[] columns) {
        var newColumns = new Column[columns.length];
        for (int i=0; i<columns.length; i++) {
            newColumns[i] = new Column(columns[i]);
        }
        return newColumns;
    }

    static class Column {
        final Type type;
        final String name;

        // 0: not part of primary key, 1: ascending order, -1: descending order
        int pk;

        boolean nullLow;

        Column(Type type, String name) {
            this.type = type;
            this.name = name;
        }

        Column(Column c) {
            type = c.type;
            name = c.name;
            pk = c.pk;
        }
    }

    static Type randomType(Random rnd) {
        int code = rnd.nextInt(27);

        final Class clazz = switch (code) {
            default -> throw new AssertionError();

            case 0 -> byte.class;
            case 1 -> short.class;
            case 2 -> int.class;
            case 3 -> long.class;

            case 4 -> Byte.class;
            case 5 -> Short.class;
            case 6 -> Integer.class;
            case 7 -> Long.class;

            case 8 -> byte[].class;
            case 9 -> short[].class;
            case 10 -> int[].class;
            case 11 -> long[].class;

            case 12 -> boolean.class;
            case 13 -> float.class;
            case 14 -> double.class;
            case 15 -> char.class;

            case 16 -> Boolean.class;
            case 17 -> Float.class;
            case 18 -> Double.class;
            case 19 -> Character.class;

            case 20 -> boolean[].class;
            case 21 -> float[].class;
            case 22 -> double[].class;
            case 23 -> char[].class;

            case 24 -> String.class;
            case 25 -> BigInteger.class;
            case 26 -> BigDecimal.class;
        };

        boolean nullable = false;
        if (!clazz.isPrimitive()) {
            nullable = rnd.nextBoolean();
        }

        boolean unsigned = false;
        if (code < 12) {
            unsigned = rnd.nextBoolean();
        }

        return new Type(code, clazz, nullable, unsigned);
    }

    record Type(int code, Class clazz, boolean nullable, boolean unsigned) {
        Object randomValue(Random rnd) {
            if (nullable && rnd.nextInt(5) == 0) {
                return null;
            }

            switch (code) {
                default:
                    throw new AssertionError();
                case 0:
                case 4:
                    return (byte) rnd.nextInt();
                case 1:
                case 5:
                    return (short) rnd.nextInt();
                case 2:
                case 6:
                    return rnd.nextInt();
                case 3:
                case 7:
                    return rnd.nextLong();
                case 12:
                case 16:
                    return rnd.nextBoolean();
                case 13:
                case 17:
                    return rnd.nextFloat();
                case 14:
                case 18:
                    return rnd.nextDouble();
                case 15:
                case 19:
                    return randomChar(rnd);

                case 8: {
                    var bytes = new byte[rnd.nextInt(20)];
                    rnd.nextBytes(bytes);
                    return bytes;
                }

                case 9: {
                    var a = new short[rnd.nextInt(20)];
                    for (int i = 0; i < a.length; i++) {
                        a[i] = (short) rnd.nextInt();
                    }
                    return a;
                }

                case 10: {
                    var a = new int[rnd.nextInt(20)];
                    for (int i = 0; i < a.length; i++) {
                        a[i] = rnd.nextInt();
                    }
                    return a;
                }

                case 11: {
                    var a = new long[rnd.nextInt(20)];
                    for (int i = 0; i < a.length; i++) {
                        a[i] = rnd.nextLong();
                    }
                    return a;
                }

                case 20: {
                    var a = new boolean[rnd.nextInt(20)];
                    for (int i = 0; i < a.length; i++) {
                        a[i] = rnd.nextBoolean();
                    }
                    return a;
                }

                case 21: {
                    var a = new float[rnd.nextInt(20)];
                    for (int i = 0; i < a.length; i++) {
                        a[i] = rnd.nextFloat();
                    }
                    return a;
                }

                case 22: {
                    var a = new double[rnd.nextInt(20)];
                    for (int i = 0; i < a.length; i++) {
                        a[i] = rnd.nextDouble();
                    }
                    return a;
                }

                case 23: {
                    var a = new char[rnd.nextInt(20)];
                    for (int i = 0; i < a.length; i++) {
                        a[i] = (char) rnd.nextInt();
                    }
                    return a;
                }

                case 24: {
                    var chars = new char[rnd.nextInt(20)];
                    for (int i = 0; i < chars.length; i++) {
                        chars[i] = randomChar(rnd);
                    }
                    return new String(chars);
                }

                case 25:
                    return RowTestUtils.randomBigInteger(rnd);
                case 26:
                    return RowTestUtils.randomBigDecimal(rnd);

            }
        }

        static char randomChar(Random rnd) {
            return (char) ('a' + rnd.nextInt(26));
        }
    }
}
