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

import java.lang.reflect.Array;
import java.lang.reflect.Method;

import java.math.*;

import java.util.*;

import java.util.concurrent.ThreadLocalRandom;

import org.cojen.maker.*;

import org.cojen.tupl.*;

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

    private static void fuzz(int n) {
        for (int i=0; i<n; i++) {
            long seed = ThreadLocalRandom.current().nextLong();
            try {
                fuzz(seed);
            } catch (Throwable e) {
                throw new AssertionError("seed: " + seed, e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void fuzz(long seed) throws Exception {
        var rnd = new Random(seed);

        Database db = Database.open(new DatabaseConfig());

        for (int i=0; i<100; i++) {
            Column[] columns = randomColumns(rnd);
            Class<?> rowType = randomRowType(rnd, columns);
            Index ix = db.openIndex(rowType.getName());
            Table rowView = ix.asTable(rowType);

            basicTests(rowType, rowView);

            var cmp = keyComparator(columns, rowType);
            var set = new TreeSet<Object>(cmp);

            for (int j=0; j<10; j++) {
                var row = rowView.newRow();
                fillInColumns(rnd, columns, row);
                rowView.store(null, row);

                var clone = row.getClass().getMethod("clone").invoke(row);
                assertEquals(row, clone);
                assertEquals(row.hashCode(), clone.hashCode());
                assertEquals(row.toString(), clone.toString());

                rowView.load(null, row);
                assertEquals(clone, row);

                if (!set.add(row)) {
                    set.remove(row);
                    set.add(row);
                }
            }

            // Verify correct ordering.

            int count = 0;
            Iterator<Object> it = set.iterator();
            RowScanner scanner = rowView.newRowScanner(null);
            for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
                count++;
                assertEquals(it.next(), row);
            }

            assertEquals(set.size(), count);

            // Verify filtering which matches the exact row.

            String filter = filterAll(rnd, columns);
            scanner = rowView.newRowScanner(null);
            for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
                filterAllMatch(rowView, filter, filterAllArgs(columns, row), row);
            }

            filter = filterAll2(rnd, columns);
            scanner = rowView.newRowScanner(null);
            for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
                filterAllMatch(rowView, filter, filterAllArgs(columns, row), row);
            }

            filter = filterAll3(rnd, columns);
            scanner = rowView.newRowScanner(null);
            for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
                filterAllMatch(rowView, filter, filterAllArgsIn(columns, row), row);
            }

            truncateAndClose(ix, rowView);
        }

        db.close();
    }

    @SuppressWarnings("unchecked")
    private static void basicTests(Class<?> rowType, Table rowView) throws Exception {
        // Tests on an empty row instance and index.

        assertTrue(rowView.isEmpty());

        var row = rowView.newRow();
        assertTrue(rowType.isInstance(row));
        var clone = row.getClass().getMethod("clone").invoke(row);
        assertTrue(row != clone);
        assertEquals(row, clone);
        assertEquals(row.hashCode(), clone.hashCode());
        assertEquals(row.toString(), clone.toString());
        assertEquals(rowType.getName() + "{}", row.toString());
        rowView.reset(clone);
        assertEquals(row, clone);

        try {
            rowView.load(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().indexOf("isn't fully specified") >= 0);
        }

        try {
            rowView.exists(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().indexOf("isn't fully specified") >= 0);
        }

        try {
            rowView.delete(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().indexOf("isn't fully specified") >= 0);
        }

        try {
            rowView.store(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().indexOf("columns are unset") >= 0);
        }

        try {
            rowView.exchange(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().indexOf("columns are unset") >= 0);
        }

        try {
            rowView.insert(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().indexOf("columns are unset") >= 0);
        }

        try {
            rowView.replace(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().indexOf("columns are unset") >= 0);
        }

        try {
            rowView.update(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().indexOf("isn't fully specified") >= 0);
        }

        try {
            rowView.merge(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().indexOf("isn't fully specified") >= 0);
        }

        RowScanner scanner = rowView.newRowScanner(null);
        assertNull(scanner.row());
        assertNull(scanner.step());
        assertNull(scanner.step(row));
        try {
            scanner.step(null);
            fail();
        } catch (NullPointerException e) {
        }
        scanner.close();

        RowUpdater updater = rowView.newRowUpdater(null);
        assertNull(updater.row());
        assertNull(updater.step());
        assertNull(updater.step(row));
        try {
            updater.update();
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().indexOf("current") >= 0);
        }
        try {
            updater.update(null);
            fail();
        } catch (NullPointerException e) {
        }
        try {
            updater.delete();
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().indexOf("current") >= 0);
        }
        try {
            updater.delete(null);
            fail();
        } catch (NullPointerException e) {
        }
        updater.close();
    }

    private static void truncateAndClose(Index ix, Table rv) throws Exception {
        var updater = rv.newRowUpdater(null);
        while (updater.row() != null) {
            updater.delete();
        }
        ix.drop();
    }

    private static void fillInColumns(Random rnd, Column[] columns, Object row) throws Exception {
        Class<?> rowClass = row.getClass();
        for (Column c : columns) {
            Method m = rowClass.getMethod(c.name, c.type.clazz);
            m.invoke(row, c.type.randomValue(rnd));
        }
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
            bob.append(c.name).append(" >= ?").append(i).append(" && ");
            bob.append(c.name).append(" <= ?").append(i);
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
    private static void filterAllMatch(Table rv, String filter, Object[] args, Object row)
        throws Exception
    {
        RowScanner scanner = rv.newRowScanner(null, filter, args);
        Object matchRow = scanner.row();
        assertNotNull(matchRow);
        assertEquals(row, matchRow);
        Object nextRow = scanner.step();
        assertNull(nextRow);
        scanner.close();
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

        for (int i=0; i<pkNames.length; i++) {
            pkNames[i] = columns[i].name;
            if (rnd.nextBoolean()) {
                // Descending order.
                pkNames[i] = '-' + pkNames[i];
                columns[i].pk = -1;
            } else {
                columns[i].pk = 1;
            }
        }

        AnnotationMaker am = cm.addAnnotation(PrimaryKey.class, true);
        am.put("value", pkNames);

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
                        result = 1;
                    }
                } else if (b == null) {
                    result = -1;
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

    static class Column {
        final Type type;
        final String name;

        // 0: not part of primary key, 1: ascending order, -1: descending order
        int pk;

        Column(Type type, String name) {
            this.type = type;
            this.name = name;
        }
    }

    static Type randomType(Random rnd) {
        int code = rnd.nextInt(27);

        final Class clazz;

        switch (code) {
        default: throw new AssertionError();
        case 0: clazz = byte.class; break;
        case 1: clazz = short.class; break;
        case 2: clazz = int.class; break;
        case 3: clazz = long.class; break;

        case 4: clazz = Byte.class; break;
        case 5: clazz = Short.class; break;
        case 6: clazz = Integer.class; break;
        case 7: clazz = Long.class; break;

        case 8: clazz = byte[].class; break;
        case 9: clazz = short[].class; break;
        case 10: clazz = int[].class; break;
        case 11: clazz = long[].class; break;

        case 12: clazz = boolean.class; break;
        case 13: clazz = float.class; break;
        case 14: clazz = double.class; break;
        case 15: clazz = char.class; break;

        case 16: clazz = Boolean.class; break;
        case 17: clazz = Float.class; break;
        case 18: clazz = Double.class; break;
        case 19: clazz = Character.class; break;

        case 20: clazz = boolean[].class; break;
        case 21: clazz = float[].class; break;
        case 22: clazz = double[].class; break;
        case 23: clazz = char[].class; break;

        case 24: clazz = String.class; break;
        case 25: clazz = BigInteger.class; break;
        case 26: clazz = BigDecimal.class; break;
        }

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

    static class Type {
        final int code;
        final Class clazz;
        final boolean nullable;
        final boolean unsigned;

        Type(int code, Class clazz, boolean nullable, boolean unsigned) {
            this.code = code;
            this.clazz = clazz;
            this.nullable = nullable;
            this.unsigned = unsigned;
        }

        Object randomValue(Random rnd) {
            if (nullable && rnd.nextInt(5) == 0) {
                return null;
            }

            switch (code) {
            default: throw new AssertionError();
            case 0: case 4: return (byte) rnd.nextInt();
            case 1: case 5: return (short) rnd.nextInt();
            case 2: case 6: return rnd.nextInt();
            case 3: case 7: return rnd.nextLong();
            case 12: case 16: return rnd.nextBoolean();
            case 13: case 17: return rnd.nextFloat();
            case 14: case 18: return rnd.nextDouble();
            case 15: case 19: return randomChar(rnd);

            case 8: {
                var bytes = new byte[rnd.nextInt(20)];
                rnd.nextBytes(bytes);
                return bytes;
            }

            case 9: {
                var a = new short[rnd.nextInt(20)];
                for (int i=0; i<a.length; i++) {
                    a[i] = (short) rnd.nextInt();
                }
                return a;
            }

            case 10: {
                var a = new int[rnd.nextInt(20)];
                for (int i=0; i<a.length; i++) {
                    a[i] = rnd.nextInt();
                }
                return a;
            }

            case 11: {
                var a = new long[rnd.nextInt(20)];
                for (int i=0; i<a.length; i++) {
                    a[i] = rnd.nextLong();
                }
                return a;
            }

            case 20: {
                var a = new boolean[rnd.nextInt(20)];
                for (int i=0; i<a.length; i++) {
                    a[i] = rnd.nextBoolean();
                }
                return a;
            }

            case 21: {
                var a = new float[rnd.nextInt(20)];
                for (int i=0; i<a.length; i++) {
                    a[i] = rnd.nextFloat();
                }
                return a;
            }

            case 22: {
                var a = new double[rnd.nextInt(20)];
                for (int i=0; i<a.length; i++) {
                    a[i] = rnd.nextDouble();
                }
                return a;
            }

            case 23: {
                var a = new char[rnd.nextInt(20)];
                for (int i=0; i<a.length; i++) {
                    a[i] = (char) rnd.nextInt();
                }
                return a;
            }

            case 24: {
                var chars = new char[rnd.nextInt(20)];
                for (int i=0; i<chars.length; i++) {
                    chars[i] = randomChar(rnd);
                }
                return new String(chars);
            }

            case 25: return RowTestUtils.randomBigInteger(rnd);
            case 26: return RowTestUtils.randomBigDecimal(rnd);

            }
        }

        static char randomChar(Random rnd) {
            return (char) ('a' + rnd.nextInt(26));
        }
    }
}
