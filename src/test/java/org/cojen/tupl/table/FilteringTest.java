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

import java.util.*;

import java.util.concurrent.TimeUnit;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;
import org.cojen.tupl.Scanner;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class FilteringTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(FilteringTest.class.getName());
    }

    @Test
    public void arrayColumnOrderingA() throws Exception {
        // Verify basic filter ordering.

        Database db = Database.open(new DatabaseConfig());
        Table<A> table = db.openTable(A.class);

        int[] nums = {-1_000_000_000, -100, 0, 100, 1_000_000_000};

        for (int num : nums) {
            var a = new int[] {num};
            
            A row = table.newRow();
            row.id1(a);
            row.id2(a);
            row.value1(a);
            row.value2(a);
            row.value3(a);
            row.value4(a);
            row.x("");
            table.insert(null, row);
        }

        findArrayRows(table, nums, "id1", false);
        findArrayRows(table, nums, "id2", false);
        findArrayRows(table, nums, "value1", false);
        findArrayRows(table, nums, "value2", false);
        findArrayRows(table, nums, "value3", true);
        findArrayRows(table, nums, "value4", true);

        db.close();
    }

    @Test
    public void arrayColumnOrderingB() throws Exception {
        // Verify basic filter ordering.

        Database db = Database.open(new DatabaseConfig());
        Table<B> table = db.openTable(B.class);

        int[] nums = {-1_000_000_000, -100, 0, 100, 1_000_000_000};

        for (int num : nums) {
            var a = new int[] {num};
            
            B row = table.newRow();
            row.id1(a);
            row.id2(a);
            row.x("");
            table.insert(null, row);
        }

        findArrayRows(table, nums, "id1", true);
        findArrayRows(table, nums, "id2", true);

        db.close();
    }

    @Test
    public void arrayColumnOrderingC() throws Exception {
        // Verify basic filter ordering.

        Database db = Database.open(new DatabaseConfig());
        Table<C> table = db.openTable(C.class);

        int[] nums = {-1_000_000_000, -100, 0, 100, 1_000_000_000};

        for (int num : nums) {
            var a = new int[] {num};
            
            C row = table.newRow();
            row.id1(a);
            row.id2(a);
            row.x("");
            table.insert(null, row);
        }

        findArrayRows(table, nums, "id1", true);
        findArrayRows(table, nums, "id2", true);

        db.close();
    }

    @Test
    public void arrayColumnOrderingAB() throws Exception {
        // Verify basic filter ordering.

        Database db = Database.open(new DatabaseConfig());
        Table<AB> table = db.openTable(AB.class);

        byte[] nums = {-100, 0, 100};

        for (byte num : nums) {
            var a = new byte[] {num};
            
            AB row = table.newRow();
            row.id1(a);
            row.id2(a);
            row.value1(a);
            row.value2(a);
            row.value3(a);
            row.value4(a);
            row.x("");
            table.insert(null, row);
        }

        findArrayRows(table, nums, "id1", false);
        findArrayRows(table, nums, "id2", false);
        findArrayRows(table, nums, "value1", false);
        findArrayRows(table, nums, "value2", false);
        findArrayRows(table, nums, "value3", true);
        findArrayRows(table, nums, "value4", true);

        db.close();
    }

    @Test
    public void arrayColumnOrderingBB() throws Exception {
        // Verify basic filter ordering.

        Database db = Database.open(new DatabaseConfig());
        Table<BB> table = db.openTable(BB.class);

        byte[] nums = {-100, 0, 100};

        for (byte num : nums) {
            var a = new byte[] {num};
            
            BB row = table.newRow();
            row.id1(a);
            row.id2(a);
            row.x("");
            table.insert(null, row);
        }

        findArrayRows(table, nums, "id1", true);
        findArrayRows(table, nums, "id2", true);

        db.close();
    }

    @Test
    public void arrayColumnOrderingCB() throws Exception {
        // Verify basic filter ordering.

        Database db = Database.open(new DatabaseConfig());
        Table<CB> table = db.openTable(CB.class);

        byte[] nums = {-100, 0, 100};

        for (byte num : nums) {
            var a = new byte[] {num};
            
            CB row = table.newRow();
            row.id1(a);
            row.id2(a);
            row.x("");
            table.insert(null, row);
        }

        findArrayRows(table, nums, "id1", true);
        findArrayRows(table, nums, "id2", true);

        db.close();
    }

    @Test
    public void arrayColumnOrderingF() throws Exception {
        // Verify basic filter ordering.

        Database db = Database.open(new DatabaseConfig());
        Table<F> table = db.openTable(F.class);

        float[] nums = {-1_000_000_000, -100, 0, 100, 1_000_000_000};

        for (float num : nums) {
            var a = new float[] {num};
            
            F row = table.newRow();
            row.id1(a);
            row.id2(a);
            row.value1(a);
            row.value2(a);
            row.x("");
            table.insert(null, row);
        }

        findArrayRows(table, nums, "id1");
        findArrayRows(table, nums, "id2");
        findArrayRows(table, nums, "value1");
        findArrayRows(table, nums, "value2");

        db.close();
    }

    @Test
    public void arrayColumnOrderingG() throws Exception {
        // Verify basic filter ordering.

        Database db = Database.open(new DatabaseConfig());
        Table<G> table = db.openTable(G.class);

        float[] nums = {-1_000_000_000, -100, 0, 100, 1_000_000_000};

        for (float num : nums) {
            var a = new float[] {num};
            
            G row = table.newRow();
            row.id1(a);
            row.id2(a);
            row.x("");
            table.insert(null, row);
        }

        findArrayRows(table, nums, "id1");
        findArrayRows(table, nums, "id2");

        db.close();
    }

    private void findArrayRows(Table table, int[] nums, String column, boolean unsigned)
        throws Exception
    {
        findArrayRows(table, nums, column, unsigned, "<");
        findArrayRows(table, nums, column, unsigned, ">=");
        findArrayRows(table, nums, column, unsigned, ">");
        findArrayRows(table, nums, column, unsigned, "<=");
    }

    @SuppressWarnings("unchecked")
    private void findArrayRows(Table table, int[] nums, String column, boolean unsigned, String op)
        throws Exception
    {
        String filter = column + ' ' + op + " ?";
        Method method = null;

        for (int arg : nums) {
            long longArg = unsigned ? (arg & 0xffff_ffffL) : arg;

            var expect = new HashSet<Long>();
            for (int num : nums) {
                long longValue = unsigned ? (num & 0xffff_ffffL) : num;
                switch (op) {
                case "<":
                    if (longValue < longArg) {
                        expect.add(longValue);
                    }
                    break;
                case ">=":
                    if (longValue >= longArg) {
                        expect.add(longValue);
                    }
                    break;
                case ">":
                    if (longValue > longArg) {
                        expect.add(longValue);
                    }
                    break;
                case "<=":
                    if (longValue <= longArg) {
                        expect.add(longValue);
                    }
                    break;
                default: fail();
                }
            }

            Scanner scanner = table.newScanner(null, filter, new int[] {arg});
            for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
                if (method == null) {
                    method = row.getClass().getMethod(column);
                }
                int match = ((int[]) method.invoke(row))[0];
                long longMatch = unsigned ? (match & 0xffff_ffffL) : match;
                assertTrue(expect.contains(longMatch));
                expect.remove(longMatch);
            }

            assertTrue(expect.isEmpty());
        }
    }

   private void findArrayRows(Table table, byte[] nums, String column, boolean unsigned)
        throws Exception
    {
        findArrayRows(table, nums, column, unsigned, "<");
        findArrayRows(table, nums, column, unsigned, ">=");
        findArrayRows(table, nums, column, unsigned, ">");
        findArrayRows(table, nums, column, unsigned, "<=");
    }

    @SuppressWarnings("unchecked")
    private void findArrayRows(Table table, byte[] nums,
                               String column, boolean unsigned, String op)
        throws Exception
    {
        String filter = column + ' ' + op + " ?";
        Method method = null;

        for (int arg : nums) {
            long longArg = unsigned ? (arg & 0xffL) : arg;

            var expect = new HashSet<Long>();
            for (int num : nums) {
                long longValue = unsigned ? (num & 0xffL) : num;
                switch (op) {
                case "<":
                    if (longValue < longArg) {
                        expect.add(longValue);
                    }
                    break;
                case ">=":
                    if (longValue >= longArg) {
                        expect.add(longValue);
                    }
                    break;
                case ">":
                    if (longValue > longArg) {
                        expect.add(longValue);
                    }
                    break;
                case "<=":
                    if (longValue <= longArg) {
                        expect.add(longValue);
                    }
                    break;
                default: fail();
                }
            }

            Scanner scanner = table.newScanner(null, filter, new int[] {arg});
            for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
                if (method == null) {
                    method = row.getClass().getMethod(column);
                }
                byte match = ((byte[]) method.invoke(row))[0];
                long longMatch = unsigned ? (match & 0xffL) : match;
                assertTrue(expect.contains(longMatch));
                expect.remove(longMatch);
            }

            assertTrue(expect.isEmpty());
        }
    }

    private void findArrayRows(Table table, float[] nums, String column) throws Exception {
        findArrayRows(table, nums, column, "<");
        findArrayRows(table, nums, column, ">=");
        findArrayRows(table, nums, column, ">");
        findArrayRows(table, nums, column, "<=");
    }

    @SuppressWarnings("unchecked")
    private void findArrayRows(Table table, float[] nums, String column, String op)
        throws Exception
    {
        String filter = column + ' ' + op + " ?";
        Method method = null;

        for (float arg : nums) {
            var expect = new HashSet<Float>();
            for (float num : nums) {
                switch (op) {
                case "<":
                    if (num < arg) {
                        expect.add(num);
                    }
                    break;
                case ">=":
                    if (num >= arg) {
                        expect.add(num);
                    }
                    break;
                case ">":
                    if (num > arg) {
                        expect.add(num);
                    }
                    break;
                case "<=":
                    if (num <= arg) {
                        expect.add(num);
                    }
                    break;
                default: fail();
                }
            }

            Scanner scanner = table.newScanner(null, filter, new float[] {arg});
            for (Object row = scanner.row(); row != null; row = scanner.step(row)) {
                if (method == null) {
                    method = row.getClass().getMethod(column);
                }
                float match = ((float[]) method.invoke(row))[0];
                assertTrue(expect.contains(match));
                expect.remove(match);
            }

            assertTrue(expect.isEmpty());
        }
    }

    @PrimaryKey({"id1", "id2"})
    public interface A {
        int[] id1();
        void id1(int[] a);

        int[] id2();
        void id2(int[] a);

        int[] value1();
        void value1(int[] a);

        @Nullable
        int[] value2();
        void value2(int[] a);

        @Unsigned
        int[] value3();
        void value3(int[] a);

        @Nullable @Unsigned
        int[] value4();
        void value4(int[] a);

        // Last one is just a dummy.
        String x();
        void x(String x);
    }

    @PrimaryKey({"id1", "id2"})
    public interface B {
        @Unsigned
        int[] id1();
        void id1(int[] a);

        @Unsigned
        int[] id2();
        void id2(int[] a);

        // Last one is just a dummy.
        String x();
        void x(String x);
    }

    @PrimaryKey({"id1", "id2"})
    public interface C {
        @Nullable @Unsigned
        int[] id1();
        void id1(int[] a);

        @Nullable @Unsigned
        int[] id2();
        void id2(int[] a);

        // Last one is just a dummy.
        String x();
        void x(String x);
    }

    @PrimaryKey({"id1", "id2"})
    public interface AB {
        byte[] id1();
        void id1(byte[] a);

        byte[] id2();
        void id2(byte[] a);

        byte[] value1();
        void value1(byte[] a);

        @Nullable
        byte[] value2();
        void value2(byte[] a);

        @Unsigned
        byte[] value3();
        void value3(byte[] a);

        @Nullable @Unsigned
        byte[] value4();
        void value4(byte[] a);

        // Last one is just a dummy.
        String x();
        void x(String x);
    }

    @PrimaryKey({"id1", "id2"})
    public interface BB {
        @Unsigned
        byte[] id1();
        void id1(byte[] a);

        @Unsigned
        byte[] id2();
        void id2(byte[] a);

        // Last one is just a dummy.
        String x();
        void x(String x);
    }

    @PrimaryKey({"id1", "id2"})
    public interface CB {
        @Nullable @Unsigned
        byte[] id1();
        void id1(byte[] a);

        @Nullable @Unsigned
        byte[] id2();
        void id2(byte[] a);

        // Last one is just a dummy.
        String x();
        void x(String x);
    }

    @PrimaryKey({"id1", "id2"})
    public interface F {
        float[] id1();
        void id1(float[] a);

        float[] id2();
        void id2(float[] a);

        float[] value1();
        void value1(float[] a);

        @Nullable
        float[] value2();
        void value2(float[] a);

        // Last one is just a dummy.
        String x();
        void x(String x);
    }

    @PrimaryKey({"id1", "id2"})
    public interface G {
        @Nullable
        float[] id1();
        void id1(float[] a);

        @Nullable
        float[] id2();
        void id2(float[] a);

        // Last one is just a dummy.
        String x();
        void x(String x);
    }

    @Test
    public void nonFilter() throws Exception {
        // Test a "true" filter which returns everything, and with a "false" filter which
        // returns nothing.

        Database db = Database.open(new DatabaseConfig()
                                    .lockTimeout(10, TimeUnit.MILLISECONDS));
        Table<MyRow> table = db.openTable(MyRow.class);

        for (int i=0; i<3; i++) {
            MyRow row = table.newRow();
            row.id(i);
            row.name("name" + i);
            table.insert(null, row);
        }

        // This filter expression always returns true.
        var scanner = table.newScanner(null, "name >= ?1 || name < ?1");
        int count = 0;
        for (MyRow row = scanner.row(); row != null; row = scanner.step(row)) {
            assertEquals(count, row.id());
            count++;
        }
        assertEquals(3, count);

        // This filter expression always returns false.
        scanner = table.newScanner(null, "name >= ?1 && name < ?1");
        count = 0;
        for (MyRow row = scanner.row(); row != null; row = scanner.step(row)) {
            count++;
        }
        assertEquals(0, count);

        // When everything is filtered out, no lock acquisition should occur.

        Transaction txn;
        {
            txn = db.newTransaction();
            var row = table.newRow();
            row.id(1);
            table.delete(txn, row);
        }

        table.newScanner(null, "name >= ?1 && name < ?1");

        txn.reset();

        db.close();
    }

    @PrimaryKey("id")
    public interface MyRow {
        int id();
        void id(int id);

        String name();
        void name(String str);
    }

    @Test
    public void columnToColumn() throws Exception {
        // Basic column to column filtering.

        Database db = Database.open(new DatabaseConfig());
        Table<MyRow2> table = db.openTable(MyRow2.class);

        for (int i=0; i<10; i++) {
            MyRow2 row = table.newRow();
            row.id(i);
            row.name1("name" + i);
            row.name2("name" + (i & ~1));
            table.insert(null, row);
        }

        var scanner = table.newScanner(null, "name1 == name2");
        int count = 0;
        for (MyRow2 row = scanner.row(); row != null; row = scanner.step(row)) {
            assertEquals(row.name1(), row.name2());
            assertEquals(0, (row.id() & 1));
            assertEquals("name" + row.id(), row.name1());
            count++;
        }
        assertEquals(5, count);

        assertEquals(5, table.newStream(null, "name1 == name2").count());
        assertEquals(5, table.newStream(null).filter(table.predicate("name1 == name2")).count());

        scanner = table.newScanner(null, "name1 != name2 && id >= ? && name1 != name2", 6);
        count = 0;
        for (MyRow2 row = scanner.row(); row != null; row = scanner.step(row)) {
            assertNotEquals(row.name1(), row.name2());
            assertTrue((row.id() & 1) != 0);
            assertEquals("name" + row.id(), row.name1());
            assertTrue(row.id() >= 6);
            count++;
        }
        assertEquals(2, count);

        db.close();
    }

    @PrimaryKey("id")
    public interface MyRow2 {
        int id();
        void id(int id);

        String name1();
        void name1(String str);

        String name2();
        void name2(String str);
    }

    @Test
    public void filterString() throws Exception {
        Database db = Database.open(new DatabaseConfig());
        Table<MyRow3> table = db.openTable(MyRow3.class);

        String str = table.newScanner(null).toString();
        assertTrue(str.contains("unfiltered"));

        str = table.newScanner(null, "name == ?", "x").toString();
        assertTrue(str.contains("name == \"x\""));

        str = table.newScanner
            (null, "(id == ? || array1 != ?) && num1 > ?", -10, new int[] {1, 2}, 5).toString();
        assertTrue(str.contains("(id == -10 || array1 != [1, 2]) && num1 > 5"));

        str = table.newScanner
            (null, "(num2 != ? || num3 < ?) || num3 <= ?", -1, null, -1).toString();
        assertTrue(str.contains("num2 != 4294967295 || num3 < null || num3 <= -1"));

        str = table.newScanner
            (null, "num4 != ? && num4 < ? && num5 <= ?", null, -1, -1).toString();
        assertTrue(str.contains
                   ("num4 != null && num4 < 4294967295 && num5 <= 18446744073709551615"));

        str = table.newScanner(null, "!(id in ?)", new int[] {1, -2}).toString();
        assertTrue(str.contains("!(id in [1, -2])"));

        int[][] a = {{1, 2}, {3, 4}};
        str = table.newScanner(null, "array1 in ?", (Object) a).toString();
        assertTrue(str.contains("array1 in [[1, 2], [3, 4]]"));

        int[][] b = {{-1, 2}, {-3, 4}};
        str = table.newScanner(null, "array2 in ?", (Object) b).toString();
        assertTrue(str.contains("array2 in [[4294967295, 2], [4294967293, 4]]"));

        db.close();
    }

    @Test
    public void filterString2() throws Exception {
        Database db = Database.open(new DatabaseConfig());
        Table<MyRow2> table = db.openTable(MyRow2.class);

        {
            MyRow2 row = table.newRow();
            row.id(1);
            row.name1("a");
            row.name2("a");
            table.insert(null, row);
            row.id(2);
            row.name1("a");
            row.name2("b");
            table.insert(null, row);
            row.id(3);
            row.name1("b");
            row.name2("a");
            table.insert(null, row);
            row.id(4);
            row.name1("b");
            row.name2("b");
            table.insert(null, row);
        }

        String query = "(name2 == ?1 || name1 == ?2) && (name1 != ?2 || name2 != ?1)";
        try (var scanner = table.newScanner(null, query, "a", "b")) {
            var row = scanner.row();
            assertEquals("{id=1, name1=a, name2=a}", row.toString());
            row = scanner.step();
            assertEquals("{id=4, name1=b, name2=b}", row.toString());
            assertNull(scanner.step());
        }

        db.close();
    }

    @PrimaryKey("id")
    public interface MyRow3 {
        int id();
        void id(int id);

        String name();
        void name(String str);

        int[] array1();
        void array1(int[] a);

        @Unsigned
        int[] array2();
        void array2(int[] a);

        Integer num1();
        void num1(Integer n);

        @Unsigned
        Integer num2();
        void num2(Integer n);

        @Nullable
        Integer num3();
        void num3(Integer n);

        @Nullable @Unsigned
        Integer num4();
        void num4(Integer n);

        @Unsigned
        long num5();
        void num5(long n);
    }
}
