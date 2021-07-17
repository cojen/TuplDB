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

import java.util.*;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

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
        Table<A> view = db.openTable(A.class);

        int[] nums = {-1_000_000_000, -100, 0, 100, 1_000_000_000};

        for (int num : nums) {
            var a = new int[] {num};
            
            A row = view.newRow();
            row.id1(a);
            row.id2(a);
            row.value1(a);
            row.value2(a);
            row.value3(a);
            row.value4(a);
            row.x("");
            assertTrue(view.insert(null, row));
        }

        findArrayRows(view, nums, "id1", false);
        findArrayRows(view, nums, "id2", false);
        findArrayRows(view, nums, "value1", false);
        findArrayRows(view, nums, "value2", false);
        findArrayRows(view, nums, "value3", true);
        findArrayRows(view, nums, "value4", true);
    }

    @Test
    public void arrayColumnOrderingB() throws Exception {
        // Verify basic filter ordering.

        Database db = Database.open(new DatabaseConfig());
        Table<B> view = db.openTable(B.class);

        int[] nums = {-1_000_000_000, -100, 0, 100, 1_000_000_000};

        for (int num : nums) {
            var a = new int[] {num};
            
            B row = view.newRow();
            row.id1(a);
            row.id2(a);
            row.x("");
            assertTrue(view.insert(null, row));
        }

        findArrayRows(view, nums, "id1", true);
        findArrayRows(view, nums, "id2", true);
    }

    @Test
    public void arrayColumnOrderingC() throws Exception {
        // Verify basic filter ordering.

        Database db = Database.open(new DatabaseConfig());
        Table<C> view = db.openTable(C.class);

        int[] nums = {-1_000_000_000, -100, 0, 100, 1_000_000_000};

        for (int num : nums) {
            var a = new int[] {num};
            
            C row = view.newRow();
            row.id1(a);
            row.id2(a);
            row.x("");
            assertTrue(view.insert(null, row));
        }

        findArrayRows(view, nums, "id1", true);
        findArrayRows(view, nums, "id2", true);
    }

    @Test
    public void arrayColumnOrderingAB() throws Exception {
        // Verify basic filter ordering.

        Database db = Database.open(new DatabaseConfig());
        Table<AB> view = db.openTable(AB.class);

        byte[] nums = {-100, 0, 100};

        for (byte num : nums) {
            var a = new byte[] {num};
            
            AB row = view.newRow();
            row.id1(a);
            row.id2(a);
            row.value1(a);
            row.value2(a);
            row.value3(a);
            row.value4(a);
            row.x("");
            assertTrue(view.insert(null, row));
        }

        findArrayRows(view, nums, "id1", false);
        findArrayRows(view, nums, "id2", false);
        findArrayRows(view, nums, "value1", false);
        findArrayRows(view, nums, "value2", false);
        findArrayRows(view, nums, "value3", true);
        findArrayRows(view, nums, "value4", true);
    }

    @Test
    public void arrayColumnOrderingBB() throws Exception {
        // Verify basic filter ordering.

        Database db = Database.open(new DatabaseConfig());
        Table<BB> view = db.openTable(BB.class);

        byte[] nums = {-100, 0, 100};

        for (byte num : nums) {
            var a = new byte[] {num};
            
            BB row = view.newRow();
            row.id1(a);
            row.id2(a);
            row.x("");
            assertTrue(view.insert(null, row));
        }

        findArrayRows(view, nums, "id1", true);
        findArrayRows(view, nums, "id2", true);
    }

    @Test
    public void arrayColumnOrderingCB() throws Exception {
        // Verify basic filter ordering.

        Database db = Database.open(new DatabaseConfig());
        Table<CB> view = db.openTable(CB.class);

        byte[] nums = {-100, 0, 100};

        for (byte num : nums) {
            var a = new byte[] {num};
            
            CB row = view.newRow();
            row.id1(a);
            row.id2(a);
            row.x("");
            assertTrue(view.insert(null, row));
        }

        findArrayRows(view, nums, "id1", true);
        findArrayRows(view, nums, "id2", true);
    }

    @Test
    public void arrayColumnOrderingF() throws Exception {
        // Verify basic filter ordering.

        Database db = Database.open(new DatabaseConfig());
        Table<F> view = db.openTable(F.class);

        float[] nums = {-1_000_000_000, -100, 0, 100, 1_000_000_000};

        for (float num : nums) {
            var a = new float[] {num};
            
            F row = view.newRow();
            row.id1(a);
            row.id2(a);
            row.value1(a);
            row.value2(a);
            row.x("");
            assertTrue(view.insert(null, row));
        }

        findArrayRows(view, nums, "id1");
        findArrayRows(view, nums, "id2");
        findArrayRows(view, nums, "value1");
        findArrayRows(view, nums, "value2");
    }

    @Test
    public void arrayColumnOrderingG() throws Exception {
        // Verify basic filter ordering.

        Database db = Database.open(new DatabaseConfig());
        Table<G> view = db.openTable(G.class);

        float[] nums = {-1_000_000_000, -100, 0, 100, 1_000_000_000};

        for (float num : nums) {
            var a = new float[] {num};
            
            G row = view.newRow();
            row.id1(a);
            row.id2(a);
            row.x("");
            assertTrue(view.insert(null, row));
        }

        findArrayRows(view, nums, "id1");
        findArrayRows(view, nums, "id2");
    }

    private void findArrayRows(Table view, int[] nums, String column, boolean unsigned)
        throws Exception
    {
        findArrayRows(view, nums, column, unsigned, "<");
        findArrayRows(view, nums, column, unsigned, ">=");
        findArrayRows(view, nums, column, unsigned, ">");
        findArrayRows(view, nums, column, unsigned, "<=");
    }

    @SuppressWarnings("unchecked")
    private void findArrayRows(Table view, int[] nums, String column, boolean unsigned, String op)
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

            RowScanner scanner = view.newRowScanner(null, filter, new int[] {arg});
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

   private void findArrayRows(Table view, byte[] nums, String column, boolean unsigned)
        throws Exception
    {
        findArrayRows(view, nums, column, unsigned, "<");
        findArrayRows(view, nums, column, unsigned, ">=");
        findArrayRows(view, nums, column, unsigned, ">");
        findArrayRows(view, nums, column, unsigned, "<=");
    }

    @SuppressWarnings("unchecked")
    private void findArrayRows(Table view, byte[] nums,
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

            RowScanner scanner = view.newRowScanner(null, filter, new int[] {arg});
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

    private void findArrayRows(Table view, float[] nums, String column) throws Exception {
        findArrayRows(view, nums, column, "<");
        findArrayRows(view, nums, column, ">=");
        findArrayRows(view, nums, column, ">");
        findArrayRows(view, nums, column, "<=");
    }

    @SuppressWarnings("unchecked")
    private void findArrayRows(Table view, float[] nums, String column, String op)
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

            RowScanner scanner = view.newRowScanner(null, filter, new float[] {arg});
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
}
