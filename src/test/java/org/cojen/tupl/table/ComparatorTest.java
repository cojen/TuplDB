/*
 *  Copyright (C) 2022 Cojen.org
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

import java.math.BigInteger;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class ComparatorTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ComparatorTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
        mDb = Database.open(new DatabaseConfig());
        mTable = mDb.openTable(TestRow.class);
    }

    @After
    public void teardown() throws Exception {
        mDb.close();
        mDb = null;
    }

    private Database mDb;
    private Table<TestRow> mTable;

    @PrimaryKey("id")
    public interface TestRow extends Comparable<TestRow> {
        long id();
        void id(long id);

        String str1();
        void str1(String str);

        @Nullable
        String str2();
        void str2(String str);

        @Nullable
        Integer num1();
        void num1(Integer num);

        @Unsigned
        int num2();
        void num2(int num);

        @Nullable @Unsigned
        Integer num3();
        void num3(Integer num);

        BigInteger num4();
        void num4(BigInteger num);

        int[] array1();
        void array1(int[] a);

        @Unsigned
        byte[] array2();
        void array2(byte[] a);
    }

    @Test
    public void bad() throws Exception {
        String[] bad = {"+", "!id", "--id", "id"};

        for (String spec : bad) {
            try {
                mTable.comparator(spec);
                fail();
            } catch (IllegalArgumentException e) {
                assertTrue(e.getMessage().contains("specification"));
            }
        }

        try {
            mTable.comparator("+idx-foo");
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("Unknown column"));
        }
    }

    @Test
    public void basic() throws Exception {
        TestRow row1 = mTable.newRow();
        row1.id(1);
        row1.str1("r1");
        row1.str2(null);
        row1.num1(123);
        row1.num2(123);
        row1.num3(null);
        row1.num4(BigInteger.valueOf(10));
        row1.array1(new int[] {1});
        row1.array2(new byte[] {1});

        TestRow row2 = mTable.newRow();
        row2.id(2);
        row2.str1("r2");
        row2.str2("str2");
        row2.num1(null);
        row2.num2(-1);
        row2.num3(-1);
        row2.num4(BigInteger.valueOf(20));
        row1.array1(new int[] {2});
        row1.array2(new byte[] {2});

        assertEquals(-1, row1.compareTo(row2));

        assertEquals( 0, mTable.comparator("").compare(row1, row2));

        assertEquals(-1, mTable.comparator("+id").compare(row1, row2));
        assertEquals( 1, mTable.comparator("-id").compare(row1, row2));

        assertEquals(-1, mTable.comparator("+str1").compare(row1, row2));
        assertEquals(-1, mTable.comparator("+!str1").compare(row1, row2));

        assertEquals( 1, mTable.comparator("+str2").compare(row1, row2));
        assertEquals(-1, mTable.comparator("+!str2").compare(row1, row2));
        assertEquals(-1, mTable.comparator("-str2").compare(row1, row2));
        assertEquals( 1, mTable.comparator("-!str2").compare(row1, row2));

        assertEquals(-1, mTable.comparator("+num1").compare(row1, row2));
        assertEquals( 1, mTable.comparator("+!num1").compare(row1, row2));
        assertEquals( 1, mTable.comparator("-num1").compare(row1, row2));
        assertEquals(-1, mTable.comparator("-!num1").compare(row1, row2));

        assertEquals(-1, mTable.comparator("+num2").compare(row1, row2));

        assertEquals( 1, mTable.comparator("+num3").compare(row1, row2));
        assertEquals(-1, mTable.comparator("+!num3").compare(row1, row2));

        assertEquals(-1, mTable.comparator("+num4").compare(row1, row2));
        assertEquals( 1, mTable.comparator("-num4").compare(row1, row2));

        assertEquals(-1, mTable.comparator("+array1").compare(row1, row2));
        assertEquals( 1, mTable.comparator("-array1").compare(row1, row2));

        assertEquals(-1, mTable.comparator("+array2").compare(row1, row2));
        assertEquals( 1, mTable.comparator("-array2").compare(row1, row2));
    }

    @Test
    public void composite() throws Exception {
        TestRow row1 = mTable.newRow();
        row1.str1("r1");

        TestRow row2 = mTable.newRow();
        row2.str1("r2");

        assertEquals(0, row1.compareTo(row2));

        assertEquals( 0, mTable.comparator("+id+str2").compare(row1, row2));
        assertEquals(-1, mTable.comparator("+id+str2+str1+num1").compare(row1, row2));
        assertEquals(-1, mTable.comparator("+id+num1+str2+str1").compare(row1, row2));

        // Test caching and canonicalization.
        var cmp1 = mTable.comparator("+id+str1");
        var cmp2 = mTable.comparator("+id+str1+id");
        var cmp3 = mTable.comparator("+!id+str1");

        assertSame(cmp1, cmp2);
        assertSame(cmp2, cmp3);
    }
}
