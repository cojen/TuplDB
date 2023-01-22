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

package org.cojen.tupl.remote;

import java.net.ServerSocket;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import org.cojen.tupl.rows.RowTestUtils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RemoteRowCrudTest {
    // FIXME: extend RowCrudTest and don't duplicate the tests

    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RemoteRowCrudTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
        mServerDb = Database.open(new DatabaseConfig());
        Server server = mServerDb.newServer();

        // FIXME: create a subclass that forces RemoteProxyMaker to make a converter
        if (false) {
            // Use a different row type definition on the server (num1 is different).
            Class<?> rowType = RowTestUtils.newRowType(TestRow.class.getName(),
                                                       long.class, "+id",
                                                       String.class, "str1",
                                                       String.class, "str2?",
                                                       long.class, "num1");

            server.classResolver(name -> name.equals(rowType.getName()) ? rowType : null);
        }

        var ss = new ServerSocket(0);
        server.acceptAll(ss, 123456);

        mDb = Database.connect(ss.getLocalSocketAddress(), 111, 123456);
        mTable = mDb.openTable(TestRow.class);
    }

    @After
    public void teardown() throws Exception {
        mDb.close();
        mDb = null;
        mServerDb.close();
        mServerDb = null;
    }

    private Database mServerDb;
    private Database mDb;
    private Table<TestRow> mTable;

    @PrimaryKey("id")
    public interface TestRow {
        long id();
        void id(long id);

        String str1();
        void str1(String str);

        @Nullable
        String str2();
        void str2(String str);

        int num1();
        void num1(int num);
    }

    @Test
    public void basic() throws Exception {
        assertEquals(TestRow.class, mTable.rowType());
        assertTrue(mTable.isEmpty());
        assertSame(mTable, mDb.openTable(TestRow.class));

        TestRow row = mTable.newRow();
        assertTrue(row.toString().endsWith("TestRow{}"));
        mTable.unsetRow(row);
        assertTrue(row.toString().endsWith("TestRow{}"));

        try {
            row.id();
            fail();
        } catch (UnsetColumnException e) {
        }

        try {
            row.str1();
            fail();
        } catch (UnsetColumnException e) {
        }

        try {
            row.str2();
            fail();
        } catch (UnsetColumnException e) {
        }

        try {
            row.num1();
            fail();
        } catch (UnsetColumnException e) {
        }

        row.str1("hello");
        assertEquals("hello", row.str1());

        try {
            mTable.load(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertEquals("Primary key isn't fully specified", e.getMessage());
        }

        try {
            mTable.exists(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertEquals("Primary key isn't fully specified", e.getMessage());
        }

        assertEquals("hello", row.str1());

        row.id(1);
        assertFalse(mTable.load(null, row));

        assertTrue(row.toString().endsWith("TestRow{*id=1}"));

        try {
            row.str1();
            fail();
        } catch (UnsetColumnException e) {
        }

        assertEquals(1, row.id());

        try {
            mTable.store(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertEquals("Some required columns are unset: num1, str1, str2", e.getMessage());
        }

        assertFalse(mTable.delete(null, row));
        assertTrue(row.toString().endsWith("TestRow{*id=1}"));

        row.str1("hello");
        row.str2(null);
        row.num1(100);
        assertTrue(row.toString().endsWith("TestRow{*id=1, *num1=100, *str1=hello, *str2=null}"));

        assertTrue(mTable.insert(null, row));
        assertTrue(row.toString().endsWith("TestRow{id=1, num1=100, str1=hello, str2=null}"));
        assertFalse(mTable.isEmpty());
        assertTrue(mTable.exists(null, row));
        assertTrue(mTable.load(null, row));
        assertTrue(row.toString().endsWith("TestRow{id=1, num1=100, str1=hello, str2=null}"));

        TestRow row2 = mTable.newRow();
        row2.id(1);
        assertTrue(mTable.load(null, row2));
        assertEquals(row, row2);
        assertEquals(row.hashCode(), row2.hashCode());
        assertEquals(row.toString(), row2.toString());
        assertFalse(mTable.insert(null, row2));

        row2.str2("world");
        assertTrue(mTable.update(null, row2));
        assertTrue(row2.toString().endsWith("TestRow{id=1, num1=100, str1=hello, str2=world}"));

        row.str1("howdy");
        assertTrue(mTable.update(null, row));
        assertTrue(row.toString().endsWith("TestRow{id=1, num1=100, str1=howdy, str2=null}"));
        row.str1("hi");
        assertTrue(mTable.merge(null, row));
        assertTrue(row.toString().endsWith("TestRow{id=1, num1=100, str1=hi, str2=world}"));

        row2.num1(-555);
        assertTrue(mTable.update(null, row2));
        assertTrue(row2.toString().endsWith("TestRow{id=1, num1=-555, str1=hello, str2=world}"));

        mTable.unsetRow(row2);
        row2.id(1);
        row2.num1(999);
        assertTrue(mTable.update(null, row2));
        assertTrue(row2.toString().endsWith("TestRow{id=1, num1=999}"));

        row2.str2("everyone");
        assertTrue(mTable.merge(null, row2));
        assertTrue(row2.toString().endsWith("TestRow{id=1, num1=999, str1=hi, str2=everyone}"));

        assertTrue(mTable.replace(null, row));
        mTable.load(null, row2);
        assertTrue(row2.toString().endsWith("TestRow{id=1, num1=100, str1=hi, str2=world}"));

        assertTrue(mTable.delete(null, row2));
        assertFalse(mTable.delete(null, row));
        assertTrue(mTable.isEmpty());

        assertTrue(row.toString().endsWith("TestRow{id=1, num1=100, str1=hi, str2=world}"));
        assertTrue(row2.toString().endsWith("TestRow{id=1, num1=100, str1=hi, str2=world}"));

        Transaction txn = mTable.newTransaction(null);
        assertTrue(mTable.insert(txn, row));
        assertTrue(mTable.exists(txn, row2));
        assertFalse(mTable.isEmpty());
        txn.reset(); // rollback

        assertFalse(mTable.exists(null, row2));
        assertTrue(mTable.isEmpty());

        mTable.store(null, row);
        row2.str1("hello");
        TestRow row3 = mTable.exchange(null, row2);
        assertEquals(row, row3);
        mTable.delete(null, row3);
        assertNull(mTable.exchange(null, row2));
    }

    @Test
    public void loadSideEffect() throws Exception {
        // When a load operation returns false, the state of all non-key fields must be unset,
        // but all the fields must remain unchanged.

        TestRow row = mTable.newRow();
        row.id(10);
        row.str1("hello");
        row.str2("world");
        row.num1(123);
        TestRow copy = mTable.cloneRow(row);
        assertEquals(row, copy);
        TestRow copy2 = mTable.newRow();
        mTable.copyRow(row, copy2);
        assertEquals(copy, copy2);
        assertFalse(mTable.load(null, row));
        assertTrue(row.toString().contains("TestRow{*id=10}"));

        assertNotEquals(row.toString(), copy.toString());
        assertEquals(0, mTable.comparator("+id+str1+str2+num1").compare(row, copy));
    }

    @Test
    public void updateEntry() throws Exception {
        Index ix = mDb.openIndex("test");
        Table<Entry> table = ix.asTable(Entry.class);

        Entry e = table.newRow();
        try {
            table.update(null, e);
            fail();
        } catch (IllegalStateException ex) {
            assertTrue(ex.getMessage().contains("Primary key isn't fully specified"));
        }

        e.key("hello".getBytes());
        assertFalse(table.update(null, e));

        e.value("world".getBytes());
        assertFalse(table.update(null, e));

        assertTrue(table.insert(null, e));

        e.value("world!".getBytes());
        assertTrue(table.update(null, e));

        assertArrayEquals("world!".getBytes(), ix.load(null, e.key()));

        table.unsetRow(e);
        e.key("hello".getBytes());

        assertTrue(table.merge(null, e));
        assertArrayEquals("world!".getBytes(), e.value());

        e.value("world!!!".getBytes());
        assertTrue(table.merge(null, e));
        assertArrayEquals("world!!!".getBytes(), e.value());

        assertArrayEquals("world!!!".getBytes(), ix.load(null, e.key()));
    }
}
