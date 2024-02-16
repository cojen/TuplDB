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

import org.cojen.tupl.table.RowTestUtils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RemoteMissingClientColumnTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RemoteMissingClientColumnTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
        mServerDb = Database.open(new DatabaseConfig());
        Server server = mServerDb.newServer();

        // Use a different row type definition on the server which has an additional column.
        mServerRowType = RowTestUtils.newRowType(TestRow.class.getName(),
                                                 long.class, "+id",
                                                 String.class, "str1",
                                                 int.class, "num1",
                                                 int.class, "num2");

        server.classResolver(name -> name.equals(mServerRowType.getName()) ? mServerRowType : null);

        var ss = new ServerSocket(0);
        server.acceptAll(ss, 123456);

        mDb = Database.connect(ss.getLocalSocketAddress(), null, 111, 123456);
        mTable = mDb.openTable(TestRow.class);
    }

    @After
    public void teardown() throws Exception {
        if (mDb != null) {
            mDb.close();
            mDb = null;
        }
        if (mServerDb != null) {
            mServerDb.close();
            mServerDb = null;
        }
    }

    private Database mServerDb;
    private Class<?> mServerRowType;
    private Database mDb;
    private Table<TestRow> mTable;

    @PrimaryKey("id")
    public interface TestRow {
        long id();
        void id(long id);

        String str1();
        void str1(String s);

        int num2();
        void num2(int n);
    }

    @Test
    public void store() throws Exception {
        try {
            TestRow row = mTable.newRow();
            row.id(1);
            row.str1("str1");
            row.num2(2);
            mTable.store(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("unset"));
            assertTrue(e.getMessage().contains("num1"));
        }
    }

    @Test
    public void load() throws Exception {
        storeServerRow();

        {
            TestRow row = mTable.newRow();
            row.id(1);
            mTable.load(null, row);
            assertEquals("str1", row.str1());
            assertEquals(2, row.num2());
        }

        try (Scanner<TestRow> scanner = mTable.newScanner(null)) {
            TestRow row = scanner.row();
            assertEquals("str1", row.str1());
            assertEquals(2, row.num2());
        }

        try (Updater<TestRow> updater = mTable.newUpdater(null)) {
            TestRow row = updater.row();
            assertEquals("str1", row.str1());
            assertEquals(2, row.num2());
        }

        try (Updater<TestRow> updater = mTable.newUpdater(null, "{id, num2}")) {
            TestRow row = updater.row();
            assertTrue(row.toString().endsWith("{id=1, num2=2}"));
        }
    }

    @Test
    public void update() throws Exception {
        storeServerRow();

        {
            TestRow row = mTable.newRow();
            row.id(1);
            row.str1("hello");
            mTable.merge(null, row);
            assertEquals("hello", row.str1());
            assertEquals(2, row.num2());
        }

        {
            Object serverRow = loadServerRow(1);
            assertTrue(serverRow.toString().endsWith("{id=1, num1=1, num2=2, str1=hello}"));
        }

        try (Updater<TestRow> updater = mTable.newUpdater(null)) {
            TestRow row = updater.row();
            row.num2(222);
            updater.update();
        }

        {
            Object serverRow = loadServerRow(1);
            assertTrue(serverRow.toString().endsWith("{id=1, num1=1, num2=222, str1=hello}"));
        }

        {
            TestRow row = mTable.newRow();
            row.id(1);
            mTable.load(null, row);
            assertEquals("hello", row.str1());
            assertEquals(222, row.num2());
        }
    }

    @SuppressWarnings("unchecked")
    private void storeServerRow() throws Exception {
        var table = (Table<Object>) mServerDb.openTable(mServerRowType);
        var id = mServerRowType.getMethod("id", long.class);
        var str1 = mServerRowType.getMethod("str1", String.class);
        var num1 = mServerRowType.getMethod("num1", int.class);
        var num2 = mServerRowType.getMethod("num2", int.class);

        var row = table.newRow();
        id.invoke(row, 1);
        str1.invoke(row, "str1");
        num1.invoke(row, 1);
        num2.invoke(row, 2);
        table.store(null, row);
    }

    @SuppressWarnings("unchecked")
    private Object loadServerRow(long id) throws Exception {
        var table = (Table<Object>) mServerDb.openTable(mServerRowType);
        var row = table.newRow();
        mServerRowType.getMethod("id", long.class).invoke(row, id);
        table.load(null, row);
        return row;
    }
}
