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
public class RemoteMissingServerColumnTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RemoteMissingServerColumnTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
        mServerDb = Database.open(new DatabaseConfig());
        Server server = mServerDb.newServer();

        // Use a different row type definition on the server which is missing a column.
        mServerRowType = RowTestUtils.newRowType(TestRow.class.getName(),
                                                 long.class, "+id",
                                                 String.class, "str1",
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

        int num1();
        void num1(int n);

        int num2();
        void num2(int n);
    }

    @Test
    public void store() throws Exception {
        try {
            TestRow row = mTable.newRow();
            row.id(1);
            row.str1("str1");
            row.num1(1);
            row.num2(2);
            mTable.store(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertEquals("Unknown column: num1", e.getMessage());
        }
    }

    @Test
    public void load() throws Exception {
        storeServerRow();

        // Note that load is strict. It must mark all columns as clean as a side-effect, but
        // this isn't possible if the client has a column unknown to the server.
        try {
            TestRow row = mTable.newRow();
            row.id(1);
            mTable.load(null, row);
            fail();
        } catch (IllegalStateException e) {
            assertEquals("Unknown column: num1", e.getMessage());
        }

        // Note that the scanner is lenient. Scanners support projection, and so it's okay for
        // some columns to be unset.
        try (Scanner<TestRow> scanner = mTable.newScanner(null)) {
            TestRow row = scanner.row();
            assertEquals("str1", row.str1());
            assertEquals(2, row.num2());
            try {
                row.num1();
                fail();
            } catch (UnsetColumnException e) {
            }
        }

        try (Scanner<TestRow> scanner = mTable.newScanner(null, "{id, num2}")) {
            TestRow row = scanner.row();
            assertTrue(row.toString().endsWith("{id=1, num2=2}"));
            try {
                row.num1();
                fail();
            } catch (UnsetColumnException e) {
            }
        }

        // The updater is also lenient.
        try (Updater<TestRow> updater = mTable.newUpdater(null)) {
            TestRow row = updater.row();
            assertTrue(row.toString().endsWith("{id=1, num2=2, str1=str1}"));
            try {
                row.num1();
                fail();
            } catch (UnsetColumnException e) {
            }
        }

        try (Updater<TestRow> updater = mTable.newUpdater(null, "{id, num2}")) {
            TestRow row = updater.row();
            assertTrue(row.toString().endsWith("{id=1, num2=2}"));
            try {
                row.num1();
                fail();
            } catch (UnsetColumnException e) {
            }
        }
    }

    @Test
    public void update() throws Exception {
        storeServerRow();

        {
            TestRow row = mTable.newRow();
            row.id(1);
            row.num1(123);
            try {
                mTable.update(null, row);
                fail();
            } catch (IllegalStateException e) {
                assertEquals("Unknown column: num1", e.getMessage());
            } 
        }

        // The update failed, and so nothing should have changed.
        {
            Object serverRow = loadServerRow(1);
            assertTrue(serverRow.toString().endsWith("{id=1, num2=2, str1=str1}"));
        }

        // Note that merge is strict for the same reason that load is strict. It must mark all
        // columns as clean as a side-effect, but this isn't possible if the client has a
        // column unknown to the server.
        {
            TestRow row = mTable.newRow();
            row.id(1);
            row.str1("hello");
            try {
                mTable.merge(null, row);
                fail();
            } catch (IllegalStateException e) {
                assertEquals("Unknown column: num1", e.getMessage());
            } 
        }

        // The merge failed, and so nothing should have changed.
        {
            Object serverRow = loadServerRow(1);
            assertTrue(serverRow.toString().endsWith("{id=1, num2=2, str1=str1}"));
        }

        {
            TestRow row = mTable.newRow();
            row.id(1);
            row.str1("hello");
            mTable.update(null, row);
        }

        {
            Object serverRow = loadServerRow(1);
            assertTrue(serverRow.toString().endsWith("{id=1, num2=2, str1=hello}"));
        }
    }

    @SuppressWarnings("unchecked")
    private void storeServerRow() throws Exception {
        var table = (Table<Object>) mServerDb.openTable(mServerRowType);
        var id = mServerRowType.getMethod("id", long.class);
        var str1 = mServerRowType.getMethod("str1", String.class);
        var num2 = mServerRowType.getMethod("num2", int.class);

        var row = table.newRow();
        id.invoke(row, 1);
        str1.invoke(row, "str1");
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
