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
public class RemoteRowConvertTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RemoteRowConvertTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
        mServerDb = Database.open(new DatabaseConfig());
        Server server = mServerDb.newServer();

        // Use a different row type definition on the server.
        mServerRowType = RowTestUtils.newRowType(TestRow.class.getName(),
                                                 long.class, "+id",
                                                 float.class, "num1",
                                                 int.class, "num2",
                                                 Integer.class, "num3?",
                                                 float.class, "num4");

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

        int num1();
        void num1(int n);

        float num2();
        void num2(float n);

        int num3();
        void num3(int n);

        @Unsigned
        int num4();
        void num4(int n);
    }

    @Test
    public void convertSuccess() throws Exception {
        TestRow row1 = mTable.newRow();
        row1.id(1);
        row1.num1(1);
        row1.num2(2);
        row1.num3(3);
        row1.num4(4);
        mTable.insert(null, row1);

        {
            TestRow row = mTable.newRow();
            row.id(1);
            assertTrue(mTable.tryLoad(null, row));
            assertEquals(row1, row);
            assertEquals(1, row.id());
            assertEquals(1, row.num1());
            assertTrue(2.0 == row.num2());
            assertEquals(3, row.num3());
            assertEquals(4, row.num4());
        }

        try (var scanner = mTable.newScanner(null)) {
            for (var row = scanner.row(); row != null; row = scanner.step()) {
                assertEquals(row1, row);
            }
        }

        try (var updater = mTable.newUpdater(null)) {
            for (var row = updater.row(); row != null; row = updater.step()) {
                assertEquals(row1, row);
            }
        }

        TestRow row2 = mTable.newRow();
        row2.id(1);
        row2.num1(10);
        row2.num2(20);
        mTable.merge(null, row2);

        {
            TestRow row = mTable.newRow();
            row.id(1);
            assertTrue(mTable.tryLoad(null, row));
            assertEquals(row2, row);
        }

        TestRow row3 = mTable.newRow();
        row3.id(1);
        row3.num3(30);
        row3.num4(40);
        mTable.update(null, row3);

        {
            TestRow row = mTable.newRow();
            row.id(1);
            assertTrue(mTable.tryLoad(null, row));
            assertEquals(10, row.num1());
            assertTrue(20.0 == row.num2());
            assertEquals(30, row.num3());
            assertEquals(40, row.num4());
        }

        try (var updater = mTable.newUpdater(null)) {
            for (var row = updater.row(); row != null; ) {
                row.num1(100);
                row.num3(300);
                row = updater.update();
            }
        }

        try (var updater = mTable.newUpdater(null)) {
            for (var row = updater.row(); row != null; ) {
                row.num2(200);
                row.num4(400);
                row = updater.update();
            }
        }

        {
            TestRow row = mTable.newRow();
            row.id(1);
            assertTrue(mTable.tryLoad(null, row));
            assertEquals(100, row.num1());
            assertTrue(200.0 == row.num2());
            assertEquals(300, row.num3());
            assertEquals(400, row.num4());
        }
    }

    @Test
    public void storeConvertFailure() throws Exception {
        {
            TestRow row = mTable.newRow();
            row.id(1);
            row.num1(Integer.MAX_VALUE - 3);
            row.num2(2);
            row.num3(3);
            row.num4(4);

            try {
                mTable.store(null, row);
                fail();
            } catch (ConversionException e) {
                assertTrue(e.getMessage().contains("" + (Integer.MAX_VALUE - 3)));
                assertTrue(e.getMessage().contains("num1"));
            }
        }

        {
            TestRow row = mTable.newRow();
            row.id(1);
            row.num1(1);
            row.num2(2.5f);
            row.num3(3);
            row.num4(4);

            try {
                mTable.store(null, row);
                fail();
            } catch (ConversionException e) {
                assertTrue(e.getMessage().contains("2.5"));
                assertTrue(e.getMessage().contains("num2"));
            }
        }

        {
            TestRow row = mTable.newRow();
            row.id(1);
            row.num1(1);
            row.num2(2);
            row.num3(3);
            row.num4(-4);

            try {
                mTable.store(null, row);
                fail();
            } catch (ConversionException e) {
                assertTrue(e.getMessage().contains("4294967292"));
                assertTrue(e.getMessage().contains("num4"));
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void loadConvertFailure() throws Exception {
        // First store a few rows using the server row type.

        {
            var table = (Table<Object>) mServerDb.openTable(mServerRowType);
            var id = mServerRowType.getMethod("id", long.class);
            var num1 = mServerRowType.getMethod("num1", float.class);
            var num2 = mServerRowType.getMethod("num2", int.class);
            var num3 = mServerRowType.getMethod("num3", Integer.class);
            var num4 = mServerRowType.getMethod("num4", float.class);

            {
                var row = table.newRow();
                id.invoke(row, 1);
                num1.invoke(row, 1.5f);
                num2.invoke(row, 2);
                num3.invoke(row, 3);
                num4.invoke(row, 4.0f);
                table.store(null, row);
            }

            {
                var row = table.newRow();
                id.invoke(row, 2);
                num1.invoke(row, 1.0f);
                num2.invoke(row, Integer.MAX_VALUE - 3);
                num3.invoke(row, 3);
                num4.invoke(row, 4.0f);
                table.insert(null, row);
            }

            {
                var row = table.newRow();
                id.invoke(row, 3);
                num1.invoke(row, 1.0f);
                num2.invoke(row, 2);
                num3.invoke(row, (Integer) null);
                num4.invoke(row, 4.0f);
                table.insert(null, row);
            }

            {
                var row = table.newRow();
                id.invoke(row, 4);
                num1.invoke(row, 1.0f);
                num2.invoke(row, 2);
                num3.invoke(row, 3);
                num4.invoke(row, -4.0f);
                table.insert(null, row);
            }
        }

        // Now verify conversion errors.

        for (int mode = 0; mode <= 2; mode++) {
            try {
                TestRow row = load(mode, 1);
                fail();
            } catch (ConversionException e) {
                assertTrue(e.getMessage().contains("1.5"));
                assertTrue(e.getMessage().contains("num1"));
            }

            try {
                TestRow row = load(mode, 2);
                fail();
            } catch (ConversionException e) {
                assertTrue(e.getMessage().contains("" + (Integer.MAX_VALUE - 3)));
                assertTrue(e.getMessage().contains("num2"));
            }

            try {
                TestRow row = load(mode, 3);
                fail();
            } catch (ConversionException e) {
                assertTrue(e.getMessage().contains("non-nullable"));
                assertTrue(e.getMessage().contains("num3"));
            }

            try {
                TestRow row = load(mode, 4);
                fail();
            } catch (ConversionException e) {
                assertTrue(e.getMessage().contains("-4.0"));
                assertTrue(e.getMessage().contains("num4"));
            }
        }
    }

    /**
     * @param mode 0: load, 1: scanner, 2: updater
     */
    private TestRow load(int mode, long id) throws Exception {
        switch (mode) {
        case 0:
            TestRow row = mTable.newRow();
            row.id(id);
            mTable.load(null, row);
            return row;

        case 1:
            try (Scanner<TestRow> scanner = mTable.newScanner(null, "id == ?", id)) {
                return scanner.row();
            }

        case 2:
            try (Updater<TestRow> updater = mTable.newUpdater(null, "id == ?", id)) {
                return updater.row();
            }
            
        default:
            fail();
            return null;
        }
    }
}
