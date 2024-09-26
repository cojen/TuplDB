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

import java.util.function.Consumer;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class IndexUpdaterTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(IndexUpdaterTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
        mDb = Database.open(new DatabaseConfig());
        mTable = (StoredTable<TestRow>) mDb.openTable(TestRow.class);
        mIndex = mTable.viewSecondaryIndex("str1");
    }

    @After
    public void teardown() throws Exception {
        mDb.close();
        mDb = null;
    }

    private Database mDb;
    private StoredTable<TestRow> mTable;
    private StoredTable<TestRow> mIndex;

    @PrimaryKey("id")
    @SecondaryIndex("str1")
    public interface TestRow {
        int id();
        void id(int id);

        String str1();
        void str1(String str);

        int num1();
        void num1(int num);
    }

    @Test
    public void deleteAll() throws Exception {
        fill(1, 10);

        Transaction txn = mDb.newTransaction();
        Updater<TestRow> updater = mIndex.newUpdater(txn);

        try (updater) {
            for (TestRow row; (row = updater.row()) != null; ) {
                updater.delete();
            }
        }

        txn.commit();

        assertTrue(mIndex.isEmpty());
        assertTrue(mIndex.viewUnjoined().isEmpty());
        assertTrue(mTable.isEmpty());
    }

    @Test
    public void deleteFiltered() throws Exception {
        fill(1, 10);

        Transaction txn = mDb.newTransaction();
        Updater<TestRow> updater = mIndex.newUpdater(txn, "id == ? || str1 == ?", 1, "str-5");

        try (updater) {
            for (TestRow row; (row = updater.row()) != null; ) {
                updater.delete();
            }
        }

        txn.commit();

        assertEquals(8, count(mIndex));
        assertEquals(8, count(mIndex.viewUnjoined()));
        assertEquals(8, count(mTable));

        TestRow row = mTable.newRow();

        row.id(1);
        assertFalse(mTable.tryLoad(null, row));
        row.str1("str-1");
        assertFalse(mIndex.tryLoad(null, row));

        row.id(5);
        assertFalse(mTable.tryLoad(null, row));
        row.str1("str-5");
        assertFalse(mIndex.tryLoad(null, row));

        row.id(3);
        assertTrue(mTable.tryLoad(null, row));
        row.str1("str-3");
        assertTrue(mIndex.tryLoad(null, row));
    }

    @Test
    public void deleteStepped() throws Exception {
        fill(1, 10);

        Transaction txn = mDb.newTransaction();
        Updater<TestRow> updater = mIndex.newUpdater(txn);

        try (updater) {
            for (TestRow row; (row = updater.row()) != null; ) {
                if ((row.id() & 1) == 0) {
                    updater.delete();
                } else {
                    updater.step();
                }
            }
        }

        txn.commit();

        assertEquals(5, count(mIndex));
        assertEquals(5, count(mIndex.viewUnjoined()));
        assertEquals(5, count(mTable));

        fillCheck(mIndex, null, null, 1, 3, 5, 7, 9);
        fillCheck(mTable, null, null,1, 3, 5, 7, 9);
    }

    @Test
    public void updateAll() throws Exception {
        fill(1, 10);

        Transaction txn = mDb.newTransaction();
        Updater<TestRow> updater = mIndex.newUpdater(txn);

        try (updater) {
            for (TestRow row; (row = updater.row()) != null; ) {
                row.num1(row.num1() + 9000);
                updater.update();
            }
        }

        txn.commit();

        assertEquals(10, count(mIndex));
        assertEquals(10, count(mIndex.viewUnjoined()));
        assertEquals(10, count(mTable));

        Consumer<TestRow> post = row -> row.num1(row.num1() - 9000);

        fillCheck(mIndex, null, post, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        fillCheck(mTable, null, post, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void updateFiltered() throws Exception {
        fill(1, 10);

        Transaction txn = mDb.newTransaction();
        Updater<TestRow> updater = mIndex.newUpdater(txn, "id == ? || str1 == ?", 1, "str-5");

        try (updater) {
            for (TestRow row; (row = updater.row()) != null; ) {
                row.num1(row.num1() + 9000);
                updater.update();
            }
        }

        txn.commit();

        assertEquals(10, count(mIndex));
        assertEquals(10, count(mIndex.viewUnjoined()));
        assertEquals(10, count(mTable));

        Consumer<TestRow> post = row -> {
            if (row.id() == 1 || row.str1().equals("str-5")) {
                row.num1(row.num1() - 9000);
            }
        };

        fillCheck(mIndex, null, post, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        fillCheck(mTable, null, post, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void updateAllAlterKey() throws Exception {
        fill(1, 10);

        Transaction txn = mDb.newTransaction();
        Updater<TestRow> updater = mIndex.newUpdater(txn);

        try (updater) {
            for (TestRow row; (row = updater.row()) != null; ) {
                int id = row.id();
                if ((id & 1) == 0) {
                    row.id(-id);
                } else {
                    row.id(id + 9000);
                }
                updater.update();
            }
        }

        txn.commit();

        assertEquals(10, count(mIndex));
        assertEquals(10, count(mIndex.viewUnjoined()));
        assertEquals(10, count(mTable));

        Consumer<TestRow> pre = row -> {
            int id = row.id();
            if ((id & 1) == 0) {
                row.id(-id);
            } else {
                row.id(id + 9000);
            }
        };

        Consumer<TestRow> post = row -> {
            int id = row.id();
            if (id < 0) {
                row.id(-id);
            } else {
                row.id(id - 9000);
            }
        };

        fillCheck(mIndex, pre, post, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        fillCheck(mTable, pre, post, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void updateFilteredAlterKey() throws Exception {
        fill(1, 10);

        Transaction txn = mDb.newTransaction();
        Updater<TestRow> updater = mIndex.newUpdater(txn, "id >= ? && num1 <= ?", 3, 108);

        try (updater) {
            for (TestRow row; (row = updater.row()) != null; ) {
                int id = row.id();
                if ((id & 1) == 0) {
                    row.id(-id);
                } else {
                    row.id(id + 9000);
                }
                updater.update();
            }
        }

        txn.commit();

        assertEquals(10, count(mIndex));
        assertEquals(10, count(mIndex.viewUnjoined()));
        assertEquals(10, count(mTable));

        Consumer<TestRow> pre = row -> {
            int id = row.id();
            if (id < 3 || id > 8) {
                return;
            }
            if ((id & 1) == 0) {
                row.id(-id);
            } else {
                row.id(id + 9000);
            }
        };

        Consumer<TestRow> post = row -> {
            int id = row.id();
            if (id < 0) {
                row.id(-id);
            } else if (id >= 3 && row.num1() <= 108) {
                row.id(id - 9000);
            }
        };

        fillCheck(mIndex, pre, post, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        fillCheck(mTable, pre, post, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    private void fill(int from, int to) throws Exception {
        for (int id = from; id <= to; id++) {
            TestRow row = mTable.newRow();
            row.id(id);
            row.str1("str-" + id);
            row.num1(id + 100);
            mTable.store(null, row);
        }
    }

    private void fillCheck(Table<TestRow> table, Consumer<TestRow> pre, Consumer<TestRow> post,
                           int... ids)
        throws Exception
    {
        for (int id : ids) {
            TestRow row = table.newRow();
            row.id(id);
            row.str1("str-" + id);

            if (pre != null) {
                pre.accept(row);
            }

            assertTrue(table.tryLoad(null, row));

            if (post != null) {
                post.accept(row);
            }

            assertEquals(id, row.id());
            assertEquals("str-" + id, row.str1());
            assertEquals(id + 100, row.num1());
        }
    }

    private <R> long count(Table<R> table) throws Exception {
        return table.newStream(null).count();
    }

    private <R> void dump(Table<R> table) throws Exception {
        System.out.println("dump: " + table);
        try (Scanner<R> scanner = table.newScanner(null)) {
            for (R row = scanner.row(); row != null; row = scanner.step(row)) {
                System.out.println(row);
            }
        }
    }
}
