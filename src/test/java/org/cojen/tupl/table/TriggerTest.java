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

import java.util.Arrays;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class TriggerTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(TriggerTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
        mDb = Database.open(new DatabaseConfig());
        mTable = (StoredTable<TestRow>) mDb.openTable(TestRow.class);
    }

    @After
    public void teardown() throws Exception {
        for (int i=10; --i>=0;) {
            try {
                var stats = mDb.stats();
                assertEquals(0, stats.lockCount);
                assertEquals(0, stats.cursorCount);
                assertEquals(0, stats.transactionCount);
                break;
            } catch (AssertionError e) {
                if (i == 0) {
                    throw e;
                }
                // Wait for any background threads to finish.
                Thread.sleep(1000);
            }
        }

        mDb.close();
        mDb = null;
        mTable = null;
    }

    private Database mDb;
    private StoredTable<TestRow> mTable;

    @Test
    public void store() throws Exception {
        var trigger = new TestTrigger();

        TestRow row = mTable.newRow();
        row.extra("extra!");
        row.id(1);
        row.value("v1");
        mTable.store(null, row);

        mTable.setTrigger(trigger);

        row = mTable.newRow();
        row.extra("extra!");
        row.id(2);
        row.value("v2");
        mTable.store(null, row);
        assertSame(row, trigger.row);
        assertNull(trigger.oldValue);
        assertNotNull(trigger.newValue);
        assertFalse(trigger.partial);

        row = mTable.newRow();
        row.extra("extra!");
        row.id(1);
        row.value("hello");
        mTable.store(null, row);
        assertSame(row, trigger.row);
        assertNotNull(trigger.oldValue);
        assertNotNull(trigger.newValue);
        assertFalse(Arrays.equals(trigger.oldValue, trigger.newValue));

        mTable.setTrigger(null);

        trigger.row = null;
        row.value("hello!");
        mTable.store(null, row);
        assertNull(trigger.row);
    }

    @Test
    public void exchange() throws Exception {
        var trigger = new TestTrigger();

        TestRow row = mTable.newRow();
        row.extra("extra!");
        row.id(1);
        row.value("v1");
        assertNull(mTable.exchange(null, row));

        mTable.setTrigger(trigger);

        row = mTable.newRow();
        row.extra("extra!");
        row.id(2);
        row.value("v2");
        assertNull(mTable.exchange(null, row));
        assertSame(row, trigger.row);
        assertNull(trigger.oldValue);
        assertNotNull(trigger.newValue);
        assertFalse(trigger.partial);

        row = mTable.newRow();
        row.extra("extra!");
        row.id(1);
        row.value("hello");
        TestRow oldRow = mTable.exchange(null, row);
        assertEquals("v1", oldRow.value());
        assertSame(row, trigger.row);
        assertNotNull(trigger.oldValue);
        assertNotNull(trigger.newValue);
        assertFalse(Arrays.equals(trigger.oldValue, trigger.newValue));

        mTable.setTrigger(null);

        trigger.row = null;
        row.value("hello!");
        oldRow = mTable.exchange(null, row);
        assertEquals("hello", oldRow.value());
        assertNull(trigger.row);
    }

    @Test
    public void insert() throws Exception {
        var trigger = new TestTrigger();

        TestRow row = mTable.newRow();
        row.extra("extra!");
        row.id(1);
        row.value("v1");
        mTable.insert(null, row);

        mTable.setTrigger(trigger);

        row = mTable.newRow();
        row.extra("extra!");
        row.id(2);
        row.value("v2");
        mTable.insert(null, row);
        assertSame(row, trigger.row);
        assertNull(trigger.oldValue);
        assertNotNull(trigger.newValue);
        assertFalse(trigger.partial);

        row = mTable.newRow();
        row.extra("extra!");
        row.id(1);
        row.value("hello");
        trigger.row = null;
        try {
            mTable.insert(null, row);
            fail();
        } catch (UniqueConstraintException e) {
        }
        assertNull(trigger.row);

        mTable.setTrigger(null);

        trigger.row = null;
        row.id(3);
        row.value("hello!");
        mTable.insert(null, row);
        assertNull(trigger.row);
    }

    @Test
    public void replace() throws Exception {
        var trigger = new TestTrigger();

        TestRow row = mTable.newRow();
        row.extra("extra!");
        row.id(1);
        row.value("v1");
        mTable.store(null, row);

        row.value("v1-a");
        mTable.replace(null, row);

        mTable.setTrigger(trigger);

        row = mTable.newRow();
        row.extra("extra!");
        row.id(1);
        row.value("v1-b");
        mTable.replace(null, row);
        assertSame(row, trigger.row);
        assertNotNull(trigger.oldValue);
        assertNotNull(trigger.newValue);
        assertFalse(Arrays.equals(trigger.oldValue, trigger.newValue));
        assertFalse(trigger.partial);

        mTable.setTrigger(null);

        trigger.row = null;
        row.value("v1-c");
        mTable.replace(null, row);
        assertNull(trigger.row);
    }

    @Test
    public void delete() throws Exception {
        var trigger = new TestTrigger();

        TestRow row = mTable.newRow();
        row.extra("extra!");
        row.id(1);
        row.value("v1");
        mTable.store(null, row);

        mTable.delete(null, row);
        mTable.insert(null, row);

        mTable.setTrigger(trigger);

        row = mTable.newRow();
        row.extra("extra!");
        row.id(1);
        mTable.delete(null, row);
        assertSame(row, trigger.row);
        assertNotNull(trigger.oldValue);
        assertNull(trigger.newValue);
        assertFalse(trigger.partial);

        mTable.setTrigger(null);

        trigger.row = null;
        row.value("xxx");
        mTable.insert(null, row);
        mTable.delete(null, row);
        assertNull(trigger.row);
    }

    @Test
    public void updateAll() throws Exception {
        var trigger = new TestTrigger();

        TestRow row = mTable.newRow();
        row.extra("extra!");
        row.id(1);
        row.value("v1");
        mTable.store(null, row);

        row.value("v2");
        mTable.update(null, row);
        mTable.load(null, row);
        assertEquals("v2", row.value());

        mTable.setTrigger(trigger);

        row = mTable.newRow();
        row.extra("extra!");
        row.id(2);
        row.value("v3");
        try {
            mTable.update(null, row);
            fail();
        } catch (NoSuchRowException e) {
        }
        assertNull(trigger.row);
        row.id(1);
        mTable.update(null, row);
        assertSame(row, trigger.row);
        assertNotNull(trigger.oldValue);
        assertNotNull(trigger.newValue);
        assertFalse(Arrays.equals(trigger.oldValue, trigger.newValue));
        mTable.load(null, row);
        assertEquals("v3", row.value());
        assertFalse(trigger.partial);

        mTable.setTrigger(null);

        trigger.row = null;
        row.value("v4");
        mTable.update(null, row);
        assertNull(trigger.row);
        mTable.load(null, row);
        assertEquals("v4", row.value());
    }

    @Test
    public void updatePartial() throws Exception {
        var trigger = new TestTrigger();

        TestRow row = mTable.newRow();
        row.extra("nothing");
        row.id(1);
        row.value("v1");
        mTable.store(null, row);

        row = mTable.newRow();
        row.id(1);
        row.value("v2");
        mTable.update(null, row);
        try {
            row.extra();
            fail();
        } catch (UnsetColumnException e) {
        }
        mTable.load(null, row);
        assertEquals("v2", row.value());
        assertEquals("nothing", row.extra());

        mTable.setTrigger(trigger);

        row = mTable.newRow();
        row.id(1);
        row.extra("extra!");
        mTable.update(null, row);
        assertSame(row, trigger.row);
        assertNotNull(trigger.oldValue);
        assertNotNull(trigger.newValue);
        assertTrue(trigger.partial);
        assertFalse(Arrays.equals(trigger.oldValue, trigger.newValue));
        mTable.load(null, row);
        assertEquals("v2", row.value());
        assertEquals("extra!", row.extra());

        mTable.setTrigger(null);

        trigger.row = null;
        row = mTable.newRow();
        row.id(1);
        row.extra("nothing");
        mTable.update(null, row);
        assertNull(trigger.row);
        mTable.load(null, row);
        assertEquals("v2", row.value());
        assertEquals("nothing", row.extra());
    }

    @Test
    public void mergePartial() throws Exception {
        var trigger = new TestTrigger();

        TestRow row = mTable.newRow();
        row.extra("nothing");
        row.id(1);
        row.value("v1");
        mTable.store(null, row);

        row = mTable.newRow();
        row.id(1);
        row.value("v2");
        mTable.merge(null, row);
        assertEquals("v2", row.value());
        assertEquals("nothing", row.extra());
        mTable.load(null, row);
        assertEquals("v2", row.value());
        assertEquals("nothing", row.extra());

        mTable.setTrigger(trigger);

        row = mTable.newRow();
        row.id(1);
        row.extra("extra!");
        mTable.merge(null, row);
        assertEquals("v2", row.value());
        assertEquals("extra!", row.extra());
        assertSame(row, trigger.row);
        assertNotNull(trigger.oldValue);
        assertNotNull(trigger.newValue);
        assertFalse(trigger.partial);
        assertFalse(Arrays.equals(trigger.oldValue, trigger.newValue));
        mTable.load(null, row);
        assertEquals("v2", row.value());
        assertEquals("extra!", row.extra());

        mTable.setTrigger(null);

        trigger.row = null;
        row = mTable.newRow();
        row.id(1);
        row.extra("nothing");
        mTable.merge(null, row);
        assertNull(trigger.row);
        mTable.load(null, row);
        assertEquals("v2", row.value());
        assertEquals("nothing", row.extra());
    }

    @Test
    public void updaterDelete() throws Exception {
        var trigger = new TestTrigger();

        for (int i=1; i<=5; i++) {
            TestRow row = mTable.newRow();
            row.extra("nothing");
            row.id(i);
            row.value("v" + i);
            mTable.store(null, row);
        }

        mTable.setTrigger(trigger);

        Updater<TestRow> updater = mTable.newUpdater(null, "value == ?", "v3");
        while (updater.row() != null) {
            updater.delete();
        }

        Index ix = mDb.openIndex(TestRow.class.getName());
        assertEquals(4, ix.count(null, null));

        assertFalse(trigger.partial);
        assertNotNull(trigger.oldValue);
        assertNull(trigger.newValue);
        assertNull(trigger.row);

        TestRow row = mTable.newRow();
        row.id(3);
        assertFalse(mTable.tryLoad(null, row));

        Transaction txn = mDb.newTransaction();
        updater = mTable.newUpdater(txn, "value == ?", "v2");
        while (updater.row() != null) {
            updater.delete();
        }
        txn.commit();

        assertEquals(3, ix.count(null, null));

        assertFalse(trigger.partial);
        assertNotNull(trigger.oldValue);
        assertNull(trigger.newValue);
        assertNull(trigger.row);

        row = mTable.newRow();
        row.id(2);
        assertFalse(mTable.tryLoad(null, row));
    }

    @Test
    public void updaterUpdateInPlace() throws Exception {
        // The key portion of the row doesn't change.

        var trigger = new TestTrigger();

        for (int i=1; i<=5; i++) {
            TestRow row = mTable.newRow();
            row.extra("nothing");
            row.id(i);
            row.value("v" + i);
            mTable.store(null, row);
        }

        mTable.setTrigger(trigger);

        Updater<TestRow> updater = mTable.newUpdater(null, "value == ?", "v3");
        while (updater.row() != null) {
            updater.row().extra("extra!");
            updater.update();
        }

        assertTrue(trigger.partial);
        assertNotNull(trigger.oldValue);
        assertNotNull(trigger.newValue);
        assertFalse(Arrays.equals(trigger.oldValue, trigger.newValue));
        assertEquals(3, trigger.row.id());
        assertEquals("v3", trigger.row.value());
        assertEquals("extra!", trigger.row.extra());

        TestRow row = mTable.newRow();
        row.id(3);
        mTable.load(null, row);
        assertEquals("v3", row.value());
        assertEquals("extra!", row.extra());

        Transaction txn = mDb.newTransaction();
        updater = mTable.newUpdater(txn, "value == ?", "v2");
        while (updater.row() != null) {
            updater.row().extra("extra!!!");
            updater.update();
        }
        txn.commit();

        assertTrue(trigger.partial);
        assertNotNull(trigger.oldValue);
        assertNotNull(trigger.newValue);
        assertFalse(Arrays.equals(trigger.oldValue, trigger.newValue));
        assertEquals(2, trigger.row.id());
        assertEquals("v2", trigger.row.value());
        assertEquals("extra!!!", trigger.row.extra());

        row = mTable.newRow();
        row.id(2);
        mTable.load(null, row);
        assertEquals("v2", row.value());
        assertEquals("extra!!!", row.extra());
    }

    @Test
    public void updaterUpdateOutOfSequence() throws Exception {
        // The key portion of the row changes.

        var trigger = new TestTrigger();

        for (int i=1; i<=5; i++) {
            TestRow row = mTable.newRow();
            row.extra("nothing");
            row.id(i);
            row.value("v" + i);
            mTable.store(null, row);
        }

        mTable.setTrigger(trigger);

        Updater<TestRow> updater = mTable.newUpdater(null, "value == ?", "v3");
        while (updater.row() != null) {
            updater.row().id(103);
            updater.update();
        }

        assertTrue(trigger.partial);
        assertNull(trigger.oldValue);
        assertNotNull(trigger.newValue);
        assertEquals(103, trigger.row.id());
        assertEquals("v3", trigger.row.value());

        TestRow row = mTable.newRow();
        row.id(3);
        assertFalse(mTable.tryLoad(null, row));
        row.id(103);
        mTable.load(null, row);
        assertEquals("v3", row.value());

        Transaction txn = mDb.newTransaction();
        updater = mTable.newUpdater(txn, "value == ?", "v2");
        while (updater.row() != null) {
            updater.row().id(102);
            updater.update();
        }
        txn.commit();

        assertTrue(trigger.partial);
        assertNull(trigger.oldValue);
        assertNotNull(trigger.newValue);
        assertEquals(102, trigger.row.id());
        assertEquals("v2", trigger.row.value());

        row = mTable.newRow();
        row.id(2);
        assertFalse(mTable.tryLoad(null, row));
        row.id(102);
        mTable.load(null, row);
        assertEquals("v2", row.value());
    }

    @PrimaryKey("id")
    public interface TestRow {
        long id();
        void id(long id);

        String value();
        void value(String val);

        String extra();
        void extra(String val);
    }

    static class TestTrigger extends Trigger<TestRow> {
        TestRow row;
        byte[] oldValue, newValue;
        boolean partial;

        @Override
        public void store(Transaction txn, TestRow row, byte[] key,
                          byte[] oldValue, byte[] newValue)
        {
            assertNotNull(txn);
            assertNotNull(row);
            assertNotNull(key);
            this.row = row;
            this.oldValue = oldValue;
            this.newValue = newValue;
            partial = false;
        }

        @Override
        public void storeP(Transaction txn, TestRow row, byte[] key,
                           byte[] oldValue, byte[] newValue)
        {
            store(txn, row, key, oldValue, newValue);
            partial = true;
        }

        @Override
        public void insert(Transaction txn, TestRow row, byte[] key, byte[] newValue) {
            store(txn, row, key, null, newValue);
            partial = false;
        }

        @Override
        public void insertP(Transaction txn, TestRow row, byte[] key, byte[] newValue) {
            store(txn, row, key, null, newValue);
            partial = true;
        }

        @Override
        public void delete(Transaction txn, TestRow row, byte[] key, byte[] oldValue) {
            store(txn, row, key, oldValue, null);
            partial = false;
        }

        @Override
        public void delete(Transaction txn, byte[] key, byte[] oldValue) {
            assertNotNull(txn);
            assertNotNull(key);
            this.row = null;
            this.oldValue = oldValue;
            this.newValue = newValue;
            partial = false;
        }
    }
}
