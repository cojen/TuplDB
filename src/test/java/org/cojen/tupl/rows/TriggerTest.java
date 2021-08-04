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
        mTable = (AbstractTable<TestRow>) mDb.openTable(TestRow.class);
    }

    @After
    public void teardown() throws Exception {
        mDb.close();
        mDb = null;
        mTable = null;
    }

    private Database mDb;
    private AbstractTable<TestRow> mTable;

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
        assertTrue(row == trigger.row);
        assertNull(trigger.oldValue);
        assertNotNull(trigger.newValue);

        row = mTable.newRow();
        row.extra("extra!");
        row.id(1);
        row.value("hello");
        mTable.store(null, row);
        assertTrue(row == trigger.row);
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
        assertTrue(row == trigger.row);
        assertNull(trigger.oldValue);
        assertNotNull(trigger.newValue);

        row = mTable.newRow();
        row.extra("extra!");
        row.id(1);
        row.value("hello");
        TestRow oldRow = mTable.exchange(null, row);
        assertEquals("v1", oldRow.value());
        assertTrue(row == trigger.row);
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
        assertTrue(mTable.insert(null, row));

        mTable.setTrigger(trigger);

        row = mTable.newRow();
        row.extra("extra!");
        row.id(2);
        row.value("v2");
        assertTrue(mTable.insert(null, row));
        assertTrue(row == trigger.row);
        assertNull(trigger.oldValue);
        assertNotNull(trigger.newValue);

        row = mTable.newRow();
        row.extra("extra!");
        row.id(1);
        row.value("hello");
        trigger.row = null;
        assertFalse(mTable.insert(null, row));
        assertNull(trigger.row);

        mTable.setTrigger(null);

        trigger.row = null;
        row.id(3);
        row.value("hello!");
        assertTrue(mTable.insert(null, row));
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
        assertTrue(mTable.replace(null, row));

        mTable.setTrigger(trigger);

        row = mTable.newRow();
        row.extra("extra!");
        row.id(1);
        row.value("v1-b");
        assertTrue(mTable.replace(null, row));
        assertTrue(row == trigger.row);
        assertNotNull(trigger.oldValue);
        assertNotNull(trigger.newValue);
        assertFalse(Arrays.equals(trigger.oldValue, trigger.newValue));

        mTable.setTrigger(null);

        trigger.row = null;
        row.value("v1-c");
        assertTrue(mTable.replace(null, row));
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

        assertTrue(mTable.delete(null, row));
        assertTrue(mTable.insert(null, row));

        mTable.setTrigger(trigger);

        row = mTable.newRow();
        row.extra("extra!");
        row.id(1);
        assertTrue(mTable.delete(null, row));
        assertTrue(row == trigger.row);
        assertNotNull(trigger.oldValue);
        assertNull(trigger.newValue);

        mTable.setTrigger(null);

        trigger.row = null;
        row.value("xxx");
        assertTrue(mTable.insert(null, row));
        assertTrue(mTable.delete(null, row));
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
        assertFalse(mTable.update(null, row));
        assertNull(trigger.row);
        row.id(1);
        assertTrue(mTable.update(null, row));
        assertTrue(row == trigger.row);
        assertNotNull(trigger.oldValue);
        assertNotNull(trigger.newValue);
        assertFalse(Arrays.equals(trigger.oldValue, trigger.newValue));
        mTable.load(null, row);
        assertEquals("v3", row.value());

        mTable.setTrigger(null);

        trigger.row = null;
        row.value("v4");
        assertTrue(mTable.update(null, row));
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
        assertTrue(mTable.update(null, row));
        assertTrue(row == trigger.row);
        assertNotNull(trigger.oldValue);
        assertNotNull(trigger.newValue);
        assertTrue(trigger.update);
        assertFalse(Arrays.equals(trigger.oldValue, trigger.newValue));
        mTable.load(null, row);
        assertEquals("v2", row.value());
        assertEquals("extra!", row.extra());

        mTable.setTrigger(null);

        trigger.row = null;
        row = mTable.newRow();
        row.id(1);
        row.extra("nothing");
        assertTrue(mTable.update(null, row));
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
        assertTrue(mTable.merge(null, row));
        assertEquals("v2", row.value());
        assertEquals("extra!", row.extra());
        assertTrue(row == trigger.row);
        assertNotNull(trigger.oldValue);
        assertNotNull(trigger.newValue);
        assertFalse(trigger.update);
        assertFalse(Arrays.equals(trigger.oldValue, trigger.newValue));
        mTable.load(null, row);
        assertEquals("v2", row.value());
        assertEquals("extra!", row.extra());

        mTable.setTrigger(null);

        trigger.row = null;
        row = mTable.newRow();
        row.id(1);
        row.extra("nothing");
        assertTrue(mTable.merge(null, row));
        assertNull(trigger.row);
        mTable.load(null, row);
        assertEquals("v2", row.value());
        assertEquals("nothing", row.extra());
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
        boolean update;

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
            update = false;
        }

        @Override
        public void update(Transaction txn, TestRow row, byte[] key,
                           byte[] oldValue, byte[] newValue)
        {
            store(txn, row, key, oldValue, newValue);
            update = true;
        }
    }
}
