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

import java.math.BigDecimal;
import java.math.BigInteger;

import java.util.HashSet;
import java.util.Random;

import java.util.concurrent.TimeUnit;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.maker.ClassMaker;

import org.cojen.tupl.*;

import org.cojen.tupl.core.CoreDatabase;

import static org.cojen.tupl.TestUtils.*;

import static org.cojen.tupl.rows.RowTestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class IndexingTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(IndexingTest.class.getName());
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
    }

    @Test
    public void basic() throws Exception {
        Database db = Database.open(new DatabaseConfig().directPageAccess(false).
                                    lockTimeout(100, TimeUnit.MILLISECONDS));
        Table<TestRow> table = db.openTable(TestRow.class);

        Table<TestRow> alt = table.viewAlternateKey("path");
        Table<TestRow> ix1 = table.viewSecondaryIndex("name");
        Table<TestRow> ix2 = table.viewSecondaryIndex("num", "name");

        assertNull(ix1.viewAlternateKey("path"));
        assertNull(ix1.viewSecondaryIndex("name"));

        {
            TestRow row = table.newRow();
            row.id(1);
            row.path("path1");
            row.name("name1");
            row.num(new BigDecimal("123"));
            table.store(null, row);

            row.id(2);
            row.path("path2");
            row.name("name2");
            row.num(new BigDecimal("123"));
            table.store(null, row);

            row.id(3);
            row.path("path3");
            row.name("name1");
            row.num(new BigDecimal("987"));
            table.store(null, row);

            row.id(4);
            row.path("path1");
            row.name("name4");
            row.num(new BigDecimal("555"));
            try {
                table.store(null, row);
                fail();
            } catch (UniqueConstraintException e) {
                // Alternate key constraint.
            }
        }

        {
            TestRow row = table.newRow();
            row.path("path2");
            assertTrue(alt.load(null, row));
            assertEquals(2, row.id());
            assertEquals("path2", row.path());
            try {
                row.name();
                fail();
            } catch (UnsetColumnException e) {
            }
            assertTrue(row.toString().contains("TestRow{id=2, path=path2}"));
            assertTrue(alt.exists(null, row));
            try {
                alt.store(null, row);
            } catch (UnmodifiableViewException e) {
            }
        }

        {
            TestRow row = table.newRow();
            row.id(3);
            try {
                ix1.load(null, row);
                fail();
            } catch (IllegalStateException e) {
            }
            row.name("name1");
            assertTrue(ix1.load(null, row));
            try {
                row.path();
                fail();
            } catch (UnsetColumnException e) {
            }
            assertTrue(row.toString().contains("TestRow{id=3, name=name1}"));
            assertTrue(ix1.exists(null, row));
            try {
                ix1.insert(null, row);
            } catch (UnmodifiableViewException e) {
            }
        }

        {
            TestRow row = table.newRow();
            row.id(2);
            row.num(new BigDecimal("123"));
            try {
                ix2.load(null, row);
                fail();
            } catch (IllegalStateException e) {
            }
            row.name("name2");
            assertTrue(ix2.load(null, row));
            try {
                row.path();
                fail();
            } catch (UnsetColumnException e) {
            }
            assertTrue(row.toString().contains("TestRow{id=2, name=name2, num=123}"));
            assertTrue(ix2.exists(null, row));
            try {
                ix2.delete(null, row);
            } catch (UnmodifiableViewException e) {
            }
        }

        try {
            ix1.newRowUpdater(null);
            fail();
        } catch (UnmodifiableViewException e) {
        }

        scanExpect(alt, "TestRow{id=1, path=path1}",
                   "TestRow{id=2, path=path2}", "TestRow{id=3, path=path3}");

        scanExpect(ix1, "TestRow{id=1, name=name1}",
                   "TestRow{id=3, name=name1}", "TestRow{id=2, name=name2}");

        scanExpect(ix2, "TestRow{id=2, name=name2, num=123}",
                   "TestRow{id=1, name=name1, num=123}", "TestRow{id=3, name=name1, num=987}");

        {
            TestRow row = table.newRow();
            row.id(2);
            assertTrue(table.delete(null, row));
        }

        scanExpect(alt, "TestRow{id=1, path=path1}", "TestRow{id=3, path=path3}");

        scanExpect(ix1, "TestRow{id=1, name=name1}", "TestRow{id=3, name=name1}");

        scanExpect(ix2, "TestRow{id=1, name=name1, num=123}", "TestRow{id=3, name=name1, num=987}");

        {
            Transaction txn = db.newTransaction();
            TestRow row = table.newRow();
            row.id(3);
            assertTrue(table.delete(txn, row));

            {
                TestRow rowx = table.newRow();
                rowx.path("path3");
                try {
                    alt.load(null, rowx);
                    fail();
                } catch (LockTimeoutException e) {
                }
            }

            {
                TestRow rowx = table.newRow();
                rowx.name("name1");
                rowx.id(3);
                try {
                    ix1.load(null, rowx);
                    fail();
                } catch (LockTimeoutException e) {
                }
            }

            {
                TestRow rowx = table.newRow();
                rowx.num(new BigDecimal("987"));
                rowx.name("name1");
                rowx.id(3);
                try {
                    ix2.load(null, rowx);
                    fail();
                } catch (LockTimeoutException e) {
                }
            }

            txn.commit();

            {
                TestRow rowx = table.newRow();
                rowx.path("path3");
                assertFalse(alt.exists(null, rowx));
            }

            {
                TestRow rowx = table.newRow();
                rowx.name("name1");
                rowx.id(3);
                assertFalse(ix1.exists(null, rowx));
            }

            {
                TestRow rowx = table.newRow();
                rowx.num(new BigDecimal("987"));
                rowx.name("name1");
                rowx.id(3);
                assertFalse(ix2.exists(null, rowx));
            }
        }

        scanExpect(alt, "TestRow{id=1, path=path1}");

        scanExpect(ix1, "TestRow{id=1, name=name1}");

        scanExpect(ix2, "TestRow{id=1, name=name1, num=123}");

        {
            TestRow row = table.newRow();
            row.id(1);
            row.path("no-path");
            row.name("no-name");
            row.num(BigDecimal.ZERO);
            TestRow oldRow = table.exchange(null, row);
            assertTrue(oldRow.toString().contains("{id=1, name=name1, num=123, path=path1}"));
        }

        scanExpect(alt, "TestRow{id=1, path=no-path}");

        scanExpect(ix1, "TestRow{id=1, name=no-name}");

        scanExpect(ix2, "TestRow{id=1, name=no-name, num=0}");

        {
            TestRow row = table.newRow();
            row.id(5);
            row.path("path5");
            row.name("name5");
            row.num(new BigDecimal("555"));
            assertTrue(table.insert(null, row));
        }

        scanExpect(alt, "TestRow{id=1, path=no-path}", "TestRow{id=5, path=path5}");

        scanExpect(ix1, "TestRow{id=5, name=name5}", "TestRow{id=1, name=no-name}");

        scanExpect(ix2, "TestRow{id=1, name=no-name, num=0}", "TestRow{id=5, name=name5, num=555}");

        {
            TestRow row = table.newRow();
            row.id(5);
            row.path("no-path");
            row.name("!name5");
            row.num(new BigDecimal("-5"));

            try {
                table.replace(null, row);
                fail();
            } catch (UniqueConstraintException e) {
                // Alternate key constraint.
            }

            row.path("path5");
            assertTrue(table.replace(null, row));
        }

        scanExpect(alt, "TestRow{id=1, path=no-path}", "TestRow{id=5, path=path5}");

        scanExpect(ix1, "TestRow{id=5, name=!name5}", "TestRow{id=1, name=no-name}");

        scanExpect(ix2, "TestRow{id=5, name=!name5, num=-5}", "TestRow{id=1, name=no-name, num=0}");

        {
            TestRow row = table.newRow();
            row.id(5);
            row.path("!path5");
            row.name("!name55");
            row.num(new BigDecimal("55"));
            assertTrue(table.update(null, row));
        }

        scanExpect(alt, "TestRow{id=5, path=!path5}", "TestRow{id=1, path=no-path}");

        scanExpect(ix1, "TestRow{id=5, name=!name55}", "TestRow{id=1, name=no-name}");

        scanExpect(ix2, "TestRow{id=1, name=no-name, num=0}",
                   "TestRow{id=5, name=!name55, num=55}");

        {
            TestRow row = table.newRow();
            row.id(1);
            row.name("name1");
            assertTrue(table.update(null, row));
        }

        scanExpect(alt, "TestRow{id=5, path=!path5}", "TestRow{id=1, path=no-path}");

        scanExpect(ix1, "TestRow{id=5, name=!name55}", "TestRow{id=1, name=name1}");

        scanExpect(ix2, "TestRow{id=1, name=name1, num=0}", "TestRow{id=5, name=!name55, num=55}");

        {
            TestRow row = table.newRow();
            row.id(5);
            row.name("name5");
            assertTrue(table.merge(null, row));
            assertTrue(row.toString().contains("TestRow{id=5, name=name5, num=55, path=!path5}"));
        }

        scanExpect(alt, "TestRow{id=5, path=!path5}", "TestRow{id=1, path=no-path}");

        scanExpect(ix1, "TestRow{id=1, name=name1}", "TestRow{id=5, name=name5}");

        scanExpect(ix2, "TestRow{id=1, name=name1, num=0}", "TestRow{id=5, name=name5, num=55}");

        {
            // Lock all the index entries that shouldn't be updated.
            Transaction txn = db.newTransaction();
            RowScanner<TestRow> s = ix1.newRowScanner(txn);
            for (TestRow row = s.row(); s.row() != null; row = s.step(row)) {}
            s = ix2.newRowScanner(txn);
            for (TestRow row = s.row(); s.row() != null; row = s.step(row)) {}

            TestRow row = table.newRow();
            row.id(1);
            row.path("path-1");
            row.name("name1");        // no change
            row.num(BigDecimal.ZERO); // no change
            table.store(null, row);

            txn.exit();
        }

        scanExpect(alt, "TestRow{id=5, path=!path5}", "TestRow{id=1, path=path-1}");

        scanExpect(ix1, "TestRow{id=1, name=name1}", "TestRow{id=5, name=name5}");

        scanExpect(ix2, "TestRow{id=1, name=name1, num=0}", "TestRow{id=5, name=name5, num=55}");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void basicBackfill() throws Exception {
        var config = new DatabaseConfig().directPageAccess(false);
        //config.eventListener(EventListener.printTo(System.out));
        Database db = Database.open(config);

        final String typeName = newRowTypeName();

        final Object[] spec = {
            long.class, "+id",
            String.class, "name",
            BigInteger.class, "num?"
        };

        Class t1 = newRowType(typeName, spec);
        var accessors1 = access(spec, t1);
        var setters1 = accessors1[1];
        var table1 = db.openIndex("test").asTable(t1);

        // Fill 'er up.

        final int fillAmount = 10_000;

        final long seed = 8675309;
        var rnd = new Random(seed);

        var uniqueNames = new HashSet<String>();

        for (int i=0; i<fillAmount; i++) {
            var row = table1.newRow();
            setters1[0].invoke(row, i); // id

            var name = (String) randomValue(rnd, spec, 1);
            uniqueNames.add(name);
            setters1[1].invoke(row, name); // name

            setters1[2].invoke(row, randomValue(rnd, spec, 2)); // num

            table1.store(null, row);
        }

        // Define the table again, with an alt key and secondary index. Note that alt key
        // collisions are likely, but this shouldn't stop the backfill. Which rows exist in the
        // alt key index is undefined, but with the current implementation, the survivors are
        // the ones with the highest corresponding primary key.

        ClassMaker cm = newRowTypeMaker(typeName, spec);
        addAlternateKey(cm, "name");
        addSecondaryIndex(cm, "-num");
        Class t2 = cm.finish();
        var accessors2 = access(spec, t2);
        var setters2 = accessors2[1];
        var table2 = db.openIndex("test").asTable(t2);

        Table nameTable = null, numTable = null;
        for (int i=0; i<1000; i++) {
            try {
                nameTable = table2.viewAlternateKey("name");
                numTable = table2.viewSecondaryIndex("num");
            } catch (IllegalStateException e) {
                assertTrue(e.getMessage().contains("not found"));
            }
            if (nameTable != null && numTable != null) {
                break;
            }
            // Wait for backfill to finish.
            sleep(100);
        }

        assertNotNull(nameTable);
        assertNotNull(numTable);

        assertEquals(uniqueNames.size(), count(nameTable));
        assertEquals(fillAmount, count(numTable));

        verifyIndex(table2, nameTable, fillAmount - uniqueNames.size());
        verifyIndex(nameTable, table2, 0);

        verifyIndex(table2, numTable, 0);
        verifyIndex(numTable, table2, 0);

        // Acting on either table instance should affect the indexes in the same way.

        {
            var row = table1.newRow();
            setters1[0].invoke(row, fillAmount); // id
            setters1[1].invoke(row, uniqueNames.iterator().next()); // name
            setters1[2].invoke(row, BigInteger.ZERO); // num
            try {
                table1.store(null, row);
                fail();
            } catch (UniqueConstraintException e) {
            }
        }

        {
            var row = table2.newRow();
            setters2[0].invoke(row, fillAmount); // id
            setters2[1].invoke(row, uniqueNames.iterator().next()); // name
            setters2[2].invoke(row, BigInteger.ZERO); // num
            try {
                table2.store(null, row);
                fail();
            } catch (UniqueConstraintException e) {
            }
        }

        {
            var row = table1.newRow();
            setters1[0].invoke(row, 1); // id
            assertTrue(table1.delete(null, row));
        }

        assertEquals(uniqueNames.size() - 1, count(nameTable));
        assertEquals(fillAmount - 1, count(numTable));

        {
            var row = table2.newRow();
            setters2[0].invoke(row, 2); // id
            assertTrue(table2.delete(null, row));
        }

        assertEquals(uniqueNames.size() - 2, count(nameTable));
        assertEquals(fillAmount - 2, count(numTable));

        verifyIndex(table2, nameTable, fillAmount - uniqueNames.size());
        verifyIndex(nameTable, table2, 0);

        verifyIndex(table2, numTable, 0);
        verifyIndex(numTable, table2, 0);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void concurrentBackfill() throws Exception {
        var config = new DatabaseConfig().directPageAccess(false);
        //config.eventListener(EventListener.printTo(System.out));
        Database db = Database.open(config);

        final String typeName = newRowTypeName();

        final Object[] spec = {
            long.class, "+id",
            String.class, "name"
        };

        Class t1 = newRowType(typeName, spec);
        var accessors1 = access(spec, t1);
        var setters1 = accessors1[1];
        var table1 = db.openIndex("test").asTable(t1);

        // Fill 'er up.

        final int fillAmount = 100;

        final long seed = 9035768;
        var rnd = new Random(seed);

        for (int i=0; i<fillAmount; i++) {
            var row = table1.newRow();
            setters1[0].invoke(row, i); // id
            setters1[1].invoke(row, "" + randomValue(rnd, spec, 1) + i); // name
            table1.store(null, row);
        }

        // Define the table again, with a secondary index added. Hold a lock on one row to
        // stall the backfill.

        Transaction txn1 = db.newTransaction();
        var lockedRow = table1.newRow();
        setters1[0].invoke(lockedRow, fillAmount / 2); // id
        assertTrue(table1.delete(txn1, lockedRow));

        ClassMaker cm = newRowTypeMaker(typeName, spec);
        addSecondaryIndex(cm, "name");
        Class t2 = cm.finish();
        var accessors2 = access(spec, t2);
        var setters2 = accessors2[1];
        var table2 = db.openIndex("test").asTable(t2);

        sleep(1000);
        try {
            table2.viewSecondaryIndex("name");
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("not found"));
        }

        // As the backfill is running, make some index changes that need to be tracked and
        // applied properly before the backfill finishes.

        // low
        Transaction txn2;
        {
            txn2 = db.newTransaction();
            var row = table1.newRow();
            setters1[0].invoke(row, 0); // id
            setters1[1].invoke(row, "name0"); // name
            table1.store(txn2, row);
        }

        // high
        {
            var row = table1.newRow();
            setters1[0].invoke(row, fillAmount - 1); // id
            setters1[1].invoke(row, "name" + (fillAmount - 1)); // name
            table1.store(null, row);
        }

        // Rollback and release the lock.
        txn1.reset();

        // At this point, backfill is expected to get stuck finishing the new index, because
        // txn2 hasn't finished. Make another change for the backfill to handle.
        sleep(1000);
        {
            var row = table1.newRow();
            setters1[0].invoke(row, fillAmount - 1); // id
            setters1[1].invoke(row, "name-xxx"); // name
            table1.store(null, row);
        }

        // Commit and release the lock.
        txn2.commit();

        Table nameTable = null;
        for (int i=0; i<1000; i++) {
            try {
                nameTable = table2.viewSecondaryIndex("name");
            } catch (IllegalStateException e) {
                assertTrue(e.getMessage().contains("not found"));
            }
            if (nameTable != null) {
                break;
            }
            // Wait for backfill to finish.
            sleep(100);
        }

        assertNotNull(nameTable);

        assertEquals(fillAmount, count(nameTable));

        verifyIndex(table2, nameTable, 0);
        verifyIndex(nameTable, table2, 0);
    }

    @Test
    public void replicaBackfill() throws Exception {
        replicaBackfill(false);
    }

    @Test
    public void replicaBackfillRecover() throws Exception {
        replicaBackfill(true);
    }

    @SuppressWarnings("unchecked")
    private void replicaBackfill(boolean stall) throws Exception {
        var replicaRepl = new SocketReplicator(null, 0);
        var leaderRepl = new SocketReplicator("localhost", replicaRepl.getPort());

        var config = new DatabaseConfig().directPageAccess(false).replicate(leaderRepl);
        //config.eventListener(EventListener.printTo(System.out));

        var leaderDb = newTempDatabase(getClass(), config);
        waitToBecomeLeader(leaderDb, 10);

        config.replicate(replicaRepl);
        var replicaDb = (CoreDatabase) newTempDatabase(getClass(), config);

        final String typeName = newRowTypeName();

        final Object[] spec = {
            long.class, "+id",
            BigDecimal.class, "num"
        };

        Class t1 = newRowType(typeName, spec);
        var accessors1 = access(spec, t1);
        var setters1 = accessors1[1];
        var table1 = leaderDb.openIndex("test").asTable(t1);

        // Fill 'er up.

        final int fillAmount = 10_000;

        final long seed = 8675309;
        var rnd = new Random(seed);

        for (int i=0; i<fillAmount; i++) {
            var row = table1.newRow();
            setters1[0].invoke(row, i); // id
            setters1[1].invoke(row, randomValue(rnd, spec, 1)); // num
            table1.store(null, row);
        }

        fence(leaderRepl, replicaRepl);
        Table replicaTable = replicaDb.openIndex("test").asTable(t1);

        // Define the table again, with a secondary index added.

        if (stall) {
            replicaDb.rowStore().mStallTasks = true;
        }

        ClassMaker cm = newRowTypeMaker(typeName, spec);
        addSecondaryIndex(cm, "num");
        Class t2 = cm.finish();
        var accessors2 = access(spec, t2);
        var setters2 = accessors2[1];
        var table2 = leaderDb.openIndex("test").asTable(t2);

        Table numTable = null;
        for (int i=0; i<1000; i++) {
            try {
                numTable = table2.viewSecondaryIndex("num");
            } catch (IllegalStateException e) {
                assertTrue(e.getMessage().contains("not found"));
            }
            if (numTable != null) {
                break;
            }
            // Wait for backfill to finish.
            sleep(100);
        }

        assertNotNull(numTable);

        if (stall) {
            for (int i=0; i<10; i++) {
                try {
                    replicaTable.viewSecondaryIndex("num");
                    fail();
                } catch (IllegalStateException e) {
                    assertTrue(e.getMessage().contains("not found"));
                }
                sleep(100);
            }

            // Simulate re-opening the replica.
            RowStore rs = replicaDb.rowStore();
            rs.mStallTasks = false;
            rs.finishAllWorkflowTasks();
        }

        Table replicaNumTable = null;
        for (int i=0; i<1000; i++) {
            try {
                replicaNumTable = replicaTable.viewSecondaryIndex("num");
            } catch (IllegalStateException e) {
                assertTrue(e.getMessage().contains("not found"));
            }
            if (replicaNumTable != null) {
                break;
            }
            // Wait for backfill to finish.
            sleep(100);
        }

        assertNotNull(replicaNumTable);

        assertEquals(fillAmount, count(numTable));
        verifyIndex(table2, numTable, 0);
        verifyIndex(numTable, table2, 0);

        assertEquals(fillAmount, count(replicaNumTable));
        verifyIndex(replicaTable, replicaNumTable, 0);
        verifyIndex(replicaNumTable, replicaTable, 0);

        // Acting on either leader table instance should affect the indexes in the same way.

        {
            var row = table1.newRow();
            setters1[0].invoke(row, 1); // id
            setters1[1].invoke(row, new BigDecimal("6739083340380621870")); // num
            table1.store(null, row);
        }

        {
            var row = table2.newRow();
            setters2[0].invoke(row, 2); // id
            setters2[1].invoke(row, new BigDecimal("3609827348865711141")); // num
            table2.store(null, row);
        }

        {
            var row = table2.newRow();
            setters2[0].invoke(row, fillAmount); // id
            setters2[1].invoke(row, new BigDecimal("4943524186873198773")); // num
            table2.store(null, row);
        }

        assertEquals(fillAmount + 1, count(numTable));
        verifyIndex(table2, numTable, 0);
        verifyIndex(numTable, table2, 0);

        fence(leaderRepl, replicaRepl);

        assertEquals(fillAmount + 1, count(replicaNumTable));
        verifyIndex(replicaTable, replicaNumTable, 0);
        verifyIndex(replicaNumTable, replicaTable, 0);

        leaderDb.close();
        replicaDb.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void addColumnAndIndex() throws Exception {
        var config = new DatabaseConfig().directPageAccess(false);
        //config.eventListener(EventListener.printTo(System.out));
        Database db = Database.open(config);

        final String typeName = newRowTypeName();

        Object[] spec = {
            long.class, "+id",
            BigInteger.class, "num?"
        };

        ClassMaker cm = newRowTypeMaker(typeName, spec);
        Class t1 = cm.finish();
        var accessors1 = access(spec, t1);
        var setters1 = accessors1[1];
        var table1 = db.openIndex("test").asTable(t1);

        // Fill 'er up.

        final int fillAmount = 100;

        final long seed = 8675308;
        var rnd = new Random(seed);

        for (int i=0; i<fillAmount; i++) {
            var row = table1.newRow();
            setters1[0].invoke(row, i); // id
            setters1[1].invoke(row, randomValue(rnd, spec, 1)); // num
            table1.store(null, row);
        }

        // Now add an index which refers to a newly added column. It should default to "".

        spec = new Object[] {
            long.class, "+id",
            String.class, "name",
            BigInteger.class, "num?"
        };

        cm = newRowTypeMaker(typeName, spec);
        addSecondaryIndex(cm, "-name");
        Class t2 = cm.finish();
        var accessors2 = access(spec, t2);
        var setters2 = accessors2[1];
        var table2 = db.openIndex("test").asTable(t2);

        Table nameTable = null;
        for (int i=0; i<1000; i++) {
            try {
                nameTable = table2.viewSecondaryIndex("name");
            } catch (IllegalStateException e) {
                assertTrue(e.getMessage().contains("not found"));
            }
            if (nameTable != null) {
                break;
            }
            // Wait for backfill to finish.
            sleep(100);
        }

        assertEquals(fillAmount, verifyIndex(table2, nameTable, 0));

        // Add a new row using the new table definition.

        {
            var row = table2.newRow();
            setters2[0].invoke(row, 999998L); // id
            setters2[1].invoke(row, "name-999998"); // name
            setters2[2].invoke(row, new BigInteger("999998")); // num
            table2.store(null, row);
        }

        assertEquals(fillAmount + 1, verifyIndex(table2, nameTable, 0));

        // Add a new row using the old table definition.

        {
            var row = table1.newRow();
            setters1[0].invoke(row, 999999L); // id
            setters1[1].invoke(row, new BigInteger("999999")); // num
            table1.store(null, row);
        }

        assertEquals(fillAmount + 2, verifyIndex(table2, nameTable, 0));
    }

    @Test
    public void dropIndex() throws Exception {
        dropIndex(false);
    }

    @Test
    public void dropIndexWithStall() throws Exception {
        dropIndex(true);
    }

    @SuppressWarnings("unchecked")
    private void dropIndex(boolean stall) throws Exception {
        var config = new DatabaseConfig().directPageAccess(false);
        //config.eventListener(EventListener.printTo(System.out));
        Database db = Database.open(config);

        final String typeName = newRowTypeName();

        final Object[] spec = {
            long.class, "+id",
            String.class, "name",
            BigInteger.class, "num?"
        };

        ClassMaker cm = newRowTypeMaker(typeName, spec);
        addSecondaryIndex(cm, "name");
        Class t1 = cm.finish();
        var accessors1 = access(spec, t1);
        var setters1 = accessors1[1];
        var table1 = db.openIndex("test").asTable(t1);

        // Fill 'er up.

        final int fillAmount = 10_000;

        final long seed = 8675308;
        var rnd = new Random(seed);

        for (int i=0; i<fillAmount; i++) {
            var row = table1.newRow();
            setters1[0].invoke(row, i); // id

            var name = (String) randomValue(rnd, spec, 1);
            setters1[1].invoke(row, name); // name

            setters1[2].invoke(row, randomValue(rnd, spec, 2)); // num

            table1.store(null, row);
        }

        var nameTable = table1.viewSecondaryIndex("name");
        verifyIndex(nameTable, table1, 0);
        assertEquals(fillAmount, nameTable.newStream(null).count());
        long nameTableId = ((AbstractTable) nameTable).mSource.id();

        assertNotNull(db.indexById(nameTableId));

        // Define the table again, but without the secondary index.

        Transaction txn = null;
        RowScanner scanner = null;
        if (stall) {
            // Prevent deleting the index until the scan has finished.
            txn = db.newTransaction();
            txn.lockMode(LockMode.READ_COMMITTED);
            scanner = nameTable.newRowScanner(txn);
        }

        Class t2 = newRowType(typeName, spec);
        var accessors2 = access(spec, t2);
        var setters2 = accessors2[1];
        var table2 = db.openIndex("test").asTable(t2);

        try {
            table2.viewSecondaryIndex("name");
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("not found"));
        }

        if (stall) {
            try {
                assertEquals(fillAmount, nameTable.newStream(null).count());
            } catch (Exception e) {
                if (e instanceof LockTimeoutException) {
                    // Okay too.
                } else {
                    throw e;
                }
            }

            scanner.close();
            txn.exit();
        }

        for (int i=100; --i>=0; ) {
            long count = nameTable.newStream(null).count();
            if (count == 0) {
                break;
            }
            if (i == 0) {
                assertEquals(0, count);
            }
            sleep(100);
        }

        {
            var row = table1.newRow();
            setters1[0].invoke(row, 10); // id
            setters1[1].invoke(row, "new name"); // name
            table1.merge(null, row);
            //System.out.println(row);
        }

        {
            var row = table2.newRow();
            setters2[0].invoke(row, 20); // id
            setters2[1].invoke(row, "new name"); // name
            table2.merge(null, row);
            //System.out.println(row);
        }

        try {
            table2.viewSecondaryIndex("name");
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("not found"));
        }

        assertEquals(0, nameTable.newStream(null).count());

        assertNull(db.indexById(nameTableId));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void dropIndexReplica() throws Exception {
        var replicaRepl = new SocketReplicator(null, 0);
        var leaderRepl = new SocketReplicator("localhost", replicaRepl.getPort());

        var config = new DatabaseConfig().directPageAccess(false).replicate(leaderRepl);
        //config.eventListener(EventListener.printTo(System.out));

        var leaderDb = newTempDatabase(getClass(), config);
        waitToBecomeLeader(leaderDb, 10);

        config.replicate(replicaRepl);
        var replicaDb = newTempDatabase(getClass(), config);

        final String typeName = newRowTypeName();

        final Object[] spec = {
            long.class, "+id",
            String.class, "name",
            BigInteger.class, "num?"
        };

        ClassMaker cm = newRowTypeMaker(typeName, spec);
        addSecondaryIndex(cm, "name");
        Class t1 = cm.finish();
        var accessors1 = access(spec, t1);
        var setters1 = accessors1[1];
        var table1 = leaderDb.openIndex("test").asTable(t1);

        // Fill 'er up.

        final int fillAmount = 10_000;

        final long seed = 8675308;
        var rnd = new Random(seed);

        for (int i=0; i<fillAmount; i++) {
            var row = table1.newRow();
            setters1[0].invoke(row, i); // id

            var name = (String) randomValue(rnd, spec, 1);
            setters1[1].invoke(row, name); // name

            setters1[2].invoke(row, randomValue(rnd, spec, 2)); // num

            table1.store(null, row);
        }

        var nameTable = table1.viewSecondaryIndex("name");
        verifyIndex(nameTable, table1, 0);
        assertEquals(fillAmount, nameTable.newStream(null).count());
        long nameTableId = ((AbstractTable) nameTable).mSource.id();

        assertNotNull(leaderDb.indexById(nameTableId));

        fence(leaderRepl, replicaRepl);

        Table replicaTable = replicaDb.openIndex("test").asTable(t1);

        var replicaNameTable = replicaTable.viewSecondaryIndex("name");
        assertEquals(nameTableId, ((AbstractTable) replicaNameTable).mSource.id());

        verifyIndex(replicaNameTable, replicaTable, 0);
        assertEquals(fillAmount, replicaNameTable.newStream(null).count());

        // Define the table again, but without the secondary index.

        Class t2 = newRowType(typeName, spec);
        var accessors2 = access(spec, t2);
        var setters2 = accessors2[1];
        var table2 = leaderDb.openIndex("test").asTable(t2);

        try {
            table2.viewSecondaryIndex("name");
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("not found"));
        }

        for (int i=100; --i>=0; ) {
            long count = nameTable.newStream(null).count();
            if (count == 0) {
                break;
            }
            if (i == 0) {
                assertEquals(0, count);
            }
            sleep(100);
        }

        for (int i=100; --i>=0; ) {
            long count = replicaNameTable.newStream(null).count();
            if (count == 0) {
                break;
            }
            if (i == 0) {
                assertEquals(0, count);
            }
            sleep(100);
        }

        try {
            table2.viewSecondaryIndex("name");
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("not found"));
        }

        try {
            replicaTable.viewSecondaryIndex("name");
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("not found"));
        }

        leaderDb.close();
        replicaDb.close();
    }

    private static <R> int verifyIndex(Table<R> a, Table<R> b, int expectMissing)
        throws Exception
    {
        int found = 0;
        RowScanner<R> s = a.newRowScanner(null);

        for (R ra = s.row(); s.row() != null; ra = s.step(ra)) {
            found++;
            R rb = a.cloneRow(ra);
            assertTrue(b.load(null, rb));
            a.load(null, rb);
            if (ra.equals(rb)) {
                assertEquals(ra.hashCode(), rb.hashCode());
            } else {
                if (--expectMissing < 0) {
                    assertEquals(ra, rb);
                }
            }
        }

        assertEquals(0, expectMissing);

        return found;
    }

    private static <R> long count(Table<R> table) throws Exception {
        long count = 0;
        RowScanner<R> s = table.newRowScanner(null);
        for (R row = s.row(); s.row() != null; row = s.step(row)) {
            count++;
        }
        return count;
    }

    private static <R> void dump(Table<R> table) throws Exception {
        RowScanner<R> s = table.newRowScanner(null);
        for (R row = s.row(); s.row() != null; row = s.step(row)) {
            System.out.println(row);
        }
    }

    private static <R> void scanExpect(Table<R> table, String... expectRows) throws Exception {
        int pos = 0;
        RowScanner<R> s = table.newRowScanner(null);
        for (R row = s.row(); s.row() != null; row = s.step(row)) {
            String expectRow = expectRows[pos++];
            assertTrue(row.toString().contains(expectRow));
        }
        assertEquals(expectRows.length, pos);
    }

    /**
     * Writes a fence to the leader and waits for the replica to catch up.
     */
    private static void fence(SocketReplicator leaderRepl, SocketReplicator replicaRepl)
        throws Exception
    {
        byte[] message = ("fence:" + System.nanoTime()).getBytes();
        leaderRepl.writeControl(message);
        replicaRepl.waitForControl(message);
    }

    @PrimaryKey("id")
    @AlternateKey("path")
    @SecondaryIndex("name")
    @SecondaryIndex({"+num", "-name"})
    public interface TestRow {
        long id();
        void id(long id);

        String path();
        void path(String str);

        String name();
        void name(String str);

        BigDecimal num();
        void num(BigDecimal num);
    }
}
