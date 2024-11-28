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

import java.math.BigDecimal;
import java.math.BigInteger;

import java.util.HashSet;
import java.util.Random;

import java.util.concurrent.TimeUnit;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.maker.ClassMaker;

import org.cojen.tupl.*;

import org.cojen.tupl.core.LocalDatabase;

import static org.cojen.tupl.TestUtils.*;

import static org.cojen.tupl.table.RowTestUtils.*;

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
        Database db = Database.open(new DatabaseConfig()
                                    .lockTimeout(100, TimeUnit.MILLISECONDS));
        var table = (StoredTable<TestRow>) db.openTable(TestRow.class);

        Table<TestRow> alt = table.viewAlternateKey("path").viewUnjoined();
        var ix1 = (StoredTable<TestRow>) table.viewSecondaryIndex("name").viewUnjoined();
        var ix2 = (StoredTable<TestRow>) table.viewSecondaryIndex("num", "name").viewUnjoined();

        try {
            ix1.viewAlternateKey("path");
            fail();
        } catch (IllegalStateException e) {
        }

        try {
            ix1.viewSecondaryIndex("name");
            fail();
        } catch (IllegalStateException e) {
        }

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
            alt.load(null, row);
            assertEquals(2, row.id());
            assertEquals("path2", row.path());
            try {
                row.name();
                fail();
            } catch (UnsetColumnException e) {
            }
            assertTrue(row.toString().contains("{id=2, path=path2}"));
            assertTrue(alt.exists(null, row));
            try {
                alt.store(null, row);
            } catch (UnmodifiableViewException e) {
            }
        }

        {
            TestRow row = table.newRow();
            row.path("pathx");
            row.name("xxx");
            row.id(2);
            assertFalse(alt.tryLoad(null, row));
            assertTrue(row.toString().contains("{*path=pathx}"));
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
            ix1.load(null, row);
            try {
                row.path();
                fail();
            } catch (UnsetColumnException e) {
            }
            assertTrue(row.toString().contains("{id=3, name=name1}"));
            assertTrue(ix1.exists(null, row));
            try {
                ix1.insert(null, row);
            } catch (UnmodifiableViewException e) {
            }
        }

        {
            TestRow row = table.newRow();
            row.name("namex");
            row.path("xxx");
            row.id(2);
            assertFalse(ix1.tryLoad(null, row));
            assertTrue(row.toString().contains("{*id=2, *name=namex}"));
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
            ix2.load(null, row);
            try {
                row.path();
                fail();
            } catch (UnsetColumnException e) {
            }
            assertTrue(row.toString().contains("{id=2, name=name2, num=123}"));
            assertTrue(ix2.exists(null, row));
            try {
                ix2.delete(null, row);
            } catch (UnmodifiableViewException e) {
            }
        }

        {
            TestRow row = table.newRow();
            row.num(new BigDecimal("999"));
            row.name("namex");
            row.path("xxx");
            row.id(2);
            assertFalse(ix2.tryLoad(null, row));
            assertTrue(row.toString().contains("{*id=2, *name=namex, *num=999}"));
        }

        try {
            ix1.newUpdater(null);
            fail();
        } catch (UnmodifiableViewException e) {
        }

        scanExpect(alt, "{id=1, path=path1}",
                   "{id=2, path=path2}", "{id=3, path=path3}");

        scanExpect(ix1, "{id=1, name=name1}",
                   "{id=3, name=name1}", "{id=2, name=name2}");

        scanExpect(ix2, "{id=2, name=name2, num=123}",
                   "{id=1, name=name1, num=123}", "{id=3, name=name1, num=987}");

        {
            TestRow row = table.newRow();
            row.id(2);
            table.delete(null, row);
        }

        scanExpect(alt, "{id=1, path=path1}", "{id=3, path=path3}");

        scanExpect(ix1, "{id=1, name=name1}", "{id=3, name=name1}");

        scanExpect(ix2, "{id=1, name=name1, num=123}", "{id=3, name=name1, num=987}");

        {
            Transaction txn = db.newTransaction();
            TestRow row = table.newRow();
            row.id(3);
            table.delete(txn, row);

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

        scanExpect(alt, "{id=1, path=path1}");

        scanExpect(ix1, "{id=1, name=name1}");

        scanExpect(ix2, "{id=1, name=name1, num=123}");

        {
            TestRow row = table.newRow();
            row.id(1);
            row.path("no-path");
            row.name("no-name");
            row.num(BigDecimal.ZERO);
            TestRow oldRow = table.exchange(null, row);
            assertTrue(oldRow.toString().contains("{id=1, name=name1, num=123, path=path1}"));
        }

        scanExpect(alt, "{id=1, path=no-path}");

        scanExpect(ix1, "{id=1, name=no-name}");

        scanExpect(ix2, "{id=1, name=no-name, num=0}");

        {
            TestRow row = table.newRow();
            row.id(5);
            row.path("path5");
            row.name("name5");
            row.num(new BigDecimal("555"));
            table.insert(null, row);
        }

        scanExpect(alt, "{id=1, path=no-path}", "{id=5, path=path5}");

        scanExpect(ix1, "{id=5, name=name5}", "{id=1, name=no-name}");

        scanExpect(ix2, "{id=1, name=no-name, num=0}", "{id=5, name=name5, num=555}");

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
            table.replace(null, row);
        }

        scanExpect(alt, "{id=1, path=no-path}", "{id=5, path=path5}");

        scanExpect(ix1, "{id=5, name=!name5}", "{id=1, name=no-name}");

        scanExpect(ix2, "{id=5, name=!name5, num=-5}", "{id=1, name=no-name, num=0}");

        {
            TestRow row = table.newRow();
            row.id(5);
            row.path("!path5");
            row.name("!name55");
            row.num(new BigDecimal("55"));
            table.update(null, row);
        }

        scanExpect(alt, "{id=5, path=!path5}", "{id=1, path=no-path}");

        scanExpect(ix1, "{id=5, name=!name55}", "{id=1, name=no-name}");

        scanExpect(ix2, "{id=1, name=no-name, num=0}",
                   "{id=5, name=!name55, num=55}");

        {
            TestRow row = table.newRow();
            row.id(1);
            row.name("name1");
            table.update(null, row);
        }

        scanExpect(alt, "{id=5, path=!path5}", "{id=1, path=no-path}");

        scanExpect(ix1, "{id=5, name=!name55}", "{id=1, name=name1}");

        scanExpect(ix2, "{id=1, name=name1, num=0}", "{id=5, name=!name55, num=55}");

        {
            TestRow row = table.newRow();
            row.id(5);
            row.name("name5");
            table.merge(null, row);
            assertTrue(row.toString().contains("{id=5, name=name5, num=55, path=!path5}"));
        }

        scanExpect(alt, "{id=5, path=!path5}", "{id=1, path=no-path}");

        scanExpect(ix1, "{id=1, name=name1}", "{id=5, name=name5}");

        scanExpect(ix2, "{id=1, name=name1, num=0}", "{id=5, name=name5, num=55}");

        {
            // Lock all the index entries that shouldn't be updated.
            Transaction txn = db.newTransaction();
            Scanner<TestRow> s = ix1.newScanner(txn);
            for (TestRow row = s.row(); s.row() != null; row = s.step(row)) {}
            s = ix2.newScanner(txn);
            for (TestRow row = s.row(); s.row() != null; row = s.step(row)) {}

            TestRow row = table.newRow();
            row.id(1);
            row.path("path-1");
            row.name("name1");        // no change
            row.num(BigDecimal.ZERO); // no change
            table.store(null, row);

            txn.exit();
        }

        scanExpect(alt, "{id=5, path=!path5}", "{id=1, path=path-1}");

        scanExpect(ix1, "{id=1, name=name1}", "{id=5, name=name5}");

        scanExpect(ix2, "{id=1, name=name1, num=0}", "{id=5, name=name5, num=55}");

        db.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void basicBackfill() throws Exception {
        var config = new DatabaseConfig();
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
        var table2 = (StoredTable) db.openIndex("test").asTable(t2);

        Table nameTable = null, numTable = null;
        for (int i=0; i<1000; i++) {
            try {
                nameTable = table2.viewAlternateKey("name").viewUnjoined();
                numTable = table2.viewSecondaryIndex("num").viewUnjoined();
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
            table1.delete(null, row);
        }

        assertEquals(uniqueNames.size() - 1, count(nameTable));
        assertEquals(fillAmount - 1, count(numTable));

        {
            var row = table2.newRow();
            setters2[0].invoke(row, 2); // id
            table2.delete(null, row);
        }

        assertEquals(uniqueNames.size() - 2, count(nameTable));
        assertEquals(fillAmount - 2, count(numTable));

        verifyIndex(table2, nameTable, fillAmount - uniqueNames.size());
        verifyIndex(nameTable, table2, 0);

        verifyIndex(table2, numTable, 0);
        verifyIndex(numTable, table2, 0);

        db.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void concurrentBackfill() throws Exception {
        var config = new DatabaseConfig();
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
        table1.delete(txn1, lockedRow);

        ClassMaker cm = newRowTypeMaker(typeName, spec);
        addSecondaryIndex(cm, "name");
        Class t2 = cm.finish();
        var accessors2 = access(spec, t2);
        var setters2 = accessors2[1];
        var table2 = (StoredTable) db.openIndex("test").asTable(t2);

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
                nameTable = table2.viewSecondaryIndex("name").viewUnjoined();
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

        db.close();
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

        var config = new DatabaseConfig().replicate(leaderRepl);
        //config.eventListener(EventListener.printTo(System.out));

        var leaderDb = newTempDatabase(getClass(), config);
        waitToBecomeLeader(leaderDb, 10);

        config.replicate(replicaRepl);
        var replicaDb = (LocalDatabase) newTempDatabase(getClass(), config);

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
        var replicaTable = (StoredTable) replicaDb.openIndex("test").asTable(t1);

        // Define the table again, with a secondary index added.

        if (stall) {
            replicaDb.rowStore().mStallTasks = true;
        }

        ClassMaker cm = newRowTypeMaker(typeName, spec);
        addSecondaryIndex(cm, "num");
        Class t2 = cm.finish();
        var accessors2 = access(spec, t2);
        var setters2 = accessors2[1];
        var table2 = (StoredTable) leaderDb.openIndex("test").asTable(t2);

        Table numTable = null;
        for (int i=0; i<1000; i++) {
            try {
                numTable = table2.viewSecondaryIndex("num").viewUnjoined();
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
                replicaNumTable = replicaTable.viewSecondaryIndex("num").viewUnjoined();
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
        var config = new DatabaseConfig();
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
        var table2 = (StoredTable) db.openIndex("test").asTable(t2);

        Table nameTable = null;
        for (int i=0; i<1000; i++) {
            try {
                nameTable = table2.viewSecondaryIndex("name").viewUnjoined();
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

        db.close();
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
        var config = new DatabaseConfig();
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
        var table1 = (StoredTable) db.openIndex("test").asTable(t1);

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

        var nameTable = table1.viewSecondaryIndex("name").viewUnjoined();
        verifyIndex(nameTable, table1, 0);
        assertEquals(fillAmount, nameTable.newStream(null).count());
        long nameTableId = ((StoredTable) nameTable).mSource.id();

        assertNotNull(db.indexById(nameTableId));

        // Define the table again, but without the secondary index.

        Transaction txn = null;
        Scanner scanner = null;
        if (stall) {
            // Prevent deleting the index until the scan has finished.
            txn = db.newTransaction();
            txn.lockMode(LockMode.READ_COMMITTED);
            scanner = nameTable.newScanner(txn);
        }

        Class t2 = newRowType(typeName, spec);
        var accessors2 = access(spec, t2);
        var setters2 = accessors2[1];
        var table2 = (StoredTable) db.openIndex("test").asTable(t2);

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
            try {
                nameTable.newStream(null).count();
            } catch (Throwable e) {
                if (e instanceof DeletedIndexException) {
                    break;
                }
                throw e;
            }
            if (i == 0) {
                fail();
            }
            sleep(100);
        }

        {
            var row = table1.newRow();
            setters1[0].invoke(row, 10); // id
            setters1[1].invoke(row, "new name"); // name
            table1.merge(null, row);
        }

        {
            var row = table2.newRow();
            setters2[0].invoke(row, 20); // id
            setters2[1].invoke(row, "new name"); // name
            table2.merge(null, row);
        }

        try {
            table2.viewSecondaryIndex("name");
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("not found"));
        }

        try {
            nameTable.newStream(null).count();
            fail();
        } catch (Throwable e) {
            if (!(e instanceof DeletedIndexException)) {
                throw e;
            }
        }

        assertNull(db.indexById(nameTableId));

        db.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void dropIndexReplica() throws Exception {
        var replicaRepl = new SocketReplicator(null, 0);
        var leaderRepl = new SocketReplicator("localhost", replicaRepl.getPort());

        var config = new DatabaseConfig().replicate(leaderRepl);
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
        var table1 = (StoredTable) leaderDb.openIndex("test").asTable(t1);

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

        var nameTable = table1.viewSecondaryIndex("name").viewUnjoined();
        verifyIndex(nameTable, table1, 0);
        assertEquals(fillAmount, nameTable.newStream(null).count());
        long nameTableId = ((StoredTable) nameTable).mSource.id();

        assertNotNull(leaderDb.indexById(nameTableId));

        fence(leaderRepl, replicaRepl);

        var replicaTable = (StoredTable) replicaDb.openIndex("test").asTable(t1);

        var replicaNameTable = replicaTable.viewSecondaryIndex("name").viewUnjoined();
        assertEquals(nameTableId, ((StoredTable) replicaNameTable).mSource.id());

        verifyIndex(replicaNameTable, replicaTable, 0);
        assertEquals(fillAmount, replicaNameTable.newStream(null).count());

        // Define the table again, but without the secondary index.

        Class t2 = newRowType(typeName, spec);
        var accessors2 = access(spec, t2);
        var setters2 = accessors2[1];
        var table2 = (StoredTable) leaderDb.openIndex("test").asTable(t2);

        try {
            table2.viewSecondaryIndex("name");
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("not found"));
        }

        for (int i=100; --i>=0; ) {
            try {
                nameTable.newStream(null).count();
            } catch (Throwable e) {
                if (e instanceof DeletedIndexException) {
                    break;
                }
                throw e;
            }
            if (i == 0) {
                fail();
            }
            sleep(100);
        }

        for (int i=100; --i>=0; ) {
            try {
                replicaNameTable.newStream(null).count();
            } catch (Throwable e) {
                if (e instanceof DeletedIndexException) {
                    break;
                }
                throw e;
            }
            if (i == 0) {
                fail();
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
        Scanner<R> s = a.newScanner(null);

        for (R ra = s.row(); s.row() != null; ra = s.step(ra)) {
            found++;
            R rb = a.cloneRow(ra);
            assertTrue(b.tryLoad(null, rb));
            a.tryLoad(null, rb);
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
        Scanner<R> s = table.newScanner(null);
        for (R row = s.row(); s.row() != null; row = s.step(row)) {
            count++;
        }
        return count;
    }

    private static <R> void dump(Table<R> table) throws Exception {
        Scanner<R> s = table.newScanner(null);
        for (R row = s.row(); s.row() != null; row = s.step(row)) {
            System.out.println(row);
        }
    }

    private static <R> void scanExpect(Table<R> table, String... expectRows) throws Exception {
        int pos = 0;
        Scanner<R> s = table.newScanner(null);
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

    @Test
    public void coveringIndex() throws Exception {
        // Test for when only the value portion of a covering index changes. The "num" column
        // is defined after the primary key has been fully specified in the index, and so that
        // defines it as the value portion.

        Database db = Database.open(new DatabaseConfig());
        var table = (StoredTable<TestRow2>) db.openTable(TestRow2.class);
        Table<TestRow2> ix = table.viewSecondaryIndex("name", "id", "num").viewUnjoined();

        TestRow2 row = table.newRow();
        row.id(1);
        row.name("name-1");
        row.path("path-1");
        row.num(100);
        table.store(null, row);

        assertTrue(ix.tryLoad(null, row));

        row.num(200);
        table.update(null, row);

        row.num(200);
        assertTrue(ix.tryLoad(null, row));

        db.close();
    }

    @PrimaryKey("id")
    @SecondaryIndex({"name", "id", "num"})
    public interface TestRow2 {
        long id();
        void id(long id);

        String name();
        void name(String str);

        String path();
        void path(String str);

        int num();
        void num(int num);
    }

    @Test
    public void loadJoin() throws Exception {
        Database db = Database.open(new DatabaseConfig());
        var table = (StoredTable<TestRow>) db.openTable(TestRow.class);

        Table<TestRow> alt = table.viewAlternateKey("path");
        Table<TestRow> ix1 = table.viewSecondaryIndex("name");
        Table<TestRow> ix2 = table.viewSecondaryIndex("num", "name");

        for (int i=1; i<=3; i++) {
            TestRow row = table.newRow();
            row.id(i);
            row.path("path-" + i);
            row.name("name-" + i);
            row.num(BigDecimal.valueOf(i));
            table.insert(null, row);
        }

        // Test successful loads.

        for (int i=1; i<=3; i++) {
            TestRow expect = table.newRow();
            expect.id(2);
            table.load(null, expect);
            
            Transaction txn = switch (i) {
                case 1 -> db.newTransaction(); case 2 -> Transaction.BOGUS; default -> null;
            };

            {
                TestRow row = alt.newRow();
                row.path("path-2");
                row.id(999); // should be ignored
                alt.load(txn, row);
                assertEquals(expect, row);
            }

            if (i == 1) {
                txn.reset();
            }

            {
                TestRow row = ix1.newRow();
                row.name("name-2");
                row.id(2);
                ix1.load(txn, row);
                assertEquals(expect, row);
            }

            if (i == 1) {
                txn.reset();
            }

            {
                TestRow row = ix2.newRow();
                row.num(BigDecimal.valueOf(2));
                row.name("name-2");
                row.id(2);
                ix2.load(txn, row);
                assertEquals(expect, row);
            }

            if (i == 1) {
                txn.reset();
            }
        }

        // Test simple failed loads.

        for (int i=1; i<=3; i++) {
            TestRow expect = table.newRow();
            expect.id(2);
            table.load(null, expect);
            
            Transaction txn = switch (i) {
                case 1 -> db.newTransaction(); case 2 -> Transaction.BOGUS; default -> null;
            };

            {
                TestRow row = alt.newRow();
                row.path("path-x");
                row.id(999); // should be ignored
                TestRow copy = alt.cloneRow(row);
                assertFalse(alt.tryLoad(txn, row));
                assertTrue(row.toString().contains("{*path=path-x}"));
                // Fields must remain unchanged.
                assertEquals(0, alt.comparator("+path+id").compare(row, copy));
            }

            if (i == 1) {
                txn.reset();
            }

            {
                TestRow row = ix1.newRow();
                row.name("name-x");
                row.id(2);
                assertFalse(ix1.tryLoad(txn, row));
                assertTrue(row.toString().contains("{*id=2, *name=name-x}"));
            }

            if (i == 1) {
                txn.reset();
            }

            {
                TestRow row = ix2.newRow();
                row.num(BigDecimal.valueOf(2));
                row.name("name-x");
                row.id(2);
                assertFalse(ix2.tryLoad(txn, row));
                assertTrue(row.toString().contains("{*id=2, *name=name-x, *num=2}"));
            }

            if (i == 1) {
                txn.reset();
            }
        }

        db.close();
    }

    @Test
    public void loadJoinRace() throws Exception {
        // Test the join double check.

        Database db = Database.open(new DatabaseConfig().lockTimeout(2, TimeUnit.SECONDS));
        Index ix = db.openIndex("test");
        var table = (StoredTable<TestRow>) ix.asTable(TestRow.class);
        var alt = (StoredTable<TestRow>) table.viewAlternateKey("path");

        {
            TestRow row = table.newRow();
            row.id(1);
            row.path("path");
            row.name("name");
            row.num(BigDecimal.ZERO);
            table.insert(null, row);
        }

        byte[] key;
        try (Cursor c = ix.newCursor(null)) {
            c.first();
            key = c.key();
        }

        Transaction txn1 = db.newTransaction();
        ix.lockExclusive(txn1, key);

        try {
            TestRow row = alt.newRow();
            row.path("path");
            alt.load(null, row);
            fail();
        } catch (LockTimeoutException e) {
            // Cannot load the primary row, which is locked by txn1.
        }

        {
            TestRow row = alt.newRow();
            row.path("path");
            // Unjoined load isn't blocked.
            alt.viewUnjoined().load(null, row);
            assertEquals(1, row.id());
        }

        // Block in a background thread.
        var task = startTestTaskAndWaitUntilBlocked(() -> {
            try {
                TestRow row = alt.newRow();
                row.path("path");
                row.id(999); // should be ignored
                TestRow copy = alt.cloneRow(row);
                assertFalse(alt.tryLoad(null, row));
                assertTrue(row.toString().contains("{*path=path}"));
                // Fields must remain unchanged.
                assertEquals(0, alt.comparator("+path+id").compare(row, copy));
            } catch (Throwable e) {
                RowUtils.rethrow(e);
            }
        });

        // Now update the row such that the double check fails.
        TestRow row = table.newRow();
        row.id(1);
        row.path("xxx");
        table.update(txn1, row);
        txn1.commit();

        task.join();

        db.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void inconsistentIndexes() throws Exception {
        var config = new DatabaseConfig();
        Database db = Database.open(config);

        final String typeName = newRowTypeName();

        final Object[] spec1 = {
            long.class, "+id",
            int.class, "num"
        };
        ClassMaker cm1 = newRowTypeMaker(typeName, spec1);
        Class t1 = cm1.finish();
        var accessors1 = access(spec1, t1);
        var getters1 = accessors1[0];
        var setters1 = accessors1[1];
        var table1 = db.openIndex("test").asTable(t1);

        // Add a "name" column and a secondary index on it.
        final Object[] spec2 = {
            long.class, "+id",
            String.class, "name",
            int.class, "num"
        };
        ClassMaker cm2 = newRowTypeMaker(typeName, spec2);
        addSecondaryIndex(cm2, "+name");
        Class t2 = cm2.finish();
        var accessors2 = access(spec2, t2);
        var getters2 = accessors2[0];
        var setters2 = accessors2[1];
        var table2 = db.openIndex("test").asTable(t2);

        // Wait for the secondary index to be ready.
        Table secondary = null;
        for (int i=0; i<1000; i++) {
            try {
                secondary = ((StoredTable) table2).viewSecondaryIndex("name").viewUnjoined();
                break;
            } catch (IllegalStateException e) {
                assertTrue(e.getMessage().contains("not found"));
                Thread.sleep(100);
            }
        }

        // FIXME: Sometimes the secondary index is never ready.
        assertNotNull(secondary);

        // Can store using original table definition despite not having the "name" column as
        // needed by the secondary index.
        {
            var row = table1.newRow();
            setters1[0].invoke(row, 1); // id
            setters1[1].invoke(row, 1); // num
            table1.store(null, row);
        }

        // Can store using new table definition.
        {
            var row = table2.newRow();
            setters2[0].invoke(row, 2); // id
            setters2[1].invoke(row, "name-2"); // name
            setters2[2].invoke(row, 2); // num
            table2.store(null, row);
        }

        // Loading from the original table should work.
        {
            var row = table1.newRow();
            setters1[0].invoke(row, 1); // id
            table1.load(null, row);
            assertEquals(1L, getters1[0].invoke(row));
            assertEquals(1, getters1[1].invoke(row));

            row = table1.newRow();
            setters1[0].invoke(row, 2); // id
            table1.load(null, row);
            assertEquals(2L, getters1[0].invoke(row));
            assertEquals(2, getters1[1].invoke(row));
        }

        // Loading from the new table should work.
        {
            var row = table2.newRow();
            setters2[0].invoke(row, 1); // id
            table2.load(null, row);
            assertEquals(1L, getters2[0].invoke(row));
            assertEquals("", getters2[1].invoke(row));
            assertEquals(1, getters2[2].invoke(row));

            row = table2.newRow();
            setters2[0].invoke(row, 2); // id
            table2.load(null, row);
            assertEquals(2L, getters2[0].invoke(row));
            assertEquals("name-2", getters2[1].invoke(row));
            assertEquals(2, getters2[2].invoke(row));
        }

        // Directly examine the secondary index.
        try (Scanner s = secondary.newScanner(null)) {
            var row = s.row();
            assertEquals(1L, row.getClass().getMethod("id").invoke(row));
            assertEquals("", row.getClass().getMethod("name").invoke(row));
            row = s.step();
            assertEquals(2L, row.getClass().getMethod("id").invoke(row));
            assertEquals("name-2", row.getClass().getMethod("name").invoke(row));
            assertNull(s.step());
        }

        // Indirectly examine the secondary index.
        //System.out.println(table2.scannerPlan(null, "name == ? || name == ?"));
        try (Scanner s = table2.newScanner(null, "name == ? || name == ?", "", "name-2")) {
            var row = s.row();
            assertEquals(1L, row.getClass().getMethod("id").invoke(row));
            assertEquals("", row.getClass().getMethod("name").invoke(row));
            row = s.step();
            assertEquals(2L, row.getClass().getMethod("id").invoke(row));
            assertEquals("name-2", row.getClass().getMethod("name").invoke(row));
            assertNull(s.step());
        }

        // Deleting rows against the original table should still delete the secondary index
        // entries.
        for (long id = 1; id <= 2; id++) {
            var row = table1.newRow();
            setters1[0].invoke(row, id);
            table1.delete(null, row);
        }

        assertTrue(secondary.isEmpty());

        try (Scanner s = secondary.newScanner(null)) {
            assertNull(s.row());
        }

        db.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void inconsistentIndexes2() throws Exception {
        // Similar to the inconsistentIndexes test, except it stores a row using the original
        // definition early. This allows a schema version to be created for it to use.

        var config = new DatabaseConfig();
        Database db = Database.open(config);

        final String typeName = newRowTypeName();

        final Object[] spec1 = {
            long.class, "+id",
            int.class, "num"
        };
        ClassMaker cm1 = newRowTypeMaker(typeName, spec1);
        Class t1 = cm1.finish();
        var accessors1 = access(spec1, t1);
        var getters1 = accessors1[0];
        var setters1 = accessors1[1];
        var table1 = db.openIndex("test").asTable(t1);

        // Store a row using original table definition before changing it.
        {
            var row = table1.newRow();
            setters1[0].invoke(row, 1); // id
            setters1[1].invoke(row, 1); // num
            table1.store(null, row);
        }

        // Add a "name" column and a secondary index on it.
        final Object[] spec2 = {
            long.class, "+id",
            String.class, "name",
            int.class, "num"
        };
        ClassMaker cm2 = newRowTypeMaker(typeName, spec2);
        addSecondaryIndex(cm2, "+name");
        Class t2 = cm2.finish();
        var accessors2 = access(spec2, t2);
        var getters2 = accessors2[0];
        var setters2 = accessors2[1];
        var table2 = db.openIndex("test").asTable(t2);

        // Wait for the secondary index to be ready.
        Table secondary;
        while (true) {
            try {
                secondary = ((StoredTable) table2).viewSecondaryIndex("name").viewUnjoined();
                break;
            } catch (IllegalStateException e) {
                assertTrue(e.getMessage().contains("not found"));
                Thread.yield();
            }
        }

        // Store a row using the original table definition, which should insert an index entry.
        {
            var row = table1.newRow();
            setters1[0].invoke(row, 2); // id
            setters1[1].invoke(row, 2); // num
            table1.store(null, row);
        }

        // Store using new table definition.
        {
            var row = table2.newRow();
            setters2[0].invoke(row, 3); // id
            setters2[1].invoke(row, "name-3"); // name
            setters2[2].invoke(row, 3); // num
            table2.store(null, row);
        }

        // Directly examine the secondary index.
        try (Scanner s = secondary.newScanner(null)) {
            var row = s.row();
            assertEquals(1L, row.getClass().getMethod("id").invoke(row));
            assertEquals("", row.getClass().getMethod("name").invoke(row));
            row = s.step();
            assertEquals(2L, row.getClass().getMethod("id").invoke(row));
            assertEquals("", row.getClass().getMethod("name").invoke(row));
            row = s.step();
            assertEquals(3L, row.getClass().getMethod("id").invoke(row));
            assertEquals("name-3", row.getClass().getMethod("name").invoke(row));
            assertNull(s.step());
        }

        // Indirectly examine the secondary index.
        //System.out.println(table2.scannerPlan(null, "name == ? || name == ?"));
        try (Scanner s = table2.newScanner(null, "name == ? || name == ?", "", "name-3")) {
            var row = s.row();
            assertEquals(1L, row.getClass().getMethod("id").invoke(row));
            assertEquals("", row.getClass().getMethod("name").invoke(row));
            row = s.step();
            assertEquals(2L, row.getClass().getMethod("id").invoke(row));
            assertEquals("", row.getClass().getMethod("name").invoke(row));
            row = s.step();
            assertEquals(3L, row.getClass().getMethod("id").invoke(row));
            assertEquals("name-3", row.getClass().getMethod("name").invoke(row));
            assertNull(s.step());
        }

        // Deleting rows against the original table should still delete the secondary index
        // entries.
        for (long id = 1; id <= 3; id++) {
            var row = table1.newRow();
            setters1[0].invoke(row, id);
            table1.delete(null, row);
        }

        assertTrue(secondary.isEmpty());

        try (Scanner s = secondary.newScanner(null)) {
            assertNull(s.row());
        }

        db.close();
    }

    @Test
    public void loadOneViaSecondary() throws Exception {
        // Test joining to the primary index when only one row is loaded.

        Database db = Database.open(new DatabaseConfig());
        Table<TestRow> table = db.openTable(TestRow.class);

        TestRow row = table.newRow();
        row.id(1);
        row.path("path-1");
        row.name("name-1");
        row.num(BigDecimal.ONE);
        table.insert(null, row);

        try (var scanner = table.newScanner(null, "path == ?", "path-1")) {
            row = scanner.row();
            assertEquals(1, row.id());
        }

        db.close();
    }

    @Test
    public void doubleCheck() throws Exception {
        // Test that READ_UNCOMMITTED transaction double checks the primary row because it
        // might have been concurrently modified.

        Database db = Database.open(new DatabaseConfig());
        Index ix = db.openIndex("test");
        Table<TestRow> table = ix.asTable(TestRow.class);

        TestRow row = table.newRow();
        row.id(1);
        row.path("path-1");
        row.name("name-1");
        row.num(BigDecimal.ONE);
        table.insert(null, row);

        // Update the row without updating the secondary indexes to simulate concurrent
        // modification.
        try (Cursor c = ix.newCursor(null)) {
            c.first();
            byte[] value = c.value();
            // Expected to update to "path-2".
            value[value.length - 1]++;
            c.store(value);
        }

        Transaction txn = db.newTransaction();
        try {
            txn.lockMode(LockMode.READ_UNCOMMITTED);

            try (var scanner = table.newScanner(txn, "path == ?", "path-1")) {
                // Filtered out.
                assertNull(scanner.row());
            }
        } finally {
            txn.reset();
        }

        db.close();
    }
}
