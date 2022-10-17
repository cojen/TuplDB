/*
 *  Copyright (C) 2017-2022 Cojen.org
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

package org.cojen.tupl.core;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class UpdaterTest extends ScannerTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(UpdaterTest.class.getName());
    }

    @Override
    protected Scanner<Entry> newScanner(Index ix, Transaction txn) throws Exception {
        return newUpdater(ix, txn);
    }

    protected Updater<Entry> newUpdater(Index ix, Transaction txn) throws Exception {
        return ix.asTable(Entry.class).newUpdater(txn);
    }

    @Test
    public void emptyNull() throws Exception {
        empty(null);
    }

    @Test
    public void emptyBogus() throws Exception {
        empty(Transaction.BOGUS);
    }

    @Test
    public void emptyReadCommitted() throws Exception {
        Transaction txn = mDb.newTransaction();
        txn.lockMode(LockMode.READ_COMMITTED);
        empty(txn);
        txn.reset();
    }

    @Test
    public void emptyUpgradableRead() throws Exception {
        Transaction txn = mDb.newTransaction();
        txn.lockMode(LockMode.UPGRADABLE_READ);
        empty(txn);
        txn.reset();
    }

    private void empty(Transaction txn) throws Exception {
        Index ix = mDb.openIndex("test");

        Updater<Entry> u = newUpdater(ix, txn);
        assertNull(u.row());
        assertNull(u.step());
        u.close();
        assertNull(u.row());
        assertNull(u.step());

        u = newUpdater(ix, txn);
        var count = new AtomicInteger();
        u.forEachRemaining(e -> count.getAndIncrement());
        assertEquals(0, count.get());
        assertNull(u.row());

        u = newUpdater(ix, txn);
        try {
            u.update();
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("No current row"));
        }
        assertNull(u.row());
    }

    @Test
    public void deleteOneNull() throws Exception {
        deleteOne(null);
    }

    @Test
    public void deleteOneBogus() throws Exception {
        deleteOne(Transaction.BOGUS);
    }

    @Test
    public void deleteOneReadCommitted() throws Exception {
        Transaction txn = mDb.newTransaction();
        txn.lockMode(LockMode.READ_COMMITTED);
        deleteOne(txn);
        txn.commit();
    }

    @Test
    public void deleteOneUpgradableRead() throws Exception {
        Transaction txn = mDb.newTransaction();
        txn.lockMode(LockMode.UPGRADABLE_READ);
        deleteOne(txn);
        txn.commit();
    }

    public void deleteOne(Transaction txn) throws Exception {
        Index ix = mDb.openIndex("test");
        byte[] key = "hello".getBytes();
        byte[] value = "world".getBytes();
        ix.store(null, key, value);

        Updater<Entry> u = newUpdater(ix, txn);
        assertNull(u.delete());
        assertNull(u.row());
        assertEquals(0, ix.count(null, null));

        if (txn != null && txn != Transaction.BOGUS) {
            txn.commit();
        }

        assertEquals(0, ix.count(null, null));
    }

    @Test
    public void updateOneNull() throws Exception {
        updateOne(null);
    }

    @Test
    public void updateOneBogus() throws Exception {
        updateOne(Transaction.BOGUS);
    }

    @Test
    public void updateOneReadCommitted() throws Exception {
        Transaction txn = mDb.newTransaction();
        txn.lockMode(LockMode.READ_COMMITTED);
        updateOne(txn);
        txn.commit();
    }

    @Test
    public void updateOneUpgradableRead() throws Exception {
        Transaction txn = mDb.newTransaction();
        txn.lockMode(LockMode.UPGRADABLE_READ);
        updateOne(txn);
        txn.commit();
    }

    public void updateOne(Transaction txn) throws Exception {
        Index ix = mDb.openIndex("test");
        byte[] key = "hello".getBytes();
        byte[] value = "world".getBytes();
        ix.store(null, key, value);

        Updater<Entry> u = newUpdater(ix, txn);
        Entry e = u.row();
        byte[] value2 = "world!".getBytes();
        e.value(value2);
        assertNull(u.update());
        assertNull(u.row());
        fastAssertArrayEquals(value2, ix.load(txn, key));
        assertEquals(1, ix.count(null, null));

        u = newUpdater(ix, txn);
        e = u.row();
        byte[] value3 = "world!!!".getBytes();
        e.value(value3);
        assertNull(u.update());
        e.value(value2);
        try {
            assertNull(u.update());
            fail();
        } catch (IllegalStateException ex) {
            assertTrue(ex.getMessage().contains("No current row"));
        }
        assertNull(u.row());
        fastAssertArrayEquals(value3, ix.load(txn, key));
        assertEquals(1, ix.count(null, null));

        if (txn != null && txn != Transaction.BOGUS) {
            txn.commit();
        }

        fastAssertArrayEquals(value3, ix.load(null, key));
        assertEquals(1, ix.count(null, null));
    }

    @Test
    public void updateManyNull() throws Exception {
        updateMany(null);
    }

    @Test
    public void updateManyBogus() throws Exception {
        updateMany(Transaction.BOGUS);
    }

    @Test
    public void updateManyReadCommitted() throws Exception {
        Transaction txn = mDb.newTransaction();
        txn.lockMode(LockMode.READ_COMMITTED);
        updateMany(txn);
        txn.commit();
    }

    @Test
    public void updateManyUpgradableRead() throws Exception {
        Transaction txn = mDb.newTransaction();
        txn.lockMode(LockMode.UPGRADABLE_READ);
        updateMany(txn);
        txn.commit();
    }

    public void updateMany(Transaction txn) throws Exception {
        Index ix = mDb.openIndex("test");
        for (int i=0; i<10; i++) {
            ix.store(null, ("key-" + i).getBytes(), ("value-" + i).getBytes());
        }

        try (Updater<Entry> u = newUpdater(ix, txn)) {
            for (Entry e = u.row(); e != null; e = u.update(e)) {
                e.value((new String(e.value()) + "-v2").getBytes());
            }
        }
        
        for (int i=0; i<10; i++) {
            byte[] key = ("key-" + i).getBytes();
            byte[] value = ("value-" + i + "-v2").getBytes();
            fastAssertArrayEquals(value, ix.load(txn, key));
        }

        if (txn != null && !txn.lockMode().noReadLock) {
            Transaction txn2 = mDb.newTransaction();
            for (int i=0; i<10; i++) {
                byte[] key = ("key-" + i).getBytes();
                byte[] value = ("value-" + i + "-v2").getBytes();
                // Still locked by the original transaction.
                assertEquals(LockResult.TIMED_OUT_LOCK, txn2.tryLockShared(ix.id(), key, 0));
            }
            txn2.exit();
            txn.commit();
        }

        for (int i=0; i<10; i++) {
            byte[] key = ("key-" + i).getBytes();
            byte[] value = ("value-" + i + "-v2").getBytes();
            fastAssertArrayEquals(value, ix.load(null, key));
        }
    }

    @Test
    public void updateSome() throws Exception {
        Index ix = mDb.openIndex("test");
        for (int i=0; i<10; i++) {
            ix.store(null, ("" + i).getBytes(), ("value-" + i).getBytes());
        }

        try (Updater<Entry> u = newUpdater(ix, null)) {
            for (Entry e = u.row(); e != null; ) {
                int i = e.key()[0] - '0';
                if ((i & 1) == 1) {
                    e = u.step(e);
                } else {
                    e.value(("updated-" + i).getBytes());
                    e = u.update(e);
                }
            }
        }

        for (int i=0; i<10; i++) {
            byte[] value = ix.load(null, ("" + i).getBytes());
            var str = new String(value);
            String expect = (((i & 1) == 1) ? "value-" : "updated-") + i;
            assertEquals(expect, str);
        }
    }
}
