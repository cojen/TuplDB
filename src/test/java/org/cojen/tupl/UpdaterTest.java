/*
 *  Copyright (C) 2017 Cojen.org
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
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.*;
import static org.junit.Assert.*;

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
    protected Scanner newScanner(View view, Transaction txn) throws Exception {
        return newUpdater(view, txn);
    }

    protected Updater newUpdater(View view, Transaction txn) throws Exception {
        return view.newUpdater(txn);
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

        Updater u = newUpdater(ix, txn);
        assertNull(u.key());
        assertNull(u.value());
        assertFalse(u.step());
        u.close();
        assertNull(u.key());
        assertNull(u.value());
        assertFalse(u.step());

        u = newUpdater(ix, txn);
        assertFalse(u.step(0));
        assertFalse(u.step(1));
        try {
            u.step(-1);
            fail();
        } catch (IllegalArgumentException e) {
        }

        u = newUpdater(ix, txn);
        AtomicInteger count = new AtomicInteger();
        u.scanAll((k, v) -> count.getAndIncrement());
        assertEquals(0, count.get());
        assertNull(u.key());
        assertNull(u.value());

        u = newUpdater(ix, txn);
        u.updateAll((k, v) -> {
            count.getAndIncrement();
            return null;
        });
        assertEquals(0, count.get());
        assertNull(u.key());
        assertNull(u.value());
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

        Updater u = newUpdater(ix, txn);
        u.updateAll((k, v) -> null);
        assertNull(u.key());
        assertNull(u.value());
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

        Updater u = newUpdater(ix, txn);
        byte[] value2 = "world!".getBytes();
        u.updateAll((k, v) -> value2);
        assertNull(u.key());
        assertNull(u.value());
        fastAssertArrayEquals(value2, ix.load(txn, key));
        assertEquals(1, ix.count(null, null));

        u = newUpdater(ix, txn);
        byte[] value3 = "world!!!".getBytes();
        assertFalse(u.update(value3));
        assertFalse(u.update(value2));
        assertNull(u.key());
        assertNull(u.value());
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

        Updater u = newUpdater(ix, txn);
        u.updateAll((k, v) -> {
            return (new String(v) + "-v2").getBytes();
        });
        assertNull(u.key());
        assertNull(u.value());

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
                // Still locked by original transaction.
                assertEquals(LockResult.TIMED_OUT_LOCK, txn2.tryLockShared(ix.getId(), key, 0));
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
}
