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

package org.cojen.tupl.remote;

import java.net.ServerSocket;

import java.io.*;

import java.util.*;

import java.util.concurrent.TimeUnit;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.dirmi.DisposedException;

import org.cojen.tupl.*;

import org.cojen.tupl.diag.*;

import org.cojen.tupl.io.Utils;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RemoteTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RemoteTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
        mServerDb = newTempDatabase(getClass());

        var ss = new ServerSocket(0);
        mServerDb.newServer().acceptAll(ss, 123456);

        mClientDb = Database.connect(ss.getLocalSocketAddress(), null, 111, 123456);
    }

    @After
    public void teardown() throws Exception {
        if (mClientDb != null) {
            mClientDb.close();
            mClientDb = null;
        }

        mServerDb = null;

        deleteTempDatabases(getClass());
    }

    private Database mServerDb;
    private Database mClientDb;

    @Test
    public void misc() throws Exception {
        assertFalse(mClientDb.isClosed());
        assertTrue(mClientDb.isLeader());
        assertFalse(mClientDb.failover());
        assertEquals(-1, mClientDb.capacityLimit());

        DatabaseStats stats = mClientDb.stats();
        assertEquals(4096, stats.pageSize);
        assertEquals(0, stats.openIndexes);
        var ix = mClientDb.openIndex("x");
        stats = mClientDb.stats();
        assertEquals(1, stats.openIndexes);

        ix.drop();
        assertTrue(ix.isClosed());
        try {
            ix.drop();
            fail();
        } catch (ClosedIndexException e) {
        }

        try {
            mClientDb.renameIndex(ix, "xx");
            fail();
        } catch (ClosedIndexException e) {
        }

        ix = mClientDb.openIndex("a");
        mClientDb.renameIndex(ix, "b");
        assertEquals("b", ix.nameString());
        var ix2 = mClientDb.findIndex("b");
        assertEquals("b", ix.nameString());
        assertEquals(ix.id(), ix2.id());
        assertEquals(ix2, mClientDb.findIndex("b"));

        assertFalse(ix.isUnmodifiable());
        assertTrue(ix.isModifyAtomic());
        assertEquals(Ordering.ASCENDING, ix.ordering());
        assertEquals(Ordering.DESCENDING, ix.viewReverse().ordering());

        mClientDb.flush();
        mClientDb.sync();

        assertTrue(mClientDb.preallocate(4096 * 1000) > 1_000_000);
        assertTrue(mClientDb.stats().totalPages >= 1000);

        try {
            mClientDb.newServer();
            fail();
        } catch (UnsupportedOperationException e) {
        }

        var bout = new ByteArrayOutputStream();
        mClientDb.createCachePrimer(bout);
        byte[] bytes = bout.toByteArray();
        assertTrue(bytes.length > 0);
        mClientDb.applyCachePrimer(new ByteArrayInputStream(bytes));

        assertTrue(mClientDb.indexRegistryByName().isUnmodifiable());
        assertTrue(mClientDb.indexRegistryById().isUnmodifiable());

        var temp = mClientDb.newTemporaryIndex();
        assertNull(temp.name());

        assertTrue(temp.verify(null, 1));
        assertTrue(mClientDb.verify(null, 1));

        assertTrue(temp.isEmpty());
        byte[] key = "hello".getBytes();
        byte[] value = "world".getBytes();
        temp.store(Transaction.BOGUS, key, value);
        assertFalse(temp.isEmpty());
        assertEquals(1, temp.count(null, null));
        assertEquals(1, temp.count(key, true, key, true));
        assertEquals(0, temp.count(key, false, key, false));

        var vo = new VerificationObserver() {
            boolean fail;
            boolean pass;

            @Override
            public boolean indexBegin(Index index, int height) {
                if (index != null && index.id() != temp.id()) {
                    fail = true;
                }
                return true;
            }

            public boolean indexComplete(Index index, boolean passed, String message) {
                if ((index != null && index.id() != temp.id()) || !passed) {
                    fail = true;
                }
                return true;
            }

            @Override
            public boolean indexNodePassed(long id, int level,
                                           int entryCount, int freeBytes, int largeValueCount)
            {
                pass = true;
                return true;
            }
        };

        assertTrue(temp.verify(vo, 0));
        assertFalse(vo.fail);
        assertTrue(vo.pass);
        vo.pass = false;

        ix.drop();

        assertTrue(mClientDb.verify(vo, 0));
        assertFalse(vo.fail);
        assertTrue(vo.pass);

        var co = new CompactionObserver() {
            int begin;
            int complete;
            int visited;

            @Override
            public boolean indexBegin(Index index) {
                begin++;
                return true;
            }

            public boolean indexComplete(Index index) {
                complete++;
                return true;
            }

            @Override
            public boolean indexNodeVisited(long id) {
                visited++;
                return true;
            }
        };

        assertTrue(mClientDb.compactFile(co, 0.5));

        assertTrue(co.begin > 0);
        assertTrue(co.complete > 0);
        assertTrue(co.visited > 0);

        assertTrue(mClientDb.compactFile(null, 0.5));
    }

    @Test
    public void deadlock() throws Exception {
        Index ix = mClientDb.openIndex("test");
        assertFalse(ix.isClosed());

        byte[] key1 = "k1".getBytes();
        byte[] key2 = "k2".getBytes();

        Transaction txn1 = ix.newTransaction(null);
        Transaction txn2 = ix.newTransaction(null);

        txn2.lockTimeout(60, TimeUnit.SECONDS);
        assertEquals(1, txn2.lockTimeout(TimeUnit.MINUTES));
        txn2.check();

        ix.load(txn1, key1);

        var task2 = startTestTaskAndWaitUntilBlockedSocket(() -> {
            try {
                ix.load(txn2, key2);
                ix.load(txn2, key1);
            } catch (IOException e) {
                throw Utils.rethrow(e);
            }
        });

        try {
            ix.load(txn1, key2);
            fail();
        } catch (DeadlockException e) {
            assertTrue(e.isGuilty());

            Set<DeadlockInfo> set = e.deadlockSet();
            assertEquals(2, set.size());

            byte[] lastKey = null;
            for (var info : set) {
                assertEquals(ix.id(), info.indexId());
                assertEquals(ix.nameString(), info.indexNameString());
                byte[] key = info.key();
                if (lastKey != null) {
                    assertFalse(Arrays.equals(lastKey, key));
                }
                assertTrue(Arrays.equals(key1, key) || Arrays.equals(key2, key));
                lastKey = key;
            }
        }

        ix.close();
        assertTrue(ix.isClosed());
    }

    @Test
    public void snapshot() throws Exception {
        Index ix = mClientDb.openIndex("test");
        byte[] key = "hello".getBytes();
        byte[] value = "world".getBytes();
        ix.store(null, key, value);
        mClientDb.checkpoint();

        byte[] bytes;

        try (Snapshot snap = mClientDb.beginSnapshot()) {
            assertTrue(snap.length() > 8192);
            assertTrue(snap.position() > 100);
            assertTrue(snap.isCompressible());

            var bout = new ByteArrayOutputStream();
            snap.writeTo(bout);
            snap.close();

            bytes = bout.toByteArray();

            assertEquals(snap.length(), bytes.length);
        }

        var config = new DatabaseConfig().baseFile(newTempBaseFile(getClass()));
        var copy = Database.restoreFromSnapshot(config, new ByteArrayInputStream(bytes));

        Index ix2 = copy.indexById(ix.id());
        assertEquals(ix.nameString(), ix2.nameString());
        assertArrayEquals(value, ix2.load(null, key));

        copy.close();
    }

    @Test
    public void deleteIndex() throws Exception {
        Index clientIx = mClientDb.openIndex("test");
        Index serverIx = mServerDb.findIndex("test");
        long id = clientIx.id();
        assertEquals(id, serverIx.id());

        byte[] key = "hello".getBytes();
        byte[] value = "world".getBytes();
        clientIx.store(null, key, value);

        Runnable task = mClientDb.deleteIndex(clientIx);

        assertNull(mServerDb.findIndex("test"));
        assertNull(mServerDb.indexById(id));
        try {
            assertNull(serverIx.load(null, key));
            fail();
        } catch (DeletedIndexException e) {
        }
        try {
            serverIx.store(null, key, value);
            fail();
        } catch (DeletedIndexException e) {
        }

        assertNull(mClientDb.findIndex("test"));
        assertNull(mClientDb.indexById(id));
        try {
            assertNull(clientIx.load(null, key));
            fail();
        } catch (DeletedIndexException e) {
        }
        try {
            clientIx.store(null, key, value);
            fail();
        } catch (DeletedIndexException e) {
        }

        task.run();

        try {
            mClientDb.deleteIndex(clientIx);
            fail();
        } catch (ClosedIndexException e) {
        }

        try {
            clientIx.load(null, key);
            fail();
        } catch (DisposedException e) {
            // For the time being, client indexes behave differently than server indexes when
            // closed. It became fully closed when the task was run.
        }
    }
}
