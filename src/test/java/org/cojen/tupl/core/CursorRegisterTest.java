/*
 *  Copyright (C) 2018 Cojen.org
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

package org.cojen.tupl.core;

import java.io.OutputStream;

import java.net.ServerSocket;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import java.util.concurrent.TimeUnit;

import org.cojen.tupl.repl.ReplicatorConfig;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class CursorRegisterTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(CursorRegisterTest.class.getName());
    }

    @After
    public void teardown() throws Exception {
        closeTempDatabases(getClass());
        deleteTempFiles(getClass());
    }

    @Test
    public void tempIndex() throws Exception {
        var config = new DatabaseConfig()
            .durabilityMode(DurabilityMode.NO_FLUSH);

        Database db = newTempDatabase(getClass(), config);
        Index ix = db.newTemporaryIndex();

        Cursor c = ix.newCursor(null);
        assertFalse(c.register());
        c.close();

        Transaction txn = db.newTransaction();
        c = ix.newCursor(txn);
        assertFalse(c.register());
        c.close();
        txn.reset();
    }

    @Test
    public void noRedo() throws Exception {
        // Register doesn't work if transaction doesn't redo.

        Database db = Database.open(new DatabaseConfig());
        Index ix = db.openIndex("test");

        Transaction txn = db.newTransaction();
        Cursor c = ix.newCursor(txn);
        assertFalse(c.register());

        c = ix.newCursor(null);
        assertFalse(c.register());

        db.close();
    }

    @Test
    public void reopenRecover() throws Exception {
        var config = new DatabaseConfig()
            .durabilityMode(DurabilityMode.NO_FLUSH);

        Database db = newTempDatabase(getClass(), config);
        Index ix = db.openIndex("test");

        Transaction txn = db.newTransaction();
        Cursor c = ix.newCursor(txn);
        assertTrue(c.register());
        c.findNearby("hello".getBytes());
        c.store("world".getBytes());

        db.checkpoint();

        c.findNearby("hello!".getBytes());
        c.store("world!".getBytes());

        c.findNearby("delete-me".getBytes());
        c.store("junk".getBytes());
        c.store(null); // delete

        txn.commit();

        db = reopenTempDatabase(getClass(), db, config);
        ix = db.openIndex("test");

        fastAssertArrayEquals("world".getBytes(), ix.load(null, "hello".getBytes()));
        fastAssertArrayEquals("world!".getBytes(), ix.load(null, "hello!".getBytes()));
        assertNull(ix.load(null, "delete-me".getBytes()));

        // Register should still work, and without an explicit transaction.
        c = ix.newCursor(null);
        assertTrue(c.register());
        c.findNearby("apple".getBytes());
        c.store("jacks".getBytes());
        db.checkpoint();
        c.findNearby("banana".getBytes());
        c.store("split".getBytes());

        db = reopenTempDatabase(getClass(), db, config);
        ix = db.openIndex("test");

        assertEquals(4, ix.count(null, null));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void longRunningCursors() throws Exception {
        var config = new DatabaseConfig()
            .checkpointRate(-1, null)
            .durabilityMode(DurabilityMode.NO_FLUSH);

        Database db = newTempDatabase(getClass(), config);
        Index[] indexes = {db.openIndex("test-0"), db.openIndex("test-1")};

        Transaction[] txns = {db.newTransaction(), db.newTransaction(), db.newTransaction()};

        var cursors = new Cursor[100];
        for (int i=0; i<cursors.length; i++) {
            Cursor c = indexes[i % indexes.length].newCursor(txns[i % txns.length]);
            c.register();
            cursors[i] = c;
        }

        for (int i=0; i<1000; i++) {
            if (i % 100 == 1) {
                db.checkpoint();
            }
            for (int j=0; j<cursors.length; j++) {
                Cursor c = cursors[j];
                c.findNearby(("key-" + i + "-" + j).getBytes());
                c.store(("value-" + i + "-" + j).getBytes());
            }
            if (i == 500) {
                // Unregister a cursor, just for fun.
                cursors[0].unregister();
            }
        }

        for (Transaction txn : txns) {
            txn.commit();
        }

        Map<String, String>[] expect = new Map[indexes.length];

        for (int i=0; i<indexes.length; i++) {
            Map<String, String> map = new TreeMap<>();
            expect[i] = map;

            try (Cursor c = indexes[i].newCursor(null)) {
                for (c.first(); c.key() != null; c.next()) {
                    map.put(new String(c.key()), new String(c.value()));
                }
            }
        }

        db = reopenTempDatabase(getClass(), db, config);

        indexes = new Index[indexes.length];

        for (int i=0; i<indexes.length; i++) {
            indexes[i] = db.openIndex("test-" + i);
        }

        for (int i=0; i<indexes.length; i++) {
            Index ix = indexes[i];
            Map<String, String> map = expect[i];
            assertEquals(map.size(), ix.count(null, null));
            for (Map.Entry<String, String> e : map.entrySet()) {
                assertEquals(e.getValue(), new String(ix.load(null, e.getKey().getBytes())));
            }
        }
    }

    @Test
    public void valueAccessor() throws Exception {
        // Verify that cursor key is registered, to recover cursors which write to a stream.

        var config = new DatabaseConfig()
            .checkpointRate(-1, null)
            .durabilityMode(DurabilityMode.NO_FLUSH);

        Database db = newTempDatabase(getClass(), config);
        Index ix = db.openIndex("test");

        final long seed = 937854;
        var rnd = new Random(seed);

        Cursor c = ix.newCursor(null);
        final byte[] key = "hello".getBytes();
        c.find(key);

        OutputStream out = c.newValueOutputStream(0);

        for (int i=0; i<100_000; i++) {
            out.write((byte) rnd.nextInt());
        }

        db.checkpoint();
        
        for (int i=0; i<100_000; i++) {
            out.write((byte) rnd.nextInt());
        }

        out.flush();

        db = reopenTempDatabase(getClass(), db, config);

        ix = db.openIndex("test");

        byte[] value = ix.load(null, key);
        assertEquals(200_000, value.length);

        rnd = new Random(seed);
        for (int i=0; i<value.length; i++) {
            assertEquals(value[i], (byte) rnd.nextInt());
        }
    }

    @Test
    public void nestedStoreCommit() throws Exception {
        // Test nested transaction commits against a registered cursor.

        var config = new DatabaseConfig()
            .checkpointRate(-1, null)
            .durabilityMode(DurabilityMode.NO_FLUSH);

        Database db = newTempDatabase(getClass(), config);
        Index ix = db.openIndex("test");

        // Insert one inside the scope.
        Transaction txn = db.newTransaction();
        txn.enter();
        Cursor c = ix.newCursor(txn);
        c.find("k1".getBytes());
        c.register();
        c.commit("v1".getBytes());
        c.close();
        txn.commitAll();

        db = reopenTempDatabase(getClass(), db, config);
        ix = db.openIndex("test");
        fastAssertArrayEquals("v1".getBytes(), ix.load(null, "k1".getBytes()));

        // Delete one inside the scope.
        txn = db.newTransaction();
        txn.enter();
        c = ix.newCursor(txn);
        c.find("k1".getBytes());
        c.register();
        c.commit(null);
        c.close();
        txn.commitAll();

        db = reopenTempDatabase(getClass(), db, config);
        ix = db.openIndex("test");
        assertNull(ix.load(null, "k1".getBytes()));

        // Insert two inside the scope, only commit on the second.
        txn = db.newTransaction();
        txn.enter();
        c = ix.newCursor(txn);
        c.find("k2".getBytes());
        c.register();
        c.store("v2".getBytes());
        c.findNearby("k3".getBytes());
        c.commit("v3".getBytes());
        c.close();
        txn.commitAll();

        db = reopenTempDatabase(getClass(), db, config);
        ix = db.openIndex("test");
        fastAssertArrayEquals("v2".getBytes(), ix.load(null, "k2".getBytes()));
        fastAssertArrayEquals("v3".getBytes(), ix.load(null, "k3".getBytes()));

        // Delete two inside the scope, only commit on the second.
        txn = db.newTransaction();
        txn.enter();
        c = ix.newCursor(txn);
        c.find("k2".getBytes());
        c.register();
        c.store(null);
        c.findNearby("k3".getBytes());
        c.commit(null);
        c.close();
        txn.commitAll();

        db = reopenTempDatabase(getClass(), db, config);
        ix = db.openIndex("test");
        assertNull(ix.load(null, "k2".getBytes()));
        assertNull(ix.load(null, "k3".getBytes()));
    }

    @Test
    public void replication() throws Exception {
        ServerSocket leaderSocket = newServerSocket();

        var replConfig = new ReplicatorConfig()
            .groupToken(283947)
            .localSocket(leaderSocket);

        var config = new DatabaseConfig()
            .durabilityMode(DurabilityMode.NO_FLUSH)
            .lockTimeout(5, TimeUnit.SECONDS)
            .replicate(replConfig);

        Database leaderDb = newTempDatabase(getClass(), config);
        Index leaderIx = null;

        {
            for (int i=10; --i>=0; ) {
                try {
                    leaderIx = leaderDb.openIndex("test");
                    break;
                } catch (UnmodifiableReplicaException e) {
                    if (i == 0) {
                        throw e;
                    }
                }
                Thread.sleep(100);
            }

            Transaction txn = leaderDb.newTransaction();
            Cursor c = leaderIx.newCursor(txn);
            assertTrue(c.register());
            c.findNearby("hello".getBytes());
            c.store("world".getBytes());

            // Force replica to restore from here.
            leaderDb.checkpoint();
            leaderDb.suspendCheckpoints();

            c.findNearby("hello!".getBytes());
            c.store("world!".getBytes());

            txn.commit();
        }

        ServerSocket replicaSocket = newServerSocket();
        replConfig.localSocket(replicaSocket);
        replConfig.addSeed(leaderSocket.getLocalSocketAddress());

        config.replicate(replConfig);

        Database replicaDb = newTempDatabase(getClass(), config);

        leaderDb.resumeCheckpoints();

        Index ix = replicaDb.openIndex("test");

        wait: {
            for (int i=0; i<50; i++) {
                if (ix.count(null, null) > 1) {
                    break wait;
                }
                Thread.sleep(100);
            }
            fail("Replica isn't caught up");
        }

        assertEquals(2, ix.count(null, null));
        fastAssertArrayEquals("world".getBytes(), ix.load(null, "hello".getBytes()));
        fastAssertArrayEquals("world!".getBytes(), ix.load(null, "hello!".getBytes()));

        // Test cursor reopen after replica has closed the index.

        Transaction txn = leaderDb.newTransaction();
        Cursor c = leaderIx.newCursor(txn);
        assertTrue(c.register());
        byte[] key = "key".getBytes();
        c.findNearby(key);
        byte[] value = "value".getBytes();
        c.store(value);
        txn.flush();

        wait: {
            for (int i=0; i<50; i++) {
                try {
                    ix.exists(null, key);
                } catch (LockTimeoutException e) {
                    // Cannot acquire lock because transaction isn't committed yet.
                    break wait;
                }
                Thread.sleep(100);
            }
            fail("Replica isn't caught up(2)");
        }

        // Replica can freely close the index.
        ix.close();

        byte[] value2 = "value2".getBytes();
        c.store(value2);
        txn.commit();

        ix = replicaDb.openIndex("test");
        
        wait: {
            for (int i=0; i<50; i++) {
                byte[] found = ix.load(null, key);
                if (Arrays.equals(found, value2)) {
                    break wait;
                }
                Thread.sleep(100);
            }
            fail("Replica isn't caught up(3)");
        }
    }
}
