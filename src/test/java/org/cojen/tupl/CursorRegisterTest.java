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

package org.cojen.tupl;

import java.net.ServerSocket;

import java.util.Map;
import java.util.TreeMap;

import org.cojen.tupl.repl.ReplicatorConfig;

import org.junit.*;
import static org.junit.Assert.*;

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
    public void reopenRecover() throws Exception {
        DatabaseConfig config = new DatabaseConfig()
            .directPageAccess(false)
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

        txn.commit();

        db = reopenTempDatabase(getClass(), db, config);
        ix = db.openIndex("test");

        fastAssertArrayEquals("world".getBytes(), ix.load(null, "hello".getBytes()));
        fastAssertArrayEquals("world!".getBytes(), ix.load(null, "hello!".getBytes()));

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
        DatabaseConfig config = new DatabaseConfig()
            .directPageAccess(false)
            .checkpointRate(-1, null)
            .durabilityMode(DurabilityMode.NO_FLUSH);

        Database db = newTempDatabase(getClass(), config);
        Index[] indexes = {db.openIndex("test-0"), db.openIndex("test-1")};

        Transaction[] txns = {db.newTransaction(), db.newTransaction(), db.newTransaction()};

        Cursor[] cursors = new Cursor[100];
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

            indexes[i].newScanner(null).scanAll((k, v) -> {
                map.put(new String(k), new String(v));
            });
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
    public void replication() throws Exception {
        ServerSocket leaderSocket = new ServerSocket(0);

        ReplicatorConfig replConfig = new ReplicatorConfig()
            .groupToken(283947)
            .localSocket(leaderSocket);

        DatabaseConfig config = new DatabaseConfig()
            .directPageAccess(false)
            .durabilityMode(DurabilityMode.NO_FLUSH)
            .replicate(replConfig);

        Database leaderDb = newTempDatabase(getClass(), config);

        {
            Index ix = null;
            for (int i=10; --i>=0; ) {
                try {
                    ix = leaderDb.openIndex("test");
                    break;
                } catch (UnmodifiableReplicaException e) {
                    if (i == 0) {
                        throw e;
                    }
                }
                Thread.sleep(100);
            }

            Transaction txn = leaderDb.newTransaction();
            Cursor c = ix.newCursor(txn);
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

        ServerSocket replicaSocket = new ServerSocket(0);
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
    }
}
