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

import java.util.concurrent.TimeUnit;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.dirmi.Connector;
import org.cojen.dirmi.Environment;
import org.cojen.dirmi.Serializer;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.Database;
import org.cojen.tupl.DatabaseConfig;
import org.cojen.tupl.DeadlockException;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.Index;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.LockTimeoutException;
import org.cojen.tupl.Ordering;
import org.cojen.tupl.PrimaryKey;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.core.CoreDeadlockInfo;
import org.cojen.tupl.core.DetachedDeadlockInfo;

import org.cojen.tupl.diag.DatabaseStats;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RemoteTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RemoteTest.class.getName());
    }

    @Test
    public void basic() throws Exception {
        var db = Database.open(new DatabaseConfig());
        var server = ServerDatabase.from(db);

        Environment env = Environment.create();

        env.customSerializers
            (Serializer.simple(DatabaseStats.class),
             Serializer.simple(TimeUnit.class),
             Serializer.simple(DurabilityMode.class),
             Serializer.simple(LockMode.class),
             Serializer.simple(LockResult.class),
             Serializer.simple(Ordering.class),
             LockTimeoutExceptionSerializer.THE,
             DeadlockInfoSerializer.THE,
             DeadlockExceptionSerializer.THE);

        env.export("main", server);
        env.connector(Connector.local(env));
        var remote = env.connect(RemoteDatabase.class, "main", null).root();
        var client = ClientDatabase.from(remote);

        System.out.println(client.isClosed());
        System.out.println(client.stats());

        Index ix = client.openIndex("test");
        System.out.println(ix);
        System.out.println(ix.isEmpty());

        ix.store(null, "hello".getBytes(), "world".getBytes());

        byte[] result = ix.load(null, "hello".getBytes());
        System.out.println(new String(result));

        /*
        Transaction txn = client.newTransaction();
        ix.delete(txn, "hello".getBytes());
        System.out.println(ix.exists(txn, "hello".getBytes()));

        Transaction txn0 = client.newTransaction();
        System.out.println(ix.exists(txn0, "world".getBytes()));

        new Thread(() -> {
            try {
                Thread.sleep(500);
                System.out.println(ix.exists(txn0, "hello".getBytes()));
            } catch (Exception e) {
                e.printStackTrace();
                txn0.reset(e);
            }
        }).start();

        System.out.println(ix.exists(txn, "world".getBytes()));

        */

        try (Cursor c = ix.newCursor(null)) {
            System.out.println(c);
            byte[] key;
            System.out.println(c.first());
            for (; (key = c.key()) != null; c.next()) {
                byte[] value = c.value();
                String valueStr = value == null ? "null" : new String(value);
                System.out.println(new String(key) + " -> " + valueStr);
            }
        }

        System.out.println("---");

        Table<Tab> clientTable = client.openTable(Tab.class);

        {
            Table<Tab> serverTable = db.openTable(Tab.class);
            Tab row = serverTable.newRow();
            row.id(1);
            row.value("hello");
            serverTable.insert(null, row);
            row.id(2);
            row.value("world");
            serverTable.insert(null, row);
        }

        try (var scanner = clientTable.newScanner(null)) {
            scanner.forEachRemaining(row -> System.out.println(row));
        }
    }

    @PrimaryKey("id")
    public static interface Tab {
        long id();
        void id(long id);

        String value();
        void value(String v);
    }
}
