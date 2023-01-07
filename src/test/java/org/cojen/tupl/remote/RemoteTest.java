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

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.Database;
import org.cojen.tupl.DatabaseConfig;
import org.cojen.tupl.Entry;
import org.cojen.tupl.Index;
import org.cojen.tupl.PrimaryKey;
import org.cojen.tupl.SecondaryIndex;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;

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

        var ss = new ServerSocket(0);
        db.newServer().acceptAll(ss, 123456);

        var client = Database.connect(ss.getLocalSocketAddress(), 111, 123456);

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

        Table<Entry> etab = ix.asTable(Entry.class);

        try (var scanner = etab.newScanner(null)) {
            scanner.forEachRemaining(row -> System.out.println(row));
        }

        try (var scanner = etab.newScanner(null, "{value} key != ?", new byte[0])) {
            scanner.forEachRemaining(row -> System.out.println(row));
        }

        System.out.println("---");

        Table<Tab> clientTable = client.openTable(Tab.class);

        System.out.println(clientTable.newRow());
        System.out.println(clientTable.cloneRow(clientTable.newRow()));
        clientTable.unsetRow(clientTable.newRow());
        clientTable.copyRow(clientTable.newRow(), clientTable.newRow());
        System.out.println("---");

        {
            Table<Tab> serverTable = db.openTable(Tab.class);
            Tab row = serverTable.newRow();
            row.id(1);
            row.value("hello");
            row.name("name-" + row.value());
            serverTable.insert(null, row);
            row.id(2);
            row.value("world");
            row.name("name-" + row.value());
            serverTable.insert(null, row);
            row.id(3);
            row.value("end");
            row.name("name-" + row.value());
            serverTable.insert(null, row);
        }

        try (var scanner = clientTable.newScanner(null)) {
            scanner.forEachRemaining(row -> System.out.println(row));
        }

        System.out.println("---");

        try (var scanner = clientTable.newScanner(null, "value == ?", "world")) {
            scanner.forEachRemaining(row -> System.out.println(row));
        }

        System.out.println("---");

        try (var scanner = clientTable.newScanner(null, "{value} value != ?", "world")) {
            scanner.forEachRemaining(row -> System.out.println(row));
        }

        System.out.println("---");

        try (var scanner = clientTable.newScanner(null, "name == ?", "name-world")) {
            scanner.forEachRemaining(row -> System.out.println(row));
        }

        System.out.println("---");

        try (var scanner = clientTable.newScanner(null, "{id}")) {
            scanner.forEachRemaining(row -> System.out.println(row));
        }

        System.out.println("---");

        try (var scanner = clientTable.newScanner(null, "{value} name == ?", "name-world")) {
            scanner.forEachRemaining(row -> System.out.println(row));
        }

        System.out.println("---");

        System.out.println(clientTable.scannerPlan(null, "{+value}"));

        try (var scanner = clientTable.newScanner(null, "{+value}")) {
            scanner.forEachRemaining(row -> System.out.println(row));
        }

        client.close();
        db.close();
    }

    @PrimaryKey("id")
    @SecondaryIndex("name")
    public static interface Tab {
        long id();
        void id(long id);

        String value();
        void value(String v);

        String name();
        void name(String n);
    }
}
