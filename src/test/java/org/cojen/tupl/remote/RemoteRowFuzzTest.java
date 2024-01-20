/*
 *  Copyright (C) 2023 Cojen.org
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

import java.io.IOException;

import java.net.ServerSocket;

import java.util.Map;

import java.util.concurrent.ConcurrentHashMap;

import org.cojen.dirmi.ClassResolver;

import org.cojen.tupl.*;

import org.cojen.tupl.table.FuzzTest;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RemoteRowFuzzTest extends FuzzTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RemoteRowFuzzTest.class.getName());
    }

    @Override
    protected Database createTempDb() throws Exception {
        var stuff = new ServerStuff(Database.open(new DatabaseConfig()));

        var ss = new ServerSocket(0);
        stuff.acceptAll(ss, 123456);

        Database clientDb = Database.connect(ss.getLocalSocketAddress(), null, 123456);

        mClientToServerMap.put(clientDb, stuff);

        return clientDb;
    }

    @Override
    protected void closeTempDb(Database clientDb) throws Exception {
        clientDb.close();
        mClientToServerMap.remove(clientDb).close();
    }

    @Override
    protected void installRowType(Database clientDb, Class<?> rowType) {
        mClientToServerMap.get(clientDb).installRowType(rowType);
    }

    private static class ServerStuff implements ClassResolver {
        private final Database mServerDb;
        private final Server mServer;

        private final Map<String, Class<?>> mRowTypeMap = new ConcurrentHashMap<>();

        ServerStuff(Database serverDb) throws IOException {
            mServerDb = serverDb;
            mServer = serverDb.newServer();
            mServer.classResolver(this);
        }

        @Override
        public Class<?> resolveClass(String name) {
            return mRowTypeMap.get(name);
        }

        void acceptAll(ServerSocket ss, int port) throws IOException {
            mServer.acceptAll(ss, port);
        }

        void installRowType(Class<?> rowType) {
            mRowTypeMap.put(rowType.getName(), rowType);
        }

        void close() throws IOException {
            mServerDb.close();
        }
    }

    private final Map<Database, ServerStuff> mClientToServerMap = new ConcurrentHashMap<>();
}
