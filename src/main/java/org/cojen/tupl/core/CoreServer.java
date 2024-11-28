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

package org.cojen.tupl.core;

import java.io.IOException;

import java.net.ServerSocket;
import java.net.Socket;

import org.cojen.dirmi.ClassResolver;
import org.cojen.dirmi.Environment;

import org.cojen.tupl.Database;
import org.cojen.tupl.Server;

import org.cojen.tupl.remote.RemoteUtils;
import org.cojen.tupl.remote.ServerDatabase;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class CoreServer implements Server {
    private final Environment mEnv;
    private final Servers mServers;

    CoreServer(LocalDatabase db, Servers servers) throws IOException {
        mEnv = export(db);
        mServers = servers;
        servers.add(this);
    }

    private static Environment export(LocalDatabase db) throws IOException {
        ServerDatabase server = ServerDatabase.from(db);
        Environment env = RemoteUtils.createEnvironment();
        env.export(Database.class.getName(), server);
        return env;
    }

    @Override
    public void acceptAll(ServerSocket ss, long... tokens) throws IOException {
        if (tokens.length < 1 || tokens.length > 2) {
            throw new IllegalArgumentException("Must provide one or two tokens");
        }

        mEnv.acceptAll(ss, s -> {
            try {
                return RemoteUtils.testConnection(s.getInputStream(), s.getOutputStream(), tokens);
            } catch (IOException e) {
                throw Utils.rethrow(e);
            }
        });
    }

    @Override
    public void classResolver(ClassResolver resolver) {
        mEnv.classResolver(resolver);
    }

    /**
     * Called for sockets which were accepted by the replication layer.
     */
    void acceptedAndValidated(Socket s) throws IOException {
        mEnv.accepted(s);
    }

    @Override
    public void close() {
        mServers.remove(this);
        mEnv.close();
    }
}
