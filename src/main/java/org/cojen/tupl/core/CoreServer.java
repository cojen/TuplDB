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

import java.io.Closeable;
import java.io.IOException;

import java.net.ServerSocket;

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
    private volatile CoreDatabase mDatabase;
    private volatile long[] mTokens;

    CoreServer(CoreDatabase db) throws IOException {
        ServerDatabase server = ServerDatabase.from(db);
        Environment env = RemoteUtils.createEnvironment();
        env.export(Database.class.getName(), server);
        mEnv = env;
        mDatabase = db;
    }

    @Override
    public Server tokens(long... tokens) {
        if (tokens.length < 1 || tokens.length > 2) {
            throw new IllegalArgumentException("Must provide one or two tokens");
        }
        mTokens = tokens.clone();
        return this;
    }

    @Override
    public Server acceptAll(ServerSocket ss) throws IOException {
        if (mTokens == null) {
            throw new IllegalStateException("No tokens");
        }

        mEnv.acceptAll(ss, s -> {
            try {
                return RemoteUtils.testConnection(s.getInputStream(), s.getOutputStream(), mTokens);
            } catch (IOException e) {
                throw Utils.rethrow(e);
            }
        });

        return this;
    }

    @Override
    public void close() {
        mTokens = null;

        CoreDatabase db = mDatabase;

        if (db != null) {
            mDatabase = null;
            db.unregister(this);
        }

        mEnv.close();
    }
}
