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

import java.util.HashSet;
import java.util.Set;

/**
 * Maintains a collection of Server instances.
 *
 * @author Brian S O'Neill
 */
final class Servers implements Closeable {
    private Object mServers;

    private volatile CoreServer mReplServer;

    private boolean mClosed;

    CoreServer newServer(LocalDatabase db) throws IOException {
        return new CoreServer(db, this);
    }

    /**
     * Returns a server used for sockets accepted by the replication layer.
     */
    CoreServer replServer(LocalDatabase db) throws IOException {
        CoreServer server = mReplServer;
        return server != null ? server : openReplServer(db);
    }

    private synchronized CoreServer openReplServer(LocalDatabase db) throws IOException {
        CoreServer server = mReplServer;
        if (server == null) {
            mReplServer = server = new CoreServer(db, this);
        }
        return server;
    }

    @Override
    public synchronized void close() {
        mClosed = true;

        if (mServers != null) {
            if (mServers instanceof Set set) {
                for (Object server : set) {
                    ((CoreServer) server).close();
                }
            } else {
                ((CoreServer) mServers).close();
            }

            mServers = null;
        }
    }

    @SuppressWarnings("unchecked")
    synchronized void add(CoreServer server) {
        if (mClosed) {
            server.close();
        } else if (mServers == null) {
            mServers = server;
        } else if (mServers instanceof Set set) {
            set.add(server);
        } else {
            var set = new HashSet(4);
            set.add(mServers);
            set.add(server);
            mServers = set;
        }
    }

    synchronized void remove(CoreServer server) {
        if (mServers instanceof Set set) {
            if (set.remove(server) && set.isEmpty()) {
                mServers = null;
            }
        } else if (mServers == server) {
            mServers = null;
        }
    }
}
