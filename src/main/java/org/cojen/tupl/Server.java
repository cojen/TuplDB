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

package org.cojen.tupl;

import java.io.Closeable;
import java.io.IOException;

import java.net.ServerSocket;

/**
 * Controls remote access into a database.
 *
 * @author Brian S O'Neill
 * @see Database#newServer
 */
public interface Server extends Closeable {
    /**
     * Optionally specify authentication tokens. At least one token must match in order for a
     * connection to be accepted. Each token is converted to a binary representation, possibly
     * by calling {@code toString} first. Array tokens are supported by recursively converting
     * all elements.
     *
     * @return this server instance
     */
    Server tokens(Object... tokens);

    /**
     * Call to enable remote access via the replication system. This eliminates the requirement
     * to open another socket port, and it doesn't introduce any overhead.
     *
     * @throws IllegalStateException if replication isn't enabled
     * @see DatabaseConfig#replicate
     */
    void acceptReplicator() throws IOException;

    /**
     * Call to enable remote access via all sockets accepted by the given {@code ServerSocket}.
     */
    void acceptAll(ServerSocket ss) throws IOException;

    /**
     * Disables remote access, closes all acceptors, and closes all existing connections.
     */
    @Override
    void close();
}
