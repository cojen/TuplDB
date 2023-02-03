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

import org.cojen.dirmi.ClassResolver;

/**
 * Controls remote access into a database.
 *
 * @author Brian S O'Neill
 * @see Database#newServer
 */
public interface Server extends Closeable {
    /**
     * Call to enable remote access via all sockets accepted by the given {@code ServerSocket}.
     * At least one token must match in order for a connection to be accepted.
     *
     * @throws IllegalArgumentException if not given one or two tokens
     */
    void acceptAll(ServerSocket ss, long... tokens) throws IOException;

    /**
     * Optionally supply an object which helps find classes. This feature is primarily intended
     * for finding row interface definitions which are dynamically generated. The resolver must
     * be set before accepting connections in order for it to actually be used. Changing it
     * doesn't affect existing connections.
     *
     * @hidden
     */
    void classResolver(ClassResolver resolver);

    /**
     * Disables remote access, closes all acceptors, and closes all existing connections.
     */
    @Override
    void close();
}
