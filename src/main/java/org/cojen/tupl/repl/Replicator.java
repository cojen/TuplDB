/*
 *  Copyright (C) 2017 Cojen.org
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

package org.cojen.tupl.repl;

import java.io.Closeable;
import java.io.IOException;

import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketAddress;

import java.util.function.Consumer;

/**
 * Defines common features available to all types of replicators.
 *
 * @author Brian S O'Neill
 */
public interface Replicator extends Closeable {
    long getLocalMemberId();

    SocketAddress getLocalAddress();

    /**
     * Returns the effective local role, as known by the group. Changes to the role don't
     * become effective until proposed by the leader, committed, and then applied.
     */
    Role getLocalRole();

    /**
     * Connect to any replication group member, for any particular use. An {@link
     * #socketAcceptor acceptor} must be installed on the group member being connected to for
     * the connect to succeed.
     *
     * @throws IllegalArgumentException if address is null
     * @throws ConnectException if not given a member address or if the connect fails
     */
    Socket connect(SocketAddress addr) throws IOException;

    /**
     * Install a callback to be invoked when plain connections are established to the local
     * group member. No new connections are accepted (of any type) until the callback returns.
     *
     * @param acceptor acceptor to use, or pass null to disable
     */
    void socketAcceptor(Consumer<Socket> acceptor);

    /**
     * Durably persist all data up to the highest index. The highest term, the highest index,
     * and the commit index are all recovered when reopening the replicator. Incomplete data
     * beyond this is discarded.
     */
    void sync() throws IOException;
}
