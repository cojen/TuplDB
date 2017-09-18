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
import java.io.InputStream;
import java.io.IOException;

import java.net.SocketAddress;

import java.util.Map;

/**
 * Receiver-side object for controlling the transmission of a database snapshot to a new group
 * member. A {@code SnapshotReceiver} is paired with a {@link SnapshotSender} on the remote
 * member which has the complete database.
 *
 * @author Brian S O'Neill
 * @see DirectReplicator#requestSnapshot DirectReplicator.requestSnapshot
 * @see SnapshotSender
 */
public interface SnapshotReceiver extends Closeable {
    /**
     * Member address which is sending the snapshot.
     */
    SocketAddress senderAddress();

    /**
     * Options granted by the sender.
     *
     * @return non-null map, possibly empty
     */
    Map<String, String> options();

    /**
     * Returns the expected length of the snapshot (in bytes) or -1 if unknown.
     */
    long length();

    /**
     * Returns the log index to start reading from. Is expected to be the highest exclusive
     * index applied by the snapshot.
     */
    long index();

    /**
     * Returns a stream to read the snapshot from. Close the stream or this receiver when done.
     */
    InputStream inputStream() throws IOException;
}
