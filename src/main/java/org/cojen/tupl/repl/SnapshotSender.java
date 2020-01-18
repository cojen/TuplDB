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
import java.io.OutputStream;

import java.net.SocketAddress;

import java.util.Map;

/**
 * Sender-side object for controlling the transmission of a database snapshot to a new group
 * member. A {@code SnapshotSender} is paired with a {@link SnapshotReceiver} on the remote
 * member which is joining the group.
 *
 * @author Brian S O'Neill
 * @see Replicator#snapshotRequestAcceptor Replicator.snapshotRequestAcceptor
 * @see SnapshotReceiver
 */
public interface SnapshotSender extends Closeable {
    /**
     * Member address which is receiving the snapshot.
     */
    SocketAddress receiverAddress();

    /**
     * Options requested by the receiver.
     *
     * @return non-null map, possibly empty
     */
    Map<String, String> options();

    /**
     * Begin writing the snapshot to the receiver.
     *
     * @param length expected length of snapshot (in bytes) or -1 if unknown
     * @param position log reading begins at this position; expected to be the highest exclusive
     * position applied by the snapshot
     * @param options granted to the receiver; can pass null if none
     * @return a stream to write the snapshot into; close the stream or this sender when done
     */
    OutputStream begin(long length, long position, Map<String, String> options) throws IOException;
}
