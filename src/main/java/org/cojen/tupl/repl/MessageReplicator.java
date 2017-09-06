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

import java.io.IOException;

import java.util.Map;

import java.util.function.Consumer;

/**
 * Message-oriented replication interface, which is a bit easier to use than {@link
 * StreamReplicator}. Although it has slightly higher overhead, control message passing is
 * handled automatically. Replicas are still required to be reading in order for control
 * messages to be processed, however.
 *
 * @author Brian S O'Neill
 * @see StreamReplicator
 */
public interface MessageReplicator extends Replicator {
    /**
     * Open a replicator instance, creating it if necessary.
     *
     * @throws IllegalArgumentException if misconfigured
     */
    public static MessageReplicator open(ReplicatorConfig config) throws IOException {
        // FIXME
        throw null;
    }

    /**
     * Start accepting replication data, to be called for new or existing replicators.
     *
     * @return false if already started
     */
    boolean start() throws IOException;

    /**
     * Start by receiving a {@link #requestSnapshot snapshot} from another group member,
     * expected to be called only by newly joined members.
     *
     * @param options requested options; can pass null if none
     * @return null if no snapshot could be found and replicator hasn't started
     * @throws ConnectException if a snapshot was found, but requesting it failed
     * @throws IllegalStateException if already started
     */
    SnapshotReceiver restore(Map<String, String> options) throws IOException;

    /**
     * Returns a new reader which accesses data starting from the given index. The reader
     * returns EOF whenever the end of a term is reached. At the end of a term, try to obtain a
     * new writer to determine if the local member has become the leader.
     *
     * <p>When passing true for the follow parameter, a reader is always provided at the
     * requested index. When passing false for the follow parameter, null is returned if the
     * current member is the leader for the given index.
     *
     * <p><b>Note: Reader instances are not expected to be thread-safe.</b>
     *
     * @param index index to start reading from, known to have been committed
     * @param follow pass true to obtain an active reader, even if local member is the leader
     * @return reader or possibly null when follow is false
     * @throws IllegalStateException if index is lower than the start index
     */
    Reader newReader(long index, boolean follow);

    /**
     * Returns a new writer for the leader to write into, or else returns null if the local
     * member isn't the leader. The writer stops accepting messages when the term has ended,
     * and possibly another leader has been elected.
     *
     * <p><b>Note: Writer instances are not expected to be thread-safe.</b>
     *
     * @return writer or null if not the leader
     * @throws IllegalStateException if an existing writer for the current term already exists
     */
    Writer newWriter();

    /**
     * Returns a new writer for the leader to write into, or else returns null if the local
     * member isn't the leader. The writer stops accepting messages when the term has ended,
     * and possibly another leader has been elected.
     *
     * <p><b>Note: Writer instances are not expected to be thread-safe.</b>
     *
     * @param index expected index to start writing from as leader; method returns null if
     * index doesn't match
     * @return writer or null if not the leader
     * @throws IllegalArgumentException if given index is negative
     * @throws IllegalStateException if an existing writer for the current term already exists
     */
    Writer newWriter(long index);

    /**
     * Connect to a remote replication group member, for receiving a database snapshot. An
     * {@link #snapshotRequestAcceptor acceptor} must be installed on the group member being
     * connected to for the request to succeed.
     * 
     * <p>The sender is selected as the one which has the fewest count of active snapshot
     * sessions. If all the counts are the same, then a sender is instead randomly selected,
     * favoring a follower over a leader.
     *
     * @param options requested options; can pass null if none
     * @return null if no snapshot could be found
     * @throws ConnectException if a snapshot was found, but requesting it failed
     */
    SnapshotReceiver requestSnapshot(Map<String, String> options) throws IOException;

    /**
     * Install a callback to be invoked when a snapshot is requested by a new group member.
     *
     * @param acceptor acceptor to use, or pass null to disable
     */
    void snapshotRequestAcceptor(Consumer<SnapshotSender> acceptor);

    public static interface Reader extends Replicator.Reader {
        /**
         * Blocks until a log message is available, never reading past a commit index or term.
         *
         * @return complete message or null if the term end has been reached
         * @throws IllegalStateException if log was deleted (index is too low)
         */
        byte[] readMessage() throws IOException;

        /**
         * Blocks until a message is available, and then fully or partially copies it into the
         * given buffer. Messages are partially copied only when the given buffer length is too
         * small. When a message has been fully copied, 0 is returned.
         *
         * @return the amount of bytes remaining in the message, or EOF (-1) if the term end
         * has been reached
         * @throws IllegalStateException if log was deleted (index is too low)
         */
        int readMessage(byte[] buf, int offset, int length) throws IOException;
    }

    public static interface Writer extends Replicator.Writer {
        /**
         * Write a single message to the log.
         *
         * @return false only if the writer is deactivated
         */
        boolean writeMessage(byte[] message) throws IOException;

        /**
         * Write a single message to the log.
         *
         * @return false only if the writer is deactivated
         */
        boolean writeMessage(byte[] message, int offset, int length) throws IOException;

        /**
         * Write a single message to the log, as part of an atomic batch.
         *
         * @param finished pass true for the last message in the batch
         * @return false only if the writer is deactivated
         */
        boolean writeMessage(byte[] message, int offset, int length, boolean finished)
            throws IOException;
    }
}
