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
import java.io.File;
import java.io.InterruptedIOException;
import java.io.IOException;

import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketAddress;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import java.util.function.Consumer;
import java.util.function.LongConsumer;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public interface StreamReplicator extends Closeable {
    /**
     * Open a replicator instance, creating it if necessary.
     *
     * @throws IllegalArgumentException if misconfigured
     */
    public static StreamReplicator open(ReplicatorConfig config) throws IOException {
        if (config == null) {
            throw new IllegalArgumentException("No configuration");
        }

        File base = config.mBaseFile;
        if (base == null) {
            throw new IllegalArgumentException("No base file configured");
        }

        long groupId = config.mGroupId;
        if (groupId == 0) {
            throw new IllegalArgumentException("No group id configured");
        }

        SocketAddress localAddress = config.mLocalAddress;
        if (localAddress == null) {
            throw new IllegalArgumentException("No local address configured");
        }

        if (config.mSeeds != null && !config.mSeeds.isEmpty()) {
            throw new IllegalArgumentException("Seeding isn't supported yet");
        }

        Map<Long, SocketAddress> members = config.mStaticMembers;

        long localMemberId = 0;

        if (members != null && !members.isEmpty()) {
            // Check for duplicate addresses.
            Set<SocketAddress> addresses = new HashSet<>();
            addresses.add(localAddress);
            for (Map.Entry<Long, SocketAddress> e : members.entrySet()) {
                SocketAddress addr = e.getValue();
                if (addr == null) {
                    throw new IllegalArgumentException("Null address");
                }
                if (addr.equals(localAddress)) {
                    localMemberId = e.getKey();
                } else if (!addresses.add(addr)) {
                    throw new IllegalArgumentException("Duplicate address: " + addr);
                }
            }
        }

        if (localMemberId == 0) {
            throw new IllegalArgumentException("No local member id provided");
        }

        if (config.mMkdirs) {
            base.getParentFile().mkdirs();
        }

        Controller con = new Controller(new FileStateLog(base), groupId);
        con.start(members, localMemberId);

        return con;
    }

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
     * Durably persist all data up to the highest index. The highest term, the highest index,
     * and the commit index are all recovered when reopening the replicator. Incomplete data
     * beyond this is discarded.
     */
    void sync() throws IOException;

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
     * @return previous acceptor or null if none
     */
    Consumer<Socket> socketAcceptor(Consumer<Socket> acceptor);

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
     * @throws ConnectException if no senders could be connected to
     */
    SnapshotReceiver requestSnapshot(Map<String, String> options) throws IOException;

    /**
     * Install a callback to be invoked when a snapshot is requested by a new group member.
     *
     * @param acceptor acceptor to use, or pass null to disable
     * @return previous acceptor or null if none
     */
    Consumer<SnapshotSender> snapshotRequestAcceptor(Consumer<SnapshotSender> acceptor);

    public static interface Accessor extends Closeable {
        /**
         * Returns the fixed term being accessed.
         */
        long term();

        /**
         * Returns the fixed index at the start of the term.
         */
        long termStartIndex();

        /**
         * Returns the current term end index, which is Long.MAX_VALUE if unbounded. The end
         * index is always permitted to retreat, but never lower than the commit index.
         */
        long termEndIndex();

        /**
         * Returns the next log index which will be accessed.
         */
        long index();

        @Override
        void close();
    }

    public static interface Reader extends Accessor {
        /**
         * Blocks until log messages are available, never reading past a commit index or term.
         *
         * @return amount of bytes read, or EOF (-1) if the term end has been reached
         * @throws IllegalStateException if log was deleted (index is too low)
         */
        default int read(byte[] buf) throws IOException {
            return read(buf, 0, buf.length);
        }

        /**
         * Blocks until log messages are available, never reading past a commit index or term.
         *
         * @return amount of bytes read, or EOF (-1) if the term end has been reached
         * @throws IllegalStateException if log was deleted (index is too low)
         */
        int read(byte[] buf, int offset, int length) throws IOException;
    }

    public static interface Writer extends Accessor {
        /**
         * Write complete messages to the log.
         *
         * @return amount of bytes written, which is less than the message length only if the
         * writer is deactivated
         */
        default int write(byte[] messages) throws IOException {
            return write(messages, 0, messages.length);
        }

        /**
         * Write complete messages to the log.
         *
         * @return amount of bytes written, which is less than the given length only if the
         * writer is deactivated
         */
        default int write(byte[] messages, int offset, int length) throws IOException {
            return write(messages, offset, length, index() + length);
        }

        /**
         * Write complete or partial messages to the log.
         *
         * @param highestIndex highest index (exclusive) which can become the commit index
         * @return amount of bytes written, which is less than the given length only if the
         * writer is deactivated
         */
        int write(byte[] messages, int offset, int length, long highestIndex) throws IOException;

        /**
         * Blocks until the commit index reaches the given index.
         *
         * @param nanosTimeout relative nanosecond time to wait; infinite if &lt;0
         * @return current commit index, or -1 if deactivated before the index could be
         * reached, or -2 if timed out
         */
        long waitForCommit(long index, long nanosTimeout) throws InterruptedIOException;

        /**
         * Invokes the given task when the commit index reaches the requested index. The
         * current commit index is passed to the task, or -1 if the term ended before the index
         * could be reached. If the task can be run when this method is called, then the
         * current thread invokes it immediately.
         */
        void uponCommit(long index, LongConsumer task);

        /**
         * Returns true if writes into the leader are deactived, after having become a
         * follower. The term doesn't end until the new leader writes something.
         */
        boolean isDeactivated();
    }
}
