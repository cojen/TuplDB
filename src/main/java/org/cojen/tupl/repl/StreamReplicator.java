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
import java.io.IOException;

import java.net.SocketAddress;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
     * <p><b>Note: Reader instances are not expected to be thread-safe.</b>
     *
     * @param index index to start reading from, known to have been committed
     * @return reader or null if timed out
     * @throws IllegalStateException if index is lower than the start index
     */
    Reader newReader(long index) throws IOException;

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
    Writer newWriter() throws IOException;

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
    Writer newWriter(long index) throws IOException;

    public static interface Reader extends Closeable {
        /**
         * Returns the fixed term this reader is accessing.
         */
        long term();

        /**
         * Returns the next log index which can be read from.
         */
        long index();

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

        @Override
        void close();
    }

    public static interface Writer extends Closeable {
        /**
         * Returns the fixed term being written to.
         */
        long term();

        /**
         * Returns the next log index which will be written to.
         */
        long index();

        /**
         * Write complete messages to the log.
         *
         * @return amount of bytes written, which is less than the message length only if the term
         * end has been reached
         */
        default int write(byte[] messages) throws IOException {
            return write(messages, 0, messages.length);
        }

        /**
         * Write complete messages to the log.
         *
         * @return amount of bytes written, which is less than the given length only if the
         * term end has been reached
         */
        default int write(byte[] messages, int offset, int length) throws IOException {
            return write(messages, offset, length, index() + length);
        }

        /**
         * Write complete or partial messages to the log.
         *
         * @param highestIndex highest index (exclusive) which can become the commit index
         * @return amount of bytes written, which is less than the given length only if the
         * term end has been reached
         */
        int write(byte[] messages, int offset, int length, long highestIndex) throws IOException;

        /**
         * Blocks until the commit index reaches the given index.
         *
         * @param nanosTimeout relative nanosecond time to wait; infinite if &lt;0
         * @return current commit index, or -1 if term finished before the index could be
         * reached, or -2 if timed out, or MIN_VALUE if closed
         */
        long waitForCommit(long index, long nanosTimeout) throws IOException;

        @Override
        void close();
    }
}
