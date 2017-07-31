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
public interface Replicator extends Closeable {
    /**
     * Open a replicator instance, creating it if necessary.
     *
     * @throws IllegalArgumentException if misconfigured
     */
    public static Replicator open(ReplicatorConfig config) throws IOException {
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
     * @param nanosTimeout maximum time to wait for a term to be created at the given index;
     * pass -1 for infinite timeout
     * @return reader or null if timed out
     * @throws IllegalStateException if index is lower than the start index
     */
    Reader newReader(long index, long nanosTimeout) throws IOException;

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
}
