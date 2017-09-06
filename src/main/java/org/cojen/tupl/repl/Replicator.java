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
import java.io.InterruptedIOException;
import java.io.IOException;

import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketAddress;

import java.util.function.Consumer;
import java.util.function.LongConsumer;

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
     * @throws ConnectException if not given a member address or of the connect fails
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

    /**
     * Common interface for accessing replication data, for a given term.
     */
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

    /**
     * Common interface for reading from a replicator, for a given term.
     */
    public static interface Reader extends Accessor {
    }

    /**
     * Common interface for writing to a replicator, for a given term.
     */
    public static interface Writer extends Accessor {
        /**
         * Blocks until the commit index reaches the given index.
         *
         * @param nanosTimeout relative nanosecond time to wait; infinite if &lt;0
         * @return current commit index, or -1 if deactivated before the index could be
         * reached, or -2 if timed out
         */
        long waitForCommit(long index, long nanosTimeout) throws InterruptedIOException;

        /**
         * Blocks until the commit index reaches the end of the term.
         *
         * @param nanosTimeout relative nanosecond time to wait; infinite if &lt;0
         * @return current commit index, or -1 if closed before the index could be
         * reached, or -2 if timed out
         */
        default long waitForEndCommit(long nanosTimeout) throws InterruptedIOException {
            long endNanos = nanosTimeout > 0 ? (System.nanoTime() + nanosTimeout) : 0;

            long endIndex = termEndIndex();

            while (true) {
                long index = waitForCommit(endIndex, nanosTimeout);
                if (index == -2) {
                    // Timed out.
                    return -2;
                }
                endIndex = termEndIndex();
                if (endIndex == Long.MAX_VALUE) {
                    // Assume closed.
                    return -1;
                }
                if (index == endIndex) {
                    // End reached.
                    return index;
                }
                // Term ended even lower, so try again.
                if (nanosTimeout > 0) {
                    nanosTimeout = Math.max(0, endNanos - System.nanoTime());
                }
            }
        }

        /**
         * Invokes the given task when the commit index reaches the requested index. The
         * current commit index is passed to the task, or -1 if the term ended before the index
         * could be reached. If the task can be run when this method is called, then the
         * current thread invokes it immediately.
         */
        void uponCommit(long index, LongConsumer task);

        /**
         * Invokes the given task when the commit index reaches the end of the term. The
         * current commit index is passed to the task, or -1 if if closed. If the task can be
         * run when this method is called, then the current thread invokes it immediately.
         */
        default void uponEndCommit(LongConsumer task) {
            uponCommit(termEndIndex(), index -> {
                long endIndex = termEndIndex();
                if (endIndex == Long.MAX_VALUE) {
                    // Assume closed.
                    task.accept(-1);
                } else if (index == endIndex) {
                    // End reached.
                    task.accept(index);
                } else {
                    // Term ended even lower, so try again.                        
                    uponEndCommit(task);
                }
            });
        }
    }
}
