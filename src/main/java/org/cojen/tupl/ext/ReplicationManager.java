/*
 *  Copyright (C) 2011-2017 Cojen.org
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

package org.cojen.tupl.ext;

import java.io.Closeable;
import java.io.InputStream;
import java.io.IOException;

import org.cojen.tupl.ConfirmationFailureException;
import org.cojen.tupl.Database;
import org.cojen.tupl.DatabaseConfig;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.EventListener;
import org.cojen.tupl.Index;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.UnmodifiableReplicaException;

/**
 * Interface which replaces the redo log, for replicating transactional operations.
 * Non-transactional operations, or those with {@link DurabilityMode#NO_REDO NO_REDO}
 * durability will not pass through the replication manager.
 *
 * @author Brian S O'Neill
 * @see DatabaseConfig#replicate
 */
public interface ReplicationManager extends Closeable {
    /**
     * Return a fixed non-zero value which identifies the replication manager implementation
     * and its encoding format. Value should be chosen randomly, so as not to collide with
     * other implementations.
     */
    long encoding();

    /**
     * Called when the database is created, in an attempt to retrieve an existing database
     * snapshot from a replication peer. If null is returned, the database will try to start
     * reading replication data at the lowest position.
     *
     * @return null if no snapshot could be found
     * @throws IOException if a snapshot was found, but requesting it failed
     */
    default InputStream restoreRequest() throws IOException {
        return null;
    }

    /**
     * Start the replication manager in replica mode. Invocation of this method implies that
     * all data lower than the given position is confirmed. All data at or higher than the
     * given position might be discarded.
     *
     * <p>After started, the reported {@link #readPosition position} must match the one
     * provided to this method. The position can change only after read and write operations
     * have been performed.
     *
     * @param position position to start reading from; 0 is the lowest position
     * @throws IllegalArgumentException if position is negative
     * @throws IllegalStateException if already started
     */
    void start(long position) throws IOException;

    /**
     * Called after replication threads have started, providing an opportunity to wait until
     * replication has sufficiently "caught up". The thread which is opening the database
     * invokes this method, and so it blocks until recovery completes. Default implementation
     * does nothing.
     *
     * @param accessor provides access to the database
     */
    default void ready(Accessor accessor) throws IOException {}

    static interface Accessor extends EventListener {
        /**
         * Access the database which is being replicated.
         */
        Database database();

        /**
         * Attempt to write a replicated control message, and then calls {@link
         * ReplicationManager#control control} when confirmed. Control messages might get
         * replayed, and so they must be idempotent.
         *
         * @return confirmed log position just after the message
         * @throws UnmodifiableReplicaException if not the leader
         */
        long control(byte[] message) throws IOException;
    }

    /**
     * Returns the next position a replica will read from, which must be confirmed. Position is
     * never negative and never retreats.
     */
    long readPosition();

    /**
     * Blocks at most once, reading as much replication input as possible. Returns -1 if local
     * instance has become the leader, and a writer instance now exists.
     *
     * @return amount read, or -1 if leader
     * @throws IllegalStateException if not started
     */
    int read(byte[] b, int off, int len) throws IOException;

    /**
     * Returns an object which allows the leader to write changes. A new instance is required
     * after a leader transition. Returned object can be null if local instance is a replica.
     */
    Writer writer() throws IOException;

    static interface Writer {
        /**
         * Returns the next position a leader will write to. Valid only if local instance is
         * the leader.
         */
        long position();

        /**
         * Invokes the given callback upon a leadership change. Callback should be invoked at
         * most once, but extra invocations are ignored.
         *
         * @return false if not leader
         */
        boolean leaderNotify(Runnable callback);

        /**
         * Fully writes the given data, returning a potential confirmation position. When the
         * local instance loses leadership, all data rolls back to the highest confirmed
         * position.
         *
         * <p>An optional commit parameter defines the highest log position which immediately
         * follows a transaction commit operation. If leadership is lost, the message stream is
         * guaranteed to be truncated at a position no higher than the highest commit position
         * ever provided. The given commit position is ignored if it's higher than what has
         * actually been written.
         *
         * @param b message buffer
         * @param off message buffer offset
         * @param len message length
         * @param commitPos highest transaction commit position; pass 0 if nothing changed
         * @return false if not leader
         * @throws IllegalArgumentException if commitPos is negative
         */
        boolean write(byte[] b, int off, int len, long commitPos) throws IOException;

        /**
         * Blocks until all data up to the given log position is confirmed.
         *
         * @param commitPos commit position which was passed to the write method
         * @return false if not leader at the given position
         * @throws ConfirmationFailureException
         */
        default boolean confirm(long commitPos) throws IOException {
            return confirm(commitPos, -1);
        }

        /**
         * Blocks until all data up to the given log position is confirmed.
         *
         * @param commitPos commit position which was passed to the write method
         * @param timeoutNanos pass -1 for infinite
         * @return false if not leader at the given position
         * @throws ConfirmationFailureException
         */
        boolean confirm(long commitPos, long timeoutNanos) throws IOException;

        /**
         * Blocks until the leadership end is confirmed. This method must be called before
         * switching to replica mode.
         *
         * @return the end commit position; same as next read position
         * @throws ConfirmationFailureException if end position cannot be determined
         */
        default long confirmEnd() throws ConfirmationFailureException {
            return confirmEnd(-1);
        }

        /**
         * Blocks until the leadership end is confirmed. This method must be called before
         * switching to replica mode.
         *
         * @return the end commit position; same as next read position
         * @throws ConfirmationFailureException if end position cannot be determined
         */
        long confirmEnd(long timeoutNanos) throws ConfirmationFailureException;
    }

    /**
     * Durably flushes all local data to non-volatile storage, up to the current position.
     */
    void sync() throws IOException;

    /**
     * Durably flushes all local data to non-volatile storage, up to the given confirmed
     * position, and then blocks until fully confirmed.
     *
     * @throws ConfirmationFailureException
     */
    default void syncConfirm(long position) throws IOException {
        syncConfirm(position, -1);
    }

    /**
     * Durably flushes all local data to non-volatile storage, up to the given confirmed
     * position, and then blocks until fully confirmed.
     *
     * @param timeoutNanos pass -1 for infinite
     * @throws ConfirmationFailureException
     */
    void syncConfirm(long position, long timeoutNanos) throws IOException;

    /**
     * Indicates that all data prior to the given log position has been durably
     * checkpointed. The log can discard the old data. This method is never invoked
     * concurrently, and the implementation should return quickly.
     *
     * @param position log position immediately after the checkpoint position
     */
    void checkpointed(long position) throws IOException;

    /**
     * Called after a control operation has been confirmed by a leader or received by a
     * replica. All replication processing and checkpoints are suspended until this method
     * returns. Any exception thrown by this method causes the database to panic and close.
     *
     * @param position log position just after the message
     */
    default void control(long position, byte[] message) throws IOException {}

    /**
     * Notification to replica after an index is renamed. The current thread is free to perform
     * any blocking operations &mdash; it will not suspend replication processing unless {@link
     * DatabaseConfig#maxReplicaThreads all} replication threads are consumed.
     *
     * @param index non-null index reference
     * @param oldName non-null old index name
     * @param newName non-null new index name
     */
    default void notifyRename(Index index, byte[] oldName, byte[] newName) {}

    /**
     * Notification to replica after an index is dropped. The current thread is free to perform
     * any blocking operations &mdash; it will not suspend replication processing unless {@link
     * DatabaseConfig#maxReplicaThreads all} replication threads are consumed.
     *
     * @param index non-null closed and dropped index reference
     */
    default void notifyDrop(Index index) {}

    /**
     * Forward a change from a replica to the leader. Change must arrive back through the input
     * stream. This method can be invoked concurrently by multiple threads.
     *
     * @return false if local instance is not a replica or if no leader has
     * been established
     */
    //boolean forward(byte[] b, int off, int len) throws IOException;
}
