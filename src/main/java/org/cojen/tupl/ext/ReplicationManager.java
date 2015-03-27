/*
 *  Copyright 2012-2013 Brian S O'Neill
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.cojen.tupl.ext;

import java.io.Closeable;
import java.io.IOException;

import org.cojen.tupl.DatabaseConfig;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.EventListener;
import org.cojen.tupl.Index;
import org.cojen.tupl.LockMode;

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
     * invokes this method, and so it blocks until recovery completes.
     *
     * @param listener optional listener for posting recovery events to
     */
    void recover(EventListener listener) throws IOException;

    /**
     * Returns the next position a replica will read from, which must be confirmed. Position is
     * never negative and never retreats.
     */
    long readPosition();

    /**
     * Blocks at most once, reading as much replication input as possible. Returns -1 if local
     * instance has become the leader.
     *
     * @return amount read, or -1 if leader
     * @throws IllegalStateException if not started
     */
    int read(byte[] b, int off, int len) throws IOException;

    /**
     * Called to acknowledge mode change from replica to leader, or vice versa. Until flip is
     * called, all read and write operations fail as if the leadership mode is indeterminate.
     * Reads fail as if the local instance is the leader, and writes fail as if the local
     * instance is a replica.
     */
    void flip();

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
         * Fully writes the given data, returning a confirmation position. When the local
         * instance loses leadership, all data rolls back to the highest confirmed position.
         *
         * @return confirmation position, or -1 if not leader
         */
        long write(byte[] b, int off, int len) throws IOException;

        /**
         * Blocks until all data up to the given log position is confirmed.
         *
         * @return false if not leader
         * @throws ConfirmationFailureException
         */
        boolean confirm(long position) throws IOException;

        /**
         * Blocks until all data up to the given log position is confirmed.
         *
         * @param timeoutNanos pass -1 for infinite
         * @return false if not leader
         * @throws ConfirmationFailureException
         */
        boolean confirm(long position, long timeoutNanos) throws IOException;
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
    void syncConfirm(long position) throws IOException;

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
     * Notification to replica when an entry is stored into an index. All notifications are
     * {@link LockMode#READ_UNCOMMITTED uncommitted}, and so loading with an appropriate lock
     * mode is required for confirmation. The current thread is free to perform any blocking
     * operations &mdash; it will not suspend replication processing unless {@link
     * DatabaseConfig#maxReplicaThreads all} replication threads are consumed.
     *
     * @param index non-null index reference
     * @param key non-null key; contents must not be modified
     * @param value null if entry is deleted; contents can be modified
     */
    void notifyStore(Index index, byte[] key, byte[] value);

    /**
     * Notification to replica after an index is renamed. The current thread is free to perform
     * any blocking operations &mdash; it will not suspend replication processing unless {@link
     * DatabaseConfig#maxReplicaThreads all} replication threads are consumed.
     *
     * @param index non-null index reference
     * @param oldName non-null old index name
     * @param newName non-null new index name
     */
    void notifyRename(Index index, byte[] oldName, byte[] newName);

    /**
     * Notification to replica after an index is dropped. The current thread is free to perform
     * any blocking operations &mdash; it will not suspend replication processing unless {@link
     * DatabaseConfig#maxReplicaThreads all} replication threads are consumed.
     *
     * @param index non-null closed and dropped index reference
     */
    void notifyDrop(Index index);

    /**
     * Forward a change from a replica to the leader. Change must arrive back through the input
     * stream. This method can be invoked concurrently by multiple threads.
     *
     * @return false if local instance is not a replica or if no leader has
     * been established
     */
    //boolean forward(byte[] b, int off, int len) throws IOException;
}
