/*
 *  Copyright 2020 Cojen.org
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

package org.cojen.tupl;

/**
 * Defines a callback which is notified when a pending transaction has finished. To be
 * effective, the callback must be {@linkplain Transaction#attach(CommitCallback) attached}
 * prior to committing the transaction. If an exception is thrown by the initial commit
 * invocation, the callback isn't invoked.
 *
 * <p>A pending transaction is one which is replicated, and it uses the {@link
 * DurabilityMode#NO_SYNC NO_SYNC} or {@link DurabilityMode#NO_FLUSH NO_FLUSH} durability mode.
 * A replicated transaction which uses the {@link DurabilityMode#SYNC SYNC} durability mode
 * blocks the committing thread until replication is confirmed, and the callback isn't invoked.
 *
 * <p>A unique callback instance can be allocated per transaction, or an instance can be shared
 * for multiple transactions. When the callback is shared, thread safety must be taken into
 * consideration.
 *
 * @author Brian S O'Neill
 *
 * @see Transaction#attach(CommitCallback)
 */
@FunctionalInterface
public interface CommitCallback {
    /**
     * Called when a pending transaction has committed or rolled back. When committed, the
     * status is null. When rolled back, the status is an object which can be converted to a
     * string to get a meaningful message.
     *
     * <p>The callback is invoked from a limited set of threads, and so the implementation
     * should avoid performing any blocking operations. Otherwise, pending transactions can
     * pile up in an unfinished state.
     *
     * @param txnId non-zero transaction id
     * @param status null if committed, or else non-null if rolled back
     */
    void finished(long txnId, Object status);

    /**
     * Called when a transaction is moving to the pending state. By default, this method does
     * nothing, but it can be overridden to verify that pending transactions are being created.
     */
    default void pending(long txnId) { }
}
