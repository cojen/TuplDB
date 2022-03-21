/*
 *  Copyright (C) 2021 Cojen.org
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
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.core;

import java.io.IOException;

import org.cojen.tupl.LockFailureException;
import org.cojen.tupl.Transaction;

/**
 * Maintains a set of locks which match on row predicates, which can be used for implementing
 * serializable transaction isolation.
 *
 * @author Brian S O'Neill
 * @see RowPredicate
 */
public interface RowPredicateLock<R> {
    /**
     * Acquires shared access for all the predicate locks, waiting if necessary, and retains
     * the locks for the entire transaction scope. If lock acquisition times out, all locks
     * acquired up to that point are still retained.
     *
     * @param row is passed to the {@code RowPredicate.test} method
     * @return object which must be closed at most once after the associated row lock has
     * been been acquired
     * @throws IllegalStateException if too many shared locks
     */
    Closer openAcquire(Transaction txn, R row) throws IOException;

    /**
     * Variant which can act upon a partially filled in row.
     *
     * @param row partially specified row
     * @param key is passed to the {@code RowPredicate.test} method
     * @param value is passed to the {@code RowPredicate.test} method
     */
    Closer openAcquire(Transaction txn, R row, byte[] key, byte[] value) throws IOException;

    /**
     * Acquires shared access for all the predicate locks, without waiting, and retains the
     * locks for the entire transaction scope. If lock acquisition fails, all locks
     * acquired up to that point are released.
     *
     * Returns null if not acquired, or else a Closer instance. Returns NonCloser if accepted,
     * but all the necessary locks were already held. Call close on the Closer when the
     * specific row lock has been acquired, or else call failed if an exception is thrown.
     *
     * This method is primarily used in conjunction with automatic columns. If null is
     * returned, another random number is generated.
     *
     * @param row is passed to the {@code RowPredicate.test} method
     * @return null if not acquired
     * @throws IllegalStateException if too many shared locks
     */
    Closer tryOpenAcquire(Transaction txn, R row) throws IOException;

    /**
     * Acquires shared access for all the predicate locks and an upgradable row lock, waiting
     * if necessary. The returned object is a Lock or an array of Locks, which must be pushed
     * to the transaction by the caller. If an exception is thrown, all locks acquired up to
     * that point are released.
     *
     * @param key is passed to the {@code RowPredicate.test} method
     * @param value is passed to the {@code RowPredicate.test} method
     * @return null or a Lock, or an array of Locks
     * @throws IllegalStateException if too many shared locks
     */
    Object acquireLocksNoPush(Transaction txn, byte[] key, byte[] value)
        throws LockFailureException;

    /**
     * Enables predicate locking for the current transaction scope.
     */
    void redoPredicateMode(Transaction txn) throws IOException;

    /**
     * Adds a predicate lock into the set, and waits for all existing transactions which match
     * on it to finish. Once added, the lock remains in the set for the entire scope of the
     * given transaction, held exclusively. If the add operation times out, the lock is removed
     * from the set, and it isn't added to the transaction.
     *
     * @param txn exclusive owner of the lock
     * @param predicate defines the lock matching rules
     * @return an object which can release the predicate lock before the transaction exits; is
     * null if no actual predicate was installed
     */
    Closer addPredicate(Transaction txn, RowPredicate<R> predicate) throws LockFailureException;

    /**
     * Acquires an exclusive lock, which blocks all calls to openAcquire and addPredicate, and
     * waits for existing locks to be released. The exclusive lock is released when the
     * callback returns or throws an exception.
     *
     * @param mustWait if not null, is called when the lock might not be immediately available
     * @param callback runs with exclusive lock held
     */
    void withExclusiveNoRedo(Transaction txn, Runnable mustWait, Runnable callback)
        throws IOException;

    /**
     * Count the number of predicates currently in the set. O(n) cost.
     */
    int countPredicates();

    /**
     * Returns a class which can be extended for evaluating predicate locks directly. When used,
     * the predicate instances cannot be recycled.
     */
    Class<? extends RowPredicate<R>> evaluatorClass();

    public static interface Closer extends AutoCloseable {
        void close();
    }

    public static final class NonCloser implements Closer {
        public static final NonCloser THE = new NonCloser();

        private NonCloser() {
        }

        @Override
        public void close() {
        }
    }
}
