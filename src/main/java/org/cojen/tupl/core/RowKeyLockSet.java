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

import org.cojen.tupl.LockFailureException;
import org.cojen.tupl.Transaction;

/**
 * Maintains a set of locks which match on row key predicates, which can be used for
 * implementing serializable transaction isolation.
 *
 * @author Brian S O'Neill
 * @see RowKeyPredicate
 */
public interface RowKeyLockSet<R> {
    /**
     * Acquires shared access for all the row locks, waiting if necessary, and retains the
     * locks for the entire transaction scope. If lock acquisition times out, all row locks
     * acquired up to that point are still retained.
     *
     * @param row is passed to the {@link RowKeyPredicate#testRow} method
     * @throws IllegalStateException if too many shared locks
     */
    void acquireShared(Transaction txn, R row) throws LockFailureException;

    /**
     * Count the number of predicates currently in the set. O(n) cost.
     */
    int countPredicates();

    /**
     * Adds a predicate lock into the set, and waits for all existing transactions which match
     * on it to finish. Once added, the lock remains in the set for the entire scope of the
     * given transaction, held exclusively. If the add operation times out, the lock is removed
     * from the set, and it isn't added to the transaction.
     *
     * @param txn exclusive owner of the lock
     * @param predicate defines the lock matching rules
     */
    void addPredicate(Transaction txn, RowKeyPredicate<R> predicate) throws LockFailureException;

    /**
     * Returns a class which can be extended for evaluating predicate locks directly.
     */
    Class<? extends RowKeyPredicate<R>> evaluatorClass();

    /**
     * @param evaluator can only be added once; no recycling
     * @see #addPredicate
     */
    void addEvaluator(Transaction txn, RowKeyPredicate<R> evaluator) throws LockFailureException;
}
