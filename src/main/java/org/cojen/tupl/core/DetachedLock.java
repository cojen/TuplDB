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
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Transaction;

/**
 * Defines a Lock which isn't bound to an Index and key. A DetachedLock can only ever have one
 * exclusive owner because the exclusive lock is only downgraded to upgradable when released.
 * DetachedLocks aren't registered with LockManager, and leaving the lock in an upgradable
 * state prevents any attempt to remove a DetachedLock from the LockManager, which would fail.
 *
 * @author Brian S O'Neill
 */
public interface DetachedLock {
    /**
     * Any transaction can attempt to acquire the shared lock.
     *
     * @throws IllegalStateException if too many shared locks
     */
    void acquireShared(Transaction txn) throws LockFailureException;

    /**
     * Any transaction can attempt to acquire the shared lock.
     *
     * @return INTERRUPTED, TIMED_OUT_LOCK, DEADLOCK, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE,
     * or OWNED_EXCLUSIVE
     * @throws IllegalStateException if too many shared locks
     */
    LockResult tryAcquireShared(Transaction txn, long nanosTimeout);

    /**
     * Only the owner can attempt to acquire the exclusive lock.
     */
    void acquireExclusive() throws LockFailureException;

    /**
     * Only the owner can attempt to acquire the exclusive lock.
     *
     * @return INTERRUPTED, TIMED_OUT_LOCK, DEADLOCK, ACQUIRED, or OWNED_EXCLUSIVE
     */
    LockResult tryAcquireExclusive(long nanosTimeout);
}
