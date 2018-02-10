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

package org.cojen.tupl;

/**
 * Result code returned by transactional operations which acquire locks.
 *
 * @author Brian S O'Neill
 * @see Transaction
 * @see Cursor
 */
public enum LockResult {
    /**
     * Lock rejection caused by illegal lock mode upgrade.
     * @see IllegalUpgradeException
     * @see LockUpgradeRule
     */
    ILLEGAL(0),

    /**
     * Lock rejection caused by thread interruption.
     *
     * @see LockInterruptedException
     */
    INTERRUPTED(0),

    /** Lock rejection caused by wait timeout, not deadlock. */
    //TIMED_OUT_LATCH(1),

    /**
     * Lock rejection caused by wait timeout or deadlock.
     *
     * @see LockTimeoutException
     */
    TIMED_OUT_LOCK(1),

    /** Lock rejection caused by deadlock. */
    //DEADLOCK(0),

    /** Lock granted for the first time. */
    ACQUIRED(2),

    /** Exclusive lock granted as an upgrade from an owned upgradable lock. */
    UPGRADED(2),

    /**
     * Shared lock is already owned, so no extra unlock should be performed.
     * This result is only possible when trying to acquire a shared lock.
     */
    OWNED_SHARED(3),

    /**
     * Upgradable lock is already owned, so no extra unlock should be
     * performed. This result is possible when trying to acquire a shared or
     * upgradable lock.
     */
    OWNED_UPGRADABLE(3),

    /**
     * Exclusive lock is already owned, so no extra unlock should be performed.
     * This result is possible when trying to acquire any type of lock.
     */
    OWNED_EXCLUSIVE(3),

    /**
     * Result from lock check indicating that locker doesn't own the lock, or the result from a
     * method which didn't require that a lock be acquired.
     */
    UNOWNED(0);

    // 1: timed out, 2: acquired, 3: owned
    private final int mType;

    private LockResult(int type) {
        mType = type;
    }

    /**
     * Rreturns true if lock request timed out. Applicable to {@link #TIMED_OUT_LOCK}.
     */
    public boolean isTimedOut() {
        return mType == 1;
    }

    /**
     * Returns true if lock was just acquired or was already owned. Applicable
     * to {@link #ACQUIRED}, {@link #UPGRADED}, {@link #OWNED_SHARED}, {@link
     * #OWNED_UPGRADABLE}, and {@link #OWNED_EXCLUSIVE}.
     */
    public boolean isHeld() {
        return mType >= 2;
    }

    /**
     * Returns true if lock was already owned when requested. Applicable to {@link
     * #OWNED_SHARED}, {@link #OWNED_UPGRADABLE}, and {@link #OWNED_EXCLUSIVE}.
     */
    public boolean alreadyOwned() {
        return mType == 3;
    }

    boolean isAcquired() {
        return mType == 2;
    }

    /**
     * Returns the lowest common owned level.
     *
     * {@literal UNOWNED < OWNED_SHARED < OWNED_UPGRADABLE < OWNED_EXCLUSIVE}
     */
    LockResult commonOwned(LockResult other) {
        if (this == UNOWNED) {
            return this;
        } else if (this == OWNED_SHARED) {
            return other == UNOWNED ? other : this;
        } else if (this == OWNED_UPGRADABLE) {
            return (other == UNOWNED || other == OWNED_SHARED) ? other : this;
        } else if (this == OWNED_EXCLUSIVE) {
            return other;
        } else {
            throw new IllegalArgumentException();
        }
    }
}
