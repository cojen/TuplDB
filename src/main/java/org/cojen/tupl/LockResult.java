/*
 *  Copyright 2011-2015 Cojen.org
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

    /** Result from lock check indicating that locker doesn't own the lock. */
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
}
