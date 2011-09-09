/*
 *  Copyright 2011 Brian S O'Neill
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
 * 
 *
 * @author Brian S O'Neill
 */
public enum LockResult {
    /** Lock rejection caused by illegal lock mode upgrade. */
    ILLEGAL(0),

    /** Lock rejection caused by thread interruption. */
    INTERRUPTED(0),

    /** Lock rejection caused by wait timeout, not deadlock. */
    TIMED_OUT_LATCH(1),

    /** Lock rejection caused by wait timeout or deadlock. */
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
    OWNED_EXCLUSIVE(3);

    // 1: timed out, 2: acquired, 3: owned
    private final int mType;

    private LockResult(int type) {
        mType = type;
    }

    public boolean isTimedOut() {
        return mType == 1;
    }

    /**
     * @return true if acquired or owned
     */
    public boolean isGranted() {
        return mType >= 2;
    }

    public boolean isOwned() {
        return mType == 3;
    }
}
