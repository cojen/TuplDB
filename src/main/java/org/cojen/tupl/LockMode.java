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
 * Various lock modes for use within {@link Transaction transactions}. Except
 * for {@link #UNSAFE}, all modes follow the same policy when modifying
 * entries. They all differ with respect to entries which are being read.
 *
 * <p>When an entry is modified, an exclusive lock is acquired, which is
 * typically held until the end of the transaction. When transaction scopes are
 * committed, all held locks transfer to the parent scope. Uncommitted scopes
 * release all of their acquired locks.
 *
 * <p>Modes ordered from strongest to weakest:
 * <ul>
 * <li>{@link #UPGRADABLE_READ} (default)
 * <li>{@link #REPEATABLE_READ}
 * <li>{@link #READ_COMMITTED}
 * <li>{@link #READ_UNCOMMITTED}
 * <li>{@link #UNSAFE}
 * </ul>
 *
 * @author Brian S O'Neill
 * @see Transaction#lockMode
 */
public enum LockMode {
    /**
     * Lock mode which acquires upgradable locks when reading entries and
     * retains them to the end of the transaction or scope. If an entry guarded
     * by an upgradable lock is modified, the lock is first upgraded to be
     * exclusive.
     */
    UPGRADABLE_READ(LockManager.TYPE_UPGRADABLE, false),

    /**
     * Lock mode which acquires shared locks when reading entries and retains
     * them to the end of the transaction or scope. Attempting to modify
     * entries guarded by a shared lock is {@link LockResult#ILLEGAL
     * illegal}. Consider using {@link #UPGRADABLE_READ} instead.
     *
     * @see LockUpgradeRule
     */
    REPEATABLE_READ(LockManager.TYPE_SHARED, false),

    /**
     * Lock mode which acquires shared locks when reading entries and releases
     * them as soon as possible.
     */
    READ_COMMITTED(0, false),

    /**
     * Lock mode which never acquires locks when reading entries.
     * Modifications made by concurrent transactions are visible for reading,
     * but they might get rolled back.
     */
    READ_UNCOMMITTED(0, true),

    /**
     * Lock mode which never acquires locks. This mode bypasses all
     * transactional safety, permitting modifications even when locked by other
     * transactions. These modifications are immediately committed, and so
     * rollback is not possible.
     */
    UNSAFE(0, true);

    /** Is 0 if not repeatable, TYPE_SHARED or TYPE_UPGRADABLE otherwise. */
    final int repeatable;

    final boolean noReadLock;

    private LockMode(int repeatable, boolean noReadLock) {
        this.repeatable = repeatable;
        this.noReadLock = noReadLock;
    }

    /**
     * Returns true if acquired locks are retained for the duration of the
     * transaction. Applicable to {@link #UPGRADABLE_READ} and {@link #REPEATABLE_READ}.
     */
    public boolean isRepeatable() {
        return repeatable != 0;
    }
}
