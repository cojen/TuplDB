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
 * Various lock modes for use within transactions. Except for {@link UNSAFE},
 * all modes follow the same policy when modifying entries. They all differ
 * with respect to entries which are being read.
 *
 * <p>When an entry is modified, an exclusive lock is acquired, which is
 * typically held until the end of the transaction. All locks acquired within
 * transaction scopes are released for uncommitted modifications. Scopes which
 * are committed transfer exclusive locks to the parent scope, and all other
 * acquired locks are released.
 *
 * @author Brian S O'Neill
 */
public enum LockMode {
    /**
     * Lock mode which acquires upgradable locks when reading entries and
     * retains them to the end of the transaction or scope. If an entry guarded
     * by an upgradable lock is modified, the lock is first upgraded to be
     * exclusive.
     */
    UPGRADABLE_READ,

    /**
     * Lock mode which acquires shared locks when reading entries and retains
     * them to the end of the transaction or scope. Attempting to modify
     * entries guarded by a shared lock is {@link LockResult.ILLEGAL
     * illegal}. Consider using {@link UPGRADABLE_READ} instead.
     */
    REPEATABLE_READ,

    /**
     * Lock mode which acquires shared locks when reading entries and releases
     * them as soon as possible.
     */
    READ_COMMITTED,

    /**
     * Lock mode which never acquires locks when reading entries.
     * Modifications made by concurrent transactions are visible for reading,
     * but they might get rolled back.
     */
    READ_UNCOMMITTED,

    /**
     * Lock mode which never acquires locks. This mode bypasses all
     * transactional safety, permitting modifications even when locked by other
     * transactions. These modifications are immediately committed.
     */
    UNSAFE;
}
