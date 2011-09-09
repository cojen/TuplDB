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
public class Locker {
    private final LockManager mManager;

    private byte[] mLastKey;
    private int mLastKeyHashCode;

    private int mHashCode;

    public Locker(LockManager manager) {
        if (manager == null) {
            throw new IllegalArgumentException("LockManager is null");
        }
        mManager = manager;
    }

    /**
     * Attempt to acquire a shared lock for the current entry, which denies
     * exclusive locks. If return value is OWNED_*, locker already owns a
     * strong enough lock, and no extra unlock should be performed.
     *
     * @param key key to lock; instance is not cloned
     * @param nanosTimeout maximum time to wait for lock; negative timeout is infinite
     * @return INTERRUPTED, TIMED_OUT_LOCK, ACQUIRED, OWNED_SHARED,
     * OWNED_UPGRADABLE, or OWNED_EXCLUSIVE
     * @throws IllegalStateException if too many shared locks
     */
    public LockResult lockShared(byte[] key, long nanosTimeout) {
        return mManager.lockShared(this, attempt(key), key, nanosTimeout);
    }

    /**
     * Attempt to acquire an upgradable lock for the current entry, which
     * exclusive and additional upgradable locks. If return value is OWNED_*,
     * locker already owns a strong enough lock, and no extra unlock should be
     * performed. If ILLEGAL is returned, locker holds a shared lock, which
     * cannot be upgraded.
     *
     * @param key key to lock; instance is not cloned
     * @param nanosTimeout maximum time to wait for lock; negative timeout is infinite
     * @return ILLEGAL, INTERRUPTED, TIMED_OUT_LOCK, ACQUIRED,
     * OWNED_UPGRADABLE, or OWNED_EXCLUSIVE
     */
    public LockResult lockUpgradable(byte[] key, long nanosTimeout) {
        return mManager.lockUpgradable(this, attempt(key), key, nanosTimeout);
    }

    /**
     * Attempt to acquire an exclusive lock for the current entry, which denies
     * any additional locks. If return value is OWNED_EXCLUSIVE, locker already
     * owns exclusive lock, and no extra unlock should be performed. If ILLEGAL
     * is returned, locker holds a shared lock, which cannot be upgraded.
     *
     * @param key key to lock; instance is not cloned
     * @param nanosTimeout maximum time to wait for lock; negative timeout is infinite
     * @return ILLEGAL, INTERRUPTED, TIMED_OUT_LOCK, ACQUIRED, UPGRADED, or
     * OWNED_EXCLUSIVE
     */
    public LockResult lockExclusive(byte[] key, long nanosTimeout) {
        return mManager.lockExclusive(this, attempt(key), key, nanosTimeout);
    }

    /**
     * Fully unlocks key associated with last lock attempt. Lock must already
     * be held.
     *
     * @throws IllegalStateException if lock not held or no last attempt
     */
    public void unlock() {
        mManager.unlock(this, mLastKeyHashCode, lastAttempt());
    }

    /**
     * Fully unlocks given key. Lock must already be held.
     *
     * @param key key to unlock
     * @throws IllegalStateException if lock not held
     */
    public void unlock(byte[] key) {
        mManager.unlock(this, LockManager.hashCode(key), key);
    }

    /**
     * Unlocks key associated with last lock attempt, retaining a shared lock.
     * Lock must already be held.
     *
     * @throws IllegalStateException if lock not held, if too many shared
     * locks, or no last attempt
     */
    public void unlockToShared() {
        mManager.unlockToShared(this, mLastKeyHashCode, lastAttempt());
    }

    /**
     * Unlocks given key, retaining a shared lock. Lock must already be held.
     *
     * @param key key to unlock
     * @throws IllegalStateException if lock not held or too many shared locks
     */
    public void unlockToShared(byte[] key) {
        mManager.unlockToShared(this, LockManager.hashCode(key), key);
    }

    /**
     * Unlocks key associated with last lock attempt, retaining an upgradable
     * lock. Lock must already be held as exclusive or upgradable.
     *
     * @throws IllegalStateException if lock not held or no last attempt
     */
    public void unlockToUpgradable() {
        mManager.unlockToUpgradable(this, mLastKeyHashCode, lastAttempt());
    }

    /**
     * Unlocks given key, retaining an upgradable lock. Lock must already be
     * held as exclusive or upgradable.
     *
     * @param key key to unlock
     * @throws IllegalStateException if lock not held
     */
    public void unlockToUpgradable(byte[] key) {
        mManager.unlockToUpgradable(this, LockManager.hashCode(key), key);
    }

    /**
     * Release all locks currently held by this Locker.
     */
    public void unlockAll() {
        // FIXME
    }

    @Override
    public final int hashCode() {
        // Caching the default hashcode doubles the performance of this method,
        // when using the HotSpot client or server VM.
        int hash = mHashCode;
        if (hash == 0) {
            hash = mHashCode = super.hashCode();
        }
        return hash;
    }

    private int attempt(byte[] key) {
        mLastKey = key;
        return mLastKeyHashCode = LockManager.hashCode(key);
    }

    private byte[] lastAttempt() {
        byte[] key = mLastKey;
        if (key == null) {
            throw new IllegalStateException("No last lock attempt");
        }
        return key;
    }
}
