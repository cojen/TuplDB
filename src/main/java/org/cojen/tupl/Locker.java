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

import static org.cojen.tupl.LockManager.*;

/**
 * Accumulates a scoped stack of locks, bound to arbitrary keys. Locker
 * instances can only be safely used by one thread at a time. Lockers can be
 * exchanged by threads, as long as a happens-before relationship is
 * established. Without proper exclusion, multiple threads interacting with a
 * Locker instance may cause database corruption.
 *
 * @author Brian S O'Neill
 */
/*P*/
class Locker extends LockOwner {
    final LockManager mManager;

    ParentScope mParentScope;

    // Is null if empty; Lock instance if one; Block if more.
    Object mTailBlock;

    /**
     * @param manager null for Transaction.BOGUS or when closing down LockManager
     */
    Locker(LockManager manager) {
        mManager = manager;
    }

    private LockManager manager() {
        LockManager manager = mManager;
        if (manager == null) {
            throw new IllegalStateException("Transaction is bogus");
        }
        return manager;
    }

    @Override
    public final LocalDatabase getDatabase() {
        return manager().mDatabaseRef.get();
    }

    @Override
    public void attach(Object obj) {
        // Thread-local lockers aren't accessible from the public API.
        throw new UnsupportedOperationException();
    }

    @Override
    public Object attachment() {
        return null;
    }

    /**
     * Returns true if the current transaction scope is nested.
     */
    public final boolean isNested() {
        return mParentScope != null;
    }

    /**
     * Counts the current transaction scope nesting level. Count is zero if non-nested.
     */
    public final int nestingLevel() {
        int count = 0;
        ParentScope parent = mParentScope;
        while (parent != null) {
            count++;
            parent = parent.mParentScope;
        }
        return count;
    }

    /**
     * @param lockType TYPE_SHARED, TYPE_UPGRADABLE, or TYPE_EXCLUSIVE
     */
    final LockResult tryLock(int lockType, long indexId, byte[] key, int hash, long nanosTimeout)
        throws DeadlockException
    {
        LockResult result = manager().getLockHT(hash)
            .tryLock(lockType, this, indexId, key, hash, nanosTimeout);

        if (result == LockResult.TIMED_OUT_LOCK) {
            Lock waitingFor = mWaitingFor;
            if (waitingFor != null) {
                try {
                    // Perform deadlock detection except for the fast-fail case.
                    if (nanosTimeout != 0) {
                        detectDeadlock(waitingFor, lockType, nanosTimeout, hash);
                    }
                } finally {
                    mWaitingFor = null;
                }
            }
        }

        return result;
    }

    /**
     * @param lockType TYPE_SHARED, TYPE_UPGRADABLE, or TYPE_EXCLUSIVE
     */
    final LockResult lock(int lockType, long indexId, byte[] key, int hash, long nanosTimeout)
        throws LockFailureException
    {
        LockResult result = manager().getLockHT(hash)
            .tryLock(lockType, this, indexId, key, hash, nanosTimeout);
        if (result.isHeld()) {
            return result;
        }
        throw failed(lockType, result, nanosTimeout, hash);
    }

    /**
     * NT == No Timeout or deadlock exception thrown
     *
     * @param lockType TYPE_SHARED, TYPE_UPGRADABLE, or TYPE_EXCLUSIVE
     * @return {@link LockResult#TIMED_OUT_LOCK TIMED_OUT_LOCK}, {@link
     * LockResult#ACQUIRED ACQUIRED}, {@link LockResult#OWNED_SHARED
     * OWNED_SHARED}, {@link LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or
     * {@link LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     */
    @SuppressWarnings("incomplete-switch")
    final LockResult lockNT(int lockType, long indexId, byte[] key, int hash, long nanosTimeout)
        throws LockFailureException
    {
        LockResult result = manager().getLockHT(hash)
            .tryLock(lockType, this, indexId, key, hash, nanosTimeout);
        if (!result.isHeld()) {
            switch (result) {
            case ILLEGAL:
                throw new IllegalUpgradeException();
            case INTERRUPTED:
                throw new LockInterruptedException();
            }
        }
        return result;
    }

    /**
     * Attempts to acquire a shared lock for the given key, denying exclusive
     * locks. If return value is {@link LockResult#alreadyOwned owned}, transaction
     * already owns a strong enough lock, and no extra unlock should be
     * performed.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @param key non-null key to lock; instance is not cloned
     * @param nanosTimeout maximum time to wait for lock; negative timeout is infinite
     * @return {@link LockResult#INTERRUPTED INTERRUPTED}, {@link
     * LockResult#TIMED_OUT_LOCK TIMED_OUT_LOCK}, {@link LockResult#ACQUIRED
     * ACQUIRED}, {@link LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws IllegalStateException if too many shared locks
     * @throws DeadlockException if deadlock was detected after waiting full timeout
     */
    public final LockResult tryLockShared(long indexId, byte[] key, long nanosTimeout)
        throws DeadlockException
    {
        return tryLock(TYPE_SHARED, indexId, key, hash(indexId, key), nanosTimeout);
    }

    final LockResult tryLockShared(long indexId, byte[] key, int hash, long nanosTimeout)
        throws DeadlockException
    {
        return tryLock(TYPE_SHARED, indexId, key, hash, nanosTimeout);
    }

    /**
     * Attempts to acquire a shared lock for the given key, denying exclusive
     * locks. If return value is {@link LockResult#alreadyOwned owned}, transaction
     * already owns a strong enough lock, and no extra unlock should be
     * performed.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @param key non-null key to lock; instance is not cloned
     * @param nanosTimeout maximum time to wait for lock; negative timeout is infinite
     * @return {@link LockResult#ACQUIRED ACQUIRED}, {@link
     * LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws IllegalStateException if too many shared locks
     * @throws LockFailureException if interrupted or timed out
     * @throws DeadlockException if deadlock was detected after waiting full timeout
     */
    public final LockResult lockShared(long indexId, byte[] key, long nanosTimeout)
        throws LockFailureException
    {
        return lock(TYPE_SHARED, indexId, key, hash(indexId, key), nanosTimeout);
    }

    final LockResult lockShared(long indexId, byte[] key, int hash, long nanosTimeout)
        throws LockFailureException
    {
        return lock(TYPE_SHARED, indexId, key, hash, nanosTimeout);
    }

    /**
     * NT == No Timeout or deadlock exception thrown
     *
     * @return {@link LockResult#TIMED_OUT_LOCK TIMED_OUT_LOCK}, {@link
     * LockResult#ACQUIRED ACQUIRED}, {@link LockResult#OWNED_SHARED
     * OWNED_SHARED}, {@link LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or
     * {@link LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     */
    final LockResult lockSharedNT(long indexId, byte[] key, int hash, long nanosTimeout)
        throws LockFailureException
    {
        return lockNT(TYPE_SHARED, indexId, key, hash, nanosTimeout);
    }

    /**
     * Attempts to acquire an upgradable lock for the given key, denying
     * exclusive and additional upgradable locks. If return value is {@link
     * LockResult#alreadyOwned owned}, transaction already owns a strong enough
     * lock, and no extra unlock should be performed. If {@link
     * LockResult#ILLEGAL ILLEGAL} is returned, transaction holds a shared
     * lock, which cannot be upgraded.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @param key non-null key to lock; instance is not cloned
     * @param nanosTimeout maximum time to wait for lock; negative timeout is infinite
     * @return {@link LockResult#ILLEGAL ILLEGAL}, {@link
     * LockResult#INTERRUPTED INTERRUPTED}, {@link LockResult#TIMED_OUT_LOCK
     * TIMED_OUT_LOCK}, {@link LockResult#ACQUIRED ACQUIRED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws DeadlockException if deadlock was detected after waiting full timeout
     */
    public final LockResult tryLockUpgradable(long indexId, byte[] key, long nanosTimeout)
        throws DeadlockException
    {
        return tryLock(TYPE_UPGRADABLE, indexId, key, hash(indexId, key), nanosTimeout);
    }

    final LockResult tryLockUpgradable(long indexId, byte[] key, int hash, long nanosTimeout)
        throws DeadlockException
    {
        return tryLock(TYPE_UPGRADABLE, indexId, key, hash, nanosTimeout);
    }

    /**
     * Attempts to acquire an upgradable lock for the given key, denying
     * exclusive and additional upgradable locks. If return value is {@link
     * LockResult#alreadyOwned owned}, transaction already owns a strong enough
     * lock, and no extra unlock should be performed.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @param key non-null key to lock; instance is not cloned
     * @param nanosTimeout maximum time to wait for lock; negative timeout is infinite
     * @return {@link LockResult#ACQUIRED ACQUIRED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws LockFailureException if interrupted, timed out, or illegal upgrade
     * @throws DeadlockException if deadlock was detected after waiting full timeout
     */
    public final LockResult lockUpgradable(long indexId, byte[] key, long nanosTimeout)
        throws LockFailureException
    {
        return lock(TYPE_UPGRADABLE, indexId, key, hash(indexId, key), nanosTimeout);
    }

    final LockResult lockUpgradable(long indexId, byte[] key, int hash, long nanosTimeout)
        throws LockFailureException
    {
        return lock(TYPE_UPGRADABLE, indexId, key, hash, nanosTimeout);
    }

    /**
     * NT == No Timeout or deadlock exception thrown
     *
     * @return {@link LockResult#TIMED_OUT_LOCK TIMED_OUT_LOCK}, {@link
     * LockResult#ACQUIRED ACQUIRED}, {@link LockResult#OWNED_SHARED
     * OWNED_SHARED}, {@link LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or
     * {@link LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     */
    final LockResult lockUpgradableNT(long indexId, byte[] key, int hash, long nanosTimeout)
        throws LockFailureException
    {
        return lockNT(TYPE_UPGRADABLE, indexId, key, hash, nanosTimeout);
    }

    /**
     * Attempts to acquire an exclusive lock for the given key, denying any
     * additional locks. If return value is {@link LockResult#alreadyOwned
     * owned}, transaction already owns exclusive lock, and no extra unlock
     * should be performed. If {@link LockResult#ILLEGAL ILLEGAL} is returned,
     * transaction holds a shared lock, which cannot be upgraded.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @param key non-null key to lock; instance is not cloned
     * @param nanosTimeout maximum time to wait for lock; negative timeout is infinite
     * @return {@link LockResult#ILLEGAL ILLEGAL}, {@link
     * LockResult#INTERRUPTED INTERRUPTED}, {@link LockResult#TIMED_OUT_LOCK
     * TIMED_OUT_LOCK}, {@link LockResult#ACQUIRED ACQUIRED}, {@link
     * LockResult#UPGRADED UPGRADED}, or {@link LockResult#OWNED_EXCLUSIVE
     * OWNED_EXCLUSIVE}
     * @throws DeadlockException if deadlock was detected after waiting full timeout
     */
    public final LockResult tryLockExclusive(long indexId, byte[] key, long nanosTimeout)
        throws DeadlockException
    {
        return tryLock(TYPE_EXCLUSIVE, indexId, key, hash(indexId, key), nanosTimeout);
    }

    final LockResult tryLockExclusive(long indexId, byte[] key, int hash, long nanosTimeout)
        throws DeadlockException
    {
        return tryLock(TYPE_EXCLUSIVE, indexId, key, hash, nanosTimeout);
    }

    /**
     * Attempts to acquire an exclusive lock for the given key, denying any
     * additional locks. If return value is {@link LockResult#alreadyOwned owned},
     * transaction already owns exclusive lock, and no extra unlock should be
     * performed.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @param key non-null key to lock; instance is not cloned
     * @param nanosTimeout maximum time to wait for lock; negative timeout is infinite
     * @return {@link LockResult#ACQUIRED ACQUIRED}, {@link LockResult#UPGRADED
     * UPGRADED}, or {@link LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws LockFailureException if interrupted, timed out, or illegal upgrade
     * @throws DeadlockException if deadlock was detected after waiting full timeout
     */
    public final LockResult lockExclusive(long indexId, byte[] key, long nanosTimeout)
        throws LockFailureException
    {
        return lock(TYPE_EXCLUSIVE, indexId, key, hash(indexId, key), nanosTimeout);
    }

    final LockResult lockExclusive(long indexId, byte[] key, int hash, long nanosTimeout)
        throws LockFailureException
    {
        return lock(TYPE_EXCLUSIVE, indexId, key, hash, nanosTimeout);
    }

    /**
     * Lock acquisition used by recovery.
     *
     * @param lock Lock instance to insert, unless another already exists. The mIndexId,
     * mKey, and mHashCode fields must be set.
     */
    final LockResult lockExclusive(Lock lock, long nanosTimeout) throws LockFailureException {
        LockResult result = mManager.getLockHT(lock.mHashCode)
            .tryLockExclusive(this, lock, nanosTimeout);
        if (result.isHeld()) {
            return result;
        }
        throw failed(TYPE_EXCLUSIVE, result, nanosTimeout, lock.mHashCode);
    }

    /**
     * NT == No Timeout or deadlock exception thrown
     *
     * @return {@link LockResult#TIMED_OUT_LOCK TIMED_OUT_LOCK}, {@link
     * LockResult#ACQUIRED ACQUIRED}, {@link LockResult#OWNED_SHARED
     * OWNED_SHARED}, {@link LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or
     * {@link LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     */
    final LockResult lockExclusiveNT(long indexId, byte[] key, int hash, long nanosTimeout)
        throws LockFailureException
    {
        return lockNT(TYPE_EXCLUSIVE, indexId, key, hash, nanosTimeout);
    }

    /**
     * Checks if an upgrade attempt should be made when the locker only holds a shared lock.
     *
     * @param count current lock count, not zero
     */
    final boolean canAttemptUpgrade(int count) {
        LockUpgradeRule lockUpgradeRule = mManager.mDefaultLockUpgradeRule;
        return lockUpgradeRule == LockUpgradeRule.UNCHECKED
            | (lockUpgradeRule == LockUpgradeRule.LENIENT & count == 1);
    }

    /**
     * @param lockType TYPE_SHARED, TYPE_UPGRADABLE, or TYPE_EXCLUSIVE
     */
    @SuppressWarnings("incomplete-switch")
    LockFailureException failed(int lockType, LockResult result, long nanosTimeout, int hash)
        throws DeadlockException
    {
        Lock waitingFor;

        switch (result) {
        case TIMED_OUT_LOCK:
            waitingFor = mWaitingFor;
            if (waitingFor != null) {
                try {
                    detectDeadlock(waitingFor, lockType, nanosTimeout, hash);
                } finally {
                    mWaitingFor = null;
                }
            }
            break;
        case ILLEGAL:
            return new IllegalUpgradeException();
        case INTERRUPTED:
            return new LockInterruptedException();
        default:
            waitingFor = mWaitingFor;
            mWaitingFor = null;
        }

        if (result.isTimedOut()) {
            Object att = waitingFor == null ? null
                : waitingFor.findOwnerAttachment(this, lockType, hash);
            return new LockTimeoutException(nanosTimeout, att);
        }

        return new LockFailureException();
    }

    /**
     * @param waitingFor should not be not null
     * @param lockType TYPE_SHARED, TYPE_UPGRADABLE, or TYPE_EXCLUSIVE
     */
    private void detectDeadlock(Lock waitingFor, int lockType, long nanosTimeout, int hash)
        throws DeadlockException
    {
        DeadlockDetector detector = new DeadlockDetector(this);
        if (detector.scan()) {
            Object att = waitingFor == null ? null
                : waitingFor.findOwnerAttachment(this, lockType, hash);
            throw new DeadlockException(nanosTimeout, att,
                                        detector.mGuilty,
                                        detector.newDeadlockSet(lockType, hash));
        }
    }

    /**
     * Checks the lock ownership for the given key.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link
     * LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     */
    public final LockResult lockCheck(long indexId, byte[] key) {
        return manager().check(this, indexId, key, hash(indexId, key));
    }

    /**
     * Returns the index id of the last lock acquired, within the current scope.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @return locked index id
     * @throws IllegalStateException if no locks held
     */
    public final long lastLockedIndex() {
        return peek().mIndexId;
    }

    /**
     * Returns the key of the last lock acquired, within the current scope.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @return locked key; instance is not cloned
     * @throws IllegalStateException if no locks held
     */
    public final byte[] lastLockedKey() {
        return peek().mKey;
    }

    private Lock peek() {
        Object tailObj = mTailBlock;
        if (tailObj == null) {
            throw new IllegalStateException("No locks held");
        }
        return (tailObj instanceof Lock) ? ((Lock) tailObj) : (((Block) tailObj).last());
    }

    /**
     * Fully releases last lock acquired, within the current scope. If the last
     * lock operation was an upgrade, for a lock not immediately acquired,
     * unlock is not allowed. Instead, an IllegalStateException is thrown.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i> Also, the current
     * implementation does not accurately track scopes. It may permit an unlock operation to
     * cross a scope boundary, which has undefined behavior.
     *
     * @throws IllegalStateException if no locks held, or if unlocking a
     * non-immediate upgrade
     */
    public final void unlock() {
        Object tailObj = mTailBlock;
        if (tailObj == null) {
            throw new IllegalStateException("No locks held");
        }
        if (tailObj instanceof Lock) {
            mTailBlock = null;
            mManager.unlock(this, (Lock) tailObj);
        } else {
            ((Block) tailObj).unlockLast(this);
        }
    }

    /**
     * Releases last lock acquired, within the current scope, retaining a
     * shared lock. If the last lock operation was an upgrade, for a lock not
     * immediately acquired, unlock is not allowed. Instead, an
     * IllegalStateException is thrown.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i> Also, the current
     * implementation does not accurately track scopes. It may permit an unlock operation to
     * cross a scope boundary, which has undefined behavior.
     *
     * @throws IllegalStateException if no locks held, or if too many shared
     * locks, or if unlocking a non-immediate upgrade
     */
    public final void unlockToShared() {
        Object tailObj = mTailBlock;
        if (tailObj == null) {
            throw new IllegalStateException("No locks held");
        }
        if (tailObj instanceof Lock) {
            mManager.unlockToShared(this, (Lock) tailObj);
        } else {
            ((Block) tailObj).unlockLastToShared(this);
        }
    }

    /**
     * Releases last lock acquired or upgraded, within the current scope,
     * retaining an upgradable lock.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i> Also, the current
     * implementation does not accurately track scopes. It may permit an unlock operation to
     * cross a scope boundary, which has undefined behavior.
     *
     * @throws IllegalStateException if no locks held, or if last lock is shared
     */
    public final void unlockToUpgradable() {
        Object tailObj = mTailBlock;
        if (tailObj == null) {
            throw new IllegalStateException("No locks held");
        }
        if (tailObj instanceof Lock) {
            mManager.unlockToUpgradable(this, (Lock) tailObj);
        } else {
            ((Block) tailObj).unlockLastToUpgradable(this);
        }
    }

    /**
     * @return new parent scope
     */
    final ParentScope scopeEnter() {
        ParentScope parent = new ParentScope();
        parent.mParentScope = mParentScope;
        Object tailObj = mTailBlock;
        parent.mTailBlock = tailObj;
        if (tailObj instanceof Block) {
            parent.mTailBlockSize = ((Block) tailObj).mSize;
        }
        mParentScope = parent;
        return parent;
    }

    /**
     * Promote all locks acquired within this scope to the parent scope.
     */
    final void promote() {
        Object tailObj = mTailBlock;
        if (tailObj != null) {
            ParentScope parent = mParentScope;
            parent.mTailBlock = tailObj;
            if (tailObj instanceof Block) {
                parent.mTailBlockSize = ((Block) tailObj).mSize;
            }
        }
    }

    /**
     * Releases all locks held by this Locker, within the current scope. If not
     * in a scope, all held locks are released.
     */
    final void scopeUnlockAll() {
        ParentScope parent = mParentScope;
        Object parentTailObj;
        if (parent == null || (parentTailObj = parent.mTailBlock) == null) {
            // Unlock everything.
            Object tailObj = mTailBlock;
            if (tailObj instanceof Lock) {
                mManager.unlock(this, (Lock) tailObj);
                mTailBlock = null;
            } else {
                Block tail = (Block) tailObj;
                if (tail != null) {
                    do {
                        tail.unlockToSavepoint(this, 0);
                        tail = tail.pop();
                    } while (tail != null);
                    mTailBlock = null;
                }
            }
        } else if (parentTailObj instanceof Lock) {
            Object tailObj = mTailBlock;
            if (tailObj instanceof Block) {
                Block tail = (Block) tailObj;
                while (true) {
                    Block prev = tail.peek();
                    if (prev == null) {
                        tail.unlockToSavepoint(this, 1);
                        break;
                    }
                    tail.unlockToSavepoint(this, 0);
                    tail.discard();
                    tail = prev;
                }
                mTailBlock = tail;
            }
        } else {
            Block tail = (Block) mTailBlock;
            while (tail != parentTailObj) {
                tail.unlockToSavepoint(this, 0);
                tail = tail.pop();
            }
            tail.unlockToSavepoint(this, parent.mTailBlockSize);
            mTailBlock = tail;
        }
    }

    /**
     * Transfers all exclusive locks held by this Locker, for the top scope only. All other
     * locks are released.
     */
    final PendingTxn transferExclusive() {
        PendingTxn pending;

        Object tailObj = mTailBlock;
        if (tailObj instanceof Lock) {
            pending = mManager.transferExclusive(this, (Lock) tailObj, null);
        } else if (tailObj == null) {
            pending = new PendingTxn(null);
        } else {
            pending = null;
            Block tail = (Block) tailObj;
            do {
                pending = tail.transferExclusive(this, pending);
                tail = tail.pop();
            } while (tail != null);
        }

        mTailBlock = null;

        return pending;
    }

    /**
     * Exits the current scope, releasing all held locks.
     *
     * @return old parent scope
     */
    final ParentScope scopeExit() {
        scopeUnlockAll();
        return popScope();
    }

    /**
     * Releases all locks held by this Locker, and exits all scopes.
     */
    final void scopeExitAll() {
        mParentScope = null;
        scopeUnlockAll();
        mTailBlock = null;
    }

    /**
     * Discards all the locks held by this Locker, and exits all scopes. Calling this prevents
     * the locks from ever being released -- they leak. Should only be called in response to
     * some fatal error.
     */
    final void discardAllLocks() {
        mParentScope = null;
        mTailBlock = null;
    }

    /**
     * @param upgrade only 0 or 1 allowed
     */
    final void push(Lock lock, int upgrade) {
        Object tailObj = mTailBlock;
        if (tailObj == null) {
            mTailBlock = upgrade == 0 ? lock : new Block(lock);
        } else if (tailObj instanceof Lock) {
            // Don't push lock upgrade if it applies to the last acquisition
            // within this scope. This is required for unlockLast.
            if (tailObj != lock || mParentScope != null) {
                mTailBlock = new Block((Lock) tailObj, lock, upgrade);
            }
        } else {
            ((Block) tailObj).pushLock(this, lock, upgrade);
        }
    }

    /**
     * @return old parent scope
     */
    private ParentScope popScope() {
        ParentScope parent = mParentScope;
        if (parent == null) {
            mTailBlock = null;
        } else {
            mTailBlock = parent.mTailBlock;
            mParentScope = parent.mParentScope;
        }
        return parent;
    }

    static final class Block {
        private static final int FIRST_BLOCK_CAPACITY = 8;
        // Limited by number of bits available in mUpgrades.
        private static final int HIGHEST_BLOCK_CAPACITY = 64;

        private Lock[] mLocks;
        private long mUpgrades;
        // Size must always be least 1.
        int mSize;

        private Block mPrev;

        // Always creates first as an upgrade.
        Block(Lock first) {
            (mLocks = new Lock[FIRST_BLOCK_CAPACITY])[0] = first;
            mUpgrades = 1;
            mSize = 1;
        }

        // First is never an upgrade.
        Block(Lock first, Lock second, int upgrade) {
            Lock[] locks = new Lock[FIRST_BLOCK_CAPACITY];
            locks[0] = first;
            locks[1] = second;
            mLocks = locks;
            mUpgrades = upgrade << 1;
            mSize = 2;
        }

        private Block(Block prev, Lock first, int upgrade) {
            mPrev = prev;
            int capacity = prev.mLocks.length;
            if (capacity < FIRST_BLOCK_CAPACITY) {
                capacity = FIRST_BLOCK_CAPACITY;
            } else if (capacity < HIGHEST_BLOCK_CAPACITY) {
                capacity <<= 1;
            }
            (mLocks = new Lock[capacity])[0] = first;
            mUpgrades = upgrade;
            mSize = 1;
        }

        void pushLock(Locker locker, Lock lock, int upgrade) {
            Lock[] locks = mLocks;
            int size = mSize;

            // Don't push lock upgrade if it applies to the last acquisition
            // within this scope. This is required for unlockLast.
            ParentScope parent;
            if (upgrade != 0
                && ((parent = locker.mParentScope) == null || parent.mTailBlockSize != size)
                && locks[size - 1] == lock)
            {
                return;
            }

            if (size < locks.length) {
                locks[size] = lock;
                mUpgrades |= ((long) upgrade) << size;
                mSize = size + 1;
            } else {
                locker.mTailBlock = new Block(this, lock, upgrade);
            }
        }

        Lock last() {
            return mLocks[mSize - 1];
        }

        void unlockLast(Locker locker) {
            int size = mSize - 1;

            long upgrades = mUpgrades;
            long mask = 1L << size;
            if ((upgrades & mask) != 0) {
                throw new IllegalStateException("Cannot unlock non-immediate upgrade");
            }

            Lock[] locks = mLocks;
            locker.mManager.unlock(locker, locks[size]);

            // Only pop lock if unlock succeeded.
            locks[size] = null;
            if (size == 0) {
                locker.mTailBlock = mPrev;
                mPrev = null;
            } else {
                mUpgrades &= upgrades & ~mask;
                mSize = size;
            }
        }

        void unlockLastToShared(Locker locker) {
            int size = mSize - 1;
            if ((mUpgrades & (1L << size)) != 0) {
                throw new IllegalStateException("Cannot unlock non-immediate upgrade");
            }
            locker.mManager.unlockToShared(locker, mLocks[size]);
        }

        void unlockLastToUpgradable(Locker locker) {
            Lock[] locks = mLocks;
            int size = mSize;
            locker.mManager.unlockToUpgradable(locker, locks[--size]);

            long upgrades = mUpgrades;
            long mask = 1L << size;
            if ((upgrades & mask) != 0) {
                // Pop upgrade off stack, but only if unlock succeeded.
                locks[size] = null;
                if (size == 0) {
                    locker.mTailBlock = mPrev;
                    mPrev = null;
                } else {
                    mUpgrades = upgrades & ~mask;
                    mSize = size;
                }
            }
        }

        /**
         * Note: If target size is zero, caller MUST pop and discard the block. Otherwise, the
         * block size will be zero, which is illegal.
         */
        void unlockToSavepoint(Locker locker, int targetSize) {
            int size = mSize;
            if (size > targetSize) {
                Lock[] locks = mLocks;
                LockManager manager = locker.mManager;
                size--;
                long mask = 1L << size;
                long upgrades = mUpgrades;
                while (true) {
                    Lock lock = locks[size];
                    if ((upgrades & mask) != 0) {
                        manager.unlockToUpgradable(locker, lock);
                    } else {
                        manager.unlock(locker, lock);
                    }
                    locks[size] = null;
                    if (size == targetSize) {
                        break;
                    }
                    size--;
                    mask >>>= 1;
                }
                mUpgrades = upgrades & ~(~0L << size);
                mSize = size;
            }
        }

        /**
         * Note: Caller MUST pop and discard the block.
         */
        PendingTxn transferExclusive(Locker locker, PendingTxn pending) {
            int size = mSize;
            if (size > 0) {
                Lock[] locks = mLocks;
                LockManager manager = locker.mManager;
                do {
                    Lock lock = locks[--size];
                    pending = manager.transferExclusive(locker, lock, pending);
                } while (size != 0);
            }
            return pending;
        }

        Block pop() {
            Block prev = mPrev;
            mPrev = null;
            return prev;
        }

        Block peek() {
            return mPrev;
        }

        void discard() {
            mPrev = null;
        }
    }

    static final class ParentScope {
        ParentScope mParentScope;
        Object mTailBlock;
        // Must be zero if tail is not a block.
        int mTailBlockSize;

        // These fields are used by Transaction.
        LockMode mLockMode;
        long mLockTimeoutNanos;
        int mHasState;
        long mSavepoint;
    }
}
