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

package org.cojen.tupl.core;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.lang.ref.WeakReference;

import java.util.Arrays;

import java.util.concurrent.ThreadLocalRandom;

import org.cojen.tupl.DeadlockException;
import org.cojen.tupl.IllegalUpgradeException;
import org.cojen.tupl.LockFailureException;
import org.cojen.tupl.LockInterruptedException;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.LockTimeoutException;
import org.cojen.tupl.LockUpgradeRule;

import static org.cojen.tupl.core.LockManager.*;

/**
 * Accumulates a scoped stack of locks, bound to arbitrary keys. Locker
 * instances can only be safely used by one thread at a time. Lockers can be
 * exchanged by threads, as long as a happens-before relationship is
 * established. Without proper exclusion, multiple threads interacting with a
 * Locker instance may cause database corruption.
 *
 * @author Brian S O'Neill
 */
class Locker implements DatabaseAccess { // weak access to database
    final LockManager mManager;
    final int mHash;

    // Locker is currently waiting to acquire this lock. Used for deadlock detection.
    Lock mWaitingFor;

    ParentScope mParentScope;

    // Is null if empty; Lock instance if one; Block if more.
    private Object mTailBlock;

    // Must use this when pushing locks, as needed by the findAnyConflict method.
    private static final VarHandle cTailBlockHandle;

    static {
        try {
            cTailBlockHandle = MethodHandles.lookup().findVarHandle
                (Locker.class, "mTailBlock", Object.class);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    /**
     * @param manager null for Transaction.BOGUS or when closing down LockManager
     */
    Locker(LockManager manager) {
        this(manager, ThreadLocalRandom.current().nextInt());
    }

    /**
     * @param manager null for Transaction.BOGUS or when closing down LockManager
     */
    Locker(LockManager manager, int hash) {
        mManager = manager;
        mHash = hash;
    }

    /**
     * Used for selecting a transaction context. Equivalent to identity hash code, but more
     * efficient.
     */
    @Override
    public final int hashCode() {
        return mHash;
    }

    private LockManager manager() {
        LockManager manager = mManager;
        if (manager == null) {
            throw new IllegalStateException("Transaction is bogus");
        }
        return manager;
    }

    @Override
    public LocalDatabase getDatabase() {
        LockManager manager = mManager;
        if (manager != null) {
            WeakReference<LocalDatabase> ref = manager.mDatabaseRef;
            if (ref != null) {
                return ref.get();
            }
        }
        return null;
    }

    public void attach(Object obj) {
        // Thread-local lockers aren't accessible from the public API.
        throw new UnsupportedOperationException();
    }

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
    final LockResult doTryLock(int lockType, long indexId, byte[] key, int hash, long nanosTimeout)
        throws DeadlockException
    {
        LockResult result = manager().getBucket(hash)
            .tryLock(lockType, this, indexId, key, hash, nanosTimeout);

        if (!result.isHeld()) {
            try {
                // Perform deadlock detection except for the fast-fail case. The lock result
                // shouldn't be DEADLOCK when the timeout is 0. See Lock class.
                if (nanosTimeout != 0) {
                    Lock waitingFor = mWaitingFor;
                    if (waitingFor != null) {
                        waitingFor.detectDeadlock(this, lockType, nanosTimeout);
                    }
                }
            } finally {
                mWaitingFor = null;
            }
        }

        return result;
    }

    /**
     * @param lockType TYPE_SHARED, TYPE_UPGRADABLE, or TYPE_EXCLUSIVE
     */
    final LockResult doLock(int lockType, long indexId, byte[] key, int hash, long nanosTimeout)
        throws LockFailureException
    {
        LockResult result = manager().getBucket(hash)
            .tryLock(lockType, this, indexId, key, hash, nanosTimeout);
        if (result.isHeld()) {
            return result;
        }
        throw failed(lockType, result, nanosTimeout);
    }

    /**
     * Attempts to acquire a shared lock for the given key, denying exclusive
     * locks. If return value is {@link LockResult#isAlreadyOwned owned}, transaction
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
     * @throws DeadlockException if deadlock was detected after waiting the full timeout,
     * unless the timeout is zero
     */
    final LockResult doTryLockShared(long indexId, byte[] key, long nanosTimeout)
        throws DeadlockException
    {
        return doTryLock(TYPE_SHARED, indexId, key, hash(indexId, key), nanosTimeout);
    }

    final LockResult doTryLockShared(long indexId, byte[] key, int hash, long nanosTimeout)
        throws DeadlockException
    {
        return doTryLock(TYPE_SHARED, indexId, key, hash, nanosTimeout);
    }

    /**
     * Attempts to acquire a shared lock for the given key, denying exclusive
     * locks. If return value is {@link LockResult#isAlreadyOwned owned}, transaction
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
     * @throws DeadlockException if deadlock was detected after waiting the full timeout
     */
    final LockResult doLockShared(long indexId, byte[] key, long nanosTimeout)
        throws LockFailureException
    {
        return doLock(TYPE_SHARED, indexId, key, hash(indexId, key), nanosTimeout);
    }

    final LockResult doLockShared(long indexId, byte[] key, int hash, long nanosTimeout)
        throws LockFailureException
    {
        return doLock(TYPE_SHARED, indexId, key, hash, nanosTimeout);
    }

    /**
     * Attempts to acquire an upgradable lock for the given key, denying
     * exclusive and additional upgradable locks. If return value is {@link
     * LockResult#isAlreadyOwned owned}, transaction already owns a strong enough
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
     * @throws DeadlockException if deadlock was detected after waiting the full timeout,
     * unless the timeout is zero
     */
    final LockResult doTryLockUpgradable(long indexId, byte[] key, long nanosTimeout)
        throws DeadlockException
    {
        return doTryLock(TYPE_UPGRADABLE, indexId, key, hash(indexId, key), nanosTimeout);
    }

    final LockResult doTryLockUpgradable(long indexId, byte[] key, int hash, long nanosTimeout)
        throws DeadlockException
    {
        return doTryLock(TYPE_UPGRADABLE, indexId, key, hash, nanosTimeout);
    }

    /**
     * Attempts to acquire an upgradable lock for the given key, denying
     * exclusive and additional upgradable locks. If return value is {@link
     * LockResult#isAlreadyOwned owned}, transaction already owns a strong enough
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
     * @throws DeadlockException if deadlock was detected after waiting the full timeout
     */
    final LockResult doLockUpgradable(long indexId, byte[] key, long nanosTimeout)
        throws LockFailureException
    {
        return doLock(TYPE_UPGRADABLE, indexId, key, hash(indexId, key), nanosTimeout);
    }

    final LockResult doLockUpgradable(long indexId, byte[] key, int hash, long nanosTimeout)
        throws LockFailureException
    {
        return doLock(TYPE_UPGRADABLE, indexId, key, hash, nanosTimeout);
    }

    /**
     * Attempts to acquire an exclusive lock for the given key, denying any
     * additional locks. If return value is {@link LockResult#isAlreadyOwned
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
     * @throws DeadlockException if deadlock was detected after waiting the full timeout,
     * unless the timeout is zero
     */
    final LockResult doTryLockExclusive(long indexId, byte[] key, long nanosTimeout)
        throws DeadlockException
    {
        return doTryLock(TYPE_EXCLUSIVE, indexId, key, hash(indexId, key), nanosTimeout);
    }

    final LockResult doTryLockExclusive(long indexId, byte[] key, int hash, long nanosTimeout)
        throws DeadlockException
    {
        return doTryLock(TYPE_EXCLUSIVE, indexId, key, hash, nanosTimeout);
    }

    /**
     * Attempts to acquire an exclusive lock for the given key, denying any
     * additional locks. If return value is {@link LockResult#isAlreadyOwned owned},
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
     * @throws DeadlockException if deadlock was detected after waiting the full timeout
     */
    final LockResult doLockExclusive(long indexId, byte[] key, long nanosTimeout)
        throws LockFailureException
    {
        return doLock(TYPE_EXCLUSIVE, indexId, key, hash(indexId, key), nanosTimeout);
    }

    final LockResult doLockExclusive(long indexId, byte[] key, int hash, long nanosTimeout)
        throws LockFailureException
    {
        return doLock(TYPE_EXCLUSIVE, indexId, key, hash, nanosTimeout);
    }

    /**
     * Lock acquisition used by undo recovery.
     *
     * @param lock Lock instance to insert, unless another already exists. The mIndexId,
     * mKey, and mHashCode fields must be set.
     */
    final void recoverLock(Lock lock) {
        mManager.getBucket(lock.mHashCode).recoverLock(this, lock);
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
     * Acquire a shared lock, with infinite timeout, but don't push the lock into the owned
     * lock stack. Returns the lock which was acquired, or null if already owned.
     */
    final Lock doLockSharedNoPush(long indexId, byte[] key) throws LockFailureException {
        int hash = hash(indexId, key);
        LockManager.Bucket bucket = mManager.getBucket(hash);

        Lock lock;
        LockResult result;

        bucket.acquireExclusive();
        try {
            lock = bucket.lockAccess(indexId, key, hash);
            result = lock.tryLockShared(bucket, this, -1);
        } finally {
            bucket.releaseExclusive();
        }

        if (!result.isHeld()) {
            throw failed(TYPE_SHARED, result, -1);
        }

        return result == LockResult.ACQUIRED ? lock : null;
    }

    /**
     * Acquire an upgradable lock, with infinite timeout, but don't push the lock into the
     * owned lock stack. Returns the lock which was acquired, or null if already owned.
     */
    final Lock doLockUpgradableNoPush(long indexId, byte[] key) throws LockFailureException {
        int hash = hash(indexId, key);
        LockManager.Bucket bucket = mManager.getBucket(hash);

        Lock lock;
        LockResult result;

        bucket.acquireExclusive();
        try {
            lock = bucket.lockAccess(indexId, key, hash);
            result = lock.tryLockUpgradable(bucket, this, -1);
        } finally {
            bucket.releaseExclusive();
        }

        if (!result.isHeld()) {
            throw failed(TYPE_UPGRADABLE, result, -1);
        }

        return result == LockResult.ACQUIRED ? lock : null;
    }

    /**
     * @param lockType TYPE_SHARED, TYPE_UPGRADABLE, or TYPE_EXCLUSIVE
     */
    @SuppressWarnings("fallthrough")
    LockFailureException failed(int lockType, LockResult result, long nanosTimeout)
        throws DeadlockException
    {
        Lock waitingFor;

        switch (result) {
        case DEADLOCK:
            nanosTimeout = 0;
            // Fallthrough...
        case TIMED_OUT_LOCK:
            waitingFor = mWaitingFor;
            if (waitingFor != null) {
                try {
                    waitingFor.detectDeadlock(this, lockType, nanosTimeout);
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

        if (result.isTimedOut() || result == LockResult.DEADLOCK) {
            Object att = waitingFor == null ? null
                : waitingFor.findOwnerAttachment(this, false, lockType);
            if (result.isTimedOut()) {
                return new LockTimeoutException(nanosTimeout, att);
            } else {
                return new DeadlockException(nanosTimeout, att, true);
            }
        }

        return new LockFailureException();
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
     * @return locked index id, 0 if no locks are held
     */
    public final long lastLockedIndex() {
        Object tailObj = mTailBlock;
        if (tailObj == null) {
            return 0;
        }
        return peek(tailObj).mIndexId;
    }

    /**
     * Returns the key of the last lock acquired, within the current scope.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @return locked key (not cloned), or null if no locks are held
     */
    public final byte[] lastLockedKey() {
        Object tailObj = mTailBlock;
        if (tailObj == null) {
            return null;
        }
        return peek(tailObj).mKey;
    }

    /**
     * Checks if the last acquired lock was against the given index id and key.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @return true if lock matches and was just acquired
     */
    public final boolean wasAcquired(long indexId, byte[] key) {
        Object tailObj = mTailBlock;
        if (tailObj == null) {
            return false;
        }
        Lock lock = peek(tailObj);
        return lock.mIndexId == indexId && Arrays.equals(lock.mKey, key);
    }

    private static Lock peek(Object tailObj) {
        return (tailObj instanceof Lock lock) ? lock : ((Block) tailObj).last();
    }

    /**
     * Fully releases the last lock or group acquired, within the current scope. If the last
     * lock operation was an upgrade, for a lock not immediately acquired, unlock is not
     * allowed. Instead, an IllegalStateException is thrown.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @throws IllegalStateException if no locks held, or if exclusive lock held, or if
     * crossing a scope boundary, or if unlocking a non-immediate upgrade
     */
    public final void unlock() {
        Object tailObj = mTailBlock;
        if (tailObj == null) {
            throw new IllegalStateException("No locks held");
        }
        if (tailObj instanceof Lock lock) {
            ParentScope parent = mParentScope;
            if (parent != null && parent.mTailBlock == tailObj) {
                throw new IllegalStateException("Cannot cross a scope boundary");
            }
            mManager.unlock(this, lock);
            mTailBlock = null;
        } else {
            Block.unlockLast((Block) tailObj, this);
        }
    }

    /**
     * Fully releases the last lock or group acquired, within the current scope. If the last
     * lock operation was an upgrade, for a lock not immediately acquired, unlock is not
     * allowed. Instead, an IllegalStateException is thrown.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @throws IllegalStateException if no locks held, or if crossing a scope boundary, or if
     * unlocking a non-immediate upgrade
     */
    final void doUnlock() {
        Object tailObj = mTailBlock;
        if (tailObj == null) {
            throw new IllegalStateException("No locks held");
        }
        if (tailObj instanceof Lock lock) {
            ParentScope parent = mParentScope;
            if (parent != null && parent.mTailBlock == tailObj) {
                throw new IllegalStateException("Cannot cross a scope boundary");
            }
            mManager.doUnlock(this, lock);
            mTailBlock = null;
        } else {
            Block.doUnlockLast((Block) tailObj, this);
        }
    }

    /**
     * Releases the last lock or group acquired, within the current scope, retaining a shared
     * lock. If the last lock operation was an upgrade, for a lock not immediately acquired,
     * unlock is not allowed. Instead, an IllegalStateException is thrown.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @throws IllegalStateException if no locks held, or if exclusive lock held, or if
     * crossing a scope boundary, or if too many shared locks, or if unlocking a non-immediate
     * upgrade
     */
    public final void unlockToShared() {
        Object tailObj = mTailBlock;
        if (tailObj == null) {
            throw new IllegalStateException("No locks held");
        }
        if (tailObj instanceof Lock lock) {
            ParentScope parent = mParentScope;
            if (parent != null && parent.mTailBlock == tailObj) {
                throw new IllegalStateException("Cannot cross a scope boundary");
            }
            mManager.unlockToShared(this, lock);
        } else {
            Block.unlockLastToShared((Block) tailObj, this);
        }
    }

    /**
     * Releases the last lock or group acquired, within the current scope, retaining a shared
     * lock. If the last lock operation was an upgrade, for a lock not immediately acquired,
     * unlock is not allowed. Instead, an IllegalStateException is thrown.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @throws IllegalStateException if no locks held, or if crossing a scope boundary, or if
     * too many shared locks, or if unlocking a non-immediate upgrade
     */
    final void doUnlockToShared() {
        Object tailObj = mTailBlock;
        if (tailObj == null) {
            throw new IllegalStateException("No locks held");
        }
        if (tailObj instanceof Lock lock) {
            ParentScope parent = mParentScope;
            if (parent != null && parent.mTailBlock == tailObj) {
                throw new IllegalStateException("Cannot cross a scope boundary");
            }
            mManager.doUnlockToShared(this, lock);
        } else {
            Block.doUnlockLastToShared((Block) tailObj, this);
        }
    }

    /**
     * Releases the last lock or group acquired or upgraded, within the current scope,
     * retaining an upgradable lock.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @throws IllegalStateException if no locks held, or if crossing a scope boundary, or if
     * last lock is shared
     */
    final void doUnlockToUpgradable() {
        Object tailObj = mTailBlock;
        if (tailObj == null) {
            throw new IllegalStateException("No locks held");
        }
        if (tailObj instanceof Lock lock) {
            ParentScope parent = mParentScope;
            if (parent != null && parent.mTailBlock == tailObj) {
                throw new IllegalStateException("Cannot cross a scope boundary");
            }
            mManager.doUnlockToUpgradable(this, lock);
        } else {
            Block.doUnlockLastToUpgradable((Block) tailObj, this);
        }
    }

    /**
     * Combines the last lock acquired or upgraded into a group which can be unlocked together.
     *
     * <p><i>Note: This method is intended for advanced use cases.</i>
     *
     * @throws IllegalStateException if combining an acquire with an upgrade
     */
    public final void unlockCombine() {
        if (mTailBlock instanceof Block block) {
            Block.unlockCombine(block);
        }
    }

    /**
     * @return new parent scope
     */
    final ParentScope scopeEnter() {
        var parent = new ParentScope();
        parent.mParentScope = mParentScope;
        Object tailObj = mTailBlock;
        parent.mTailBlock = tailObj;
        if (tailObj instanceof Block block) {
            parent.mTailBlockSize = block.mSize;
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
            if (tailObj instanceof Block block) {
                parent.mTailBlockSize = block.mSize;
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
            if (tailObj instanceof Lock lock) {
                mManager.doUnlock(this, lock);
                mTailBlock = null;
            } else {
                var tail = (Block) tailObj;
                if (tail != null) {
                    do {
                        tail.unlockToSavepoint(this, 0);
                        tail = tail.prev();
                    } while (tail != null);
                    mTailBlock = null;
                }
            }
        } else if (parentTailObj instanceof Lock) {
            Object tailObj = mTailBlock;
            if (tailObj instanceof Block tail) {
                while (true) {
                    Block prev = tail.prev();
                    if (prev == null) {
                        tail.unlockToSavepoint(this, 1);
                        break;
                    }
                    tail.unlockToSavepoint(this, 0);
                    tail = prev;
                }
                mTailBlock = tail;
            }
        } else {
            var tail = (Block) mTailBlock;
            while (tail != parentTailObj) {
                tail.unlockToSavepoint(this, 0);
                tail = tail.prev();
            }
            tail.unlockToSavepoint(this, parent.mTailBlockSize);
            mTailBlock = tail;
        }
    }

    /**
     * Releases all locks until a prepare lock is found, keeping it. If no prepare is found,
     * then all locks are released.
     *
     * @throws IllegalStateException if in a scope
     */
    final void unlockToPrepare() {
        if (mParentScope != null) {
            throw new IllegalStateException();
        }

        while (true) {
            Object tailObj = mTailBlock;
            if (tailObj instanceof Lock lock) {
                if (!lock.isPrepareLock()) {
                    mManager.doUnlock(this, lock);
                    mTailBlock = null;
                }
                return;
            }
            var tail = (Block) tailObj;
            if (tail == null || tail.last().isPrepareLock()) {
                return;
            }
            Block.doUnlockLast(tail, this);
        }
    }

    /**
     * Releases all locks except the prepare lock, which should be on the top of the stack.
     *
     * @throws IllegalStateException if in a scope or if the prepare lock isn't the top
     */
    final void unlockAllExceptPrepare() {
        if (mParentScope != null) {
            throw new IllegalStateException();
        }

        Object tailObj = mTailBlock;

        if (tailObj instanceof Lock lock) {
            if (lock.isPrepareLock()) {
                // Nothing to do.
                return;
            }
        } else if (tailObj != null) {
            Lock lock = ((Block) tailObj).removeLastIfPrepare(this);
            scopeUnlockAll();
            mTailBlock = lock;
            return;
        }

        throw new IllegalStateException(String.valueOf(tailObj));
    }

    /**
     * Releases all non-exclusive locks which are held. Assumes that no parent scope exists.
     */
    final void unlockNonExclusive() {
        transferExclusive(this);
    }

    /**
     * Transfers all exclusive locks to a new owner and releases the rest, breaking apart all
     * combined locks. Assumes that no parent scope exists.
     *
     * @param newOwner must have no locks as a pre-condition; will own all the exclusive locks
     * when this method returns
     */
    final void transferExclusive(Locker newOwner) {
        Object tailObj = mTailBlock;

        if (tailObj == null) {
            return;
        }

        if (tailObj instanceof Lock lock) {
            if (lock.mLockCount != ~0) {
                mManager.doUnlock(this, lock);
            } else if (newOwner == this) {
                return;
            } else {
                mManager.takeLockOwnership(lock, newOwner);
                cTailBlockHandle.setRelease(newOwner, lock);
            }
            mTailBlock = null;
            return;
        }

        var tail = (Block) tailObj;
        Block last = null;
        do {
            int size = tail.transferExclusive(this, newOwner);
            if (size <= 0) {
                tail = tail.prev();
            } else {
                if (last == null) {
                    cTailBlockHandle.setRelease(newOwner, tail);
                } else {
                    last.mPrev = tail;
                }
                last = tail;
                tail = tail.prev();
            }
        } while (tail != null);

        if (newOwner != this) {
            mTailBlock = null;
        }
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

    final void push(Lock lock) {
        Object tailObj = mTailBlock;
        if (tailObj == null) {
            cTailBlockHandle.setRelease(this, lock);
        } else if (tailObj instanceof Lock tailLock) {
            cTailBlockHandle.setRelease(this, new Block(tailLock, lock));
        } else {
            ((Block) tailObj).pushLock(this, lock, 0);
        }
    }

    final void pushUpgrade(Lock lock) {
        Object tailObj = mTailBlock;
        if (tailObj instanceof Lock tailLock) {
            // Don't push lock upgrade if it applies to the last acquisition
            // within this scope. This is required for unlockLast.
            if (tailObj != lock || mParentScope != null) {
                var block = new Block(tailLock, lock);
                block.secondUpgrade();
                cTailBlockHandle.setRelease(this, block);
            }
        } else {
            ((Block) tailObj).pushLock(this, lock, 1L << 63);
        }
    }

    /**
     * Returns a lock which matches the index and predicate and is held exclusive or
     * upgradable. This method is intended to be called by a thread which is independent of the
     * one which is allowed to modify the Locker.
     *
     * @return conflicting lock or null if none found
     */
    final Lock findAnyConflict(long indexId, RowPredicate predicate) {
        Object tailObj = cTailBlockHandle.getAcquire(this);

        if (tailObj != null) {
            if (tailObj instanceof Lock lock) {
                if (lock.mIndexId == indexId && predicate.test(lock.mKey) &&
                    ((int) Lock.cLockCountHandle.getAcquire(lock)) < 0)
                {
                    return lock;
                }
            } else {
                var block = (Block) tailObj;
                do {
                    Lock lock = block.findAnyConflict(indexId, predicate);
                    if (lock != null) {
                        return lock;
                    }
                } while ((block = block.prevAcquire()) != null);
            }
        }

        return null;
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
        // Limited by number of bits available in mUpgrades and mUnlockGroup.
        private static final int HIGHEST_BLOCK_CAPACITY = 64;

        private final Lock[] mLocks;
        private long mUpgrades;
        // Size must always be at least 1.
        private int mSize;
        private long mUnlockGroup;

        private Block mPrev;

        // Must use these when pushing locks, as needed by the findAnyConflict method.
        private static final VarHandle cPrevHandle, cSizeHandle;

        static {
            try {
                var lookup = MethodHandles.lookup();
                cPrevHandle = lookup.findVarHandle(Block.class, "mPrev", Block.class);
                cSizeHandle = lookup.findVarHandle(Block.class, "mSize", int.class);
            } catch (Throwable e) {
                throw Utils.rethrow(e);
            }
        }

        Block(Lock first, Lock second) {
            var locks = new Lock[FIRST_BLOCK_CAPACITY];
            locks[0] = first;
            locks[1] = second;
            mLocks = locks;
            cSizeHandle.setRelease(this, 2);
        }

        void secondUpgrade() {
            mUpgrades = 1L << 62;
        }

        /**
         * @param upgrade {@literal 0 or 1L << 63}
         */
        private Block(Block prev, Lock first, long upgrade) {
            int capacity = prev.mLocks.length;
            if (capacity < HIGHEST_BLOCK_CAPACITY) {
                capacity <<= 1;
            }
            (mLocks = new Lock[capacity])[0] = first;
            mUpgrades = upgrade;
            cSizeHandle.setRelease(this, 1);
            cPrevHandle.setRelease(this, prev);
        }

        /**
         * @param upgrade {@literal 0 or 1L << 63}
         */
        void pushLock(Locker locker, Lock lock, long upgrade) {
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
                mUpgrades |= upgrade >>> size;
                cSizeHandle.setRelease(this, size + 1);
            } else {
                cTailBlockHandle.setRelease(locker, new Block(this, lock, upgrade));
            }
        }

        Lock last() {
            return mLocks[mSize - 1];
        }

        /**
         * Note: Caller must not access this Block again if the resulting size is zero.
         *
         * @throws IllegalStateException if the last is a prepare lock
         */
        Lock removeLastIfPrepare(Locker locker) {
            int size = mSize - 1;
            Lock lock = mLocks[size];
            if (!lock.isPrepareLock()) {
                throw new IllegalStateException();
            } else {
                mLocks[size] = null;
                if (size == 0) {
                    locker.mTailBlock = mPrev;
                } else {
                    mSize = size;
                }
            }
            return lock;
        }

        static void unlockLast(Block block, Locker locker) {
            int size = block.mSize;
            while (true) {
                size--;

                long upgrades = block.mUpgrades;
                long mask = (1L << 63) >>> size;
                if ((upgrades & mask) != 0) {
                    throw new IllegalStateException("Cannot unlock non-immediate upgrade");
                }

                Lock[] locks = block.mLocks;
                Lock lock = locks[size];
                block.parentCheck(locker, lock);

                locker.mManager.unlock(locker, lock);

                // Only pop lock if unlock succeeded.
                locks[size] = null;

                if (size == 0) {
                    Block prev = block.mPrev;
                    locker.mTailBlock = prev;
                    if ((block.mUnlockGroup & mask) == 0) {
                        return;
                    }
                    block = prev;
                    size = block.mSize;
                } else {
                    block.mUpgrades = upgrades & ~mask;
                    block.mSize = size;
                    long unlockGroup = block.mUnlockGroup;
                    if ((unlockGroup & mask) == 0) {
                        return;
                    }
                    block.mUnlockGroup = unlockGroup & ~mask;
                }
            }
        }

        static void doUnlockLast(Block block, Locker locker) {
            int size = block.mSize;
            while (true) {
                size--;

                long upgrades = block.mUpgrades;
                long mask = (1L << 63) >>> size;
                if ((upgrades & mask) != 0) {
                    throw new IllegalStateException("Cannot unlock non-immediate upgrade");
                }

                Lock[] locks = block.mLocks;
                Lock lock = locks[size];
                block.parentCheck(locker, lock);

                locker.mManager.doUnlock(locker, lock);

                // Only pop lock if unlock succeeded.
                locks[size] = null;

                if (size == 0) {
                    Block prev = block.mPrev;
                    locker.mTailBlock = prev;
                    if ((block.mUnlockGroup & mask) == 0) {
                        return;
                    }
                    block = prev;
                    size = block.mSize;
                } else {
                    block.mUpgrades = upgrades & ~mask;
                    block.mSize = size;
                    long unlockGroup = block.mUnlockGroup;
                    if ((unlockGroup & mask) == 0) {
                        return;
                    }
                    block.mUnlockGroup = unlockGroup & ~mask;
                }
            }
        }

        static void unlockLastToShared(Block block, Locker locker) {
            int size = block.mSize;
            while (true) {
                size--;

                long mask = (1L << 63) >>> size;
                if ((block.mUpgrades & mask) != 0) {
                    throw new IllegalStateException("Cannot unlock non-immediate upgrade");
                }

                Lock lock = block.mLocks[size];
                block.parentCheck(locker, lock);

                locker.mManager.unlockToShared(locker, lock);

                if ((block.mUnlockGroup & mask) == 0) {
                    return;
                }

                if (size == 0) {
                    block = block.mPrev;
                    size = block.mSize;
                }
            }
        }

        static void doUnlockLastToShared(Block block, Locker locker) {
            int size = block.mSize;
            while (true) {
                size--;

                long mask = (1L << 63) >>> size;
                if ((block.mUpgrades & mask) != 0) {
                    throw new IllegalStateException("Cannot unlock non-immediate upgrade");
                }

                Lock lock = block.mLocks[size];
                block.parentCheck(locker, lock);

                locker.mManager.doUnlockToShared(locker, lock);

                if ((block.mUnlockGroup & mask) == 0) {
                    return;
                }

                if (size == 0) {
                    block = block.mPrev;
                    size = block.mSize;
                }
            }
        }

        static void doUnlockLastToUpgradable(Block block, Locker locker) {
            int size = block.mSize;
            while (true) {
                size--;

                Lock[] locks = block.mLocks;
                Lock lock = locks[size];
                block.parentCheck(locker, lock);

                locker.mManager.doUnlockToUpgradable(locker, lock);

                long upgrades = block.mUpgrades;
                long mask = (1L << 63) >>> size;

                if ((upgrades & mask) == 0) {
                    if ((block.mUnlockGroup & mask) == 0) {
                        return;
                    }
                    if (size == 0) {
                        block = block.mPrev;
                        size = block.mSize;
                    }
                } else {
                    // Pop upgrade off stack, but only if unlock succeeded.
                    locks[size] = null;

                    if (size == 0) {
                        Block prev = block.mPrev;
                        locker.mTailBlock = prev;
                        if ((block.mUnlockGroup & mask) == 0) {
                            return;
                        }
                        block = prev;
                        size = block.mSize;
                    } else {
                        block.mUpgrades = upgrades & ~mask;
                        block.mSize = size;
                        long unlockGroup = block.mUnlockGroup;
                        if ((unlockGroup & mask) == 0) {
                            return;
                        }
                        block.mUnlockGroup = unlockGroup & ~mask;
                    }
                }
            }
        }

        static void unlockCombine(Block block) {
            while (true) {
                // Find the combine position, by searching backwards for a zero bit.

                int size = block.mSize - 1;

                // Set all unused rightmost bits to 1.
                long mask = block.mUnlockGroup | (~(1L << 63) >>> size);

                // Hacker's Delight section 2-1. Create word with a single 1-bit at the
                // position of the rightmost 0-bit, producing 0 if none.
                mask = ~mask & (mask + 1);

                if (mask == 0) {
                    block = block.mPrev;
                    continue;
                }

                long upgrades = block.mUpgrades;

                long prevMask;
                if (size != 0) {
                    prevMask = upgrades >> 1;
                } else {
                    Block prev = block.mPrev;
                    if (prev == null) {
                        // Group of one, so nothing to do.
                        return;
                    }
                    prevMask = prev.mUpgrades << (prev.mSize - 1);
                }

                if (((upgrades ^ prevMask) & mask) != 0) {
                    throw new IllegalStateException("Cannot combine an acquire with an upgrade");
                }

                if (mask < 0 && block.mPrev == null) {
                    // Nothing left to combine with.
                    return;
                }

                block.mUnlockGroup |= mask;
                return;
            }
        }

        private void parentCheck(Locker locker, Lock lock) throws IllegalStateException {
            ParentScope parent = locker.mParentScope;
            if (parent != null) {
                Object parentTail = parent.mTailBlock;
                if (parentTail == lock || (parentTail == this && parent.mTailBlockSize == mSize)) {
                    throw new IllegalStateException("Cannot cross a scope boundary");
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
                long mask = (1L << 63) >>> size;
                long upgrades = mUpgrades;
                while (true) {
                    Lock lock = locks[size];
                    if ((upgrades & mask) != 0) {
                        manager.doUnlockToUpgradable(locker, lock);
                    } else {
                        manager.doUnlock(locker, lock);
                    }
                    locks[size] = null;
                    if (size == targetSize) {
                        break;
                    }
                    size--;
                    mask <<= 1;
                }
                mUpgrades = upgrades & ~(~0L >>> size);
                mSize = size;
            }
        }

        /**
         * @return remaining size; caller MUST pop and discard the block if zero
         */
        int transferExclusive(Locker locker, Locker newOwner) {
            final Lock[] locks = mLocks;
            final LockManager manager = locker.mManager;
            final int size = mSize;

            long upgrades = mUpgrades;
            int newSize = 0;

            for (int i=0; i<size; i++) {
                Lock lock = locks[i];
                if (lock.mLockCount != ~0) {
                    manager.doUnlock(locker, lock);
                } else if (upgrades >= 0) { // don't track lock twice if it was upgraded
                    if (newSize != i) {
                        locks[newSize] = lock;
                    }
                    if (newOwner != locker) {
                        manager.takeLockOwnership(lock, newOwner);
                    }
                    newSize++;
                }
                upgrades <<= 1;
            }

            if (newSize != size) {
                for (int i=newSize; i<size; i++) {
                    locks[i] = null;
                }
                mUpgrades = 0;
                mSize = newSize;
                mUnlockGroup = 0;
            }

            return newSize;
        }

        Lock findAnyConflict(long indexId, RowPredicate predicate) {
            Lock[] locks = mLocks;
            for (int i = (int) cSizeHandle.getAcquire(this); --i >= 0; ) {
                Lock lock = locks[i];
                if (lock != null && lock.mIndexId == indexId && predicate.test(lock.mKey) &&
                    ((int) Lock.cLockCountHandle.getAcquire(lock)) < 0)
                {
                    return lock;
                }
            }
            return null;
        }

        Block prev() {
            return mPrev;
        }

        Block prevAcquire() {
            return (Block) cPrevHandle.getAcquire(this);
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
