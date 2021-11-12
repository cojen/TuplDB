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

import java.util.Arrays;

import org.cojen.tupl.DeadlockException;
import org.cojen.tupl.LockResult;

import org.cojen.tupl.util.Latch;
import org.cojen.tupl.util.LatchCondition;

import static org.cojen.tupl.LockResult.*;

/**
 * Partially reentrant shared/upgradable/exclusive lock, with fair acquisition
 * methods. Locks are owned by Lockers, not Threads. Implementation relies on
 * latching for mutual exclusion, but condition variable logic is used for
 * transferring ownership between Lockers.
 *
 * @author Brian S O'Neill
 * @see LockManager
 */
/*P*/
class Lock {
    long mIndexId;
    byte[] mKey;
    int mHashCode;

    // Next entry in LockManager hash collision chain.
    Lock mLockNext;

    // 0xxx...  shared locks held (up to (2^31)-2)
    // 1xxx...  upgradable and shared locks held (up to (2^31)-2)
    // 1111...  exclusive lock held (~0)
    int mLockCount;

    // Exclusive or upgradable locker.
    Locker mOwner;

    // Locker instance if one shared locker, or else a hashtable for more. Field is re-used
    // to indicate when an exclusive lock has ghosted an entry, which should be deleted when
    // the transaction commits. A C-style union type would be handy. Object is a GhostFrame if
    // entry is ghosted.
    private Object mSharedLockersObj;

    // Waiters for upgradable lock. Contains only regular waiters.
    LatchCondition mQueueU;

    // Waiters for shared and exclusive locks. Contains regular and shared waiters.
    LatchCondition mQueueSX;

    static final VarHandle cLockCountHandle;

    static {
        try {
            var lookup = MethodHandles.lookup();
            cLockCountHandle = lookup.findVarHandle(Lock.class, "mLockCount", int.class);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    /**
     * @param locker optional locker
     */
    final boolean isAvailable(Locker locker) {
        return mLockCount >= 0 || mOwner == locker;
    }

    /**
     * Called with any latch held, which is retained.
     *
     * @return UNOWNED, OWNED_SHARED, OWNED_UPGRADABLE, or OWNED_EXCLUSIVE
     */
    final LockResult check(Locker locker) {
        int count = mLockCount;
        return mOwner == locker
            ? (count == ~0 ? OWNED_EXCLUSIVE : OWNED_UPGRADABLE)
            : ((count != 0 && isSharedLocker(locker)) ? OWNED_SHARED : UNOWNED);
    }

    final boolean isPrepareLock() {
        return mIndexId == Tree.PREPARED_TXNS_ID;
    }

    /**
     * Called with exclusive latch held, which is retained. If return value is TIMED_OUT_LOCK,
     * the locker's mWaitingFor field is set to this Lock as a side-effect.
     *
     * @return INTERRUPTED, TIMED_OUT_LOCK, ACQUIRED, OWNED_SHARED,
     * OWNED_UPGRADABLE, or OWNED_EXCLUSIVE
     * @throws IllegalStateException if too many shared locks
     */
    final LockResult tryLockShared(Latch latch, Locker locker, long nanosTimeout) {
        if (mOwner == locker) {
            return mLockCount == ~0 ? OWNED_EXCLUSIVE : OWNED_UPGRADABLE;
        }

        LatchCondition queueSX = mQueueSX;
        if (queueSX != null) {
            if (isSharedLocker(locker)) {
                return OWNED_SHARED;
            }
            if (nanosTimeout == 0) {
                locker.mWaitingFor = this;
                return TIMED_OUT_LOCK;
            }
        } else {
            int count = mLockCount;
            if (count == ~0) {
                if (nanosTimeout == 0) {
                    locker.mWaitingFor = this;
                    return TIMED_OUT_LOCK;
                }
                mQueueSX = queueSX = new LatchCondition();
            } else if (count != 0 && isSharedLocker(locker)) {
                return OWNED_SHARED;
            } else {
                addSharedLocker(count, locker);
                return ACQUIRED;
            }
        }

        locker.mWaitingFor = this;

        // Await for shared lock.
        int w = queueSX.awaitShared(latch, nanosTimeout);
        queueSX = mQueueSX;

        if (queueSX == null) {
            // Assume LockManager was closed.
            locker.mWaitingFor = null;
            return INTERRUPTED;
        }

        if (queueSX.isEmpty()) {
            // Indicate that last signal has been consumed, and also free memory.
            mQueueSX = null;
        }

        if (w > 0) {
            locker.mWaitingFor = null;
            // After consuming one signal, next shared waiter must be signaled, and so on. Do
            // this before calling addSharedLocker, in case it throws an exception.
            queueSX.signalShared(latch);
            addSharedLocker(mLockCount, locker);
            return ACQUIRED;
        } else if (w == 0) {
            return TIMED_OUT_LOCK;
        } else {
            locker.mWaitingFor = null;
            return INTERRUPTED;
        }
    }

    /**
     * Called with exclusive latch held, which is retained. If return value is TIMED_OUT_LOCK,
     * the locker's mWaitingFor field is set to this Lock as a side-effect.
     *
     * @return ILLEGAL, INTERRUPTED, TIMED_OUT_LOCK, ACQUIRED,
     * OWNED_UPGRADABLE, or OWNED_EXCLUSIVE
     */
    final LockResult tryLockUpgradable(Latch latch, Locker locker, long nanosTimeout) {
        if (mOwner == locker) {
            return mLockCount == ~0 ? OWNED_EXCLUSIVE : OWNED_UPGRADABLE;
        }

        int count = mLockCount;
        if (count != 0 && isSharedLocker(locker)) {
            if (!locker.canAttemptUpgrade(count)) {
                return ILLEGAL;
            }
            if (count > 0) {
                // Give the impression that lock was always held upgradable. This prevents
                // pushing the lock into the locker twice.
                mLockCount = (count - 1) | 0x80000000;
                mOwner = locker;
                return OWNED_UPGRADABLE;
            }
        }

        LatchCondition queueU = mQueueU;
        if (queueU != null) {
            if (nanosTimeout == 0) {
                locker.mWaitingFor = this;
                return TIMED_OUT_LOCK;
            }
        } else {
            if (count >= 0) {
                mLockCount = count | 0x80000000;
                mOwner = locker;
                return ACQUIRED;
            }
            if (nanosTimeout == 0) {
                locker.mWaitingFor = this;
                return TIMED_OUT_LOCK;
            }
            mQueueU = queueU = new LatchCondition();
        }

        locker.mWaitingFor = this;

        // Await for upgradable lock.
        int w = queueU.await(latch, nanosTimeout);
        queueU = mQueueU;

        if (queueU == null) {
            // Assume LockManager was closed.
            locker.mWaitingFor = null;
            return INTERRUPTED;
        }

        if (queueU.isEmpty()) {
            // Indicate that last signal has been consumed, and also free memory.
            mQueueU = null;
        }

        if (w > 0) {
            locker.mWaitingFor = null;
            mLockCount |= 0x80000000;
            mOwner = locker;
            return ACQUIRED;
        } else if (w == 0) {
            return TIMED_OUT_LOCK;
        } else {
            locker.mWaitingFor = null;
            return INTERRUPTED;
        }
    }

    /**
     * Called with exclusive latch held, which is retained. If return value is TIMED_OUT_LOCK,
     * the locker's mWaitingFor field is set to this Lock as a side-effect.
     *
     * @return ILLEGAL, INTERRUPTED, TIMED_OUT_LOCK, ACQUIRED, UPGRADED, or
     * OWNED_EXCLUSIVE
     */
    final LockResult tryLockExclusive(Latch latch, Locker locker, long nanosTimeout) {
        final LockResult ur = tryLockUpgradable(latch, locker, nanosTimeout);
        if (!ur.isHeld() || ur == OWNED_EXCLUSIVE) {
            return ur;
        }

        LatchCondition queueSX = mQueueSX;
        quick: {
            if (queueSX == null) {
                if (mLockCount == 0x80000000) {
                    mLockCount = ~0;
                    return ur == OWNED_UPGRADABLE ? UPGRADED : ACQUIRED;
                } else if (nanosTimeout != 0) {
                    mQueueSX = queueSX = new LatchCondition();
                    break quick;
                }
            } else if (nanosTimeout != 0) {
                break quick;
            }
            if (ur == ACQUIRED) {
                unlockUpgradable(latch);
            }
            locker.mWaitingFor = this;
            return TIMED_OUT_LOCK;
        }

        locker.mWaitingFor = this;

        // Await for exclusive lock.
        int w = queueSX.await(latch, nanosTimeout);
        queueSX = mQueueSX;

        if (queueSX == null) {
            // Assume LockManager was closed.
            locker.mWaitingFor = null;
            return INTERRUPTED;
        }

        if (queueSX.isEmpty()) {
            // Indicate that last signal has been consumed, and also free memory.
            mQueueSX = null;
        }

        if (w > 0) {
            locker.mWaitingFor = null;
            mLockCount = ~0;
            return ur == OWNED_UPGRADABLE ? UPGRADED : ACQUIRED;
        } else {
            if (ur == ACQUIRED) {
                unlockUpgradable(latch);
            }
            if (w == 0) {
                return TIMED_OUT_LOCK;
            } else {
                locker.mWaitingFor = null;
                return INTERRUPTED;
            }
        }
    }

    /**
     * Called internally to unlock an upgradable lock which was just
     * acquired. Implementation is a just a smaller version of the regular
     * unlock method. It doesn't have to deal with ghosts.
     */
    private void unlockUpgradable(Latch latch) {
        mOwner = null;
        LatchCondition queueU = mQueueU;
        if (queueU != null) {
            // Signal at most one upgradable lock waiter.
            queueU.signal(latch);
        }
        mLockCount &= 0x7fffffff;
    }

    /**
     * Called with exclusive latch held, which is released unless an exception is thrown.
     *
     * @param bucket only released if no exception is thrown
     * @throws IllegalStateException if lock not held or if exclusive lock held
     */
    final void unlock(Locker locker, LockManager.Bucket bucket) {
        if (mOwner == locker) {
            doUnlockOwned(bucket);
        } else {
            doUnlockShared(locker, bucket);
        }
    }

    /**
     * Called with exclusive latch held, which is released unless an exception is thrown.
     *
     * @param bucket briefly released and re-acquired for deleting a ghost
     * @throws IllegalStateException if lock not held
     */
    final void doUnlock(Locker locker, LockManager.Bucket bucket) {
        if (mOwner == locker) {
            doUnlockOwnedUnrestricted(bucket);
        } else {
            doUnlockShared(locker, bucket);
        }
    }

    /**
     * Called with exclusive latch held, which is released unless an exception is thrown.
     *
     * @param bucket briefly released and re-acquired for deleting a ghost
     * @throws IllegalStateException if lock not held or if exclusive lock held
     */
    private void doUnlockOwned(LockManager.Bucket bucket) {
        int count = mLockCount;

        if (count != ~0) {
            // Unlocking an upgradable lock.
            mOwner = null;
            LatchCondition queueU = mQueueU;
            if ((mLockCount = count & 0x7fffffff) == 0 && queueU == null && mQueueSX == null) {
                // Lock is now completely unused.
                bucket.remove(this);
            } else if (queueU != null) {
                // Signal at most one upgradable lock waiter.
                queueU.signal(bucket);
            }
        } else {
            // Unlocking an exclusive lock.
            throw unlockFail();
        }

        bucket.releaseExclusive();
    }

    /**
     * Called with exclusive latch held, which is released unless an exception is thrown.
     *
     * @param bucket briefly released and re-acquired for deleting a ghost
     */
    protected void doUnlockOwnedUnrestricted(LockManager.Bucket bucket) {
        int count = mLockCount;

        unlock:
        if (count != ~0) {
            // Unlocking an upgradable lock.
            mOwner = null;
            LatchCondition queueU = mQueueU;
            if ((mLockCount = count & 0x7fffffff) == 0 && queueU == null && mQueueSX == null) {
                // Lock is now completely unused.
                bucket.remove(this);
            } else if (queueU != null) {
                // Signal at most one upgradable lock waiter.
                queueU.signal(bucket);
            }
        } else {
            // Unlocking an exclusive lock.
            deleteGhost(bucket);
            mOwner = null;
            mLockCount = 0;
            // The call to deleteGhost might have released and re-acquired the latch guarding
            // the state of this lock, so must obtain the latest references to the queues.
            LatchCondition queueU = mQueueU;
            LatchCondition queueSX = mQueueSX;
            if (queueU != null) {
                // Signal at most one upgradable lock waiter.
                queueU.signal(bucket);
                if (queueSX == null) {
                    break unlock;
                }
            } else if (queueSX == null) {
                // Lock is now completely unused.
                bucket.remove(this);
                break unlock;
            }
            // Signal at most one shared lock waiter. There aren't any exclusive lock waiters,
            // because they would need to acquire the upgradable lock first, which was held.
            queueSX.signal(bucket);
        }

        bucket.releaseExclusive();
    }

    /**
     * @param bucket never released, even if an exception is thrown
     * @throws IllegalStateException if lock not held
     */
    private void doUnlockShared(Locker locker, LockManager.Bucket bucket) {
        int count = mLockCount;

        unlock: {
            check: {
                if ((count & 0x7fffffff) != 0) {
                    Object sharedObj = mSharedLockersObj;
                    if (sharedObj == locker) {
                        mSharedLockersObj = null;
                        break check;
                    } else if (sharedObj instanceof LockerHTEntry[] entries) {
                        if (lockerHTremove(entries, locker)) {
                            if ((count & 0x7fffffff) == 1) {
                                mSharedLockersObj = null;
                            }
                            break check;
                        }
                    }
                }

                if (isClosed(locker)) {
                    break unlock;
                }

                throw new IllegalStateException("Lock not held");
            }

            mLockCount = --count;

            LatchCondition queueSX = mQueueSX;
            if (count == 0x80000000) {
                if (queueSX != null) {
                    // Signal any exclusive lock waiter. Queue shouldn't contain any shared
                    // lock waiters, because no exclusive lock is held. In case there are any,
                    // signal them instead.
                    queueSX.signal(bucket);
                }
            } else if (count == 0 && queueSX == null && mQueueU == null) {
                // Lock is now completely unused.
                bucket.remove(this);
            }
        }

        bucket.releaseExclusive();
    }

    /**
     * Called with exclusive latch held, which is released unless an exception is thrown.
     *
     * @param latch only released if no exception is thrown
     * @throws IllegalStateException if lock not held, if exclusive lock held, or if too many
     * shared locks
     */
    final void unlockToShared(Locker locker, Latch latch) {
        if (mOwner == locker) {
            int count = mLockCount;

            if (count != ~0) {
                // Unlocking upgradable lock into shared. Retain upgradable lock if too many
                // shared locks are held (IllegalStateException is thrown).
                addSharedLocker(count & 0x7fffffff, locker);
            } else {
                // Unlocking exclusive lock into shared.
                throw unlockFail();
            }

            mOwner = null;

            // Signal at most one upgradable lock waiter.
            LatchCondition queueU = mQueueU;
            if (queueU != null) {
                queueU.signal(latch);
            }
        } else if ((mLockCount == 0 || !isSharedLocker(locker)) && !isClosed(locker)) {
            throw new IllegalStateException("Lock not held");
        }

        latch.releaseExclusive();
    }

    /**
     * Called with exclusive latch held, which is released unless an exception is thrown.
     *
     * @param latch briefly released and re-acquired for deleting a ghost
     * @throws IllegalStateException if lock not held or if too many shared locks
     */
    final void doUnlockToShared(Locker locker, Latch latch) {
        ownerCheck: if (mOwner == locker) {
            LatchCondition queueU;
            int count = mLockCount;

            if (count != ~0) {
                // Unlocking upgradable lock into shared. Retain upgradable lock if too many
                // shared locks are held (IllegalStateException is thrown).
                addSharedLocker(count & 0x7fffffff, locker);
                mOwner = null;
                queueU = mQueueU;
            } else {
                // Unlocking exclusive lock into shared.
                deleteGhost(latch);
                doAddSharedLocker(1, locker);
                mOwner = null;
                // The call to deleteGhost might have released and re-acquired the latch guarding
                // the state of this lock, so must obtain the latest references to the queues.
                queueU = mQueueU;
                LatchCondition queueSX = mQueueSX;
                if (queueSX != null) {
                    if (queueU != null) {
                        // Signal at most one upgradable lock waiter, and keep the latch.
                        queueU.signal(latch);
                    }
                    // Signal the first shared lock waiter. Queue doesn't contain any exclusive
                    // lock waiters, because they would need to acquire upgradable lock first,
                    // which was held.
                    queueSX.signal(latch);
                    break ownerCheck;
                }
            }

            // Signal at most one upgradable lock waiter.
            if (queueU != null) {
                queueU.signal(latch);
            }
        } else if ((mLockCount == 0 || !isSharedLocker(locker)) && !isClosed(locker)) {
            throw new IllegalStateException("Lock not held");
        }

        latch.releaseExclusive();
    }

    /**
     * Called with exclusive latch held, which is released unless an exception is thrown.
     *
     * @param latch briefly released and re-acquired for deleting a ghost
     * @throws IllegalStateException if lock not held
     */
    final void doUnlockToUpgradable(Locker locker, Latch latch) {
        if (mOwner != locker) {
            if (isClosed(locker)) {
                latch.releaseExclusive();
                return;
            }
            String message = "Exclusive or upgradable lock not held";
            if (mLockCount == 0 || !isSharedLocker(locker)) {
                message = "Lock not held";
            }
            throw new IllegalStateException(message);
        }
        if (mLockCount == ~0) {
            deleteGhost(latch);
            mLockCount = 0x80000000;
            LatchCondition queueSX = mQueueSX;
            if (queueSX != null) {
                queueSX.signalShared(latch);
            }
        }
        latch.releaseExclusive();
    }

    private IllegalStateException unlockFail() {
        String message;
        if (isPrepareLock()) {
            // White lie to avoid exposing the special prepare lock.
            message = "No locks held";
        } else {
            message = "Cannot unlock an exclusive lock";
        }
        return new IllegalStateException(message);
    }

    private static boolean isClosed(Locker locker) {
        LocalDatabase db = locker.getDatabase();
        return db != null && db.isClosed();
    }

    /**
     * Should only be called when exclusive lock is held, and releasing it is allowed.
     *
     * @param latch might be briefly released and re-acquired
     */
    private void deleteGhost(Latch latch) {
        // TODO: Unlock due to rollback can be optimized. It never needs to actually delete
        // ghosts, because the undo actions replaced them.

        Object obj = mSharedLockersObj;
        if (obj instanceof GhostFrame gf) {
            mSharedLockersObj = null;
            // Note that the LocalDatabase is obtained via a weak reference, but no null check
            // needs to be performed. The LocalDatabase would have to have been closed first,
            // but doing this transfers lock ownership. Ghosts cannot be deleted if the
            // ownership has changed, and this is checked by the caller of this method.
            gf.action(mOwner.getDatabase(), latch, this);
        }
    }

    final boolean matches(long indexId, byte[] key, int hash) {
        return mHashCode == hash && mIndexId == indexId && Arrays.equals(mKey, key);
    }

    /**
     * Must hold exclusive lock to be valid.
     */
    final void setGhostFrame(GhostFrame frame) {
        mSharedLockersObj = frame;
    }

    final void setSharedLocker(Locker owner) {
        mSharedLockersObj = owner;
    }

    /**
     * Is null, a Locker, a LockerHTEntry[], or a GhostFrame.
     */
    final Object getSharedLocker() {
        return mSharedLockersObj;
    }

    /**
     * @param lockType TYPE_SHARED, TYPE_UPGRADABLE, or TYPE_EXCLUSIVE
     */
    final void detectDeadlock(Locker locker, int lockType, long nanosTimeout)
        throws DeadlockException
    {
        var detector = new DeadlockDetector(locker, true);
        if (detector.scan()) {
            Object att = findOwnerAttachment(locker, false, lockType);
            throw new DeadlockException(nanosTimeout, att,
                                        detector.mGuilty,
                                        detector.newDeadlockSet(lockType));
        }
    }

    /**
     * Find an exclusive owner attachment, or the first found shared owner attachment. Might
     * acquire and release a shared latch to access the shared owner attachment.
     *
     * @param lockType TYPE_SHARED, TYPE_UPGRADABLE, or TYPE_EXCLUSIVE
     */
    final Object findOwnerAttachment(Locker locker, boolean latched, int lockType) {
        // See note in DeadlockDetector regarding unlatched access to this Lock.

        Locker owner = mOwner;
        if (owner != null && owner != locker) {
            Object att = owner.attachment();
            if (att != null) {
                return att;
            }
        }

        if (lockType != LockManager.TYPE_EXCLUSIVE) {
            // Only an exclusive lock request can be blocked by shared locks.
            return null;
        }

        Object sharedObj = mSharedLockersObj;
        if (sharedObj == null) {
            return null;
        }

        if (sharedObj instanceof Locker held) {
            return held.attachment();
        }

        if (sharedObj instanceof LockerHTEntry[] entries) {
            if (!latched) {
                // Need a latch to safely check the shared lock owner hashtable.
                LockManager manager = locker.mManager;
                if (manager != null) {
                    LockManager.Bucket bucket = manager.getBucket(mHashCode);
                    bucket.acquireShared();
                    try {
                        return findOwnerAttachment(locker, true, lockType);
                    } finally {
                        bucket.releaseShared();
                    }
                }
            } else {
                for (int i=entries.length; --i>=0; ) {
                    for (LockerHTEntry e = entries[i]; e != null; e = e.mNext) {
                        owner = e.mOwner;
                        if (owner != null) {
                            Object att = owner.attachment();
                            if (att != null) {
                                return att;
                            }
                        }
                    }
                }
            }
        }

        return null;
    }

    /**
     * Note: Caller can short-circuit this test by checking the lock count first. If negative,
     * then this method should return false. If the caller has already determined that mQueueSX
     * is non-null, then the short-circuit test is redundant and isn't useful.
     *
     * Must be called with latch held.
     */
    final boolean isSharedLocker(Locker locker) {
        Object sharedObj = mSharedLockersObj;
        if (sharedObj == locker) {
            return true;
        }
        if (sharedObj instanceof LockerHTEntry[] entries) {
            return lockerHTcontains(entries, locker);
        }
        return false;
    }

    /**
     * Removes the locker if isSharedLocker would return true, and there exists more than just
     * the one shared locker. Return values:
     *
     * -1: locker isn't a shared lock owner; must wait for remaining shared lockers to leave
     *  0: locker is now the full owner; must wait for remaining shared lockers to leave
     *  1: locker is the sole shared lock owner, and so there's no need to wait
     *  2: no shared locks are held at all, and so there's no need to wait
     *
     * Must be called with exclusive latch held, and must only be called when mOwner is null.
     */
    final int claimOwnership(Locker locker) {
        Object sharedObj = mSharedLockersObj;

        if (sharedObj == locker) {
            return 1;
        }

        int count = mLockCount;
        if ((count & 0x7fffffff) == 0) {
            return 2;
        }

        if (sharedObj instanceof LockerHTEntry[] entries) {
            if ((count & 0x7fffffff) == 1) {
                if (lockerHTcontains(entries, locker)) {
                    return 1;
                }
            } else if ((count & 0x7fffffff) > 1 && lockerHTremove(entries, locker)) {
                mLockCount = count - 1;
                mOwner = locker;
                return 0;
            }
        }

        return -1;
    }

    /**
     * @return null, Locker, or a non-empty Locker[]
     */
    final Object copyLockers() {
        int count = mLockCount;
        if (count == ~0 || (count &= 0x7fffffff) == 0) {
            return mOwner;
        }

        if (!(mSharedLockersObj instanceof LockerHTEntry[] entries)) {
            return mOwner == null ? mSharedLockersObj
                : new Locker[] {(Locker) mSharedLockersObj, mOwner};
        }

        Locker[] lockers;
        if (mOwner == null) {
            lockers = new Locker[count];
        } else {
            lockers = new Locker[count + 1];
            lockers[lockers.length - 1] = mOwner;
        }

        int i = 0;
        for (LockerHTEntry e : entries) {
            for (; e != null; e = e.mNext) {
                lockers[i++] = e.mOwner;
            }
        }
        
        return lockers;
    }

    final void addSharedLocker(int count, Locker locker) {
        if ((count & 0x7fffffff) >= 0x7ffffffe) {
            throw new IllegalStateException("Too many shared locks held");
        }
        doAddSharedLocker(count + 1, locker);
    }

    private void doAddSharedLocker(int newCount, Locker locker) {
        Object sharedObj = mSharedLockersObj;
        if (sharedObj == null) {
            mSharedLockersObj = locker;
        } else if (sharedObj instanceof LockerHTEntry[] entries) {
            lockerHTadd(entries, newCount & 0x7fffffff, locker);
        } else {
            // Initial capacity of must be a power of 2.
            var entries = new LockerHTEntry[4];
            lockerHTadd(entries, (Locker) sharedObj);
            lockerHTadd(entries, locker);
            mSharedLockersObj = entries;
        }
        mLockCount = newCount;
    }

    private static boolean lockerHTcontains(LockerHTEntry[] entries, Locker locker) {
        int hash = locker.hashCode();
        for (LockerHTEntry e = entries[hash & (entries.length - 1)]; e != null; e = e.mNext) {
            if (e.mOwner == locker) {
                return true;
            }
        }
        return false;
    }

    private void lockerHTadd(LockerHTEntry[] entries, int newSize, Locker locker) {
        if (newSize > (entries.length >> 1)) {
            int capacity = entries.length << 1;
            var newEntries = new LockerHTEntry[capacity];
            int newMask = capacity - 1;

            for (int i=entries.length; --i>=0; ) {
                for (LockerHTEntry e = entries[i]; e != null; ) {
                    LockerHTEntry next = e.mNext;
                    int ix = e.mOwner.hashCode() & newMask;
                    e.mNext = newEntries[ix];
                    newEntries[ix] = e;
                    e = next;
                }
            }

            mSharedLockersObj = entries = newEntries;
        }

        lockerHTadd(entries, locker);
    }

    private static void lockerHTadd(LockerHTEntry[] entries, Locker locker) {
        int index = locker.hashCode() & (entries.length - 1);
        var e = new LockerHTEntry();
        e.mOwner = locker;
        e.mNext = entries[index];
        entries[index] = e;
    }

    private static boolean lockerHTremove(LockerHTEntry[] entries, Locker locker) {
        int index = locker.hashCode() & (entries.length - 1);
        for (LockerHTEntry e = entries[index], prev = null; e != null; e = e.mNext) {
            if (e.mOwner == locker) {
                if (prev == null) {
                    entries[index] = e.mNext;
                } else {
                    prev.mNext = e.mNext;
                }
                return true;
            } else {
                prev = e;
            }
        }
        return false;
    }

    /**
     * Entry for simple hashtable of Lockers.
     */
    static final class LockerHTEntry {
        Locker mOwner;
        LockerHTEntry mNext;
    }
}
