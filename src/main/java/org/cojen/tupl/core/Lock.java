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
final class Lock {
    long mIndexId;
    byte[] mKey;
    int mHashCode;

    // Next entry in LockManager hash collision chain.
    Lock mLockManagerNext;

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

    /**
     * @param locker optional locker
     */
    boolean isAvailable(Locker locker) {
        return mLockCount >= 0 || mOwner == locker;
    }

    /**
     * Called with any latch held, which is retained.
     *
     * @return UNOWNED, OWNED_SHARED, OWNED_UPGRADABLE, or OWNED_EXCLUSIVE
     */
    LockResult check(Locker locker) {
        int count = mLockCount;
        return mOwner == locker
            ? (count == ~0 ? OWNED_EXCLUSIVE : OWNED_UPGRADABLE)
            : ((count != 0 && isSharedLocker(locker)) ? OWNED_SHARED : UNOWNED);
    }

    /**
     * Called with exclusive latch held, which is retained. If return value is TIMED_OUT_LOCK,
     * the locker's mWaitingFor field is set to this Lock as a side-effect.
     *
     * @return INTERRUPTED, TIMED_OUT_LOCK, ACQUIRED, OWNED_SHARED,
     * OWNED_UPGRADABLE, or OWNED_EXCLUSIVE
     * @throws IllegalStateException if too many shared locks
     */
    LockResult tryLockShared(Latch latch, Locker locker, long nanosTimeout) {
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
        long nanosEnd = nanosTimeout < 0 ? 0 : (System.nanoTime() + nanosTimeout);

        // Await for shared lock.
        int w = queueSX.awaitShared(latch, nanosTimeout, nanosEnd);
        queueSX = mQueueSX;

        if (queueSX == null) {
            // Assume LockManager was closed.
            locker.mWaitingFor = null;
            return INTERRUPTED;
        }

        if (queueSX.isEmpty()) {
            // Indicate that last signal has been consumed, and also free memory.
            mQueueSX = null;
        } else {
            // After consuming one signal, next shared waiter must be signaled, and so on.
            queueSX.signalShared(latch);
        }

        if (w >= 1) {
            addSharedLocker(mLockCount, locker);
            locker.mWaitingFor = null;
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
    LockResult tryLockUpgradable(Latch latch, Locker locker, long nanosTimeout) {
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
        long nanosEnd = nanosTimeout < 0 ? 0 : (System.nanoTime() + nanosTimeout);

        // Await for upgradable lock.
        int w = queueU.await(latch, nanosTimeout, nanosEnd);
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

        if (w >= 1) {
            mLockCount |= 0x80000000;
            mOwner = locker;
            locker.mWaitingFor = null;
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
    LockResult tryLockExclusive(Latch latch, Locker locker, long nanosTimeout) {
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
        long nanosEnd = nanosTimeout < 0 ? 0 : (System.nanoTime() + nanosTimeout);

        // Await for exclusive lock.
        int w = queueSX.await(latch, nanosTimeout, nanosEnd);
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

        if (w >= 1) {
            mLockCount = ~0;
            locker.mWaitingFor = null;
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
     * @param ht only released if no exception is thrown
     * @throws IllegalStateException if lock not held or if exclusive lock held
     */
    void unlock(Locker locker, LockManager.LockHT ht) {
        if (mOwner == locker) {
            int count = mLockCount;

            if (count != ~0) {
                // Unlocking an upgradable lock.
                mOwner = null;
                LatchCondition queueU = mQueueU;
                if ((mLockCount = count & 0x7fffffff) == 0 && queueU == null && mQueueSX == null) {
                    // Lock is now completely unused.
                    ht.remove(this);
                } else if (queueU != null) {
                    // Signal at most one upgradable lock waiter.
                    queueU.signal(ht);
                }
            } else {
                // Unlocking an exclusive lock.
                throw unlockFail();
            }
        } else {
            doUnlockShared(locker, ht);
        }

        ht.releaseExclusive();
    }

    /**
     * Called with exclusive latch held, which is released unless an exception is thrown.
     *
     * @param ht briefly released and re-acquired for deleting a ghost
     * @throws IllegalStateException if lock not held
     */
    void doUnlock(Locker locker, LockManager.LockHT ht) {
        if (mOwner == locker) {
            doUnlockOwned(ht);
        } else {
            doUnlockShared(locker, ht);
        }

        ht.releaseExclusive();
    }

    /**
     * @param ht briefly released and re-acquired for deleting a ghost
     */
    private void doUnlockOwned(LockManager.LockHT ht) {
        LatchCondition queueU = mQueueU;
        int count = mLockCount;

        if (count != ~0) {
            // Unlocking an upgradable lock.
            mOwner = null;
            if ((mLockCount = count & 0x7fffffff) == 0 && queueU == null && mQueueSX == null) {
                // Lock is now completely unused.
                ht.remove(this);
            } else if (queueU != null) {
                // Signal at most one upgradable lock waiter.
                queueU.signal(ht);
            }
        } else {
            // Unlocking an exclusive lock.
            deleteGhost(ht);
            mOwner = null;
            mLockCount = 0;
            LatchCondition queueSX = mQueueSX;
            if (queueU != null) {
                // Signal at most one upgradable lock waiter.
                queueU.signal(ht);
                if (queueSX == null) {
                    return;
                }
            } else if (queueSX == null) {
                // Lock is now completely unused.
                ht.remove(this);
                return;
            }
            // Signal at most one shared lock waiter. There aren't any exclusive lock waiters,
            // because they would need to acquire the upgradable lock first, which was held.
            queueSX.signal(ht);
        }
    }

    /**
     * @param ht never released, even if an exception is thrown
     * @throws IllegalStateException if lock not held
     */
    private void doUnlockShared(Locker locker, LockManager.LockHT ht) {
        int count = mLockCount;

        unlock: {
            if ((count & 0x7fffffff) != 0) {
                Object sharedObj = mSharedLockersObj;
                if (sharedObj == locker) {
                    mSharedLockersObj = null;
                    break unlock;
                } else if (sharedObj instanceof LockerHTEntry[]) {
                    var entries = (LockerHTEntry[]) sharedObj;
                    if (lockerHTremove(entries, locker)) {
                        if (count == 2) {
                            mSharedLockersObj = lockerHTgetOne(entries);
                        }
                        break unlock;
                    }
                }
            }

            if (isClosed(locker)) {
                return;
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
                queueSX.signal(ht);
            }
        } else if (count == 0 && queueSX == null && mQueueU == null) {
            // Lock is now completely unused.
            ht.remove(this);
        }
    }

    /**
     * Called with exclusive latch held, which is released unless an exception is thrown.
     *
     * @param latch only released if no exception is thrown
     * @throws IllegalStateException if lock not held, if exclusive lock held, or if too many
     * shared locks
     */
    void unlockToShared(Locker locker, Latch latch) {
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
    void doUnlockToShared(Locker locker, Latch latch) {
        ownerCheck: if (mOwner == locker) {
            LatchCondition queueU = mQueueU;
            int count = mLockCount;

            if (count != ~0) {
                // Unlocking upgradable lock into shared. Retain upgradable lock if too many
                // shared locks are held (IllegalStateException is thrown).
                addSharedLocker(count & 0x7fffffff, locker);
                mOwner = null;
            } else {
                // Unlocking exclusive lock into shared.
                deleteGhost(latch);
                doAddSharedLocker(1, locker);
                mOwner = null;
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
    void doUnlockToUpgradable(Locker locker, Latch latch) {
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
        if (mLockCount != ~0) {
            // Already upgradable.
            latch.releaseExclusive();
            return;
        }
        deleteGhost(latch);
        mLockCount = 0x80000000;
        LatchCondition queueSX = mQueueSX;
        if (queueSX != null) {
            queueSX.signalShared(latch);
        }
        latch.releaseExclusive();
    }

    /**
     * Releases an exclusive lock and never removes it from the LockManager. Is used to
     * transfer ownership of a Locker between threads, which must have a reference back to this
     * Lock. No check is made to verify that lock is held exclusive. The other thread, upon
     * acquiring the lock, must not push this Lock instance to the Locker again.
     *
     * @param ht never released, even if an exception is thrown
     */
    void signalExclusive(LockManager.LockHT ht) {
        mOwner = null;
        mLockCount = 0;
        // Signal behavior is a simplified from used by doUnlockOwned.
        if (mQueueU != null) {
            mQueueU.signal(ht);
        }
        if (mQueueSX != null) {
            mQueueSX.signal(ht);
        }
    }

    private IllegalStateException unlockFail() {
        return new IllegalStateException("Cannot unlock an exclusive lock");
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
        if (obj instanceof GhostFrame) {
            mSharedLockersObj = null;
            // Note that the Database is obtained via a weak reference, but no null check needs
            // to be performed. The Database would have to have been closed first, but doing
            // this transfers lock ownership. Ghosts cannot be deleted if the ownership has
            // changed, and this is checked by the caller of this method.
            ((GhostFrame) obj).action(mOwner.getDatabase(), latch, this);
        }
    }

    boolean matches(long indexId, byte[] key, int hash) {
        return mHashCode == hash && mIndexId == indexId && Arrays.equals(mKey, key);
    }

    /**
     * Must hold exclusive lock to be valid.
     */
    void setGhostFrame(GhostFrame frame) {
        mSharedLockersObj = frame;
    }

    void setSharedLocker(Locker owner) {
        mSharedLockersObj = owner;
    }

    /**
     * Is null, a Locker, a LockerHTEntry[], or a GhostFrame.
     */
    Object getSharedLocker() {
        return mSharedLockersObj;
    }

    /**
     * @param lockType TYPE_SHARED, TYPE_UPGRADABLE, or TYPE_EXCLUSIVE
     */
    void detectDeadlock(Locker locker, int lockType, long nanosTimeout)
        throws DeadlockException
    {
        var detector = new DeadlockDetector(locker);
        if (detector.scan()) {
            Object att = findOwnerAttachment(locker, lockType);
            throw new DeadlockException(nanosTimeout, att,
                                        detector.mGuilty,
                                        detector.newDeadlockSet(lockType));
        }
    }

    /**
     * Find an exclusive owner attachment, or the first found shared owner attachment. Might
     * acquire and release a shared latch to access the shared owner attachment.
     *
     * @param locker pass null if already latched
     * @param lockType TYPE_SHARED, TYPE_UPGRADABLE, or TYPE_EXCLUSIVE
     */
    Object findOwnerAttachment(Locker locker, int lockType) {
        // See note in DeadlockDetector regarding unlatched access to this Lock.

        Locker owner = mOwner;
        if (owner != null) {
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

        if (sharedObj instanceof Locker) {
            return ((Locker) sharedObj).attachment();
        }

        if (sharedObj instanceof LockerHTEntry[]) {
            if (locker != null) {
                // Need a latch to safely check the shared lock owner hashtable.
                LockManager manager = locker.mManager;
                if (manager != null) {
                    LockManager.LockHT ht = manager.getLockHT(mHashCode);
                    ht.acquireShared();
                    try {
                        return findOwnerAttachment(null, lockType);
                    } finally {
                        ht.releaseShared();
                    }
                }
            } else {
                var entries = (LockerHTEntry[]) sharedObj;

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
     */
    private boolean isSharedLocker(Locker locker) {
        Object sharedObj = mSharedLockersObj;
        if (sharedObj == locker) {
            return true;
        }
        if (sharedObj instanceof LockerHTEntry[]) {
            return lockerHTcontains((LockerHTEntry[]) sharedObj, locker);
        }
        return false;
    }

    private void addSharedLocker(int count, Locker locker) {
        if ((count & 0x7fffffff) >= 0x7ffffffe) {
            throw new IllegalStateException("Too many shared locks held");
        }
        doAddSharedLocker(count + 1, locker);
    }

    private void doAddSharedLocker(int newCount, Locker locker) {
        Object sharedObj = mSharedLockersObj;
        if (sharedObj == null) {
            mSharedLockersObj = locker;
        } else if (sharedObj instanceof LockerHTEntry[]) {
            var entries = (LockerHTEntry[]) sharedObj;
            lockerHTadd(entries, newCount & 0x7fffffff, locker);
        } else {
            // Initial capacity of must be a power of 2.
            var entries = new LockerHTEntry[8];
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

    private static Locker lockerHTgetOne(LockerHTEntry[] entries) {
        for (LockerHTEntry e : entries) {
            if (e != null) {
                return e.mOwner;
            }
        }
        throw new AssertionError("No lockers in hashtable");
    }

    /**
     * Entry for simple hashtable of Lockers.
     */
    static final class LockerHTEntry {
        Locker mOwner;
        LockerHTEntry mNext;
    }
}
