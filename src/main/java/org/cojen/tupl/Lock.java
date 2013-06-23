/*
 *  Copyright 2011-2013 Brian S O'Neill
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

import java.util.Arrays;

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
    Locker mLocker;

    // Locker instance if one shared locker, or else a hashtable for more.
    // Field is re-used to indicate when an exclusive lock has ghosted an
    // entry, which should be deleted when the transaction commits. A C-style
    // union type would be handy. Object is a Tree if entry is ghosted.
    Object mSharedLockersObj;

    // Waiters for upgradable lock. Contains only WaitQueue.Node instances.
    WaitQueue mQueueU;

    // Waiters for shared and exclusive locks. Contains WaitQueue.Node and Shared instances.
    WaitQueue mQueueSX;

    /**
     * @param locker optional locker
     */
    boolean isAvailable(Locker locker) {
        return mLockCount >= 0 || mLocker == locker;
    }

    /**
     * Called with any latch held, which is retained.
     *
     * @return UNOWNED, OWNED_SHARED, OWNED_UPGRADABLE, or OWNED_EXCLUSIVE
     */
    LockResult check(Locker locker) {
        int count = mLockCount;
        return mLocker == locker
            ? (count == ~0 ? OWNED_EXCLUSIVE : OWNED_UPGRADABLE)
            : ((count != 0 && isSharedLocker(locker)) ? OWNED_SHARED : UNOWNED);
    }

    /**
     * Called with exclusive latch held, which is retained. If return value is
     * TIMED_OUT_LOCK and timeout was non-zero, the locker's mWaitingFor field
     * is set to this Lock as a side-effect.
     *
     * @return INTERRUPTED, TIMED_OUT_LOCK, ACQUIRED, OWNED_SHARED,
     * OWNED_UPGRADABLE, or OWNED_EXCLUSIVE
     * @throws IllegalStateException if too many shared locks
     */
    LockResult tryLockShared(Latch latch, Locker locker, long nanosTimeout) {
        if (mLocker == locker) {
            return mLockCount == ~0 ? OWNED_EXCLUSIVE : OWNED_UPGRADABLE;
        }

        WaitQueue queueSX = mQueueSX;
        if (queueSX != null) {
            if (nanosTimeout == 0) {
                return TIMED_OUT_LOCK;
            }
        } else {
            LockResult r = tryLockShared(locker);
            if (r != null) {
                return r;
            }
            if (nanosTimeout == 0) {
                return TIMED_OUT_LOCK;
            }
            mQueueSX = queueSX = new WaitQueue();
        }

        locker.mWaitingFor = this;
        long nanosEnd = nanosTimeout < 0 ? 0 : (System.nanoTime() + nanosTimeout);

        while (true) {
            // Await for shared lock.
            int w = queueSX.await(latch, new WaitQueue.Shared(), nanosTimeout, nanosEnd);
            queueSX = mQueueSX;

            // After consuming one signal, next shared waiter must be signaled, and so on.
            if (queueSX != null && !queueSX.signalNextShared()) {
                // Indicate that last signal has been consumed, and also free memory.
                mQueueSX = null;
            }

            if (w < 1) {
                if (w == 0) {
                    return TIMED_OUT_LOCK;
                } else {
                    locker.mWaitingFor = null;
                    return INTERRUPTED;
                }
            }

            // Because latch was released while waiting on condition, check
            // everything again.

            if (mLocker == locker) {
                locker.mWaitingFor = null;
                return mLockCount == ~0 ? OWNED_EXCLUSIVE : OWNED_UPGRADABLE;
            }

            LockResult r = tryLockShared(locker);
            if (r != null) {
                locker.mWaitingFor = null;
                return r;
            }

            // Signal was bogus or lock was grabbed by another thread, so retry.

            if (nanosTimeout >= 0 && (nanosTimeout = nanosEnd - System.nanoTime()) <= 0) {
                return TIMED_OUT_LOCK;
            }

            if (mQueueSX == null) {
                mQueueSX = queueSX = new WaitQueue();
            }
        }
    }

    /**
     * Called with exclusive latch held, which is retained. If return value is
     * TIMED_OUT_LOCK and timeout was non-zero, the locker's mWaitingFor field
     * is set to this Lock as a side-effect.
     *
     * @return ILLEGAL, INTERRUPTED, TIMED_OUT_LOCK, ACQUIRED,
     * OWNED_UPGRADABLE, or OWNED_EXCLUSIVE
     */
    LockResult tryLockUpgradable(Latch latch, Locker locker, long nanosTimeout) {
        if (mLocker == locker) {
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
                mLocker = locker;
                return OWNED_UPGRADABLE;
            }
        }

        WaitQueue queueU = mQueueU;
        if (queueU != null) {
            if (nanosTimeout == 0) {
                return TIMED_OUT_LOCK;
            }
        } else {
            if (count >= 0) {
                mLockCount = count | 0x80000000;
                mLocker = locker;
                return ACQUIRED;
            }
            if (nanosTimeout == 0) {
                return TIMED_OUT_LOCK;
            }
            mQueueU = queueU = new WaitQueue();
        }

        locker.mWaitingFor = this;
        long nanosEnd = nanosTimeout < 0 ? 0 : (System.nanoTime() + nanosTimeout);

        while (true) {
            // Await for exclusive lock.
            int w = queueU.await(latch, new WaitQueue.Node(), nanosTimeout, nanosEnd);
            queueU = mQueueU;

            if (queueU != null && queueU.isEmpty()) {
                // Indicate that last signal has been consumed, and also free memory.
                mQueueU = null;
            }

            if (w < 1) {
                if (w == 0) {
                    return TIMED_OUT_LOCK;
                } else {
                    locker.mWaitingFor = null;
                    return INTERRUPTED;
                }
            }

            // Because latch was released while waiting on condition, check
            // everything again.

            if (mLocker == locker) {
                locker.mWaitingFor = null;
                return mLockCount == ~0 ? OWNED_EXCLUSIVE : OWNED_UPGRADABLE;
            }

            count = mLockCount;
            if (count != 0 && isSharedLocker(locker)) {
                if (!locker.canAttemptUpgrade(count)) {
                    // Signal that another waiter can get the lock instead.
                    if (queueU != null) {
                        queueU.signal();
                    }
                    locker.mWaitingFor = null;
                    return ILLEGAL;
                }
                if (count > 0) {
                    // Give the impression that lock was always held upgradable. This prevents
                    // pushing the lock into the locker twice.
                    mLockCount = (count - 1) | 0x80000000;
                    mLocker = locker;
                    return OWNED_UPGRADABLE;
                }
            }

            if (count >= 0) {
                mLockCount = count | 0x80000000;
                mLocker = locker;
                locker.mWaitingFor = null;
                return ACQUIRED;
            }

            // Signal was bogus or lock was grabbed by another thread, so retry.

            if (nanosTimeout >= 0 && (nanosTimeout = nanosEnd - System.nanoTime()) <= 0) {
                return TIMED_OUT_LOCK;
            }

            if (mQueueU == null) {
                mQueueU = queueU = new WaitQueue();
            }
        }
    }

    /**
     * Called with exclusive latch held, which is retained. If return value is
     * TIMED_OUT_LOCK and timeout was non-zero, the locker's mWaitingFor field
     * is set to this Lock as a side-effect.
     *
     * @return ILLEGAL, INTERRUPTED, TIMED_OUT_LOCK, ACQUIRED, UPGRADED, or
     * OWNED_EXCLUSIVE
     */
    LockResult tryLockExclusive(Latch latch, Locker locker, long nanosTimeout) {
        final LockResult ur = tryLockUpgradable(latch, locker, nanosTimeout);
        if (!ur.isHeld() || ur == OWNED_EXCLUSIVE) {
            return ur;
        }

        WaitQueue queueSX = mQueueSX;
        if (queueSX != null) {
            if (nanosTimeout == 0) {
                if (ur == ACQUIRED) {
                    unlockUpgradable();
                }
                return TIMED_OUT_LOCK;
            }
        } else {
            if (mLockCount == 0x80000000) {
                mLockCount = ~0;
                return ur == OWNED_UPGRADABLE ? UPGRADED : ACQUIRED;
            }
            if (nanosTimeout == 0) {
                if (ur == ACQUIRED) {
                    unlockUpgradable();
                }
                return TIMED_OUT_LOCK;
            }
            mQueueSX = queueSX = new WaitQueue();
        }

        locker.mWaitingFor = this;
        long nanosEnd = nanosTimeout < 0 ? 0 : (System.nanoTime() + nanosTimeout);

        while (true) {
            // Await for exclusive lock.
            int w = queueSX.await(latch, new WaitQueue.Node(), nanosTimeout, nanosEnd);
            queueSX = mQueueSX;

            if (queueSX != null && queueSX.isEmpty()) {
                // Indicate that last signal has been consumed, and also free memory.
                mQueueSX = null;
            }

            if (w < 1) {
                if (ur == ACQUIRED) {
                    unlockUpgradable();
                }
                if (w == 0) {
                    return TIMED_OUT_LOCK;
                } else {
                    locker.mWaitingFor = null;
                    return INTERRUPTED;
                }
            }

            // Because latch was released while waiting on condition, check
            // everything again.

            acquired: {
                int count = mLockCount;
                if (count == 0x80000000) {
                    mLockCount = ~0;
                } else if (count != ~0) {
                    break acquired;
                }
                locker.mWaitingFor = null;
                return ur == OWNED_UPGRADABLE ? UPGRADED : ACQUIRED;
            }

            // Signal was bogus or lock was grabbed by another thread, so retry.

            if (nanosTimeout >= 0 && (nanosTimeout = nanosEnd - System.nanoTime()) <= 0) {
                return TIMED_OUT_LOCK;
            }

            if (mQueueSX == null) {
                mQueueSX = queueSX = new WaitQueue();
            }
        }
    }

    /**
     * Called internally to unlock an upgradable lock which was just
     * acquired. Implementation is a just a smaller version of the regular
     * unlock method. It doesn't have to deal with ghosts.
     */
    private void unlockUpgradable() {
        mLocker = null;
        WaitQueue queueU = mQueueU;
        if (queueU != null) {
            // Signal at most one upgradable lock waiter.
            queueU.signal();
        }
        mLockCount &= 0x7fffffff;
    }

    /**
     * Called with exclusive latch held, which is retained.
     *
     * @param latch briefly released and re-acquired for deleting a ghost
     * @return true if lock is now completely unused
     * @throws IllegalStateException if lock not held
     */
    boolean unlock(Locker locker, Latch latch) {
        if (mLocker == locker) {
            deleteGhost(latch);
            mLocker = null;
            WaitQueue queueU = mQueueU;
            if (queueU != null) {
                // Signal at most one upgradable lock waiter.
                queueU.signal();
            }
            int count = mLockCount;
            if (count != ~0) {
                // Unlocking upgradable lock.
                return (mLockCount = count & 0x7fffffff) == 0
                    && queueU == null && mQueueSX == null;
            } else {
                // Unlocking exclusive lock.
                mLockCount = 0;
                WaitQueue queueSX = mQueueSX;
                if (queueSX == null) {
                    return queueU == null;
                } else {
                    // Signal first shared lock waiter. Queue doesn't contain
                    // any exclusive lock waiters, because they would need to
                    // acquire upgradable lock first, which was held.
                    queueSX.signal();
                    return false;
                }
            }
        } else {
            int count = mLockCount;

            unlock: {
                if ((count & 0x7fffffff) != 0) {
                    Object sharedObj = mSharedLockersObj;
                    if (sharedObj == locker) {
                        mSharedLockersObj = null;
                        break unlock;
                    } else if (sharedObj instanceof LockerHTEntry[]) {
                        LockerHTEntry[] entries = (LockerHTEntry[]) sharedObj;
                        if (lockerHTremove(entries, locker)) {
                            if (count == 2) {
                                mSharedLockersObj = lockerHTgetOne(entries);
                            }
                            break unlock;
                        }
                    }
                }

                throw new IllegalStateException("Lock not held");
            }

            mLockCount = --count;

            WaitQueue queueSX = mQueueSX;
            if (count == 0x80000000) {
                if (queueSX != null) {
                    // Signal any exclusive lock waiter. Queue shouldn't contain
                    // any shared lock waiters, because no exclusive lock is
                    // held. In case there are any, signal them instead.
                    queueSX.signal();
                }
                return false;
            } else {
                return count == 0 && queueSX == null && mQueueU == null;
            }
        }
    }

    /**
     * Called with exclusive latch held, which is retained.
     *
     * @param latch briefly released and re-acquired for deleting a ghost
     * @throws IllegalStateException if lock not held or too many shared locks
     */
    void unlockToShared(Locker locker, Latch latch) {
        if (mLocker == locker) {
            deleteGhost(latch);
            mLocker = null;
            WaitQueue queueU = mQueueU;
            if (queueU != null) {
                // Signal at most one upgradable lock waiter.
                queueU.signal();
            }
            int count = mLockCount;
            if (count != ~0) {
                // Unlocking upgradable lock into shared.
                if ((count &= 0x7fffffff) >= 0x7ffffffe) {
                    // Retain upgradable lock when this happens.
                    // mLockCount = count;
                    throw new IllegalStateException("Too many shared locks held");
                }
                addSharedLocker(count, locker);
            } else {
                // Unlocking exclusive lock into shared.
                addSharedLocker(0, locker);
                WaitQueue queueSX = mQueueSX;
                if (queueSX != null) {
                    // Signal first shared lock waiter. Queue doesn't contain
                    // any exclusive lock waiters, because they would need to
                    // acquire upgradable lock first, which was held.
                    queueSX.signal();
                }
            }
        } else if (mLockCount == 0 || !isSharedLocker(locker)) {
            throw new IllegalStateException("Lock not held");
        }
    }

    /**
     * Called with exclusive latch held, which is retained.
     *
     * @param latch briefly released and re-acquired for deleting a ghost
     * @throws IllegalStateException if lock not held
     */
    void unlockToUpgradable(Locker locker, Latch latch) {
        if (mLocker != locker) {
            String message = "Exclusive or upgradable lock not held";
            if (mLockCount == 0 || !isSharedLocker(locker)) {
                message = "Lock not held";
            }
            throw new IllegalStateException(message);
        }
        if (mLockCount != ~0) {
            // Already upgradable.
            return;
        }
        deleteGhost(latch);
        mLockCount = 0x80000000;
        WaitQueue queueSX = mQueueSX;
        if (queueSX != null) {
            queueSX.signalShared();
        }
    }

    /**
     * @param latch might be briefly released and re-acquired
     */
    void deleteGhost(Latch latch) {
        // TODO: Unlock due to rollback can be optimized. It never needs to
        // actually delete ghosts, because the undo actions replaced
        // them. Calling TreeCursor.deleteGhost performs a pointless search.

        Object obj = mSharedLockersObj;
        if (!(obj instanceof Tree)) {
            return;
        }

        Tree tree = (Tree) obj;

        try {
            while (true) {
                TreeCursor c = new TreeCursor(tree, null);
                c.autoload(false);
                byte[] key = mKey;
                mSharedLockersObj = null;

                // Release to prevent deadlock, since additional latches are
                // required for delete.
                latch.releaseExclusive();
                try {
                    if (c.deleteGhost(key)) {
                        break;
                    }
                    // Reopen closed index.
                    tree = (Tree) tree.mDatabase.indexById(tree.mId);
                    if (tree == null) {
                        // Assume index was deleted.
                        break;
                    }
                } finally {
                    latch.acquireExclusive();
                }
            }
        } catch (Throwable e) {
            // Exception indicates that database is borked. Ghost will get
            // cleaned up when database is re-opened.
            latch.releaseExclusive();
            try {
                Utils.closeQuietly(null, ((Tree) obj).mDatabase, e);
            } finally {
                latch.acquireExclusive();
            }
        }
    }

    /**
     * Called with exclusive latch held, which is retained.
     */
    /*
    boolean unlockIfNonExclusive(Locker locker) {
        if (mLockCount == ~0) {
            // Retain exclusive lock. Since this method is only called via
            // Locker.scopeUnlockAllNonExclusive, lock is already known to be
            // held at some level. No need to check if mLocker field matches.
            return false;
        } else {
            // Unlock upgradable or shared lock.
            unlock(locker);
            return true;
        }
    }
    */

    boolean matches(long indexId, byte[] key, int hash) {
        return mHashCode == hash && mIndexId == indexId && Arrays.equals(mKey, key);
    }

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

    /**
     * @return ACQUIRED, OWNED_SHARED, or null
     */
    private LockResult tryLockShared(Locker locker) {
        int count = mLockCount;
        if (count == ~0) {
            return null;
        }
        if (count != 0 && isSharedLocker(locker)) {
            return OWNED_SHARED;
        }
        if ((count & 0x7fffffff) >= 0x7ffffffe) {
            throw new IllegalStateException("Too many shared locks held");
        }
        addSharedLocker(count, locker);
        return ACQUIRED;
    }

    private void addSharedLocker(int count, Locker locker) {
        count++;
        Object sharedObj = mSharedLockersObj;
        if (sharedObj == null) {
            mSharedLockersObj = locker;
        } else if (sharedObj instanceof LockerHTEntry[]) {
            LockerHTEntry[] entries = (LockerHTEntry[]) sharedObj;
            lockerHTadd(entries, count & 0x7fffffff, locker);
        } else {
            // Initial capacity of must be a power of 2.
            LockerHTEntry[] entries = new LockerHTEntry[8];
            lockerHTadd(entries, (Locker) sharedObj);
            lockerHTadd(entries, locker);
            mSharedLockersObj = entries;
        }
        mLockCount = count;
    }

    private static boolean lockerHTcontains(LockerHTEntry[] entries, Locker locker) {
        int hash = locker.hashCode();
        for (LockerHTEntry e = entries[hash & (entries.length - 1)]; e != null; e = e.mNext) {
            if (e.mLocker == locker) {
                return true;
            }
        }
        return false;
    }

    private void lockerHTadd(LockerHTEntry[] entries, int newSize, Locker locker) {
        if (newSize > (entries.length >> 1)) {
            int capacity = entries.length << 1;
            LockerHTEntry[] newEntries = new LockerHTEntry[capacity];
            int newMask = capacity - 1;

            for (int i=entries.length; --i>=0; ) {
                for (LockerHTEntry e = entries[i]; e != null; ) {
                    LockerHTEntry next = e.mNext;
                    int ix = e.mLocker.hashCode() & newMask;
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
        LockerHTEntry e = new LockerHTEntry();
        e.mLocker = locker;
        e.mNext = entries[index];
        entries[index] = e;
    }

    private static boolean lockerHTremove(LockerHTEntry[] entries, Locker locker) {
        int index = locker.hashCode() & (entries.length - 1);
        for (LockerHTEntry e = entries[index], prev = null; e != null; e = e.mNext) {
            if (e.mLocker == locker) {
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
                return e.mLocker;
            }
        }
        throw new AssertionError("No lockers in hashtable");
    }

    /**
     * Entry for simple hashtable of Lockers.
     */
    static final class LockerHTEntry {
        Locker mLocker;
        LockerHTEntry mNext;
    }
}
