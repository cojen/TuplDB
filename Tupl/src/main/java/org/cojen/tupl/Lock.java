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
 * Partially reentrant shared/upgradable/exclusive lock, with fair acquisition
 * methods. Locks are owned by Lockers, not Threads. Implementation relies on
 * latching for mutual exclusion, but condition variable logic is used for
 * transferring ownership between Lockers.
 *
 * @author Brian S O'Neill
 * @see LockManager
 */
final class Lock {
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
    Object mSharedLockersObj;

    // Waiters for upgradable lock. Contains only Node instances.
    WaitQueue mQueueU;

    // Waiters for shared and exclusive locks. Contains Node and Shared instances.
    WaitQueue mQueueSX;

    /**
     * Called with exclusive latch held, which is retained.
     *
     * @return INTERRUPTED, TIMED_OUT_LOCK, ACQUIRED, OWNED_SHARED,
     * OWNED_UPGRADABLE, or OWNED_EXCLUSIVE
     * @throws IllegalStateException if too many shared locks
     */
    LockResult lockShared(Latch latch, Locker locker, long nanosTimeout) {
        if (mLocker == locker) {
            return mLockCount == ~0 ? LockResult.OWNED_EXCLUSIVE : LockResult.OWNED_UPGRADABLE;
        }

        WaitQueue queueSX = mQueueSX;
        if (queueSX != null) {
            if (nanosTimeout == 0) {
                return LockResult.TIMED_OUT_LOCK;
            }
        } else {
            LockResult r = tryLockShared(locker);
            if (r != null) {
                return r;
            }
            if (nanosTimeout == 0) {
                return LockResult.TIMED_OUT_LOCK;
            }
            mQueueSX = queueSX = new WaitQueue();
        }

        while (true) {
            // Await for shared lock.
            int w = queueSX.await(latch, new WaitQueue.Shared(), nanosTimeout);
            queueSX = mQueueSX;

            if (queueSX != null && queueSX.isEmpty()) {
                // Indicate that last signal has been consumed, and also free memory.
                mQueueSX = null;
            }

            if (w < 1) {
                return w == 0 ? LockResult.TIMED_OUT_LOCK : LockResult.INTERRUPTED;
            }

            // Because latch was released while waiting on condition, check
            // everything again.

            if (mLocker == locker) {
                return mLockCount == ~0 ? LockResult.OWNED_EXCLUSIVE : LockResult.OWNED_UPGRADABLE;
            }

            LockResult r = tryLockShared(locker);
            if (r != null) {
                return r;
            }

            // Signal was bogus or lock was grabbed by another thread, so retry.
            // FIXME: Although retry is expected to be very rare, the timeout
            // should still be adjusted.

            if (mQueueSX == null) {
                mQueueSX = queueSX;
            }
        }
    }

    /**
     * Called with exclusive latch held, which is retained.
     *
     * @return ILLEGAL, INTERRUPTED, TIMED_OUT_LOCK, ACQUIRED,
     * OWNED_UPGRADABLE, or OWNED_EXCLUSIVE
     */
    LockResult lockUpgradable(Latch latch, Locker locker, long nanosTimeout) {
        if (mLocker == locker) {
            return mLockCount == ~0 ? LockResult.OWNED_EXCLUSIVE : LockResult.OWNED_UPGRADABLE;
        }

        int count = mLockCount;
        if (count != 0 && isSharedLocker(locker)) {
            return LockResult.ILLEGAL;
        }

        WaitQueue queueU = mQueueU;
        if (queueU != null) {
            if (nanosTimeout == 0) {
                return LockResult.TIMED_OUT_LOCK;
            }
        } else {
            if (count >= 0) {
                mLockCount = count | 0x80000000;
                mLocker = locker;
                return LockResult.ACQUIRED;
            }
            if (nanosTimeout == 0) {
                return LockResult.TIMED_OUT_LOCK;
            }
            mQueueU = queueU = new WaitQueue();
        }

        while (true) {
            // Await for exclusive lock.
            int w = queueU.await(latch, new WaitQueue.Node(), nanosTimeout);
            queueU = mQueueU;

            if (queueU != null && queueU.isEmpty()) {
                // Indicate that last signal has been consumed, and also free memory.
                mQueueU = null;
            }

            if (w < 1) {
                return w == 0 ? LockResult.TIMED_OUT_LOCK : LockResult.INTERRUPTED;
            }

            // Because latch was released while waiting on condition, check
            // everything again.

            if (mLocker == locker) {
                return mLockCount == ~0 ? LockResult.OWNED_EXCLUSIVE : LockResult.OWNED_UPGRADABLE;
            }

            count = mLockCount;
            if (count != 0 && isSharedLocker(locker)) {
                // Signal that another waiter can get the lock instead.
                if (queueU != null) {
                    queueU.signalOne();
                }
                return LockResult.ILLEGAL;
            }

            if (count >= 0) {
                mLockCount = count | 0x80000000;
                mLocker = locker;
                return LockResult.ACQUIRED;
            }

            // Signal was bogus or lock was grabbed by another thread, so retry.
            // FIXME: Although retry is expected to be very rare, the timeout
            // should still be adjusted.

            if (mQueueU == null) {
                mQueueU = queueU;
            }
        }
    }

    /**
     * Called with exclusive latch held, which is retained.
     *
     * @return ILLEGAL, INTERRUPTED, TIMED_OUT_LOCK, ACQUIRED, UPGRADED, or
     * OWNED_EXCLUSIVE
     */
    LockResult lockExclusive(Latch latch, Locker locker, long nanosTimeout) {
        final LockResult ur = lockUpgradable(latch, locker, nanosTimeout);
        if (!ur.isGranted() || ur == LockResult.OWNED_EXCLUSIVE) {
            return ur;
        }

        WaitQueue queueSX = mQueueSX;
        if (queueSX != null) {
            if (nanosTimeout == 0) {
                if (ur == LockResult.ACQUIRED) {
                    unlock(locker);
                }
                return LockResult.TIMED_OUT_LOCK;
            }
        } else {
            int count = mLockCount;
            if (count == 0x80000000) {
                mLockCount = ~0;
                return ur == LockResult.OWNED_UPGRADABLE
                    ? LockResult.UPGRADED : LockResult.ACQUIRED;
            }
            if (nanosTimeout == 0) {
                if (ur == LockResult.ACQUIRED) {
                    unlock(locker);
                }
                return LockResult.TIMED_OUT_LOCK;
            }
            mQueueSX = queueSX = new WaitQueue();
        }

        while (true) {
            // Await for exclusive lock.
            int w = queueSX.await(latch, new WaitQueue.Node(), nanosTimeout);
            queueSX = mQueueSX;

            if (queueSX != null && queueSX.isEmpty()) {
                // Indicate that last signal has been consumed, and also free memory.
                mQueueSX = null;
            }

            if (w < 1) {
                if (ur == LockResult.ACQUIRED) {
                    unlock(locker);
                }
                return w == 0 ? LockResult.TIMED_OUT_LOCK : LockResult.INTERRUPTED;
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
                return ur == LockResult.OWNED_UPGRADABLE
                    ? LockResult.UPGRADED : LockResult.ACQUIRED;
            }

            // Signal was bogus or lock was grabbed by another thread, so retry.
            // FIXME: Although retry is expected to be very rare, the timeout
            // should still be adjusted.

            if (mQueueSX == null) {
                mQueueSX = queueSX;
            }
        }
    }

    /**
     * Called with exclusive latch held, which is retained.
     *
     * @return true if lock is now completely unused
     * @throws IllegalStateException if lock not held
     */
    boolean unlock(Locker locker) {
        if (mLocker == locker) {
            mLocker = null;
            WaitQueue queueU = mQueueU;
            if (queueU != null) {
                // Signal at most one upgradable lock waiter.
                queueU.signalOne();
            }
            int count = mLockCount;
            if (count != ~0) {
                // Unlocking upgradable lock.
                return (mLockCount = count & 0x7fffffff) == 0 && queueU == null;
            } else {
                // Unlocking exclusive lock.
                mLockCount = 0;
                WaitQueue queueSX = mQueueSX;
                if (queueSX == null) {
                    return queueU == null;
                } else {
                    // Signal all shared lock waiters. Queue doesn't contain
                    // any exclusive lock waiters, because they would need to
                    // acquire upgradable lock first, which was held.
                    queueSX.signalAll();
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
                    queueSX.signalSharedOrOneExclusive();
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
     * @throws IllegalStateException if lock not held or too many shared locks
     */
    void unlockToShared(Locker locker) {
        if (mLocker == locker) {
            mLocker = null;
            WaitQueue queueU = mQueueU;
            if (queueU != null) {
                // Signal at most one upgradable lock waiter.
                queueU.signalOne();
            }
            int count = mLockCount;
            if (count != ~0) {
                // Unlocking upgradable lock into shared.
                if ((count &= 0x7fffffff) >= 0x7ffffffe) {
                    mLockCount = count;
                    throw new IllegalStateException("Too many shared locks held");
                }
                addSharedLocker(count, locker);
            } else {
                // Unlocking exclusive lock into shared.
                addSharedLocker(0, locker);
                WaitQueue queueSX = mQueueSX;
                if (queueSX != null) {
                    // Signal all shared lock waiters. Queue doesn't contain
                    // any exclusive lock waiters, because they would need to
                    // acquire upgradable lock first, which was held.
                    queueSX.signalAll();
                }
            }
        } else if (mLockCount == 0 || !isSharedLocker(locker)) {
            throw new IllegalStateException("Lock not held");
        }
    }

    /**
     * Called with exclusive latch held, which is retained.
     *
     * @throws IllegalStateException if lock not held
     */
    void unlockToUpgradable(Locker locker) {
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
        mLockCount = 0x80000000;
        WaitQueue queueSX = mQueueSX;
        if (queueSX != null) {
            queueSX.signalShared();
        }
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
            return LockResult.OWNED_SHARED;
        }
        if ((count & 0x7fffffff) >= 0x7ffffffe) {
            throw new IllegalStateException("Too many shared locks held");
        }
        addSharedLocker(count, locker);
        return LockResult.ACQUIRED;
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
