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

import java.lang.invoke.VarHandle;

import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;

import java.util.concurrent.ThreadLocalRandom;

import org.cojen.tupl.Index;
import org.cojen.tupl.LockFailureException;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.LockUpgradeRule;

import static org.cojen.tupl.LockResult.*;

import org.cojen.tupl.util.Latch;

/**
 * Manages all Lock instances using a specialized striped hashtable.
 *
 * @author Brian S O'Neill
 */
public final class LockManager {
    // Parameter passed to Bucket.tryLock. For new Lock instances, value will be stored as-is
    // into Lock.mLockCount field, which is why the numbers seem a bit weird.
    public static final int TYPE_SHARED = 1, TYPE_UPGRADABLE = 0x80000000, TYPE_EXCLUSIVE = ~0;

    final WeakReference<LocalDatabase> mDatabaseRef;

    final LockUpgradeRule mDefaultLockUpgradeRule;
    final long mDefaultTimeoutNanos;

    private final Bucket[] mBuckets;
    private final int mBucketShift;

    private final ThreadLocal<SoftReference<Locker>> mLocalLockerRef;

    /**
     * @param db optional; used by DeadlockDetector to resolve index names
     */
    LockManager(LocalDatabase db, LockUpgradeRule lockUpgradeRule, long timeoutNanos) {
        this(db, lockUpgradeRule, timeoutNanos, Runtime.getRuntime().availableProcessors() * 16);
    }

    private LockManager(LocalDatabase db, LockUpgradeRule lockUpgradeRule, long timeoutNanos,
                        int numBuckets)
    {
        mDatabaseRef = db == null ? null : new WeakReference<>(db);

        if (lockUpgradeRule == null) {
            lockUpgradeRule = LockUpgradeRule.STRICT;
        }
        mDefaultLockUpgradeRule = lockUpgradeRule;
        mDefaultTimeoutNanos = timeoutNanos;

        numBuckets = Utils.roundUpPower2(Math.max(2, numBuckets));
        mBuckets = new Bucket[numBuckets];
        for (int i=0; i<numBuckets; i++) {
            mBuckets[i] = new Bucket();
        }
        mBucketShift = Integer.numberOfLeadingZeros(numBuckets - 1);

        mLocalLockerRef = new ThreadLocal<>();
    }

    final Index indexById(long id) {
        if (mDatabaseRef != null) {
            LocalDatabase db = mDatabaseRef.get();
            if (db != null) {
                try {
                    return db.indexById(id);
                } catch (Exception e) {
                }
            }
        }

        return null;
    }

    /**
     * @return total number of locks actively held, of any type
     */
    public long numLocksHeld() {
        long count = 0;
        for (Bucket bucket : mBuckets) {
            count += bucket.size();
        }
        return count;
    }

    /**
     * Returns true if a shared lock can be granted for the given key. Caller must hold the
     * node latch which contains the key.
     *
     * @param locker optional locker
     */
    final boolean isAvailable(Locker locker, long indexId, byte[] key, int hash) {
        // Note that no Bucket latch is acquired. The current thread is not required to
        // immediately observe the activity of other threads acting upon the same lock. If
        // another thread has just acquired an exclusive lock, it must still acquire the node
        // latch before any changes can be made.
        return getBucket(hash).isAvailable(locker, indexId, key, hash);
    }

    final LockResult check(Locker locker, long indexId, byte[] key, int hash) {
        Bucket bucket = getBucket(hash);
        bucket.acquireShared();
        try {
            Lock lock = bucket.lockFor(indexId, key, hash);
            return lock == null ? UNOWNED : lock.check(locker);
        } finally {
            bucket.releaseShared();
        }
    }

    final void unlock(Locker locker, Lock lock) {
        Bucket bucket = getBucket(lock.mHashCode);
        bucket.acquireExclusive();
        try {
            lock.unlock(locker, bucket);
        } catch (Throwable e) {
            bucket.releaseExclusive();
            throw e;
        }
    }

    final void doUnlock(Locker locker, Lock lock) {
        Bucket bucket = getBucket(lock.mHashCode);
        bucket.acquireExclusive();
        try {
            lock.doUnlock(locker, bucket);
        } catch (Throwable e) {
            bucket.releaseExclusive();
            throw e;
        }
    }

    final void unlockToShared(Locker locker, Lock lock) {
        Bucket bucket = getBucket(lock.mHashCode);
        bucket.acquireExclusive();
        try {
            lock.unlockToShared(locker, bucket);
        } catch (Throwable e) {
            bucket.releaseExclusive();
            throw e;
        }
    }

    final void doUnlockToShared(Locker locker, Lock lock) {
        Bucket bucket = getBucket(lock.mHashCode);
        bucket.acquireExclusive();
        try {
            lock.doUnlockToShared(locker, bucket);
        } catch (Throwable e) {
            bucket.releaseExclusive();
            throw e;
        }
    }

    final void doUnlockToUpgradable(Locker locker, Lock lock) {
        Bucket bucket = getBucket(lock.mHashCode);
        bucket.acquireExclusive();
        try {
            lock.doUnlockToUpgradable(locker, bucket);
        } catch (Throwable e) {
            bucket.releaseExclusive();
            throw e;
        }
    }

    /**
     * Take ownership of an upgradable or exclusive lock.
     */
    final void takeLockOwnership(Lock lock, Locker locker) {
        Bucket bucket = getBucket(lock.mHashCode);
        bucket.acquireExclusive();
        try {
            if (lock.mLockCount < 0) {
                lock.mOwner = locker;
            }
        } finally {
            bucket.releaseExclusive();
        }
    }

    /**
     * Mark a lock as referencing a ghosted entry. Caller must ensure that lock
     * is already exclusively held.
     *
     * @param frame must be bound to the ghost position
     */
    final void ghosted(long indexId, byte[] key, int hash, GhostFrame frame) {
        Bucket bucket = getBucket(hash);
        bucket.acquireExclusive();
        try {
            bucket.lockFor(indexId, key, hash).setGhostFrame(frame);
        } finally {
            bucket.releaseExclusive();
        }
    }

    final Locker lockSharedLocal(long indexId, byte[] key, int hash) throws LockFailureException {
        Locker locker = localLocker();
        LockResult result = getBucket(hash)
            .tryLock(TYPE_SHARED, locker, indexId, key, hash, mDefaultTimeoutNanos);
        if (result.isHeld()) {
            return locker;
        }
        throw locker.failed(TYPE_SHARED, result, mDefaultTimeoutNanos);
    }

    final Locker lockExclusiveLocal(long indexId, byte[] key, int hash)
        throws LockFailureException
    {
        return lockExclusiveLocal(indexId, key, hash, mDefaultTimeoutNanos);
    }

    final Locker lockExclusiveLocal(long indexId, byte[] key, int hash, long timeoutNanos)
        throws LockFailureException
    {
        Locker locker = localLocker();
        LockResult result = getBucket(hash)
            .tryLock(TYPE_EXCLUSIVE, locker, indexId, key, hash, timeoutNanos);
        if (result.isHeld()) {
            return locker;
        }
        throw locker.failed(TYPE_EXCLUSIVE, result, timeoutNanos);
    }

    final Locker localLocker() {
        SoftReference<Locker> lockerRef = mLocalLockerRef.get();
        Locker locker;
        if (lockerRef == null || (locker = lockerRef.get()) == null) {
            mLocalLockerRef.set(new SoftReference<>(locker = new Locker(this)));
        }
        return locker;
    }

    final DetachedLockImpl newDetachedLock(LocalTransaction owner) {
        var lock = new DetachedLockImpl();
        initDetachedLock(lock, owner);
        return lock;
    }

    final void initDetachedLock(DetachedLockImpl lock, LocalTransaction owner) {
        int hash = ThreadLocalRandom.current().nextInt();
        lock.init(hash, owner, getBucket(hash));
    }

    /**
     * Interrupts all waiters, and exclusive locks are transferred to hidden
     * locker. This prevents them from being acquired again.
     */
    final void close() {
        var locker = new Locker(null);
        for (Bucket bucket : mBuckets) {
            bucket.close(locker);
        }
        if (mDatabaseRef != null) {
            mDatabaseRef.clear();
        }
    }

    final static int hash(long indexId, byte[] key) {
        return (int) Hasher.hash(indexId, key);
    }

    Bucket getBucket(int hash) {
        return mBuckets[hash >>> mBucketShift];
    }

    /**
     * Simple hashtable of Locks.
     */
    static final class Bucket extends Latch {
        private static final float LOAD_FACTOR = 0.75f;

        private Lock[] mEntries;
        private int mSize;
        private int mGrowThreshold;

        // Increments with each rehash or when the close method is called. Is negative when
        // either of these operations is in progress, and is positive otherwise.
        private volatile int mStamp;

        // Padding to prevent cache line sharing.
        private long a0, a1, a2;

        Bucket() {
            // Initial capacity of must be a power of 2.
            mEntries = new Lock[16];
            mGrowThreshold = (int) (mEntries.length * LOAD_FACTOR);
        }

        int size() {
            acquireShared();
            int size = mSize;
            releaseShared();
            return size;
        }

        /**
         * Returns true if a shared lock can be granted for the given key. Caller must hold the
         * node latch which contains the key.
         *
         * @param locker optional locker
         */
        boolean isAvailable(Locker locker, long indexId, byte[] key, int hash) {
            // Optimistically find the lock.
            int stamp = mStamp;
            if (stamp >= 0) {
                Lock[] entries = mEntries;
                int index = hash & (entries.length - 1);
                for (Lock e = entries[index]; e != null; ) {
                    VarHandle.loadLoadFence();
                    if (e.matches(indexId, key, hash)) {
                        return e.isAvailable(locker);
                    }
                    e = e.mLockNext;
                }
                // Not found.
                if (stamp == mStamp) {
                    return true;
                }
            }

            Lock lock;
            acquireShared();
            try {
                lock = lockFor(indexId, key, hash);
            } finally {
                releaseShared();
            }

            return lock == null || lock.isAvailable(locker);
        }

        /**
         * Finds a lock or returns null if not found. Caller must hold latch.
         *
         * @return null if not found
         */
        Lock lockFor(long indexId, byte[] key, int hash) {
            Lock[] entries = mEntries;
            int index = hash & (entries.length - 1);
            for (Lock e = entries[index]; e != null; e = e.mLockNext) {
                if (e.matches(indexId, key, hash)) {
                    return e;
                }
            }
            return null;
        }

        /**
         * Finds or creates a lock. Caller must hold exclusive latch.
         */
        Lock lockAccess(long indexId, byte[] key, int hash) {
            Lock[] entries = mEntries;
            int index = hash & (entries.length - 1);
            for (Lock lock = entries[index]; lock != null; lock = lock.mLockNext) {
                if (lock.matches(indexId, key, hash)) {
                    return lock;
                }
            }

            if (mSize >= mGrowThreshold) {
                entries = rehash(entries);
                index = hash & (entries.length - 1);
            }

            var lock = new Lock();

            lock.mIndexId = indexId;
            lock.mKey = key;
            lock.mHashCode = hash;
            lock.mLockNext = entries[index];

            // Fence so that the isAvailable method doesn't observe a broken chain.
            VarHandle.storeStoreFence();
            entries[index] = lock;

            mSize++;

            return lock;
        }

        /**
         * @param type TYPE_*
         */
        LockResult tryLock(int type,
                           Locker locker, long indexId, byte[] key, int hash,
                           long nanosTimeout)
        {
            Lock lock;
            LockResult result;
            lockEx: {
                lockNonEx: {
                    acquireExclusive();
                    try {
                        Lock[] entries = mEntries;
                        int index = hash & (entries.length - 1);
                        for (lock = entries[index]; lock != null; lock = lock.mLockNext) {
                            if (lock.matches(indexId, key, hash)) {
                                if (type == TYPE_SHARED) {
                                    result = lock.tryLockShared(this, locker, nanosTimeout);
                                    break lockNonEx;
                                } else if (type == TYPE_UPGRADABLE) {
                                    result = lock.tryLockUpgradable(this, locker, nanosTimeout);
                                    break lockNonEx;
                                } else {
                                    result = lock.tryLockExclusive(this, locker, nanosTimeout);
                                    break lockEx;
                                }
                            }
                        }

                        if (mSize >= mGrowThreshold) {
                            entries = rehash(entries);
                            index = hash & (entries.length - 1);
                        }

                        lock = new Lock();

                        lock.mIndexId = indexId;
                        lock.mKey = key;
                        lock.mHashCode = hash;
                        lock.mLockNext = entries[index];

                        lock.mLockCount = type;
                        if (type == TYPE_SHARED) {
                            lock.setSharedLocker(locker);
                        } else {
                            lock.mOwner = locker;
                        }

                        // Fence so that the isAvailable method doesn't observe a broken chain.
                        VarHandle.storeStoreFence();
                        entries[index] = lock;

                        mSize++;
                    } finally {
                        releaseExclusive();
                    }

                    locker.push(lock);
                    return ACQUIRED;
                }

                // Result of shared/upgradable attempt for existing Lock.

                if (result == ACQUIRED) {
                    locker.push(lock);
                }

                return result;
            }

            // Result of exclusive attempt for existing Lock.

            if (result == ACQUIRED) {
                locker.push(lock);
            } else if (result == UPGRADED) {
                locker.pushUpgrade(lock);
            }

            return result;
        }

        /**
         * @param lock Lock instance to insert, unless another already exists. The mIndexId,
         * mKey, and mHashCode fields must be set.
         */
        void recoverLock(Locker locker, Lock lock) {
            int hash = lock.mHashCode;

            acquireExclusive();
            try {
                Lock[] entries = mEntries;
                int index = hash & (entries.length - 1);
                for (Lock e = entries[index]; e != null; e = e.mLockNext) {
                    if (e.matches(lock.mIndexId, lock.mKey, hash)) {
                        // Lock already exists, but make sure lock upgrades are captured
                        // and any ghost frame is preserved.
                        if (lock.mLockCount == ~0) {
                            e.mLockCount = ~0;
                        }
                        Object ghost = lock.getSharedLocker();
                        if (ghost instanceof GhostFrame gf) {
                            e.setGhostFrame(gf);
                        }
                        return;
                    }
                }

                if (mSize >= mGrowThreshold) {
                    entries = rehash(entries);
                    index = hash & (entries.length - 1);
                }

                lock.mLockNext = entries[index];
                lock.mOwner = locker;

                // Fence so that the isAvailable method doesn't observe a broken chain.
                VarHandle.storeStoreFence();
                entries[index] = lock;

                mSize++;
            } finally {
                releaseExclusive();
            }

            locker.push(lock);
        }

        /**
         * Caller must hold latch and ensure that Lock is in this Bucket.
         *
         * @throws NullPointerException if lock is not in this Bucket
         */
        void remove(Lock lock) {
            Lock[] entries = mEntries;
            int index = lock.mHashCode & (entries.length - 1);
            Lock e = entries[index];
            if (e == lock) {
                entries[index] = e.mLockNext;
            } else while (true) {
                Lock next = e.mLockNext;
                if (next == lock) {
                    e.mLockNext = next.mLockNext;
                    break;
                }
                e = next;
            }
            mSize--;
        }

        void close(Locker locker) {
            acquireExclusive();
            try {
                if (mSize > 0) {
                    // Signal that close is in progress.
                    mStamp |= 0x80000000;

                    Lock[] entries = mEntries;
                    for (int i=entries.length; --i>=0 ;) {
                        for (Lock e = entries[i], prev = null; e != null; ) {
                            Lock next = e.mLockNext;

                            if (e.mLockCount == ~0) {
                                // Transfer exclusive lock.
                                e.mOwner = locker;
                                prev = e;
                            } else {
                                // Release and remove lock.
                                e.mLockCount = 0;
                                e.mOwner = null;
                                if (prev == null) {
                                    entries[i] = next;
                                } else {
                                    prev.mLockNext = next;
                                }
                                e.mLockNext = null;
                                mSize--;
                            }

                            e.setSharedLocker(null);

                            // Interrupt all waiters.

                            Latch.Condition q = e.mQueueU;
                            if (q != null) {
                                q.clear();
                                e.mQueueU = null;
                            }

                            q = e.mQueueSX;
                            if (q != null) {
                                q.clear();
                                e.mQueueSX = null;
                            }

                            e = next;
                        }
                    }

                    mStamp = (mStamp + 1) & ~0x80000000;
                }
            } finally {
                releaseExclusive();
            }
        }

        private Lock[] rehash(Lock[] entries) {
            int capacity = entries.length << 1;
            var newEntries = new Lock[capacity];
            int newMask = capacity - 1;

            // Signal that rehash is in progress.
            mStamp |= 0x80000000;

            for (int i=entries.length; --i>=0 ;) {
                for (Lock e = entries[i]; e != null; ) {
                    Lock next = e.mLockNext;
                    int ix = e.mHashCode & newMask;
                    e.mLockNext = newEntries[ix];
                    newEntries[ix] = e;
                    e = next;
                }
            }

            mEntries = entries = newEntries;
            mStamp = (mStamp + 1) & ~0x80000000;

            mGrowThreshold = (int) (capacity * LOAD_FACTOR);
            return entries;
        }
    }
}
