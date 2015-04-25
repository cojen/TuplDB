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

import java.lang.ref.WeakReference;

import static org.cojen.tupl.LockResult.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class LockManager {
    // Parameter passed to LockHT.tryLock. For new Lock instances, value will be stored as-is
    // into Lock.mLockCount field, which is why the numbers seem a bit weird.
    static final int TYPE_SHARED = 1, TYPE_UPGRADABLE = 0x80000000, TYPE_EXCLUSIVE = ~0;

    final LockUpgradeRule mDefaultLockUpgradeRule;
    final long mDefaultTimeoutNanos;

    private final LockHT[] mHashTables;
    private final int mHashTableShift;

    private final ThreadLocal<WeakReference<Locker>> mLocalLockerRef;

    LockManager(LockUpgradeRule lockUpgradeRule, long timeoutNanos) {
        this(lockUpgradeRule, timeoutNanos, Runtime.getRuntime().availableProcessors() * 16);
    }

    private LockManager(LockUpgradeRule lockUpgradeRule, long timeoutNanos, int numHashTables) {
        if (lockUpgradeRule == null) {
            lockUpgradeRule = LockUpgradeRule.STRICT;
        }
        mDefaultLockUpgradeRule = lockUpgradeRule;
        mDefaultTimeoutNanos = timeoutNanos;

        numHashTables = Utils.roundUpPower2(Math.max(2, numHashTables));
        mHashTables = new LockHT[numHashTables];
        for (int i=0; i<numHashTables; i++) {
            mHashTables[i] = new LockHT();
        }
        mHashTableShift = Integer.numberOfLeadingZeros(numHashTables - 1);

        mLocalLockerRef = new ThreadLocal<>();
    }

    /**
     * @return total number of locks actively held, of any type
     */
    public long numLocksHeld() {
        long count = 0;
        for (LockHT ht : mHashTables) {
            count += ht.size();
        }
        return count;
    }

    /**
     * Returns true if a shared lock can be immediately granted. Caller must
     * hold a coarse latch to prevent this state from changing.
     *
     * @param locker optional locker
     */
    final boolean isAvailable(LockOwner locker, long indexId, byte[] key, int hash) {
        LockHT ht = getLockHT(hash);
        ht.acquireShared();
        try {
            Lock lock = ht.lockFor(indexId, key, hash);
            return lock == null ? true : lock.isAvailable(locker);
        } finally {
            ht.releaseShared();
        }
    }

    final LockResult check(LockOwner locker, long indexId, byte[] key, int hash) {
        LockHT ht = getLockHT(hash);
        ht.acquireShared();
        try {
            Lock lock = ht.lockFor(indexId, key, hash);
            return lock == null ? LockResult.UNOWNED : lock.check(locker);
        } finally {
            ht.releaseShared();
        }
    }

    final void unlock(LockOwner locker, Lock lock) {
        LockHT ht = getLockHT(lock.mHashCode);
        ht.acquireExclusive();
        try {
            if (lock.unlock(locker, ht)) {
                ht.remove(lock);
            }
        } finally {
            ht.releaseExclusive();
        }
    }

    final void unlockToShared(LockOwner locker, Lock lock) {
        LockHT ht = getLockHT(lock.mHashCode);
        ht.acquireExclusive();
        try {
            lock.unlockToShared(locker, ht);
        } finally {
            ht.releaseExclusive();
        }
    }

    final void unlockToUpgradable(LockOwner locker, Lock lock) {
        LockHT ht = getLockHT(lock.mHashCode);
        ht.acquireExclusive();
        try {
            lock.unlockToUpgradable(locker, ht);
        } finally {
            ht.releaseExclusive();
        }
    }

    final PendingTxn transferExclusive(LockOwner locker, Lock lock, PendingTxn pending) {
        LockHT ht = getLockHT(lock.mHashCode);
        ht.acquireExclusive();
        try {
            return lock.transferExclusive(locker, pending);
        } finally {
            ht.releaseExclusive();
        }
    }

    /**
     * Mark a lock as referencing a ghosted entry. Caller must ensure that lock
     * is already exclusively held.
     */
    final void ghosted(Tree tree, byte[] key, int hash) {
        LockHT ht = getLockHT(hash);
        ht.acquireExclusive();
        try {
            ht.lockFor(tree.mId, key, hash).mSharedLockOwnersObj = tree;
        } finally {
            ht.releaseExclusive();
        }
    }

    final Locker lockSharedLocal(long indexId, byte[] key, int hash) throws LockFailureException {
        Locker locker = localLocker();
        LockResult result = getLockHT(hash)
            .tryLock(TYPE_SHARED, locker, indexId, key, hash, mDefaultTimeoutNanos);
        if (result.isHeld()) {
            return locker;
        }
        throw locker.failed(result, mDefaultTimeoutNanos);
    }

    final Locker lockExclusiveLocal(long indexId, byte[] key, int hash)
        throws LockFailureException
    {
        Locker locker = localLocker();
        LockResult result = getLockHT(hash)
            .tryLock(TYPE_EXCLUSIVE, locker, indexId, key, hash, mDefaultTimeoutNanos);
        if (result.isHeld()) {
            return locker;
        }
        throw locker.failed(result, mDefaultTimeoutNanos);
    }

    final Locker localLocker() {
        WeakReference<Locker> lockerRef = mLocalLockerRef.get();
        Locker locker;
        if (lockerRef == null || (locker = lockerRef.get()) == null) {
            mLocalLockerRef.set(new WeakReference<>(locker = new Locker(this)));
        }
        return locker;
    }

    /**
     * Interrupts all waiters, and exclusive locks are transferred to hidden
     * locker. This prevents them from being acquired again.
     */
    final void close() {
        Locker locker = new Locker();
        for (LockHT ht : mHashTables) {
            ht.close(locker);
        }
    }

    final static int hash(long indexId, byte[] key) {
        return (int) Hasher.hash(indexId, key);
    }

    LockHT getLockHT(int hash) {
        return mHashTables[hash >>> mHashTableShift];
    }

    /**
     * Simple hashtable of Locks.
     */
    static final class LockHT extends Latch {
        private static final float LOAD_FACTOR = 0.75f;

        private transient Lock[] mEntries;
        private int mSize;
        private int mGrowThreshold;

        // Padding to prevent cache line sharing.
        private long a0, a1, a2, a3;

        LockHT() {
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
         * Caller must hold latch.
         *
         * @return null if not found
         */
        Lock lockFor(long indexId, byte[] key, int hash) {
            Lock[] entries = mEntries;
            int index = hash & (entries.length - 1);
            for (Lock e = entries[index]; e != null; e = e.mLockManagerNext) {
                if (e.matches(indexId, key, hash)) {
                    return e;
                }
            }
            return null;
        }

        /**
         * @param type defined in Lock class
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
                        for (lock = entries[index]; lock != null; lock = lock.mLockManagerNext) {
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
                            int capacity = entries.length << 1;
                            Lock[] newEntries = new Lock[capacity];
                            int newMask = capacity - 1;

                            for (int i=entries.length; --i>=0 ;) {
                                for (Lock e = entries[i]; e != null; ) {
                                    Lock next = e.mLockManagerNext;
                                    int ix = e.mHashCode & newMask;
                                    e.mLockManagerNext = newEntries[ix];
                                    newEntries[ix] = e;
                                    e = next;
                                }
                            }

                            mEntries = entries = newEntries;
                            mGrowThreshold = (int) (capacity * LOAD_FACTOR);
                            index = hash & newMask;
                        }

                        lock = new Lock();

                        lock.mIndexId = indexId;
                        lock.mKey = key;
                        lock.mHashCode = hash;
                        lock.mLockManagerNext = entries[index];

                        lock.mLockCount = type;
                        if (type == TYPE_SHARED) {
                            lock.mSharedLockOwnersObj = locker;
                        } else {
                            lock.mOwner = locker;
                        }

                        entries[index] = lock;
                        mSize++;
                    } finally {
                        releaseExclusive();
                    }

                    locker.push(lock, 0);
                    return LockResult.ACQUIRED;
                }

                // Result of shared/upgradable attempt for existing Lock.

                if (result == ACQUIRED) {
                    locker.push(lock, 0);
                }

                return result;
            }

            // Result of exclusive attempt for existing Lock.

            if (result == ACQUIRED) {
                locker.push(lock, 0);
            } else if (result == UPGRADED) {
                locker.push(lock, 1);
            }

            return result;
        }

        /**
         * @param newLock Lock instance to insert, unless another already exists. The mIndexId,
         * mKey, and mHashCode fields must be set.
         */
        LockResult tryLockExclusive(Locker locker, Lock newLock, long nanosTimeout) {
            int hash = newLock.mHashCode;

            Lock lock;
            LockResult result;
            lockEx: {
                acquireExclusive();
                try {
                    Lock[] entries = mEntries;
                    int index = hash & (entries.length - 1);
                    for (lock = entries[index]; lock != null; lock = lock.mLockManagerNext) {
                        if (lock.matches(newLock.mIndexId, newLock.mKey, hash)) {
                            result = lock.tryLockExclusive(this, locker, nanosTimeout);
                            break lockEx;
                        }
                    }

                    if (mSize >= mGrowThreshold) {
                        int capacity = entries.length << 1;
                        Lock[] newEntries = new Lock[capacity];
                        int newMask = capacity - 1;

                        for (int i=entries.length; --i>=0 ;) {
                            for (Lock e = entries[i]; e != null; ) {
                                Lock next = e.mLockManagerNext;
                                int ix = e.mHashCode & newMask;
                                e.mLockManagerNext = newEntries[ix];
                                newEntries[ix] = e;
                                e = next;
                            }
                        }

                        mEntries = entries = newEntries;
                        mGrowThreshold = (int) (capacity * LOAD_FACTOR);
                        index = hash & newMask;
                    }

                    lock = newLock;
                    lock.mLockManagerNext = entries[index];
                    lock.mLockCount = ~0;
                    lock.mOwner = locker;

                    entries[index] = lock;
                    mSize++;
                } finally {
                    releaseExclusive();
                }

                locker.push(lock, 0);
                return LockResult.ACQUIRED;
            }

            if (result == ACQUIRED) {
                locker.push(lock, 0);
            } else if (result == UPGRADED) {
                locker.push(lock, 1);
            }

            return result;
        }

        /**
         * Caller must hold latch and ensure that Lock is in hashtable.
         *
         * @throws NullPointerException if lock is not in hashtable
         */
        void remove(Lock lock) {
            Lock[] entries = mEntries;
            int index = lock.mHashCode & (entries.length - 1);
            Lock e = entries[index];
            if (e == lock) {
                entries[index] = e.mLockManagerNext;
            } else while (true) {
                Lock next = e.mLockManagerNext;
                if (next == lock) {
                    e.mLockManagerNext = next.mLockManagerNext;
                    break;
                }
                e = next;
            }
            mSize--;
        }

        void close(LockOwner locker) {
            acquireExclusive();
            try {
                if (mSize > 0) {
                    Lock[] entries = mEntries;
                    for (int i=entries.length; --i>=0 ;) {
                        for (Lock e = entries[i], prev = null; e != null; ) {
                            Lock next = e.mLockManagerNext;

                            if (e.mLockCount == ~0) {
                                // Transfer exclusive lock.
                                e.mOwner = locker;
                            } else {
                                // Release and remove lock.
                                e.mLockCount = 0;
                                e.mOwner = null;
                                if (prev == null) {
                                    entries[i] = next;
                                } else {
                                    prev.mLockManagerNext = next;
                                }
                                e.mLockManagerNext = null;
                                mSize--;
                            }

                            e.mSharedLockOwnersObj = null;

                            // Interrupt all waiters.

                            WaitQueue q = e.mQueueU;
                            if (q != null) {
                                q.clear();
                                e.mQueueU = null;
                            }

                            q = e.mQueueSX;
                            if (q != null) {
                                q.clear();
                                e.mQueueSX = null;
                            }

                            prev = e;
                            e = next;
                        }
                    }
                }
            } finally {
                releaseExclusive();
            }
        }
    }
}
