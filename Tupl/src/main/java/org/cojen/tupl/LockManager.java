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

import static org.cojen.tupl.LockResult.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class LockManager {
    private final long mDefaultTimeoutNanos;

    private final LockHT[] mHashTables;
    private final int mHashTableMask;

    private final ThreadLocal<Locker> mLocalLocker;

    LockManager(long timeoutNanos) {
        this(timeoutNanos, Runtime.getRuntime().availableProcessors() * 16);
    }

    private LockManager(long timeoutNanos, int numHashTables) {
        mDefaultTimeoutNanos = timeoutNanos;

        numHashTables = Utils.roundUpPower2(numHashTables);
        mHashTables = new LockHT[numHashTables];
        for (int i=0; i<numHashTables; i++) {
            mHashTables[i] = new LockHT();
        }
        mHashTableMask = numHashTables - 1;

        mLocalLocker = new ThreadLocal<Locker>();
    }

    /**
     * @return total number of locks actively held, of any type
     */
    public int numLocksHeld() {
        int count = 0;
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
    final boolean isAvailable(Locker locker, long indexId, byte[] key, int hash) {
        LockHT ht = getLockHT(hash);
        Latch latch = ht.mLatch;
        latch.acquireSharedUnfair();
        try {
            Lock lock = ht.lockFor(indexId, key, hash, false);
            return lock == null ? true : lock.isAvailable(locker);
        } finally {
            latch.releaseShared();
        }
    }

    final LockResult check(Locker locker, long indexId, byte[] key, int hash) {
        LockHT ht = getLockHT(hash);
        Latch latch = ht.mLatch;
        latch.acquireSharedUnfair();
        try {
            Lock lock = ht.lockFor(indexId, key, hash, false);
            return lock == null ? LockResult.UNOWNED : lock.check(locker);
        } finally {
            latch.releaseShared();
        }
    }

    final LockResult tryLockShared(Locker locker, long indexId, byte[] key, int hash,
                                   long nanosTimeout)
    {
        LockHT ht = getLockHT(hash);

        Lock lock;
        LockResult result;
        {
            Latch latch = ht.mLatch;
            latch.acquireExclusiveUnfair();
            try {
                lock = ht.lockFor(indexId, key, hash, true);
                result = lock.tryLockShared(latch, locker, nanosTimeout);
            } finally {
                latch.releaseExclusive();
            }
        }

        if (result == ACQUIRED) {
            locker.push(lock, 0);
        }

        return result;
    }

    final LockResult tryLockUpgradable(Locker locker, long indexId, byte[] key, int hash,
                                       long nanosTimeout)
    {
        LockHT ht = getLockHT(hash);

        Lock lock;
        LockResult result;
        {
            Latch latch = ht.mLatch;
            latch.acquireExclusiveUnfair();
            try {
                lock = ht.lockFor(indexId, key, hash, true);
                result = lock.tryLockUpgradable(latch, locker, nanosTimeout);
            } finally {
                latch.releaseExclusive();
            }
        }

        if (result == ACQUIRED) {
            locker.push(lock, 0);
        }

        return result;
    }

    final LockResult tryLockExclusive(Locker locker, long indexId, byte[] key, int hash,
                                      long nanosTimeout)
    {
        LockHT ht = getLockHT(hash);

        Lock lock;
        LockResult result;
        {
            Latch latch = ht.mLatch;
            latch.acquireExclusiveUnfair();
            try {
                lock = ht.lockFor(indexId, key, hash, true);
                result = lock.tryLockExclusive(latch, locker, nanosTimeout);
            } finally {
                latch.releaseExclusive();
            }
        }

        if (result == ACQUIRED) {
            locker.push(lock, 0);
        } else if (result == UPGRADED) {
            locker.push(lock, 1);
        }

        return result;
    }

    final void unlock(Locker locker, Lock lock) {
        int hash = lock.mHashCode;
        getLockHT(hash).unlock(locker, lock.mIndexId, lock.mKey, hash);
    }

    final void unlockToShared(Locker locker, Lock lock) {
        LockHT ht = getLockHT(lock.mHashCode);
        Latch latch = ht.mLatch;
        latch.acquireExclusiveUnfair();
        try {
            lock.unlockToShared(locker);
        } finally {
            latch.releaseExclusive();
        }
    }

    final void unlockToUpgradable(Locker locker, Lock lock) {
        LockHT ht = getLockHT(lock.mHashCode);
        Latch latch = ht.mLatch;
        latch.acquireExclusiveUnfair();
        try {
            lock.unlockToUpgradable(locker);
        } finally {
            latch.releaseExclusive();
        }
    }

    /*
    final boolean unlockIfNonExclusive(Locker locker, Lock lock) {
        LockHT ht = getLockHT(lock.mHashCode);
        Latch latch = ht.mLatch;
        latch.acquireExclusiveUnfair();
        try {
            return lock.unlockIfNonExclusive(locker);
        } finally {
            latch.releaseExclusive();
        }
    }
    */

    final Locker lockSharedLocal(long indexId, byte[] key) throws LockFailureException {
        Locker locker = localLocker();
        locker.lockShared(indexId, key, mDefaultTimeoutNanos);
        return locker;
    }

    final Locker lockExclusiveLocal(long indexId, byte[] key) throws LockFailureException {
        Locker locker = localLocker();
        locker.lockExclusive(indexId, key, mDefaultTimeoutNanos);
        return locker;
    }

    final Locker localLocker() {
        Locker locker = mLocalLocker.get();
        if (locker == null) {
            mLocalLocker.set(locker = new Locker(this));
        }
        return locker;
    }

    final static int hash(long indexId, byte[] key) {
        int hash = ((int) indexId) ^ ((int) (indexId >>> 32));
        for (int i=key.length; --i>=0; ) {
            hash = hash * 31 + key[i];
        }
        // Scramble the hashcode a bit, just like HashMap does.
        hash ^= (hash >>> 20) ^ (hash >>> 12);
        return hash ^ (hash >>> 7) ^ (hash >>> 4);
    }

    private LockHT getLockHT(int hash) {
        return mHashTables[hash & mHashTableMask];
    }

    /**
     * Simple hashtable of Locks.
     */
    static final class LockHT {
        private static final float LOAD_FACTOR = 0.75f;

        private Lock[] mEntries;
        private int mSize;
        private int mGrowThreshold;

        final Latch mLatch;

        LockHT() {
            // Initial capacity of must be a power of 2.
            mEntries = new Lock[16];
            mGrowThreshold = (int) (mEntries.length * LOAD_FACTOR);
            mLatch = new Latch();
        }

        int size() {
            Latch latch = mLatch;
            latch.acquireExclusiveUnfair();
            int size = mSize;
            latch.releaseExclusive();
            return size;
        }

        /**
         * Caller must hold latch.
         */
        Lock lockFor(long indexId, byte[] key, int hash, boolean create) {
            Lock[] entries = mEntries;
            int index = hash & (entries.length - 1);
            for (Lock e = entries[index]; e != null; e = e.mLockManagerNext) {
                if (e.matches(indexId, key, hash)) {
                    return e;
                }
            }

            if (!create) {
                return null;
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

            Lock lock = new Lock();
            lock.mIndexId = indexId;
            lock.mKey = key;
            lock.mHashCode = hash;
            lock.mLockManagerNext = entries[index];
            entries[index] = lock;
            mSize++;

            return lock;
        }

        void unlock(Locker locker, long indexId, byte[] key, int hash) {
            Latch latch = mLatch;
            latch.acquireExclusiveUnfair();
            try {
                Lock[] entries = mEntries;
                int index = hash & (entries.length - 1);
                for (Lock e = entries[index], prev = null; e != null; e = e.mLockManagerNext) {
                    if (e.matches(indexId, key, hash)) {
                        if (e.unlock(locker)) {
                            // Remove last use of lock.
                            if (prev == null) {
                                entries[index] = e.mLockManagerNext;
                            } else {
                                prev.mLockManagerNext = e.mLockManagerNext;
                            }
                            mSize--;
                        }
                        return;
                    } else {
                        prev = e;
                    }
                }
            } finally {
                latch.releaseExclusive();
            }

            throw new IllegalStateException("Lock not held");
        }
    }
}
