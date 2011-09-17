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

import java.util.Arrays;

import static org.cojen.tupl.LockResult.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public final class LockManager {
    private final LockHT[] mHashTables;
    private final int mHashTableMask;

    public LockManager() {
        this(Runtime.getRuntime().availableProcessors() * 16);
    }

    private LockManager(int numHashTables) {
        // Round up to power of 2.
        numHashTables = Integer.highestOneBit(numHashTables - 1) << 1;
        mHashTables = new LockHT[numHashTables];
        for (int i=0; i<numHashTables; i++) {
            mHashTables[i] = new LockHT();
        }
        mHashTableMask = numHashTables - 1;
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

    final LockResult check(Locker locker, byte[] key) {
        int hash = hashCode(key);
        LockHT ht = getLockHT(hash);
        Latch latch = ht.mLatch;
        latch.acquireSharedUnfair();
        try {
            return ht.lockFor(key, hash).check(locker);
        } finally {
            latch.releaseShared();
        }
    }

    final LockResult lockShared(Locker locker, byte[] key, long nanosTimeout) {
        int hash = hashCode(key);
        LockHT ht = getLockHT(hash);

        Lock lock;
        LockResult result;
        {
            Latch latch = ht.mLatch;
            latch.acquireExclusiveUnfair();
            try {
                lock = ht.lockFor(key, hash);
                result = lock.lockShared(latch, locker, nanosTimeout);
            } finally {
                latch.releaseExclusive();
            }
        }

        if (result == ACQUIRED) {
            locker.push(lock, 0);
        }

        return result;
    }

    final LockResult lockUpgradable(Locker locker, byte[] key, long nanosTimeout) {
        int hash = hashCode(key);
        LockHT ht = getLockHT(hash);

        Lock lock;
        LockResult result;
        {
            Latch latch = ht.mLatch;
            latch.acquireExclusiveUnfair();
            try {
                lock = ht.lockFor(key, hash);
                result = lock.lockUpgradable(latch, locker, nanosTimeout);
            } finally {
                latch.releaseExclusive();
            }
        }

        if (result == ACQUIRED) {
            locker.push(lock, 0);
        }

        return result;
    }

    final LockResult lockExclusive(Locker locker, byte[] key, long nanosTimeout) {
        int hash = hashCode(key);
        LockHT ht = getLockHT(hash);

        Lock lock;
        LockResult result;
        {
            Latch latch = ht.mLatch;
            latch.acquireExclusiveUnfair();
            try {
                lock = ht.lockFor(key, hash);
                result = lock.lockExclusive(latch, locker, nanosTimeout);
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

    final byte[] unlock(Locker locker, Lock lock) {
        int hash = lock.mHashCode;
        byte[] key = lock.mKey;
        getLockHT(hash).unlock(locker, key, hash);
        return key;
    }

    final byte[] unlockToShared(Locker locker, Lock lock) {
        int hash = lock.mHashCode;
        byte[] key = lock.mKey;
        LockHT ht = getLockHT(hash);
        Latch latch = ht.mLatch;
        latch.acquireExclusiveUnfair();
        try {
            ht.get(key, hash).unlockToShared(locker);
        } finally {
            latch.releaseExclusive();
        }
        return key;
    }

    final byte[] unlockToUpgradable(Locker locker, Lock lock) {
        int hash = lock.mHashCode;
        byte[] key = lock.mKey;
        LockHT ht = getLockHT(hash);
        Latch latch = ht.mLatch;
        latch.acquireExclusiveUnfair();
        try {
            ht.get(key, hash).unlockToUpgradable(locker);
        } finally {
            latch.releaseExclusive();
        }
        return key;
    }

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

    final static int hashCode(byte[] key) {
        int hash = 0;
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
        Lock get(byte[] key, int hash) {
            Lock[] entries = mEntries;
            int index = hash & (entries.length - 1);
            for (Lock e = entries[index]; e != null; e = e.mLockManagerNext) {
                if (hash == e.mHashCode && Arrays.equals(e.mKey, key)) {
                    return e;
                }
            }
            throw new IllegalStateException("Lock not held");
        }

        /**
         * Caller must hold latch.
         */
        Lock lockFor(byte[] key, int hash) {
            Lock[] entries = mEntries;
            int index = hash & (entries.length - 1);
            for (Lock e = entries[index]; e != null; e = e.mLockManagerNext) {
                if (hash == e.mHashCode && Arrays.equals(e.mKey, key)) {
                    return e;
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

            Lock lock = new Lock();
            lock.mKey = key;
            lock.mHashCode = hash;
            lock.mLockManagerNext = entries[index];
            entries[index] = lock;
            mSize++;
            return lock;
        }

        void unlock(Locker locker, byte[] key, int hash) {
            Latch latch = mLatch;
            latch.acquireExclusiveUnfair();
            try {
                Lock[] entries = mEntries;
                int index = hash & (entries.length - 1);
                for (Lock e = entries[index], prev = null; e != null; e = e.mLockManagerNext) {
                    if (hash == e.mHashCode && Arrays.equals(e.mKey, key)) {
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
