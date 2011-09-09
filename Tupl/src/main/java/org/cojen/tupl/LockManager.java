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

    final LockResult lockShared(Locker locker, int hash, byte[] key, long nanosTimeout) {
        LockHT ht = getLockHT(hash);
        Latch latch = ht.mLatch;
        latch.acquireExclusiveUnfair();
        try {
            return ht.lockFor(hash, key).lockShared(latch, locker, nanosTimeout);
        } finally {
            latch.releaseExclusive();
        }
    }

    final LockResult lockUpgradable(Locker locker, int hash, byte[] key, long nanosTimeout) {
        LockHT ht = getLockHT(hash);
        Latch latch = ht.mLatch;
        latch.acquireExclusiveUnfair();
        try {
            return ht.lockFor(hash, key).lockUpgradable(latch, locker, nanosTimeout);
        } finally {
            latch.releaseExclusive();
        }
    }

    final LockResult lockExclusive(Locker locker, int hash, byte[] key, long nanosTimeout) {
        LockHT ht = getLockHT(hash);
        Latch latch = ht.mLatch;
        latch.acquireExclusiveUnfair();
        try {
            return ht.lockFor(hash, key).lockExclusive(latch, locker, nanosTimeout);
        } finally {
            latch.releaseExclusive();
        }
    }

    final void unlock(Locker locker, int hash, byte[] key) {
        getLockHT(hash).unlock(locker, hash, key);
    }

    final void unlockToShared(Locker locker, int hash, byte[] key) {
        LockHT ht = getLockHT(hash);
        Latch latch = ht.mLatch;
        latch.acquireExclusiveUnfair();
        try {
            ht.get(hash, key).unlockToShared(locker);
        } finally {
            latch.releaseExclusive();
        }
    }

    final void unlockToUpgradable(Locker locker, int hash, byte[] key) {
        LockHT ht = getLockHT(hash);
        Latch latch = ht.mLatch;
        latch.acquireExclusiveUnfair();
        try {
            ht.get(hash, key).unlockToUpgradable(locker);
        } finally {
            latch.releaseExclusive();
        }
    }

    final static int hashCode(byte[] key) {
        int hash = 0;
        for (int i=key.length; --i>=0;) {
            hash = hash * 31 + (key[i] & 0xff);
        }
        return hash;
    }

    private LockHT getLockHT(int hash) {
        return mHashTables[hash & mHashTableMask];
    }

    /**
     * Simple hashtable of Locks.
     */
    static class LockHT {
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

        /**
         * Caller must hold latch.
         */
        Lock get(int hash, byte[] key) {
            Lock[] entries = mEntries;
            int index = hash & (entries.length - 1);
            for (Lock e = entries[index]; e != null; e = e.mNext) {
                if (Arrays.equals(e.mKey, key)) {
                    return e;
                }
            }
            throw new IllegalStateException("Lock not held");
        }

        /**
         * Caller must hold latch.
         */
        Lock lockFor(int hash, byte[] key) {
            Lock[] entries = mEntries;
            int index = hash & (entries.length - 1);
            for (Lock e = entries[index]; e != null; e = e.mNext) {
                if (Arrays.equals(e.mKey, key)) {
                    return e;
                }
            }

            if (mSize >= mGrowThreshold) {
                int capacity = entries.length << 1;
                Lock[] newEntries = new Lock[capacity];
                int newMask = capacity - 1;

                for (int i=entries.length; --i>=0 ;) {
                    for (Lock e = entries[i]; e != null; ) {
                        Lock next = e.mNext;
                        int ix = LockManager.hashCode(e.mKey) & newMask;
                        e.mNext = newEntries[ix];
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
            lock.mNext = entries[index];
            entries[index] = lock;
            mSize++;
            return lock;
        }

        void unlock(Locker locker, int hash, byte[] key) {
            Latch latch = mLatch;
            latch.acquireExclusiveUnfair();
            try {
                Lock[] entries = mEntries;
                int index = hash & (entries.length - 1);
                for (Lock e = entries[index], prev = null; e != null; e = e.mNext) {
                    if (Arrays.equals(e.mKey, key)) {
                        if (e.unlock(locker)) {
                            // Remove last use of lock.
                            if (prev == null) {
                                entries[index] = e.mNext;
                            } else {
                                prev.mNext = e.mNext;
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
