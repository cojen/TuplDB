/*
 *  Copyright (C) 2021 Cojen.org
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
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.core;

import java.util.concurrent.TimeUnit;

import org.cojen.tupl.LockFailureException;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.util.Latch;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class DetachedLockImpl extends Lock implements DetachedLock {
    LockManager.Bucket mBucket;

    DetachedLockImpl() {
    }

    void init(int hash, LocalTransaction owner, LockManager.Bucket bucket) {
        mHashCode = hash;
        mLockCount = 0x80000000; // held upgradable by the owner
        mOwner = owner;

        mBucket = bucket;
    }

    @Override
    public final void acquireShared(Transaction txn) throws LockFailureException {
        acquireShared((LocalTransaction) txn);
    }

    final void acquireShared(LocalTransaction txn) throws LockFailureException {
        long nanosTimeout = txn.lockTimeout(TimeUnit.NANOSECONDS);
        LockResult result = tryAcquireShared(txn, nanosTimeout);
        if (!result.isHeld()) {
            throw txn.failed(LockManager.TYPE_SHARED, result, nanosTimeout);
        }
    }

    /**
     * Acquire a shared lock, but don't push the lock into the owned lock stack. Returns this
     * lock if acquired, or null if already owned.
     */
    final Lock acquireSharedNoPush(LocalTransaction txn) throws LockFailureException {
        long nanosTimeout = txn.lockTimeout(TimeUnit.NANOSECONDS);

        LockResult result;

        LockManager.Bucket bucket = mBucket;
        bucket.acquireExclusive();
        try {
            result = tryLockShared(bucket, txn, nanosTimeout);
        } finally {
            bucket.releaseExclusive();
        }

        if (!result.isHeld()) {
            throw txn.failed(LockManager.TYPE_SHARED, result, nanosTimeout);
        }

        return result == LockResult.ACQUIRED ? this : null;
    }

    @Override
    public final LockResult tryAcquireShared(Transaction txn, long nanosTimeout) {
        return tryAcquireShared((LocalTransaction) txn, nanosTimeout);
    }

    final LockResult tryAcquireShared(LocalTransaction txn, long nanosTimeout) {
        LockResult result;

        LockManager.Bucket bucket = mBucket;
        bucket.acquireExclusive();
        try {
            result = tryLockShared(bucket, txn, nanosTimeout);
        } finally {
            bucket.releaseExclusive();
        }

        if (result == LockResult.ACQUIRED) {
            txn.push(this);
        }

        return result;
    }

    @Override
    public final void acquireExclusive() throws LockFailureException {
        long nanosTimeout = ((LocalTransaction) mOwner).lockTimeout(TimeUnit.NANOSECONDS);
        LockResult result = tryAcquireExclusive(nanosTimeout);
        if (!result.isHeld()) {
            throw mOwner.failed(LockManager.TYPE_EXCLUSIVE, result, nanosTimeout);
        }
    }

    @Override
    public final LockResult tryAcquireExclusive(long nanosTimeout) {
        Locker locker = mOwner;
        LockResult result;

        LockManager.Bucket bucket = mBucket;
        bucket.acquireExclusive();
        try {
            result = tryLockExclusive(bucket, locker, nanosTimeout);
        } finally {
            bucket.releaseExclusive();
        }

        if (result == LockResult.UPGRADED) {
            locker.push(this);
            result = LockResult.ACQUIRED;
        }

        return result;
    }

    @Override
    protected void doUnlockOwnedUnrestricted(LockManager.Bucket bucket) {
        // This is a stripped down version of the doUnlockToUpgradable method.

        if (mLockCount == ~0) {
            mLockCount = 0x80000000;
            Latch.Condition queueSX = mQueueSX;
            if (queueSX != null) {
                queueSX.signalTagged(bucket);
            }
        }

        bucket.releaseExclusive();
    }
}
