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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.util.concurrent.TimeUnit;

import org.cojen.tupl.DeadlockException;
import org.cojen.tupl.DeadlockInfo;
import org.cojen.tupl.LockFailureException;
import org.cojen.tupl.LockInterruptedException;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.LockTimeoutException;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.util.LatchCondition;

/**
 * 
 *
 * @author Brian S O'Neill
 */
/*P*/
final class RowPredicateLockImpl<R> implements RowPredicateLock<R> {
    private final LockManager mManager;
    private final long mIndexId;

    // Linked stack of VersionLocks.
    private volatile VersionLock mNewestVersion;

    private static final VarHandle cNewestVersionHandle, cLockNextHandle;

    // Linked stack of Evaluators.
    private volatile Evaluator<R> mLastEvaluator;

    private static final VarHandle cLastEvaluatorHandle, cNextHandle;

    static {
        try {
            var lookup = MethodHandles.lookup();

            cNewestVersionHandle = lookup.findVarHandle
                (RowPredicateLockImpl.class, "mNewestVersion", VersionLock.class);

            cLockNextHandle = lookup.findVarHandle(Lock.class, "mLockNext", Lock.class);

            cLastEvaluatorHandle = lookup.findVarHandle
                (RowPredicateLockImpl.class, "mLastEvaluator", Evaluator.class);

            cNextHandle = lookup.findVarHandle(Evaluator.class, "mNext", Evaluator.class);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    RowPredicateLockImpl(LockManager manager, long indexId) {
        mManager = manager;
        mIndexId = indexId;
        mNewestVersion = newVersion();
    }

    private VersionLock newVersion() {
        var version = new VersionLock();
        mManager.initDetachedLock(version, null); // initially unowned
        return version;
    }

    @Override
    public Closer openAcquire(Transaction txn, R row) throws LockFailureException {
        var local = (LocalTransaction) txn;
        VersionLock lock = mNewestVersion;
        lock.acquire(local);

        try {
            for (Evaluator<R> e = mLastEvaluator; e != null; e = e.mPrev) {
                if (e.test(row)) {
                    e.matched(local);
                }
            }
            return lock;
        } catch (Throwable e) {
            lock.close();
            throw e;
        }
    }

    @Override
    public Closer openAcquire(Transaction txn, byte[] key, byte[] value)
        throws LockFailureException
    {
        var local = (LocalTransaction) txn;
        VersionLock lock = mNewestVersion;
        lock.acquire(local);

        try {
            for (Evaluator<R> e = mLastEvaluator; e != null; e = e.mPrev) {
                if (e.test(key, value)) {
                    e.matched(local);
                }
            }
            return lock;
        } catch (Throwable e) {
            lock.close();
            throw e;
        }
    }

    @Override
    public int countPredicates() {
        int count = 0;
        for (Evaluator<R> e = mLastEvaluator; e != null; e = e.mPrev) {
            count++;
        }
        return count;
    }

    @Override
    public Closer addPredicate(Transaction txn, RowPredicate<R> predicate)
        throws LockFailureException
    {
        if (predicate instanceof Evaluator) {
            var evaluator = (Evaluator<R>) predicate;
            addEvaluator(txn, evaluator);
            return evaluator;
        } else {
            return addNonEvaluator(txn, predicate);
        }
    }

    private Closer addNonEvaluator(Transaction txn, RowPredicate<R> predicate)
        throws LockFailureException
    {
        if (predicate instanceof RowPredicate.None) {
            return null;
        }

        var evaluator = new Evaluator<R>() {
            @Override
            public boolean test(R row) {
                return predicate.test(row);
            }

            @Override
            public boolean test(byte[] key, byte[] value) {
                return predicate.test(key, value);
            }

            @Override
            public boolean test(byte[] key) {
                return predicate.test(key);
            }

            @Override
            public String toString() {
                return predicate.toString();
            }
        };

        addEvaluator(txn, evaluator);

        return evaluator;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<? extends RowPredicate<R>> evaluatorClass() {
        return (Class) Evaluator.class;
    }

    private void addEvaluator(final Transaction txn, final Evaluator<R> evaluator)
        throws LockFailureException
    {
        final var local = (LocalTransaction) txn;
        evaluator.mLock = this;
        mManager.initDetachedLock(evaluator, local);

        // Adopts a similar concurrent linked list design as used by CursorFrame.bind.
        add: {
            // Next is set to self to indicate that the evaluator is the last.
            evaluator.mNext = evaluator;

            for (int trials = CursorFrame.SPIN_LIMIT;;) {
                Evaluator<R> last = mLastEvaluator;
                evaluator.mPrev = last;
                if (last == null) {
                    if (cLastEvaluatorHandle.compareAndSet(this, null, evaluator)) {
                        break add;
                    }
                } else if (last.mNext == last && cNextHandle.compareAndSet(last, last, evaluator)) {
                    // Catch up before replacing the last frame reference.
                    while (mLastEvaluator != last) Thread.onSpinWait();
                    mLastEvaluator = evaluator;
                    break add;
                }
                if (--trials < 0) {
                    // Spinning too much due to high contention. Back off a tad.
                    Thread.yield();
                    trials = CursorFrame.SPIN_LIMIT << 1;
                } else {
                    Thread.onSpinWait();
                }
            }
        }

        try {
            // Add a new version to the list, pointing towards the older versions. Removal
            // isn't performed out of order, it doesn't need to be strict, and the list is
            // never empty. Overall, this makes it simpler than how evaluators are added.

            VersionLock version;
            {
                var newVersion = newVersion();
                while (true) {
                    version = mNewestVersion;
                    newVersion.mLockNext = version;
                    if (cNewestVersionHandle.compareAndSet(this, version, newVersion)) {
                        break;
                    }
                    Thread.onSpinWait();
                }
            }

            // Sweep through the versions, from newest to oldest, and wait for all matching
            // transactions to finish.

            // FIXME: Timeout needs to be adjusted for each await call.

            VersionLock newerVersion = null;

            while (true) {
                boolean discard = version.await(mIndexId, evaluator, local);
                var olderVersion = (VersionLock) version.mLockNext;
                if (discard && newerVersion != null) {
                    cLockNextHandle.weakCompareAndSet(newerVersion, version, olderVersion);
                }
                if (olderVersion == null) {
                    break;
                }
                newerVersion = version;
                version = olderVersion;
            }

            evaluator.acquireExclusive();
        } catch (Throwable e) {
            remove(evaluator);
            throw e;
        }
    }

    private void remove(final Evaluator<R> lock) {
        // Adopts a similar concurrent linked list design as used by CursorFrame.unbind.

        for (int trials = CursorFrame.SPIN_LIMIT;;) {
            Evaluator<R> n = lock.mNext;

            if (n == null) {
                // Not in the list.
                return;
            }

            if (n == lock) {
                // Removing the last evaluator.
                if (cNextHandle.compareAndSet(lock, n, null)) {
                    // Update previous evaluator to be the new last evaluator.
                    Evaluator<R> p;
                    do {
                        p = lock.mPrev;
                    } while (p != null && (p.mNext != lock
                                           || !cNextHandle.compareAndSet(p, lock, p)));
                    // Catch up before replacing the last evaluator reference.
                    while (mLastEvaluator != lock) Thread.onSpinWait();
                    mLastEvaluator = p;
                    return;
                }
            } else {
                // Uninstalling an interior or first evaluator.
                if (n.mPrev == lock && cNextHandle.compareAndSet(lock, n, null)) {
                    // Update next reference chain to skip over the removed evaluator.
                    Evaluator<R> p;
                    do {
                        p = lock.mPrev;
                    } while (p != null && (p.mNext != lock ||
                                           !cNextHandle.compareAndSet(p, lock, n)));
                    // Update previous reference chain to skip over the removed evaluator.
                    n.mPrev = p;
                    return;
                }
            }

            if (--trials < 0) {
                // Spinning too much due to high contention. Back off a tad.
                Thread.yield();
                trials = CursorFrame.SPIN_LIMIT << 1;
            } else {
                Thread.onSpinWait();
            }
        }
    }

    // FIXME: Define a striped variant for improved concurrency.
    private static final class VersionLock extends DetachedLockImpl implements Closer {

        private static final VarHandle cIndexIdHandle, cQueueUHandle;

        static {
            try {
                var lookup = MethodHandles.lookup();
                // Re-purpose the otherwise unused mIndexId and mQueueU fields for open acquires.
                cIndexIdHandle = lookup.findVarHandle
                    (VersionLock.class, "mIndexId", long.class);
                cQueueUHandle = lookup.findVarHandle
                    (VersionLock.class, "mQueueU", LatchCondition.class);
            } catch (Throwable e) {
                throw Utils.rethrow(e);
            }
        }

        /**
         * Similar to acquireShared except it doesn't block if a queue exists.
         *
         * @throws IllegalStateException if an exclusive lock is held
         */
        void acquire(Locker locker) {
            LockManager.Bucket bucket = mBucket;
            bucket.acquireExclusive();
            try {
                int count = mLockCount;
                if (count == ~0) {
                    throw new IllegalStateException();
                }
                cIndexIdHandle.getAndAdd(this, 1);
                if (count != 0 && isSharedLocker(locker)) {
                    return;
                }
                addSharedLocker(count, locker);
            } finally {
                bucket.releaseExclusive();
            }

            locker.push(this);
        }

        /**
         * Called when an openAcquire step is finished.
         */
        @Override
        public void close() {
            if (((long) cIndexIdHandle.getAndAdd(this, -1)) == 1) {
                var queue = (LatchCondition) cQueueUHandle.getAcquire(this);
                if (queue != null) {
                    LockManager.Bucket bucket = mBucket;
                    bucket.acquireExclusive();
                    try {
                        queue.signalAll(bucket);
                    } finally {
                        bucket.releaseExclusive();
                    }
                }
            }
        }

        /**
         * Wait for transactions to finish which are using this version and have also locked
         * rows that are matched by the given evaluator.
         *
         * @return true if should discard
         */
        boolean await(long indexId, Evaluator<?> evaluator, LocalTransaction txn)
            throws LockFailureException
        {
            if (((int) cLockCountHandle.getAcquire(this)) == 0x80000000) {
                // No shared owners, so return immediately.
                return true;
            }

            final LockManager.Bucket bucket = mBucket;
            final boolean owner;

            bucket.acquireExclusive();
            try {
                if (mLockCount == 0x80000000) {
                    // No shared owners, so return immediately.
                    bucket.releaseExclusive();
                    return true;
                }

                LatchCondition queue = mQueueSX;
                if (queue == null) {
                    // Create a queue to indicate that we're waiting.
                    mQueueSX = queue = new LatchCondition();
                }
            } catch (Throwable e) {
                bucket.releaseExclusive();
                throw e;
            }

            if (((long) cIndexIdHandle.getVolatile(this)) != 0) {
                // Need to wait for in-flight open acquires to finish.

                var queue = mQueueU;
                if (queue == null) {
                    try {
                        queue = new LatchCondition();
                        cQueueUHandle.setRelease(this, queue);
                    } catch (Throwable e) {
                        bucket.releaseExclusive();
                        throw e;
                    }
                }

                if (((long) cIndexIdHandle.getVolatile(this)) != 0) {
                    var w = queue.await(bucket, txn.mLockTimeoutNanos);
                    if (mLockCount == 0x80000000) {
                        bucket.releaseExclusive();
                        return true;
                    }
                    if (w <= 0 && ((long) cIndexIdHandle.getVolatile(this)) != 0) {
                        bucket.releaseExclusive();
                        throw failed(txn, w);
                    }
                }
            }

            // While we're waiting, scan all the current lock owners (lockers) and see if we
            // can give up early. If none of the lockers hold locks that would match the
            // evaluator, then there's no need to wait. If just one locker matches, then a full
            // wait is required.

            // Need a stable copy of the current lockers. Note that it doesn't matter if more
            // lockers are added later because they'll see the new evaluator.

            final Object lockers;

            bucket.downgrade();
            try {
                lockers = copyLockers();
            } finally {
                bucket.releaseShared();
            }

            Lock conflict;

            waitCheck: {
                if (lockers != null) {
                    if (lockers instanceof Locker locker) {
                        if (locker != txn) {
                            conflict = findAnyConflict(locker, indexId, evaluator);
                            if (conflict != null) {
                                // Must wait.
                                break waitCheck;
                            }
                        }
                    } else for (Locker locker : (Locker[]) lockers) {
                        if (locker != txn) {
                            conflict = findAnyConflict(locker, indexId, evaluator);
                            if (conflict != null) {
                                // Must wait.
                                break waitCheck;
                            }
                            if (((int) cLockCountHandle.getAcquire(this)) == 0x80000000) {
                                // No shared owners anymore.
                                return true;
                            }
                        }
                    }
                }

                // No need to wait because no matching locks have been found.
                return false;
            }

            if (((int) cLockCountHandle.getAcquire(this)) == 0x80000000) {
                // No shared owners anymore.
                return true;
            }

            int w;
            DeadlockInfo deadlock;

            bucket.acquireExclusive();
            doWait: try {
                LatchCondition queueSX = mQueueSX;

                if (queueSX == null) {
                    // Assume LockManager was closed.
                    w = -1;
                    deadlock = null;
                    break doWait;
                }

                if (mOwner == null) {
                    // Attempt to claim ownership. Otherwise, the shared count would never go
                    // to zero, and so we'd never be signalled.
                    if (claimOwnership(txn) > 0) {
                        // No shared owners anymore or txn is the sole shared lock owner.
                        return true;
                    }
                } else {
                    if (mLockCount == 0x80000000) {
                        // No shared owners anymore.
                        return true;
                    }

                    if (isSharedLocker(txn)) {
                        // We cannot claim ownership, and the shared count will never go to
                        // zero. This is an instant deadlock.
                        String desc = String.valueOf(mOwner.mWaitingFor);
                        deadlock = new DetachedDeadlockInfo(desc, mOwner.attachment());
                        w = 0;
                        break doWait;
                    }
                }

                try {
                    txn.mWaitingFor = evaluator;
                    w = queueSX.await(bucket, txn.mLockTimeoutNanos);
                } finally {
                    if (mOwner == txn) {
                        // Convert back to a regular shared lock owner.
                        addSharedLocker(mLockCount, txn);
                        mOwner = null;
                    }
                }

                queueSX = mQueueSX;

                if (queueSX == null) {
                    // Assume LockManager was closed.
                    txn.mWaitingFor = null;
                    w = -1;
                    deadlock = null;
                    break doWait;
                }

                if (w > 0) {
                    txn.mWaitingFor = null;
                    // Wake up all waiting threads.
                    queueSX.signalAll(bucket);
                    return true;
                }

                deadlock = null;
            } finally {
                bucket.releaseExclusive();
            }

            if (deadlock != null) {
                var infos = new DeadlockInfo[2];
                infos[0] = deadlock;

                Object attachment = conflict.findOwnerAttachment
                    (txn, false, LockManager.TYPE_EXCLUSIVE);
                infos[1] = DeadlockDetector.newDeadlockInfo(txn.mManager, conflict, attachment);

                throw new DeadlockException
                    (0, deadlock.ownerAttachment(), true, new DeadlockInfoSet(infos));
            }

            throw failed(txn, w);
        }

        private static Lock findAnyConflict(Locker locker, long indexId, Evaluator<?> evaluator) {
            return evaluator.hasMatched() ? evaluator : locker.findAnyConflict(indexId, evaluator);
        }

        private static LockFailureException failed(LocalTransaction txn, int w)
            throws DeadlockException
        {
            LockResult result = w < 0 ? LockResult.INTERRUPTED : LockResult.TIMED_OUT_LOCK;
            return txn.failed(LockManager.TYPE_EXCLUSIVE, result, txn.mLockTimeoutNanos);
        }
    }

    /**
     * Implements the actual predicate lock instances.
     */
    public static abstract class Evaluator<R>
        extends DetachedLockImpl implements RowPredicate<R>, Closer
    {
        private RowPredicateLockImpl<R> mLock;

        private volatile Evaluator<R> mNext;
        private volatile Evaluator<R> mPrev;

        private boolean mMatched;

        private static final VarHandle cMatchedHandle;

        static {
            try {
                cMatchedHandle = MethodHandles.lookup().findVarHandle
                    (Evaluator.class, "mMatched", boolean.class);
            } catch (Throwable e) {
                throw Utils.rethrow(e);
            }
        }

        @Override
        protected final void doUnlockOwnedUnrestricted(LockManager.Bucket bucket) {
            super.doUnlockOwnedUnrestricted(bucket);
            mLock.remove(this);
        }

        final void matched(LocalTransaction txn) throws LockFailureException {
            cMatchedHandle.weakCompareAndSetPlain(this, false, true);
            acquireShared(txn);
        }

        /**
         * Returns true if the evaluator ever matched a row, although it can return false
         * negatives. That is, false can be returned even when a row did match. Performance is
         * more important.
         */
        final boolean hasMatched() {
            return (boolean) cMatchedHandle.getOpaque(this);
        }

        /**
         * Called to release the predicate lock before the transaction exits.
         */
        @Override
        public void close() {
            var bucket = mBucket;
            bucket.acquireExclusive();
            doUnlockOwnedUnrestricted(bucket);
        }
    }
}
