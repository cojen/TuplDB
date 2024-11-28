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

import java.io.IOException;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.util.concurrent.ThreadLocalRandom;

import org.cojen.tupl.DeadlockException;
import org.cojen.tupl.LockFailureException;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.diag.DeadlockInfo;

import org.cojen.tupl.util.Latch;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class RowPredicateLockImpl<R> implements RowPredicateLock<R> {
    private final LockManager mManager;
    private final long mIndexId;

    private final ThreadLocal<Integer> mVersionStripe;

    private static final int cNumStripes = Runtime.getRuntime().availableProcessors();

    // Linked stack of VersionLocks.
    private volatile StripedVersionLock mNewestVersion;

    private static final VarHandle cNewestVersionHandle, cLockNextHandle;

    // Linked stack of Evaluators.
    private volatile Evaluator<R> mLastEvaluator;

    private static final VarHandle cLastEvaluatorHandle, cNextHandle;

    static {
        try {
            var lookup = MethodHandles.lookup();

            cNewestVersionHandle = lookup.findVarHandle
                (RowPredicateLockImpl.class, "mNewestVersion", StripedVersionLock.class);

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

        mVersionStripe = new ThreadLocal<>() {
            @Override
            protected Integer initialValue() {
                return ThreadLocalRandom.current().nextInt(cNumStripes);
            }
        };

        mNewestVersion = newVersion();
    }

    private StripedVersionLock newVersion() {
        var version = new StripedVersionLock();
        mManager.initDetachedLock(version, null); // initially unowned
        return version;
    }

    private VersionLock selectNewestVersion() {
        while (true) {
            VersionLock version = mNewestVersion.select(this);
            if (version.validate()) {
                return version;
            }
        }
    }

    @Override
    public Closer openAcquire(Transaction txn, R row) throws IOException {
        if (txn.lockMode() == LockMode.UNSAFE) {
            return NonCloser.THE;
        }

        VersionLock version = selectNewestVersion();
        var local = (LocalTransaction) txn;

        if (version.acquireNoPush(local)) {
            local.push(version);
        }

        try {
            for (Evaluator<R> e = mLastEvaluator; e != null; e = e.mPrev) {
                if (e.test(row)) {
                    e.matchAcquire(local);
                }
            }
            return version;
        } catch (Throwable e) {
            version.close();
            throw e;
        }
    }

    @Override
    public Closer openAcquireP(Transaction txn, R row, byte[] key, byte[] value)
        throws IOException
    {
        if (txn.lockMode() == LockMode.UNSAFE) {
            return NonCloser.THE;
        }

        VersionLock version = selectNewestVersion();
        var local = (LocalTransaction) txn;

        if (version.acquireNoPush(local)) {
            local.push(version);
        }

        try {
            for (Evaluator<R> e = mLastEvaluator; e != null; e = e.mPrev) {
                if (e.testP(row, key, value)) {
                    e.matchAcquire(local);
                }
            }
            return version;
        } catch (Throwable e) {
            version.close();
            throw e;
        }
    }

    @Override
    public Closer tryOpenAcquire(Transaction txn, R row, byte[] key, byte[] value)
        throws IOException
    {
        if (txn.lockMode() == LockMode.UNSAFE) {
            return NonCloser.THE;
        }

        VersionLock version = selectNewestVersion();
        var local = (LocalTransaction) txn;

        Closer closer;
        if (version.acquireNoPush(local)) {
            local.push(version);
            // Indicate that at least one lock was acquired.
            closer = version;
        } else {
            // No locks actually acquired yet (was already held).
            closer = NonCloser.THE;
        }

        try {
            for (Evaluator<R> e = mLastEvaluator; e != null; e = e.mPrev) {
                if (row != null) {
                    if (!e.test(row)) {
                        continue;
                    }
                } else if (!e.test(key, value)) {
                    continue;
                }

                LockResult result = e.tryMatchAcquire(local);
                if (result == LockResult.ACQUIRED) {
                    txn.unlockCombine();
                    closer = version;
                } else if (!result.isHeld()) {
                    if (closer == version) {
                        txn.unlock();
                    }
                    return null;
                }
            }
            return closer;
        } catch (Throwable e) {
            version.close();
            throw e;
        }
    }

    @Override
    public Object acquireLocksNoPush(Transaction txn, byte[] key, byte[] value)
        throws LockFailureException
    {
        // Assume this method is called by ReplEngine, in which case txn is never UNSAFE.

        VersionLock version = selectNewestVersion();
        var local = (LocalTransaction) txn;

        Object locks = null;
        int numLocks = 0;

        if (version.acquireNoPush(local)) {
            locks = version;
            numLocks = 1;
        }

        try {
            for (Evaluator<R> e = mLastEvaluator; ; e = e.mPrev) {
                Lock lock;
                if (e == null) {
                    lock = local.doLockUpgradableNoPush(mIndexId, key);
                } else {
                    if (!e.test(key, value)) {
                        continue;
                    }
                    lock = e.matchAcquireNoPush(local);
                }

                if (lock != null) {
                    if (locks == null) {
                        locks = lock;
                    } else if (locks instanceof Lock first) {
                        var array = new Lock[4];
                        array[0] = first;
                        array[1] = lock;
                        locks = array;
                    } else {
                        var array = (Lock[]) locks;
                        if (numLocks >= array.length) {
                            var newArray = new Lock[array.length << 1];
                            System.arraycopy(array, 0, newArray, 0, array.length);
                            array = newArray;
                        }
                        array[numLocks] = lock;
                    }
                    numLocks++;
                }

                if (e == null) {
                    break;
                }
            }

            return locks;
        } catch (Exception e) {
            if (locks != null) {
                if (locks instanceof Lock lock) {
                    unlockUnowned(lock);
                } else {
                    for (Lock lock : (Lock[]) locks) {
                        unlockUnowned(lock);
                    }
                }
            }

            throw e;
        } finally {
            version.close();
        }
    }

    private void unlockUnowned(Lock lock) {
        LockManager.Bucket bucket = mManager.getBucket(lock.mHashCode);
        bucket.acquireExclusive();
        try {
            if (lock instanceof DetachedLock) {
                lock.doUnlock(null, bucket);
            } else {
                lock.doUnlockOwnedUnrestricted(bucket);
            }
        } finally {
            bucket.releaseExclusive();
        }
    }

    @Override
    public void redoPredicateMode(Transaction txn) throws IOException {
        ((CoreTransaction) txn).redoPredicateMode();
    }

    @Override
    public Closer addPredicate(Transaction txn, RowPredicate<R> predicate)
        throws LockFailureException
    {
        if (predicate instanceof Evaluator<R> evaluator) {
            addEvaluator(txn, evaluator);
            return evaluator;
        } else {
            return addNonEvaluator(txn, predicate);
        }
    }

    private Evaluator addNonEvaluator(Transaction txn, RowPredicate<R> predicate)
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
    public Closer addGuard(Transaction txn) throws LockFailureException {
        var guard = new Guard<R>();
        addEvaluator(txn, guard);
        return guard;
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

            StripedVersionLock version;
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

            StripedVersionLock newerVersion = null;

            while (true) {
                boolean discard = version.await(mIndexId, evaluator, local);
                var olderVersion = version.mLockNext;
                if (discard && newerVersion != null) {
                    cLockNextHandle.weakCompareAndSet(newerVersion, version, olderVersion);
                }
                if (olderVersion == null) {
                    break;
                }
                newerVersion = version;
                version = (StripedVersionLock) olderVersion;
            }

            evaluator.acquireExclusive();
        } catch (Throwable e) {
            remove(evaluator);
            throw e;
        }
    }

    @Override
    public void withExclusiveNoRedo(Transaction txn, Runnable mustWait, Runnable callback)
        throws IOException
    {
        var local = (LocalTransaction) txn;

        local.enter();
        try {
            // Adding a predicate that matches 'all' will block all new calls to openAcquire,
            // which in turn blocks new rows from being inserted.
            Evaluator all = addNonEvaluator(local, RowPredicate.all());

            // Holding the version lock blocks addPredicate calls once they call version.await.
            // They won't acquire the exclusive evaluator lock until this step completes.
            final VersionLock version = selectNewestVersion();

            if (version.acquireNoPush(local)) {
                local.push(version);
            }

            try {
                // Wait for existing row scan operations to finish.
                for (Evaluator<R> e = mLastEvaluator; e != null; e = e.mPrev) {
                    if (e != all) {
                        if (mustWait != null) {
                            // Pessimistic invocation.
                            mustWait.run();
                            mustWait = null;
                        }

                        e.acquireShared(local);
                    }
                }

                callback.run();
            } finally {
                version.close();
            }
        } finally {
            txn.exit();
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
    @SuppressWarnings("unchecked")
    public Class<? extends RowPredicate<R>> evaluatorClass() {
        return (Class) Evaluator.class;
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

    private static final class StripedVersionLock extends VersionLock {
        private VersionLock[] mStripes;

        private static final VarHandle cStripesHandle;
        private static final VarHandle cStripesElementHandle;

        static {
            try {
                var lookup = MethodHandles.lookup();

                cStripesHandle = lookup.findVarHandle
                    (StripedVersionLock.class, "mStripes", VersionLock[].class);

                cStripesElementHandle = MethodHandles.arrayElementVarHandle(VersionLock[].class);
            } catch (Throwable e) {
                throw Utils.rethrow(e);
            }
        }

        /**
         * Select a VersionLock lock, latched exclusively. Must call validate.
         */
        VersionLock select(RowPredicateLockImpl<?> parent) {
            VersionLock lock;
            int which;
            {
                var stripes = (VersionLock[]) cStripesHandle.getAcquire(this);
                if (stripes == null) {
                    if (mBucket.tryAcquireExclusive()) {
                        return this;
                    }
                    which = parent.mVersionStripe.get();
                } else {
                    which = parent.mVersionStripe.get();
                    lock = (VersionLock) cStripesElementHandle.getAcquire(stripes, which);
                    if (lock != null && lock.mBucket.tryAcquireExclusive()) {
                        return lock;
                    }
                    which = ThreadLocalRandom.current().nextInt(cNumStripes);
                    parent.mVersionStripe.set(which);
                    lock = (VersionLock) cStripesElementHandle.getAcquire(stripes, which);
                    if (lock != null) {
                        lock.mBucket.acquireExclusive();
                        return lock;
                    }
                }
            }

            while (true) {
                LockManager.Bucket bucket = mBucket;
                bucket.acquireExclusive();

                VersionLock[] stripes = mStripes;

                obtainLock: try {
                    if (stripes == null) {
                        stripes = new VersionLock[cNumStripes];
                        cStripesHandle.setRelease(this, stripes);
                    } else {
                        lock = stripes[which];
                        if (lock != null) {
                            break obtainLock;
                        }
                    }
                    lock = new VersionLock();
                    parent.mManager.initDetachedLock(lock, null);
                    cStripesElementHandle.setRelease(stripes, which, lock);
                } finally {
                    bucket.releaseExclusive();
                }

                if (lock.mBucket.tryAcquireExclusive()) {
                    return lock;
                }

                which = ThreadLocalRandom.current().nextInt(cNumStripes);
                parent.mVersionStripe.set(which);
                lock = (VersionLock) cStripesElementHandle.getAcquire(stripes, which);

                if (lock != null) {
                    lock.mBucket.acquireExclusive();
                    return lock;
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
            boolean result = true;
            var stripes = (VersionLock[]) cStripesHandle.getAcquire(this);

            if (stripes != null) {
                for (int i=0; i<stripes.length; i++) {
                    var lock = (VersionLock) cStripesElementHandle.getAcquire(stripes, i);
                    if (lock != null) {
                        result &= lock.doAwait(indexId, evaluator, txn);
                    }
                }
            }

            return result & doAwait(indexId, evaluator, txn);
        }
    }

    private static class VersionLock extends DetachedLockImpl implements Closer {

        private static final VarHandle cIndexIdHandle, cQueueUHandle;

        static {
            try {
                var lookup = MethodHandles.lookup();
                // Re-purpose the otherwise unused mIndexId and mQueueU fields for open acquires.
                cIndexIdHandle = lookup.findVarHandle
                    (VersionLock.class, "mIndexId", long.class);
                cQueueUHandle = lookup.findVarHandle
                    (VersionLock.class, "mQueueU", Latch.Condition.class);
            } catch (Throwable e) {
                throw Utils.rethrow(e);
            }
        }

        /**
         * Caller must hold exclusive bucket latch.
         */
        final boolean validate() {
            // If mLockCount is 0, then the call to doAwait returned true, which means that
            // this version was discarded.
            if (mLockCount != 0) {
                return true;
            } else {
                mBucket.releaseExclusive();
                return false;
            }
        }

        /**
         * Similar to acquireShared except it doesn't block if a queue exists. Caller must
         * hold exclusive bucket latch.
         *
         * @return true if just acquired
         * @throws IllegalStateException if an exclusive lock is held
         */
        final boolean acquireNoPush(Locker locker) {
            try {
                int count = mLockCount;
                if (count == ~0) {
                    throw new IllegalStateException();
                }
                cIndexIdHandle.getAndAdd(this, 1L);
                if (count != 0 && isSharedLocker(locker)) {
                    return false;
                }
                addSharedLocker(count, locker);
            } finally {
                mBucket.releaseExclusive();
            }

            return true;
        }

        /**
         * Called when an openAcquire step finishes successfully.
         */
        @Override
        public final void close() {
            if (((long) cIndexIdHandle.getAndAdd(this, -1L)) == 1L) {
                signalQueueU();
            }
        }

        private void signalQueueU() {
            var queue = (Latch.Condition) cQueueUHandle.getAcquire(this);
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

        /**
         * Wait for transactions to finish which are using this version and have also locked
         * rows that are matched by the given evaluator.
         *
         * @return true if should discard
         */
        final boolean doAwait(long indexId, Evaluator<?> evaluator, LocalTransaction txn)
            throws LockFailureException
        {
            final LockManager.Bucket bucket = mBucket;

            bucket.acquireExclusive();
            try {
                if ((mLockCount & 0x7fffffff) == 0) {
                    // No shared owners, so return immediately. Set the lock count to zero to
                    // indicate that this version was discarded.
                    mLockCount = 0;
                    bucket.releaseExclusive();
                    return true;
                }

                Latch.Condition queue = mQueueSX;
                if (queue == null) {
                    // Create a queue to indicate that we're waiting.
                    mQueueSX = queue = new Latch.Condition();
                }
            } catch (Throwable e) {
                bucket.releaseExclusive();
                throw e;
            }

            if (((long) cIndexIdHandle.getVolatile(this)) != 0L) {
                // Need to wait for in-flight open acquires to finish.

                var queue = mQueueU;
                if (queue == null) {
                    try {
                        queue = new Latch.Condition();
                        cQueueUHandle.setRelease(this, queue);
                    } catch (Throwable e) {
                        bucket.releaseExclusive();
                        throw e;
                    }
                }

                if (((long) cIndexIdHandle.getVolatile(this)) != 0L) {
                    var w = queue.await(bucket, txn.mLockTimeoutNanos);
                    if ((mLockCount & 0x7fffffff) == 0) {
                        // No shared owners anymore.
                        mLockCount = 0;
                        bucket.releaseExclusive();
                        return true;
                    }
                    if (w <= 0 && ((long) cIndexIdHandle.getVolatile(this)) != 0L) {
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
                            if ((((int) cLockCountHandle.getAcquire(this)) & 0x7fffffff) == 0) {
                                // No shared owners anymore, but need to double check with a
                                // latch before this version can be safely discarded.
                                break waitCheck;
                            }
                        }
                    }
                }

                // No need to wait because no matching locks have been found.
                return false;
            }

            int w;
            DeadlockInfo deadlock;

            bucket.acquireExclusive();
            doWait: try {
                if ((mLockCount & 0x7fffffff) == 0) {
                    // No shared owners anymore.
                    mLockCount = 0;
                    return true;
                }

                Latch.Condition queueSX = mQueueSX;

                if (queueSX == null) {
                    // Assume LockManager was closed.
                    w = -1;
                    deadlock = null;
                    break doWait;
                }

                if (mOwner == null) {
                    // Attempt to claim ownership. Otherwise, the shared count would never go
                    // to zero, and so we'd never be signaled.
                    if (claimOwnership(txn) > 0) {
                        // No shared owners anymore, or txn is the sole shared lock owner.
                        return (mLockCount &= 0x7fffffff) == 0;
                    }
                } else {
                    if ((mLockCount & 0x7fffffff) == 0) {
                        // No shared owners anymore.
                        mLockCount = 0;
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
                    return (mLockCount &= 0x7fffffff) == 0;
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

        final void matchAcquire(LocalTransaction txn) throws LockFailureException {
            cMatchedHandle.weakCompareAndSetPlain(this, false, true);
            acquireShared(txn);
        }

        final LockResult tryMatchAcquire(LocalTransaction txn) throws LockFailureException {
            cMatchedHandle.weakCompareAndSetPlain(this, false, true);
            return tryAcquireShared(txn, 0);
        }

        final Lock matchAcquireNoPush(LocalTransaction txn) throws LockFailureException {
            cMatchedHandle.weakCompareAndSetPlain(this, false, true);
            return acquireSharedNoPush(txn);
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

    private static final class Guard<R> extends Evaluator<R> {
        @Override
        public boolean test(R row) {
            return false;
        }

        @Override
        public boolean test(byte[] key, byte[] value) {
            return false;
        }

        public boolean test(byte[] key) {
            return false;
        }
    }
}
