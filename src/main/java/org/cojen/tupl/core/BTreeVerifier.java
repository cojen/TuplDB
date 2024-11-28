/*
 *  Copyright (C) 2024 Cojen.org
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

import java.io.InterruptedIOException;
import java.io.IOException;

import java.util.concurrent.Executor;

import org.cojen.tupl.diag.VerificationObserver;

import org.cojen.tupl.util.Latch;
import org.cojen.tupl.util.LocalPool;

/**
 * Parallel tree verify utility. After construction, call start and then call await.
 *
 * @author Brian S. O'Neill
 */
final class BTreeVerifier extends BTreeSeparator {
    // Note: The BTreeSeparator is extended because it supports parallel processing, but the
    // necessary methods are overridden such that no actual data transfer occurs.

    private final VerificationObserver mObserver;

    private final LocalPool<Observer> mObserverPool;

    private final Latch mLatch;
    private final Latch.Condition mCondition;

    private boolean mStarted, mFinished;
    private volatile boolean mStopped;

    /**
     * @param executor used for parallel separation; pass null to use only the starting thread
     * @param workerCount maximum parallelism; must be at least 1
     */
    BTreeVerifier(VerificationObserver observer, BTree source, Executor executor, int workerCount) {
        super(null, new BTree[] {source}, executor, workerCount);
        mObserver = observer;
        mObserverPool = new LocalPool<>(null, workerCount);
        mLatch = new Latch();
        mCondition = new Latch.Condition();
    }

    /**
     * @return true if stopped
     */
    public boolean await() throws IOException {
        mLatch.acquireExclusive();
        try {
            while (!mFinished) {
                if (mCondition.await(mLatch) < 0) {
                    stop();
                    throw new InterruptedIOException();
                }
            }
        } finally {
            mLatch.releaseExclusive();
        }

        return mStopped;
    }

    @Override
    protected void finished(Chain<BTree> firstRange) {
        mLatch.acquireExclusive();
        mFinished = true;
        mCondition.signalAll(mLatch);
        mLatch.releaseExclusive();
    }

    @Override
    protected Worker newWorker(int spawnCount, byte[] lowKey, byte[] highKey, int numSources) {
        return new Verifier(spawnCount, lowKey, highKey, numSources);
    }

    private void begin(BTreeCursor source, int height) {
        mLatch.acquireExclusive();
        try {
            if (!mStarted) {
                mStarted = true;
                if (!mObserver.indexBegin(source.mTree, height)) {
                    mStopped = true;
                    stop();
                }
            }
        } finally {
            mLatch.releaseExclusive();
        }
    }

    final class Verifier extends Worker {
        Verifier(int spawnCount, byte[] lowKey, byte[] highKey, int numSources) {
            super(spawnCount, lowKey, highKey, numSources);
        }

        @Override
        protected void transfer(BTreeCursor source, BTreeCursor unused) throws IOException {
            LocalPool.Entry<Observer> entry = mObserverPool.access();

            try {
                Observer obs = entry.get();
                if (obs == null) {
                    int height = source.height();
                    begin(source, height);
                    obs = new Observer(mObserver, height);
                    entry.replace(obs);
                }

                Node[] stack = obs.mStack;

                if (!source.verifyFrames(stack.length, stack, source.mFrame, obs)) {
                    mStopped = true;
                    stop();
                    return;
                }

                source.skipToNextLeaf();
            } finally {
                entry.release();
            }
        }

        @Override
        protected void skip(BTreeCursor source) throws IOException {
            // Nothing should be skipped.
            throw new AssertionError();
        }
    }

    static final class Observer extends VerifyObserver {
        Node[] mStack;

        Observer(VerificationObserver wrapped, int height) {
            super(wrapped);
            mStack = new Node[height];
        }
    }
}
