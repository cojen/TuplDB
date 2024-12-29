/*
 *  Copyright (C) 2016-2018 Cojen.org
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

import java.io.InterruptedIOException;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import java.util.concurrent.Executor;

import java.util.concurrent.atomic.LongAdder;

import org.cojen.tupl.Entry;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Sorter;
import org.cojen.tupl.Transaction;

import static org.cojen.tupl.core.PageOps.*;

/**
 * Sorter which performs a multi-level parallel merge sort.
 *
 * @author Brian S O'Neill
 */
final class ParallelSorter implements Sorter, Node.Supplier {
    private static final int MIN_SORT_TREES = 8;
    private static final int MAX_SORT_TREES = 64; // absolute max allowed is 32768 (garbage field)

    // These sizes must be powers of two.
    private static final int LEVEL_MIN_SIZE = 8;
    private static final int L0_MAX_SIZE = 256;   // max number of trees at first level
    private static final int L1_MAX_SIZE = 1024;  // max number of trees at higher levels

    private static final int MERGE_THREAD_COUNT = Runtime.getRuntime().availableProcessors();

    private static final int S_READY = 0, S_FINISHING = 1, S_EXCEPTION = 2, S_RESET = 3;

    private final LocalDatabase mDatabase;
    private final Executor mExecutor;

    // Active sort trees, each of which has only a root node.
    private BTree[] mSortTrees;
    private int mSortTreesSize;

    // Pool of trees with only a root node.
    private BTree[] mSortTreePool;
    private int mSortTreePoolSize;

    // Last task added which is acting on sort trees (one root node), forming a stack. For
    // higher levels, BTreeMergers are used. Unlike BTreeMergers, the sort tree merger task
    // cannot be stopped. A thread can only wait for it to finish naturally.
    private Merger mLastMerger;
    private int mMergerCount;

    // The trees in these levels are expected to contain more than one node.
    private volatile List<Level> mSortTreeLevels;

    private LongAdder mFinishCounter;
    private long mFinishCount;

    private int mState;
    private Throwable mException;

    ParallelSorter(LocalDatabase db, Executor executor) {
        mDatabase = db;
        mExecutor = executor;
    }

    private static final class Level {
        final int mLevelNum;
        BTree[] mTrees;
        int mSize;
        BTreeMerger mMerger;
        boolean mStopped;

        Level(int levelNum) {
            mLevelNum = levelNum;
            mTrees = new BTree[LEVEL_MIN_SIZE];
        }

        synchronized void stop() {
            mStopped = true;
            if (mMerger != null) {
                mMerger.stop();
            }
        }

        // Caller must be synchronized on this object.
        void waitUntilFinished() throws InterruptedIOException {
            try {
                while (mMerger != null) {
                    wait();
                }
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }
        }

        synchronized BTree waitForFirstTree() throws InterruptedIOException {
            waitUntilFinished();
            return mTrees[0];
        }

        synchronized void finished(BTreeMerger merger) {
            if (merger == mMerger) {
                mMerger = null;
                notifyAll();
            }
        }
    }

    @Override
    public synchronized void add(byte[] key, byte[] value) throws IOException {
        CommitLock.Shared shared = mDatabase.commitLock().acquireShared();
        try {
            Node node;
            if (mSortTreesSize == 0) {
                BTree sortTree = allocSortTree();
                (mSortTrees = new BTree[MIN_SORT_TREES])[0] = sortTree;
                mSortTreesSize = 1;
                node = sortTree.mRoot;
            } else {
                BTree sortTree = mSortTrees[mSortTreesSize - 1];
                node = latchRootDirty(sortTree);
            }

            try {
                node = Node.appendToSortLeaf(node, mDatabase, key, value, this);
            } finally {
                node.releaseExclusive();
            }
        } catch (Throwable e) {
            exception(e);
            throw e;
        } finally {
            shared.release();
        }

        if (mSortTreesSize >= MAX_SORT_TREES) {
            mergeSortTrees();
        }
    }

    @Override
    public synchronized void addBatch(byte[][] kvPairs, int offset, int size) throws IOException {
        if (size <= 0) {
            return;
        }

        final LocalDatabase db = mDatabase;
        final CommitLock commitLock = db.commitLock();
        final CommitLock.Shared shared = commitLock.acquireShared();

        while (true) {
            Node node;
            try {
                if (mSortTreesSize == 0) {
                    BTree sortTree = allocSortTree();
                    (mSortTrees = new BTree[MIN_SORT_TREES])[0] = sortTree;
                    mSortTreesSize = 1;
                    node = sortTree.mRoot;
                } else {
                    BTree sortTree = mSortTrees[mSortTreesSize - 1];
                    node = latchRootDirty(sortTree);
                }
            } catch (Throwable e) {
                shared.release();
                exception(e);
                throw e;
            }

            while (true) {
                try {
                    byte[] key = kvPairs[offset++];
                    byte[] value = kvPairs[offset++];
                    node = Node.appendToSortLeaf(node, db, key, value, this);
                } catch (Throwable e) {
                    node.releaseExclusive();
                    shared.release();
                    exception(e);
                    throw e;
                }

                size--;

                if (mSortTreesSize >= MAX_SORT_TREES || commitLock.hasQueuedThreads()) {
                    node.releaseExclusive();
                    shared.release();
                    if (mSortTreesSize >= MAX_SORT_TREES) {
                        mergeSortTrees();
                    }
                    if (size <= 0) {
                        return;
                    }
                    commitLock.acquireShared(shared);
                    break;
                } else if (size <= 0) {
                    node.releaseExclusive();
                    shared.release();
                    return;
                }
            }
        }
    }

    @Override
    public BTree finish() throws IOException {
        try {
            BTree tree = doFinish(null);
            finishComplete(true, true);
            return tree;
        } catch (Throwable e) {
            resetAsync(e);
            throw e;
        }
    }

    @Override
    public Scanner<Entry> finishScan() throws IOException {
        return finishScan(new SortScanner(mDatabase));
    }

    @Override
    public Scanner<Entry> finishScan(Scanner<Entry> src) throws IOException {
        return finishScan(new SortScanner(mDatabase), src);
    }

    @Override
    public Scanner<Entry> finishScanReverse() throws IOException {
        return finishScan(new SortReverseScanner(mDatabase));
    }

    @Override
    public Scanner<Entry> finishScanReverse(Scanner<Entry> src) throws IOException {
        return finishScan(new SortReverseScanner(mDatabase), src);
    }

    private Scanner<Entry> finishScan(SortScanner dst) throws IOException {
        try {
            BTree tree = doFinish(dst);
            if (tree != null) {
                finishComplete(true, true);
                dst.ready(tree);
            }
            return dst;
        } catch (Throwable e) {
            resetAsync(e);
            throw e;
        }
    }

    private Scanner<Entry> finishScan(SortScanner dst, Scanner<Entry> src) throws IOException {
        if (src == null) {
            return finishScan(dst);
        }

        var addTask = new Runnable() {
            // this:      not started
            // Thread:    running
            // null:      finished
            // Throwable: failed (and finished)
            private Object mState = this;

            @Override
            public void run() {
                doRun: {
                    synchronized (this) {
                        if (mState == null) {
                            break doRun;
                        }
                        mState = Thread.currentThread();
                    }

                    Object state;
                    try {
                        addAll(src);
                        state = null;
                    } catch (Throwable e) {
                        state = e;
                    }

                    synchronized (this) {
                        mState = state;
                        notifyAll();
                    }
                }

                Utils.closeQuietly(src);
            }

            synchronized void waitUntilFinished() throws InterruptedIOException {
                while (true) {
                    Object state = mState;
                    if (state == null) {
                        return;
                    }
                    if (state instanceof Throwable t) {
                        throw Utils.rethrow(t);
                    }
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        state = mState;
                        mState = null; // don't let it start
                        if (state instanceof Thread t) {
                            t.interrupt();
                        }
                        var ex = new InterruptedIOException();
                        if (state instanceof Throwable t) {
                            ex.addSuppressed(t);
                        }
                        throw ex;
                    }
                }
            }
        };

        mExecutor.execute(addTask);

        dst.notReady(new SortScanner.Supplier() {
            @Override
            public BTree get() throws IOException {
                try {
                    addTask.waitUntilFinished();
                    BTree tree = doFinish(null);
                    finishComplete(true, true);
                    return tree;
                } catch (Throwable e) {
                    resetAsync(e);
                    throw e;
                }
            }

            @Override
            public void close() throws IOException {
                reset();
            }
        });

        return dst;
    }

    /**
     * @param dst pass null to always wait to finish
     */
    private BTree doFinish(SortScanner dst) throws IOException {
        Level finishLevel;

        synchronized (this) {
            checkState();

            setState(S_FINISHING);
            mFinishCount = 0;

            try {
                while (mMergerCount != 0) {
                    wait();
                }
                if (mLastMerger != null) {
                    throw new AssertionError();
                }
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }

            Level[] levels = stopTreeMergers();
            int numLevelTrees = 0;

            if (levels != null) {
                for (Level level : levels) {
                    synchronized (level) {
                        numLevelTrees += level.mSize;
                    }
                }
            }

            final BTree[] sortTrees = mSortTrees;
            final int size = mSortTreesSize;
            mSortTrees = null;
            mSortTreesSize = 0;

            BTree[] allTrees;

            if (size == 0) {
                if (numLevelTrees == 0) {
                    return mDatabase.newTemporaryTree();
                }
                if (numLevelTrees == 1) {
                    return levels[0].mTrees[0];
                }
                allTrees = new BTree[numLevelTrees];
            } else {
                BTree tree;
                if (size == 1) {
                    tree = sortTrees[0];
                    CommitLock.Shared shared = mDatabase.commitLock().acquireShared();
                    Node node;
                    try {
                        node = latchRootDirty(tree);
                    } finally {
                        shared.release();
                    }
                    node.sortLeaf();
                    node.releaseExclusive();
                } else {
                    tree = mDatabase.newTemporaryTree();
                    doMergeSortTrees(null, sortTrees, size, tree);
                }

                if (numLevelTrees == 0) {
                    return tree;
                }

                allTrees = new BTree[numLevelTrees + 1];
                // Place newest tree at the end, to favor its entries if any duplicates exist.
                allTrees[numLevelTrees] = tree;
            }

            // Iterate in reverse order, to favor entries at lower levels if any duplicates exist.
            for (int i = levels.length, pos = 0; --i >= 0; ) {
                Level level = levels[i];
                System.arraycopy(level.mTrees, 0, allTrees, pos, level.mSize);
                pos += level.mSize;
            }

            // Merge the remaining trees and store into an existing level object.

            // Remove all the unused levels, freeing up memory. Keep level 0, in order to
            // permit the reset method to stop the merge, unblocking threads.
            for (int i = mSortTreeLevels.size(); --i >= 1; ) {
                mSortTreeLevels.remove(i);
            }

            finishLevel = levels[0];
            levels = null;
            finishLevel.mSize = 0;
            BTreeMerger merger = newTreeMerger(allTrees, finishLevel, finishLevel);
            finishLevel.mMerger = merger;
            merger.start();

            mFinishCounter = merger;
        }

        if (dst == null) {
            return finishLevel.waitForFirstTree();
        }

        dst.notReady(new SortScanner.Supplier() {
            @Override
            public BTree get() throws IOException {
                try {
                    BTree tree = finishLevel.waitForFirstTree();
                    finishComplete(true, true);
                    return tree;
                } catch (Throwable e) {
                    resetAsync(e);
                    throw e;
                }
            }

            @Override
            public void close() throws IOException {
                reset();
            }
        });

        return null;
    }

    /**
     * Caller must be synchronized.
     *
     * @return null if no levels exist
     */
    private Level[] stopTreeMergers() throws InterruptedIOException {
        List<Level> list = mSortTreeLevels;

        if (list == null) {
            return null;
        }

        while (true) {
            // Clone it in case more levels are added while waiting to stop.
            Level[] levels;
            synchronized (list) {
                levels = list.toArray(new Level[list.size()]);
            }

            for (Level level : levels) {
                level.stop();
            }

            for (Level level : levels) {
                synchronized (level) {
                    level.waitUntilFinished();
                }
            }

            synchronized (list) {
                if (list.size() <= levels.length) {
                    return levels;
                }
            }
        }
    }

    private synchronized void finishComplete(boolean checkException, boolean clearException)
        throws IOException
    {
        mSortTreeLevels = null;

        if (mFinishCounter != null) {
            mFinishCount = mFinishCounter.sum();
            mFinishCounter = null;
        }

        // Drain the pool.
        if (mSortTreePoolSize > 0) {
            do {
                BTree tree = mSortTreePool[--mSortTreePoolSize];
                mSortTreePool[mSortTreePoolSize] = null;
                mDatabase.quickDeleteTemporaryTree(tree);
            } while (mSortTreePoolSize > 0);
        }

        if (mState == S_EXCEPTION) {
            if (checkException) {
                checkState();
            }
            if (!clearException) {
                return;
            }
        }

        setState(S_READY);
        mException = null;
    }

    @Override
    public synchronized long progress() {
        return mFinishCounter != null ? mFinishCounter.sum() : mFinishCount;
    }

    @Override
    public void reset() throws IOException {
        reset(false);
    }

    private void resetAsync(Throwable cause) {
        try {
            mExecutor.execute(() -> {
                try {
                    reset(true);
                } catch (Throwable e) {
                    Utils.uncaught(e);
                }
            });
        } catch (Throwable e) {
            cause.addSuppressed(e);
        }
    }

    private void reset(boolean isAsync) throws IOException {
        List<BTree> toDrop = null;

        synchronized (this) {
            while (mState == S_RESET) {
                if (isAsync) {
                    return;
                }
                try {
                    wait();
                } catch (InterruptedException e) {
                    throw new InterruptedIOException();
                }
            }

            setState(S_RESET);
            mFinishCounter = null;
            mFinishCount = 0;

            try {
                try {
                    while (mMergerCount != 0) {
                        wait();
                    }
                } catch (InterruptedException e) {
                    throw new InterruptedIOException();
                }

                Level[] levels = stopTreeMergers();

                if (levels != null) {
                    for (Level level : levels) {
                        BTree[] trees;
                        int size;
                        synchronized (level) {
                            trees = level.mTrees;
                            size = level.mSize;
                            level.mSize = 0;
                        }
                        if (size != 0) {
                            if (toDrop == null) {
                                toDrop = new ArrayList<>();
                            }
                            for (int i=0; i<size; i++) {
                                toDrop.add(trees[i]);
                            }
                        }
                    }

                    mSortTreeLevels = null;
                }

                BTree[] sortTrees = mSortTrees;
                int size = mSortTreesSize;
                mSortTrees = null;
                mSortTreesSize = 0;

                if (size != 0) {
                    if (toDrop == null) {
                        toDrop = new ArrayList<>();
                    }
                    for (int i=0; i<size; i++) {
                        toDrop.add(sortTrees[i]);
                    }
                }
            } catch (Throwable e) {
                exception(e);
                throw e;
            }
        }

        try {
            if (toDrop != null) {
                if (toDrop.size() == 1) {
                    toDrop.get(0).drop(false).run();
                } else {
                    runDropTasks(toDrop);
                }
            }

            finishComplete(false, !isAsync);
        } catch (Throwable e) {
            exception(e);
            throw e;
        }
    }

    private void runDropTasks(List<BTree> toDrop) throws IOException {
        int numThreads = Math.min(toDrop.size(), MERGE_THREAD_COUNT);

        var controller = new Runnable() {
            private int mPos;
            private int mActive = numThreads;

            @Override
            public void run() {
                while (true) {
                    BTree tree;
                    synchronized (this) {
                        int pos = mPos;
                        if (pos >= toDrop.size()) {
                            inactive();
                            return;
                        }
                        tree = toDrop.get(pos);
                        mPos = pos + 1;
                    }
                    try {
                        tree.drop(false).run();
                    } catch (Throwable e) {
                        inactive();
                        if (!mDatabase.isClosed()) {
                            Utils.rethrow(e);
                        }
                    }
                }
            }

            synchronized void waitUntilFinished() throws InterruptedIOException {
                while (mActive > 0) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        throw new InterruptedIOException();
                    }
                }
            }

            private synchronized void inactive() {
                if (--mActive <= 0) {
                    notifyAll();
                }
            }
        };

        for (int i=1; i<numThreads; i++) {
            mExecutor.execute(controller);
        }

        controller.run();
        controller.waitUntilFinished();
    }

    /**
     * Implementation of Node.Supplier, as required by Node.appendToSortLeaf. Caller must be
     * synchronized and hold commit lock.
     */
    @Override
    public Node newNode() throws IOException {
        if (mSortTreesSize >= mSortTrees.length) {
            mSortTrees = Arrays.copyOf(mSortTrees, MAX_SORT_TREES);
        }
        BTree sortTree = allocSortTree();
        mSortTrees[mSortTreesSize++] = sortTree;
        return sortTree.mRoot;
    }

    // Caller must be synchronized and hold commit lock.
    private BTree allocSortTree() throws IOException {
        checkState();

        BTree tree;
        Node root;

        int size = mSortTreePoolSize;
        if (size > 0) {
            tree = mSortTreePool[--size];
            mSortTreePoolSize = size;
            root = latchRootDirty(tree);
        } else {
            tree = mDatabase.newTemporaryTree(true);
            root = tree.mRoot;
        }

        root.asSortLeaf();
        return tree;
    }

    // Caller must hold commit lock.
    private Node latchRootDirty(BTree tree) throws IOException {
        Node root = tree.mRoot;
        root.acquireExclusive();
        try {
            mDatabase.markDirty(tree, root);
            return root;
        } catch (Throwable e) {
            root.releaseExclusive();
            throw e;
        }
    }

    // Caller must be synchronized.
    private void mergeSortTrees() throws IOException {
        // Merge the sort tree nodes into a new temporary index.

        final BTree dest = mDatabase.newTemporaryTree();

        final BTree[] sortTrees = mSortTrees;
        final int size = mSortTreesSize;
        mSortTrees = new BTree[MAX_SORT_TREES];
        mSortTreesSize = 0;

        if (mSortTreeLevels == null) {
            mSortTreeLevels = new ArrayList<>();
        }

        var merger = new Merger(mLastMerger, sortTrees, size, dest);
        mLastMerger = merger;

        try {
            while (mMergerCount >= MERGE_THREAD_COUNT) {
                wait();
            }
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        }

        mMergerCount++;
        mExecutor.execute(merger);
    }

    /**
     * Merger of sort trees, which only consist of a single node.
     */
    private final class Merger implements Runnable {
        private BTree[] mSortTrees;
        private int mSize;
        private BTree mDest;

        Merger mPrev;

        // Is set when more trees must be added when merge is done.
        Merger mNext;

        Merger(Merger prev, BTree[] sortTrees, int size, BTree dest) {
            mPrev = prev;
            mSortTrees = sortTrees;
            mSize = size;
            mDest = dest;
        }

        @Override
        public void run() {
            try {
                doMergeSortTrees(this, mSortTrees, mSize, mDest);
            } catch (Throwable e) {
                exception(e);
            }
        }
    }

    /**
     * @param merger can be null if not called from Merger class
     */
    private void doMergeSortTrees(Merger merger, BTree[] sortTrees, int size, BTree dest)
        throws IOException
    {
        Throwable ex = null;

        final BTreeCursor appender = dest.newCursor(Transaction.BOGUS);
        try {
            appender.firstLeaf();

            final CommitLock commitLock = mDatabase.commitLock();
            CommitLock.Shared shared = commitLock.acquireShared();
            try {
                mDatabase.checkClosed();

                // Latch and sort all the nodes.
                for (int i=0; i<size; i++) {
                    Node node = latchRootDirty(sortTrees[i]);
                    node.sortLeaf();

                    // Use the garbage field for encoding the node order. Bit 0 is used for
                    // detecting duplicates.
                    node.garbage(i << 1);
                }

                // Heapify.
                for (int i=size >>> 1; --i>=0; ) {
                    siftDown(sortTrees, size, i, sortTrees[i]);
                }

                if (commitLock.hasQueuedThreads()) {
                    // Release and re-acquire, to unblock any threads waiting for
                    // checkpoint to begin.
                    shared.release();
                    shared = commitLock.acquireShared();
                    for (int i=0; i<size; i++) {
                        BTree sortTree = sortTrees[i];
                        mDatabase.markDirty(sortTree, sortTree.mRoot);
                    }
                }

                int len = size;

                while (true) {
                    BTree sortTree = sortTrees[0];
                    Node node = sortTree.mRoot;

                    int order = node.garbage();
                    if ((order & 1) == 0) {
                        appender.appendTransfer(node);
                    } else {
                        // Node has a duplicate entry which must be deleted.
                        node.deleteFirstSortLeafEntry();
                        node.garbage(order & ~1);
                    }

                    if (!node.hasKeys()) {
                        // Shrink the heap, and stash the tree at the end.
                        len--;
                        if (len == 0) {
                            // All done.
                            break;
                        }
                        BTree last = sortTrees[len];
                        sortTrees[len] = sortTree;
                        sortTree = last;
                    }

                    // Fix the heap.
                    siftDown(sortTrees, len, 0, sortTree);
                }
            } finally {
                shared.release();
            }
        } catch (Throwable e) {
            ex = e;
        } finally {
            appender.reset();
        }

        // Unlatch the sort tree nodes.
        for (int i=0; i<size; i++) {
            sortTrees[i].mRoot.releaseExclusive();
        }

        // Recycle the sort trees.
        synchronized (this) {
            if (mSortTreePool == null || mSortTreePoolSize == 0) {
                mSortTreePool = sortTrees;
                mSortTreePoolSize = size;
            } else {
                int totalSize = mSortTreePoolSize + size;
                if (totalSize > mSortTreePool.length) {
                    mSortTreePool = Arrays.copyOf(mSortTreePool, totalSize);
                }
                System.arraycopy(sortTrees, 0, mSortTreePool, mSortTreePoolSize, size);
                mSortTreePoolSize = totalSize;
            }

            if (merger != null) {
                merger.mSortTrees = null;
                merger.mSize = 0;

                Merger prev = merger.mPrev;
                if (prev != null && prev.mDest != null) {
                    // Cannot add destination tree out of order. Leave the merge count alone
                    // for now, preventing unbounded growth of the merger stack.
                    prev.mNext = merger;
                    return;
                }

                while (true) {
                    addToLevel(selectLevel(0), L0_MAX_SIZE, merger.mDest);
                    merger.mDest = null;
                    merger.mPrev = null;
                    mMergerCount--;

                    Merger next = merger.mNext;
                    if (next == null) {
                        if (mLastMerger == merger) {
                            if (mMergerCount != 0) {
                                throw new AssertionError();
                            }
                            mLastMerger = null;
                        }
                        break;
                    } else {
                        merger = next;
                    }
                }

                notifyAll();
            }
        }

        if (ex != null) {
            Utils.rethrow(ex);
        }
    }

    private static void siftDown(BTree[] sortTrees, int size, int pos, BTree element)
        throws IOException
    {
        int half = size >>> 1;
        while (pos < half) {
            int childPos = (pos << 1) + 1;
            BTree child = sortTrees[childPos];
            int rightPos = childPos + 1;
            if (rightPos < size && compareSortTrees(child, sortTrees[rightPos]) > 0) {
                childPos = rightPos;
                child = sortTrees[childPos];
            }
            if (compareSortTrees(element, child) <= 0) {
                break;
            }
            sortTrees[pos] = child;
            pos = childPos;
        }
        sortTrees[pos] = element;
    }

    private static int compareSortTrees(BTree leftTree, BTree rightTree) throws IOException {
        Node left = leftTree.mRoot;
        Node right = rightTree.mRoot;

        int compare = Node.compareKeys
            (left, p_ushortGetLE(left.mPageAddr, left.searchVecStart()),
             right, p_ushortGetLE(right.mPageAddr, right.searchVecStart()));

        if (compare == 0) {
            // Use node order (encoded in garbage field) for eliminating duplicates.
            // Signal that the first entry from the node with the lower key must be
            // deleted. Use bit 0 to signal this.

            int leftOrder = left.garbage();
            int rightOrder = right.garbage();

            if (leftOrder < rightOrder) {
                left.garbage(leftOrder | 1);
                compare = -1;
            } else {
                right.garbage(rightOrder | 1);
                compare = 1;
            }
        }

        return compare;
    }

    private Level selectLevel(int levelNum) {
        return selectLevel(levelNum, mSortTreeLevels);
    }

    private static Level selectLevel(int levelNum, List<Level> levels) {
        synchronized (levels) {
            if (levelNum < levels.size()) {
                return levels.get(levelNum);
            }
            var level = new Level(levelNum);
            levels.add(level);
            return level;
        }
    }

    private void addToLevel(Level level, int maxSize, BTree tree) {
        BTreeMerger merger;

        try {
            synchronized (level) {
                BTree[] trees = level.mTrees;
                int size = level.mSize;

                if (size >= trees.length) {
                    level.mTrees = trees = Arrays.copyOfRange(trees, 0, trees.length << 1);
                }

                trees[size++] = tree;

                if (size < maxSize || level.mStopped) {
                    level.mSize = size;
                    return;
                }

                Level nextLevel = selectLevel(level.mLevelNum + 1);
                level.mSize = 0;
                trees = trees.clone();

                merger = newTreeMerger(trees, level, nextLevel);

                // Don't start a new merger until any existing one has finished.
                level.waitUntilFinished();

                level.mMerger = merger;
            }
        } catch (Throwable e) {
            exception(e);
            return;
        }

        try {
            merger.start();
        } catch (Throwable e) {
            level.finished(merger);
            throw e;
        }
    }

    private BTreeMerger newTreeMerger(BTree[] trees, Level level, Level nextLevel) {
        return new BTreeMerger(mDatabase, trees, mExecutor, MERGE_THREAD_COUNT) {
            @Override
            protected void merged(BTree tree) {
                addToLevel(nextLevel, L1_MAX_SIZE, tree);
            }

            @Override
            protected void remainder(BTree tree) {
                if (tree != null) {
                    addToLevel(nextLevel, L1_MAX_SIZE, tree);
                } else {
                    Throwable ex = exceptionCheck();
                    if (ex != null) {
                        exception(ex);
                    }
                    level.finished(this);
                }
            }
        };
    }

    // Caller must be synchronized.
    private void checkState() throws InterruptedIOException {
        if (mState != S_READY) {
            switch (mState) {
                case S_FINISHING -> throw new IllegalStateException("Finish in progress");
                case S_EXCEPTION -> {
                    Throwable e = mException;
                    if (e != null) {
                        Utils.addLocalTrace(e);
                        throw Utils.rethrow(e);
                    }
                }
            }
            throw new InterruptedIOException("Sorter is reset");
        }
    }

    private synchronized void exception(Throwable e) {
        if (mException == null) {
            mException = e;
        }
        setState(S_EXCEPTION);
    }

    // Caller must be synchronized.
    private void setState(int state) {
        mState = state;
        notifyAll();
    }
}
