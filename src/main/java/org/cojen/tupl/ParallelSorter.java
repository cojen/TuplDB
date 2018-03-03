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

package org.cojen.tupl;

import java.io.InterruptedIOException;
import java.io.IOException;

import java.lang.ref.WeakReference;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import java.util.concurrent.Executor;

import static org.cojen.tupl.PageOps.*;

/**
 * Sorter which performs a multi-level parallel merge sort.
 *
 * @author Brian S O'Neill
 */
/*P*/
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
    private Tree[] mSortTrees;
    private int mSortTreesSize;

    // Pool of trees with only a root node.
    private Tree[] mSortTreePool;
    private int mSortTreePoolSize;

    // Last task added which is acting on sort trees (one root node), forming a stack. For
    // higher levels, TreeMergers are used. Unlike TreeMergers, the sort tree merger task
    // cannot be stopped. A thread can only wait for it to finish naturally.
    private Merger mLastMerger;
    private int mMergerCount;

    // The trees in these levels are expected to contain more than one node.
    private volatile List<Level> mSortTreeLevels;

    private TreeMerger mFinishMerger;
    private long mFinishCount;

    private int mState;
    private Throwable mException;

    ParallelSorter(LocalDatabase db, Executor executor) {
        mDatabase = db;
        mExecutor = executor;
    }

    private static final class Level {
        final int mLevelNum;
        Tree[] mTrees;
        int mSize;
        TreeMerger mMerger;
        boolean mStopped;

        Level(int levelNum) {
            mLevelNum = levelNum;
            mTrees = new Tree[LEVEL_MIN_SIZE];
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

        synchronized void finished(TreeMerger merger) {
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
                Tree sortTree = allocSortTree();
                (mSortTrees = new Tree[MIN_SORT_TREES])[0] = sortTree;
                mSortTreesSize = 1;
                node = sortTree.mRoot;
            } else {
                Tree sortTree = mSortTrees[mSortTreesSize - 1];
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
    public Index finish() throws IOException {
        try {
            Tree tree = doFinish();
            finishComplete();
            return tree;
        } catch (Throwable e) {
            try {
                reset();
            } catch (Exception e2) {
                e.addSuppressed(e2);
            }
            throw e;
        }
    }

    private Tree doFinish() throws IOException {
        Level finishLevel;

        synchronized (this) {
            checkState();

            mState = S_FINISHING;
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
                mSortTreeLevels = null;
            }

            final Tree[] sortTrees = mSortTrees;
            final int size = mSortTreesSize;
            mSortTrees = null;
            mSortTreesSize = 0;

            Tree[] allTrees;

            if (size == 0) {
                if (numLevelTrees == 0) {
                    return mDatabase.newTemporaryIndex();
                }
                if (numLevelTrees == 1) {
                    return levels[0].mTrees[0];
                }
                allTrees = new Tree[numLevelTrees];
            } else {
                Tree tree;
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
                    tree = mDatabase.newTemporaryIndex();
                    doMergeSortTrees(null, sortTrees, size, tree);
                }

                if (numLevelTrees == 0) {
                    return tree;
                }

                allTrees = new Tree[numLevelTrees + 1];
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
            finishLevel = levels[0];
            levels = null;
            finishLevel.mSize = 0;
            TreeMerger merger = newTreeMerger(allTrees, finishLevel, finishLevel);
            finishLevel.mMerger = merger;
            merger.start();

            mFinishMerger = merger;
        }

        synchronized (finishLevel) {
            finishLevel.waitUntilFinished();
            return finishLevel.mTrees[0];
        }
    }

    /**
     * Caller must be synchronized.
     *
     * @return null of no levels exist
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

    private synchronized void finishComplete() throws IOException {
        if (mFinishMerger != null) {
            mFinishCount = mFinishMerger.sum();
            mFinishMerger = null;
        }

        // Drain the pool.
        if (mSortTreePoolSize > 0) {
            do {
                Tree tree = mSortTreePool[--mSortTreePoolSize];
                mSortTreePool[mSortTreePoolSize] = null;
                mDatabase.quickDeleteTemporaryTree(tree);
            } while (mSortTreePoolSize > 0);
        }

        if (mState == S_EXCEPTION) {
            checkState();
        }

        mState = S_READY;
        mException = null;
    }

    @Override
    public synchronized long progress() {
        return mFinishMerger != null ? mFinishMerger.sum() : mFinishCount;
    }

    @Override
    public void reset() throws IOException {
        List<Tree> toDrop = null;

        synchronized (this) {
            mState = S_RESET;
            mFinishMerger = null;
            mFinishCount = 0;

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
                    Tree[] trees;
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

            Tree[] sortTrees = mSortTrees;
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
        }

        if (toDrop != null) for (Tree tree : toDrop) {
            tree.drop(false).run();
        }

        finishComplete();
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
        Tree sortTree = allocSortTree();
        mSortTrees[mSortTreesSize++] = sortTree;
        return sortTree.mRoot;
    }

    // Caller must be synchronized and hold commit lock.
    private Tree allocSortTree() throws IOException {
        checkState();

        Tree tree;
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
    private Node latchRootDirty(Tree tree) throws IOException {
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

        final Tree dest = mDatabase.newTemporaryIndex();

        final Tree[] sortTrees = mSortTrees;
        final int size = mSortTreesSize;
        mSortTrees = new Tree[MAX_SORT_TREES];
        mSortTreesSize = 0;

        if (mSortTreeLevels == null) {
            mSortTreeLevels = new ArrayList<>();
        }

        Merger merger;

        try {
            while (mMergerCount >= MERGE_THREAD_COUNT) {
                wait();
            }
            merger = new Merger(mLastMerger, sortTrees, size, dest);
            mLastMerger = merger;
            mMergerCount++;
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        }

        mExecutor.execute(merger);
    }

    /**
     * Merger of sort trees, which only consist of a single node. Weak reference is to the
     * peviously added worker.
     */
    private final class Merger extends WeakReference<Merger> implements Runnable {
        private Tree[] mSortTrees;
        private int mSize;
        private Tree mDest;

        // Is set when more trees must be added when merge is done.
        Merger mNext;

        Merger(Merger prev, Tree[] sortTrees, int size, Tree dest) {
            super(prev);
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
    private void doMergeSortTrees(Merger merger, Tree[] sortTrees, int size, Tree dest)
        throws IOException
    {
        Throwable ex = null;

        final TreeCursor appender = dest.newCursor(Transaction.BOGUS);
        try {
            appender.firstAny();

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
                        Tree sortTree = sortTrees[i];
                        mDatabase.markDirty(sortTree, sortTree.mRoot);
                    }
                }

                int len = size;

                while (true) {
                    Tree sortTree = sortTrees[0];
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
                        Tree last = sortTrees[len];
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

                Merger prev = merger.get();
                if (prev != null && prev.mDest != null) {
                    // Cannot add destination tree out of order. Leave the merge count alone
                    // for now, preventing unbounded growth of the merger stack.
                    prev.mNext = merger;
                    return;
                }

                while (true) {
                    addToLevel(selectLevel(0), L0_MAX_SIZE, merger.mDest);
                    merger.mDest = null;
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

    private static void siftDown(Tree[] sortTrees, int size, int pos, Tree element)
        throws IOException
    {
        int half = size >>> 1;
        while (pos < half) {
            int childPos = (pos << 1) + 1;
            Tree child = sortTrees[childPos];
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

    private static int compareSortTrees(Tree leftTree, Tree rightTree) throws IOException {
        Node left = leftTree.mRoot;
        Node right = rightTree.mRoot;

        int compare = Node.compareKeys
            (left, p_ushortGetLE(left.mPage, left.searchVecStart()),
             right, p_ushortGetLE(right.mPage, right.searchVecStart()));

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
            Level level = new Level(levelNum);
            levels.add(level);
            return level;
        }
    }

    private void addToLevel(Level level, int maxSize, Tree tree) {
        TreeMerger merger;

        try {
            synchronized (level) {
                Tree[] trees = level.mTrees;
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

    private TreeMerger newTreeMerger(Tree[] trees, Level level, Level nextLevel) {
        return new TreeMerger(mDatabase, trees, mExecutor, MERGE_THREAD_COUNT) {
            @Override
            protected void merged(Tree tree) {
                addToLevel(nextLevel, L1_MAX_SIZE, tree);
            }

            @Override
            protected void remainder(Tree tree) {
                if (tree != null) {
                    addToLevel(nextLevel, L1_MAX_SIZE, tree);
                } else {
                    level.finished(this);
                }
            }
        };
    }

    // Caller must be synchronized.
    private void checkState() throws InterruptedIOException {
        if (mState != S_READY) {
            switch (mState) {
            case S_FINISHING:
                throw new IllegalStateException("Finish in progress");
            case S_EXCEPTION:
                Throwable e = mException;
                if (e != null) {
                    Utils.addLocalTrace(e);
                    throw Utils.rethrow(e);
                }
            }
            throw new InterruptedIOException("Sorter is reset");
        }
    }

    private synchronized void exception(Throwable e) {
        if (mException == null) {
            mException = e;
        }
        mState = S_EXCEPTION;
    }
}
