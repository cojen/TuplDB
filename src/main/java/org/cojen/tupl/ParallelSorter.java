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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import java.util.concurrent.Executor;

import org.cojen.tupl.util.Latch;
import org.cojen.tupl.util.LatchCondition;

import static org.cojen.tupl.PageOps.*;

/**
 * Sorter which performs a multi-level parallel merge sort.
 *
 * @author Brian S O'Neill
 */
/*P*/
class ParallelSorter implements Sorter {
    private static final int MIN_SORT_TREES = 8;
    private static final int MAX_SORT_TREES = 64; // absolute max allowed is 32768 (garbage field)
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

    // Count of mergers which are acting on sort trees (one root node). For higher levels,
    // TreeMergers are used. Unlike TreeMergers, sort tree mergers cannot be stopped. A thread
    // can only wait for them to finish naturally.
    private int mActiveSortTreeMergers;

    // The trees in these levels are expected to contain more than one node.
    private List<ReserveQueue<Tree>> mSortTreeLevels;

    private Latch mActiveMergersLatch;
    private LatchCondition mActiveMergersCondition;
    private Set<TreeMerger> mActiveMergers;

    private int mState;
    private Throwable mException;

    ParallelSorter(LocalDatabase db, Executor executor) {
        mDatabase = db;
        mExecutor = executor;
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
                node = Node.appendToSortLeaf(node, mDatabase, key, value, this::nextSortNode);
            } finally {
                node.releaseExclusive();
            }
        } finally {
            shared.release();
        }
    }

    // Caller must be synchronized and hold commit lock.
    private Node nextSortNode(Node current) throws IOException {
        current.releaseExclusive();

        int size = mSortTreesSize;
        if (size >= MAX_SORT_TREES) {
            mergeSortTrees();
            mSortTrees = new Tree[MAX_SORT_TREES];
            size = 0;
        } else if (size >= mSortTrees.length) {
            mSortTrees = Arrays.copyOf(mSortTrees, MAX_SORT_TREES);
        }

        Tree sortTree = allocSortTree();

        mSortTrees[size] = sortTree;
        mSortTreesSize = size + 1;
        
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
            root = mDatabase.allocDirtyNode(NodeContext.MODE_UNEVICTABLE);
            tree = mDatabase.newTemporaryTree(root);
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

    @Override
    public Index finish() throws IOException {
        try {
            return doFinish();
        } catch (Throwable e) {
            try {
                reset();
            } catch (Exception e2) {
                e.addSuppressed(e2);
            }
            throw e;
        }
    }

    @Override
    public void reset() throws IOException {
        Object shouldWait;
        synchronized (this) {
            mState = S_RESET;

            Tree[] sortTrees = mSortTrees;
            int size = mSortTreesSize;
            mSortTrees = null;
            mSortTreesSize = 0;

            while (mActiveSortTreeMergers > 0) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    throw new InterruptedIOException();
                }
            }

            for (int i=0; i<size; i++) {
                sortTrees[i].drop(false).run();
            }

            shouldWait = mActiveMergersLatch;
        }

        if (shouldWait != null) {
            waitForInactivity(true);
        }

        List<ReserveQueue<Tree>> toDrop = null;

        synchronized (this) {
            toDrop = mSortTreeLevels;
            mSortTreeLevels = null;
        }

        if (toDrop != null) {
            for (int i = toDrop.size(); --i >= 0; ) {
                toDrop.get(i).scan(tree -> {
                    try {
                        tree.drop(false).run();
                    } catch (Throwable e) {
                        Utils.rethrow(e);
                    }
                });
            }
        }

        synchronized (this) {
            finishComplete();
        }
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

    private Tree doFinish() throws IOException {
        Object shouldWait;
        synchronized (this) {
            checkState();

            mState = S_FINISHING;

            Tree[] sortTrees = mSortTrees;
            int size = mSortTreesSize;
            mSortTrees = null;
            mSortTreesSize = 0;

            while (mActiveSortTreeMergers > 0) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    throw new InterruptedIOException();
                }
            }

            if (size == 0) {
                if (mSortTreeLevels == null || mSortTreeLevels.isEmpty()) {
                    Tree tree = mDatabase.newTemporaryIndex();
                    finishComplete();
                    return tree;
                }
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
                    tree = doMergeSortTrees(sortTrees, size);
                }
                if (mSortTreeLevels == null || mSortTreeLevels.isEmpty()) {
                    finishComplete();
                    return tree;
                }

                ReserveQueue<Tree> level = selectLevel(0);
                int levelPos = reserveLevelPos(0, level, L0_MAX_SIZE);
                level.set(levelPos, tree);
            }

            shouldWait = mActiveMergersLatch;
        }

        if (shouldWait != null) {
            waitForInactivity(true);
        }

        Tree[] allTrees;
        synchronized (this) {
            if (mSortTreeLevels.size() == 1) {
                ReserveQueue<Tree> level = mSortTreeLevels.get(0);
                allTrees = new Tree[level.size()];
                level.poll(allTrees, 0, allTrees.length);
                if (level.size() != 0) {
                    throw new AssertionError();
                }
            } else {
                int allTreeCount = 0;
                for (int i=mSortTreeLevels.size(); --i>=0; ) {
                    allTreeCount += mSortTreeLevels.get(i).size();
                }

                allTrees = new Tree[allTreeCount];

                // Iterate in reverse order to favor duplicates at lower levels, which were
                // added more recently.
                for (int i = mSortTreeLevels.size(), pos = 0; --i >= 0; ) {
                    ReserveQueue<Tree> level = mSortTreeLevels.get(i);
                    pos += level.poll(allTrees, pos, level.size());
                    if (level.size() != 0) {
                        throw new AssertionError();
                    }
                }
            }

            if (allTrees.length <= 1) {
                Tree tree;
                if (allTrees.length == 0) {
                    tree = mDatabase.newTemporaryIndex();
                } else {
                    tree = allTrees[0];
                }
                mSortTreeLevels.clear();
                finishComplete();
                return tree;
            }

            initForMerging();
        }

        mergeTrees(allTrees, 0);

        waitForInactivity(false);

        synchronized (this) {
            ReserveQueue<Tree> level = mSortTreeLevels.get(0);
            Tree tree = level.poll();
            if (level.size() != 0) {
                throw new AssertionError();
            }
            finishComplete();
            return tree;
        }
    }

    // Caller must be synchronized.
    private void finishComplete() throws IOException {
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

    private void waitForInactivity(boolean stop) throws InterruptedIOException {
        mActiveMergersLatch.acquireExclusive();
        try {
            if (stop) {
                for (TreeMerger m : mActiveMergers) {
                    m.stop();
                }
            }

            while (!mActiveMergers.isEmpty()) {
                if (mActiveMergersCondition.await(mActiveMergersLatch, -1, 0) < 0) {
                    throw new InterruptedIOException();
                }
            }
        } finally {
            mActiveMergersLatch.releaseExclusive();
        }
    }

    // Caller must be synchronized.
    private void mergeSortTrees() throws IOException {
        // Merge the sort tree nodes into a new temporary index.

        Tree[] sortTrees = mSortTrees;
        int size = mSortTreesSize;
        mSortTrees = null;
        mSortTreesSize = 0;

        // Select the level and reserve the position early. The merge tasks run in parallel and
        // finish out of order. Reserving the position is necessary to properly select the last
        // added entry when duplicate keys are encountered.
        ReserveQueue<Tree> level = selectLevel(0);
        int levelPos = reserveLevelPos(0, level, L0_MAX_SIZE);

        mActiveSortTreeMergers++;
        try {
            mExecutor.execute(() -> {
                Tree tree;
                try {
                    tree = doMergeSortTrees(sortTrees, size);
                } catch (Throwable e) {
                    synchronized (this) {
                        mActiveSortTreeMergers--;
                        exception(e);
                        notifyAll();
                    }
                    return;
                }

                synchronized (this) {
                    level.set(levelPos, tree);
                    if (--mActiveSortTreeMergers == 0) {
                        notifyAll();
                    }
                }
            });
        } catch (Throwable e) {
            mActiveSortTreeMergers--;
            notifyAll();
            throw e;
        }
    }

    /**
     * @return new temporary index
     */
    private Tree doMergeSortTrees(Tree[] sortTrees, final int size) throws IOException {
        Tree dest = mDatabase.newTemporaryIndex();

        TreeCursor appender = dest.newCursor(Transaction.BOGUS);
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
        }

        return dest;
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

    // Caller must be synchronized.
    private void initForMerging() {
        if (mActiveMergersLatch == null) {
            mActiveMergersLatch = new Latch();
            mActiveMergersCondition = new LatchCondition();
            mActiveMergers = new HashSet<>();
        }
    }

    // Caller must be synchronized.
    private ReserveQueue<Tree> selectLevel(int levelNum) {
        if (mSortTreeLevels == null) {
            mSortTreeLevels = new ArrayList<>();
        } else if (levelNum < mSortTreeLevels.size()) {
            return mSortTreeLevels.get(levelNum);
        }
        ReserveQueue<Tree> level = new ReserveQueue<>(MIN_SORT_TREES);
        mSortTreeLevels.add(level);
        return level;
    }

    // Caller must be synchronized.
    private int reserveLevelPos(int levelNum, ReserveQueue<Tree> level, int maxLevelSize)
        throws IOException
    {
        if (level.size() >= maxLevelSize && mState == S_READY) {
            int avail = level.available();
            if (avail > 1) {
                Tree[] toMerge = new Tree[avail];
                level.poll(toMerge, 0, toMerge.length);
                initForMerging();
                mergeTrees(toMerge, levelNum + 1);
            }
        }
        return level.reserve();
    }

    // Must have called initForMerging.
    private void mergeTrees(Tree[] toMerge, int targetLevel) throws IOException {
        ReserveQueue<Tree> level = selectLevel(targetLevel);
        int levelPos = reserveLevelPos(targetLevel, level, L1_MAX_SIZE);

        TreeMerger tm = new TreeMerger(mDatabase, toMerge, mExecutor, MERGE_THREAD_COUNT) {
            @Override
            protected void merged(Tree tree) {
                synchronized (ParallelSorter.this) {
                    level.set(levelPos, tree);
                }
            }

            @Override
            protected void remainder(Tree tree) {
                synchronized (ParallelSorter.this) {
                    if (tree == null) {
                        ParallelSorter.this.finished(this);
                    } else {
                        level.add(tree);
                    }
                }
            }
        };

        mActiveMergersLatch.acquireExclusive();
        try {
            mActiveMergers.add(tm);
        } finally {
            mActiveMergersLatch.releaseExclusive();
        }

        tm.start();
    }

    private void finished(TreeMerger merger) {
        mActiveMergersLatch.acquireExclusive();
        try {
            mActiveMergers.remove(merger);
            if (mActiveMergers.isEmpty()) {
                mActiveMergersCondition.signalAll();
            }
        } finally {
            mActiveMergersLatch.releaseExclusive();
        }

        Throwable e = merger.exceptionCheck();
        if (e != null) synchronized (this) {
            exception(e);
        }
    }

    // Caller must be synchronized.
    private void exception(Throwable e) {
        if (mException == null) {
            mException = e;
        }
        mState = S_EXCEPTION;
    }
}
