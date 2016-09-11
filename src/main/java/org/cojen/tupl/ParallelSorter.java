/*
 *  Copyright 2016 Cojen.org
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
    private static final int MAX_SORT_TREES = 64; // absolute max allowed is 32768
    private static final int L0_MAX_SIZE = 256;   // max number of trees at first level
    private static final int L1_MAX_SIZE = 1024;  // max number of trees at higher levels

    private static final int MERGE_THREAD_COUNT = Runtime.getRuntime().availableProcessors();

    private static final int STATE_READY = 0, STATE_FINISHING = 1, STATE_RESET = 2;

    private final LocalDatabase mDatabase;
    private final Executor mExecutor;

    // Active sort trees, each of which has only a root node.
    private Tree[] mSortTrees;
    private int mSortTreesSize;

    // Pool of trees with only a root node.
    private Tree[] mSortTreePool;
    private int mSortTreePoolSize;

    private List<List<Tree>> mSortTreeLevels;

    private Latch mActiveMergersLatch;
    private LatchCondition mActiveMergersCondition;
    private Set<TreeMerger> mActiveMergers;
    private int mActiveNodeMergers;

    private int mState;

    ParallelSorter(LocalDatabase db, Executor executor) {
        mDatabase = db;
        mExecutor = executor;
    }

    @Override
    public synchronized void add(byte[] key, byte[] value) throws IOException {
        CommitLock lock = mDatabase.commitLock();
        lock.lock();
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
            lock.unlock();
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
            root = mDatabase.allocDirtyNode(NodeUsageList.MODE_UNEVICTABLE);
            tree = mDatabase.newTemporaryTree(root);
        }

        root.asSortLeaf();
        return tree;
    }

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
        // FIXME: stop any active merging

        synchronized (this) {
            mState = STATE_RESET;
            finishComplete();
        }
    }

    // Caller must be synchronized.
    private void checkState() throws InterruptedIOException {
        if (mState != 0) {
            if (mState == STATE_FINISHING) {
                throw new IllegalStateException("Finish in progress");
            }
            throw new InterruptedIOException("Sorter is reset");
        }
    }

    private Tree doFinish() throws IOException {
        Object shouldWait;
        synchronized (this) {
            checkState();

            mState = STATE_FINISHING;

            Tree[] sortTrees = mSortTrees;
            int size = mSortTreesSize;
            mSortTrees = null;
            mSortTreesSize = 0;

            while (mActiveNodeMergers > 0) {
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
                    Node node = latchRootDirty(tree);
                    node.sortLeaf();
                    node.releaseExclusive();
                } else {
                    tree = doMergeSortTrees(sortTrees, size);
                }
                if (mSortTreeLevels == null || mSortTreeLevels.isEmpty()) {
                    finishComplete();
                    return tree;
                }
                addToLevel(tree, 0, L0_MAX_SIZE);
            }

            shouldWait = mActiveMergersLatch;
        }

        if (shouldWait != null) {
            waitForInactivity(true);
        }

        Tree[] allTrees;
        synchronized (this) {
            if (mSortTreeLevels.size() == 1) {
                List<Tree> trees = mSortTreeLevels.get(0);
                allTrees = trees.toArray(new Tree[trees.size()]);
                trees.clear();
            } else {
                int allTreeCount = 0;
                for (int i=mSortTreeLevels.size(); --i>=0; ) {
                    allTreeCount += mSortTreeLevels.get(i).size();
                }

                allTrees = new Tree[allTreeCount];

                // Iterate in reverse order to favor duplicates at lower levels, which were
                // added more recently.
                int pos = 0;
                for (int i=mSortTreeLevels.size(); --i>=0; ) {
                    List<Tree> trees = mSortTreeLevels.get(i);
                    for (int j=0; j<trees.size(); j++) {
                        allTrees[pos++] = trees.get(j);
                    }
                    trees.clear();
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
            Tree tree = mSortTreeLevels.get(0).get(0);
            mSortTreeLevels.clear();
            finishComplete();
            return tree;
        }
    }

    // Caller must be synchronized.
    private void finishComplete() throws IOException {
        // Drain the pool.
        while (mSortTreePoolSize > 0) {
            mDatabase.quickDeleteTemporaryTree(mSortTreePool[--mSortTreePoolSize]);
        }
        mState = STATE_READY;
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

        mActiveNodeMergers++;
        try {
            mExecutor.execute(() -> {
                Tree tree;
                try {
                    tree = doMergeSortTrees(sortTrees, size);
                } catch (Throwable e) {
                    synchronized (this) {
                        mActiveNodeMergers--;
                        notifyAll();
                    }
                    // FIXME: stash it for later
                    throw Utils.rethrow(e);
                }

                synchronized (this) {
                    mActiveNodeMergers--;
                    try {
                        addToLevel(tree, 0, L0_MAX_SIZE);
                    } catch (Throwable e) {
                        // FIXME: stash it for later
                        throw Utils.rethrow(e);
                    } finally {
                        notifyAll();
                    }
                }
            });
        } catch (Throwable e) {
            mActiveNodeMergers--;
            notifyAll();
            throw e;
        }
    }

    /**
     * @return new temporary index
     */
    private Tree doMergeSortTrees(Tree[] sortTrees, final int size) throws IOException {
        // Latch and sort all the nodes.
        for (int i=0; i<size; i++) {
            Node node = latchRootDirty(sortTrees[i]);
            node.sortLeaf();
            // Use the garbage field for encoding the node order. Bit 0 is used for detecting
            // duplicates.
            node.garbage(i << 1);
        }

        // Heapify.
        for (int i=size >>> 1; --i>=0; ) {
            siftDown(sortTrees, size, i, sortTrees[i]);
        }

        Tree dest = mDatabase.newTemporaryIndex();

        TreeCursor appender = dest.newCursor(Transaction.BOGUS);
        try {
            appender.firstAny();
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
    private void addToLevel(Tree tree, int level, int maxLevelSize) throws IOException {
        if (mSortTreeLevels == null) {
            mSortTreeLevels = new ArrayList<>();
        } else if (level < mSortTreeLevels.size()) {
            List<Tree> trees = mSortTreeLevels.get(level);
            trees.add(tree);
            if (trees.size() >= maxLevelSize && mState == STATE_READY) {
                Tree[] toMerge = trees.toArray(new Tree[trees.size()]);
                trees.clear();
                initForMerging();
                mergeTrees(toMerge, level + 1);
            }
            return;
        }
        List<Tree> trees = new ArrayList<>();
        trees.add(tree);
        mSortTreeLevels.add(trees);
    }

    // Must have called initForMerging.
    private void mergeTrees(Tree[] toMerge, int targetLevel) throws IOException {
        TreeMerger tm = new TreeMerger
            (mDatabase, MERGE_THREAD_COUNT, toMerge, (merger, target) -> {
                if (target == null) {
                    finished(merger);
                } else {
                    try {
                        synchronized (this) {
                            addToLevel(target, targetLevel, L1_MAX_SIZE);
                        }
                    } catch (Throwable e) {
                        throw Utils.rethrow(e);
                    }
                }
            });

        mActiveMergersLatch.acquireExclusive();
        try {
            mActiveMergers.add(tm);
        } finally {
            mActiveMergersLatch.releaseExclusive();
        }

        tm.start(mExecutor);
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

        // FIXME: stash it for later
        //System.out.println("ex: " + merger.exceptionCheck());
    }
}
