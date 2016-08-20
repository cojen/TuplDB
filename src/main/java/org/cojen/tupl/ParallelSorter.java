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
import java.util.PriorityQueue;
import java.util.Set;

import java.util.concurrent.Executor;

import static org.cojen.tupl.PageOps.*;

/**
 * Sorter which performs a multi-level parallel merge sort.
 *
 * @author Brian S O'Neill
 */
/*P*/
class ParallelSorter implements Sorter {
    private static final int SORT_NODES = 64; // absolute max allowed is 32768
    private static final int L0_MAX_SIZE = 256; // max number of trees at first level
    private static final int L1_MAX_SIZE = 1024; // max number of trees at higher levels

    private static final int MERGE_THREAD_COUNT = Runtime.getRuntime().availableProcessors();

    private final LocalDatabase mDatabase;

    // Top of active sort node stack.
    private Node mSortNodeTop;
    private int mSortNodeCount;

    // Top of recycled sort node stack.
    private Node mNodePoolTop;

    private final List<List<Tree>> mSortTreeLevels;

    private final Set<TreeMerger> mActiveMergers;
    private int mActiveNodeMergers;

    private final Executor mExecutor;

    private boolean mFinishing;

    ParallelSorter(LocalDatabase db, Executor executor) {
        mDatabase = db;
        mSortTreeLevels = new ArrayList<>();
        mActiveMergers = new HashSet<>();
        mExecutor = executor;
    }

    @Override
    public synchronized void add(byte[] key, byte[] value) throws IOException {
        Node node = mSortNodeTop;

        CommitLock lock = mDatabase.commitLock();
        lock.lock();
        try {
            if (node == null) {
                node = allocSortNode();
                mSortNodeTop = node;
                mSortNodeCount = 1;
            } else {
                latchDirty(node);
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

    @Override
    public Index finish() throws IOException {
        try {
            return doFinish();
        } finally {
            reset();
        }
    }

    @Override
    public synchronized void reset() throws IOException {
        // FIXME: implement reset
        /* drain the pool...
        node.makeEvictable();
        mDatabase.deleteNode(node);
        */
    }

    // Caller must be synchronized.
    private void checkFinishing() {
        if (mFinishing) {
            throw new IllegalStateException("finish in progress");
        }
    }

    private Tree doFinish() throws IOException {
        synchronized (this) {
            checkFinishing();

            mFinishing = true;

            Node topNode = mSortNodeTop;
            int count = mSortNodeCount;
            mSortNodeTop = null;
            mSortNodeCount = 0;

            while (mActiveNodeMergers > 0) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    throw new InterruptedIOException();
                }
            }

            if (topNode != null) {
                doMergeNodes(topNode, count);
            }
        }

        waitForInactivity(true);

        List<Tree> allTrees;
        synchronized (this) {
            int allTreeCount = 0;
            for (List<Tree> trees : mSortTreeLevels) {
                allTreeCount += trees.size();
            }

            allTrees = new ArrayList<>(allTreeCount);

            // Iterate in reverse order to favor duplicates at lower levels.
            for (int i=mSortTreeLevels.size(); --i>=0; ) {
                List<Tree> trees = mSortTreeLevels.get(i);
                allTrees.addAll(trees);
                trees.clear();
            }
        }

        Tree tree;
        if (allTrees.size() <= 1) {
            if (allTrees.isEmpty()) {
                tree = mDatabase.newTemporaryIndex();
            } else {
                tree = allTrees.get(0);
            }
            synchronized (this) {
                mSortTreeLevels.clear();
            }
        } else {
            mergeTrees(allTrees, 0);

            waitForInactivity(false);

            synchronized (this) {
                tree = mSortTreeLevels.get(0).get(0);
                mSortTreeLevels.clear();
            }
        }

        return tree;
    }

    private void waitForInactivity(boolean stop) throws InterruptedIOException {
        try {
            synchronized (mActiveMergers) {
                if (stop) {
                    for (TreeMerger m : mActiveMergers) {
                        m.stop();
                    }
                }

                while (!mActiveMergers.isEmpty()) {
                    mActiveMergers.wait();
                }
            }
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        }
    }

    private void finished(TreeMerger merger) {
        synchronized (mActiveMergers) {
            mActiveMergers.remove(merger);
            if (mActiveMergers.isEmpty()) {
                mActiveMergers.notifyAll();
            }
        }

        // FIXME: capture this
        System.out.println("ex: " + merger.exceptionCheck());
    }

    // Caller must be synchronized.
    private Node nextSortNode(Node current) throws IOException {
        current.releaseExclusive();
        if (mSortNodeCount >= SORT_NODES) {
            mergeNodes();
        }
        Node next = allocSortNode();
        next.mLessUsed = mSortNodeTop;
        mSortNodeTop = next;
        mSortNodeCount++;
        return next;
    }

    // Caller must be synchronized.
    private void mergeNodes() throws IOException {
        // Merge the nodes into a new temporary index.

        Node topNode = mSortNodeTop;
        int count = mSortNodeCount;
        mSortNodeTop = null;
        mSortNodeCount = 0;

        mActiveNodeMergers++;
        try {
            mExecutor.execute(() -> {
                try {
                    doMergeNodes(topNode, count);
                } catch (Throwable e) {
                    // FIXME: stash it for later
                    Utils.rethrow(e);
                }
            });
        } catch (Throwable e) {
            mActiveNodeMergers--;
            notifyAll();
            throw e;
        }
    }

    private void doMergeNodes(Node topNode, int count) throws IOException {
        PriorityQueue<Node> pq = new PriorityQueue<>(count, ParallelSorter::compareSortNode);

        do {
            latchDirty(topNode);
            topNode.sortLeaf();

            // Use the garbage field for encoding the node order. Bit 0 is used for detecting
            // duplicates.
            topNode.garbage((--count) << 1);
            pq.add(topNode);

            Node less = topNode.mLessUsed;
            topNode.mLessUsed = null;
            topNode = less;
        } while (count > 0);

        Tree dest = mDatabase.newTemporaryIndex();

        TreeCursor appender = dest.newCursor(Transaction.BOGUS);
        try {
            appender.firstAny();
            for (Node node; (node = pq.poll()) != null; ) {
                int order = node.garbage();
                if ((order & 1) == 0) {
                    appender.appendTransfer(node);
                } else {
                    // Node has a duplicate entry which must be deleted.
                    node.deleteFirstSortLeafEntry();
                    node.garbage(order & ~1);
                }

                if (node.hasKeys()) {
                    pq.add(node);
                } else {
                    // Recycle the node.
                    node.releaseExclusive();
                    synchronized (this) {
                        node.mLessUsed = mNodePoolTop;
                        mNodePoolTop = node;
                    }
                }
            }
        } finally {
            appender.reset();
        }

        synchronized (this) {
            mActiveNodeMergers--;
            try {
                addToLevel(dest, 0, L0_MAX_SIZE);
            } finally {
                notifyAll();
            }
        }
    }

    // Caller must be synchronized.
    private Node allocSortNode() throws IOException {
        checkFinishing();

        Node node = mNodePoolTop;
        if (node == null) {
            // FIXME: Track node using a special op in an UndoLog instance.
            node = mDatabase.allocDirtyNode(NodeUsageList.MODE_UNEVICTABLE);
        } else {
            mNodePoolTop = node.mLessUsed;
            node.mLessUsed = null;
            latchDirty(node);
        }

        node.asSortLeaf();
        return node;
    }

    private static int compareSortNode(Node left, Node right) {
        try {
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
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    private void latchDirty(Node node) throws IOException {
        node.acquireExclusive();
        try {
            // FIXME: When id changes... oops! Needs to be tracked again in undo log.
            // FIXME: Not all callers hold commit lock!
            // FIXME: If shouldMarkDirty, then don't. Allocate a new node instead. Rollback the
            // transaction which is tracking the nodes, deleting them. The prepareToDelete
            // method ensures that changes destined for the checkpoint are flushed out.
            mDatabase.markUnmappedDirty(node);
        } catch (Throwable e) {
            node.releaseExclusive();
            throw e;
        }
    }

    // Caller must be synchronized.
    private void addToLevel(Tree tree, int level, int maxLevelSize) throws IOException {
        if (level >= mSortTreeLevels.size()) {
            List<Tree> trees = new ArrayList<>();
            trees.add(tree);
            mSortTreeLevels.add(trees);
        } else {
            List<Tree> trees = mSortTreeLevels.get(level);
            trees.add(tree);
            if (trees.size() >= maxLevelSize && !mFinishing) {
                mergeTrees(trees, level + 1);
            }
        }
    }

    private void mergeTrees(List<Tree> trees, int targetLevel) throws IOException {
        Tree[] toMerge = trees.toArray(new Tree[trees.size()]);
        trees.clear();

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

        synchronized (mActiveMergers) {
            mActiveMergers.add(tm);
        }

        tm.start(mExecutor);
    }
}
