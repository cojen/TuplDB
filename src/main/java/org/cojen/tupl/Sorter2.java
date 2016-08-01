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
import java.util.List;
import java.util.PriorityQueue;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import static org.cojen.tupl.PageOps.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
/*P*/
class Sorter2 {
    private static final int SORT_NODES = 64; // absolute max allowed is 65536
    private static final int L0_MAX_SIZE = 256; // max number of trees at first level
    private static final int L1_MAX_SIZE = 1024; // max number of trees at higher levels

    private static final int MERGE_THREAD_COUNT = Runtime.getRuntime().availableProcessors();
    private static final int MAX_THREAD_COUNT = MERGE_THREAD_COUNT * 4;

    private final LocalDatabase mDatabase;

    private final Node[] mSortNodes;
    private int mSortNodePos;

    private final List<List<Tree>> mSortTreeLevels;

    private ExecutorService mExecutor;
    private int mActiveThreads;

    Sorter2(LocalDatabase db) {
        mDatabase = db;
        mSortNodes = new Node[SORT_NODES];
        mSortTreeLevels = new ArrayList<>();
    }

    /**
     * Add an entry into the sorter. If multiple entries are added with matching keys, only the
     * last one added is kept.
     */
    public synchronized void add(byte[] key, byte[] value) throws IOException {
        Node node = mSortNodes[mSortNodePos];

        CommitLock lock = mDatabase.commitLock();
        lock.lock();
        try {
            if (node == null) {
                node = allocSortNode();
                mSortNodes[mSortNodePos] = node;
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

    /**
     * Finish sorting the entries, and return a temporary index with the results.
     */
    public synchronized Index finish() throws IOException {
        try {
            while (mActiveThreads > 0) {
                wait();
            }
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        }

        // FIXME
        return null;
    }

    /**
     * Discards all the entries and frees up space in the database.
     */
    public synchronized void reset() throws IOException {
        // FIXME
        throw null;
    }

    // Caller must be synchronized.
    private Node nextSortNode(Node current) throws IOException {
        current.releaseExclusive();
        int pos = mSortNodePos + 1;

        if (pos >= mSortNodes.length) {
            // Merge the nodes into a new temporary index.

            // FIXME: use custom PriorityQueue and avoid double copy
            Node[] nodes = mSortNodes.clone();

            executor(1).execute(() -> {
                Tree dest;
                try {
                    PriorityQueue<Node> pq = new PriorityQueue<>
                        (nodes.length, Sorter2::compareSortNode);

                    for (int i=0; i<nodes.length; i++) {
                        Node node = nodes[i];
                        latchDirty(node);
                        node.sortLeaf();
                        // FIXME: store order in garbage field
                        pq.add(node);
                    }

                    dest = mDatabase.newTemporaryIndex();
                    TreeCursor appender = dest.newAppendCursor();
                    try {
                        while (true) {
                            Node node = pq.poll();
                            if (node == null) {
                                break;
                            }
                            appender.appendTransfer(node);
                            if (node.hasKeys()) {
                                pq.add(node);
                            } else {
                                // FIXME: locally recycle the nodes!
                                mDatabase.deleteNode(node);
                            }
                        }
                    } finally {
                        appender.reset();
                    }
                } catch (Throwable e) {
                    synchronized (this) {
                        mActiveThreads--;
                        notify();
                    }
                    throw Utils.rethrow(e);
                }

                synchronized (this) {
                    mActiveThreads--;
                    try {
                        addToLevel(dest, 0, L0_MAX_SIZE);
                    } catch (Throwable e) {
                        throw Utils.rethrow(e);
                    }
                    notify();
                }
            });

            pos = 0;
        }

        Node next = allocSortNode();

        mSortNodes[pos] = next;
        mSortNodePos = pos;

        return next;
    }

    private Node allocSortNode() throws IOException {
        // FIXME: Track node using a special op in an UndoLog instance.
        Node node = mDatabase.allocDirtyNode(NodeUsageList.MODE_UNEVICTABLE);
        node.asSortLeaf();
        return node;
    }

    private static int compareSortNode(Node left, Node right) {
        try {
            int compare = Node.compareKeys
                (left, p_ushortGetLE(left.mPage, left.searchVecStart()),
                 right, p_ushortGetLE(right.mPage, right.searchVecStart()));

            if (compare == 0) {
                // FIXME: use order as tie-breaker
                System.out.println("tie!");
            }

            return compare;
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    private void latchDirty(Node node) throws IOException {
        node.acquireExclusive();
        try {
            // FIXME: Rename this method; can be used for more than just undo log nodes.
            mDatabase.markUndoLogDirty(node);
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
            return;
        }

        List<Tree> trees = mSortTreeLevels.get(level);
        trees.add(tree);
        //System.out.println("" + level + ", " + trees.size());

        if (trees.size() < maxLevelSize) {
            return;
        }

        Tree[] toMerge = trees.toArray(new Tree[trees.size()]);
        trees.clear();

        ExecutorService executor = executor(MERGE_THREAD_COUNT + 1);

        executor.execute(() -> {
            Tree merged;
            try {
                merged = new TreeMerger(mDatabase, MERGE_THREAD_COUNT, toMerge).merge(executor);
            } catch (Throwable e) {
                synchronized (this) {
                    mActiveThreads -= MERGE_THREAD_COUNT + 1;
                    notify();
                }
                throw Utils.rethrow(e);
            }

            synchronized (this) {
                mActiveThreads -= MERGE_THREAD_COUNT + 1;
                try {
                    addToLevel(merged, level + 1, L1_MAX_SIZE);
                } catch (Throwable e) {
                    throw Utils.rethrow(e);
                }
                notify();
            }
        });
    }

    // Caller must be synchronized
    private ExecutorService executor(int threadsRequired) throws InterruptedIOException {
        ExecutorService executor = mExecutor;
        if (executor == null) {
            mExecutor = executor = Executors.newCachedThreadPool(r -> {
                Thread t = new Thread(r);
                t.setDaemon(true);
                return t;
            });
        }

        while (mActiveThreads + threadsRequired > MAX_THREAD_COUNT) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }
        }

        mActiveThreads += threadsRequired;

        return executor;
    }
}
