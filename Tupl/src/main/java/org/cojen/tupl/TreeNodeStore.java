/*
 *  Copyright 2011 Brian S O'Neill
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
import java.util.List;

import java.util.concurrent.locks.Lock;

import static org.cojen.tupl.TreeNode.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class TreeNodeStore {
    private static final int ENCODING_VERSION = 20110514;

    private final PageStore mPageStore;

    private final BufferPool mSpareBufferPool;

    private final Latch mCacheLatch;
    private final int mMaxCachedNodeCount;
    private int mCachedNodeCount;
    private TreeNode mMostRecentlyUsed;
    private TreeNode mLeastRecentlyUsed;

    private final Lock mSharedCommitLock;

    private final TreeNode mRoot;

    // Is either CACHED_DIRTY_0 or CACHED_DIRTY_1. Access is guarded by commit lock.
    private byte mCommitState;

    TreeNodeStore(PageStore store, int minCachedNodeCount, int maxCachedNodeCount)
        throws IOException
    {
        this(store, minCachedNodeCount, maxCachedNodeCount,
             Runtime.getRuntime().availableProcessors());
    }

    TreeNodeStore(PageStore store, int minCachedNodeCount, int maxCachedNodeCount,
                  int spareBufferCount)
        throws IOException
    {
        if (minCachedNodeCount > maxCachedNodeCount) {
            throw new IllegalArgumentException
                ("Minimum cached node count exceeds maximum count: " +
                 minCachedNodeCount + " > " + maxCachedNodeCount);
        }

        if (maxCachedNodeCount < 2) {
            // At least two nodes are required for eviction code to function
            // correctly. It always assumes that the least recently used node
            // points to a valid more recently used node.
            throw new IllegalArgumentException
                ("Maximum cached node count is too small: " + maxCachedNodeCount);
        }

        mPageStore = store;

        mSpareBufferPool = new BufferPool(store.pageSize(), spareBufferCount);

        mCacheLatch = new Latch();
        mMaxCachedNodeCount = maxCachedNodeCount;

        mSharedCommitLock = store.sharedCommitLock();
        mSharedCommitLock.lock();
        try {
            mCommitState = CACHED_DIRTY_0;
        } finally {
            mSharedCommitLock.unlock();
        }

        mRoot = loadRoot();

        // Pre-allocate nodes. They are automatically added to the usage list,
        // and so nothing special needs to be done to allow them to get used. Since
        // the initial state is clean, evicting these nodes does nothing.
        try {
            for (int i=minCachedNodeCount; --i>=0; ) {
                allocLatchedNode().releaseExclusive();
            }
        } catch (OutOfMemoryError e) {
            mMostRecentlyUsed = null;
            mLeastRecentlyUsed = null;
            throw new OutOfMemoryError
                ("Unable to allocate the minimum required number of cached nodes: " +
                 minCachedNodeCount);
        }
    }

    /**
     * Loads the root node, or creates one if store is new. Root node is not eligible for
     * eviction.
     */
    private TreeNode loadRoot() throws IOException {
        byte[] header = new byte[12];
        mPageStore.readExtraCommitData(header);
        int version = DataIO.readInt(header, 0);

        if (version == 0) {
            // Assume store is new and return a new empty leaf node.
            return new TreeNode(pageSize(), true);
        }

        if (version != ENCODING_VERSION) {
            throw new CorruptPageStoreException("Unknown encoding version: " + version);
        }

        long rootId = DataIO.readLong(header, 4);

        TreeNode root = new TreeNode(pageSize(), false);
        root.read(this, rootId);
        return root;
    }

    /**
     * Returns the tree root node, which is always the same instance.
     */
    TreeNode root() {
        return mRoot;
    }

    /**
     * Returns the fixed size of all pages in the store, in bytes.
     */
    int pageSize() {
        return mPageStore.pageSize();
    }

    /**
     * Access the shared commit lock, which prevents commits while held.
     */
    Lock sharedCommitLock() {
        return mSharedCommitLock;
    }

    /**
     * Returns a new or recycled TreeNode instance, latched exclusively, with an id
     * of zero and a clean state.
     */
    TreeNode allocLatchedNode() throws IOException {
        mCacheLatch.acquireExclusiveUnfair();
        try {
            int max = mMaxCachedNodeCount;
            if (mCachedNodeCount < max) {
                TreeNode node = new TreeNode(pageSize(), false);
                node.acquireExclusiveUnfair();

                mCachedNodeCount++;
                if ((node.mLessUsed = mMostRecentlyUsed) == null) {
                    mLeastRecentlyUsed = node;
                } else {
                    mMostRecentlyUsed.mMoreUsed = node;
                }
                mMostRecentlyUsed = node;

                return node;
            }

            do {
                TreeNode node = mLeastRecentlyUsed;
                (mLeastRecentlyUsed = node.mMoreUsed).mLessUsed = null;
                node.mMoreUsed = null;
                (node.mLessUsed = mMostRecentlyUsed).mMoreUsed = node;
                mMostRecentlyUsed = node;

                if (node.tryAcquireExclusiveUnfair()) {
                    if (evict(node)) {
                        // Return with latch still held.
                        return node;
                    } else {
                        node.releaseExclusive();
                    }
                }
            } while (--max > 0);
        } finally {
            mCacheLatch.releaseExclusive();
        }

        // FIXME: Throw a better exception. Also, try all nodes again, but with
        // stronger latch request before giving up.
        throw new IllegalStateException("Cache is full");
    }

    /**
     * Returns a new reserved node, latched exclusively and marked dirty. Caller
     * must hold commit lock.
     */
    TreeNode newNodeForSplit() throws IOException {
        TreeNode node = allocLatchedNode();
        node.mId = mPageStore.reservePage();
        node.mCachedState = mCommitState;
        return node;
    }

    /**
     * Caller must hold exclusive latch on node and commit lock. Latch is never released
     * by this method, even if an exception is thrown.
     *
     * @return false if node cannot be evicted
     */
    private boolean evict(TreeNode node) throws IOException {
        if (!node.canEvict()) {
            return false;
        }

        int state = node.mCachedState;
        if (state != CACHED_CLEAN) {
            // TODO: Keep some sort of cache of ids known to be dirty. If
            // reloaded before commit, then they're still dirty. Without this
            // optimization, too many pages are allocated when: evictions are
            // high, write rate is high, and commits are bogged down. A Bloom
            // filter is not appropriate, because of false positives.
            mSharedCommitLock.lock();
            try {
                node.write(this);
            } finally {
                mSharedCommitLock.unlock();
            }
            node.mCachedState = CACHED_CLEAN;
        }

        node.mId = 0;
        // FIXME: child node array should be recycled
        node.mChildNodes = null;

        return true;
    }

    /**
     * Caller must hold commit lock and any latch on node.
     */
    boolean shouldMarkDirty(TreeNode node) {
        return node.mCachedState != mCommitState;
    }

    /**
     * Caller must hold commit lock and exclusive latch on node. Method does
     * nothing if node is already dirty. Latch is never released by this method,
     * even if an exception is thrown.
     *
     * @return true if just made dirty and id changed
     */
    boolean markDirty(TreeNode node) throws IOException {
        byte state = node.mCachedState;
        if (state == mCommitState) {
            return false;
        } else {
            doMarkDirty(node);
            return true;
        }
    }

    /**
     * Caller must hold commit lock and exclusive latch on node. Method must
     * not be called if node is already dirty. Latch is never released by this
     * method, even if an exception is thrown.
     */
    void doMarkDirty(TreeNode node) throws IOException {
        long oldId = node.mId;
        long newId = mPageStore.reservePage();
        if (oldId != 0) {
            mPageStore.deletePage(oldId);
        }
        if (node.mCachedState != CACHED_CLEAN) {
            node.write(this);
        }
        node.mId = newId;
        node.mCachedState = mCommitState;
    }

    /**
     * Indicate that non-root node is most recently used. Root node is not
     * managed in usage list and cannot be evicted.
     */
    void used(TreeNode node) {
        // Because this method can be a bottleneck, don't wait for exclusive
        // latch. If node is popular, it will get more chances to be identified
        // as most recently used. This strategy works well enough because cache
        // eviction is always a best-guess approach.
        if (mCacheLatch.tryAcquireExclusiveUnfair()) {
            TreeNode moreUsed = node.mMoreUsed;
            if (moreUsed != null) {
                TreeNode lessUsed = node.mLessUsed;
                if ((moreUsed.mLessUsed = lessUsed) == null) {
                    mLeastRecentlyUsed = moreUsed;
                } else {
                    lessUsed.mMoreUsed = moreUsed;
                }
                node.mMoreUsed = null;
                (node.mLessUsed = mMostRecentlyUsed).mMoreUsed = node;
                mMostRecentlyUsed = node;
            }
            mCacheLatch.releaseExclusive();
        }
    }

    byte[] removeSpareBuffer() throws InterruptedIOException {
        return mSpareBufferPool.remove();
    }

    void addSpareBuffer(byte[] buffer) {
        mSpareBufferPool.add(buffer);
    }

    void readPage(long id, byte[] page) throws IOException {
        mPageStore.readPage(id, page);
    }

    void writeReservedPage(long id, byte[] page) throws IOException {
        mPageStore.writeReservedPage(id, page);
    }

    /**
     * Durably commit all changes to the tree, while allowing changes to be
     * made concurrently. Only one thread is granted access to this method.
     */
    void commit() throws IOException {
        final TreeNode root = mRoot;

        // Quick check.
        root.acquireSharedUnfair();
        try {
            if (root.mCachedState == CACHED_CLEAN) {
                // Root is clean, so nothing to do.
                return;
            }
        } finally {
            root.releaseShared();
        }

        // Commit lock must be acquired first, to prevent deadlock.
        mPageStore.exclusiveCommitLock().lock();
        root.acquireExclusiveUnfair();
        if (root.mCachedState == CACHED_CLEAN) {
            // Root is clean, so nothing to do.
            root.releaseExclusive();
            mPageStore.exclusiveCommitLock().unlock();
            return;
        }

        mPageStore.commit(new PageStore.CommitCallback() {
            @Override
            public byte[] prepare() throws IOException {
                return flush(root);
            }
        });
    }

    /**
     * Method is invoked with exclusive commit lock and root node latch held.
     */
    private byte[] flush(TreeNode root) throws IOException {
        final long rootId = root.mId;
        final int stateToFlush = mCommitState;
        mCommitState = (byte) (CACHED_DIRTY_0 + ((stateToFlush - CACHED_DIRTY_0) ^ 1));
        mPageStore.exclusiveCommitLock().unlock();

        // TODO: When cursor based traversal is available, use that instead of
        // breadth-first traversal.

        // Perform a breadth-first traversal of tree, finding dirty nodes.
        // Because this step can effectively deny all concurrent access to
        // the tree, acquire latches unfairly to speed it up.
        List<TreeNode> dirty = new ArrayList<TreeNode>();
        dirty.add(root);

        for (int mi=0; mi<dirty.size(); mi++) {
            TreeNode node = dirty.get(mi);

            if (node.isLeaf()) {
                node.releaseExclusive();
                continue;
            }

            // Allow reads that don't load children into the node.
            node.downgrade();

            TreeNode[] childNodes = node.mChildNodes;

            for (int ci=0; ci<childNodes.length; ci++) {
                TreeNode childNode = childNodes[ci];
                if (childNode != null) {
                    long childId = node.retrieveChildRefIdFromIndex(ci);
                    if (childId == childNode.mId) {
                        childNode.acquireExclusiveUnfair();
                        if (childId == childNode.mId && childNode.mCachedState == stateToFlush) {
                            dirty.add(childNode);
                        } else {
                            childNode.releaseExclusive();
                        }
                    }
                }
            }

            node.releaseShared();
        }

        // Now sweep through dirty nodes. This could be performed by scanning
        // the tree itself, but this leads to race conditions.

        for (int mi=0; mi<dirty.size(); mi++) {
            TreeNode node = dirty.get(mi);
            dirty.set(mi, null);
            node.acquireExclusiveUnfair();
            if (node.mCachedState != stateToFlush) {
                node.releaseExclusive();
            } else {
                node.mCachedState = CACHED_CLEAN;
                node.downgrade();
                try {
                    node.write(this);
                } finally {
                    node.releaseShared();
                }
            }
        }

        byte[] header = new byte[12];
        DataIO.writeInt(header, 0, ENCODING_VERSION);
        DataIO.writeLong(header, 4, rootId);

        return header;
    }
}
