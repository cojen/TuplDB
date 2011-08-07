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

import java.io.Closeable;
import java.io.InterruptedIOException;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import java.util.concurrent.locks.Lock;

import static org.cojen.tupl.TreeNode.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class TreeNodeStore implements Closeable {
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

        if (maxCachedNodeCount < 3) {
            // One is needed for the root node, and at least two nodes are
            // required for eviction code to function correctly. It always
            // assumes that the least recently used node points to a valid,
            // more recently used node.
            throw new IllegalArgumentException
                ("Maximum cached node count is too small: " + maxCachedNodeCount);
        }

        mPageStore = store;

        mSpareBufferPool = new BufferPool(store.pageSize(), spareBufferCount);

        mCacheLatch = new Latch();
        mMaxCachedNodeCount = maxCachedNodeCount - 1; // less one for root

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
            for (int i=minCachedNodeCount; --i>0; ) { // less one for root
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
                    if (node.evict(this)) {
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
     * Similar to markDirty method except no new page is reserved, and old page
     * is not immediately deleted. Caller must hold commit lock and exclusive
     * latch on node. Latch is never released by this method, even if an
     * exception is thrown.
     */
    void prepareToDelete(TreeNode node) throws IOException {
        // Hello. My name is Íñigo Montoya. You killed my father. Prepare to die. 
        byte state = node.mCachedState;
        if (state != CACHED_CLEAN && state != mCommitState) {
            node.write(this);
        }
    }

    /**
     * Caller must hold commit lock and exclusive latch on node. Latch is
     * never released by this method, even if an exception is thrown.
     */
    void deleteNode(TreeNode node) throws IOException {
        long id = node.mId;
        if (id != 0) {
            // TODO: Id can immediately be re-used, depending on the cached
            // state. Also see notes in the TreeNode.evict method.
            mPageStore.deletePage(id);
        }

        node.mId = 0;
        // FIXME: child node array should be recycled
        node.mChildNodes = null;

        // When node is re-allocated, it will be evicted. Ensure that eviction
        // doesn't write anything.
        node.mCachedState = CACHED_CLEAN;

        // Indicate that node is least recently used, allowing it to be
        // re-allocated immediately without evicting another node.
        mCacheLatch.acquireExclusiveUnfair();
        try {
            TreeNode lessUsed = node.mLessUsed;
            if (lessUsed != null) {
                TreeNode moreUsed = node.mMoreUsed;
                if ((lessUsed.mMoreUsed = moreUsed) == null) {
                    mMostRecentlyUsed = lessUsed;
                } else {
                    moreUsed.mLessUsed = lessUsed;
                }
                node.mLessUsed = null;
                (node.mMoreUsed = mLeastRecentlyUsed).mLessUsed = node;
                mLeastRecentlyUsed = node;
            }
        } finally {
            mCacheLatch.releaseExclusive();
        }
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

    @Override
    public void close() throws IOException {
        mPageStore.close();
    }

    /**
     * Durably commit all changes to the tree, while allowing changes to be
     * made concurrently. Only one thread is granted access to this method.
     */
    void commit() throws IOException {
        final TreeNode root = mRoot;

        // Commit lock must be acquired first, to prevent deadlock.
        mPageStore.exclusiveCommitLock().lock();
        root.acquireSharedUnfair();
        if (root.mCachedState == CACHED_CLEAN) {
            // Root is clean, so nothing to do.
            root.releaseShared();
            mPageStore.exclusiveCommitLock().unlock();
            return;
        }

        mPageStore.commit(new PageStore.CommitCallback() {
            @Override
            public byte[] prepare() throws IOException {
                return flush();
            }
        });
    }

    /**
     * Method is invoked with exclusive commit lock and shared root node latch held.
     */
    private byte[] flush() throws IOException {
        final TreeNode root = mRoot;
        final long rootId = root.mId;
        final int stateToFlush = mCommitState;
        mCommitState = (byte) (CACHED_DIRTY_0 + ((stateToFlush - CACHED_DIRTY_0) ^ 1));
        mPageStore.exclusiveCommitLock().unlock();

        // Perform a breadth-first traversal of tree, finding dirty nodes. This
        // step can effectively deny most concurrent access to the tree, but I
        // cannot figure out a safe way to find dirty nodes and allow access
        // back into the tree. Depth-first traversal using a cursor allows
        // concurrent access, but it causes some dirty nodes to get lost. It
        // might be possible to speed this step up with multiple threads -- at
        // most one per processor.

        // One approach for performing depth-first traversal is to remember
        // internal nodes which were concurrently updated, in a map. When the
        // traversal sees a clean node, it consults the map instead of
        // short-circuiting. If the node was written, then it needs to
        // traversed into, to gather up additional dirty nodes.

        List<Dirty> dirtyList = new ArrayList<Dirty>(Math.min(1000, mMaxCachedNodeCount));
        dirtyList.add(new Dirty(root, root.mId));

        for (int mi=0; mi<dirtyList.size(); mi++) {
            TreeNode node = dirtyList.get(mi).mNode;

            if (node.isLeaf()) {
                node.releaseShared();
                continue;
            }

            TreeNode[] childNodes = node.mChildNodes;

            for (int ci=0; ci<childNodes.length; ci++) {
                TreeNode childNode = childNodes[ci];
                if (childNode != null) {
                    long childId = node.retrieveChildRefIdFromIndex(ci);
                    if (childId == childNode.mId) {
                        childNode.acquireSharedUnfair();
                        if (childId == childNode.mId && childNode.mCachedState == stateToFlush) {
                            dirtyList.add(new Dirty(childNode, childId));
                        } else {
                            childNode.releaseShared();
                        }
                    }
                }
            }

            node.releaseShared();
        }

        // Sort nodes by id, which helps make writes more sequentially ordered.
        Collections.sort(dirtyList);

        // Now write out all the dirty nodes. Some of them will have already
        // been concurrently written out, so check again.

        for (int mi=0; mi<dirtyList.size(); mi++) {
            TreeNode node = dirtyList.get(mi).mNode;
            dirtyList.set(mi, null);
            node.acquireExclusiveUnfair();
            if (node.mCachedState != stateToFlush) {
                // Was already evicted.
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

    static final class Dirty implements Comparable<Dirty> {
        TreeNode mNode;
        long mId;

        Dirty(TreeNode node, long id) {
            mNode = node;
            // Stable copy of id that can be accessed without re-latching.
            mId = id;
        }

        @Override
        public int compareTo(Dirty other) {
            long a = mId;
            long b = other.mId;
            return a < b ? -1 : (a > b ? 1 : 0);
        }
    }
}
