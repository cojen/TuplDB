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

import java.io.IOException;

import java.util.List;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class Tree implements Index {
    final Database mDatabase;
    final LockManager mLockManager;

    // Id range is [0, 255] for all internal trees.
    final long mId;

    // Id is null for registry and empty for all other internal trees.
    final byte[] mIdBytes;

    // Name is null for internal trees.
    final byte[] mName;

    // Although tree roots can be created and deleted, the object which refers
    // to the root remains the same. Internal state is transferred to/from this
    // object when the tree root changes.
    final Node mRoot;

    // Maintain a stack of stubs, which are created when root nodes are
    // deleted. When a new root is created, a stub is popped, and cursors bound
    // to it are transferred into the new root. Access to this stack is guarded
    // by the root node latch.
    private Stub mStubTail;

    Tree(Database db, long id, byte[] idBytes, byte[] name, Node root) {
        mDatabase = db;
        mLockManager = db.mLockManager;
        mId = id;
        mIdBytes = idBytes;
        mName = name;
        mRoot = root;
    }

    @Override
    public long getId() {
        return mId;
    }

    @Override
    public byte[] getName() {
        return mName.clone();
    }

    @Override
    public Cursor newCursor(Transaction txn) {
        return new TreeCursor(this, txn);
    }

    @Override
    public long count(Transaction txn) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public boolean exists(Transaction txn, byte[] key) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public boolean exists(Transaction txn, byte[] key, byte[] value) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public byte[] get(Transaction txn, byte[] key) throws IOException {
        Locker locker = lockForRead(txn, key);
        try {
            return mRoot.search(this, key);
        } finally {
            if (locker != null) {
                locker.unlock();
            }
        }
    }

    @Override
    public void store(Transaction txn, byte[] key, byte[] value) throws IOException {
        TreeCursor cursor = new TreeCursor(this, txn);
        try {
            cursor.findAndStore(key, value);
        } finally {
            // FIXME: this can deadlock, because exception can be thrown at anytime
            cursor.reset();
        }
    }

    @Override
    public boolean insert(Transaction txn, byte[] key, byte[] value) throws IOException {
        TreeCursor cursor = new TreeCursor(this, txn);
        try {
            return cursor.findAndModify(key, TreeCursor.MODIFY_INSERT, value);
        } finally {
            // FIXME: this can deadlock, because exception can be thrown at anytime
            cursor.reset();
        }
    }

    @Override
    public boolean replace(Transaction txn, byte[] key, byte[] value) throws IOException {
        TreeCursor cursor = new TreeCursor(this, txn);
        try {
            return cursor.findAndModify(key, TreeCursor.MODIFY_REPLACE, value);
        } finally {
            // FIXME: this can deadlock, because exception can be thrown at anytime
            cursor.reset();
        }
    }

    @Override
    public boolean update(Transaction txn, byte[] key, byte[] oldValue, byte[] newValue)
        throws IOException
    {
        TreeCursor cursor = new TreeCursor(this, txn);
        try {
            return cursor.findAndModify(key, oldValue, newValue);
        } finally {
            // FIXME: this can deadlock, because exception can be thrown at anytime
            cursor.reset();
        }
    }

    @Override
    public boolean delete(Transaction txn, byte[] key) throws IOException {
        return replace(txn, key, null);
    }

    @Override
    public boolean remove(Transaction txn, byte[] key, byte[] value) throws IOException {
        return update(txn, key, value, null);
    }

    @Override
    public void clear(Transaction txn) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public OrderedView viewGe(byte[] key) {
        // FIXME
        throw null;
    }

    @Override
    public OrderedView viewGt(byte[] key) {
        // FIXME
        throw null;
    }

    @Override
    public OrderedView viewLe(byte[] key) {
        // FIXME
        throw null;
    }

    @Override
    public OrderedView viewLt(byte[] key) {
        // FIXME
        throw null;
    }

    @Override
    public OrderedView viewPrefix(byte[] keyPrefix) {
        // FIXME
        throw null;
    }

    @Override
    public OrderedView viewReverse() {
        // FIXME
        throw null;
    }

    /**
     * Returns true if a shared lock can be immediately granted. Caller must
     * hold a coarse latch to prevent this state from changing.
     *
     * @param locker optional locker
     */
    boolean isLockAvailable(Locker locker, byte[] key) {
        return mLockManager.isAvailable(locker, mId, key, LockManager.hashCode(mId, key));
    }

    /**
     * @param txn optional transaction instance
     * @param key non-null key instance
     * @return non-null Locker instance if caller should unlock when read is done
     */
    private Locker lockForRead(Transaction txn, byte[] key) throws LockFailureException {
        if (txn == null) {
            return lockSharedLocal(key);
        }

        switch (txn.lockMode()) {
        default: // No read lock requested by READ_UNCOMMITTED or UNSAFE.
            return null;

        case READ_COMMITTED:
            return txn.lockShared(mId, key) == LockResult.ACQUIRED ? txn : null;

        case REPEATABLE_READ:
            txn.lockShared(mId, key);
            return null;

        case UPGRADABLE_READ:
            txn.lockUpgradable(mId, key);
            return null;
        }
    }

    /**
     * @param txn optional transaction instance
     * @param key non-null key instance
     * @return non-null Locker instance if caller should unlock when write is done
     */
    Locker lockExclusive(Transaction txn, byte[] key) throws LockFailureException {
        if (txn == null) {
            return lockExclusiveLocal(key);
        }

        if (txn.lockMode() != LockMode.UNSAFE) {
            txn.lockExclusive(mId, key);
        }

        return null;
    }

    Locker lockSharedLocal(byte[] key) throws LockFailureException {
        return mLockManager.lockSharedLocal(mId, key);
    }

    Locker lockExclusiveLocal(byte[] key) throws LockFailureException {
        return mLockManager.lockExclusiveLocal(mId, key);
    }

    void redoStore(byte[] key, byte[] value) throws IOException {
        mDatabase.mRedoLog.store(mId, key, value, mDatabase.mDurabilityMode);
    }

    /**
     * @see Database#markDirty
     */
    boolean markDirty(Node node) throws IOException {
        return mDatabase.markDirty(this, node);
    }

    /**
     * Caller must exclusively hold root latch.
     */
    void addStub(Node node) {
        mStubTail = new Stub(mStubTail, node);
    }

    /**
     * Caller must exclusively hold root latch.
     */
    boolean hasStub() {
        return mStubTail != null;
    }

    /**
     * Attempts to exclusively latch and pop the tail stub node. Returns null
     * if latch cannot be immediatly obtained. Caller must exclusively hold
     * root latch and have checked that a stub exists.
     */
    Node tryPopStub() {
        Stub stub = mStubTail;
        if (stub.mNode.tryAcquireExclusiveUnfair()) {
            mStubTail = stub.mParent;
            return stub.mNode;
        }
        return null;
    }

    /**
     * Exclusively latches and pops the tail stub node. Caller must exclusively
     * hold root latch and have checked that a stub exists.
     */
    Node popStub() {
        Stub stub = mStubTail;
        stub.mNode.acquireExclusiveUnfair();
        mStubTail = stub.mParent;
        return stub.mNode;
    }

    /**
     * Checks if popped stub is still valid, because it has not been evicted
     * and it actually has cursors bound to it. Caller must hold exclusive
     * latch, which is released if node is not valid.
     *
     * @return node if valid, null otherwise
     */
    Node validateStub(Node node) {
        if (node.mId == Node.STUB_ID && node.mLastCursorFrame != null) {
            return node;
        }
        node.releaseExclusive();
        return null;
    }

    static final class Stub {
        final Stub mParent;
        final Node mNode;

        Stub(Stub parent, Node node) {
            mParent = parent;
            mNode = node;
        }
    }

    /**
     * Gather all dirty pages which should be committed. Caller must acquire
     * shared latch on root node, which is released by this method.
     */
    void gatherDirtyNodes(List<DirtyNode> dirtyList, int dirtyState) throws IOException {
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
        // short-circuiting. If the node was written, then it needs to be
        // traversed into, to gather up additional dirty nodes.

        int mi = dirtyList.size();
        dirtyList.add(new DirtyNode(mRoot, mRoot.mId));

        for (; mi<dirtyList.size(); mi++) {
            Node node = dirtyList.get(mi).mNode;

            if (node.isLeaf()) {
                node.releaseShared();
                continue;
            }

            Node[] childNodes = node.mChildNodes;

            for (int ci=0; ci<childNodes.length; ci++) {
                Node childNode = childNodes[ci];
                if (childNode != null) {
                    long childId = node.retrieveChildRefIdFromIndex(ci);
                    if (childId == childNode.mId) {
                        childNode.acquireSharedUnfair();
                        if (childId == childNode.mId && childNode.mCachedState == dirtyState) {
                            dirtyList.add(new DirtyNode(childNode, childId));
                        } else {
                            childNode.releaseShared();
                        }
                    }
                }
            }

            node.releaseShared();
        }
    }
}
