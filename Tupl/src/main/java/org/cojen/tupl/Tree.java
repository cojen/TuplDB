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
final class Tree implements View {
    final TreeNodeStore mStore;

    // Name is null for the registry tree.
    final byte[] mNameKey;

    // Although tree roots can be created and deleted, the object which refers
    // to the root remains the same. Internal state is transferred to/from this
    // object when the tree root changes.
    final TreeNode mRoot;

    // Maintain a stack of stubs, which are created when root nodes are
    // deleted. When a new root is created, a stub is popped, and cursors bound
    // to it are transferred into the new root. Access to this stack is guarded
    // by the root node latch.
    private Stub mStubTail;

    Tree(TreeNodeStore store, byte[] nameKey, TreeNode root) {
        mStore = store;
        mNameKey = nameKey;
        mRoot = root;
    }

    @Override
    public Cursor newCursor() {
        return new FullCursor(new TreeCursor(this));
    }

    @Override
    public long count() throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public boolean exists(byte[] key) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public boolean exists(byte[] key, byte[] value) throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
        return mRoot.search(this, key);
    }

    @Override
    public void store(byte[] key, byte[] value) throws IOException {
        TreeCursor cursor = new TreeCursor(this);
        try {
            cursor.findAndStore(key, value);
        } finally {
            cursor.reset();
        }
    }

    @Override
    public boolean insert(byte[] key, byte[] value) throws IOException {
        TreeCursor cursor = new TreeCursor(this);
        try {
            return cursor.findAndInsert(key, value);
        } finally {
            cursor.reset();
        }
    }

    @Override
    public boolean replace(byte[] key, byte[] value) throws IOException {
        TreeCursor cursor = new TreeCursor(this);
        try {
            return cursor.findAndReplace(key, value);
        } finally {
            cursor.reset();
        }
    }

    @Override
    public boolean update(byte[] key, byte[] oldValue, byte[] newValue) throws IOException {
        TreeCursor cursor = new TreeCursor(this);
        try {
            return cursor.findAndUpdate(key, oldValue, newValue);
        } finally {
            cursor.reset();
        }
    }

    @Override
    public boolean delete(byte[] key) throws IOException {
        return replace(key, null);
    }

    @Override
    public boolean remove(byte[] key, byte[] value) throws IOException {
        return update(key, value, null);
    }

    @Override
    public void clear() throws IOException {
        // FIXME
        throw null;
    }

    @Override
    public View viewGe(byte[] key) {
        // FIXME
        throw null;
    }

    @Override
    public View viewGt(byte[] key) {
        // FIXME
        throw null;
    }

    @Override
    public View viewLe(byte[] key) {
        // FIXME
        throw null;
    }

    @Override
    public View viewLt(byte[] key) {
        // FIXME
        throw null;
    }

    @Override
    public View viewPrefix(byte[] keyPrefix) {
        // FIXME
        throw null;
    }

    @Override
    public View viewReverse() {
        // FIXME
        throw null;
    }

    /**
     * @see TreeNodeStore#markDirty
     */
    boolean markDirty(TreeNode node) throws IOException {
        return mStore.markDirty(this, node);
    }

    /**
     * Caller must exclusively hold root latch.
     */
    void addStub(TreeNode node) {
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
    TreeNode tryPopStub() {
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
    TreeNode popStub() {
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
    TreeNode validateStub(TreeNode node) {
        if (node.mId == TreeNode.STUB_ID && node.mLastCursorFrame != null) {
            return node;
        }
        node.releaseExclusive();
        return null;
    }

    static final class Stub {
        final Stub mParent;
        final TreeNode mNode;

        Stub(Stub parent, TreeNode node) {
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
