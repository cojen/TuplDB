/*
 *  Copyright 2011-2015 Cojen.org
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.InterruptedIOException;
import java.io.IOException;

import static org.cojen.tupl.PageOps.*;
import static org.cojen.tupl.Utils.*;

/**
 * B-tree implementation.
 *
 * @author Brian S O'Neill
 */
class Tree implements View, Index {
    // Reserved internal tree ids.
    static final int
        REGISTRY_ID = 0,
        REGISTRY_KEY_MAP_ID = 1,
        //PAGE_ALLOCATOR_ID = 2,
        FRAGMENTED_TRASH_ID = 3,
        MAX_RESERVED_ID = 0xff;

    static boolean isInternal(long id) {
        return (id & ~0xff) == 0;
    }

    final LocalDatabase mDatabase;
    final LockManager mLockManager;

    // Id range is [0, 255] for all internal trees.
    final long mId;

    // Id is null for registry.
    final byte[] mIdBytes;

    // Name is null for all internal trees.
    volatile byte[] mName;

    // Although tree roots can be created and deleted, the object which refers
    // to the root remains the same. Internal state is transferred to/from this
    // object when the tree root changes.
    final Node mRoot;

    final int mMaxKeySize;
    final int mMaxEntrySize;

    Tree(LocalDatabase db, long id, byte[] idBytes, byte[] name, Node root) {
        mDatabase = db;
        mLockManager = db.mLockManager;
        mId = id;
        mIdBytes = idBytes;
        mName = name;
        mRoot = root;

        int pageSize = db.pageSize();

        // Key size is limited to ensure that internal nodes can hold at least two keys.
        // Absolute maximum is dictated by key encoding, as described in Node class.
        mMaxKeySize = Math.min(16383, (pageSize >> 1) - 22);

        // Limit maximum non-fragmented entry size to 0.75 of usable node size.
        mMaxEntrySize = ((pageSize - Node.TN_HEADER_SIZE) * 3) >> 2;
    }

    int pageSize() {
        return mDatabase.pageSize();
    }

    @Override
    public final String toString() {
        return toString(this);
    }

    static final String toString(Index ix) {
        StringBuilder b = new StringBuilder(ix.getClass().getName());
        b.append('@').append(Integer.toHexString(ix.hashCode()));
        b.append(" {");
        String nameStr = ix.getNameString();
        if (nameStr != null) {
            b.append("name").append(": ").append(nameStr);
            b.append(", ");
        }
        b.append("id").append(": ").append(ix.getId());
        return b.append('}').toString();
    }

    @Override
    public final Ordering getOrdering() {
        return Ordering.ASCENDING;
    }

    @Override
    public final long getId() {
        return mId;
    }

    @Override
    public final byte[] getName() {
        return cloneArray(mName);
    }

    @Override
    public final String getNameString() {
        byte[] name = mName;
        if (name == null) {
            return null;
        }
        try {
            return new String(name, "UTF-8");
        } catch (IOException e) {
            return new String(name);
        }
    }

    @Override
    public TreeCursor newCursor(Transaction txn) {
        return new TreeCursor(this, txn);
    }

    /*
    @Override
    public long count(byte[] lowKey, byte[] highKey) throws IOException {
        // FIXME: use bottom internal node counts if allowStoredCounts is true
        throw null;
    }
    */

    @Override
    public final byte[] load(Transaction txn, byte[] key) throws IOException {
        LocalTransaction local = check(txn);
        Locker locker = lockForLoad(local, key);
        try {
            return Node.search(mRoot, this, key);
        } finally {
            if (locker != null) {
                locker.unlock();
            }
        }
    }

    @Override
    public void store(Transaction txn, byte[] key, byte[] value) throws IOException {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }
        TreeCursor cursor = new TreeCursor(this, txn);
        cursor.autoload(false);
        cursor.findAndStore(key, value);
    }

    @Override
    public byte[] exchange(Transaction txn, byte[] key, byte[] value) throws IOException {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }
        return new TreeCursor(this, txn).findAndStore(key, value);
    }

    @Override
    public boolean insert(Transaction txn, byte[] key, byte[] value) throws IOException {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }
        TreeCursor cursor = new TreeCursor(this, txn);
        cursor.autoload(false);
        return cursor.findAndModify(key, TreeCursor.MODIFY_INSERT, value);
    }

    @Override
    public boolean replace(Transaction txn, byte[] key, byte[] value) throws IOException {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }
        TreeCursor cursor = new TreeCursor(this, txn);
        cursor.autoload(false);
        return cursor.findAndModify(key, TreeCursor.MODIFY_REPLACE, value);
    }

    @Override
    public boolean update(Transaction txn, byte[] key, byte[] oldValue, byte[] newValue)
        throws IOException
    {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }
        return new TreeCursor(this, txn).findAndModify(key, oldValue, newValue);
    }

    @Override
    public final LockResult lockShared(Transaction txn, byte[] key) throws LockFailureException {
        return check(txn).lockShared(mId, key);
    }

    @Override
    public final LockResult lockUpgradable(Transaction txn, byte[] key)
        throws LockFailureException
    {
        return check(txn).lockUpgradable(mId, key);
    }

    @Override
    public final LockResult lockExclusive(Transaction txn, byte[] key)
        throws LockFailureException
    {
        return check(txn).lockExclusive(mId, key);
    }

    @Override
    public final LockResult lockCheck(Transaction txn, byte[] key) {
        return check(txn).lockCheck(mId, key);
    }

    @Override
    public Stream newStream() {
        TreeCursor cursor = new TreeCursor(this);
        cursor.autoload(false);
        return new TreeValueStream(cursor);
    }

    @Override
    public View viewGe(byte[] key) {
        return BoundedView.viewGe(this, key);
    }

    @Override
    public View viewGt(byte[] key) {
        return BoundedView.viewGt(this, key);
    }

    @Override
    public View viewLe(byte[] key) {
        return BoundedView.viewLe(this, key);
    }

    @Override
    public View viewLt(byte[] key) {
        return BoundedView.viewLt(this, key);
    }

    @Override
    public View viewPrefix(byte[] prefix, int trim) {
        return BoundedView.viewPrefix(this, prefix, trim);
    }

    @Override
    public final boolean isUnmodifiable() {
        return isClosed();
    }

    @Override
    public long evict(Transaction txn, byte[] lowKey, byte[] highKey, byte[][] keyRef, byte[][] valueRef, int maxEntriesToEvict) throws IOException {
        return new TreeCursor(this, txn).evict(lowKey, highKey, keyRef, valueRef);
    }

    @Override
    public Stats analyze(byte[] lowKey, byte[] highKey) throws IOException {
        TreeCursor cursor = new TreeCursor(this, Transaction.BOGUS);
        try {
            cursor.autoload(false);
            cursor.random(lowKey, highKey);
            return cursor.key() == null ? new Stats(0, 0, 0, 0, 0) : cursor.analyze();
        } catch (Throwable e) {
            cursor.reset();
            throw e;
        }
    }

    /**
     * Returns a view which can be passed to an observer. Internal trees are returned as
     * unmodifiable.
     */
    final Index observableView() {
        return isInternal(mId) ? new UnmodifiableView(this) : this;
    }

    /**
     * @param view view to pass to observer
     * @return false if compaction should stop
     */
    final boolean compactTree(Index view, long highestNodeId, CompactionObserver observer)
        throws IOException
    {
        try {
            if (!observer.indexBegin(view)) {
                return false;
            }
        } catch (Throwable e) {
            uncaught(e);
            return false;
        }

        TreeCursor cursor = new TreeCursor(this, Transaction.BOGUS);
        try {
            cursor.autoload(false);

            // Find the first entry instead of calling first() to ensure that cursor is
            // positioned. Otherwise, empty trees would be skipped even when the root node
            // needed to be moved out of the compaction zone.
            cursor.find(EMPTY_BYTES);

            if (!cursor.compact(highestNodeId, observer)) {
                return false;
            }

            try {
                if (!observer.indexComplete(view)) {
                    return false;
                }
            } catch (Throwable e) {
                uncaught(e);
                return false;
            }

            return true;
        } finally {
            cursor.reset();
        }
    }

    @Override
    public final boolean verify(VerificationObserver observer) throws IOException {
        if (observer == null) {
            observer = new VerificationObserver();
        }
        Index view = observableView();
        observer.failed = false;
        verifyTree(view, observer);
        boolean passed = !observer.failed;
        observer.indexComplete(view, passed, null);
        return passed;
    }

    /**
     * @param view view to pass to observer
     * @return false if should stop
     */
    final boolean verifyTree(Index view, VerificationObserver observer) throws IOException {
        TreeCursor cursor = new TreeCursor(this, Transaction.BOGUS);
        try {
            cursor.autoload(false);
            cursor.first();
            int height = cursor.height();
            if (!observer.indexBegin(view, height)) {
                cursor.reset();
                return false;
            }
            if (!cursor.verify(height, observer)) {
                cursor.reset();
                return false;
            }
            cursor.reset();
        } catch (Throwable e) {
            observer.failed = true;
            throw e;
        }
        return true;
    }

    @Override
    public final void close() throws IOException {
        close(false, false);
    }

    /**
     * @param rootLatched true if root node is already latched by the current thread
     * @return root node if forDelete; null if already closed
     */
    final Node close(boolean forDelete, final boolean rootLatched) throws IOException {
        Node root = mRoot;

        if (!rootLatched) {
            root.acquireExclusive();
        }

        try {
            if (root.mPage == p_closedTreePage()) {
                // Already closed.
                return null;
            }

            if (isInternal(mId)) {
                throw new IllegalStateException("Cannot close an internal index");
            }

            if (root.mLastCursorFrame != null) {
                // If any active cursors, they might be in the middle of performing node splits
                // and merges. With the exclusive commit lock held, this is no longer the case.
                // Once acquired, update the cursors such that they refer to empty nodes.
                root.releaseExclusive();
                mDatabase.commitLock().acquireExclusive();
                try {
                    root.acquireExclusive();
                    if (root.mPage == p_closedTreePage()) {
                        return null;
                    }
                    if (root.mLastCursorFrame != null) {
                        root.invalidateCursors();
                    }
                } finally {
                    mDatabase.commitLock().releaseExclusive();
                }
            }

            // Root node reference cannot be cleared, so instead make it non-functional. Move
            // the page reference into a new evictable Node object, allowing it to be recycled.

            Node newRoot = root.cloneNode();
            mDatabase.swapIfDirty(root, newRoot);

            if (root.mId > 0) {
                mDatabase.nodeMapRemove(root);
            }

            root.closeRoot();

            if (forDelete) {
                mDatabase.treeClosed(this);
                return newRoot;
            }

            newRoot.acquireShared();
            try {
                mDatabase.treeClosed(this);
                newRoot.makeEvictableNow();
                if (newRoot.mId > 0) {
                    mDatabase.nodeMapPut(newRoot);
                }
            } finally {
                newRoot.releaseShared();
            }

            return null;
        } finally {
            if (!rootLatched) {
                root.releaseExclusive();
            }
        }
    }

    @Override 
    public final boolean isClosed() {
        Node root = mRoot;
        root.acquireShared();
        boolean closed = root.mPage == p_closedTreePage();
        root.releaseShared();
        return closed;
    }

    @Override
    public final void drop() throws IOException {
        drop(true).run();
    }

    /**
     * @return delete task
     */
    final Runnable drop(boolean mustBeEmpty) throws IOException {
        Node root = mRoot;
        root.acquireExclusive();
        try {
            if (root.mPage == p_closedTreePage()) {
                throw new ClosedIndexException();
            }

            if (mustBeEmpty && (!root.isLeaf() || root.hasKeys())) {
                // Note that this check also covers the transactional case, because deletes
                // store ghosts. The message could be more accurate, but it would require
                // scanning the whole index looking for ghosts. Using LockMode.UNSAFE deletes
                // it's possible to subvert the transactional case, allowing the drop to
                // proceed. The rollback logic in UndoLog accounts for this, ignoring undo
                // operations for missing indexes. Preventing the drop in this case isn't worth
                // the trouble, because UNSAFE is what it is.
                throw new IllegalStateException("Cannot drop a non-empty index");
            }

            if (isInternal(mId)) {
                throw new IllegalStateException("Cannot close an internal index");
            }

            return mDatabase.deleteTree(this);
        } finally {
            root.releaseExclusive();
        }
    }

    /**
     * Non-transactionally deletes all entries in the tree. No other cursors or threads can be
     * active in the tree. The root node is prepared for deletion as a side effect.
     */
    final void deleteAll() throws IOException {
        TreeCursor c = new TreeCursor(this, Transaction.BOGUS);
        c.autoload(false);
        for (c.first(); c.key() != null; ) {
            c.trim();
        }
    }

    @FunctionalInterface
    static interface NodeVisitor {
        void visit(Node node) throws IOException;
    }

    /**
     * Performs a depth-first traversal of the tree, only visting loaded nodes. Nodes passed to
     * the visitor are latched exclusively, and they must be released by the visitor.
     */
    final void traverseLoaded(NodeVisitor visitor) throws IOException {
        Node node = mRoot;
        node.acquireExclusive();

        if (node.mSplit != null) {
            // Create a temporary frame for the root split.
            CursorFrame frame = new CursorFrame();
            frame.bind(node, 0);
            try {
                node = finishSplit(frame, node);
            } catch (Throwable e) {
                CursorFrame.popAll(frame);
                throw e;
            }
        }

        // Frames are only used for backtracking up the tree. Frame creation and binding is
        // performed late, and none are created for leaf nodes.
        CursorFrame frame = null;
        int pos = 0;

        while (true) {
            toLower: while (node.isInternal()) {
                final int highestPos = node.highestInternalPos();
                while (true) {
                    if (pos > highestPos) {
                        break toLower;
                    }
                    long childId = node.retrieveChildRefId(pos);
                    Node child = mDatabase.nodeMapGet(childId);
                    if (child != null) {
                        child.acquireExclusive();
                        // Need to check again in case evict snuck in.
                        if (childId != child.mId) {
                            child.releaseExclusive();
                        } else {
                            frame = new CursorFrame(frame);
                            frame.bind(node, pos);
                            node.releaseExclusive();
                            node = child;
                            pos = 0;
                            continue toLower;
                        }
                    }
                    pos += 2;
                }
            }

            try {
                visitor.visit(node);
            } catch (Throwable e) {
                CursorFrame.popAll(frame);
                throw e;
            }

            if (frame == null) {
                return;
            }

            node = frame.acquireExclusive();

            if (node.mSplit != null) {
                try {
                    node = finishSplit(frame, node);
                } catch (Throwable e) {
                    CursorFrame.popAll(frame);
                    throw e;
                }
            }

            pos = frame.mNodePos;
            frame = frame.pop();
            pos += 2;
        }
    }

    final void writeCachePrimer(final DataOutput dout) throws IOException {
        traverseLoaded((node) -> {
            byte[] midKey;
            try {
                if (!node.isLeaf()) {
                    return;
                }
                int numKeys = node.numKeys();
                if (numKeys > 1) {
                    int highPos = numKeys & ~1;
                    midKey = node.midKey(highPos - 2, node, highPos);
                } else if (numKeys == 1) {
                    midKey = node.retrieveKey(0);
                } else {
                    return;
                }
            } finally {
                node.releaseExclusive();
            }

            // Omit entries with very large keys. Primer encoding format needs to change
            // for supporting larger keys.
            if (midKey.length < 0xffff) {
                dout.writeShort(midKey.length);
                dout.write(midKey);
            }
        });

        // Terminator.
        dout.writeShort(0xffff);
    }

    final void applyCachePrimer(DataInput din) throws IOException {
        new Primer(din).run();
    }

    static final void skipCachePrimer(DataInput din) throws IOException {
        while (true) {
            int len = din.readUnsignedShort();
            if (len == 0xffff) {
                break;
            }
            while (len > 0) {
                int amt = din.skipBytes(len);
                if (amt <= 0) {
                    break;
                }
                len -= amt;
            }
        }
    }

    final boolean allowStoredCounts() {
        // TODO: make configurable
        return true;
    }

    /**
     * Non-transactionally insert an entry as the highest overall. Intended for filling up a
     * new tree with ordered entries.
     *
     * @param key new highest key; no existing key can be greater than or equal to it
     * @param frame frame bound to the tree leaf node
     */
    final void append(byte[] key, byte[] value, CursorFrame frame) throws IOException {
        try {
            final CommitLock commitLock = mDatabase.commitLock();
            commitLock.acquireShared();
            Node node = latchDirty(frame);
            try {
                // TODO: inline and specialize
                node.insertLeafEntry(frame, this, frame.mNodePos, key, value);
                frame.mNodePos += 2;

                while (node.mSplit != null) {
                    if (node == mRoot) {
                        node.finishSplitRoot();
                        break;
                    }
                    Node childNode = node;
                    frame = frame.mParentFrame;
                    node = frame.mNode;
                    // Latch coupling upwards is fine because nothing should be searching a
                    // tree which is filling up.
                    node.acquireExclusive();
                    // TODO: inline and specialize
                    node.insertSplitChildRef(frame, this, frame.mNodePos, childNode);
                }
            } finally {
                node.releaseExclusive();
                commitLock.releaseShared();
            }
        } catch (Throwable e) {
            throw closeOnFailure(mDatabase, e);
        }
    }

    /**
     * Returns the frame node latched exclusively and marked dirty.
     */
    private Node latchDirty(CursorFrame frame) throws IOException {
        final LocalDatabase db = mDatabase;
        Node node = frame.mNode;
        node.acquireExclusive();

        if (db.shouldMarkDirty(node)) {
            CursorFrame parentFrame = frame.mParentFrame;
            try {
                if (parentFrame == null) {
                    db.doMarkDirty(this, node);
                } else {
                    // Latch coupling upwards is fine because nothing should be searching a tree
                    // which is filling up.
                    Node parentNode = latchDirty(parentFrame);
                    try {
                        if (db.markDirty(this, node)) {
                            parentNode.updateChildRefId(parentFrame.mNodePos, node.mId);
                        }
                    } finally {
                        parentNode.releaseExclusive();
                    }
                }
            } catch (Throwable e) {
                node.releaseExclusive();
                throw e;
            }
        }

        return node;
    }

    /**
     * Caller must hold exclusive latch and it must verify that node has
     * split. Node latch is released if an exception is thrown.
     *
     * @param frame bound cursor frame
     * @param node node which is bound to the frame, latched exclusively
     * @return replacement node, still latched
     */
    final Node finishSplit(final CursorFrame frame, Node node) throws IOException {
        while (true) {
            if (node == mRoot) {
                try {
                    node.finishSplitRoot();
                } finally {
                    node.releaseExclusive();
                }

                // Must return the node as referenced by the frame, which is no longer the root.
                return frame.acquireExclusive();
            }

            final CursorFrame parentFrame = frame.mParentFrame;
            node.releaseExclusive();

            Node parentNode = parentFrame.acquireExclusive();
            while (true) {
                if (parentNode.mSplit != null) {
                    parentNode = finishSplit(parentFrame, parentNode);
                }
                node = frame.acquireExclusive();
                if (node.mSplit == null) {
                    parentNode.releaseExclusive();
                    return node;
                }
                if (node == mRoot) {
                    // Node became the root in between the time the latch was released and
                    // re-acquired. Go back to the case for handling root splits.
                    parentNode.releaseExclusive();
                    break;
                }
                parentNode.insertSplitChildRef(parentFrame, this, parentFrame.mNodePos, node);
            }
        }
    }

    final LocalTransaction check(Transaction txn) throws IllegalArgumentException {
        if (txn instanceof LocalTransaction) {
            LocalTransaction local = (LocalTransaction) txn;
            LocalDatabase txnDb = local.mDatabase;
            if (txnDb == mDatabase || txnDb == null) {
                return local;
            }
        }
        if (txn != null) {
            throw new IllegalArgumentException("Transaction belongs to a different database");
        }
        return null;
    }

    /**
     * Returns true if a shared lock can be granted for the given key. Caller must hold the
     * node latch which contains the key.
     *
     * @param locker optional locker
     */
    final boolean isLockAvailable(Locker locker, byte[] key, int hash) {
        return mLockManager.isAvailable(locker, mId, key, hash);
    }

    /**
     * @param txn optional transaction instance
     * @param key non-null key instance
     * @return non-null Locker instance if caller should unlock when read is done
     */
    private Locker lockForLoad(LocalTransaction txn, byte[] key) throws LockFailureException {
        if (txn == null) {
            return mLockManager.lockSharedLocal(mId, key, LockManager.hash(mId, key));
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

    final Locker lockSharedLocal(byte[] key, int hash) throws LockFailureException {
        return mLockManager.lockSharedLocal(mId, key, hash);
    }

    final Locker lockExclusiveLocal(byte[] key, int hash) throws LockFailureException {
        return mLockManager.lockExclusiveLocal(mId, key, hash);
    }

    /**
     * @return non-zero position if caller should call txnCommitSync
     */
    final long redoStore(byte[] key, byte[] value) throws IOException {
        RedoWriter redo = mDatabase.mRedoWriter;
        return redo == null ? 0 : redo.store(mId, key, value, mDatabase.mDurabilityMode);
    }

    /**
     * @return non-zero position if caller should call txnCommitSync
     */
    final long redoStoreNoLock(byte[] key, byte[] value) throws IOException {
        RedoWriter redo = mDatabase.mRedoWriter;
        return redo == null ? 0 : redo.storeNoLock(mId, key, value, mDatabase.mDurabilityMode);
    }

    final void txnCommitSync(LocalTransaction txn, long commitPos) throws IOException {
        mDatabase.mRedoWriter.txnCommitSync(txn, commitPos);
    }

    /**
     * @see Database#markDirty
     */
    final boolean markDirty(Node node) throws IOException {
        return mDatabase.markDirty(this, node);
    }

    final byte[] fragmentKey(byte[] key) throws IOException {
        return mDatabase.fragment(key, key.length, mMaxKeySize);
    }

    private class Primer {
        private final DataInput mDin;
        private final int mTaskLimit;

        private int mTaskCount;
        private boolean mFinished;
        private IOException mEx;

        Primer(DataInput din) {
            mDin = din;
            // TODO: Limit should be based on the concurrency level of the I/O system.
            // TODO: Cache primer order should be scrambled, to improve cuncurrent priming.
            mTaskLimit = Runtime.getRuntime().availableProcessors() * 8;
        }

        void run() throws IOException {
            synchronized (this) {
                mTaskCount++;
            }

            prime();

            // Wait for other task threads to finish.
            synchronized (this) {
                while (true) {
                    if (mEx != null) {
                        throw mEx;
                    }
                    if (mTaskCount <= 0) {
                        break;
                    }
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        throw new InterruptedIOException();
                    }
                }
            }
        }

        void prime() {
            try {
                Cursor c = newCursor(Transaction.BOGUS);

                try {
                    c.autoload(false);

                    while (true) {
                        byte[] key;

                        synchronized (this) {
                            if (mFinished) {
                                return;
                            }

                            int len = mDin.readUnsignedShort();

                            if (len == 0xffff) {
                                mFinished = true;
                                return;
                            }

                            key = new byte[len];
                            mDin.readFully(key);

                            if (mTaskCount < mTaskLimit) spawn: {
                                Task task;
                                try {
                                    task = new Task();
                                } catch (Throwable e) {
                                    break spawn;
                                }
                                mTaskCount++;
                                task.start();
                            }
                        }

                        c.findNearby(key);
                    }
                } catch (IOException e) {
                    synchronized (this) {
                        if (mEx == null) {
                            mEx = e;
                        }
                    }
                } finally {
                    c.reset();
                }
            } finally {
                synchronized (this) {
                    mTaskCount--;
                    notifyAll();
                }
            }
        }

        class Task extends Thread {
            @Override
            public void run() {
                prime();
            }
        }
    }
}
