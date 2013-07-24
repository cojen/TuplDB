/*
 *  Copyright 2011-2013 Brian S O'Neill
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

import static org.cojen.tupl.Utils.*;

/**
 * B-tree implementation.
 *
 * @author Brian S O'Neill
 */
final class Tree implements Index {
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

    final Database mDatabase;
    final LockManager mLockManager;

    // Id range is [0, 255] for all internal trees.
    final long mId;

    // Id is null for registry.
    final byte[] mIdBytes;

    // Name is null for all internal trees.
    final byte[] mName;

    // Although tree roots can be created and deleted, the object which refers
    // to the root remains the same. Internal state is transferred to/from this
    // object when the tree root changes.
    final Node mRoot;

    final int mMaxEntrySize;

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
        // Limit maximum non-fragmented entry size to 0.75 of usable node size.
        mMaxEntrySize = ((db.pageSize() - Node.TN_HEADER_SIZE) * 3) >> 2;
    }

    @Override
    public String toString() {
        return toString(this);
    }

    static String toString(Index ix) {
        StringBuilder b = new StringBuilder(ix.getClass().getName());
        b.append('@').append(Integer.toHexString(ix.hashCode()));
        b.append(" {");
        b.append("name").append(": ").append(ix.getNameString());
        b.append(", ");
        b.append("id").append(": ").append(ix.getId());
        return b.append('}').toString();
    }

    @Override
    public long getId() {
        return mId;
    }

    @Override
    public byte[] getName() {
        byte[] name = mName;
        return name == null ? null : name.clone();
    }

    @Override
    public String getNameString() {
        byte[] name = mName;
        try {
            return name == null ? "null" : new String(name, "UTF-8");
        } catch (IOException e) {
            return new String(name);
        }
    }

    @Override
    public Cursor newCursor(Transaction txn) {
        return new TreeCursor(this, txn);
    }

    /*
    @Override
    public long count(Transaction txn) throws IOException {
        // TODO
        throw null;
    }
    */

    /*
    @Override
    public long count(Transaction txn,
                      byte[] start, boolean startInclusive,
                      byte[] end, boolean endInclusive)
        throws IOException
    {
        // TODO
        throw null;
    }
    */

    /*
    @Override
    public boolean exists(Transaction txn, byte[] key) throws IOException {
        // TODO
        throw null;
    }
    */

    /*
    @Override
    public boolean exists(Transaction txn, byte[] key, byte[] value) throws IOException {
        // TODO
        throw null;
    }
    */

    @Override
    public byte[] load(Transaction txn, byte[] key) throws IOException {
        check(txn);
        Locker locker = lockForLoad(txn, key);
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
        if (key == null) {
            throw new NullPointerException("Key is null");
        }
        TreeCursor cursor = new TreeCursor(this, txn);
        try {
            cursor.autoload(false);
            cursor.findAndStore(key, value);
        } catch (Throwable e) {
            throw closeOnFailure(cursor, e);
        }
    }

    @Override
    public byte[] exchange(Transaction txn, byte[] key, byte[] value) throws IOException {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }
        TreeCursor cursor = new TreeCursor(this, txn);
        try {
            return cursor.findAndStore(key, value);
        } catch (Throwable e) {
            throw closeOnFailure(cursor, e);
        }
    }

    @Override
    public boolean insert(Transaction txn, byte[] key, byte[] value) throws IOException {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }
        TreeCursor cursor = new TreeCursor(this, txn);
        try {
            cursor.autoload(false);
            return cursor.findAndModify(key, TreeCursor.MODIFY_INSERT, value);
        } catch (Throwable e) {
            throw closeOnFailure(cursor, e);
        }
    }

    @Override
    public boolean replace(Transaction txn, byte[] key, byte[] value) throws IOException {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }
        TreeCursor cursor = new TreeCursor(this, txn);
        try {
            cursor.autoload(false);
            return cursor.findAndModify(key, TreeCursor.MODIFY_REPLACE, value);
        } catch (Throwable e) {
            throw closeOnFailure(cursor, e);
        }
    }

    @Override
    public boolean update(Transaction txn, byte[] key, byte[] oldValue, byte[] newValue)
        throws IOException
    {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }
        TreeCursor cursor = new TreeCursor(this, txn);
        try {
            return cursor.findAndModify(key, oldValue, newValue);
        } catch (Throwable e) {
            throw closeOnFailure(cursor, e);
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

    /*
    @Override
    public void clear(Transaction txn) throws IOException {
        if (txn == null) {
            TreeCursor cursor = new TreeCursor(this, null);
            try {
                cursor.autoload(false);
                cursor.first();
                cursor.clearTo(null, false);
            } finally {
                cursor.reset();
            }
            return;
        }

        if (txn.lockMode() == LockMode.UNSAFE) {
            // TODO: Optimize for LockMode.UNSAFE.
            throw null;
        }

        txn.enter();
        try {
            txn.lockMode(LockMode.UPGRADABLE_READ);
            TreeCursor cursor = new TreeCursor(this, txn);
            try {
                cursor.autoload(false);
                cursor.first();
                while (cursor.key() != null) {
                    cursor.store(null);
                    cursor.next();
                }
            } finally {
                // TODO: this can deadlock, because exception can be thrown at anytime
                cursor.reset();
            }
            txn.commit();
        } finally {
            txn.exit();
        }
    }

    @Override
    public void clear(Transaction txn,
                      byte[] start, boolean startInclusive,
                      byte[] end, boolean endInclusive)
        throws IOException
    {
        if (txn == null) {
            TreeCursor cursor = new TreeCursor(this, null);
            try {
                cursor.autoload(false);
                if (start == null) {
                    cursor.first();
                } else if (startInclusive) {
                    cursor.findGe(start);
                } else {
                    cursor.findGt(start);
                }
                cursor.clearTo(end, endInclusive);
            } finally {
                cursor.reset();
            }
            return;
        }

        if (txn.lockMode() == LockMode.UNSAFE) {
            // TODO: Optimize for LockMode.UNSAFE.
            throw null;
        }

        txn.enter();
        try {
            txn.lockMode(LockMode.UPGRADABLE_READ);
            TreeCursor cursor = new TreeCursor(this, txn);
            try {
                cursor.autoload(false);

                if (start == null) {
                    cursor.first();
                } else if (startInclusive) {
                    cursor.findGe(start);
                } else {
                    cursor.findGt(start);
                }

                if (end == null) {
                    while (cursor.key() != null) {
                        cursor.store(null);
                        cursor.next();
                    }
                } else if (endInclusive) {
                    byte[] key;
                    while ((key = cursor.key()) != null) {
                        int compare = compareKeys(key, 0, key.length, end, 0, end.length);
                        if (compare > 0) {
                            break;
                        }
                        cursor.store(null);
                        if (compare >= 0) {
                            break;
                        }
                        cursor.next();
                    }
                } else {
                    byte[] key;
                    while ((key = cursor.key()) != null) {
                        if (compareKeys(key, 0, key.length, end, 0, end.length) >= 0) {
                            break;
                        }
                        cursor.store(null);
                        cursor.next();
                    }
                }
            } finally {
                // TODO: this can deadlock, because exception can be thrown at anytime
                cursor.reset();
            }
            txn.commit();
        } finally {
            txn.exit();
        }
    }
    */

    @Override
    public View viewGe(byte[] key) {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }
        return new BoundedView(this, key, null, 0);
    }

    @Override
    public View viewGt(byte[] key) {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }
        return new BoundedView(this, key, null, BoundedView.START_EXCLUSIVE);
    }

    @Override
    public View viewLe(byte[] key) {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }
        return new BoundedView(this, null, key, 0);
    }

    @Override
    public View viewLt(byte[] key) {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }
        return new BoundedView(this, null, key, BoundedView.END_EXCLUSIVE);
    }

    @Override
    public View viewPrefix(byte[] prefix, int trim) {
        SubView.prefixCheck(prefix, trim);

        byte[] end = prefix.clone();
        int mode;
        if (increment(end, 0, end.length)) {
            mode = BoundedView.END_EXCLUSIVE;
        } else {
            // Prefix is highest possible, so no need for an end bound.
            end = null;
            mode = 0;
        }

        View view = new BoundedView(this, prefix, end, mode);

        if (trim > 0) {
            view = new TrimmedView(view, prefix, trim);
        }

        return view;
    }

    @Override
    public View viewReverse() {
        return new ReverseView(this);
    }

    @Override
    public View viewUnmodifiable() {
        return UnmodifiableView.apply(this);
    }

    @Override
    public boolean isUnmodifiable() {
        return isClosed();
    }

    @Override
    public boolean verify(VerificationObserver observer) throws IOException {
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
     * Returns a view which can be passed to an observer. Internal trees are returned as
     * unmodifiable.
     */
    Index observableView() {
        return isInternal(mId) ? new UnmodifiableView(this) : this;
    }

    /**
     * @param view view to pass to observer
     * @return false if should stop
     */
    boolean verifyTree(Index view, VerificationObserver observer) throws IOException {
        TreeCursor cursor = new TreeCursor(this, Transaction.BOGUS);
        try {
            cursor.first();
            int height = cursor.height();
            if (!observer.indexBegin(view, height)) {
                return false;
            }
            if (!cursor.verify(height, observer)) {
                return false;
            }
        } catch (Throwable e) {
            observer.failed = true;
            throw rethrow(e);
        } finally {
            cursor.reset();
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        Node root = mRoot;
        root.acquireExclusive();
        try {
            if (root.mPage == EMPTY_BYTES) {
                // Already closed.
                return;
            }

            if (root.mLastCursorFrame != null) {
                throw new IllegalStateException("Cannot close an index which has active cursors");
            }

            if (isInternal(mId)) {
                throw new IllegalStateException("Cannot close an internal index");
            }

            if (mDatabase.mPageDb.isDurable()) {
                root.forceEvictTree(mDatabase.mPageDb);

                // Root node reference cannot be cleared, so instead make it
                // non-functional. Move the page reference into a new evictable Node object,
                // allowing it to be recycled.

                mDatabase.makeEvictable(root.closeRoot(false));
                mDatabase.treeClosed(this);
            } else {
                // Non-durable tree cannot be truly closed because nothing would reference it
                // anymore. As per the interface contract, make this reference unmodifiable,
                // but also register a replacement tree instance. Closing a non-durable tree
                // has little practical value.

                mDatabase.replaceClosedTree(this, root.closeRoot(true));
            }
        } finally {
            root.releaseExclusive();
        }
    }

    @Override 
    public boolean isClosed() {
        Node root = mRoot;
        root.acquireShared();
        boolean closed = root.mPage == EMPTY_BYTES;
        root.releaseShared();
        return closed;
    }

    @Override
    public void drop() throws IOException {
        Node root = mRoot;
        root.acquireExclusive();
        try {
            if (root.mPage == EMPTY_BYTES) {
                throw new ClosedIndexException();
            }

            if (!root.isLeaf() || root.hasKeys()) {
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

            // Root node reference cannot be cleared, so instead make it non-functional. Move
            // the page reference into a new evictable Node object, allowing it to be recycled.

            long rootId = root.mId;
            int cachedState = root.mCachedState;

            mDatabase.makeEvictable(root.closeRoot(false));
            mDatabase.dropClosedTree(this, rootId, cachedState);
        } finally {
            root.releaseExclusive();
        }
    }

    void check(Transaction txn) throws IllegalArgumentException {
        if (txn != null) {
            Database txnDb = txn.mDatabase;
            if (txnDb != null & txnDb != mDatabase) {
                throw new IllegalArgumentException("Transaction belongs to a different database");
            }
        }
    }

    /**
     * Returns true if a shared lock can be immediately granted. Caller must
     * hold a coarse latch to prevent this state from changing.
     *
     * @param locker optional locker
     */
    boolean isLockAvailable(Locker locker, byte[] key, int hash) {
        return mLockManager.isAvailable(locker, mId, key, hash);
    }

    /**
     * @param txn optional transaction instance
     * @param key non-null key instance
     * @return non-null Locker instance if caller should unlock when read is done
     */
    private Locker lockForLoad(Transaction txn, byte[] key) throws LockFailureException {
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

    /**
     * @param txn optional transaction instance
     * @param key non-null key instance
     * @return non-null Locker instance if caller should unlock when write is done
     */
    Locker lockExclusive(Transaction txn, byte[] key, int hash) throws LockFailureException {
        if (txn == null) {
            return lockExclusiveLocal(key, hash);
        }

        if (txn.lockMode() != LockMode.UNSAFE) {
            txn.lockExclusive(mId, key, hash);
        }

        return null;
    }

    Locker lockSharedLocal(byte[] key, int hash) throws LockFailureException {
        return mLockManager.lockSharedLocal(mId, key, hash);
    }

    Locker lockExclusiveLocal(byte[] key, int hash) throws LockFailureException {
        return mLockManager.lockExclusiveLocal(mId, key, hash);
    }

    void redoStore(byte[] key, byte[] value) throws IOException {
        RedoWriter redo = mDatabase.mRedoWriter;
        if (redo != null) {
            redo.store(mId, key, value, mDatabase.mDurabilityMode);
        }
    }

    void redoStoreNoLock(byte[] key, byte[] value) throws IOException {
        RedoWriter redo = mDatabase.mRedoWriter;
        if (redo != null) {
            redo.storeNoLock(mId, key, value, mDatabase.mDurabilityMode);
        }
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
        Stub stub = mStubTail;
        while (stub != null) {
            if (stub.mNode.mId == Node.STUB_ID) {
                return true;
            }
            // Node was evicted, so pop it off and try next one.
            mStubTail = stub = stub.mParent;
        }
        return false;
    }

    /**
     * Attempts to exclusively latch and pop the tail stub node. Returns null
     * if latch cannot be immediatly obtained. Caller must exclusively hold
     * root latch and have checked that a stub exists.
     */
    Node tryPopStub() {
        Stub stub = mStubTail;
        if (stub.mNode.tryAcquireExclusive()) {
            mStubTail = stub.mParent;
            return stub.mNode;
        }
        return null;
    }

    /**
     * Exclusively latches and pops the tail stub node. Caller must exclusively
     * hold root latch and have checked that a stub exists.
     */
    /*
    Node popStub() {
        Stub stub = mStubTail;
        stub.mNode.acquireExclusive();
        mStubTail = stub.mParent;
        return stub.mNode;
    }
    */

    /**
     * Checks if popped stub is still valid, because it has not been evicted
     * and it actually has cursors bound to it. Caller must hold exclusive
     * latch, which is released if node is not valid.
     *
     * @return node if valid, null otherwise
     */
    static Node validateStub(Node node) {
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
}
