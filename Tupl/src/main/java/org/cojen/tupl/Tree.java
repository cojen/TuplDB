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

import java.security.SecureRandom;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class Tree implements Index {
    // Reserved internal tree ids.
    static final int
        REGISTRY_ID = 0,
        REGISTRY_KEY_MAP_ID = 1,
        PAGE_ALLOCATOR = 2,
        MAX_RESERVED_ID = 0xff;

    static boolean isInternal(long id) {
        return (id & ~0xff) == 0;
    }

    /**
     * Returns a random id outside the internal id range.
     */
    static long randomId() {
        SecureRandom rnd = new SecureRandom();
        long id;
        while (isInternal(id = rnd.nextLong()));
        return id;
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
        byte[] name = mName;
        return name == null ? null : name.clone();
    }

    @Override
    public Cursor newCursor(Transaction txn) {
        return new TreeCursor(this, txn);
    }

    /*
    @Override
    public long count(Transaction txn) throws IOException {
        // FIXME
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
        // FIXME
        throw null;
    }
    */

    /*
    @Override
    public boolean exists(Transaction txn, byte[] key) throws IOException {
        // FIXME
        throw null;
    }
    */

    /*
    @Override
    public boolean exists(Transaction txn, byte[] key, byte[] value) throws IOException {
        // FIXME
        throw null;
    }
    */

    @Override
    public byte[] load(Transaction txn, byte[] key) throws IOException {
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
        TreeCursor cursor = new TreeCursor(this, txn);
        try {
            cursor.autoload(false);
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
            cursor.autoload(false);
            return cursor.findAndModify(key, TreeCursor.MODIFY_INSERT, value);
        } catch (Throwable e) {
            e.printStackTrace(System.out);
            throw Utils.rethrow(e);
        } finally {
            // FIXME: this can deadlock, because exception can be thrown at anytime
            cursor.reset();
        }
    }

    @Override
    public boolean replace(Transaction txn, byte[] key, byte[] value) throws IOException {
        TreeCursor cursor = new TreeCursor(this, txn);
        try {
            cursor.autoload(false);
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
            // FIXME: Optimize for LockMode.UNSAFE.
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
                // FIXME: this can deadlock, because exception can be thrown at anytime
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
            // FIXME: Optimize for LockMode.UNSAFE.
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
                        int compare = Utils.compareKeys(key, 0, key.length, end, 0, end.length);
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
                        if (Utils.compareKeys(key, 0, key.length, end, 0, end.length) >= 0) {
                            break;
                        }
                        cursor.store(null);
                        cursor.next();
                    }
                }
            } finally {
                // FIXME: this can deadlock, because exception can be thrown at anytime
                cursor.reset();
            }
            txn.commit();
        } finally {
            txn.exit();
        }
    }
    */

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
    Node popStub() {
        Stub stub = mStubTail;
        stub.mNode.acquireExclusive();
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
}
