/*
 *  Copyright (C) 2011-2017 Cojen.org
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.InterruptedIOException;
import java.io.IOException;

import java.util.Comparator;

import java.nio.charset.StandardCharsets;

import java.util.concurrent.ThreadLocalRandom;

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

    // Although tree roots can be created and deleted, the object which refers
    // to the root remains the same. Internal state is transferred to/from this
    // object when the tree root changes.
    final Node mRoot;

    // Name is null for all internal trees.
    volatile byte[] mName;

    // Linked list of stubs, which are created when the root node is deleted. They need to
    // stick around indefinitely, to ensure that any bound cursors still function normally.
    // When tree height increases again, the stub is replaced with a real node. Root node must
    // be latched exclusively when modifying this list.
    private Node mStubTail;

    Tree(LocalDatabase db, long id, byte[] idBytes, Node root) {
        mDatabase = db;
        mLockManager = db.mLockManager;
        mId = id;
        mIdBytes = idBytes;
        mRoot = root;
    }

    final int pageSize() {
        return mDatabase.pageSize();
    }

    @Override
    public final String toString() {
        return ViewUtils.toString(this);
    }

    @Override
    public final Ordering getOrdering() {
        return Ordering.ASCENDING;
    }

    @Override
    public Comparator<byte[]> getComparator() {
        return KeyComparator.THE;
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
        return new String(name, StandardCharsets.UTF_8);
    }

    @Override
    public TreeCursor newCursor(Transaction txn) {
        return new TreeCursor(this, txn);
    }

    @Override
    public Transaction newTransaction(DurabilityMode durabilityMode) {
        return mDatabase.newTransaction(durabilityMode);
    }

    @Override
    public long count(byte[] lowKey, byte[] highKey) throws IOException {
        TreeCursor cursor = newCursor(Transaction.BOGUS);
        TreeCursor high = null;
        try {
            if (highKey != null) {
                high = newCursor(Transaction.BOGUS);
                high.autoload(false);
                high.find(highKey);
                if (high.mKey == null) {
                    // Found nothing.
                    return 0;
                }
            }
            return cursor.count(lowKey, high);
        } finally {
            cursor.reset();
            if (high != null) {
                high.reset();
            }
        }
    }

    @Override
    public final byte[] load(Transaction txn, byte[] key) throws IOException {
        LocalTransaction local = check(txn);

        // If lock must be acquired and retained, acquire now and skip the quick check later.
        if (local != null) {
            int lockType = local.lockMode().repeatable;
            if (lockType != 0) {
                int hash = LockManager.hash(mId, key);
                local.lock(lockType, mId, key, hash, local.mLockTimeoutNanos);
            }
        }

        Node node = mRoot;
        node.acquireShared();

        // Note: No need to check if root has split, since root splits are always completed
        // before releasing the root latch. Also, Node.used is not invoked for the root node,
        // because it cannot be evicted.

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        while (!node.isLeaf()) {
            int childPos;
            try {
                childPos = Node.internalPos(node.binarySearch(key));
            } catch (Throwable e) {
                node.releaseShared();
                throw e;
            }

            long childId = node.retrieveChildRefId(childPos);
            Node childNode = mDatabase.nodeMapGetShared(childId);

            if (childNode != null) {
                node.releaseShared();
                node = childNode;
                node.used(rnd);
            } else {
                node = node.loadChild(mDatabase, childId, Node.OPTION_PARENT_RELEASE_SHARED);
            }

            if (node.mSplit != null) {
                node = node.mSplit.selectNode(node, key);
            }
        }

        // Sub search into leaf with shared latch held.

        // Same code as binarySearch, but instead of returning the position, it directly copies
        // the value if found. This avoids having to decode the found value location twice.

        CursorFrame frame;
        int keyHash;

        search: try {
            final /*P*/ byte[] page = node.mPage;
            final int keyLen = key.length;
            int lowPos = node.searchVecStart();
            int highPos = node.searchVecEnd();

            int lowMatch = 0;
            int highMatch = 0;

            outer: while (lowPos <= highPos) {
                int midPos = ((lowPos + highPos) >> 1) & ~1;

                int compareLoc, compareLen, i;
                compare: {
                    compareLoc = p_ushortGetLE(page, midPos);
                    compareLen = p_byteGet(page, compareLoc++);
                    if (compareLen >= 0) {
                        compareLen++;
                    } else {
                        int header = compareLen;
                        compareLen = ((compareLen & 0x3f) << 8) | p_ubyteGet(page, compareLoc++);

                        if ((header & Node.ENTRY_FRAGMENTED) != 0) {
                            // Note: An optimized version wouldn't need to copy the whole key.
                            byte[] compareKey = mDatabase.reconstructKey
                                (page, compareLoc, compareLen);

                            int fullCompareLen = compareKey.length;

                            int minLen = Math.min(fullCompareLen, keyLen);
                            i = Math.min(lowMatch, highMatch);
                            for (; i<minLen; i++) {
                                byte cb = compareKey[i];
                                byte kb = key[i];
                                if (cb != kb) {
                                    if ((cb & 0xff) < (kb & 0xff)) {
                                        lowPos = midPos + 2;
                                        lowMatch = i;
                                    } else {
                                        highPos = midPos - 2;
                                        highMatch = i;
                                    }
                                    continue outer;
                                }
                            }

                            // Update compareLen and compareLoc for use by the code after the
                            // current scope. The compareLoc is completely bogus at this point,
                            // but is corrected when the value is retrieved below.
                            compareLoc += compareLen - fullCompareLen;
                            compareLen = fullCompareLen;

                            break compare;
                        }
                    }

                    int minLen = Math.min(compareLen, keyLen);
                    i = Math.min(lowMatch, highMatch);
                    for (; i<minLen; i++) {
                        byte cb = p_byteGet(page, compareLoc + i);
                        byte kb = key[i];
                        if (cb != kb) {
                            if ((cb & 0xff) < (kb & 0xff)) {
                                lowPos = midPos + 2;
                                lowMatch = i;
                            } else {
                                highPos = midPos - 2;
                                highMatch = i;
                            }
                            continue outer;
                        }
                    }
                }

                if (compareLen < keyLen) {
                    lowPos = midPos + 2;
                    lowMatch = i;
                } else if (compareLen > keyLen) {
                    highPos = midPos - 2;
                    highMatch = i;
                } else {
                    if ((local != null && local.lockMode() != LockMode.READ_COMMITTED) ||
                        mLockManager.isAvailable
                        (local, mId, key, keyHash = LockManager.hash(mId, key)))
                    {
                        return Node.retrieveLeafValueAtLoc(node, page, compareLoc + compareLen);
                    }
                    // Need to acquire the lock before loading. To prevent deadlock, a cursor
                    // frame must be bound and then the node latch can be released.
                    frame = new CursorFrame();
                    int pos = midPos - node.searchVecStart();
                    if (node.mSplit != null) {
                        pos = node.mSplit.adjustBindPosition(pos);
                    }
                    frame.bind(node, pos);
                    break search;
                }
            }

            if ((local != null && local.lockMode() != LockMode.READ_COMMITTED) ||
                mLockManager.isAvailable(local, mId, key, keyHash = LockManager.hash(mId, key)))
            {
                return null;
            }

            // Need to lock even if no value was found.
            frame = new CursorFrame();
            frame.mNotFoundKey = key;
            int pos = lowPos - node.searchVecStart();
            if (node.mSplit != null) {
                pos = node.mSplit.adjustBindPosition(pos);
            }
            frame.bind(node, ~pos);
            break search;
        } finally {
            node.releaseShared();
        }

        try {
            Locker locker;
            if (local == null) {
                locker = lockSharedLocal(key, keyHash);
            } else if (local.lockShared(mId, key, keyHash) == LockResult.ACQUIRED) {
                locker = local;
            } else {
                // Transaction already had the lock for some reason, so don't release it.
                locker = null;
            }

            try {
                node = frame.acquireShared();
                try {
                    int pos = frame.mNodePos;
                    if (pos < 0) {
                        return null;
                    } else if (node.mSplit == null) {
                        return node.retrieveLeafValue(pos);
                    } else {
                        return node.mSplit.retrieveLeafValue(node, pos);
                    }
                } finally {
                    node.releaseShared();
                }
            } finally {
                if (locker != null) {
                    locker.unlock();
                }
            }
        } finally {
            CursorFrame.popAll(frame);
        }
    }

    @Override
    public final boolean exists(Transaction txn, byte[] key) throws IOException {
        LocalTransaction local = check(txn);

        // If lock must be acquired and retained, acquire now and skip the quick check later.
        if (local != null) {
            int lockType = local.lockMode().repeatable;
            if (lockType != 0) {
                int hash = LockManager.hash(mId, key);
                local.lock(lockType, mId, key, hash, local.mLockTimeoutNanos);
            }
        }

        Node node = mRoot;
        node.acquireShared();

        // Note: No need to check if root has split, since root splits are always completed
        // before releasing the root latch. Also, Node.used is not invoked for the root node,
        // because it cannot be evicted.

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        while (!node.isLeaf()) {
            int childPos;
            try {
                childPos = Node.internalPos(node.binarySearch(key));
            } catch (Throwable e) {
                node.releaseShared();
                throw e;
            }

            long childId = node.retrieveChildRefId(childPos);
            Node childNode = mDatabase.nodeMapGetShared(childId);

            if (childNode != null) {
                node.releaseShared();
                node = childNode;
                node.used(rnd);
            } else {
                node = node.loadChild(mDatabase, childId, Node.OPTION_PARENT_RELEASE_SHARED);
            }

            if (node.mSplit != null) {
                node = node.mSplit.selectNode(node, key);
            }
        }

        // Sub search into leaf with shared latch held.

        CursorFrame frame;
        int keyHash;

        try {
            int pos = node.binarySearch(key);

            if ((local != null && local.lockMode() != LockMode.READ_COMMITTED) ||
                mLockManager.isAvailable(local, mId, key, keyHash = LockManager.hash(mId, key)))
            {
                return pos >= 0 && node.hasLeafValue(pos) != null;
            }

            // Need to acquire the lock before loading. To prevent deadlock, a cursor
            // frame must be bound and then the node latch can be released.
            frame = new CursorFrame();

            if (pos >= 0) {
                if (node.mSplit != null) {
                    pos = node.mSplit.adjustBindPosition(pos);
                }
            } else {
                frame.mNotFoundKey = key;
                if (node.mSplit != null) {
                    pos = ~node.mSplit.adjustBindPosition(~pos);
                }
            }

            frame.bind(node, pos);
        } finally {
            node.releaseShared();
        }

        try {
            Locker locker;
            if (local == null) {
                locker = lockSharedLocal(key, keyHash);
            } else if (local.lockShared(mId, key, keyHash) == LockResult.ACQUIRED) {
                locker = local;
            } else {
                // Transaction already had the lock for some reason, so don't release it.
                locker = null;
            }

            try {
                node = frame.acquireShared();
                int pos = frame.mNodePos;
                boolean result = pos >= 0 && node.hasLeafValue(pos) != null;
                node.releaseShared();
                return result;
            } finally {
                if (locker != null) {
                    locker.unlock();
                }
            }
        } finally {
            CursorFrame.popAll(frame);
        }
    }

    @Override
    public final void store(Transaction txn, byte[] key, byte[] value) throws IOException {
        keyCheck(key);
        TreeCursor cursor = newCursor(txn);
        try {
            cursor.autoload(false);
            cursor.findAndStore(key, value);
        } finally {
            cursor.reset();
        }
    }

    @Override
    public final byte[] exchange(Transaction txn, byte[] key, byte[] value) throws IOException {
        keyCheck(key);
        TreeCursor cursor = newCursor(txn);
        try {
            return cursor.findAndStore(key, value);
        } finally {
            cursor.reset();
        }
    }

    @Override
    public final boolean insert(Transaction txn, byte[] key, byte[] value) throws IOException {
        keyCheck(key);
        TreeCursor cursor = newCursor(txn);
        try {
            cursor.autoload(false);
            return cursor.findAndModify(key, TreeCursor.MODIFY_INSERT, value);
        } finally {
            cursor.reset();
        }
    }

    @Override
    public final boolean replace(Transaction txn, byte[] key, byte[] value) throws IOException {
        keyCheck(key);
        TreeCursor cursor = newCursor(txn);
        try {
            cursor.autoload(false);
            return cursor.findAndModify(key, TreeCursor.MODIFY_REPLACE, value);
        } finally {
            cursor.reset();
        }
    }

    @Override
    public final boolean update(Transaction txn, byte[] key, byte[] value) throws IOException {
        keyCheck(key);
        TreeCursor cursor = newCursor(txn);
        try {
            // TODO: Optimize by disabling autoload and do an in-place comparison.
            return cursor.findAndModify(key, TreeCursor.MODIFY_UPDATE, value);
        } finally {
            cursor.reset();
        }
    }

    @Override
    public final boolean update(Transaction txn, byte[] key, byte[] oldValue, byte[] newValue)
        throws IOException
    {
        keyCheck(key);
        TreeCursor cursor = newCursor(txn);
        try {
            return cursor.findAndModify(key, oldValue, newValue);
        } finally {
            cursor.reset();
        }
    }

    @Override
    public LockResult touch(Transaction txn, byte[] key) throws LockFailureException {
        LocalTransaction local = check(txn);

        LockMode mode;
        if (local == null || (mode = local.lockMode()) == LockMode.READ_COMMITTED) {
            int hash = LockManager.hash(mId, key);
            if (!isLockAvailable(local, key, hash)) {
                // Acquire and release.
                if (local == null) {
                    lockSharedLocal(key, hash).unlock();
                } else {
                    LockResult result = local.lock(0, mId, key, hash, local.mLockTimeoutNanos);
                    if (result == LockResult.ACQUIRED) {
                        local.unlock();
                    }
                }
            }
        } else if (!mode.noReadLock) {
            int hash = LockManager.hash(mId, key);
            return local.lock(mode.repeatable, mId, key, hash, local.mLockTimeoutNanos);
        }

        return LockResult.UNOWNED;
    }

    @Override
    public final LockResult tryLockShared(Transaction txn, byte[] key, long nanosTimeout)
        throws DeadlockException
    {
        return check(txn).tryLockShared(mId, key, nanosTimeout);
    }

    @Override
    public final LockResult lockShared(Transaction txn, byte[] key) throws LockFailureException {
        return check(txn).lockShared(mId, key);
    }

    @Override
    public final LockResult tryLockUpgradable(Transaction txn, byte[] key, long nanosTimeout)
        throws DeadlockException
    {
        return check(txn).tryLockUpgradable(mId, key, nanosTimeout);
    }

    @Override
    public final LockResult lockUpgradable(Transaction txn, byte[] key)
        throws LockFailureException
    {
        return check(txn).lockUpgradable(mId, key);
    }

    @Override
    public final LockResult tryLockExclusive(Transaction txn, byte[] key, long nanosTimeout)
        throws DeadlockException
    {
        return check(txn).tryLockExclusive(mId, key, nanosTimeout);
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

    /**
     * Current approach for evicting data is as follows:
     * - Search for a random Node, steered towards un-cached nodes. 
     * - Once a node is picked, iterate through the keys in the node 
     *   and delete all the entries from it (provided they are within 
     *   the highkey and lowKey boundaries).
     * - This simple algorithm is an approximate LRU algorithm, which
     *   is expected to evict entries that are least recently accessed.
     * 
     * An alternative approach that was considered:
     * - Search for a random Node, steered towards un-cached nodes.
     * - Delete the node directly. 
     * - This works when all the keys and values fit within a page.  
     *   If they don't, then the entries must be fully decoded. This is
     *   necessary because there's no quick way of determining if any of
     *   the entries in a page overflow.  
     * 
     * Note: It could be that the node initially has three keys: A, B, D. As eviction is
     * progressing along, a key C could be inserted concurrently, which could then be
     * immediately deleted. This case is expected to be rare and harmless.
     */
    @Override
    public long evict(Transaction txn, byte[] lowKey, byte[] highKey,
                      Filter evictionFilter, boolean autoload)
        throws IOException
    {
        long length = 0;
        TreeCursor cursor = newCursor(txn);
        cursor.autoload(autoload);

        try {
            byte[] endKey = cursor.randomNode(lowKey, highKey);
            if (endKey == null) {
                // We did not find anything to evict.  Move on.
                return length;
            }
            
            if (lowKey != null) { 
                if (Utils.compareUnsigned(lowKey, endKey) > 0) {
                    // lowKey is past the end key.  Move on.
                    return length;
                }
                if (cursor.compareKeyTo(lowKey) < 0) {
                    // lowKey is past the current cursor position: move cursor position to lowKey
                    // findNearby will position the cursor to lowKey even if it does not exist.
                    // So we will need to skip values that don't exist before processing the keys.
                    // findNearby returns a lockResult. We can safely ignore it.
                    cursor.findNearby(lowKey);
                }
            }
            
            if (highKey != null && Utils.compareUnsigned(highKey, endKey) <= 0) {
                endKey = highKey; 
            }
            
            long[] stats = new long[2];
            while (cursor.key() != null) {
                byte[] key = cursor.key();
                byte[] value = cursor.value();
                if (value != null) {
                    cursor.valueStats(stats);
                    if (stats[0] > 0 &&
                        (evictionFilter == null || evictionFilter.isAllowed(key, value)))
                    {
                        length += key.length + stats[0]; 
                        cursor.store(null);
                    }
                } else {
                    // This is either a ghost or findNearby got us to a 
                    // key that does not exist.  Move on to next key.
                }
                cursor.nextLe(endKey);
            }
        } finally {
            cursor.reset();
        }
        return length;
    }

    @Override
    public Stats analyze(byte[] lowKey, byte[] highKey) throws IOException {
        TreeCursor cursor = newCursor(Transaction.BOGUS);
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

        TreeCursor cursor = newCursor(Transaction.BOGUS);
        try {
            cursor.autoload(false);

            // Find the first node instead of calling first() to ensure that cursor is
            // positioned. Otherwise, empty trees would be skipped even when the root node
            // needed to be moved out of the compaction zone.
            cursor.firstAny();

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
        TreeCursor cursor = newCursor(Transaction.BOGUS);
        try {
            cursor.autoload(false);
            cursor.first(); // must start with loaded key
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
        close(false, false, false);
    }

    /**
     * @param rootLatched true if root node is already latched by the current thread
     * @return root node if forDelete; null if already closed
     */
    final Node close(boolean forDelete, final boolean rootLatched) throws IOException {
        return close(forDelete, rootLatched, false);
    }

    /**
     * Close any kind of index, even an internal one.
     */
    final void forceClose() throws IOException {
        close(false, false, true);
    }

    /**
     * @param rootLatched true if root node is already latched by the current thread
     * @return root node if forDelete; null if already closed
     */
    private Node close(boolean forDelete, final boolean rootLatched, boolean force)
        throws IOException
    {
        Node root = mRoot;

        if (!rootLatched) {
            root.acquireExclusive();
        }

        try {
            if (root.mPage == p_closedTreePage()) {
                // Already closed.
                return null;
            }

            if (!force && isInternal(mId)) {
                throw new IllegalStateException("Cannot close an internal index");
            }

            // Invalidate all cursors such that they refer to empty nodes.

            if (root.hasKeys()) {
                // If any active cursors, they might be in the middle of performing node splits
                // and merges. With the exclusive commit lock held, this is no longer the case.
                root.releaseExclusive();
                mDatabase.commitLock().acquireExclusive();
                try {
                    root.acquireExclusive();
                    if (root.mPage == p_closedTreePage()) {
                        return null;
                    }
                    root.invalidateCursors();
                } finally {
                    mDatabase.commitLock().releaseExclusive();
                }
            } else {
                // No keys in the root means that no splits or merges are in progress. No need
                // to release the latch, preventing a race condition when Index.drop is called.
                // Releasing the root latch would allow another thread to sneak in and insert
                // entries, which would then get silently deleted.
                root.invalidateCursors();
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
        // Acquire early to avoid deadlock when moving tree to trash.
        CommitLock.Shared shared = mDatabase.commitLock().acquireShared();

        Node root;
        try {
            root = mRoot;
            root.acquireExclusive();
        } catch (Throwable e) {
            shared.release();
            throw e;
        }

        try {
            try {
                if (root.mPage == p_closedTreePage()) {
                    throw new ClosedIndexException();
                }

                if (mustBeEmpty && (!root.isLeaf() || root.hasKeys())) {
                    // Note that this check also covers the transactional case, because deletes
                    // store ghosts. The message could be more accurate, but it would require
                    // scanning the whole index looking for ghosts. Using LockMode.UNSAFE
                    // deletes it's possible to subvert the transactional case, allowing the
                    // drop to proceed. The rollback logic in UndoLog accounts for this,
                    // ignoring undo operations for missing indexes. Preventing the drop in
                    // this case isn't worth the trouble, because UNSAFE is what it is.
                    throw new IllegalStateException("Cannot drop a non-empty index");
                }

                if (isInternal(mId)) {
                    throw new IllegalStateException("Cannot close an internal index");
                }
            } catch (Throwable e) {
                shared.release();
                throw e;
            }

            return mDatabase.deleteTree(this, shared);
        } finally {
            root.releaseExclusive();
        }
    }

    /**
     * Non-transactionally deletes all entries in the tree. No other cursors or threads can be
     * active in the tree. The root node is prepared for deletion as a side effect.
     *
     * @return false if stopped because database is closed
     */
    final boolean deleteAll() throws IOException {
        return newCursor(Transaction.BOGUS).deleteAll();
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
                    Node child = mDatabase.nodeMapGetExclusive(childId);
                    if (child != null) {
                        frame = new CursorFrame(frame);
                        frame.bind(node, pos);
                        node.releaseExclusive();
                        node = child;
                        pos = 0;
                        continue toLower;
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
     * Caller must hold exclusive latch and it must verify that node has
     * split. Node latch is released if an exception is thrown.
     *
     * The caller should also hold at least a shared commit lock, because this function tries
     * to allocate pages, and the page db may try to acquire a shared commit lock. Since the
     * commit lock is reentrant, if the caller already holds a shared commit lock, it
     * guarantees the page db can acquire another shared commit lock.
     *
     * @param frame bound cursor frame
     * @param node node which is bound to the frame, latched exclusively
     * @return replacement node, still latched
     */
    final Node finishSplit(final CursorFrame frame, Node node) throws IOException {
        while (true) {
            if (node == mRoot) {
                // When tree loses a level, a stub node remains for any cursors which were
                // bound to the old root. When a level is added back, the cursors bound to the
                // stub must rebind to the new root node. This happens when the
                // Node.finishSplitRoot method is called, but the stub node must be latched
                // exclusively for this to work correctly. The latch direction is in reverse
                // order, and so deadlock is possible. To avoid this, fail fast and retry as
                // necessary. Whenever the node latch is released and reacquired, the split
                // state must be checked again. Another thread might have finished the split.

                Node stub = mStubTail;

                if (stub == null) {
                    try {
                        node.finishSplitRoot();
                    } finally {
                        node.releaseExclusive();
                    }
                } else withStub: {
                    if (!stub.tryAcquireExclusive()) {
                        // Try to relatch in a different order.

                        node.releaseExclusive();
                        stub.acquireExclusive();

                        try {
                            node = frame.tryAcquireExclusive();
                        } catch (Throwable e) {
                            stub.releaseExclusive();
                            throw e;
                        }

                        if (node == null) {
                            // Latch attempt failed, so start over.
                            stub.releaseExclusive();
                            break withStub;
                        }

                        if (node.mSplit == null) {
                            // Split is finished now.
                            stub.releaseExclusive();
                            return node;
                        }

                        if (node != mRoot || stub != mStubTail) {
                            // Too much state has changed, so start over.
                            node.releaseExclusive();
                            stub.releaseExclusive();
                            break withStub;
                        }
                    }

                    try {
                        node.finishSplitRoot();
                        mStubTail = stub.mNodeMapNext;

                        // Note: Some cursor frames might still be bound to the stub. This is
                        // because the cursor is popping up to the stub, as part of an
                        // iteration or findNearby operation. Since popping to a stub is
                        // equivalent to popping past the root, the cursor operation is able to
                        // handle this. Iteration will finish normally, and findNearby will
                        // start over from the root. Also see stub comments in PageOps.
                    } finally {
                        node.releaseExclusive();
                        stub.releaseExclusive();
                    }
                }

                // Must always relatch node as referenced by the frame.
                node = frame.acquireExclusive();

                if (node.mSplit != null) {
                    // Still split.
                    continue;
                }

                return node;
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

    /**
     * Caller must have exclusively latched the tree root node instance and the lone child node.
     *
     * @param child must not be a leaf node
     */
    final void rootDelete(Node child) throws IOException {
        // Allocate stuff early in case of out of memory, and while root is latched. Note that
        // stub is assigned a NodeContext. Because the stub isn't in the context usage list,
        // attempting to update its position within it has no effect. Note too that the stub
        // isn't placed into the database node map.
        Node stub = new Node(mRoot.mContext);

        // Stub isn't in the node map, so use this pointer field to link the stubs together.
        stub.mNodeMapNext = mStubTail;
        mStubTail = stub;

        mRoot.rootDelete(this, child, stub);
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
            /*P*/ // [|
            /*P*/ // if (txn == Transaction.BOGUS) return LocalTransaction.BOGUS;
            /*P*/ // ]
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
        if (redo == null) {
            return 0;
        }
        return mDatabase.anyTransactionContext().redoStoreAutoCommit
            (redo.txnRedoWriter(), mId, key, value, mDatabase.mDurabilityMode);
    }

    /**
     * @return non-zero position if caller should call txnCommitSync
     */
    final long redoStoreNoLock(byte[] key, byte[] value) throws IOException {
        RedoWriter redo = mDatabase.mRedoWriter;
        if (redo == null) {
            return 0;
        }
        return mDatabase.anyTransactionContext().redoStoreNoLockAutoCommit
            (redo.txnRedoWriter(), mId, key, value, mDatabase.mDurabilityMode);
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
                                Thread task;
                                try {
                                    task = new Thread(() -> prime());
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
    }
}
