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

    @Override
    public long count(byte[] lowKey, byte[] highKey) throws IOException {
        TreeCursor cursor = new TreeCursor(this, Transaction.BOGUS);
        TreeCursor high = null;
        try {
            if (highKey != null) {
                high = new TreeCursor(this, Transaction.BOGUS);
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

        while (!node.isLeaf()) {
            int childPos;
            try {
                childPos = Node.internalPos(node.binarySearch(key));
            } catch (Throwable e) {
                node.releaseShared();
                throw e;
            }

            long childId = node.retrieveChildRefId(childPos);
            Node childNode = mDatabase.nodeMapGet(childId);

            if (childNode != null) {
                childNode.acquireShared();

                // Need to check again in case evict snuck in.
                if (childId == childNode.mId) {
                    node.releaseShared();
                    node = childNode;
                    if (node.mSplit != null) {
                        node = node.mSplit.selectNode(node, key);
                    }
                    node.used();
                    continue;
                }

                childNode.releaseShared();
            }

            node = node.loadChild(mDatabase, childId, Node.OPTION_PARENT_RELEASE_SHARED);

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
                    frame.bind(node, midPos - node.searchVecStart());
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
            frame.bind(node, ~(lowPos - node.searchVecStart()));
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
                    if (node.mSplit != null) {
                        node = node.mSplit.selectNode(node, key);
                    }
                    int pos = frame.mNodePos;
                    return pos >= 0 ? node.retrieveLeafValue(pos) : null;
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
    public void store(Transaction txn, byte[] key, byte[] value) throws IOException {
        keyCheck(key);
        TreeCursor cursor = new TreeCursor(this, txn);
        cursor.autoload(false);
        cursor.findAndStore(key, value);
    }

    @Override
    public byte[] exchange(Transaction txn, byte[] key, byte[] value) throws IOException {
        keyCheck(key);
        return new TreeCursor(this, txn).findAndStore(key, value);
    }

    @Override
    public boolean insert(Transaction txn, byte[] key, byte[] value) throws IOException {
        keyCheck(key);
        TreeCursor cursor = new TreeCursor(this, txn);
        cursor.autoload(false);
        return cursor.findAndModify(key, TreeCursor.MODIFY_INSERT, value);
    }

    @Override
    public boolean replace(Transaction txn, byte[] key, byte[] value) throws IOException {
        keyCheck(key);
        TreeCursor cursor = new TreeCursor(this, txn);
        cursor.autoload(false);
        return cursor.findAndModify(key, TreeCursor.MODIFY_REPLACE, value);
    }

    @Override
    public boolean update(Transaction txn, byte[] key, byte[] oldValue, byte[] newValue)
        throws IOException
    {
        keyCheck(key);
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

    /*
    @Override
    public Stream newStream() {
        TreeCursor cursor = new TreeCursor(this);
        cursor.autoload(false);
        return new TreeValueStream(cursor);
    }
    */

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
        TreeCursor cursor = new TreeCursor(this, txn);
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
        new TreeCursor(this, Transaction.BOGUS).deleteAll();
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
