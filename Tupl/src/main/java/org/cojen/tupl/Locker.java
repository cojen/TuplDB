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

/**
 * Accumulates a scoped stack of locks, bound to arbitrary keys. Locker
 * instances can only be safely used by one thread at a time. Lockers can be
 * exchanged by threads, as long as a happens-before relationship is
 * established. Without proper exclusion, multiple threads interacting with a
 * Locker instance results in undefined behavior.
 *
 * @author Brian S O'Neill
 */
public class Locker {
    private static final int INITIAL_SCOPE_STACK_CAPACITY = 4;

    final LockManager mManager;

    private int mHashCode;

    // Is null if empty; Lock instance if one; Block if more.
    Object mTailBlock;
    Object mHeadBlock;

    // The logical type of the scope stack is: Stack<Stack<Lock>>
    // Is null if empty; saved tail if one; Object[] if more.
    private Object mScopeTailStack;
    private Object mScopeHeadStack;
    private int mScopeStackSize;

    Locker(LockManager manager) {
        if (manager == null) {
            throw new IllegalArgumentException("LockManager is null");
        }
        mManager = manager;
    }

    /**
     * Check the lock ownership for the given key.
     *
     * @return UNOWNED, OWNED_SHARED, OWNED_UPGRADABLE, or OWNED_EXCLUSIVE
     */
    public final LockResult check(long indexId, byte[] key) {
        return mManager.check(this, indexId, key);
    }

    /**
     * Attempt to acquire a shared lock for the given key, denying exclusive
     * locks. If return value is OWNED_*, locker already owns a strong enough
     * lock, and no extra unlock should be performed.
     *
     * @param key key to lock; instance is not cloned
     * @param nanosTimeout maximum time to wait for lock; negative timeout is infinite
     * @return INTERRUPTED, TIMED_OUT_LOCK, ACQUIRED, OWNED_SHARED,
     * OWNED_UPGRADABLE, or OWNED_EXCLUSIVE
     * @throws IllegalStateException if too many shared locks
     */
    public final LockResult lockShared(long indexId, byte[] key, long nanosTimeout) {
        return mManager.lockShared(this, indexId, key, nanosTimeout);
    }

    /**
     * Attempt to acquire an upgradable lock for the given key, denying
     * exclusive and additional upgradable locks. If return value is OWNED_*,
     * locker already owns a strong enough lock, and no extra unlock should be
     * performed. If ILLEGAL is returned, locker holds a shared lock, which
     * cannot be upgraded.
     *
     * @param key key to lock; instance is not cloned
     * @param nanosTimeout maximum time to wait for lock; negative timeout is infinite
     * @return ILLEGAL, INTERRUPTED, TIMED_OUT_LOCK, ACQUIRED,
     * OWNED_UPGRADABLE, or OWNED_EXCLUSIVE
     */
    public final LockResult lockUpgradable(long indexId, byte[] key, long nanosTimeout) {
        return mManager.lockUpgradable(this, indexId, key, nanosTimeout);
    }

    /**
     * Attempt to acquire an exclusive lock for the given key, denying any
     * additional locks. If return value is OWNED_EXCLUSIVE, locker already
     * owns exclusive lock, and no extra unlock should be performed. If ILLEGAL
     * is returned, locker holds a shared lock, which cannot be upgraded.
     *
     * @param key key to lock; instance is not cloned
     * @param nanosTimeout maximum time to wait for lock; negative timeout is infinite
     * @return ILLEGAL, INTERRUPTED, TIMED_OUT_LOCK, ACQUIRED, UPGRADED, or
     * OWNED_EXCLUSIVE
     */
    public final LockResult lockExclusive(long indexId, byte[] key, long nanosTimeout) {
        return mManager.lockExclusive(this, indexId, key, nanosTimeout);
    }

    /**
     * Returns the index of the last lock acquired.
     *
     * @return locked index id
     * @throws IllegalStateException if no locks held
     */
    public final long lastLockedIndex() {
        return peek().mIndexId;
    }

    /**
     * Returns the key of the last lock acquired.
     *
     * @return locked key; instance is not cloned
     * @throws IllegalStateException if no locks held
     */
    public final byte[] lastLockedKey() {
        return peek().mKey;
    }

    private Lock peek() {
        Object tailObj = mTailBlock;
        if (tailObj == null) {
            throw new IllegalStateException("No locks held");
        }
        return (tailObj instanceof Lock) ? ((Lock) tailObj) : (((Block) tailObj).last());
    }

    /**
     * Fully releases last lock acquired. If the last lock operation was an
     * upgrade, for a lock not immediately acquired, unlock is not
     * allowed. Instead, an IllegalStateException is thrown.
     *
     * @throws IllegalStateException if no locks held, or if unlocking a
     * non-immediate upgrade
     */
    public final void unlock() {
        Object tailObj = mTailBlock;
        if (tailObj == null) {
            throw new IllegalStateException("No locks held");
        }
        if (tailObj instanceof Lock) {
            mTailBlock = null;
            mHeadBlock = null;
            mManager.unlock(this, (Lock) tailObj);
        } else {
            ((Block) tailObj).unlockLast(this);
        }
    }

    /**
     * Releases last lock acquired, retaining a shared lock. If the last lock
     * operation was an upgrade, for a lock not immediately acquired, unlock is
     * not allowed. Instead, an IllegalStateException is thrown.
     *
     * @throws IllegalStateException if no locks held, or if too many shared
     * locks, or if unlocking a non-immediate upgrade
     */
    public final void unlockToShared() {
        Object tailObj = mTailBlock;
        if (tailObj == null) {
            throw new IllegalStateException("No locks held");
        }
        if (tailObj instanceof Lock) {
            mManager.unlockToShared(this, (Lock) tailObj);
        } else {
            ((Block) tailObj).unlockLastToShared(this);
        }
    }

    /**
     * Releases last lock acquired or upgraded, retaining an upgradable lock.
     *
     * @throws IllegalStateException if no locks held, or if last lock is shared
     */
    public final void unlockToUpgradable() {
        Object tailObj = mTailBlock;
        if (tailObj == null) {
            throw new IllegalStateException("No locks held");
        }
        if (tailObj instanceof Lock) {
            mManager.unlockToUpgradable(this, (Lock) tailObj);
        } else {
            ((Block) tailObj).unlockLastToUpgradable(this);
        }
    }

    /**
     * Release all locks held by this Locker, and exit all scopes.
     */
    public final void reset() {
        scopeUnlockAll();

        Object scopeTailStack = mScopeTailStack;
        if (scopeTailStack != null) {
            if (scopeTailStack instanceof Object[]) {
                Object[] elements = (Object[]) scopeTailStack;
                for (int i=mScopeStackSize; --i>=0; ) {
                    unlockAll(elements[i]);
                }
            } else {
                unlockAll(scopeTailStack);
            }
            mScopeTailStack = null;
            mScopeHeadStack = null;
            mScopeStackSize = 0;
        }
    }

    public final void scopeEnter() {
        // Move the current stack of locks to scope stack.

        Object tail = mTailBlock;
        Object head = mHeadBlock;
        int size = mScopeStackSize;

        if (size == 0) {
            mScopeTailStack = tail;
            mScopeHeadStack = head;
        } else {
            Object scopeTailStack = mScopeTailStack;
            if (size == 1 && !(scopeTailStack instanceof Object[])) {
                Object[] tailElements = new Object[INITIAL_SCOPE_STACK_CAPACITY];
                Object[] headElements = new Object[INITIAL_SCOPE_STACK_CAPACITY];
                tailElements[0] = scopeTailStack;
                tailElements[1] = tail;
                headElements[0] = mScopeHeadStack;
                headElements[1] = head;
                mScopeTailStack = tailElements;
                mScopeHeadStack = headElements;
            } else {
                Object[] tailElements = (Object[]) scopeTailStack;
                Object[] headElements = (Object[]) mScopeHeadStack;
                if (size >= tailElements.length) {
                    Object[] newTailElements = new Object[tailElements.length << 1];
                    System.arraycopy(tailElements, 0, newTailElements, 0, size);
                    mScopeTailStack = tailElements = newTailElements;
                    Object[] newHeadElements = new Object[headElements.length << 1];
                    System.arraycopy(headElements, 0, newHeadElements, 0, size);
                    mScopeHeadStack = headElements = newHeadElements;
                }
                tailElements[size] = tail;
                headElements[size] = head;
            }
        }

        mScopeStackSize = size + 1;
        mTailBlock = null;
    }

    /**
     * Release all locks held by this Locker, within the current scope. If not
     * in a scope, all held locks are released.
     */
    public final void scopeUnlockAll() {
        unlockAll(mTailBlock);
        mTailBlock = null;
        mHeadBlock = null;
    }

    /**
     * Release all non-exclusive locks held by this Locker, within the current
     * scope. If not in a scope, all held non-exclusive locks are released.
     */
    public final void scopeUnlockAllNonExclusive() {
        // FIXME
        throw null;
        // FIXME: Collapse non-exclusive locks within Blocks, possibly moving
        // to previous Block. Is this efficient? Should a new stack be built
        // and then reversed?
        // if (mManager.unlockIfNonExclusive(this, lock)) ...
    }

    /**
     * Exit the current scope, releasing all held locks. If requested, the
     * locks can instead be promoted to an enclosing scope. If no enclosing
     * scope exists, held locks are released.
     *
     * @param promote when true, promote all locks to enclosing scope, if one exists
     * @return false if last scope exited and all locks released
     */
    public final boolean scopeExit(boolean promote) {
        Object lastTail = mTailBlock;

        if (!promote) {
            unlockAll(lastTail);
            return popScope();
        }

        Object lastHead = mHeadBlock;

        if (!popScope()) {
            unlockAll(lastTail);
            return false;
        }

        if (lastHead != null) {
            if (lastHead instanceof Lock) {
                push((Lock) lastHead, 0);
            } else {
                Object tail = mTailBlock;
                if (tail == null) {
                    mHeadBlock = lastHead;
                } else {
                    Block lastHeadBlock = (Block) lastHead;
                    if (tail instanceof Lock) {
                        mHeadBlock = lastHeadBlock.prepend((Lock) tail);
                    } else if (lastHeadBlock.prepend((Block) tail) && tail == mHeadBlock) {
                        mHeadBlock = lastHead;
                    }
                }
                mTailBlock = lastTail;
            }
        }

        return true;
    }

    @Override
    public final int hashCode() {
        int hash = mHashCode;
        if (hash == 0) {
            // Scramble the hashcode a bit, just like HashMap does.
            hash = super.hashCode();
            hash ^= (hash >>> 20) ^ (hash >>> 12);
            return mHashCode = hash ^ (hash >>> 7) ^ (hash >>> 4);
        }
        return hash;
    }

    private void unlockAll(Object tailObj) {
        if (tailObj instanceof Lock) {
            mManager.unlock(this, (Lock) tailObj);
        } else {
            Block tail = (Block) tailObj;
            while (tail != null) {
                tail = tail.unlockAll(this);
            }
        }
    }

    /**
     * @param upgraded only 0 or 1 allowed
     */
    void push(Lock lock, int upgrade) {
        Object tailObj = mTailBlock;
        if (tailObj == null) {
            mHeadBlock = mTailBlock = upgrade == 0 ? lock : new ArrayBlock(lock);
        } else if (tailObj instanceof Lock) {
            // Don't push lock upgrade if it applies to the last acquisition.
            if (tailObj != lock) {
                mHeadBlock = mTailBlock = new ArrayBlock((Lock) tailObj, lock, upgrade);
            }
        } else {
            ((Block) tailObj).pushLock(this, lock, upgrade);
        }
    }

    private boolean popScope() {
        int size = mScopeStackSize - 1;
        if (size < 0) {
            mTailBlock = null;
            mHeadBlock = null;
            return false;
        }
        Object scopeTailStack = mScopeTailStack;
        if (scopeTailStack instanceof Object[]) {
            Object[] tailStackArray = (Object[]) scopeTailStack;
            Object[] headStackArray = (Object[]) mScopeHeadStack;
            mTailBlock = tailStackArray[size];
            mHeadBlock = headStackArray[size];
            tailStackArray[size] = null;
            headStackArray[size] = null;
        } else {
            mTailBlock = scopeTailStack;
            mHeadBlock = mScopeHeadStack;
            mScopeTailStack = null;
            mScopeHeadStack = null;
        }
        mScopeStackSize = size;
        return true;
    }

    static abstract class Block {
        Block mPrev;

        abstract int capacity();

        abstract int size();

        abstract void pushLock(Locker locker, Lock lock, int upgrade);

        abstract Lock last();

        abstract void unlockLast(Locker locker);

        abstract void unlockLastToShared(Locker locker);

        abstract void unlockLastToUpgradable(Locker locker);

        /**
         * @return previous Block or null if none left
         */
        abstract Block unlockAll(Locker locker);

        /**
         * @return new head Block
         */
        abstract Block prepend(Lock lock);

        /**
         * @return if block was absorbed into this one
         */
        abstract boolean prepend(Block block);

        /**
         * @return upgrades
         */
        abstract long copyTo(Lock[] elements);
    }

    static final class TinyBlock extends Block {
        private Lock mElement;

        TinyBlock(Lock element) {
            mElement = element;
        }

        @Override
        int capacity() {
            return 1;
        }

        @Override
        int size() {
            return 1;
        }

        @Override
        void pushLock(Locker locker, Lock lock, int upgrade) {
            // Don't push lock upgrade if it applies to the last acquisition.
            if (mElement != lock) {
                locker.mTailBlock = new ArrayBlock(this, lock, upgrade);
            }
        }

        @Override
        Lock last() {
            return mElement;
        }

        @Override
        void unlockLast(Locker locker) {
            locker.mManager.unlock(locker, mElement);

            // Only pop lock if unlock succeeded.
            mElement = null;
            if ((locker.mTailBlock = mPrev) == null) {
                locker.mHeadBlock = null;
            } else {
                mPrev = null;
            }
        }

        @Override
        void unlockLastToShared(Locker locker) {
            locker.mManager.unlockToShared(locker, mElement);
        }

        @Override
        void unlockLastToUpgradable(Locker locker) {
            locker.mManager.unlockToUpgradable(locker, mElement);
        }

        @Override
        Block unlockAll(Locker locker) {
            locker.mManager.unlock(locker, mElement);
            mElement = null;
            Block prev = mPrev;
            mPrev = null;
            return prev;
        }

        @Override
        Block prepend(Lock lock) {
            return mPrev = new TinyBlock(lock);
        }

        @Override
        boolean prepend(Block block) {
            mPrev = block;
            return false;
        }

        @Override
        long copyTo(Lock[] elements) {
            elements[0] = mElement;
            return 0;
        }
    }

    static final class ArrayBlock extends Block {
        private static final int FIRST_BLOCK_CAPACITY = 8;
        private static final int HIGHEST_BLOCK_CAPACITY = 64;

        private Lock[] mElements;
        private long mUpgrades;
        private int mSize;

        // Always creates first as an upgrade.
        ArrayBlock(Lock first) {
            (mElements = new Lock[FIRST_BLOCK_CAPACITY])[0] = first;
            mUpgrades = 1;
            mSize = 1;
        }

        // First is never an upgrade.
        ArrayBlock(Lock first, Lock second, int upgrade) {
            Lock[] elements = new Lock[FIRST_BLOCK_CAPACITY];
            elements[0] = first;
            elements[1] = second;
            mElements = elements;
            mUpgrades = upgrade << 1;
            mSize = 2;
        }

        ArrayBlock(Block prev, Lock first, int upgrade) {
            mPrev = prev;
            int capacity = prev.capacity();
            if (capacity < FIRST_BLOCK_CAPACITY) {
                capacity = FIRST_BLOCK_CAPACITY;
            } else if (capacity < HIGHEST_BLOCK_CAPACITY) {
                capacity <<= 1;
            }
            (mElements = new Lock[capacity])[0] = first;
            mUpgrades = upgrade;
            mSize = 1;
        }

        @Override
        int capacity() {
            return mElements.length;
        }

        @Override
        int size() {
            return mSize;
        }

        @Override
        void pushLock(Locker locker, Lock lock, int upgrade) {
            Lock[] elements = mElements;
            int size = mSize;

            // Don't push lock upgrade if it applies to the last acquisition.
            if (upgrade != 0 && size > 0 && elements[size - 1] == lock) {
                return;
            }

            if (size < elements.length) {
                elements[size] = lock;
                mUpgrades |= ((long) upgrade) << size;
                mSize = size + 1;
            } else {
                locker.mTailBlock = new ArrayBlock(this, lock, upgrade);
            }
        }

        @Override
        Lock last() {
            return mElements[mSize - 1];
        }

        @Override
        void unlockLast(Locker locker) {
            int size = mSize - 1;

            long upgrades = mUpgrades;
            long mask = 1L << size;
            if ((upgrades & mask) != 0) {
                throw new IllegalStateException("Cannot unlock non-immediate upgrade");
            }

            Lock[] elements = mElements;
            locker.mManager.unlock(locker, elements[size]);

            // Only pop lock if unlock succeeded.
            elements[size] = null;
            if (size == 0) {
                if ((locker.mTailBlock = mPrev) == null) {
                    locker.mHeadBlock = null;
                } else {
                    mPrev = null;
                }
            } else {
                mUpgrades &= upgrades & ~mask;
                mSize = size;
            }
        }

        @Override
        void unlockLastToShared(Locker locker) {
            int size = mSize - 1;
            if ((mUpgrades & (1L << size)) != 0) {
                throw new IllegalStateException("Cannot unlock non-immediate upgrade");
            }
            locker.mManager.unlockToShared(locker, mElements[size]);
        }

        @Override
        void unlockLastToUpgradable(Locker locker) {
            Lock[] elements = mElements;
            int size = mSize;
            locker.mManager.unlockToUpgradable(locker, elements[--size]);

            long upgrades = mUpgrades;
            long mask = 1L << size;
            if ((upgrades & mask) != 0) {
                // Pop upgrade off stack, but only if unlock succeeded.
                elements[size] = null;
                if (size == 0) {
                    if ((locker.mTailBlock = mPrev) == null) {
                        locker.mHeadBlock = null;
                    } else {
                        mPrev = null;
                    }
                } else {
                    mUpgrades = upgrades & ~mask;
                    mSize = size;
                }
            }
        }

        @Override
        Block unlockAll(Locker locker) {
            Lock[] elements = mElements;
            LockManager manager = locker.mManager;
            int i = mSize - 1;
            long mask = 1L << i;
            long upgrades = mUpgrades;
            while (true) {
                Lock element = elements[i];
                if ((upgrades & mask) != 0) {
                    manager.unlockToUpgradable(locker, element);
                } else {
                    manager.unlock(locker, element);
                }
                elements[i] = null;
                if (--i < 0) {
                    break;
                }
                mask >>= 1;
            }
            Block prev = mPrev;
            mPrev = null;
            return prev;
        }

        @Override
        Block prepend(Lock lock) {
            Lock[] elements = mElements;
            int size = mSize;
            if (size < elements.length) {
                System.arraycopy(elements, 0, elements, 1, size);
                elements[0] = lock;
                mUpgrades <<= 1;
                mSize = size + 1;
                return this;
            } else {
                return mPrev = new TinyBlock(lock);
            }
        }

        @Override
        boolean prepend(Block block) {
            Lock[] elements = mElements;
            int size = mSize;
            int otherSize = block.size();
            if (size + otherSize <= elements.length) {
                System.arraycopy(elements, 0, elements, otherSize, size);
                mUpgrades = (mUpgrades << otherSize) | block.copyTo(elements);
                mSize = size + otherSize;
                mPrev = block.mPrev;
                return true;
            } else {
                mPrev = block;
                return false;
            }
        }

        @Override
        long copyTo(Lock[] elements) {
            System.arraycopy(mElements, 0, elements, 0, mSize);
            return mUpgrades;
        }
    }
}
