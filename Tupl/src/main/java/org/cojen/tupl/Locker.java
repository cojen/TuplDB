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
    private static final int FIRST_BLOCK_CAPACITY = 10;
    private static final int HIGHEST_BLOCK_CAPACITY = 80;
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

    public Locker(LockManager manager) {
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
    public final LockResult check(byte[] key) {
        return mManager.check(this, key);
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
    public final LockResult lockShared(byte[] key, long nanosTimeout) {
        return mManager.lockShared(this, key, nanosTimeout);
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
    public final LockResult lockUpgradable(byte[] key, long nanosTimeout) {
        return mManager.lockUpgradable(this, key, nanosTimeout);
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
    public final LockResult lockExclusive(byte[] key, long nanosTimeout) {
        return mManager.lockExclusive(this, key, nanosTimeout);
    }

    /**
     * Fully releases last lock acquired.
     *
     * @return unlocked key; instance is not cloned
     * @throws IllegalStateException if no locks held
     */
    public final byte[] unlock() {
        return mManager.unlock(this, pop());
    }

    /**
     * Releases last lock acquired, retaining a shared lock.
     *
     * @return unlocked key; instance is not cloned
     * @throws IllegalStateException if no locks held, or if too many shared locks
     */
    public final byte[] unlockToShared() {
        // Peek outside of try block, because its IllegalStateException should
        // not be handled here.
        Lock lock = peek();
        try {
            return mManager.unlockToShared(this, lock);
        } catch (IllegalStateException e) {
            // Lock was released as a side effect of hitting shared lock limit.
            pop();
            throw e;
        }
    }

    /**
     * Releases last lock acquired, retaining an upgradable lock.
     *
     * @return unlocked key; instance is not cloned
     * @throws IllegalStateException if no locks held, or if last lock is shared
     */
    public final byte[] unlockToUpgradable() {
        return mManager.unlockToUpgradable(this, peek());
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
        } else if (size == 1) {
            Object[] tailElements = new Object[INITIAL_SCOPE_STACK_CAPACITY];
            tailElements[0] = mScopeTailStack;
            tailElements[1] = tail;
            mScopeTailStack = tailElements;
            Object[] headElements = new Object[INITIAL_SCOPE_STACK_CAPACITY];
            headElements[0] = mScopeHeadStack;
            headElements[1] = head;
            mScopeHeadStack = headElements;
        } else {
            Object[] tailElements = (Object[]) mScopeTailStack;
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
                push((Lock) lastHead);
            } else {
                Object tail = mTailBlock;
                if (tail == null) {
                    mTailBlock = lastTail;
                    mHeadBlock = lastHead;
                } else {
                    Block lastHeadBlock = (Block) lastHead;
                    if (tail instanceof Lock) {
                        Block newHead = lastHeadBlock.prepend((Lock) tail);
                        if (newHead != null) {
                            mHeadBlock = newHead;
                        }
                    } else {
                        lastHeadBlock.prepend((Block) tail);
                    }
                }
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

    void push(Lock lock) {
        Object tailObj = mTailBlock;
        if (tailObj == null) {
            mHeadBlock = mTailBlock = lock;
        } else if (tailObj instanceof Lock) {
            mHeadBlock = mTailBlock = new ArrayBlock((Lock) tailObj, lock);
        } else {
            ((Block) tailObj).pushLock(this, lock);
        }
    }

    private Lock pop() {
        Object tailObj = mTailBlock;
        if (tailObj == null) {
            throw new IllegalStateException("No locks held");
        }
        if (tailObj instanceof Lock) {
            mTailBlock = null;
            mHeadBlock = null;
            return (Lock) tailObj;
        }
        return ((Block) tailObj).popLock(this);
    }

    private Lock peek() {
        Object tailObj = mTailBlock;
        if (tailObj == null) {
            throw new IllegalStateException("No locks held");
        }
        if (tailObj instanceof Lock) {
            return (Lock) tailObj;
        }
        return ((Block) tailObj).last();
    }

    private boolean popScope() {
        int size = mScopeStackSize - 1;
        if (size < 0) {
            return false;
        }
        Object scopeTailStack = mScopeTailStack;
        if (scopeTailStack instanceof Object[]) {
            mTailBlock = ((Object[]) scopeTailStack)[size];
            mHeadBlock = ((Object[]) mScopeHeadStack)[size];
        } else {
            mTailBlock = scopeTailStack;
            mHeadBlock = mScopeHeadStack;
        }
        mScopeStackSize = size;
        return true;
    }

    static abstract class Block {
        Block mPrev;

        abstract int capacity();

        abstract int size();

        abstract Lock last();

        abstract void pushLock(Locker locker, Lock lock);

        abstract Lock popLock(Locker locker);

        /**
         * @return previous Block or null if none left
         */
        abstract Block unlockAll(Locker locker);

        /**
         * @return null if prepended into this Block, else new head Block
         */
        abstract Block prepend(Lock lock);

        abstract void prepend(Block block);

        abstract int copyTo(Lock[] elements, int offset);
    }

    static final class TinyBlock extends Block {
        private Lock mElement;

        TinyBlock(Lock element) {
            mElement = element;
        }

        int capacity() {
            return 1;
        }

        int size() {
            return 1;
        }

        Lock last() {
            return mElement;
        }

        void pushLock(Locker locker, Lock lock) {
            locker.mTailBlock = new ArrayBlock(this, lock);
        }

        Lock popLock(Locker locker) {
            Lock lock = mElement;
            mElement = null;
            if ((locker.mTailBlock = mPrev) == null) {
                locker.mHeadBlock = null;
            } else {
                mPrev = null;
            }
            return lock;
        }

        Block unlockAll(Locker locker) {
            locker.mManager.unlock(locker, mElement);
            mElement = null;
            Block prev = mPrev;
            mPrev = null;
            return prev;
        }

        Block prepend(Lock lock) {
            return mPrev = new TinyBlock(lock);
        }

        void prepend(Block block) {
            mPrev = block;
        }

        int copyTo(Lock[] elements, int offset) {
            if (offset < elements.length) {
                elements[offset] = mElement;
                return 1;
            }
            return 0;
        }
    }

    static final class ArrayBlock extends Block {
        private Lock[] mElements;
        private int mSize;

        ArrayBlock(Lock first, Lock second) {
            Lock[] elements = new Lock[FIRST_BLOCK_CAPACITY];
            elements[0] = first;
            elements[1] = second;
            mElements = elements;
            mSize = 2;
        }

        ArrayBlock(Block prev, Lock first) {
            mPrev = prev;
            int capacity = prev.capacity();
            if (capacity < FIRST_BLOCK_CAPACITY) {
                capacity = FIRST_BLOCK_CAPACITY;
            } else if (capacity < HIGHEST_BLOCK_CAPACITY) {
                capacity <<= 1;
            }
            (mElements = new Lock[capacity])[0] = first;
            mSize = 1;
        }

        int capacity() {
            return mElements.length;
        }

        int size() {
            return mSize;
        }

        Lock last() {
            return mElements[mSize - 1];
        }

        void pushLock(Locker locker, Lock lock) {
            Lock[] elements = mElements;
            int size = mSize;
            if (size < elements.length) {
                elements[size] = lock;
                mSize = size + 1;
            } else {
                locker.mTailBlock = new ArrayBlock(this, lock);
            }
        }

        Lock popLock(Locker locker) {
            Lock[] elements = mElements;
            int size = mSize;
            Lock lock = elements[--size];
            elements[size] = null;
            if (size == 0) {
                if ((locker.mTailBlock = mPrev) == null) {
                    locker.mHeadBlock = null;
                } else {
                    mPrev = null;
                }
            } else {
                mSize = size;
            }
            return lock;
        }

        Block unlockAll(Locker locker) {
            Lock[] elements = mElements;
            LockManager manager = locker.mManager;
            for (int i=mSize; --i>=0; ) {
                manager.unlock(locker, elements[i]);
                elements[i] = null;
            }
            Block prev = mPrev;
            mPrev = null;
            return prev;
        }

        Block prepend(Lock lock) {
            Lock[] elements = mElements;
            int size = mSize;
            if (size < elements.length) {
                System.arraycopy(elements, 0, elements, 1, size);
                elements[0] = lock;
                mSize = size + 1;
                return null;
            } else {
                return mPrev = new TinyBlock(lock);
            }
        }

        void prepend(Block block) {
            Lock[] elements = mElements;
            int size = mSize;
            int otherSize = block.size();
            if (size + otherSize <= elements.length) {
                System.arraycopy(elements, 0, elements, otherSize, size);
                block.copyTo(elements, 0);
                mSize = size + otherSize;
            } else {
                mPrev = block;
            }
        }

        int copyTo(Lock[] elements, int offset) {
            int size = mSize;
            if (offset + size <= elements.length) {
                System.arraycopy(mElements, 0, elements, offset, size);
                return size;
            }
            return 0;
        }
    }
}
