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
 * instances can only be safely used by one thread at a time. Without proper
 * synchronization, multiple threads interacting with a Locker instance results
 * in undefined behavior.
 *
 * @author Brian S O'Neill
 */
public class Locker {
    private static final int FIRST_BLOCK_CAPACITY = 10;
    private static final int HIGHEST_BLOCK_CAPACITY = 80;

    private final LockManager mManager;

    private int mHashCode;

    // Is null if empty; Lock instance if one; Block if more.
    private Object mTailBlock;

    public Locker(LockManager manager) {
        if (manager == null) {
            throw new IllegalArgumentException("LockManager is null");
        }
        mManager = manager;
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
    public LockResult lockShared(byte[] key, long nanosTimeout) {
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
    public LockResult lockUpgradable(byte[] key, long nanosTimeout) {
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
    public LockResult lockExclusive(byte[] key, long nanosTimeout) {
        return mManager.lockExclusive(this, key, nanosTimeout);
    }

    /**
     * Fully releases last lock acquired.
     *
     * @return unlocked key; instance is not cloned
     * @throws IllegalStateException if no locks held
     */
    public byte[] unlock() {
        return mManager.unlock(this, pop());
    }

    /**
     * Releases last lock acquired, retaining a shared lock.
     *
     * @return unlocked key; instance is not cloned
     * @throws IllegalStateException if no locks held, or if too many shared locks
     */
    public byte[] unlockToShared() {
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
    public byte[] unlockToUpgradable() {
        return mManager.unlockToUpgradable(this, peek());
    }

    /**
     * Release all locks currently held by this Locker.
     */
    public void unlockAll() {
        Object tailObj = mTailBlock;
        if (tailObj == null) {
            return;
        }
        if (tailObj instanceof Lock) {
            mManager.unlock(this, (Lock) tailObj);
        } else {
            Block tail = (Block) tailObj;
            while (true) {
                Lock[] elements = tail.mElements;
                for (int i=tail.mSize; --i>=0; ) {
                    mManager.unlock(this, elements[i]);
                    elements[i] = null;
                }
                Block prev = tail.mPrev;
                if (prev == null) {
                    break;
                }
                tail.mPrev = null;
                tail = prev;
            }
        }
        mTailBlock = null;
    }

    @Override
    public final int hashCode() {
        // Caching the default hashcode doubles the performance of this method,
        // when using the HotSpot client or server VM.
        int hash = mHashCode;
        if (hash == 0) {
            hash = mHashCode = super.hashCode();
        }
        return hash;
    }

    void push(Lock lock) {
        Object tailObj = mTailBlock;
        if (tailObj == null) {
            mTailBlock = lock;
        } else if (tailObj instanceof Lock) {
            mTailBlock = new Block((Lock) tailObj, lock);
        } else {
            Block tail = (Block) tailObj;
            Lock[] elements = tail.mElements;
            int size = tail.mSize;
            if (size < elements.length) {
                elements[size] = lock;
                tail.mSize = size + 1;
            } else {
                mTailBlock = new Block(tail, lock);
            }
        }
    }

    private Lock pop() {
        Object tailObj = mTailBlock;
        if (tailObj == null) {
            throw new IllegalStateException("No locks held");
        }
        if (tailObj instanceof Lock) {
            mTailBlock = null;
            return (Lock) tailObj;
        }
        Block tail = (Block) tailObj;
        Lock[] elements = tail.mElements;
        int size = tail.mSize;
        Lock lock = elements[--size];
        elements[size] = null;
        if (size == 0) {
            mTailBlock = tail.mPrev;
            tail.mPrev = null;
        } else {
            tail.mSize = size;
        }
        return lock;
    }

    private Lock peek() {
        Object tailObj = mTailBlock;
        if (tailObj == null) {
            throw new IllegalStateException("No locks held");
        }
        if (tailObj instanceof Lock) {
            return (Lock) tailObj;
        }
        Block tail = (Block) tailObj;
        return tail.mElements[tail.mSize - 1];
    }

    static final class Block {
        private Block mPrev;

        Lock[] mElements;
        int mSize;

        Block(Lock first, Lock second) {
            Lock[] elements = new Lock[FIRST_BLOCK_CAPACITY];
            elements[0] = first;
            elements[1] = second;
            mElements = elements;
            mSize = 2;
        }

        Block(Block prev, Lock first) {
            mPrev = prev;
            int capacity = prev.mElements.length;
            if (capacity < HIGHEST_BLOCK_CAPACITY) {
                capacity <<= 1;
            }
            (mElements = new Lock[capacity])[0] = first;
            mSize = 1;
        }
    }
}
