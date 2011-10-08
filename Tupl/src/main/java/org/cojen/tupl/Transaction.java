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

/**
 * Transaction instances can only be safely used by one thread at a
 * time. Transactions can be exchanged by threads, as long as a happens-before
 * relationship is established. Without proper exclusion, multiple threads
 * interacting with a Transaction instance results in undefined behavior.
 *
 * @author Brian S O'Neill
 */
public class Transaction extends Locker {
    final DurabilityMode mDurabilityMode;
    //private final UndoLog mUndo;
    //private final RedoLog mRedo;

    private LockMode mLockMode;
    // Is null if empty; LockMode instance if one; LockMode[] if more.
    private Object mLockModeStack;
    private int mLockModeStackSize;

    Transaction(LockManager manager, DurabilityMode durabilityMode, LockMode lockMode) {
        super(manager);
        mDurabilityMode = durabilityMode;
        mLockMode = lockMode;
    }

    /**
     * Set the lock mode for the current scope. Transactions begin in {@link
     * LockMode.UPGRADABLE_READ} mode, and newly entered scopes begin at the
     * outer scope's current mode. Exiting a scope reverts the lock mode.
     *
     * @param mode new lock mode
     * @throws IllegalArgumentException if mode is null
     */
    public final void lockMode(LockMode mode) {
        if (mode == null) {
            throw new IllegalArgumentException("Lock mode is null");
        } else {
            mLockMode = mode;
        }
    }

    /**
     * Returns the lock mode in currently in effect.
     */
    public final LockMode lockMode() {
        return mLockMode;
    }

    /**
     * Commit all modifications made within the current transaction scope. The
     * current scope is still valid following a commit.
     */
    public final void commit() throws IOException {
        // FIXME
        throw null;
    }

    /**
     * Enter a new transaction scope.
     */
    public final void enter() throws IOException {
        super.scopeEnter();

        Object last = mLockModeStack;
        if (last == null) {
            mLockModeStack = mLockMode;
            mLockModeStackSize = 1;
        } else {
            try {
                if (last instanceof LockMode) {
                    LockMode lastMode = (LockMode) last;
                    LockMode[] stack;
                    stack = new LockMode[INITIAL_SCOPE_STACK_CAPACITY];
                    stack[0] = lastMode;
                    stack[1] = mLockMode;
                    mLockModeStack = stack;
                    mLockModeStackSize = 2;
                } else {
                    LockMode[] stack = (LockMode[]) last;
                    int size = mLockModeStackSize;
                    if (size >= stack.length) {
                        LockMode[] newStack = new LockMode[stack.length << 1];
                        System.arraycopy(stack, 0, newStack, 0, size);
                        mLockModeStack = stack = newStack;
                    }
                    stack[size] = mLockMode;
                    mLockModeStackSize = size + 1;
                }
            } catch (Throwable e) {
                try {
                    super.scopeExit(false);
                } finally {
                    throw Utils.rethrow(e);
                }
            }
        }

        // FIXME
        throw null;
    }

    /**
     * Exit the current transaction scope, rolling back all uncommitted
     * modifications made within.
     *
     * @throws IllegalStateException if no current scope
     */
    public final void exit() throws IOException {
        // FIXME
        throw null;
    }

    /**
     * Exit all transaction scopes, rolling back all uncommitted modifications.
     */
    public final void exitAll() throws IOException {
        // FIXME
        throw null;
    }
}
