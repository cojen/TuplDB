/*
 *  Copyright 2013 Brian S O'Neill
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

import java.util.Arrays;

/**
 * Index implementation optimized for deque-like workloads. Tradeoff is reduced write
 * concurrency.
 *
 * @author Brian S O'Neill
 */
final class DequeIndex extends Latch implements Index {
    private final Tree mSource;
    private final Lock mIndexLock;
    private final TreeCursor mLowCursor, mHighCursor;

    DequeIndex(Tree source) {
        mSource = source;
        mIndexLock = new Lock();

        mLowCursor = new TreeCursor(source, Transaction.BOGUS);
        mLowCursor.autoload(false);

        mHighCursor = new TreeCursor(source, Transaction.BOGUS);
        mHighCursor.autoload(false);
    }

    @Override
    public long getId() {
        return mSource.getId();
    }

    @Override
    public byte[] getName() {
        return mSource.getName();
    }

    @Override
    public String getNameString() {
        return mSource.getNameString();
    }

    @Override
    public Cursor newCursor(Transaction txn) {
        return new DequeCursor(this, new TreeCursor(mSource, txn));
    }

    @Override
    public byte[] load(Transaction txn, byte[] key) throws IOException {
        return mSource.load(txn, key);
    }

    @Override
    public void store(Transaction txn, byte[] key, byte[] value) throws IOException {
        if (value == null) {
            delete(txn, key);
            return;
        }

        Locker locker = indexLockExclusive(txn);
        try {
            int compare = highExtremityCompare(key);

            if (compare >= 0) {
                TreeCursor highCursor = mHighCursor;
                highCursor.link(txn);
                try {
                    if (compare == 0) {
                        highCursor.store(value);
                    } else {
                        highCursor.insertExtremity(key, value, 2);
                    }
                } finally {
                    highCursor.mTxn = Transaction.BOGUS;
                }
                return;
            }

            compare = lowExtremityCompare(key);

            if (compare <= 0) {
                TreeCursor lowCursor = mLowCursor;
                lowCursor.link(txn);
                try {
                    if (compare == 0) {
                        lowCursor.store(value);
                    } else {
                        lowCursor.insertExtremity(key, value, 0);
                    }
                } finally {
                    lowCursor.mTxn = Transaction.BOGUS;
                }
                return;
            }

            mSource.store(txn, key, value);
        } finally {
            indexUnlock(locker);
        }
    }

    @Override
    public byte[] exchange(Transaction txn, byte[] key, byte[] value) throws IOException {
        Locker locker = indexLockExclusive(txn);
        try {
            int compare = highExtremityCompare(key);

            if (compare >= 0) {
                byte[] result;
                TreeCursor highCursor = mHighCursor;
                highCursor.link(txn);
                try {
                    if (compare == 0) {
                        highCursor.load();
                        result = highCursor.value();
                        highCursor.store(value);
                    } else {
                        if (value != null) {
                            highCursor.insertExtremity(key, value, 2);
                        } else {
                            entryLockExclusive(txn, key);
                        }
                        return null;
                    }
                } finally {
                    highCursor.mTxn = Transaction.BOGUS;
                }
                if (value == null) {
                    highCursor.previous();
                }
                return result;
            }

            compare = lowExtremityCompare(key);

            if (compare <= 0) {
                byte[] result;
                TreeCursor lowCursor = mLowCursor;
                lowCursor.link(txn);
                try {
                    if (compare == 0) {
                        lowCursor.load();
                        result = lowCursor.value();
                        lowCursor.store(value);
                    } else {
                        if (value != null) {
                            lowCursor.insertExtremity(key, value, 0);
                        } else {
                            entryLockExclusive(txn, key);
                        }
                        return null;
                    }
                } finally {
                    lowCursor.mTxn = Transaction.BOGUS;
                }
                if (value == null) {
                    lowCursor.next();
                }
                return result;
            }

            return mSource.exchange(txn, key, value);
        } finally {
            indexUnlock(locker);
        }
    }

    @Override
    public boolean insert(Transaction txn, byte[] key, byte[] value) throws IOException {
        if (value == null) {
            return replace(txn, key, null);
        }

        Locker locker = indexLockExclusive(txn);
        try {
            int compare = highExtremityCompare(key);

            if (compare >= 0) {
                if (compare == 0) {
                    entryLockExclusive(txn, key);
                    return false;
                } else {
                    TreeCursor highCursor = mHighCursor;
                    highCursor.link(txn);
                    try {
                        highCursor.insertExtremity(key, value, 2);
                    } finally {
                        highCursor.mTxn = Transaction.BOGUS;
                    }
                    return true;
                }
            }

            compare = lowExtremityCompare(key);

            if (compare <= 0) {
                if (compare == 0) {
                    entryLockExclusive(txn, key);
                    return false;
                } else {
                    TreeCursor lowCursor = mLowCursor;
                    lowCursor.link(txn);
                    try {
                        lowCursor.insertExtremity(key, value, 0);
                    } finally {
                        lowCursor.mTxn = Transaction.BOGUS;
                    }
                    return true;
                }
            }

            return mSource.insert(txn, key, value);
        } finally {
            indexUnlock(locker);
        }
    }

    @Override
    public boolean replace(Transaction txn, byte[] key, byte[] value) throws IOException {
        Locker locker = indexLockExclusive(txn);
        try {
            int compare = highExtremityCompare(key);

            if (compare >= 0) {
                if (compare == 0) {
                    TreeCursor highCursor = mHighCursor;
                    highCursor.link(txn);
                    try {
                        highCursor.store(value);
                    } finally {
                        highCursor.mTxn = Transaction.BOGUS;
                    }
                    if (value == null) {
                        highCursor.previous();
                    }
                    return true;
                } else {
                    entryLockExclusive(txn, key);
                    return false;
                }
            }

            compare = lowExtremityCompare(key);

            if (compare <= 0) {
                if (compare == 0) {
                    TreeCursor lowCursor = mLowCursor;
                    lowCursor.link(txn);
                    try {
                        lowCursor.store(value);
                    } finally {
                        lowCursor.mTxn = Transaction.BOGUS;
                    }
                    if (value == null) {
                        lowCursor.next();
                    }
                    return true;
                } else {
                    entryLockExclusive(txn, key);
                    return false;
                }
            }

            return mSource.replace(txn, key, value);
        } finally {
            indexUnlock(locker);
        }
    }

    @Override
    public boolean update(Transaction txn, byte[] key, byte[] oldValue, byte[] newValue)
        throws IOException
    {
        if (oldValue == null) {
            return insert(txn, key, newValue);
        }

        Locker locker = indexLockExclusive(txn);
        try {
            int compare = highExtremityCompare(key);

            if (compare >= 0) {
                if (compare == 0) {
                    TreeCursor highCursor = mHighCursor;
                    highCursor.link(txn);
                    try {
                        highCursor.load();
                        if (!Arrays.equals(oldValue, highCursor.value())) {
                            highCursor.mValue = null;
                            return false;
                        }
                        highCursor.store(newValue);
                    } finally {
                        highCursor.mTxn = Transaction.BOGUS;
                    }
                    if (newValue == null) {
                        highCursor.previous();
                    }
                    return true;
                } else {
                    entryLockExclusive(txn, key);
                    return false;
                }
            }

            compare = lowExtremityCompare(key);

            if (compare <= 0) {
                if (compare == 0) {
                    TreeCursor lowCursor = mLowCursor;
                    lowCursor.link(txn);
                    try {
                        lowCursor.load();
                        if (!Arrays.equals(oldValue, lowCursor.value())) {
                            lowCursor.mValue = null;
                            return false;
                        }
                        lowCursor.store(newValue);
                    } finally {
                        lowCursor.mTxn = Transaction.BOGUS;
                    }
                    if (newValue == null) {
                        lowCursor.next();
                    }
                    return true;
                } else {
                    entryLockExclusive(txn, key);
                    return false;
                }
            }

            return mSource.update(txn, key, oldValue, newValue);
        } finally {
            indexUnlock(locker);
        }
    }

    @Override
    public boolean delete(Transaction txn, byte[] key) throws IOException {
        Locker locker = indexLockExclusive(txn);
        try {
            int compare = highExtremityCompare(key);

            if (compare >= 0) {
                if (compare == 0) {
                    TreeCursor highCursor = mHighCursor;
                    highCursor.link(txn);
                    try {
                        highCursor.store(null);
                    } finally {
                        highCursor.mTxn = Transaction.BOGUS;
                    }
                    highCursor.previous();
                    return true;
                } else {
                    entryLockExclusive(txn, key);
                    return false;
                }
            }

            compare = lowExtremityCompare(key);

            if (compare <= 0) {
                if (compare == 0) {
                    TreeCursor lowCursor = mLowCursor;
                    lowCursor.link(txn);
                    try {
                        lowCursor.store(null);
                    } finally {
                        lowCursor.mTxn = Transaction.BOGUS;
                    }
                    lowCursor.next();
                    return true;
                } else {
                    entryLockExclusive(txn, key);
                    return false;
                }
            }

            return mSource.delete(txn, key);
        } finally {
            indexUnlock(locker);
        }
    }

    @Override
    public boolean remove(Transaction txn, byte[] key, byte[] value) throws IOException {
        return update(txn, key, value, null);
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
    public View viewReverse() {
        return new ReverseView(this);
    }

    @Override
    public View viewUnmodifiable() {
        return UnmodifiableView.apply(this);
    }

    @Override
    public boolean isUnmodifiable() {
        return mSource.isUnmodifiable();
    }

    @Override
    public boolean verify(VerificationObserver observer) throws IOException {
        return mSource.verify(observer);
    }

    @Override
    public void close() throws IOException {
        Locker locker = indexLockExclusive(null);
        try {
            mLowCursor.reset();
            mHighCursor.reset();
            mSource.close();
        } finally {
            indexUnlock(locker);
        }
    }

    @Override
    public boolean isClosed() {
        return mSource.isClosed();
    }

    @Override
    public void drop() throws IOException {
        Locker locker = indexLockExclusive(null);
        try {
            mLowCursor.reset();
            mHighCursor.reset();
            mSource.drop();
        } finally {
            indexUnlock(locker);
        }
    }

    void storeInto(TreeCursor cursor, byte[] value) throws IOException {
        byte[] key = cursor.key();
        if (key == null) {
            throw new IllegalStateException("Cursor position is undefined");
        }

        Transaction txn = cursor.mTxn;

        Locker locker = indexLockExclusive(txn);
        try {
            int compare = highExtremityCompare(key);

            if (compare >= 0) {
                TreeCursor highCursor = mHighCursor;
                if (value == null) {
                    if (compare == 0) {
                        highCursor.link(txn);
                        try {
                            highCursor.store(null);
                        } finally {
                            highCursor.mTxn = Transaction.BOGUS;
                        }
                        highCursor.previous();
                    } else {
                        entryLockExclusive(txn, key);
                    }
                } else {
                    highCursor.link(txn);
                    try {
                        if (compare == 0) {
                            highCursor.store(value);
                        } else {
                            highCursor.insertExtremity(key, value, 2);
                        }
                    } finally {
                        highCursor.mTxn = Transaction.BOGUS;
                    }
                }
                return;
            }

            compare = lowExtremityCompare(key);

            if (compare <= 0) {
                TreeCursor lowCursor = mLowCursor;
                if (value == null) {
                    if (compare == 0) {
                        lowCursor.link(txn);
                        try {
                            lowCursor.store(null);
                        } finally {
                            lowCursor.mTxn = Transaction.BOGUS;
                        }
                        lowCursor.next();
                    } else {
                        entryLockExclusive(txn, key);
                    }
                } else {
                    lowCursor.link(txn);
                    try {
                        if (compare == 0) {
                            lowCursor.store(value);
                        } else {
                            lowCursor.insertExtremity(key, value, 0);
                        }
                    } finally {
                        lowCursor.mTxn = Transaction.BOGUS;
                    }
                }
                return;
            }

            cursor.store(value);
        } finally {
            indexUnlock(locker);
        }
    }

    /**
     * @return <0 if lower than extremity, ==0 if equal, >0 if higher
     */
    private int highExtremityCompare(byte[] key) throws IOException {
        TreeCursor highCursor = mHighCursor;
        byte[] high = highCursor.key();
        if (high == null) {
            highCursor.last();
            high = highCursor.key();
            if (high == null) {
                return -1;
            }
        }
        return Utils.compareKeys(key, high);
    }

    /**
     * @return <0 if lower than extremity, ==0 if equal, >0 if higher
     */
    private int lowExtremityCompare(byte[] key) throws IOException {
        TreeCursor lowCursor = mLowCursor;
        byte[] low = lowCursor.key();
        if (low == null) {
            lowCursor.first();
            low = lowCursor.key();
            if (low == null) {
                return 1;
            }
        }
        return Utils.compareKeys(key, low);
    }

    private Locker indexLockExclusive(Transaction txn) throws LockFailureException {
        // Use a full-blown Lock when making updates, enabling deadlock detection.

        LockResult result;
        Locker locker;
        long timeoutNanos;

        acquireExclusive();
        try {
            if (txn == null || txn.lockMode() == LockMode.UNSAFE) {
                LockManager manager = mSource.mLockManager;
                locker = manager.localLocker();
                timeoutNanos = manager.mDefaultTimeoutNanos;
            } else {
                locker = txn;
                timeoutNanos = txn.mLockTimeoutNanos;
            }
            result = mIndexLock.tryLockExclusive(this, locker, timeoutNanos);
        } finally {
            releaseExclusive();
        }

        if (result.isHeld()) {
            return locker;
        }

        throw locker.failed(result, timeoutNanos);
    }

    private void indexUnlock(Locker locker) {
        acquireExclusive();
        try {
            // Second parameter is an entry latch for ghost deletion, but there are no ghosts
            // associated with this lock. Just pass in something, but it won't be used.
            mIndexLock.unlock(locker, this);
        } finally {
            releaseExclusive();
        }
    }

    private void entryLockExclusive(Transaction txn, byte[] key) throws LockFailureException {
        Tree tree = mSource;
        int hash = LockManager.hash(tree.mId, key);
        if (txn == null) {
            // Auto-commit transaction needs to briefly acquire the lock.
            tree.lockExclusiveLocal(key, hash).unlock();
        } else if (txn.lockMode() != LockMode.UNSAFE) {
            txn.lockExclusive(tree.mId, key, hash);
        }
    }
}
