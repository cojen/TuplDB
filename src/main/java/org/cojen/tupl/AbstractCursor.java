/*
 *  Copyright 2015 Brian S O'Neill
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

import static org.cojen.tupl.Utils.compareKeys;

/**
 * Implements a few {@link Cursor} methods.
 *
 * @author Brian S O'Neill
 */
public abstract class AbstractCursor implements Cursor {
    /**
     * {@inheritDoc}
     */
    @Override
    public int compareKeyTo(byte[] rkey) {
        byte[] lkey = key();
        return compareKeys(lkey, 0, lkey.length, rkey, 0, rkey.length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareKeyTo(byte[] rkey, int offset, int length) {
        byte[] lkey = key();
        return compareKeys(lkey, 0, lkey.length, rkey, offset, length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LockResult skip(long amount, byte[] limitKey, boolean inclusive) throws IOException {
        if (amount == 0 || limitKey == null) {
            return skip(amount);
        }

        final Transaction txn = link(Transaction.BOGUS);
        final boolean autoload = autoload(false);

        try {
            if (amount > 0) {
                final int cmp = inclusive ? 0 : -1;
                do {
                    next();
                    byte[] key = key();
                    if (key == null) {
                        return LockResult.UNOWNED;
                    }
                    if (compareKeys(key, 0, key.length, limitKey, 0, limitKey.length) > cmp) {
                        reset();
                        return LockResult.UNOWNED;
                    }
                } while (--amount > 0);
            } else {
                final int cmp = inclusive ? 0 : 1;
                do {
                    previous();
                    byte[] key = key();
                    if (key == null) {
                        return LockResult.UNOWNED;
                    }
                    if (compareKeys(key, 0, key.length, limitKey, 0, limitKey.length) < cmp) {
                        reset();
                        return LockResult.UNOWNED;
                    }
                } while (++amount < 0);
            }
        } finally {
            autoload(autoload);
            link(txn);
        }

        // This performs any required lock acquisition.
        return load();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void commit(byte[] value) throws IOException {
        store(value);
        Transaction txn = link();
        if (txn != null && txn != Transaction.BOGUS) {
            txn.commit();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream newStream() {
        throw new UnsupportedOperationException();
    }
}
