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

import org.cojen.tupl.Utils;

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
        return Utils.compareUnsigned(lkey, 0, lkey.length, rkey, 0, rkey.length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareKeyTo(byte[] rkey, int offset, int length) {
        byte[] lkey = key();
        return Utils.compareUnsigned(lkey, 0, lkey.length, rkey, offset, length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LockResult skip(long amount, byte[] limitKey, boolean inclusive) throws IOException {
        return skip(this, amount, limitKey, inclusive);
    }

    static LockResult skip(Cursor c, long amount, byte[] limitKey, boolean inclusive)
        throws IOException
    {
        if (amount == 0 || limitKey == null) {
            return c.skip(amount);
        }

        if (amount > 0) {
            if (inclusive) while (true) {
                LockResult result = c.nextLe(limitKey);
                if (c.key() == null || --amount <= 0) {
                    return result;
                }
                if (result == LockResult.ACQUIRED) {
                    c.link().unlock();
                }
            } else while (true) {
                LockResult result = c.nextLt(limitKey);
                if (c.key() == null || --amount <= 0) {
                    return result;
                }
                if (result == LockResult.ACQUIRED) {
                    c.link().unlock();
                }
            }
        } else {
            if (inclusive) while (true) {
                LockResult result = c.previousGe(limitKey);
                if (c.key() == null || ++amount >= 0) {
                    return result;
                }
                if (result == LockResult.ACQUIRED) {
                    c.link().unlock();
                }
            } else while (true) {
                LockResult result = c.previousGt(limitKey);
                if (c.key() == null || ++amount >= 0) {
                    return result;
                }
                if (result == LockResult.ACQUIRED) {
                    c.link().unlock();
                }
            }
        }
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

    /**
     * Simple skip implementation which doesn't support skip by zero.
     *
     * @throws IllegalArgumentException when skip amount is zero
     */
    protected LockResult doSkip(long amount) throws IOException {
        return doSkip(this, amount);
    }

    static LockResult doSkip(Cursor c, long amount) throws IOException {
        if (amount == 0) {
            throw new IllegalArgumentException("Skip is zero");
        }

        if (amount > 0) while (true) {
            LockResult result = c.next();
            if (c.key() == null || --amount <= 0) {
                return result;
            }
            if (result == LockResult.ACQUIRED) {
                c.link().unlock();
            }
        } else while (true) {
            LockResult result = c.previous();
            if (c.key() == null || ++amount >= 0) {
                return result;
            }
            if (result == LockResult.ACQUIRED) {
                c.link().unlock();
            }
        }
    }
}
