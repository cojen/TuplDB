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

import java.io.IOException;

import java.util.concurrent.TimeUnit;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class ViewUtils {
    /**
     * @throws UnpositionedCursorException if object is null
     */
    static void positionCheck(Object obj) {
        if (obj == null) {
            throw new UnpositionedCursorException("Cursor position is undefined");
        }
    }

    static long count(View view, boolean autoload, byte[] lowKey, byte[] highKey)
        throws IOException
    {
        long count = 0;
            
        Cursor c = view.newCursor(Transaction.BOGUS);
        try {
            c.autoload(autoload);

            if (lowKey == null) {
                c.first();
            } else {
                c.findGe(lowKey);
            }

            if (highKey == null) {
                for (; c.key() != null; c.next()) {
                    count++;
                }
            } else {
                for (; c.key() != null && c.compareKeyTo(highKey) < 0; c.next()) {
                    count++;
                }
            }
        } finally {
            c.reset();
        }

        return count;
    }

    static byte[] appendZero(byte[] key) {
        byte[] newKey = new byte[key.length + 1];
        System.arraycopy(key, 0, newKey, 0, key.length);
        return newKey;
    }

    /**
     * Skip implementation which only locks the last key seen, as per the Cursor.skip contract.
     */
    static LockResult skip(Cursor c, long amount, byte[] limitKey, boolean inclusive)
        throws IOException
    {
        if (amount == 0) {
            return c.skip(amount);
        }

        final boolean auto = c.autoload(false);
        final Transaction txn = c.link(Transaction.BOGUS);
        try {
            if (amount > 0) {
                int cmp = inclusive ? 1 : 0;
                while (true) {
                    c.next();
                    if (c.key() == null) {
                        return LockResult.UNOWNED;
                    }
                    if (limitKey != null && c.compareKeyTo(limitKey) >= cmp) {
                        break;
                    }
                    if (--amount <= 0) {
                        return auto ? c.load() : c.lock();
                    }
                }
            } else {
                int cmp = inclusive ? -1 : 0;
                while (true) {
                    c.previous();
                    if (c.key() == null) {
                        return LockResult.UNOWNED;
                    }
                    if (limitKey != null && c.compareKeyTo(limitKey) <= cmp) {
                        break;
                    }
                    if (++amount >= 0) {
                        return auto ? c.load() : c.lock();
                    }
                }
            }
        } finally {
            c.link(txn);
            c.autoload(auto);
        }

        c.reset();
        return LockResult.UNOWNED;
    }

    /**
     * Skip implementation which locks every key it sees, releasing all but the last one.
     *
     * @throws IllegalArgumentException when skip amount is zero
     */
    static LockResult skipWithLocks(Cursor c, long amount) throws IOException {
        if (amount == 0) {
            return c.skip(amount);
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

    /**
     * Skip implementation which locks every key it sees, releasing all but the last one.
     */
    static LockResult skipWithLocks(Cursor c, long amount, byte[] limitKey, boolean inclusive)
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
     * @param cmp 0 for nextLt, 1 for nextLe
     */
    static LockResult nextCmp(Cursor c, byte[] limitKey, int cmp) throws IOException {
        Utils.keyCheck(limitKey);

        while (true) {
            final boolean auto = c.autoload(false);
            final Transaction txn = c.link(Transaction.BOGUS);
            try {
                c.next();
            } finally {
                c.link(txn);
                c.autoload(auto);
            }

            if (c.key() != null) {
                if (c.compareKeyTo(limitKey) < cmp) {
                    LockResult result = auto ? c.load() : c.lock();
                    if (c.value() != null) {
                        return result;
                    }
                    continue;
                }
                c.reset();
            }

            return LockResult.UNOWNED;
        }
    }

    /**
     * @param cmp 0 for previousGt, -1 for previousGe
     */
    static LockResult previousCmp(Cursor c, byte[] limitKey, int cmp) throws IOException {
        Utils.keyCheck(limitKey);

        while (true) {
            final boolean auto = c.autoload(false);
            final Transaction txn = c.link(Transaction.BOGUS);
            try {
                c.previous();
            } finally {
                c.link(txn);
                c.autoload(auto);
            }

            if (c.key() != null) {
                if (c.compareKeyTo(limitKey) > cmp) {
                    LockResult result = auto ? c.load() : c.lock();
                    if (c.value() != null) {
                        return result;
                    }
                    continue;
                }
                c.reset();
            }

            return LockResult.UNOWNED;
        }
    }

    static void findNoLock(Cursor c, byte[] key) throws IOException {
        final boolean auto = c.autoload(false);
        final Transaction txn = c.link(Transaction.BOGUS);
        try {
            c.find(key);
        } finally {
            c.link(txn);
            c.autoload(auto);
        }
    }

    static void findNearbyNoLock(Cursor c, byte[] key) throws IOException {
        final boolean auto = c.autoload(false);
        final Transaction txn = c.link(Transaction.BOGUS);
        try {
            c.findNearby(key);
        } finally {
            c.link(txn);
            c.autoload(auto);
        }
    }

    static void commit(Cursor c, byte[] value) throws IOException {
        try {
            c.store(value);
        } catch (Throwable e) {
            Transaction txn = c.link();
            if (txn != null) {
                txn.reset(e);
            }
            throw e;
        }

        Transaction txn = c.link();
        if (txn != null && txn != Transaction.BOGUS) {
            txn.commit();
        }
    }

    static byte[] copyValue(byte[] value) {
        return value == Cursor.NOT_LOADED ? value : Utils.cloneArray(value);
    }

    @FunctionalInterface
    static interface LockAction {
        LockResult lock(Transaction txn, byte[] key)
            throws LockFailureException, ViewConstraintException;
    }

    static LockResult tryLock(Transaction txn, byte[] key, long nanosTimeout, LockAction action)
        throws DeadlockException, ViewConstraintException
    {
        final long originalTimeout = txn.lockTimeout(TimeUnit.NANOSECONDS);
        try {
            txn.lockTimeout(nanosTimeout, TimeUnit.NANOSECONDS);
            return action.lock(txn, key);
        } catch (DeadlockException e) {
            throw e;
        } catch (IllegalUpgradeException e) {
            return LockResult.ILLEGAL;
        } catch (LockInterruptedException e) {
            return LockResult.INTERRUPTED;
        } catch (LockFailureException e) {
            return LockResult.TIMED_OUT_LOCK;
        } finally {
            txn.lockTimeout(originalTimeout, TimeUnit.NANOSECONDS);
        }
    }

    static RuntimeException lockCleanup(Throwable e, Transaction txn, LockResult result) {
        if (result.isAcquired()) {
            try {
                txn.unlock();
            } catch (Throwable e2) {
                Utils.suppress(e, e2);
            }
        }
        throw Utils.rethrow(e);
    }

    /**
     * Closes the given resource, suppresses any new exception, and then throws the original
     * exception.
     *
     * @param c optional
     * @param e required
     */
    static RuntimeException fail(AutoCloseable c, Throwable e) {
        if (c != null) {
            try {
                c.close();
            } catch (Throwable e2) {
                Utils.suppress(e, e2);
            }
        }
        throw Utils.rethrow(e);
    }

    static final String toString(Index ix) {
        StringBuilder b = new StringBuilder();
        Utils.appendMiniString(b, ix);
        b.append(" {");
        String nameStr = ix.getNameString();
        if (nameStr != null) {
            b.append("name").append(": ").append(nameStr);
            b.append(", ");
        }
        b.append("id").append(": ").append(ix.getId());
        return b.append('}').toString();
    }
}
