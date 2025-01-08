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

package org.cojen.tupl.views;

import java.io.IOException;

import java.util.concurrent.TimeUnit;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.DeadlockException;
import org.cojen.tupl.IllegalUpgradeException;
import org.cojen.tupl.Index;
import org.cojen.tupl.LockFailureException;
import org.cojen.tupl.LockInterruptedException;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.View;
import org.cojen.tupl.ViewConstraintException;
import org.cojen.tupl.UnpositionedCursorException;

import org.cojen.tupl.core.Utils;

/**
 * Collection of static utility methods for acting on Views.
 *
 * @author Brian S O'Neill
 */
public class ViewUtils {
    /**
     * @throws UnpositionedCursorException if object is null
     */
    public static void positionCheck(Object obj) {
        if (obj == null) {
            throw new UnpositionedCursorException("Cursor position is undefined");
        }
    }

    public static boolean isEmpty(View view) throws IOException {
        Cursor c = view.newCursor(Transaction.BOGUS);
        try {
            c.autoload(false);
            c.first();
            return c.key() == null;
        } finally {
            c.reset();
        }
    }

    public static long count(View view, boolean autoload, byte[] lowKey, byte[] highKey)
        throws IOException
    {
        return count(view, autoload, lowKey, true, highKey, 0);
    }

    /**
     * @param highInclusive 1 for inclusive, 0 for exclusive
     */
    public static long count(View view, boolean autoload,
                             byte[] lowKey, boolean lowInclusive,
                             byte[] highKey, int highInclusive)
        throws IOException
    {
        long count = 0;
            
        Cursor c = view.newCursor(Transaction.BOGUS);
        try {
            c.autoload(autoload);

            if (lowKey == null) {
                c.first();
            } else if (lowInclusive) {
                c.findGe(lowKey);
            } else {
                c.findGt(lowKey);
            }

            if (highKey == null) {
                for (; c.key() != null; c.next()) {
                    count++;
                }
            } else {
                for (; c.key() != null && c.compareKeyTo(highKey) < highInclusive; c.next()) {
                    count++;
                }
            }
        } finally {
            c.reset();
        }

        return count;
    }

    public static byte[] appendZero(byte[] key) {
        var newKey = new byte[key.length + 1];
        System.arraycopy(key, 0, newKey, 0, key.length);
        return newKey;
    }

    /**
     * Skip implementation which only locks the last key seen, as per the Cursor.skip contract.
     */
    public static LockResult skip(Cursor c, long amount, byte[] limitKey, boolean inclusive)
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
    public static LockResult nextCmp(Cursor c, byte[] limitKey, int cmp) throws IOException {
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
    public static LockResult previousCmp(Cursor c, byte[] limitKey, int cmp) throws IOException {
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

    public static void findNoLock(Cursor c, byte[] key) throws IOException {
        final boolean auto = c.autoload(false);
        final Transaction txn = c.link(Transaction.BOGUS);
        try {
            c.find(key);
        } finally {
            c.link(txn);
            c.autoload(auto);
        }
    }

    public static void findNearbyNoLock(Cursor c, byte[] key) throws IOException {
        final boolean auto = c.autoload(false);
        final Transaction txn = c.link(Transaction.BOGUS);
        try {
            c.findNearby(key);
        } finally {
            c.link(txn);
            c.autoload(auto);
        }
    }

    /**
     * Returns a new transaction or enters a scope.
     */
    public static Transaction enterScope(Table table, Transaction txn) throws IOException {
        if (txn == null) {
            txn = table.newTransaction(null);
        } else {
            txn.enter();
        }
        return txn;
    }

    /**
     * Returns a new transaction or enters a scope.
     */
    public static Transaction enterScope(View view, Transaction txn) throws IOException {
        if (txn == null) {
            txn = view.newTransaction(null);
        } else {
            txn.enter();
        }
        return txn;
    }

    /**
     * Returns a new transaction or enters a scope. This variant throws an exception when given
     * a bogus transaction.
     */
    public static Transaction enterScopex(View view, Transaction txn) throws IOException {
        if (txn == null) {
            txn = view.newTransaction(null);
        } else {
            txn.enter();
        }
        return txn;
    }

    public static void commit(Cursor c, byte[] value) throws IOException {
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
        if (txn != null) {
            txn.commit();
        }
    }

    public static byte[] copyValue(byte[] value) {
        return value == Cursor.NOT_LOADED ? value : Utils.cloneArray(value);
    }

    public static boolean step(Cursor c) throws IOException {
        try {
            c.next();
            return c.key() != null;
        } catch (UnpositionedCursorException e) {
            return false;
        } catch (Throwable e) {
            throw Utils.fail(c, e);
        }
    }

    @FunctionalInterface
    public static interface LockAction {
        LockResult lock(Transaction txn, byte[] key)
            throws LockFailureException, ViewConstraintException;
    }

    public static LockResult tryLock(Transaction txn, byte[] key, long nanosTimeout,
                                     LockAction action)
        throws DeadlockException, ViewConstraintException
    {
        final long originalTimeout = txn.lockTimeout(TimeUnit.NANOSECONDS);
        try {
            txn.lockTimeout(nanosTimeout, TimeUnit.NANOSECONDS);
            return action.lock(txn, key);
        } catch (DeadlockException e) {
            if (nanosTimeout == 0) {
                // Do what the spec says.
                return LockResult.TIMED_OUT_LOCK;
            }
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
     * Returns the lowest common owned level.
     *
     * {@literal UNOWNED < OWNED_SHARED < OWNED_UPGRADABLE < OWNED_EXCLUSIVE}
     */
    public static LockResult commonOwned(LockResult a, LockResult b) {
        if (a == LockResult.UNOWNED) {
            return a;
        } else if (a == LockResult.OWNED_SHARED) {
            return b == LockResult.UNOWNED ? b : a;
        } else if (a == LockResult.OWNED_UPGRADABLE) {
            return (b == LockResult.UNOWNED || b == LockResult.OWNED_SHARED) ? b : a;
        } else if (a == LockResult.OWNED_EXCLUSIVE) {
            return b;
        } else {
            throw new IllegalArgumentException();
        }
    }

    public static String toString(Index ix) {
        var b = new StringBuilder();
        Utils.appendMiniString(b, ix);
        b.append('{');
        String nameStr = ix.nameString();
        if (nameStr != null) {
            b.append("name").append(": ").append(nameStr);
            b.append(", ");
        }
        b.append("id").append(": ").append(ix.id());
        return b.append('}').toString();
    }
}
