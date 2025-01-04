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

import java.util.Arrays;
import java.util.Comparator;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.views.BoundedView;
import org.cojen.tupl.views.DifferenceView;
import org.cojen.tupl.views.IntersectionView;
import org.cojen.tupl.views.KeyOnlyView;
import org.cojen.tupl.views.ReverseView;
import org.cojen.tupl.views.TransformedView;
import org.cojen.tupl.views.UnionView;
import org.cojen.tupl.views.UnmodifiableView;
import org.cojen.tupl.views.ViewUtils;

/**
 * Mapping of keys to values, in no particular order. Subclasses and
 * implementations may specify an explicit ordering.
 *
 * @author Brian S O'Neill
 * @see Database
 */
public interface View {
    /**
     * Returns the key ordering for this view.
     */
    public Ordering ordering();

    /**
     * Returns a key comparator for the ordering of this view, or null if unordered.
     */
    public default Comparator<byte[]> comparator() {
        return null;
    }

    /**
     * Returns a new cursor over this view.
     *
     * <p>Note that passing null for the transaction doesn't provide cursor stability with
     * respect to updates. Pass an explicit transaction and call {@link Cursor#commit commit}
     * to apply each update.
     *
     * @param txn optional transaction for Cursor to {@link Cursor#link link} to; pass null for
     * auto-commit mode
     * @return a new unpositioned cursor
     * @throws IllegalStateException if transaction belongs to another database instance
     */
    public Cursor newCursor(Transaction txn);

    /**
     * Returns a cursor intended for {@linkplain ValueAccessor accessing} values in chunks,
     * permitting them to be larger than what can fit in main memory. Essentially, this is a
     * convenience method which disables {@link Cursor#autoload(boolean) autoload}, and then
     * positions the cursor at the given key.
     *
     * <p>Note that passing null for the transaction doesn't provide cursor stability with
     * respect to updates. Instead, pass an explicit transaction and call {@link Cursor#commit
     * commit} to apply each update.
     *
     * <p>If the entry must be locked, ownership of the key instance is transferred. The key
     * must not be modified after calling this method.
     *
     * @param txn optional transaction; pass null for auto-commit mode
     * @param key non-null key
     * @return non-null cursor
     * @throws NullPointerException if key is null
     * @throws IllegalStateException if transaction belongs to another database instance
     * @throws ViewConstraintException if key is not allowed
     */
    public default Cursor newAccessor(Transaction txn, byte[] key) throws IOException {
        Cursor c = newCursor(txn);
        try {
            c.autoload(false);
            c.find(key);
            return c;
        } catch (Throwable e) {
            Utils.closeQuietly(c);
            throw e;
        }
    }

    /**
     * Returns a new transaction which is compatible with this view. If the provided durability
     * mode is null, a default mode is selected.
     *
     * @throws UnsupportedOperationException if not supported
     */
    public default Transaction newTransaction(DurabilityMode durabilityMode) {
        throw new UnsupportedOperationException();
    }

    /**
     * Non-transactionally determines if the view has nothing in it. If any transactions are
     * concurrently accessing the view, the implementation might return false even though the
     * count method would have returned zero. A return value of true always guarantees that the
     * count is zero. In other words, false negatives are possible, but false positives aren't.
     */
    public default boolean isEmpty() throws IOException {
        return ViewUtils.isEmpty(this);
    }

    /**
     * Non-transactionally counts the number of entries within the given range. Implementations
     * of this method typically scan over the entries, and so it shouldn't be expected to run
     * in constant time.
     *
     * @param lowKey inclusive lowest key in the counted range; pass null for open range
     * @param highKey exclusive highest key in the counted range; pass null for open range
     */
    public default long count(byte[] lowKey, byte[] highKey) throws IOException {
        return count(lowKey, true, highKey, false);
    }

    /**
     * Non-transactionally counts the number of entries within the given range. Implementations
     * of this method typically scan over the entries, and so it shouldn't be expected to run
     * in constant time.
     *
     * @param lowKey lowest key in the counted range; pass null for open range
     * @param lowInclusive true for inclusive key, false for exclusive, ignored if key is null
     * @param highKey highest key in the counted range; pass null for open range
     * @param highInclusive true for inclusive key, false for exclusive, ignored if key is null
     */
    public default long count(byte[] lowKey, boolean lowInclusive,
                              byte[] highKey, boolean highInclusive)
        throws IOException
    {
        return ViewUtils.count(this, false, lowKey, lowInclusive, highKey, highInclusive ? 1 : 0);
    }

    /**
     * Returns a copy of the value for the given key, or null if no matching entry exists.
     *
     * <p>If the entry must be locked, ownership of the key instance is transferred. The key
     * must not be modified after calling this method.
     *
     * @param txn optional transaction; pass null for {@link LockMode#READ_COMMITTED
     * READ_COMMITTED} locking behavior
     * @param key non-null key
     * @return copy of value, or null if entry doesn't exist
     * @throws NullPointerException if key is null
     * @throws IllegalStateException if transaction belongs to another database instance
     */
    public default byte[] load(Transaction txn, byte[] key) throws IOException {
        Cursor c = newCursor(txn);
        try {
            c.find(key);
            return c.value();
        } finally {
            c.reset();
        }
    }

    /**
     * Checks if an entry for the given key exists. This method should be called only if the
     * value doesn't need to be loaded or stored &mdash; calling exists and then calling a load
     * or store method is typically less efficient than skipping the exists check entirely.
     *
     * <p>If the entry must be locked, ownership of the key instance is transferred. The key
     * must not be modified after calling this method.
     *
     * @param txn optional transaction; pass null for {@link LockMode#READ_COMMITTED
     * READ_COMMITTED} locking behavior
     * @param key non-null key
     * @return true if entry exists
     * @throws NullPointerException if key is null
     * @throws IllegalStateException if transaction belongs to another database instance
     */
    public default boolean exists(Transaction txn, byte[] key) throws IOException {
        return load(txn, key) != null;
    }

    /**
     * Unconditionally associates a value with the given key.
     *
     * <p>If the entry must be locked, ownership of the key instance is transferred. The key
     * must not be modified after calling this method.
     *
     * @param txn optional transaction; pass null for auto-commit mode
     * @param key non-null key
     * @param value value to store; pass null to delete
     * @throws NullPointerException if key is null
     * @throws IllegalStateException if transaction belongs to another database instance
     * @throws ViewConstraintException if entry is not permitted
     */
    public default void store(Transaction txn, byte[] key, byte[] value) throws IOException {
        txn = ViewUtils.enterScope(this, txn);
        Cursor c = newCursor(txn);
        try {
            c.autoload(false);
            c.find(key);
            if (c.key() == null) {
                if (value == null) {
                    return;
                }
                throw new ViewConstraintException();
            }
            c.commit(value);
        } finally {
            txn.exit();
            c.reset();
        }
    }

    /**
     * Unconditionally associates a value with the given key, returning the previous value.
     *
     * <p>If the entry must be locked, ownership of the key instance is transferred. The key
     * must not be modified after calling this method.
     *
     * @param txn optional transaction; pass null for auto-commit mode
     * @param key non-null key
     * @param value value to store; pass null to delete
     * @return copy of previous value, or null if none
     * @throws NullPointerException if key is null
     * @throws IllegalStateException if transaction belongs to another database instance
     * @throws ViewConstraintException if entry is not permitted
     */
    public default byte[] exchange(Transaction txn, byte[] key, byte[] value) throws IOException {
        txn = ViewUtils.enterScope(this, txn);
        Cursor c = newCursor(txn);
        try {
            c.find(key);
            if (c.key() == null) {
                if (value == null) {
                    return null;
                }
                throw new ViewConstraintException();
            }
            // NOTE: Not atomic with BOGUS transaction.
            byte[] old = c.value();
            c.commit(value);
            return old;
        } finally {
            txn.exit();
            c.reset();
        }
    }

    /**
     * Associates a value with the given key, unless a corresponding value already
     * exists. Equivalent to: {@link #update(Transaction, byte[], byte[], byte[]) update(txn,
     * key, null, value)}
     *
     * <p>If the entry must be locked, ownership of the key instance is transferred. The key
     * must not be modified after calling this method.
     *
     * @param txn optional transaction; pass null for auto-commit mode
     * @param key non-null key
     * @param value value to insert, which can be null
     * @return false if entry already exists
     * @throws NullPointerException if key is null
     * @throws IllegalStateException if transaction belongs to another database instance
     * @throws ViewConstraintException if entry is not permitted
     */
    public default boolean insert(Transaction txn, byte[] key, byte[] value) throws IOException {
        return update(txn, key, null, value);
    }

    /**
     * Associates a value with the given key, but only if a corresponding value already exists.
     *
     * <p>If the entry must be locked, ownership of the key instance is transferred. The key
     * must not be modified after calling this method.
     *
     * @param txn optional transaction; pass null for auto-commit mode
     * @param key non-null key
     * @param value value to insert; pass null to delete
     * @return false if no existing entry
     * @throws NullPointerException if key is null
     * @throws IllegalStateException if transaction belongs to another database instance
     * @throws ViewConstraintException if entry is not permitted
     */
    public default boolean replace(Transaction txn, byte[] key, byte[] value) throws IOException {
        txn = ViewUtils.enterScope(this, txn);
        Cursor c = newCursor(txn);
        try {
            c.autoload(false);
            c.find(key);
            if (c.key() == null) {
                throw new ViewConstraintException();
            }
            // NOTE: Not atomic with BOGUS transaction.
            if (c.value() == null) {
                return false;
            }
            c.commit(value);
            return true;
        } finally {
            txn.exit();
            c.reset();
        }
    }

    /**
     * Associates a value with the given key, but only if the given value differs from the
     * existing value.
     *
     * <p>If the entry must be locked, ownership of the key instance is transferred. The key
     * must not be modified after calling this method.
     *
     * @param txn optional transaction; pass null for auto-commit mode
     * @param key non-null key
     * @param value value to update to; pass null to delete
     * @return false if given value matches existing value
     * @throws NullPointerException if key is null
     * @throws IllegalStateException if transaction belongs to another database instance
     * @throws ViewConstraintException if entry is not permitted
     */
    public default boolean update(Transaction txn, byte[] key, byte[] value) throws IOException {
        txn = ViewUtils.enterScope(this, txn);
        Cursor c = newCursor(txn);
        try {
            c.find(key);
            if (c.key() == null) {
                throw new ViewConstraintException();
            }
            // NOTE: Not atomic with BOGUS transaction.
            if (Arrays.equals(c.value(), value)) {
                return false;
            }
            c.commit(value);
            return true;
        } finally {
            txn.exit();
            c.reset();
        }
    }

    /**
     * Associates a value with the given key, but only if the given old value matches the
     * existing value.
     *
     * <p>If the entry must be locked, ownership of the key instance is transferred. The key
     * must not be modified after calling this method.
     *
     * @param txn optional transaction; pass null for auto-commit mode
     * @param key non-null key
     * @param oldValue expected existing value, which can be null
     * @param newValue new value to update to; pass null to delete
     * @return false if existing value doesn't match
     * @throws NullPointerException if key is null
     * @throws IllegalStateException if transaction belongs to another database instance
     * @throws ViewConstraintException if entry is not permitted
     */
    public default boolean update(Transaction txn, byte[] key, byte[] oldValue, byte[] newValue)
        throws IOException
    {
        txn = ViewUtils.enterScope(this, txn);
        Cursor c = newCursor(txn);
        try {
            c.autoload(oldValue != null);
            c.find(key);
            if (c.key() == null) {
                throw new ViewConstraintException();
            }
            // NOTE: Not atomic with BOGUS transaction.
            if (!Arrays.equals(c.value(), oldValue)) {
                return false;
            }
            if (oldValue != null || newValue != null) {
                c.commit(newValue);
            }
            return true;
        } finally {
            txn.exit();
            c.reset();
        }
    }

    /**
     * Unconditionally removes the entry associated with the given key. Equivalent to:
     * {@link #replace replace(txn, key, null)}
     *
     * <p>If the entry must be locked, ownership of the key instance is transferred. The key
     * must not be modified after calling this method.
     *
     * @param txn optional transaction; pass null for auto-commit mode
     * @param key non-null key
     * @return false if no existing entry
     * @throws NullPointerException if key is null
     * @throws IllegalStateException if transaction belongs to another database instance
     * @throws ViewConstraintException if remove is not permitted
     */
    public default boolean delete(Transaction txn, byte[] key) throws IOException {
        return replace(txn, key, null);
    }

    /**
     * Removes the entry associated with the given key, but only if the given value
     * matches. Equivalent to: {@link #update(Transaction, byte[], byte[], byte[]) update(txn,
     * key, value, null)}
     *
     * <p>If the entry must be locked, ownership of the key instance is transferred. The key
     * must not be modified after calling this method.
     *
     * @param txn optional transaction; pass null for auto-commit mode
     * @param key non-null key
     * @param value expected existing value, which can be null
     * @return false if existing value doesn't match
     * @throws NullPointerException if key is null
     * @throws IllegalStateException if transaction belongs to another database instance
     * @throws ViewConstraintException if remove is not permitted
     */
    public default boolean remove(Transaction txn, byte[] key, byte[] value) throws IOException {
        return update(txn, key, value, null);
    }

    /**
     * Touch the given key as if calling {@link #load load}, but instead only acquiring any
     * necessary locks. Method may return {@link LockResult#UNOWNED UNOWNED} if the key isn't
     * supported by this view, or if the transaction {@link LockMode} doesn't retain locks.
     *
     * <p>If the entry must be locked, ownership of the key instance is transferred. The key
     * must not be modified after calling this method.
     *
     * @param txn optional transaction; pass null for {@link LockMode#READ_COMMITTED
     * READ_COMMITTED} locking behavior
     * @param key non-null key
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link LockResult#ACQUIRED ACQUIRED}, {@link
     * LockResult#OWNED_SHARED OWNED_SHARED}, {@link LockResult#OWNED_UPGRADABLE
     * OWNED_UPGRADABLE}, or {@link LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws NullPointerException if key is null
     * @throws IllegalStateException if transaction belongs to another database instance
     */
    public default LockResult touch(Transaction txn, byte[] key) throws LockFailureException {
        // Default implementation isn't that great and should be overridden.

        try {
            LockMode mode;
            if (txn == null) {
                // There's no default way to lock/unlock a key when the transaction is null, so
                // do an existence check and assume that a lock will be acquired and released.
                exists(null, key);
            } else if ((mode = txn.lockMode()) == LockMode.READ_COMMITTED) {
                LockResult result = lockShared(txn, key);
                if (result == LockResult.ACQUIRED) {
                    txn.unlock();
                }
            } else if (!mode.noReadLock) {
                if (mode == LockMode.UPGRADABLE_READ) {
                    return lockUpgradable(txn, key);
                } else {
                    return lockShared(txn, key);
                }
            }
        } catch (LockFailureException e) {
            throw e;
        } catch (IOException e) {
            // Suppress any failure to load or any ViewConstraintException.
        }

        return LockResult.UNOWNED;
    }

    /**
     * Explicitly acquire a shared lock for the given key, denying exclusive locks. Lock is
     * retained until the end of the transaction or scope.
     *
     * <p>Transactions acquire locks automatically, and so use of this method is not
     * required. Ownership of the key instance is transferred, and so the key must not be
     * modified after calling this method.
     *
     * @param key non-null key to lock; instance is not cloned and so it must not be modified
     * after calling this method
     * @param nanosTimeout maximum time to wait for lock; negative timeout is infinite
     * @return {@link LockResult#INTERRUPTED INTERRUPTED}, {@link
     * LockResult#TIMED_OUT_LOCK TIMED_OUT_LOCK}, {@link LockResult#ACQUIRED
     * ACQUIRED}, {@link LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws IllegalStateException if transaction belongs to another database instance
     * @throws IllegalStateException if too many shared locks
     * @throws DeadlockException if deadlock was detected after waiting the full timeout,
     * unless the timeout is zero
     * @throws ViewConstraintException if key is not allowed
     */
    public default LockResult tryLockShared(Transaction txn, byte[] key, long nanosTimeout)
        throws DeadlockException, LockFailureException, ViewConstraintException
    {
        return ViewUtils.tryLock(txn, key, nanosTimeout, this::lockShared);
    }

    /**
     * Explicitly acquire a shared lock for the given key, denying exclusive locks. Lock is
     * retained until the end of the transaction or scope.
     *
     * <p>Transactions acquire locks automatically, and so use of this method is not
     * required. Ownership of the key instance is transferred, and so the key must not be
     * modified after calling this method.
     *
     * @param key non-null key to lock; instance is not cloned and so it must not be modified
     * after calling this method
     * @return {@link LockResult#ACQUIRED ACQUIRED}, {@link LockResult#OWNED_SHARED
     * OWNED_SHARED}, {@link LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws IllegalStateException if transaction belongs to another database instance
     * @throws IllegalStateException if too many shared locks
     * @throws LockFailureException if interrupted or timed out
     * @throws DeadlockException if deadlock was detected after waiting the full timeout
     * @throws ViewConstraintException if key is not allowed
     */
    public LockResult lockShared(Transaction txn, byte[] key)
        throws LockFailureException, ViewConstraintException;

    /**
     * Explicitly acquire an upgradable lock for the given key, denying exclusive and
     * additional upgradable locks. Lock is retained until the end of the transaction or scope.
     *
     * <p>Transactions acquire locks automatically, and so use of this method is not
     * required. Ownership of the key instance is transferred, and so the key must not be
     * modified after calling this method.
     *
     * @param key non-null key to lock; instance is not cloned and so it must not be modified
     * after calling this method
     * @param nanosTimeout maximum time to wait for lock; negative timeout is infinite
     * @return {@link LockResult#ILLEGAL ILLEGAL}, {@link
     * LockResult#INTERRUPTED INTERRUPTED}, {@link LockResult#TIMED_OUT_LOCK
     * TIMED_OUT_LOCK}, {@link LockResult#ACQUIRED ACQUIRED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws IllegalStateException if transaction belongs to another database instance
     * @throws DeadlockException if deadlock was detected after waiting the full timeout,
     * unless the timeout is zero
     * @throws ViewConstraintException if key is not allowed
     */
    public default LockResult tryLockUpgradable(Transaction txn, byte[] key, long nanosTimeout)
        throws DeadlockException, LockFailureException, ViewConstraintException
    {
        return ViewUtils.tryLock(txn, key, nanosTimeout, this::lockUpgradable);
    }

    /**
     * Explicitly acquire an upgradable lock for the given key, denying exclusive and
     * additional upgradable locks. Lock is retained until the end of the transaction or scope.
     *
     * <p>Transactions acquire locks automatically, and so use of this method is not
     * required. Ownership of the key instance is transferred, and so the key must not be
     * modified after calling this method.
     *
     * @param key non-null key to lock; instance is not cloned and so it must not be modified
     * after calling this method
     * @return {@link LockResult#ACQUIRED ACQUIRED}, {@link LockResult#OWNED_UPGRADABLE
     * OWNED_UPGRADABLE}, or {@link LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws IllegalStateException if transaction belongs to another database instance
     * @throws LockFailureException if interrupted, timed out, or illegal upgrade
     * @throws DeadlockException if deadlock was detected after waiting the full timeout
     * @throws ViewConstraintException if key is not allowed
     */
    public LockResult lockUpgradable(Transaction txn, byte[] key)
        throws LockFailureException, ViewConstraintException;

    /**
     * Explicitly acquire an exclusive lock for the given key, denying any additional
     * locks. Lock is retained until the end of the transaction or scope.
     *
     * <p>Transactions acquire locks automatically, and so use of this method is not
     * required. Ownership of the key instance is transferred, and so the key must not be
     * modified after calling this method.
     *
     * @param key non-null key to lock; instance is not cloned and so it must not be modified
     * after calling this method
     * @param nanosTimeout maximum time to wait for lock; negative timeout is infinite
     * @return {@link LockResult#ILLEGAL ILLEGAL}, {@link
     * LockResult#INTERRUPTED INTERRUPTED}, {@link LockResult#TIMED_OUT_LOCK
     * TIMED_OUT_LOCK}, {@link LockResult#ACQUIRED ACQUIRED}, {@link
     * LockResult#UPGRADED UPGRADED}, or {@link LockResult#OWNED_EXCLUSIVE
     * OWNED_EXCLUSIVE}
     * @throws IllegalStateException if transaction belongs to another database instance
     * @throws DeadlockException if deadlock was detected after waiting the full timeout,
     * unless the timeout is zero
     * @throws ViewConstraintException if key is not allowed
     */
    public default LockResult tryLockExclusive(Transaction txn, byte[] key, long nanosTimeout)
        throws DeadlockException, LockFailureException, ViewConstraintException
    {
        return ViewUtils.tryLock(txn, key, nanosTimeout, this::lockExclusive);
    }

    /**
     * Explicitly acquire an exclusive lock for the given key, denying any additional
     * locks. Lock is retained until the end of the transaction or scope.
     *
     * <p>Transactions acquire locks automatically, and so use of this method is not
     * required. Ownership of the key instance is transferred, and so the key must not be
     * modified after calling this method.
     *
     * @param key non-null key to lock; instance is not cloned and so it must not be modified
     * after calling this method
     * @return {@link LockResult#ACQUIRED ACQUIRED}, {@link LockResult#UPGRADED UPGRADED}, or
     * {@link LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws IllegalStateException if transaction belongs to another database instance
     * @throws LockFailureException if interrupted, timed out, or illegal upgrade
     * @throws DeadlockException if deadlock was detected after waiting the full timeout
     * @throws ViewConstraintException if key is not allowed
     */
    public LockResult lockExclusive(Transaction txn, byte[] key)
        throws LockFailureException, ViewConstraintException;

    /**
     * Checks the lock ownership for the given key.
     *
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link LockResult#OWNED_SHARED
     * OWNED_SHARED}, {@link LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws IllegalStateException if transaction belongs to another database instance
     * @throws ViewConstraintException if key is not allowed
     */
    public LockResult lockCheck(Transaction txn, byte[] key) throws ViewConstraintException;

    /**
     * Returns a sub-view, backed by this one, whose keys are greater than or
     * equal to the given key. Ownership of the key instance is transferred,
     * and so it must not be modified after calling this method.
     *
     * <p>The returned view will throw a {@link ViewConstraintException} on an attempt to
     * insert a key outside its range.
     *
     * @throws UnsupportedOperationException if view is unordered
     * @throws NullPointerException if key is null
     */
    public default View viewGe(byte[] key) {
        return switch (ordering()) {
            case ASCENDING, ASCENDING_NL -> BoundedView.viewGe(this, key);
            case DESCENDING, DESCENDING_NL -> BoundedView.viewGe(viewReverse(), key).viewReverse();
            default -> throw unsupportedOrdering();
        };
    }

    /**
     * Returns a sub-view, backed by this one, whose keys are greater than the
     * given key. Ownership of the key instance is transferred, and so it must
     * not be modified after calling this method.
     *
     * <p>The returned view will throw a {@link ViewConstraintException} on an attempt to
     * insert a key outside its range.
     *
     * @throws UnsupportedOperationException if view is unordered
     * @throws NullPointerException if key is null
     */
    public default View viewGt(byte[] key) {
        return switch (ordering()) {
            case ASCENDING, ASCENDING_NL -> BoundedView.viewGt(this, key);
            case DESCENDING, DESCENDING_NL -> BoundedView.viewGt(viewReverse(), key).viewReverse();
            default -> throw unsupportedOrdering();
        };
    }

    /**
     * Returns a sub-view, backed by this one, whose keys are less than or
     * equal to the given key. Ownership of the key instance is transferred,
     * and so it must not be modified after calling this method.
     *
     * <p>The returned view will throw a {@link ViewConstraintException} on an attempt to
     * insert a key outside its range.
     *
     * @throws UnsupportedOperationException if view is unordered
     * @throws NullPointerException if key is null
     */
    public default View viewLe(byte[] key) {
        return switch (ordering()) {
            case ASCENDING, ASCENDING_NL -> BoundedView.viewLe(this, key);
            case DESCENDING, DESCENDING_NL -> BoundedView.viewLe(viewReverse(), key).viewReverse();
            default -> throw unsupportedOrdering();
        };
    }

    /**
     * Returns a sub-view, backed by this one, whose keys are less than the
     * given key. Ownership of the key instance is transferred, and so it must
     * not be modified after calling this method.
     *
     * <p>The returned view will throw a {@link ViewConstraintException} on an attempt to
     * insert a key outside its range.
     *
     * @throws UnsupportedOperationException if view is unordered
     * @throws NullPointerException if key is null
     */
    public default View viewLt(byte[] key) {
        return switch (ordering()) {
            case ASCENDING, ASCENDING_NL -> BoundedView.viewLt(this, key);
            case DESCENDING, DESCENDING_NL -> BoundedView.viewLt(viewReverse(), key).viewReverse();
            default -> throw unsupportedOrdering();
        };
    }

    /**
     * Returns a sub-view, backed by this one, whose keys start with the given prefix.
     * Ownership of the prefix instance is transferred, and so it must not be modified after
     * calling this method.
     *
     * <p>The returned view will throw a {@link ViewConstraintException} on an attempt to
     * insert a key outside its range.
     *
     * @param trim amount of prefix length to trim from all keys in the view
     * @throws UnsupportedOperationException if view is unordered
     * @throws NullPointerException if prefix is null
     * @throws IllegalArgumentException if trim is longer than prefix
     */
    public default View viewPrefix(byte[] prefix, int trim) {
        return switch (ordering()) {
            case ASCENDING, ASCENDING_NL ->
                BoundedView.viewPrefix(this, prefix, trim);
            case DESCENDING, DESCENDING_NL ->
                BoundedView.viewPrefix(viewReverse(), prefix, trim).viewReverse();
            default -> throw unsupportedOrdering();
        };
    }

    private UnsupportedOperationException unsupportedOrdering() {
        return new UnsupportedOperationException("Unsupported ordering: " + ordering());
    }

    /**
     * Returns a sub-view, backed by this one, whose entries have been filtered out and
     * transformed.
     *
     * <p>The returned view will throw a {@link ViewConstraintException} on an attempt to
     * insert an entry not supported by the transformer.
     *
     * @throws NullPointerException if transformer is null
     */
    public default View viewTransformed(Transformer transformer) {
        return TransformedView.apply(this, transformer);
    }

    /**
     * Returns a sub-view, backed by this one, whose entries have been filtered out.
     *
     * <p>The returned view will throw a {@link ViewConstraintException} on an attempt to
     * insert an entry not supported by the filter.
     *
     * @throws NullPointerException if filter is null
     */
    public default View viewFiltered(Filter filter) {
        return viewTransformed(filter);
    }

    /**
     * Returns a view which represents the <a
     * href="https://en.wikipedia.org/wiki/Union_(set_theory)"><cite>set union</cite></a> of
     * this view and a second one. A union eliminates duplicate keys, by relying on a combiner
     * to decide how to deal with them. If the combiner chooses to {@link Combiner#discard
     * discard} duplicate keys, then the returned view represents the <a
     * href="https://en.wikipedia.org/wiki/Symmetric_difference"><cite>symmetric set
     * difference</cite></a> instead.
     *
     * <p>Storing entries in the union is permitted, if the combiner supports {@linkplain
     * Combiner#separate separation}. The separator must supply at least one non-null value, or
     * else a {@link ViewConstraintException} will be thrown.
     *
     * @param combiner combines common entries together; pass null to always choose the {@link
     * Combiner#first first}
     * @param second required second view in the union
     * @throws NullPointerException if second view is null
     * @throws IllegalArgumentException if the views don't define a consistent ordering, as
     * specified by their comparators
     */
    public default View viewUnion(Combiner combiner, View second) {
        if (combiner == null) {
            combiner = Combiner.first();
        }
        return new UnionView(combiner, this, second);
    }

    /**
     * Returns a view which represents the <a
     * href="https://en.wikipedia.org/wiki/Intersection_(set_theory)"><cite>set
     * intersection</cite></a> of this view and a second one. An intersection eliminates
     * duplicate keys, by relying on a combiner to decide how to deal with them.
     *
     * <p>Storing entries in the intersection is permitted, if the combiner supports
     * {@linkplain Combiner#separate separation}. The separator must supply two non-null
     * values, or else a {@link ViewConstraintException} will be thrown.
     *
     * @param combiner combines common entries together; pass null to always choose the {@link
     * Combiner#first first}
     * @param second required second view in the intersection
     * @throws NullPointerException if second view is null
     * @throws IllegalArgumentException if the views don't define a consistent ordering, as
     * specified by their comparators
     */
    public default View viewIntersection(Combiner combiner, View second) {
        if (combiner == null) {
            combiner = Combiner.first();
        }
        return new IntersectionView(combiner, this, second);
    }

    /**
     * Returns a view which represents the <a
     * href="https://en.wikipedia.org/wiki/Complement_(set_theory)#Relative_complement"><cite>set
     * difference</cite></a> of this view and a second one. A difference eliminates duplicate
     * keys, by relying on a combiner to decide how to deal with them.
     *
     * <p>Storing entries in the difference is permitted, if the combiner supports {@link
     * Combiner#separate separation}.  The separator must supply a non-null first value, or
     * else a {@link ViewConstraintException} will be thrown.
     *
     * @param combiner combines common entries together; pass null to always {@link
     * Combiner#discard discard} them
     * @param second required second view in the difference
     * @throws NullPointerException if second view is null
     * @throws IllegalArgumentException if the views don't define a consistent ordering, as
     * specified by their comparators
     */
    public default View viewDifference(Combiner combiner, View second) {
        if (combiner == null) {
            combiner = Combiner.discard();
        }
        return new DifferenceView(combiner, this, second);
    }

    /**
     * Returns a view, backed by this one, which only provides the keys. Values are always
     * represented as {@link Cursor#NOT_LOADED NOT_LOADED}, and attempting to store a value
     * other than null causes a {@link ViewConstraintException} to be thrown.
     */
    public default View viewKeys() {
        return new KeyOnlyView(this);
    }

    /**
     * Returns a view, backed by this one, whose natural order is reversed.
     */
    public default View viewReverse() {
        return new ReverseView(this);
    }

    /**
     * Returns a view, backed by this one, whose entries cannot be modified. Any attempt to do
     * so causes an {@link UnmodifiableViewException} to be thrown.
     */
    public default View viewUnmodifiable() {
        return UnmodifiableView.apply(this);
    }

    /**
     * Returns true if any attempt to modify this view causes an {@link
     * UnmodifiableViewException} to be thrown.
     */
    public boolean isUnmodifiable();

    /**
     * Returns true if the value-modifying methods of this view are atomic, even when not using
     * transactions.
     */
    public default boolean isModifyAtomic() {
        return false;
    }
}
