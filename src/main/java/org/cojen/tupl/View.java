/*
 *  Copyright 2012-2015 Cojen.org
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
    public Ordering getOrdering();

    /**
     * @param txn optional transaction for Cursor to {@link Cursor#link link} to
     * @return a new unpositioned cursor
     * @throws IllegalArgumentException if transaction belongs to another database instance
     */
    public Cursor newCursor(Transaction txn);

    /**
     * Non-transactionally counts the number of entries within the given range. Implementations
     * of this method typically scan over the entries, and so it shouldn't be expected to run
     * in constant time.
     *
     * @param lowKey inclusive lowest key in the counted range; pass null for open range
     * @param highKey exclusive highest key in the counted range; pass null for open range
     */
    public default long count(byte[] lowKey, byte[] highKey) throws IOException {
        return ViewUtils.count(this, false, lowKey, highKey);
    }

    /**
     * Returns a copy of the value for the given key, or null if no matching
     * entry exists.
     *
     * <p>If the entry must be locked, ownership of the key instance is
     * transferred. The key must not be modified after calling this method.
     *
     * @param txn optional transaction; pass null for {@link
     * LockMode#READ_COMMITTED READ_COMMITTED} locking behavior
     * @param key non-null key
     * @return copy of value, or null if entry doesn't exist
     * @throws NullPointerException if key is null
     * @throws IllegalArgumentException if transaction belongs to another database instance
     */
    public byte[] load(Transaction txn, byte[] key) throws IOException;

    /**
     * Unconditionally associates a value with the given key.
     *
     * <p>If the entry must be locked, ownership of the key instance is
     * transferred. The key must not be modified after calling this method.
     *
     * @param txn optional transaction; pass null for auto-commit mode
     * @param key non-null key
     * @param value value to store; pass null to delete
     * @throws NullPointerException if key is null
     * @throws IllegalArgumentException if transaction belongs to another database instance
     * @throws ViewConstraintException if entry is not permitted
     */
    public void store(Transaction txn, byte[] key, byte[] value) throws IOException;

    /**
     * Unconditionally associates a value with the given key, returning the previous value.
     *
     * <p>If the entry must be locked, ownership of the key instance is
     * transferred. The key must not be modified after calling this method.
     *
     * @param txn optional transaction; pass null for auto-commit mode
     * @param key non-null key
     * @param value value to store; pass null to delete
     * @return copy of previous value, or null if none
     * @throws NullPointerException if key is null
     * @throws IllegalArgumentException if transaction belongs to another database instance
     * @throws ViewConstraintException if entry is not permitted
     */
    public byte[] exchange(Transaction txn, byte[] key, byte[] value) throws IOException;

    /**
     * Associates a value with the given key, unless a corresponding value
     * already exists. Equivalent to: <code>update(txn, key, null,
     * value)</code>
     *
     * <p>If the entry must be locked, ownership of the key instance is
     * transferred. The key must not be modified after calling this method.
     *
     * @param txn optional transaction; pass null for auto-commit mode
     * @param key non-null key
     * @param value value to insert, which can be null
     * @return false if entry already exists
     * @throws NullPointerException if key is null
     * @throws IllegalArgumentException if transaction belongs to another database instance
     * @throws ViewConstraintException if entry is not permitted
     */
    public default boolean insert(Transaction txn, byte[] key, byte[] value) throws IOException {
        return update(txn, key, null, value);
    }

    /**
     * Associates a value with the given key, but only if a corresponding value
     * already exists.
     *
     * <p>If the entry must be locked, ownership of the key instance is
     * transferred. The key must not be modified after calling this method.
     *
     * @param txn optional transaction; pass null for auto-commit mode
     * @param key non-null key
     * @param value value to insert; pass null to delete
     * @return false if no existing entry
     * @throws NullPointerException if key is null
     * @throws IllegalArgumentException if transaction belongs to another database instance
     * @throws ViewConstraintException if entry is not permitted
     */
    public boolean replace(Transaction txn, byte[] key, byte[] value) throws IOException;

    /**
     * Associates a value with the given key, but only if the given old value
     * matches.
     *
     * <p>If the entry must be locked, ownership of the key instance is
     * transferred. The key must not be modified after calling this method.
     *
     * @param txn optional transaction; pass null for auto-commit mode
     * @param key non-null key
     * @param oldValue expected existing value, which can be null
     * @param newValue new value to update to; pass null to delete
     * @return false if existing value doesn't match
     * @throws NullPointerException if key is null
     * @throws IllegalArgumentException if transaction belongs to another database instance
     * @throws ViewConstraintException if entry is not permitted
     */
    public boolean update(Transaction txn, byte[] key, byte[] oldValue, byte[] newValue)
        throws IOException;

    /**
     * Unconditionally removes the entry associated with the given
     * key. Equivalent to: <code>replace(txn, key, null)</code>
     *
     * <p>If the entry must be locked, ownership of the key instance is
     * transferred. The key must not be modified after calling this method.
     *
     * @param txn optional transaction; pass null for auto-commit mode
     * @param key non-null key
     * @return false if no existing entry
     * @throws NullPointerException if key is null
     * @throws IllegalArgumentException if transaction belongs to another database instance
     * @throws ViewConstraintException if remove is not permitted
     */
    public default boolean delete(Transaction txn, byte[] key) throws IOException {
        return replace(txn, key, null);
    }

    /**
     * Removes the entry associated with the given key, but only if the given
     * value matches. Equivalent to: <code>update(txn, key, value, null)</code>
     *
     * <p>If the entry must be locked, ownership of the key instance is
     * transferred. The key must not be modified after calling this method.
     *
     * @param txn optional transaction; pass null for auto-commit mode
     * @param key non-null key
     * @param value expected existing value, which can be null
     * @return false if existing value doesn't match
     * @throws NullPointerException if key is null
     * @throws IllegalArgumentException if transaction belongs to another database instance
     * @throws ViewConstraintException if remove is not permitted
     */
    public default boolean remove(Transaction txn, byte[] key, byte[] value) throws IOException {
        return update(txn, key, value, null);
    }

    /**
     * Explicitly acquire a shared lock for the given key, denying exclusive locks. Lock is
     * retained until the end of the transaction or scope.
     *
     * <p>Transactions acquire locks automatically, and so use of this method is not
     * required. Ownership of the key instance is transferred, and so the key must not be
     * modified after calling this method.
     *
     * @param key non-null key to lock; instance is not cloned
     * @return {@link LockResult#ACQUIRED ACQUIRED}, {@link LockResult#OWNED_SHARED
     * OWNED_SHARED}, {@link LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws IllegalStateException if too many shared locks
     * @throws LockFailureException if interrupted or timed out
     * @throws DeadlockException if deadlock was detected after waiting full timeout
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
     * @param key non-null key to lock; instance is not cloned
     * @return {@link LockResult#ACQUIRED ACQUIRED}, {@link LockResult#OWNED_UPGRADABLE
     * OWNED_UPGRADABLE}, or {@link LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws LockFailureException if interrupted, timed out, or illegal upgrade
     * @throws DeadlockException if deadlock was detected after waiting full timeout
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
     * @param key non-null key to lock; instance is not cloned
     * @return {@link LockResult#ACQUIRED ACQUIRED}, {@link LockResult#UPGRADED UPGRADED}, or
     * {@link LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws LockFailureException if interrupted, timed out, or illegal upgrade
     * @throws DeadlockException if deadlock was detected after waiting full timeout
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
     * @throws ViewConstraintException if key is not allowed
     */
    public LockResult lockCheck(Transaction txn, byte[] key) throws ViewConstraintException;

    /**
     * Returns an unopened stream for accessing values in this view.
     */
    /*
    public default Stream newStream() {
        throw new UnsupportedOperationException();
    }
    */

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
        return BoundedView.viewGe(ViewUtils.checkOrdering(this), key);
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
        return BoundedView.viewGt(ViewUtils.checkOrdering(this), key);
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
        return BoundedView.viewLe(ViewUtils.checkOrdering(this), key);
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
        return BoundedView.viewLt(ViewUtils.checkOrdering(this), key);
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
        return BoundedView.viewPrefix(ViewUtils.checkOrdering(this), prefix, trim);
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
}
