/*
 *  Copyright 2012-2013 Brian S O'Neill
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
     * Counts the number of non-null values.
     *
     * @param txn optional transaction; pass null for {@link
     * LockMode#READ_COMMITTED READ_COMMITTED} locking behavior
     * @throws IllegalArgumentException if transaction belongs to another database instance
     * /
    public long count(Transaction txn) throws IOException;
    */

    /**
     * Counts the number of non-null values within a given range.
     *
     * @param txn optional transaction; pass null for {@link
     * LockMode#READ_COMMITTED READ_COMMITTED} locking behavior
     * @param start key range start; pass null for open range
     * @param end key range end; pass null for open range
     * @throws IllegalArgumentException if transaction belongs to another database instance
     * /
    public long count(Transaction txn,
                      byte[] start, boolean startInclusive,
                      byte[] end, boolean endInclusive)
        throws IOException;
    */

    /**
     * Returns true if an entry exists for the given key.
     *
     * <p>If the entry must be locked, ownership of the key instance is
     * transferred. The key must not be modified after calling this method.
     *
     * @param txn optional transaction; pass null for {@link
     * LockMode#READ_COMMITTED READ_COMMITTED} locking behavior
     * @param key non-null key
     * @return true if non-null value exists for the given key
     * @throws NullPointerException if key is null
     * @throws IllegalArgumentException if transaction belongs to another database instance
     * /
    public boolean exists(Transaction txn, byte[] key) throws IOException;
    */

    /**
     * Returns true if a matching key-value entry exists.
     *
     * <p>If the entry must be locked, ownership of the key instance is
     * transferred. The key must not be modified after calling this method.
     *
     * @param txn optional transaction; pass null for {@link
     * LockMode#READ_COMMITTED READ_COMMITTED} locking behavior
     * @param key non-null key
     * @param value value to compare to, which can be null
     * @return true if entry matches the given key and value
     * @throws NullPointerException if key is null
     * @throws IllegalArgumentException if transaction belongs to another database instance
     * /
    public boolean exists(Transaction txn, byte[] key, byte[] value) throws IOException;
    */

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
    public boolean insert(Transaction txn, byte[] key, byte[] value) throws IOException;

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
    public boolean delete(Transaction txn, byte[] key) throws IOException;

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
    public boolean remove(Transaction txn, byte[] key, byte[] value) throws IOException;

    /**
     * Unconditionally associates a value over a range of keys. The order in
     * which the range is scanned is determined by the order of the start and
     * end keys. The range is scanned in forward order when the start key is
     * less than the end key. When the end key is less then the start key, the
     * range is scanned in reverse from the end to the start.
     *
     * @param txn optional transaction; pass null for auto-commit mode
     * @param start key range start; pass null for open range
     * @param startInclusive true if start key is included in the range
     * @param end key range end; pass null for open range
     * @param endInclusive true if end key is included in the range
     * @param value value to store; pass null to delete
     * @return number of entries scanned and modified
     * @throws IllegalArgumentException if transaction belongs to another database instance
     */
    /*
    public long rangeStore(Transaction txn,
                           byte[] start, boolean startInclusive,
                           byte[] end, boolean endInclusive,
                           byte[] value)
        throws IOException;
    */

    /**
     * Removes all entries. If transaction is null, only entries which can be
     * locked are removed, and each removal is automatically committed.
     *
     * @param txn optional transaction; pass null for auto-commit mode
     */
    //public void clear(Transaction txn) throws IOException;

    /**
     * Removes a range of entries. If transaction is null, only entries which
     * can be locked are removed, and each removal is automatically committed.
     *
     * @param txn optional transaction; pass null for auto-commit mode
     * @param start key range start; pass null for open range
     * @param end key range end; pass null for open range
     * @throws IllegalArgumentException if transaction belongs to another database instance
     */
    /*
    public void clear(Transaction txn,
                      byte[] start, boolean startInclusive,
                      byte[] end, boolean endInclusive)
        throws IOException;
    */

    /**
     * Returns an unopened stream for accessing values in this view.
     */
    public Stream newStream();

    /**
     * Returns a sub-view, backed by this one, whose keys are greater than or
     * equal to the given key. Ownership of the key instance is transferred,
     * and so it must not be modified after calling this method.
     *
     * <p>The returned view will throw a {@link ViewConstraintException} on an attempt to
     * insert a key outside its range.
     *
     * @throws NullPointerException if key is null
     */
    public View viewGe(byte[] key);

    /**
     * Returns a sub-view, backed by this one, whose keys are greater than the
     * given key. Ownership of the key instance is transferred, and so it must
     * not be modified after calling this method.
     *
     * <p>The returned view will throw a {@link ViewConstraintException} on an attempt to
     * insert a key outside its range.
     *
     * @throws NullPointerException if key is null
     */
    public View viewGt(byte[] key);

    /**
     * Returns a sub-view, backed by this one, whose keys are less than or
     * equal to the given key. Ownership of the key instance is transferred,
     * and so it must not be modified after calling this method.
     *
     * <p>The returned view will throw a {@link ViewConstraintException} on an attempt to
     * insert a key outside its range.
     *
     * @throws NullPointerException if key is null
     */
    public View viewLe(byte[] key);

    /**
     * Returns a sub-view, backed by this one, whose keys are less than the
     * given key. Ownership of the key instance is transferred, and so it must
     * not be modified after calling this method.
     *
     * <p>The returned view will throw a {@link ViewConstraintException} on an attempt to
     * insert a key outside its range.
     *
     * @throws NullPointerException if key is null
     */
    public View viewLt(byte[] key);

    /**
     * Returns a sub-view, backed by this one, whose keys start with the given prefix.
     * Ownership of the prefix instance is transferred, and so it must not be modified after
     * calling this method.
     *
     * <p>The returned view will throw a {@link ViewConstraintException} on an attempt to
     * insert a key outside its range.
     *
     * @param trim amount of prefix length to trim from all keys in the view
     * @throws NullPointerException if prefix is null
     * @throws IllegalArgumentException if trim is longer than prefix
     */
    public View viewPrefix(byte[] prefix, int trim);

    /**
     * Returns a view, backed by this one, whose natural order is reversed.
     */
    public View viewReverse();

    /**
     * Returns a view, backed by this one, whose entries cannot be modified. Any attempt to do
     * so causes an {@link UnmodifiableViewException} to be thrown.
     */
    public View viewUnmodifiable();

    /**
     * Returns true if any attempt to modify this view causes an {@link
     * UnmodifiableViewException} to be thrown.
     */
    public boolean isUnmodifiable();
}
