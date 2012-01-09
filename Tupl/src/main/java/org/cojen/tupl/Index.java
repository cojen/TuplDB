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
 * Mapping of keys to values, ordered by key, in lexicographical
 * order. Although Java bytes are signed, they are treated as unsigned for
 * ordering purposes. The natural order of an index cannot be changed.
 *
 * @author Brian S O'Neill
 * @see Database
 */
public interface Index {
    /**
     * @return randomly assigned, unique non-zero identifier for this index
     */
    public long getId();

    /**
     * @return unique user-specified index name
     */
    public byte[] getName();

    /**
     * @param txn optional transaction for Cursor to link to
     * @return a new unpositioned cursor
     */
    public Cursor newCursor(Transaction txn);

    /**
     * Counts the number of non-null values.
     *
     * @param txn optional transaction
     */
    public long count(Transaction txn) throws IOException;

    /**
     * Counts the number of non-null values within a given range.
     *
     * @param txn optional transaction
     * @param start key range start; pass null for open range
     * @param end key range end; pass null for open range
     */
    public long count(Transaction txn,
                      byte[] start, boolean startInclusive,
                      byte[] end, boolean endInclusive)
        throws IOException;

    /**
     * Returns true if an entry exists for the given key.
     *
     * @param txn optional transaction
     * @param key non-null key
     * @return true if non-null value exists for the given key
     * @throws NullPointerException if key is null
     */
    public boolean exists(Transaction txn, byte[] key) throws IOException;

    /**
     * Returns true if a matching key-value entry exists.
     *
     * @param txn optional transaction
     * @param key non-null key
     * @param value value to compare to, which can be null
     * @return true if entry matches given key and value
     * @throws NullPointerException if key is null
     */
    public boolean exists(Transaction txn, byte[] key, byte[] value) throws IOException;

    /**
     * Returns a copy of the value for the given key, or null if no matching
     * entry exists.
     *
     * @param txn optional transaction
     * @param key non-null key
     * @return copy of value, or null if entry doesn't exist
     * @throws NullPointerException if key is null
     */
    public byte[] load(Transaction txn, byte[] key) throws IOException;

    /**
     * Unconditionally associates a value with the given key.
     *
     * @param txn optional transaction
     * @param key non-null key
     * @param value value to store; pass null to delete
     * @throws NullPointerException if key is null
     */
    public void store(Transaction txn, byte[] key, byte[] value) throws IOException;

    /**
     * Associates a value with the given key, unless a corresponding value
     * already exists.
     *
     * @param txn optional transaction
     * @param key non-null key
     * @param value value to insert, which can be null
     * @return false if entry already exists
     * @throws NullPointerException if key is null
     */
    public boolean insert(Transaction txn, byte[] key, byte[] value) throws IOException;

    /**
     * Associates a value with the given key, but only if a corresponding value
     * already exists.
     *
     * @param txn optional transaction
     * @param key non-null key
     * @param value value to insert; pass null to delete
     * @return false if no existing entry
     * @throws NullPointerException if key is null
     */
    public boolean replace(Transaction txn, byte[] key, byte[] value) throws IOException;

    /**
     * Associates a value with the given key, but only if given old value
     * matches.
     *
     * @param txn optional transaction
     * @param key non-null key
     * @param oldValue expected existing value, which can be null
     * @param newValue new value to update to; pass null to delete
     * @return false if existing value doesn't match
     * @throws NullPointerException if key is null
     */
    public boolean update(Transaction txn, byte[] key, byte[] oldValue, byte[] newValue)
        throws IOException;

    /**
     * Unconditionally removes the entry associated with the given key.
     *
     * @param txn optional transaction
     * @param key non-null key
     * @return false if no existing entry
     * @throws NullPointerException if key is null
     */
    public boolean delete(Transaction txn, byte[] key) throws IOException;

    /**
     * Removes the entry associated with the given key, but only if given value
     * matches.
     *
     * @param txn optional transaction
     * @param key non-null key
     * @param value expected existing value, which can be null
     * @return false if existing value doesn't match
     * @throws NullPointerException if key is null
     */
    public boolean remove(Transaction txn, byte[] key, byte[] value) throws IOException;

    /**
     * Removes all entries. If transaction is null, only entries which can be
     * locked are removed, and each removal is automatically committed.
     *
     * @param txn optional transaction
     */
    public void clear(Transaction txn) throws IOException;

    /**
     * Removes a range of entries. If transaction is null, only entries which
     * can be locked are removed, and each removal is automatically committed.
     *
     * @param txn optional transaction
     * @param start key range start; pass null for open range
     * @param end key range end; pass null for open range
     */
    public void clear(Transaction txn,
                      byte[] start, boolean startInclusive,
                      byte[] end, boolean endInclusive)
        throws IOException;
}
