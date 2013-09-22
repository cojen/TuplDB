/*
 *  Copyright 2011-2013 Brian S O'Neill
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

import java.io.Closeable;
import java.io.IOException;

/**
 * Mapping of keys to values, ordered by key, in lexicographical
 * order. Although Java bytes are signed, they are treated as unsigned for
 * ordering purposes. The natural order of an index cannot be changed.
 *
 * @author Brian S O'Neill
 * @see Database
 */
public interface Index extends View, Closeable {
    /**
     * @return randomly assigned, unique non-zero identifier for this index
     */
    public long getId();

    /**
     * @return unique user-specified index name
     */
    public byte[] getName();

    /**
     * @return name decoded as UTF-8
     */
    public String getNameString();

    /**
     * {@inheritDoc}
     */
    @Override
    public Ordering getOrdering();

    /**
     * {@inheritDoc}
     *
     * @throws IllegalArgumentException {@inheritDoc}
     */
    @Override
    public Cursor newCursor(Transaction txn);

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException {@inheritDoc}
     * @throws IllegalArgumentException {@inheritDoc}
     */
    @Override
    public byte[] load(Transaction txn, byte[] key) throws IOException;

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException {@inheritDoc}
     * @throws IllegalArgumentException {@inheritDoc}
     */
    @Override
    public void store(Transaction txn, byte[] key, byte[] value) throws IOException;

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException {@inheritDoc}
     * @throws IllegalArgumentException {@inheritDoc}
     */
    @Override
    public byte[] exchange(Transaction txn, byte[] key, byte[] value) throws IOException;

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException {@inheritDoc}
     * @throws IllegalArgumentException {@inheritDoc}
     */
    @Override
    public boolean insert(Transaction txn, byte[] key, byte[] value) throws IOException;

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException {@inheritDoc}
     * @throws IllegalArgumentException {@inheritDoc}
     */
    @Override
    public boolean replace(Transaction txn, byte[] key, byte[] value) throws IOException;

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException {@inheritDoc}
     * @throws IllegalArgumentException {@inheritDoc}
     */
    @Override
    public boolean update(Transaction txn, byte[] key, byte[] oldValue, byte[] newValue)
        throws IOException;

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException {@inheritDoc}
     * @throws IllegalArgumentException {@inheritDoc}
     */
    @Override
    public boolean delete(Transaction txn, byte[] key) throws IOException;

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException {@inheritDoc}
     * @throws IllegalArgumentException {@inheritDoc}
     */
    @Override
    public boolean remove(Transaction txn, byte[] key, byte[] value) throws IOException;

    /**
     * {@inheritDoc}
     */
    @Override
    /*public*/ Stream newStream();

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException {@inheritDoc}
     */
    @Override
    public View viewGe(byte[] key);

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException {@inheritDoc}
     */
    @Override
    public View viewGt(byte[] key);

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException {@inheritDoc}
     */
    @Override
    public View viewLe(byte[] key);

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException {@inheritDoc}
     */
    @Override
    public View viewLt(byte[] key);

    /**
     * {@inheritDoc}
     *
     * @throws NullPointerException {@inheritDoc}
     * @throws IllegalArgumentException {@inheritDoc}
     */
    @Override
    public View viewPrefix(byte[] prefix, int trim);

    /**
     * {@inheritDoc}
     */
    @Override
    public View viewReverse();

    /**
     * {@inheritDoc}
     */
    @Override
    public View viewUnmodifiable();

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isUnmodifiable();

    /**
     * Verifies the integrity of the index.
     *
     * @param observer optional observer; pass null for default
     * @return true if verification passed
     */
    public boolean verify(VerificationObserver observer) throws IOException;

    /**
     * Closes this index reference, causing it to become empty and {@link
     * ClosedIndexException unmodifiable}. The underlying index is still valid
     * and can be re-opened.  Closing an index is relatively expensive, and so
     * it should be kept open if frequently accessed.
     *
     * <p>An index cannot be closed if any cursors are accessing it. Also,
     * indexes should not be closed if they are referenced by active
     * transactions. Although closing the index is safe, the transaction might
     * re-open it.
     *
     * @throws IllegalStateException if any cursors are active in this index
     */
    @Override
    public void close() throws IOException;

    public boolean isClosed();

    /**
     * Fully closes and removes an empty index. An exception is thrown if the index isn't empty
     * or if an in-progress transaction is modifying it.
     *
     * @throws IllegalStateException if index isn't empty or any pending transactional changes
     * @throws ClosedIndexException if this index reference is closed
     */
    public void drop() throws IOException;
}
