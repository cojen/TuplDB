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
import java.io.Serializable;

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
     *
     * @throws IllegalStateException {@inheritDoc}
     * @throws LockFailureException {@inheritDoc} 
     * @throws DeadlockException {@inheritDoc}
     * @throws ViewConstraintException {@inheritDoc}
     */
    @Override
    public LockResult lockShared(Transaction txn, byte[] key)
        throws LockFailureException, ViewConstraintException;

    /**
     * {@inheritDoc}
     *
     * @throws LockFailureException {@inheritDoc}
     * @throws DeadlockException {@inheritDoc}
     * @throws ViewConstraintException {@inheritDoc}
     */
    @Override
    public LockResult lockUpgradable(Transaction txn, byte[] key)
        throws LockFailureException, ViewConstraintException;

    /**
     * {@inheritDoc}
     *
     * @throws LockFailureException {@inheritDoc}
     * @throws DeadlockException {@inheritDoc}
     * @throws ViewConstraintException {@inheritDoc}
     */
    @Override
    public LockResult lockExclusive(Transaction txn, byte[] key)
        throws LockFailureException, ViewConstraintException;

    /**
     * {@inheritDoc}
     *
     * @throws ViewConstraintException {@inheritDoc}
     */
    @Override
    public LockResult lockCheck(Transaction txn, byte[] key) throws ViewConstraintException;

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream newStream();

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
     *
     * @throws NullPointerException {@inheritDoc}
     */
    @Override
    public View viewTransformed(Transformer transformer);

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
     * Estimates the size of this index with a single random probe. To improve the estimate,
     * average several analysis results together.
     *
     * @param lowKey inclusive lowest key in the analysis range; pass null for open range
     * @param highKey exclusive highest key in the analysis range; pass null for open range
     */
    public abstract Stats analyze(byte[] lowKey, byte[] highKey) throws IOException;

    /**
     * Immutable copy of stats from the {@link Index#analyze analyze} method.
     */
    public static class Stats implements Serializable {
        private static final long serialVersionUID = 2L;

        private final double mEntryCount;
        private final double mKeyBytes;
        private final double mValueBytes;
        private final double mFreeBytes;
        private final double mTotalBytes;

        public Stats(double entryCount,
                     double keyBytes,
                     double valueBytes,
                     double freeBytes,
                     double totalBytes)
        {
            mEntryCount = entryCount;
            mKeyBytes = keyBytes;
            mValueBytes = valueBytes;
            mFreeBytes = freeBytes;
            mTotalBytes = totalBytes;
        } 

        /**
         * Returns the estimated number of index entries.
         */
        public double entryCount() {
            return mEntryCount;
        }

        /**
         * Returns the estimated amount of bytes occupied by keys in the index.
         */
        public double keyBytes() {
            return mKeyBytes;
        }

        /**
         * Returns the estimated amount of bytes occupied by values in the index.
         */
        public double valueBytes() {
            return mValueBytes;
        }

        /**
         * Returns the estimated amount of free bytes in the index.
         */
        public double freeBytes() {
            return mFreeBytes;
        }

        /**
         * Returns the estimated total amount of bytes in the index.
         */
        public double totalBytes() {
            return mTotalBytes;
        }

        /**
         * Adds stats into a new object.
         */
        public Stats add(Stats augend) {
            return new Stats(entryCount() + augend.entryCount(),
                             keyBytes() + augend.keyBytes(),
                             valueBytes() + augend.valueBytes(),
                             freeBytes() + augend.freeBytes(),
                             totalBytes() + augend.totalBytes());
        }

        /**
         * Subtract stats into a new object.
         */
        public Stats subtract(Stats subtrahend) {
            return new Stats(entryCount() - subtrahend.entryCount(),
                             keyBytes() - subtrahend.keyBytes(),
                             valueBytes() - subtrahend.valueBytes(),
                             freeBytes() - subtrahend.freeBytes(),
                             totalBytes() - subtrahend.totalBytes());
        }

        /**
         * Divide the stats by a scalar into a new object.
         */
        public Stats divide(double scalar) {
            return new Stats(entryCount() / scalar,
                             keyBytes() / scalar,
                             valueBytes() / scalar,
                             freeBytes() / scalar,
                             totalBytes() / scalar);
        }

        /**
         * Round the stats to whole numbers into a new object.
         */
        public Stats round() {
            return new Stats(Math.round(entryCount()),
                             Math.round(keyBytes()),
                             Math.round(valueBytes()),
                             Math.round(freeBytes()),
                             Math.round(totalBytes()));
        }

        /**
         * Divide the stats by a scalar and round to whole numbers into a new object.
         */
        public Stats divideAndRound(double scalar) {
            return new Stats(Math.round(entryCount() / scalar),
                             Math.round(keyBytes() / scalar),
                             Math.round(valueBytes() / scalar),
                             Math.round(freeBytes() / scalar),
                             Math.round(totalBytes() / scalar));
        }

        @Override
        public String toString() {
            StringBuilder b = new StringBuilder("Index.Stats {");

            boolean any = false;
            any = append(b, any, "entryCount", entryCount());
            any = append(b, any, "keyBytes", keyBytes());
            any = append(b, any, "valueBytes", valueBytes());
            any = append(b, any, "freeBytes", freeBytes());
            any = append(b, any, "totalBytes", totalBytes());

            b.append('}');
            return b.toString();
        }

        private static boolean append(StringBuilder b, boolean any, String name, double value) {
            if (!Double.isNaN(value)) {
                if (any) {
                    b.append(", ");
                }

                b.append(name).append('=');

                long v = (long) value;
                if (v == value) {
                    b.append(v);
                } else {
                    b.append(value);
                }
                
                any = true;
            }

            return any;
        }
    }

    /**
     * Verifies the integrity of the index.
     *
     * @param observer optional observer; pass null for default
     * @return true if verification passed
     */
    public boolean verify(VerificationObserver observer) throws IOException;

    /**
     * Closes this index reference, causing it to appear empty and {@link ClosedIndexException
     * unmodifiable}. The underlying index is still valid and can be re-opened.
     *
     * <p>In general, indexes should not be closed if they are referenced by active
     * transactions. Although closing the index is safe, the transaction might re-open it.
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
     * @see Database#deleteIndex Database.deleteIndex
     */
    public void drop() throws IOException;
}
