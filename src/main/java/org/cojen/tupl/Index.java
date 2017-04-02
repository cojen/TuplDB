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
     * Select a few entries, and delete them from the index. Implementation should attempt to
     * evict entries which haven't been recently used, but it might select them at random.
     *
     * @param txn optional
     * @param lowKey inclusive lowest key in the evictable range; pass null for open range
     * @param highKey exclusive highest key in the evictable range; pass null for open range
     * @param evictionFilter callback which determines which entries are allowed to be evicted;
     * pass null to evict all selected entries
     * @param autoload pass true to also load values and pass them to the filter
     * @return sum of the key and value lengths which were evicted, or 0 if none were evicted
     */
    public long evict(Transaction txn, byte[] lowKey, byte[] highKey,
                      Filter evictionFilter, boolean autoload)
        throws IOException;

    /**
     * Estimates the size of this index with a single random probe. To improve the estimate,
     * average several analysis results together.
     *
     * @param lowKey inclusive lowest key in the analysis range; pass null for open range
     * @param highKey exclusive highest key in the analysis range; pass null for open range
     */
    public abstract Stats analyze(byte[] lowKey, byte[] highKey) throws IOException;

    /**
     * Collection of stats from the {@link Index#analyze analyze} method.
     */
    public static class Stats implements Cloneable, Serializable {
        private static final long serialVersionUID = 3L;

        public double entryCount;
        public double keyBytes;
        public double valueBytes;
        public double freeBytes;
        public double totalBytes;

        public Stats(double entryCount,
                     double keyBytes,
                     double valueBytes,
                     double freeBytes,
                     double totalBytes)
        {
            this.entryCount = entryCount;
            this.keyBytes = keyBytes;
            this.valueBytes = valueBytes;
            this.freeBytes = freeBytes;
            this.totalBytes = totalBytes;
        } 

        /**
         * Returns the estimated number of index entries.
         */
        public double entryCount() {
            return entryCount;
        }

        /**
         * Returns the estimated amount of bytes occupied by keys in the index.
         */
        public double keyBytes() {
            return keyBytes;
        }

        /**
         * Returns the estimated amount of bytes occupied by values in the index.
         */
        public double valueBytes() {
            return valueBytes;
        }

        /**
         * Returns the estimated amount of free bytes in the index.
         */
        public double freeBytes() {
            return freeBytes;
        }

        /**
         * Returns the estimated total amount of bytes in the index.
         */
        public double totalBytes() {
            return totalBytes;
        }

        /**
         * Adds stats into a new object.
         */
        public Stats add(Stats augend) {
            return new Stats(entryCount + augend.entryCount,
                             keyBytes + augend.keyBytes,
                             valueBytes + augend.valueBytes,
                             freeBytes + augend.freeBytes,
                             totalBytes + augend.totalBytes);
        }

        /**
         * Subtract stats into a new object.
         */
        public Stats subtract(Stats subtrahend) {
            return new Stats(entryCount - subtrahend.entryCount,
                             keyBytes - subtrahend.keyBytes,
                             valueBytes - subtrahend.valueBytes,
                             freeBytes - subtrahend.freeBytes,
                             totalBytes - subtrahend.totalBytes);
        }

        /**
         * Divide the stats by a scalar into a new object.
         */
        public Stats divide(double scalar) {
            return new Stats(entryCount / scalar,
                             keyBytes / scalar,
                             valueBytes / scalar,
                             freeBytes / scalar,
                             totalBytes / scalar);
        }

        /**
         * Round the stats to whole numbers into a new object.
         */
        public Stats round() {
            return new Stats(Math.round(entryCount),
                             Math.round(keyBytes),
                             Math.round(valueBytes),
                             Math.round(freeBytes),
                             Math.round(totalBytes));
        }

        /**
         * Divide the stats by a scalar and round to whole numbers into a new object.
         */
        public Stats divideAndRound(double scalar) {
            return new Stats(Math.round(entryCount / scalar),
                             Math.round(keyBytes / scalar),
                             Math.round(valueBytes / scalar),
                             Math.round(freeBytes / scalar),
                             Math.round(totalBytes / scalar));
        }

        @Override
        public Stats clone() {
            try {
                return (Stats) super.clone();
            } catch (CloneNotSupportedException e) {
                throw Utils.rethrow(e);
            }
        }

        @Override
        public int hashCode() {
            long hash = Double.doubleToLongBits(entryCount);
            hash = hash * 31 + Double.doubleToLongBits(keyBytes);
            hash = hash * 31 + Double.doubleToLongBits(valueBytes);
            hash = hash * 31 + Double.doubleToLongBits(freeBytes);
            hash = hash * 31 + Double.doubleToLongBits(totalBytes);
            return (int) Utils.scramble(hash);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj != null && obj.getClass() == Stats.class) {
                Stats other = (Stats) obj;
                return entryCount == other.entryCount
                    && keyBytes == other.keyBytes
                    && valueBytes == other.valueBytes
                    && freeBytes == other.freeBytes
                    && totalBytes == other.totalBytes;
            }
            return false;
        }

        @Override
        public String toString() {
            StringBuilder b = new StringBuilder("Index.Stats {");

            boolean any = false;
            any = append(b, any, "entryCount", entryCount);
            any = append(b, any, "keyBytes", keyBytes);
            any = append(b, any, "valueBytes", valueBytes);
            any = append(b, any, "freeBytes", freeBytes);
            any = append(b, any, "totalBytes", totalBytes);

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
