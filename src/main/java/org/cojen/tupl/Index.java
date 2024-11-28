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

import org.cojen.tupl.diag.IndexStats;
import org.cojen.tupl.diag.VerificationObserver;

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
    public long id();

    /**
     * @return unique user-specified index name
     */
    public byte[] name();

    /**
     * @return name decoded as UTF-8
     */
    public String nameString();

    /**
     * Returns a {@code Table} instance which stores rows in this index. Bypassing the {@code
     * Table} and storing directly into this index should be avoided, since it interferes
     * with row encoding. Mixing encoding strategies can cause data corruption.
     *
     * @see Database#openTable
     * @return shared {@code Table} instance
     */
    // Note: This method is defined on Index and not View because schema metadata is stored
    // against an index id. A View doesn't have an id.
    public <R> Table<R> asTable(Class<R> type) throws IOException;

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
    public IndexStats analyze(byte[] lowKey, byte[] highKey) throws IOException;

    /**
     * Verifies the integrity of the index. Using multiple threads speeds up verification,
     * even though some nodes might be visited multiple times.
     *
     * @param observer optional observer; pass null for default
     * @param numThreads pass 0 for default, or if negative, the actual number will be {@code
     * (-numThreads * availableProcessors)}.
     * @return true if verification passed
     */
    public boolean verify(VerificationObserver observer, int numThreads) throws IOException;

    /**
     * Closes this index reference. The underlying index is still valid and can be re-opened,
     * unless it's a {@linkplain Database#newTemporaryIndex temporary} index.
     *
     * <p>In general, indexes should not be closed if they are referenced by active
     * transactions. Although closing the index is safe, the transaction might re-open it.
     *
     * @see ClosedIndexException
     */
    @Override
    public void close() throws IOException;

    public boolean isClosed();

    /**
     * Fully closes and removes an empty index. An exception is thrown if the index isn't empty
     * or if an in-progress transaction is modifying it.
     *
     * @throws IllegalStateException if index isn't empty or if any pending transactional
     * changes exist
     * @throws ClosedIndexException if this index reference is closed
     * @see Database#deleteIndex Database.deleteIndex
     */
    public void drop() throws IOException;
}
