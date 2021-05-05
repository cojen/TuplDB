/*
 *  Copyright 2021 Cojen.org
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

/**
 * 
 *
 * @author Brian S O'Neill
 */
public interface RowIndex<R> extends RowView<R>, Closeable {
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

    // Returns an unmodifiable reference to a secondary index or alternate key.
    //public RowView viewIndex(String... columns);

    /**
     * Verifies the integrity of the index.
     *
     * @param observer optional observer; pass null for default
     * @return true if verification passed
     */
    //public boolean verify(VerificationObserver observer) throws IOException;

    /**
     * Closes this index reference, causing it to appear empty and {@linkplain
     * ClosedIndexException unmodifiable}.
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
     * @throws IllegalStateException if index isn't empty or if any pending transactional
     * changes exist
     * @throws ClosedIndexException if this index reference is closed
     * @see Database#deleteIndex Database.deleteIndex
     */
    //public void drop() throws IOException;
}
