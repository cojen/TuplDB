/*
 *  Copyright (C) 2016-2018 Cojen.org
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

import java.io.InterruptedIOException;
import java.io.IOException;

/**
 * Utility for sorting and filling up new indexes.
 *
 * @author Brian S O'Neill
 * @see Database#newSorter Database.newSorter
 */
public interface Sorter {
    /**
     * Add an entry into the sorter. If multiple entries are added with matching keys, only the
     * last one added is kept.
     *
     * @throws IllegalStateException if sort is finishing in another thread
     * @throws InterruptedIOException if reset by another thread
     */
    public void add(byte[] key, byte[] value) throws IOException;

    /**
     * Finish sorting the entries, and return a temporary index with the results.
     *
     * @throws IllegalStateException if sort is finishing in another thread
     * @throws InterruptedIOException if reset by another thread
     */
    public Index finish() throws IOException;

    /**
     * Discards all the entries and frees up space in the database. Can be called to interrupt
     * any sort which is in progress.
     */
    public void reset() throws IOException;
}
