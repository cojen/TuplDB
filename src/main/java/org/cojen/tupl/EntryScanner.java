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

import java.util.Comparator;

import org.cojen.tupl.core.Utils;

/**
 * Scans through all entries in a view and passes them to a consumer. EntryScanner implementations
 * which perform pre-fetching can be more efficient than a {@linkplain Cursor cursor}. Any
 * exception thrown by a scan action automatically closes the EntryScanner.
 *
 * <p>EntryScanner instances can only be safely used by one thread at a time, and they must be
 * closed when no longer needed. Instances can be exchanged by threads, as long as a
 * happens-before relationship is established. Without proper exclusion, multiple threads
 * interacting with a EntryScanner instance may cause database corruption.
 *
 * @author Brian S O'Neill
 * @see View#newScanner View.newScanner
 * @see EntryUpdater
 */
public interface EntryScanner extends Closeable {
    /**
     * Returns a comparator for the ordering of this scanner, or null if unordered.
     */
    Comparator<byte[]> comparator();

    /**
     * Returns an uncopied reference to the current key, or null if closed. Array contents
     * must not be modified.
     */
    byte[] key();

    /**
     * Returns an uncopied reference to the current value, which might be null or {@link
     * Cursor#NOT_LOADED}. Array contents can be safely modified.
     */
    byte[] value();

    /**
     * Step to the next entry.
     *
     * @return false if no more entries remain and scanner has been closed
     */
    boolean step() throws IOException;

    /**
     * Calls the given action for each remaining entry, and then closes the scanner.
     */
    default void scanAll(EntryConsumer action) throws IOException {
        for (byte[] key; (key = key()) != null; step()) {
            try {
                action.accept(key, value());
            } catch (Throwable e) {
                throw Utils.fail(this, e);
            }
        }
    }

    @Override
    public void close() throws IOException;
}
