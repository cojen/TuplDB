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

import java.io.Flushable;
import java.io.IOException;

/**
 * Scans through all entries in a view, updating them along the way. Updater implementations
 * which perform pre-fetching can be more efficient than a {@link Cursor cursor}. Any exception
 * thrown by an updating action automatically closes the Updater.
 *
 * <p>Updater instances can only be safely used by one thread at a time, and they must be
 * closed when no longer needed. Instances can be exchanged by threads, as long as a
 * happens-before relationship is established. Without proper exclusion, multiple threads
 * interacting with an Updater instance may cause database corruption.
 *
 * @author Brian S O'Neill
 * @see View#newUpdater View.newUpdater
 * @see Scanner
 */
public interface Updater extends Scanner, Flushable {
    /**
     * Update the current value and then step to the next entry.
     *
     * @return false if no more entries remain and updater has been closed
     */
    boolean update(byte[] value) throws IOException;

    /**
     * Applies the given updating action for each remaining entry, and then closes the updater.
     */
    default void updateAll(EntryFunction action) throws IOException {
        while (true) {
            byte[] key = key();
            if (key == null) {
                return;
            }
            byte[] value;
            try {
                value = action.apply(key, value());
            } catch (Throwable e) {
                throw ViewUtils.fail(this, e);
            }
            update(value);
        }
    }

    /**
     * Ensures that any queued update operations are applied; flushing is automatically
     * performed when the updater is closed.
     */
    @Override
    default void flush() throws IOException {
    }
}
