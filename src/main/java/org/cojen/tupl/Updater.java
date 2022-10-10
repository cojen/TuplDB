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

import java.io.IOException;

import org.cojen.tupl.core.Utils;

/**
 * Scans through all entries in a view, updating them along the way. Updater implementations
 * which perform pre-fetching can be more efficient than a {@linkplain Cursor cursor}. Any
 * exception thrown by an updating action automatically closes the Updater.
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
public interface Updater extends Scanner {
    /**
     * Empty marker returned by {@link EntryFunction} to indicate that no update should be
     * performed.
     */
    // Note: Constant is intentionally the same as NOT_LOADED, to protect against a broken
    // Updater which is acting upon a Cursor with autoload mode off. A dumb action which
    // returns the value instance (thus forcing an update) won't accidentally destroy anything.
    public static final byte[] NO_UPDATE = Cursor.NOT_LOADED;

    /**
     * Update the current value and then step to the next entry. Pass null to delete the entry.
     *
     * @return false if no more entries remain and updater has been closed
     */
    boolean update(byte[] value) throws IOException;

    /**
     * Applies the given updating action for each remaining entry, and then closes the updater.
     * An entry is updated to the value returned by the action, or it's deleted when the action
     * returns null. If the action returns {@link #NO_UPDATE}, then no update is performed.
     */
    default void updateAll(EntryFunction action) throws IOException {
        for (byte[] key; (key = key()) != null; ) {
            byte[] value;
            try {
                value = action.apply(key, value());
            } catch (Throwable e) {
                throw Utils.fail(this, e);
            }
            if (value != NO_UPDATE) {
                update(value);
            } else {
                step();
            }
        }
    }
}
