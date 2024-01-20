/*
 *  Copyright (C) 2022 Cojen.org
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
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.table;

import java.io.IOException;

import org.cojen.tupl.Index;

/**
 * @author Brian S O'Neill
 * @see JoinedUpdater
 */
public abstract class TriggerIndexAccessor {
    TriggerIndexAccessor() {
    }

    /**
     * Notification from an index trigger that an entry was stored into an index. This method
     * is only called by the storeP and insertP trigger methods.
     */
    public abstract void stored(Index ix, byte[] key, byte[] value) throws IOException;

    /**
     * Returns true if the delete action was performed, or else the caller must do the delete.
     * It's assumed that the accessor has a reference to the correct transaction. This method
     * is called by the delete (no row) trigger method.
     */
    public abstract boolean delete(Index ix, byte[] key) throws IOException;
}
