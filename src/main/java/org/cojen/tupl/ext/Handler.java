/*
 *  Copyright 2019 Cojen.org
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

package org.cojen.tupl.ext;

import java.io.IOException;

import org.cojen.tupl.Database;

/**
 * Common interfaces for handlers.
 *
 * @author Brian S O'Neill
 */
public interface Handler {
    /**
     * Called once when the database is opened, immediately before recovery is performed. A
     * recovery handler can save the database reference and call upon it later. By default,
     * this method does nothing.
     */
    default void init(Database db) throws IOException {
    }
}
