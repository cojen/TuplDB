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

package org.cojen.tupl.core;

import java.lang.invoke.MethodHandle;

import org.cojen.tupl.Entry;

import org.cojen.tupl.table.RowMaker;

/**
 * Singleton Entry populator use by the SortScanner class.
 *
 * @author Brian S O'Neill
 */
public final class EntryPopulator {
    /**
     * Given an entry, sets the key and value, returning the original entry or a new one if
     * given null.
     *
     *   Entry populate(Entry e, byte[] key, byte[] value);
     */
    public static final MethodHandle THE = RowMaker.makePopulator(Entry.class);
}
