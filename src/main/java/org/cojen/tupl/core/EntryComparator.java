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

import java.util.Comparator;

import org.cojen.tupl.Entry;

/**
 * 
 *
 * @author Brian S O'Neill
 */
// FIXME: remove?
public final class EntryComparator implements Comparator<Entry> {
    public static final EntryComparator THE = new EntryComparator();

    private EntryComparator() {
    }

    @Override
    public int compare(Entry a, Entry b) {
        return Utils.KEY_COMPARATOR.compare(a.key(), b.key());
    }
}
